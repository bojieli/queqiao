package pep

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/metrics"
	"github.com/bojieli/queqiao/internal/pathmodel"
	"github.com/bojieli/queqiao/internal/pathsim"
	"github.com/bojieli/queqiao/internal/protocol"
)

// shadowEndpoints is a client and gateway with the carrier-liveness observer
// on, each logging JSON lines into its own buffer, as a measurement campaign
// would collect them.
type shadowEndpoints struct {
	socks                         string
	destination                   net.Listener
	relay                         *pathsim.Relay
	clientLogs, gatewayLogs       *lockedBuffer
	clientMetrics, gatewayMetrics *metrics.Registry
}

func shadowPair(t *testing.T, path *pathsim.Config, serve func(net.Listener), gatewayHook func(protocol.Frame) error) *shadowEndpoints {
	t.Helper()
	pathmodel.Reset()
	t.Cleanup(pathmodel.Reset)
	e := &shadowEndpoints{clientLogs: &lockedBuffer{}, gatewayLogs: &lockedBuffer{}, clientMetrics: metrics.New(), gatewayMetrics: metrics.New()}
	// QUEQIAO_CARRIER_SHADOW_LOGS keeps both endpoints' logs, for running
	// scripts/carrier_shadow_summary.py over a real trace.
	if dir := os.Getenv("QUEQIAO_CARRIER_SHADOW_LOGS"); dir != "" {
		t.Cleanup(func() {
			_ = os.WriteFile(filepath.Join(dir, t.Name()+"-client.jsonl"), []byte(e.clientLogs.String()), 0o600)
			_ = os.WriteFile(filepath.Join(dir, t.Name()+"-gateway.jsonl"), []byte(e.gatewayLogs.String()), 0o600)
		})
	}
	destination, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = destination.Close() })
	go serve(destination)
	e.destination = destination
	shadow := CarrierShadowConfig{Enabled: true, SuspectPTOs: 2, ReplacePTOs: 4, Interval: 10 * time.Millisecond}
	certificate, roots := testCertificate(t)
	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0", Credentials: certificate,
		DestinationPolicy: DestinationPolicy{AllowPrivate: true}, EnableQUIC: true,
		Logger:  slog.New(slog.NewJSONHandler(e.gatewayLogs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Metrics: e.gatewayMetrics, HandshakeTimeout: 30 * time.Second,
		CarrierShadow: shadow, testLaneWriteHook: gatewayHook,
	})
	if err != nil {
		t.Fatal(err)
	}
	remote := packetConn.LocalAddr().String()
	if path != nil {
		relay, err := pathsim.New("127.0.0.1:0", remote, *path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = relay.Close() })
		remote = relay.LocalAddr()
		e.relay = relay
	}
	clientListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(ClientConfig{
		ListenAddr: clientListener.Addr().String(), RemoteAddr: remote, Credentials: roots, Transport: TransportQUIC,
		Metrics:       e.clientMetrics,
		Logger:        slog.New(slog.NewJSONHandler(e.clientLogs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		CarrierShadow: shadow,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = server.ServePacketConn(ctx, packetConn) }()
	go func() { _ = client.ServeListener(ctx, clientListener) }()
	e.socks = clientListener.Addr().String()
	return e
}

// logRecords returns every JSON record with this message.
func logRecords(t *testing.T, logs, msg string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(logs, "\n") {
		if !strings.Contains(line, msg) {
			continue
		}
		record := map[string]any{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		if record["msg"] == msg {
			out = append(out, record)
		}
	}
	return out
}

// waitForRecord polls until some record with this message satisfies match.
func waitForRecord(t *testing.T, logs *lockedBuffer, msg string, budget time.Duration, match func(map[string]any) bool) map[string]any {
	t.Helper()
	deadline := time.Now().Add(budget)
	for {
		for _, record := range logRecords(t, logs.String(), msg) {
			if match(record) {
				return record
			}
		}
		if time.Now().After(deadline) {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// boundCarriers is the set of carriers a flow's lanes were bound to.
func boundCarriers(t *testing.T, logs string) map[float64]bool {
	out := map[float64]bool{}
	for _, record := range logRecords(t, logs, "carrier shadow lane bound") {
		out[record["carrier_id"].(float64)] = true
	}
	return out
}

// A blackholed connection: every packet to and from the client's existing
// source port is dropped while a flow is busy both ways. Each endpoint's
// observer watches the connection's probe timeouts mount with no transport
// progress and labels it SUSPECT; the gateway, which never closes it itself,
// labels it REPLACE and finally DEAD on QUIC's idle timeout. The watchdog's
// stall on it is recorded with the carrier, so the comparison can measure
// how much later than the 3xminRTT flow timer the design would have acted.
// The carrier manager runs as without the observer and replaces the carrier
// over a fresh source port.
func TestCarrierShadowLabelsABlackholedConnectionDead(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out a QUIC idle timeout")
	}
	requireStableImpairmentClock(t)
	e := shadowPair(t, &pathsim.Config{OneWayDelay: 20 * time.Millisecond, Seed: 5}, echoDestination, nil)
	conn, err := trySocksDial(e.socks, e.destination, 20*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(60 * time.Second))
	// Keep the flow busy in both directions until the end of the test.
	var stop atomic.Bool
	defer stop.Store(true)
	go func() { _, _ = io.Copy(io.Discard, conn) }()
	go func() {
		chunk := make([]byte, 4096)
		for !stop.Load() {
			if _, err := conn.Write(chunk); err != nil {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	time.Sleep(500 * time.Millisecond)
	before := boundCarriers(t, e.clientLogs.String())
	if len(before) == 0 {
		t.Fatalf("no carrier was bound on the client:\n%s", e.clientLogs.String())
	}
	gatewayBefore := boundCarriers(t, e.gatewayLogs.String())
	e.relay.BlackholeSources()
	blackholed := time.Now()

	onOld := func(record map[string]any) bool {
		id, _ := record["carrier_id"].(float64)
		return before[id]
	}
	if waitForRecord(t, e.clientLogs, "carrier shadow state", 10*time.Second, func(r map[string]any) bool { return onOld(r) && r["to"] == "suspect" }) == nil {
		t.Fatalf("the client never suspected the blackholed carrier:\n%s", e.clientLogs.String())
	}
	// Past SUSPECT the client's old carrier is either labelled for
	// replacement or retired first by the carrier manager, which closes it
	// locally: that ends its episode censored, not dead.
	next := waitForRecord(t, e.clientLogs, "carrier shadow state", 10*time.Second, func(r map[string]any) bool {
		return onOld(r) && (r["to"] == "replace" || r["to"] == "closed")
	})
	if next == nil {
		t.Fatalf("the client's blackholed carrier stayed SUSPECT:\n%s", e.clientLogs.String())
	}
	t.Logf("client's blackholed carrier went %v -> %v at %v PTOs, %v after the blackhole", next["from"], next["to"], next["pto_since_progress"], time.Since(blackholed).Round(time.Millisecond))
	// The watchdog declares the stall after three minimum RTTs, before k_s
	// probe timeouts have accrued: at that instant there is no carrier
	// evidence yet and the endpoint waits. The record carries the carrier so
	// the comparison can follow it to REPLACE.
	stall := waitForRecord(t, e.clientLogs, "carrier shadow stall", 10*time.Second, onOld)
	if stall == nil {
		t.Fatalf("no client stall was recorded on the blackholed carrier:\n%s", e.clientLogs.String())
	}
	if stall["legacy_remedy"] != "none" {
		t.Fatalf("stall record = %v", stall)
	}
	t.Logf("stall on the blackholed carrier: state %v, design remedy %v, %v PTOs", stall["carrier_state"], stall["design_remedy"], stall["pto_since_progress"])
	// The gateway holds the dead connection until QUIC's idle timeout.
	dead := waitForRecord(t, e.gatewayLogs, "carrier shadow state", laneDeadPathDetection+10*time.Second, func(r map[string]any) bool {
		id, _ := r["carrier_id"].(float64)
		return gatewayBefore[id] && r["to"] == "dead"
	})
	if dead == nil {
		t.Fatalf("the gateway never labelled the blackholed carrier dead:\n%s", e.gatewayLogs.String())
	}
	if dead["reason"] != "idle_timeout" {
		t.Fatalf("gateway dead record = %v", dead)
	}
	t.Logf("gateway labelled the blackholed carrier dead (%v) %v after the blackhole", dead["reason"], time.Since(blackholed).Round(time.Millisecond))
	// The client's old carrier ends too, dead on its idle timeout or closed
	// by the carrier manager; either way never active again.
	for _, record := range logRecords(t, e.clientLogs.String(), "carrier shadow state") {
		if onOld(record) && record["to"] == "active" && record["reason"] == "progress" {
			t.Fatalf("a blackholed carrier made progress: %v", record)
		}
	}
	if got := e.clientMetrics.Snapshot().CarrierShadow; got.Transitions[metrics.CarrierShadowSuspect] == 0 {
		t.Fatalf("client carrier-shadow metrics = %+v", got)
	}
	// The gateway's view of the same carrier walks the whole machine.
	gateway := e.gatewayMetrics.Snapshot().CarrierShadow
	if gateway.Transitions[metrics.CarrierShadowSuspect] == 0 || gateway.Transitions[metrics.CarrierShadowReplace] == 0 || gateway.Transitions[metrics.CarrierShadowDead] == 0 {
		t.Fatalf("gateway carrier-shadow metrics = %+v", gateway)
	}
	if gateway.PTOReached[metrics.CarrierShadowPTOBuckets-1] == 0 && gateway.PTOReached[3] == 0 {
		t.Fatalf("the gateway's dead episode is missing from the histogram: %+v", gateway)
	}
}

// A flow stalled on a healthy carrier: the receiving application stops
// reading, so flow credit stops while QUIC has nothing unacknowledged. The
// watchdog declares stalls and the client waits; the observer labels the
// carrier ACTIVE, records that the transport was acknowledging, and agrees
// with the waiting. This is the shadow form of §15.1 test 8 ("no remedy on a
// live stream").
func TestCarrierShadowLabelsAStalledFlowOnAHealthyCarrierLive(t *testing.T) {
	if testing.Short() {
		t.Skip("brings up QUIC")
	}
	// The destination accepts and never reads: the gateway's receive budget
	// fills, it stops reading the flow's stream, and the client's bytes stay
	// without flow credit while QUIC has acknowledged every packet it sent.
	held := make(chan net.Conn, 16)
	t.Cleanup(func() {
		for {
			select {
			case conn := <-held:
				_ = conn.Close()
			default:
				return
			}
		}
	})
	e := shadowPair(t, nil, func(listener net.Listener) {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			select {
			case held <- conn:
			default:
				_ = conn.Close()
			}
		}
	}, nil)
	conn, err := trySocksDial(e.socks, e.destination, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	go func() {
		chunk := make([]byte, 64*1024)
		for {
			if _, err := conn.Write(chunk); err != nil {
				return
			}
		}
	}()
	stall := waitForRecord(t, e.clientLogs, "carrier shadow stall", 15*time.Second, func(map[string]any) bool { return true })
	if stall == nil {
		t.Fatalf("the watchdog never stalled the flow:\n%s", e.clientLogs.String())
	}
	if stall["carrier_state"] != "active" || stall["transport_acked"] != true {
		t.Fatalf("stall on a healthy carrier = %v", stall)
	}
	if remedy := stall["design_remedy"]; remedy != "wait" && remedy != "wait_move_coded" {
		t.Fatalf("design remedy on a live carrier = %v", remedy)
	}
	if stall["legacy_remedy"] != "none" {
		t.Fatalf("endpoint remedy = %v", stall["legacy_remedy"])
	}
	got := e.clientMetrics.Snapshot().CarrierShadow
	if got.Stalls[metrics.CarrierShadowReplaceCarrier] != 0 || got.Transitions[metrics.CarrierShadowSuspect] != 0 {
		t.Fatalf("a healthy carrier was treated as carrier evidence: %+v", got)
	}
	if got.StallsTransportAcked == 0 {
		t.Fatalf("no stall recorded the transport as acknowledged: %+v", got)
	}
	// The watchdog still counted the stall, and nothing was dialled for it.
	if e.clientMetrics.Snapshot().FlowStallsDetected == 0 {
		t.Fatal("the watchdog did not count the stall")
	}
	if n := e.clientMetrics.Snapshot().LaneRescueAttempts; n != 0 {
		t.Fatalf("%d rescue dials for a stall on a live stream", n)
	}
}

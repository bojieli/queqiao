package pep

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/metrics"
	"github.com/bojieli/queqiao/internal/pathmodel"
	"github.com/bojieli/queqiao/internal/pathsim"
	"github.com/bojieli/queqiao/internal/protocol"
)

// Stage 8 (recovery=carrier) end to end: a real client and gateway on real
// fork send loops over an emulated path (internal/pathsim), with the whole
// stage 1-7 bundle and classifier=phased on both ends (design §14, §15.1).

// carrierPairOptions shape one client/gateway pair.
type carrierPairOptions struct {
	path pathsim.Config
	// serve runs the destination; nil echoes.
	serve func(net.Listener)
	// clientRecovery and gatewayRecovery default to carrier.
	clientRecovery, gatewayRecovery RecoveryMode
	// releasedGateway runs the gateway with every switch off and released
	// admission: an old peer.
	releasedGateway bool
	// legacyClient runs the client with every switch off: an old peer.
	legacyClient bool
	transport    TransportKind
	clientWires  []byte
	gatewayHook  func(protocol.Frame) error
	recovery     CarrierRecoveryConfig
	maxSessions  int
	// onFlow is handed every flow the client starts.
	onFlow func(*multipathFlow)
}

type carrierPair struct {
	socks                   string
	destination             net.Listener
	relay                   *pathsim.Relay
	client                  *Client
	server                  *Server
	clientLogs, gatewayLogs *lockedBuffer
	clientMetrics           *metrics.Registry
	gatewayMetrics          *metrics.Registry
	// gatewayCarrier says the gateway runs recovery=carrier, so it demotes
	// no lane either.
	gatewayCarrier bool
	tcpRelay       net.Listener
}

func newCarrierPair(t *testing.T, o carrierPairOptions) *carrierPair {
	t.Helper()
	requireStableImpairmentClock(t)
	pathmodel.Reset()
	t.Cleanup(pathmodel.Reset)
	if o.clientRecovery == "" {
		o.clientRecovery = RecoveryCarrier
	}
	if o.gatewayRecovery == "" {
		o.gatewayRecovery = RecoveryCarrier
	}
	if o.transport == "" {
		o.transport = TransportQUIC
	}
	if o.serve == nil {
		o.serve = echoDestination
	}
	if o.recovery.Interval == 0 {
		o.recovery.Interval = 10 * time.Millisecond
	}
	p := &carrierPair{clientLogs: &lockedBuffer{}, gatewayLogs: &lockedBuffer{}, clientMetrics: metrics.New(), gatewayMetrics: metrics.New()}
	destination, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = destination.Close() })
	go o.serve(destination)
	p.destination = destination
	certificate, roots := testCertificate(t)
	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	scfg := ServerConfig{
		ListenAddr: "127.0.0.1:0", Credentials: certificate,
		DestinationPolicy: DestinationPolicy{AllowPrivate: true}, EnableQUIC: true,
		EnableTCP: o.transport == TransportAuto,
		Logger:    slog.New(slog.NewJSONHandler(p.gatewayLogs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Metrics:   p.gatewayMetrics, HandshakeTimeout: 30 * time.Second, testLaneWriteHook: o.gatewayHook,
		MaxSessions: o.maxSessions,
	}
	if o.releasedGateway {
		scfg.testReleasedAdmission = true
	} else {
		scfg.CCAPI, scfg.Sched, scfg.CongestionScope = CCAPIV2, SchedClass, CongestionScopePath
		scfg.CodedRetry, scfg.Memory, scfg.Classifier = CodedRetrySticky, MemoryOwned, ClassifierPhased
		scfg.GatewayAdmission = GatewayAdmissionOwned
		scfg.Recovery = o.gatewayRecovery
	}
	server, err := NewServer(scfg)
	if err != nil {
		t.Fatal(err)
	}
	p.server = server
	p.gatewayCarrier = !o.releasedGateway && o.gatewayRecovery == RecoveryCarrier
	remote := packetConn.LocalAddr().String()
	relay, err := pathsim.New("127.0.0.1:0", remote, o.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = relay.Close() })
	p.relay = relay
	remote = relay.LocalAddr()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if o.transport == TransportAuto {
		// The TCP listener shares the relay's port number on TCP, so the
		// client's one remote address reaches both substrates.
		host, port, _ := net.SplitHostPort(remote)
		tcpListener, err := net.Listen("tcp", net.JoinHostPort(host, port))
		if err != nil {
			t.Skipf("TCP port %s busy: %v", port, err)
		}
		t.Cleanup(func() { _ = tcpListener.Close() })
		p.tcpRelay = tcpListener
		go func() { _ = server.ServeListener(ctx, tcpListener) }()
	}
	clientListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ccfg := ClientConfig{
		ListenAddr: clientListener.Addr().String(), RemoteAddr: remote, Credentials: roots, Transport: o.transport,
		EnableQUICPool: true, Metrics: p.clientMetrics, WireVersions: o.clientWires,
		Logger: slog.New(slog.NewJSONHandler(p.clientLogs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	if !o.legacyClient {
		ccfg.CCAPI, ccfg.Sched, ccfg.CongestionScope, ccfg.Topology = CCAPIV2, SchedClass, CongestionScopePath, TopologySingle
		ccfg.CodedRetry, ccfg.Memory, ccfg.Classifier = CodedRetrySticky, MemoryOwned, ClassifierPhased
		ccfg.Recovery, ccfg.CarrierRecovery = o.clientRecovery, o.recovery
	}
	client, err := NewClient(ccfg)
	if err != nil {
		t.Fatal(err)
	}
	p.client = client
	client.flowStartedForTest = o.onFlow
	go func() { _ = server.ServePacketConn(ctx, packetConn) }()
	go func() { _ = client.ServeListener(ctx, clientListener) }()
	p.socks = clientListener.Addr().String()
	return p
}

// echoStream keeps one SOCKS flow busy against an echo destination: it
// writes a numbered byte pattern and checks every byte that comes back.
type echoStream struct {
	conn     net.Conn
	echoed   atomic.Int64
	written  atomic.Int64
	stop     atomic.Bool
	pause    atomic.Bool
	err      atomic.Pointer[error]
	wg       sync.WaitGroup
	interval time.Duration
}

func startEchoStream(t *testing.T, p *carrierPair, chunk int, interval time.Duration) *echoStream {
	t.Helper()
	conn, err := trySocksDial(p.socks, p.destination, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Time{})
	e := &echoStream{conn: conn, interval: interval}
	t.Cleanup(func() { e.close() })
	e.wg.Add(2)
	go func() {
		defer e.wg.Done()
		buf := make([]byte, chunk)
		var offset int64
		for !e.stop.Load() {
			if e.pause.Load() {
				time.Sleep(5 * time.Millisecond)
				continue
			}
			for i := range buf {
				buf[i] = byte((offset + int64(i)) % 251)
			}
			n, err := conn.Write(buf)
			offset += int64(n)
			e.written.Store(offset)
			if err != nil {
				e.fail(err)
				return
			}
			time.Sleep(e.interval)
		}
	}()
	go func() {
		defer e.wg.Done()
		buf := make([]byte, 64<<10)
		var offset int64
		for {
			n, err := conn.Read(buf)
			for i := 0; i < n; i++ {
				if buf[i] != byte((offset+int64(i))%251) {
					e.fail(errors.New("echoed byte corrupted"))
					return
				}
			}
			offset += int64(n)
			e.echoed.Store(offset)
			if err != nil {
				if !e.stop.Load() {
					e.fail(err)
				}
				return
			}
		}
	}()
	return e
}

func (e *echoStream) fail(err error) {
	e.err.CompareAndSwap(nil, &err)
}

func (e *echoStream) failure() error {
	if p := e.err.Load(); p != nil {
		return *p
	}
	return nil
}

func (e *echoStream) close() {
	e.stop.Store(true)
	_ = e.conn.Close()
	e.wg.Wait()
}

// waitEchoed waits until at least want bytes have come back, or fails.
func (e *echoStream) waitEchoed(t *testing.T, want int64, budget time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	for e.echoed.Load() < want {
		if err := e.failure(); err != nil {
			t.Fatalf("flow failed after %d echoed bytes: %v", e.echoed.Load(), err)
		}
		if time.Since(start) > budget {
			t.Fatalf("only %d of %d bytes echoed within %v", e.echoed.Load(), want, budget)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return time.Since(start)
}

// countRecords is how many log records carry msg.
func countRecords(t *testing.T, logs *lockedBuffer, msg string) int {
	t.Helper()
	return len(logRecords(t, logs.String(), msg))
}

// assertNoLegacyRecovery checks that neither endpoint demoted a lane or ran
// a per-flow rescue round: under recovery=carrier neither exists.
func assertNoLegacyRecovery(t *testing.T, p *carrierPair) {
	t.Helper()
	endpoints := []*lockedBuffer{p.clientLogs}
	if p.gatewayCarrier {
		endpoints = append(endpoints, p.gatewayLogs)
	}
	for _, logs := range endpoints {
		if n := countRecords(t, logs, "flow stall suspected; lane demoted and rescue requested"); n != 0 {
			t.Fatalf("a lane was demoted %d times under recovery=carrier", n)
		}
	}
	if n := p.clientMetrics.Snapshot().LaneRescueAttempts; n != 0 {
		t.Fatalf("%d per-flow rescue dials under recovery=carrier", n)
	}
	if n := countRecords(t, p.clientLogs, "lane rescue joined"); n != 0 {
		t.Fatalf("%d per-flow rescue rounds under recovery=carrier", n)
	}
}

// A blackholed carrier, both directions, under a flow busy both ways: the
// carrier manager marks it SUSPECT on its probe timeouts, dials one standby
// from a fresh source (which the blackhole does not touch), verifies it and
// promotes it once k_p expiries pass with no progress. The flow is rebound by
// one JOIN on the new carrier, well inside its binding deadline, and every
// byte arrives intact. Exactly one new 4-tuple is used: the standby.
func TestCarrierRecoveryReplacesADeadCarrierWithinItsDeadline(t *testing.T) {
	p := newCarrierPair(t, carrierPairOptions{path: pathsim.Config{OneWayDelay: 20 * time.Millisecond, Seed: 3}})
	e := startEchoStream(t, p, 2048, 5*time.Millisecond)
	e.waitEchoed(t, 64<<10, 20*time.Second)
	if got := p.relay.Sources(); got != 1 {
		t.Fatalf("%d sources before the outage, want the one carrier", got)
	}
	p.relay.BlackholeSources()
	blackholed := time.Now()
	before := e.echoed.Load()
	took := e.waitEchoed(t, before+64<<10, 25*time.Second)
	m := p.client.carriers
	if got := m.promotions.Load(); got != 1 {
		t.Fatalf("%d promotions, want 1:\n%s", got, p.clientLogs.String())
	}
	_, deadline := m.cfg.bindingDeadlines(blackholed)
	if time.Now().After(deadline) {
		t.Fatalf("recovered after the binding deadline")
	}
	if got := p.relay.Sources(); got != 2 {
		t.Fatalf("%d sources, want the old carrier and one standby", got)
	}
	if got := m.newCarrierJoins.Load(); got != 1 {
		t.Fatalf("%d JOINs onto the new carrier, want 1", got)
	}
	assertNoLegacyRecovery(t, p)
	t.Logf("flow resumed %v after the blackhole; standby dials %d", took, m.standbyDials.Load())
}

// A flow's stream fails on a live carrier (the gateway's write of one DATA
// frame fails, so it resets the stream) while a sibling flow keeps running
// on the same carrier: one JOIN on the same carrier, no dial, no
// replacement, and the sibling is undisturbed (§10.4, test 9).
func TestCarrierRecoveryRejoinsAFailedStreamOnItsLiveCarrier(t *testing.T) {
	var armed atomic.Bool
	var fired atomic.Int32
	hook := func(frame protocol.Frame) error {
		if armed.Load() && frame.Header.Type == protocol.TypeData && fired.Add(1) == 1 {
			return errors.New("injected lane write failure")
		}
		return nil
	}
	p := newCarrierPair(t, carrierPairOptions{path: pathsim.Config{OneWayDelay: 10 * time.Millisecond, Seed: 4}, gatewayHook: hook})
	victim := startEchoStream(t, p, 1024, 5*time.Millisecond)
	victim.waitEchoed(t, 16<<10, 20*time.Second)
	// The sibling sends nothing while the fault is armed, so the failed
	// write is the victim's.
	sibling := startEchoStream(t, p, 512, 5*time.Millisecond)
	sibling.waitEchoed(t, 8<<10, 20*time.Second)
	sibling.pause.Store(true)
	time.Sleep(200 * time.Millisecond)
	armed.Store(true)
	for fired.Load() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	armed.Store(false)
	sibling.pause.Store(false)
	vBefore, sBefore := victim.echoed.Load(), sibling.echoed.Load()
	victim.waitEchoed(t, vBefore+32<<10, 20*time.Second)
	sibling.waitEchoed(t, sBefore+16<<10, 20*time.Second)
	m := p.client.carriers
	if got := m.sameCarrierJoins.Load(); got != 1 {
		t.Fatalf("%d JOINs on the same carrier, want 1:\n%s", got, p.clientLogs.String())
	}
	if got := m.promotions.Load() + m.standbyDials.Load() + m.replacementDials.Load(); got != 0 {
		t.Fatalf("carrier replaced or dialled (%d) for one stream's failure", got)
	}
	if got := p.relay.Sources(); got != 1 {
		t.Fatalf("%d sources: a dial was made for a stream failure", got)
	}
	assertNoLegacyRecovery(t, p)
}

// Missing credit on a live carrier is waited out (§10.4, test 8): the
// destination stops reading for longer than many stall thresholds, so the
// flow's sends stop being acknowledged, while the carrier keeps carrying
// QUIC's own acknowledgements. Nothing is dialled, replaced, rebound or
// demoted, and once the application reads again every byte arrives.
func TestCarrierRecoveryWaitsOutACreditStallOnALiveCarrier(t *testing.T) {
	var held atomic.Bool
	release := make(chan struct{})
	serve := func(l net.Listener) {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				buf := make([]byte, 32<<10)
				for {
					if held.Load() {
						<-release
					}
					n, err := conn.Read(buf)
					if n > 0 {
						if _, werr := conn.Write(buf[:n]); werr != nil {
							return
						}
					}
					if err != nil {
						return
					}
				}
			}()
		}
	}
	p := newCarrierPair(t, carrierPairOptions{path: pathsim.Config{OneWayDelay: 15 * time.Millisecond, Seed: 5}, serve: serve})
	e := startEchoStream(t, p, 4096, 2*time.Millisecond)
	e.waitEchoed(t, 64<<10, 20*time.Second)
	held.Store(true)
	time.Sleep(6 * time.Second)
	stalls := countRecords(t, p.clientLogs, "flow credit missing on a live stream; waiting")
	close(release)
	before := e.echoed.Load()
	e.waitEchoed(t, before+256<<10, 30*time.Second)
	m := p.client.carriers
	if got := m.promotions.Load() + m.standbyDials.Load() + m.replacementDials.Load() + m.sameCarrierJoins.Load() + m.newCarrierJoins.Load(); got != 0 {
		t.Fatalf("a credit stall moved or replaced something (%d):\n%s", got, p.clientLogs.String())
	}
	if got := p.relay.Sources(); got != 1 {
		t.Fatalf("%d sources: a credit stall dialled", got)
	}
	if stalls == 0 {
		t.Log("the flow never reached a stall threshold; the blocked application was absorbed by buffers")
	}
	if n := p.clientMetrics.Snapshot().LaneReplacements; n != 0 {
		t.Fatalf("%d stream replacements for a credit stall", n)
	}
	assertNoLegacyRecovery(t, p)
}

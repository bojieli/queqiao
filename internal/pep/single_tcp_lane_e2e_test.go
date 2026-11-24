package pep

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// These tests drive the real client and gateway -- SOCKS accept, flow open,
// Client.run's flow loop, the lane manager and the gateway's JOIN admission --
// over loopback TLS with a single TCP lane (TCPFallbackLanes = 1, the client
// default). Issue #53 measured 66 client flows that lost that one lane to the
// gateway's EOF while idle and then sat out the whole 45 s replacement grace
// with lane_replacement_attempts == 0: in v0.12.1 the TCP lane manager started
// only when TCPFallbackLanes > 1, so nothing ever dialled a replacement.
// TestRound2SingleTCPStartsRecovery covers manageLanes on its own; these
// cover the path a real flow takes to it.

// singleTCPRig is one client and one gateway with a single TCP lane, an echo
// destination, and a JSON log of the client's flow records.
type singleTCPRig struct {
	server     *Server
	client     *Client
	records    *lockedRecords
	clientAddr string
	echoAddr   string
	cancel     context.CancelFunc
	errorsCh   chan error
}

// newSingleTCPRig starts the pair. When proxy is non-nil the client reaches
// the gateway through it, so a test can make the gateway unreachable without
// stopping it.
func newSingleTCPRig(t *testing.T, proxy *switchableProxy) *singleTCPRig {
	t.Helper()
	destination, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = destination.Close() })
	go echoDestination(destination)

	certificate, roots := testCertificate(t)
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	serverListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{
		ListenAddr: serverListener.Addr().String(), Credentials: certificate,
		DestinationPolicy: DestinationPolicy{AllowPrivate: true}, EnableTCP: true, EnableQUIC: false,
		Logger: quiet,
	})
	if err != nil {
		t.Fatal(err)
	}
	remote := serverListener.Addr().String()
	if proxy != nil {
		remote = proxy.start(t, remote)
	}
	clientListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	records := &lockedRecords{}
	client, err := NewClient(ClientConfig{
		ListenAddr: clientListener.Addr().String(), RemoteAddr: remote, Credentials: roots,
		Transport: TransportTCP, TCPFallbackLanes: 1,
		DialTimeout: time.Second, HandshakeTimeout: time.Second,
		Logger: slog.New(slog.NewJSONHandler(records, &slog.HandlerOptions{Level: slog.LevelInfo})),
	})
	if err != nil {
		t.Fatal(err)
	}
	if client.cfg.TCPFallbackLanes != 1 {
		t.Fatalf("client runs %d TCP lanes, want the single-lane default", client.cfg.TCPFallbackLanes)
	}
	ctx, cancel := context.WithCancel(context.Background())
	rig := &singleTCPRig{
		server: server, client: client, records: records,
		clientAddr: clientListener.Addr().String(), echoAddr: destination.Addr().String(),
		cancel: cancel, errorsCh: make(chan error, 2),
	}
	go func() { rig.errorsCh <- server.ServeListener(ctx, serverListener) }()
	go func() { rig.errorsCh <- client.ServeListener(ctx, clientListener) }()
	t.Cleanup(func() {
		cancel()
		for range 2 {
			select {
			case err := <-rig.errorsCh:
				if err != nil {
					t.Errorf("service shutdown: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Error("service did not shut down")
				return
			}
		}
	})
	return rig
}

// echoRoundTrip writes one message through the flow and reads it back.
func echoRoundTrip(t *testing.T, conn net.Conn, message []byte, within time.Duration) {
	t.Helper()
	_ = conn.SetDeadline(time.Now().Add(within))
	defer func() { _ = conn.SetDeadline(time.Time{}) }()
	if _, err := conn.Write(message); err != nil {
		t.Fatalf("write %q: %v", message, err)
	}
	got := make([]byte, len(message))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read echo of %q: %v", message, err)
	}
	if !bytes.Equal(got, message) {
		t.Fatalf("echo = %q, want %q", got, message)
	}
}

// onlyServerSession waits for the gateway to hold exactly one session with
// exactly one healthy lane, and returns both.
func (r *singleTCPRig) onlyServerSession(t *testing.T) ([16]byte, *serverFlow, *mpLane) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r.server.sessionsMu.RLock()
		var (
			id    [16]byte
			found *serverFlow
			count int
		)
		for sessionID, sessionFlow := range r.server.sessions {
			id, found = sessionID, sessionFlow
			count++
		}
		r.server.sessionsMu.RUnlock()
		if count == 1 {
			if lanes := found.flow.healthyLanes(); len(lanes) == 1 && lanes[0].kind == TransportTCP {
				return id, found, lanes[0]
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("gateway never held one session on one TCP lane")
	return [16]byte{}, nil, nil
}

// waitIdle waits until the gateway's side of the flow has nothing sent and
// unacknowledged, so the lane that ends next is an idle flow's lane rather
// than one with data in flight.
func waitIdle(t *testing.T, session *serverFlow) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for session.flow.pendingOutbound() {
		if time.Now().After(deadline) {
			t.Fatal("gateway flow never became idle")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The other direction: the gateway has received the client's bytes (it
	// echoed them) and schedules their acknowledgement, which its ACK loop
	// holds for at most ackCoalesceDelay before writing it to the lane. Ten
	// of those is ample on loopback for the client to have it, and is the
	// short stretch of idleness the measured flows had before the cut.
	if session.flow.ackSequence.Load() == 0 {
		t.Fatal("gateway never acknowledged the client's bytes")
	}
	time.Sleep(10 * ackCoalesceDelay)
	if session.flow.pendingOutbound() {
		t.Fatal("gateway flow did not stay idle")
	}
}

// waitForGatewayLaneReplaced waits for the gateway session to hold a healthy
// TCP lane other than the one that was closed.
func waitForGatewayLaneReplaced(t *testing.T, session *serverFlow, closed *mpLane, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		for _, lane := range session.flow.healthyLanes() {
			if lane != closed && lane.kind == TransportTCP {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no replacement lane joined the gateway session within %v", within)
}

// flowRecord waits for the client's completion record for the flow, whichever
// way it ended.
func (r *singleTCPRig) flowRecord(t *testing.T, within time.Duration) map[string]any {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		for _, msg := range []string{"local flow complete", "local flow ended with error"} {
			if got := r.records.records(t, msg); len(got) > 0 {
				if len(got) != 1 {
					t.Fatalf("%d %q records, want one: %v", len(got), msg, got)
				}
				return got[0]
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("client wrote no flow record within %v", within)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func recordCount(t *testing.T, record map[string]any, field string) float64 {
	t.Helper()
	value, ok := record[field].(float64)
	if !ok {
		t.Fatalf("flow record has no numeric %s: %v", field, record)
	}
	return value
}

// The production failure, end to end: an idle single-lane TCP flow whose
// gateway closes the lane must open a zero-lane replacement, rejoin the same
// session, and keep carrying bytes in both directions.
func TestSingleTCPLaneEOFWhileIdleRecoversEndToEnd(t *testing.T) {
	rig := newSingleTCPRig(t, nil)
	conn := dialTestSOCKS(t, rig.clientAddr, rig.echoAddr)
	defer conn.Close()
	echoRoundTrip(t, conn, []byte("before the lane ends"), 5*time.Second)

	_, session, lane := rig.onlyServerSession(t)
	// Let the flow go idle: everything is delivered and acknowledged, and
	// nothing is waiting on the lane when it ends.
	waitIdle(t, session)
	if err := lane.fc.Close(); err != nil && !isClosedConnError(err) {
		t.Fatal(err)
	}

	// The 45 s grace is what the failing flows spent. A replacement that is
	// actually dialled lands in a few hundred milliseconds on loopback; ten
	// seconds is generous and still far short of the grace.
	waitForGatewayLaneReplaced(t, session, lane, 10*time.Second)
	echoRoundTrip(t, conn, []byte("after the replacement lane joined"), 10*time.Second)
	echoRoundTrip(t, conn, bytes.Repeat([]byte("still-flowing-"), 4096), 10*time.Second)

	if tcp, ok := conn.(*net.TCPConn); ok {
		if err := tcp.CloseWrite(); err != nil {
			t.Fatal(err)
		}
	}
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if rest, err := io.ReadAll(conn); err != nil || len(rest) != 0 {
		t.Fatalf("flow close: %d trailing bytes, err %v", len(rest), err)
	}
	record := rig.flowRecord(t, 10*time.Second)
	if record["msg"] != "local flow complete" {
		t.Fatalf("recovered flow did not complete cleanly: %v", record)
	}
	if attempts := recordCount(t, record, "lane_replacement_attempts"); attempts < 1 {
		t.Fatalf("flow record shows %v zero-lane replacement attempts, want at least one: %v", attempts, record)
	}
	// The initial lane counts as joined too.
	if joined := recordCount(t, record, "lanes_joined"); joined < 2 {
		t.Fatalf("flow record shows %v lanes joined, want the replacement: %v", joined, record)
	}
	if timeouts := recordCount(t, record, "lane_replacement_timeouts"); timeouts != 0 {
		t.Fatalf("recovered flow timed out a replacement wait: %v", record)
	}
}

// When the gateway no longer holds the session, the replacement JOIN is
// refused, and that refusal is final: a session identifier is never reissued.
// The flow must end on it at once instead of waiting out the 45 s grace for a
// lane nobody can provide.
func TestSingleTCPLaneEOFWithRefusedReplacementFailsPromptly(t *testing.T) {
	rig := newSingleTCPRig(t, nil)
	conn := dialTestSOCKS(t, rig.clientAddr, rig.echoAddr)
	defer conn.Close()
	echoRoundTrip(t, conn, []byte("before the gateway forgets"), 5*time.Second)

	id, session, lane := rig.onlyServerSession(t)
	waitIdle(t, session)
	// A gateway restart is the measured case: the session is gone, and the
	// lane carrying it ends.
	rig.server.unregisterSession(id, session)
	closedAt := time.Now()
	if err := lane.fc.Close(); err != nil && !isClosedConnError(err) {
		t.Fatal(err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	if n, err := io.Copy(io.Discard, conn); n != 0 {
		t.Fatalf("refused flow delivered %d more bytes (err %v)", n, err)
	}
	record := rig.flowRecord(t, 15*time.Second)
	elapsed := time.Since(closedAt)
	if elapsed > 10*time.Second {
		t.Fatalf("refused flow ended after %v; it should not wait out the %v grace", elapsed, laneReplacementWait)
	}
	if record["msg"] != "local flow ended with error" {
		t.Fatalf("refused flow did not fail: %v", record)
	}
	errText, _ := record["error"].(string)
	if !strings.Contains(errText, errResumeRefused.Error()) && !strings.Contains(errText, errReplacementAbandoned.Error()) {
		t.Fatalf("refused flow failed with %q, want a refusal or abandonment", errText)
	}
	if strings.Contains(errText, errLaneReplacementTimeout.Error()) {
		t.Fatalf("refused flow failed by timing out its grace: %q", errText)
	}
	// One replacement, refused, and no retry: the refusal is the answer.
	attempts := recordCount(t, record, "lane_replacement_attempts")
	failures := recordCount(t, record, "lane_replacement_failures")
	if attempts != 1 || failures != 1 {
		t.Fatalf("flow record shows attempts=%v failures=%v, want exactly one refused replacement: %v", attempts, failures, record)
	}
	if record["lane_replacement_last_failure"] != "peer_rejected" {
		t.Fatalf("last replacement failure = %v, want peer_rejected: %v", record["lane_replacement_last_failure"], record)
	}
	if timeouts := recordCount(t, record, "lane_replacement_timeouts"); timeouts != 0 {
		t.Fatalf("refused flow timed out a replacement wait: %v", record)
	}
}

// A gateway that is briefly unreachable when the lane ends is the case the
// grace exists for. The client has to keep dialling through the outage --
// the v0.12.1 client dialled nothing -- and the flow has to resume once a
// dial gets through.
func TestSingleTCPLaneEOFRetriesThroughUnreachableGateway(t *testing.T) {
	proxy := &switchableProxy{}
	rig := newSingleTCPRig(t, proxy)
	conn := dialTestSOCKS(t, rig.clientAddr, rig.echoAddr)
	defer conn.Close()
	echoRoundTrip(t, conn, []byte("before the path drops"), 5*time.Second)

	_, session, lane := rig.onlyServerSession(t)
	waitIdle(t, session)
	// Cut every connection through the proxy, the flow's lane included, and
	// refuse new ones: the client sees its lane end and every replacement
	// dial fail.
	proxy.setDown(true)

	deadline := time.Now().Add(10 * time.Second)
	for proxy.refusedCount() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("client made %d replacement dials while the gateway was unreachable, want at least two", proxy.refusedCount())
		}
		time.Sleep(10 * time.Millisecond)
	}
	refused := proxy.setDown(false)

	waitForGatewayLaneReplaced(t, session, lane, 20*time.Second)
	echoRoundTrip(t, conn, []byte("after the path came back"), 10*time.Second)

	_ = conn.Close()
	record := rig.flowRecord(t, 10*time.Second)
	if attempts := recordCount(t, record, "lane_replacement_attempts"); attempts < 2 {
		t.Fatalf("flow record shows %v zero-lane replacement attempts, want the failed ones and the one that landed: %v", attempts, record)
	}
	// Every dial the proxy refused was a replacement this flow counted.
	failures := recordCount(t, record, "lane_replacement_failures")
	if failures < float64(refused) {
		t.Fatalf("flow record shows %v replacement failures for %d refused dials: %v", failures, refused, record)
	}
	// The dial that got through is an attempt that did not fail.
	if attempts := recordCount(t, record, "lane_replacement_attempts"); attempts < failures+1 {
		t.Fatalf("flow record shows %v attempts for %v failures and one success: %v", attempts, failures, record)
	}
	if timeouts := recordCount(t, record, "lane_replacement_timeouts"); timeouts != 0 {
		t.Fatalf("recovered flow timed out a replacement wait: %v", record)
	}
}

// A gateway that stays unreachable cannot be told apart from one that is
// about to come back, so the client keeps dialling until the grace runs out.
// What matters for issue #53 is that it dials at all: the measured failures
// spent the same grace with zero attempts. This waits the real grace.
func TestSingleTCPLaneEOFWithUnreachableGatewayDialsUntilTheGraceEnds(t *testing.T) {
	if testing.Short() {
		t.Skipf("waits out the %v replacement grace", laneReplacementWait)
	}
	proxy := &switchableProxy{}
	rig := newSingleTCPRig(t, proxy)
	conn := dialTestSOCKS(t, rig.clientAddr, rig.echoAddr)
	defer conn.Close()
	echoRoundTrip(t, conn, []byte("before the path drops for good"), 5*time.Second)
	_, session, _ := rig.onlyServerSession(t)
	waitIdle(t, session)
	proxy.setDown(true)
	downAt := time.Now()

	_ = conn.SetReadDeadline(time.Now().Add(laneReplacementWait + 30*time.Second))
	_, _ = io.Copy(io.Discard, conn)
	record := rig.flowRecord(t, 30*time.Second)
	elapsed := time.Since(downAt)
	// The gateway may come back at any point in the grace, so the flow must
	// not be given up before it ends.
	if elapsed < laneReplacementWait-time.Second || elapsed > laneReplacementWait+15*time.Second {
		t.Fatalf("unreachable flow ended after %v, want about the %v grace", elapsed, laneReplacementWait)
	}
	if record["msg"] != "local flow ended with error" {
		t.Fatalf("flow with an unreachable gateway did not fail: %v", record)
	}
	if errText, _ := record["error"].(string); !strings.Contains(errText, errLaneReplacementTimeout.Error()) {
		t.Fatalf("unreachable flow failed with %q, want the grace to expire", errText)
	}
	attempts := recordCount(t, record, "lane_replacement_attempts")
	failures := recordCount(t, record, "lane_replacement_failures")
	if attempts < 2 || failures < 2 {
		t.Fatalf("flow record shows attempts=%v failures=%v, want repeated dials through the grace: %v", attempts, failures, record)
	}
	// The record and the proxy saw the same dials: each one refused was a
	// counted attempt, and each counted failure was one refused, except
	// that the last attempt may still have been in flight when the record
	// was written.
	if refused := float64(proxy.refusedCount()); attempts < refused || failures > refused || failures < refused-1 {
		t.Fatalf("proxy refused %v dials, flow counted %v attempts and %v failures", refused, attempts, failures)
	}
	// Still dialling well into the grace, not two early tries and then a
	// silent wait for the deadline. The backoff caps at 15 s, so a client
	// that keeps trying dials again after the 20 s mark.
	if last := proxy.lastRefusal().Sub(downAt); last < 20*time.Second {
		t.Fatalf("last replacement dial came %v after the path dropped, want retries past 20s of the %v grace", last, laneReplacementWait)
	}
}

func isClosedConnError(err error) bool {
	return errors.Is(err, net.ErrClosed)
}

// switchableProxy forwards TCP to the gateway until it is set down; then it
// closes every forwarded connection and accepts-and-closes each new one, which
// is what a client dialling an unreachable gateway sees fail at the handshake.
type switchableProxy struct {
	mu      sync.Mutex
	down    bool
	refused int
	lastAt  time.Time
	conns   map[net.Conn]struct{}
}

func (p *switchableProxy) start(t *testing.T, target string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p.conns = map[net.Conn]struct{}{}
	t.Cleanup(func() {
		_ = listener.Close()
		p.setDown(true)
	})
	go func() {
		for {
			inbound, err := listener.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			if p.down {
				p.refused++
				p.lastAt = time.Now()
				p.mu.Unlock()
				_ = inbound.Close()
				continue
			}
			p.mu.Unlock()
			outbound, err := net.Dial("tcp", target)
			if err != nil {
				_ = inbound.Close()
				continue
			}
			p.mu.Lock()
			if p.down {
				p.mu.Unlock()
				_ = inbound.Close()
				_ = outbound.Close()
				continue
			}
			p.conns[inbound] = struct{}{}
			p.conns[outbound] = struct{}{}
			p.mu.Unlock()
			pipe := func(dst, src net.Conn) {
				_, _ = io.Copy(dst, src)
				_ = dst.Close()
				_ = src.Close()
				p.mu.Lock()
				delete(p.conns, dst)
				delete(p.conns, src)
				p.mu.Unlock()
			}
			go pipe(outbound, inbound)
			go pipe(inbound, outbound)
		}
	}()
	return listener.Addr().String()
}

// setDown switches the proxy and returns how many connections it had refused
// up to that point.
func (p *switchableProxy) setDown(down bool) int {
	p.mu.Lock()
	p.down = down
	refused := p.refused
	var cut []net.Conn
	if down {
		for conn := range p.conns {
			cut = append(cut, conn)
		}
	}
	p.mu.Unlock()
	for _, conn := range cut {
		_ = conn.Close()
	}
	return refused
}

func (p *switchableProxy) lastRefusal() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastAt
}

func (p *switchableProxy) refusedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.refused
}

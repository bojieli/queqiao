package pep

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/metrics"
	"github.com/bojieli/queqiao/internal/protocol"
	"github.com/bojieli/queqiao/internal/session"
)

// Issue #159: a gateway flow that ended with an error used to close its lanes
// silently. The client saw only a stream end, took it for a dead path, and
// asked to JOIN a session the gateway had already dropped, which it refused as
// "unknown session". A failing gateway flow must say RESET first.
func TestFailingGatewayFlowResetsEveryOpenLane(t *testing.T) {
	inner, application := net.Pipe()
	defer application.Close()
	flow := newMultipathFlow(context.Background(), inner, [16]byte{1}, 7, 1024,
		protocol.FlagAckDown, protocol.FlagAckUp, nil, metrics.New())
	flow.resetOnFailure = true
	flow.idleTimeout = 50 * time.Millisecond
	flow.maxLifetime = time.Minute
	flow.stallWatchdogDisabled = true

	peers := make([]net.Conn, 2)
	for i := range peers {
		outer, peer := net.Pipe()
		defer peer.Close()
		peers[i] = peer
		if err := flow.addLane(&mpLane{id: uint64(i), kind: TransportQUIC, fc: newFrameConn(outer)}); err != nil {
			t.Fatal(err)
		}
	}
	got := make(chan protocol.Frame, len(peers))
	for _, peer := range peers {
		go func(peer net.Conn) {
			for {
				frame, err := protocol.ReadFrame(peer)
				if err != nil {
					return
				}
				if frame.Header.Type == protocol.TypeReset {
					got <- frame
					return
				}
			}
		}(peer)
	}

	if _, err := flow.run(context.Background()); !errors.Is(err, errFlowIdleTimeout) {
		t.Fatalf("flow error = %v, want idle timeout", err)
	}
	for range peers {
		select {
		case frame := <-got:
			if frame.Header.SessionID != ([16]byte{1}) || frame.Header.FlowID != 7 {
				t.Fatalf("RESET names session %x flow %d", frame.Header.SessionID, frame.Header.FlowID)
			}
			if code := resetCode(frame.Payload); code != session.ResetTransport {
				t.Fatalf("RESET code = %d, want transport", code)
			}
			if reason := string(frame.Payload[1:]); reason != "flow ended: idle timeout" {
				t.Fatalf("RESET reason = %q", reason)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("a lane closed without a RESET")
		}
	}
}

// The client end keeps closing silently: only the gateway knows that a session
// is gone for good, and the client may still re-OPEN or fall back.
func TestFailingClientFlowDoesNotSendReset(t *testing.T) {
	inner, application := net.Pipe()
	defer application.Close()
	flow := newMultipathFlow(context.Background(), inner, [16]byte{1}, 7, 1024,
		protocol.FlagAckUp, protocol.FlagAckDown, nil, metrics.New())
	flow.idleTimeout = 50 * time.Millisecond
	flow.maxLifetime = time.Minute
	flow.stallWatchdogDisabled = true
	outer, peer := net.Pipe()
	defer peer.Close()
	if err := flow.addLane(&mpLane{id: 0, kind: TransportQUIC, fc: newFrameConn(outer)}); err != nil {
		t.Fatal(err)
	}
	sawReset := make(chan bool, 1)
	go func() {
		for {
			frame, err := protocol.ReadFrame(peer)
			if err != nil {
				sawReset <- false
				return
			}
			if frame.Header.Type == protocol.TypeReset {
				sawReset <- true
				return
			}
		}
	}()
	if _, err := flow.run(context.Background()); !errors.Is(err, errFlowIdleTimeout) {
		t.Fatalf("flow error = %v, want idle timeout", err)
	}
	if <-sawReset {
		t.Fatal("client flow sent RESET")
	}
}

// End to end over one lane: the client learns the reason from the RESET and
// does not count the stream end that follows it as a lane failure, so it never
// reaches for a replacement JOIN.
func TestClientEndsFlowOnGatewayResetWithoutLaneFailure(t *testing.T) {
	gatewayInner, gatewayApp := net.Pipe()
	defer gatewayApp.Close()
	clientInner, clientApp := net.Pipe()
	defer clientApp.Close()
	gatewayMetrics, clientMetrics := metrics.New(), metrics.New()
	gateway := newMultipathFlow(context.Background(), gatewayInner, [16]byte{1}, 7, 1024,
		protocol.FlagAckDown, protocol.FlagAckUp, nil, gatewayMetrics)
	gateway.resetOnFailure = true
	gateway.idleTimeout = 50 * time.Millisecond
	gateway.maxLifetime = time.Minute
	gateway.stallWatchdogDisabled = true
	client := newMultipathFlow(context.Background(), clientInner, [16]byte{1}, 7, 1024,
		protocol.FlagAckUp, protocol.FlagAckDown, nil, clientMetrics)
	client.idleTimeout = time.Minute
	client.maxLifetime = time.Minute
	client.stallWatchdogDisabled = true

	gatewayLane, clientLane := net.Pipe()
	if err := gateway.addLane(&mpLane{id: 0, kind: TransportQUIC, fc: newFrameConn(gatewayLane)}); err != nil {
		t.Fatal(err)
	}
	if err := client.addLane(&mpLane{id: 0, kind: TransportQUIC, fc: newFrameConn(clientLane)}); err != nil {
		t.Fatal(err)
	}
	clientErr := make(chan error, 1)
	go func() {
		_, err := client.run(context.Background())
		clientErr <- err
	}()
	if _, err := gateway.run(context.Background()); !errors.Is(err, errFlowIdleTimeout) {
		t.Fatalf("gateway error = %v, want idle timeout", err)
	}
	select {
	case err := <-clientErr:
		if err == nil || !strings.Contains(err.Error(), "peer reset flow: flow ended: idle timeout") {
			t.Fatalf("client error = %v, want the gateway's RESET", err)
		}
		if errors.Is(err, errResumeRefused) {
			t.Fatalf("client tried to resume a session the gateway reset: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("client flow outlived the gateway's RESET")
	}
	if got := clientMetrics.Snapshot().LaneFailures; got != 0 {
		t.Fatalf("client counted %d lane failures after a RESET", got)
	}
}

func TestTerminalResetReasonKeepsAddressesOffTheWire(t *testing.T) {
	cases := []struct {
		err  error
		code session.ResetCode
		want string
	}{
		{context.Canceled, session.ResetTransport, "flow ended: gateway shutting down"},
		{errFlowNoProgress, session.ResetTransport, "flow ended: no delivery progress"},
		{errors.Join(errors.New("last lane failed (lane 0: read udp 192.0.2.1:1->192.0.2.2:2: timeout)"), errLaneReplacementTimeout), session.ResetTransport, "flow ended: lane replacement timeout"},
		{errReplacementAbandoned, session.ResetTransport, "flow ended: lane replacement timeout"},
		{errFlowLifetime, session.ResetTransport, "flow ended: lifetime exceeded"},
		{errors.New("read tcp 192.0.2.1:1->192.0.2.2:2: i/o timeout"), session.ResetTransport, "flow ended with error"},
	}
	for _, c := range cases {
		code, reason := terminalResetReason(c.err)
		if code != c.code || reason != c.want {
			t.Errorf("terminalResetReason(%v) = %d %q, want %d %q", c.err, code, reason, c.code, c.want)
		}
		if strings.Contains(reason, "192.0.2") {
			t.Errorf("reason %q carries an address", reason)
		}
	}
}

// A flow whose two FINs have both been seen is complete as far as the gateway
// is concerned, and keeps a tombstone for the final ACK. It must not be turned
// into a failure by a RESET, even when a timeout wins the race to close it.
func TestCompletedFlowIsNotReset(t *testing.T) {
	inner, application := net.Pipe()
	defer application.Close()
	flow := newMultipathFlow(context.Background(), inner, [16]byte{1}, 7, 1024,
		protocol.FlagAckDown, protocol.FlagAckUp, nil, metrics.New())
	flow.resetOnFailure = true
	outer, peer := net.Pipe()
	defer peer.Close()
	if err := flow.addLane(&mpLane{id: 0, kind: TransportQUIC, fc: newFrameConn(outer)}); err != nil {
		t.Fatal(err)
	}
	flow.finSent.Store(true)
	flow.remoteFinSeen.Store(true)
	sawReset := make(chan bool, 1)
	go func() {
		frame, err := protocol.ReadFrame(peer)
		sawReset <- err == nil && frame.Header.Type == protocol.TypeReset
	}()
	flow.failClose(errFlowLifetime)
	if <-sawReset {
		t.Fatal("a flow with both FINs was reset")
	}
}

// Once a failing flow has chosen the lanes it will RESET, no replacement may
// join: it would be closed without one.
func TestFailingFlowAdmitsNoLaneWhileResetting(t *testing.T) {
	inner, application := net.Pipe()
	defer application.Close()
	flow := newMultipathFlow(context.Background(), inner, [16]byte{1}, 7, 1024,
		protocol.FlagAckDown, protocol.FlagAckUp, nil, metrics.New())
	flow.resetOnFailure = true
	outer, peer := net.Pipe()
	defer peer.Close()
	if err := flow.addLane(&mpLane{id: 0, kind: TransportQUIC, fc: newFrameConn(outer)}); err != nil {
		t.Fatal(err)
	}
	// The peer never reads, so the RESET write blocks for the whole bound.
	done := make(chan struct{})
	go func() {
		flow.resetLanes(errFlowLifetime)
		close(done)
	}()
	deadline := time.Now().Add(time.Second)
	for !flow.terminating.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	late, latePeer := net.Pipe()
	defer late.Close()
	defer latePeer.Close()
	if err := flow.addLane(&mpLane{id: 1, kind: TransportQUIC, fc: newFrameConn(late)}); !errors.Is(err, errLaneFlowClosed) {
		t.Fatalf("lane admitted during the RESET: %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RESET to a peer that never reads was not bounded")
	}
}

// Whatever teardown path returns first, a flow the peer reset ends with the
// peer's reason.
func TestPeerResetReasonWinsOverTheTeardownError(t *testing.T) {
	inner, application := net.Pipe()
	defer application.Close()
	flow := newMultipathFlow(context.Background(), inner, [16]byte{1}, 7, 1024,
		protocol.FlagAckUp, protocol.FlagAckDown, nil, metrics.New())
	flow.idleTimeout = 20 * time.Millisecond
	flow.maxLifetime = time.Minute
	flow.stallWatchdogDisabled = true
	flow.notePeerReset(protocol.Frame{Payload: session.ResetPayload(session.ResetTransport, "flow ended: idle timeout")})
	_, err := flow.run(context.Background())
	if !errors.Is(err, errPeerReset) || !strings.HasPrefix(err.Error(), "peer reset flow: flow ended: idle timeout") {
		t.Fatalf("flow error = %v, want the peer's RESET first", err)
	}
}

// A staged JOIN may not have been answered yet, so its RESET carries the
// permanent protocol code: never the transport code a client would read as
// "switch to TCP", and never nothing.
func TestStagedLaneGetsAPermanentReset(t *testing.T) {
	inner, application := net.Pipe()
	defer application.Close()
	flow := newMultipathFlow(context.Background(), inner, [16]byte{1}, 7, 1024,
		protocol.FlagAckDown, protocol.FlagAckUp, nil, metrics.New())
	flow.resetOnFailure = true
	outer, peer := net.Pipe()
	defer peer.Close()
	if err := flow.addLane(&mpLane{id: 1, kind: TransportQUIC, fc: newFrameConn(outer), staged: true}); err != nil {
		t.Fatal(err)
	}
	got := make(chan protocol.Frame, 1)
	go func() {
		frame, err := protocol.ReadFrame(peer)
		if err == nil {
			got <- frame
		}
		close(got)
	}()
	flow.failClose(errFlowLifetime)
	frame, ok := <-got
	if !ok || frame.Header.Type != protocol.TypeReset || resetCode(frame.Payload) != session.ResetProtocol {
		t.Fatalf("staged lane got %+v %q, want a protocol RESET", frame.Header, frame.Payload)
	}
}

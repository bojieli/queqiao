package pep

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/identity"
	"github.com/bojieli/queqiao/internal/metrics"
	"github.com/bojieli/queqiao/internal/protocol"
	"github.com/bojieli/queqiao/internal/session"
)

func pendingTestServer() (*Server, *metrics.Registry) {
	registry := metrics.New()
	return &Server{
		cfg:      ServerConfig{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), MaxSessions: 16},
		sessions: map[[16]byte]*serverFlow{},
		metrics:  registry,
	}, registry
}

func joinPendingSession(server *Server, principal identity.Principal, sessionID [16]byte, flowID uint64) <-chan protocol.Frame {
	local, remote := net.Pipe()
	request := protocol.Frame{Header: protocol.Header{
		Version: protocol.Version, Type: protocol.TypeJoin, SessionID: sessionID,
		FlowID: flowID, Class: protocol.ClassBulk,
	}}
	go server.handleLaneJoinOpen(context.Background(), local, newFrameConn(local), principal, sessionID, 1, request)
	response := make(chan protocol.Frame, 1)
	go func() {
		defer remote.Close()
		frame, err := newFrameConn(remote).Read()
		if err == nil {
			response <- frame
		}
		close(response)
	}()
	return response
}

// Issue #159, route A: a JOIN that overtakes its flow's OPEN while the gateway
// is still dialling the destination used to be told "unknown session", which
// the client takes as permanent. It now waits for the OPEN to be decided.
func TestJoinDuringDestinationDialWaitsForTheSession(t *testing.T) {
	owner := identity.Principal{ProviderID: "provider", AccountID: "account", DeviceID: "owner"}
	server, registry := pendingTestServer()
	flow := newIsolationTestFlow(t, false)
	flow.lanes[0] = isolationLane(t, 0)
	pending, ok := server.beginPendingSession(flow.sessionID, flow.flowID, owner)
	if !ok {
		t.Fatal("pending session refused")
	}
	response := joinPendingSession(server, owner, flow.sessionID, flow.flowID)
	select {
	case frame := <-response:
		t.Fatalf("JOIN answered with type %d while its OPEN was still dialling", frame.Header.Type)
	case <-time.After(100 * time.Millisecond):
	}
	sessionFlow := newServerFlow(flow, owner, TransportQUIC, 4)
	if !server.registerSession(flow.sessionID, sessionFlow) {
		t.Fatal("register failed")
	}
	server.endPendingSession(flow.sessionID, pending)
	select {
	case frame, ok := <-response:
		if !ok || frame.Header.Type != protocol.TypeOpenOK {
			t.Fatalf("JOIN after admission answered %+v, want OPEN_OK", frame.Header)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("JOIN was not answered after the OPEN was admitted")
	}
	if got := registry.Snapshot().LaneJoinRefusals; got != ([metrics.LaneJoinReasons]uint64{}) {
		t.Fatalf("refusals counted: %v", got)
	}
}

func TestJoinOnSlowDialGetsTransientRefusal(t *testing.T) {
	owner := identity.Principal{ProviderID: "provider", AccountID: "account", DeviceID: "owner"}
	saved := pendingJoinWait
	pendingJoinWait = 50 * time.Millisecond
	t.Cleanup(func() { pendingJoinWait = saved })
	server, registry := pendingTestServer()
	sessionID := [16]byte{7}
	pending, _ := server.beginPendingSession(sessionID, 3, owner)
	defer server.endPendingSession(sessionID, pending)
	frame, ok := <-joinPendingSession(server, owner, sessionID, 3)
	if !ok || frame.Header.Type != protocol.TypeReset || resetCode(frame.Payload) != session.ResetFlowLimit {
		t.Fatalf("slow dial answered %+v %q, want the transient capacity RESET", frame.Header, frame.Payload)
	}
	if got := registry.Snapshot().LaneJoinRefusals[metrics.LaneJoinSessionPending]; got != 1 {
		t.Fatalf("session_pending = %d, want 1", got)
	}
}

func TestJoinOnFailedOpenGetsDestinationRefusal(t *testing.T) {
	owner := identity.Principal{ProviderID: "provider", AccountID: "account", DeviceID: "owner"}
	server, registry := pendingTestServer()
	sessionID := [16]byte{8}
	pending, _ := server.beginPendingSession(sessionID, 3, owner)
	response := joinPendingSession(server, owner, sessionID, 3)
	for deadline := time.Now().Add(2 * time.Second); pending.waiters.Load() == 0; {
		if time.Now().After(deadline) {
			t.Fatal("JOIN never waited on the pending OPEN")
		}
		time.Sleep(time.Millisecond)
	}
	server.endPendingSession(sessionID, pending)
	frame, ok := <-response
	if !ok || frame.Header.Type != protocol.TypeReset || resetCode(frame.Payload) != session.ResetDestination {
		t.Fatalf("failed open answered %+v %q, want the destination RESET", frame.Header, frame.Payload)
	}
	if got := registry.Snapshot().LaneJoinRefusals[metrics.LaneJoinOpenFailed]; got != 1 {
		t.Fatalf("open_failed = %d, want 1", got)
	}
}

// Only the device that sent the OPEN, naming its flow, waits; anyone else gets
// the answer the session table gives, as before.
func TestPendingSessionDoesNotHoldOtherJoins(t *testing.T) {
	owner := identity.Principal{ProviderID: "provider", AccountID: "account", DeviceID: "owner"}
	other := owner
	other.DeviceID = "other"
	server, registry := pendingTestServer()
	sessionID := [16]byte{9}
	pending, _ := server.beginPendingSession(sessionID, 3, owner)
	defer server.endPendingSession(sessionID, pending)
	for _, join := range []struct {
		who  identity.Principal
		flow uint64
	}{{other, 3}, {owner, 4}} {
		select {
		case frame := <-joinPendingSession(server, join.who, sessionID, join.flow):
			if frame.Header.Type != protocol.TypeReset {
				t.Fatalf("answer %d, want RESET", frame.Header.Type)
			}
		case <-time.After(time.Second):
			t.Fatal("an unrelated JOIN waited on a pending session")
		}
	}
	if got := registry.Snapshot().LaneJoinRefusals[metrics.LaneJoinUnknownSession]; got != 2 {
		t.Fatalf("unknown_session = %d, want 2", got)
	}
	if _, ok := server.beginPendingSession(sessionID, 3, owner); ok {
		t.Fatal("a second OPEN for a pending session was accepted")
	}
}

func TestJoinRefusalBeforeOpenOKIsItsOwnKind(t *testing.T) {
	flow := newIsolationTestFlow(t, false)
	refused := errors.New("lane join rejected: unknown session")
	refused = errors.Join(errLaneJoinRejected, refused)

	flow.requireOpenConfirmation()
	err := classifyUnadmittedRefusal(flow, refused)
	if !errors.Is(err, errLaneJoinOpenUnadmitted) || !errors.Is(err, errLaneJoinRejected) {
		t.Fatalf("unconfirmed refusal = %v", err)
	}
	if kind := replacementFailureKind(err); kind != "open_unadmitted" {
		t.Fatalf("kind = %q", kind)
	}
	for _, keep := range []error{errLaneJoinCapacity, errLaneJoinTCPMode, errLaneJoinOpenFailed, context.DeadlineExceeded} {
		if got := classifyUnadmittedRefusal(flow, keep); got != keep {
			t.Fatalf("%v reclassified as %v", keep, got)
		}
	}
	if kind := replacementFailureKind(errLaneJoinOpenFailed); kind != "open_failed" {
		t.Fatalf("open failed kind = %q", kind)
	}

	flow.confirmOpen()
	if got := classifyUnadmittedRefusal(flow, refused); errors.Is(got, errLaneJoinOpenUnadmitted) {
		t.Fatal("a refusal after OPEN_OK was blamed on the open")
	}
	if kind := replacementFailureKind(refused); kind != "peer_rejected" {
		t.Fatalf("confirmed refusal kind = %q", kind)
	}

	flow.resumeRefused.Store(true)
	if err := flow.resumeRefusal(); !errors.Is(err, errResumeRefused) {
		t.Fatalf("resume refusal = %v", err)
	}
	flow.openUnadmitted.Store(true)
	if err := flow.resumeRefusal(); !errors.Is(err, errOpenNotAdmitted) || errors.Is(err, errResumeRefused) {
		t.Fatalf("unadmitted refusal = %v", err)
	}
}

func TestCompleteLaneJoinMapsDestinationResetToOpenFailed(t *testing.T) {
	client := &Client{cfg: ClientConfig{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), HandshakeTimeout: time.Second}}
	local, remote := net.Pipe()
	defer remote.Close()
	sessionID := [16]byte{5}
	go func() {
		fc := newFrameConn(remote)
		if _, err := fc.Read(); err != nil {
			return
		}
		_ = fc.Write(protocol.Frame{Header: protocol.Header{
			Version: protocol.Version, Type: protocol.TypeReset, SessionID: sessionID, FlowID: 3, Class: protocol.ClassBulk,
		}, Payload: session.ResetPayload(session.ResetDestination, "flow open failed")})
	}()
	lane := &authenticatedLane{fc: newFrameConn(local), outer: local, sessionID: sessionID, laneID: 1, kind: TransportQUIC}
	_, err := client.completeLaneJoin(context.Background(), lane, 3, 0)
	if !errors.Is(err, errLaneJoinOpenFailed) || !errors.Is(err, errLaneJoinRejected) {
		t.Fatalf("destination refusal = %v", err)
	}
}

// Issue #159, route C: a client host asleep for longer than the gateway holds
// a silent flow comes back to sessions the gateway has, correctly, ended. The
// refusal that follows is reported as that, not as a session the gateway lost.
func TestRefusalAfterLongSuspendIsReportedAsExpiry(t *testing.T) {
	flow := newIsolationTestFlow(t, false)
	flow.resumeRefused.Store(true)
	if got := flow.suspendedFor(); got >= time.Second {
		t.Fatalf("a flow that never slept reports %v suspended", got)
	}
	if err := flow.resumeRefusal(); !errors.Is(err, errResumeRefused) {
		t.Fatalf("refusal without a suspend = %v", err)
	}
	heardAgo := func(d time.Duration) {
		flow.peerClockOffset.Store(flow.clockOffset(time.Now()) - int64(d))
	}
	heardAgo(30 * time.Second)
	if err := flow.resumeRefusal(); !errors.Is(err, errResumeRefused) {
		t.Fatalf("a suspend the gateway outlasts = %v", err)
	}
	heardAgo(390 * time.Second)
	if got := flow.suspendedFor(); got < 389*time.Second || got > 391*time.Second {
		t.Fatalf("suspendedFor = %v, want about 390s", got)
	}
	if err := flow.resumeRefusal(); !errors.Is(err, errSessionExpiredWhileSuspended) {
		t.Fatalf("refusal after a 390 s suspend = %v", err)
	}
	fields := flow.replacementLogFields()
	found := false
	for i := 0; i+1 < len(fields); i += 2 {
		if fields[i] == "host_suspended" {
			found = true
		}
	}
	if !found {
		t.Fatalf("completion record does not carry the suspension: %v", fields)
	}
	// Hearing from the peer after waking is proof the sleep cost nothing.
	flow.notePeerHeard()
	if err := flow.resumeRefusal(); !errors.Is(err, errResumeRefused) {
		t.Fatalf("refusal after the peer was heard again = %v", err)
	}
	heardAgo(390 * time.Second)
	// An unadmitted open is still that, whatever the clock did.
	flow.openUnadmitted.Store(true)
	if err := flow.resumeRefusal(); !errors.Is(err, errOpenNotAdmitted) {
		t.Fatalf("unadmitted refusal after suspend = %v", err)
	}
}

func TestEndedSessionsExpireAndStayBounded(t *testing.T) {
	var ended endedSessions
	start := time.Now()
	id := [16]byte{3}
	ended.add(id, 1, identity.Principal{}, start)
	if !ended.contains(id, start.Add(endedSessionMemory-time.Second)) || ended.contains(id, start.Add(endedSessionMemory)) {
		t.Fatal("ended session not remembered for exactly the memory")
	}
	for i := 0; i <= endedSessionCapacity; i++ {
		var other [16]byte
		other[0], other[1], other[2] = byte(i), byte(i>>8), byte(i>>16)
		other[15] = 1
		ended.add(other, 1, identity.Principal{}, start)
	}
	if got := len(ended.entries); got > endedSessionCapacity {
		t.Fatalf("memory holds %d sessions, bound is %d", got, endedSessionCapacity)
	}
}

// A session that ended is answered as ended -- never as one the gateway never
// held -- and an OPEN for it is refused rather than reaching the destination
// a second time.
func TestEndedSessionIsNotNeverHeld(t *testing.T) {
	owner := identity.Principal{ProviderID: "provider", AccountID: "account", DeviceID: "owner"}
	server, registry := pendingTestServer()
	flow := newIsolationTestFlow(t, false)
	sessionFlow := newServerFlow(flow, owner, TransportQUIC, 4)
	if !server.registerSession(flow.sessionID, sessionFlow) {
		t.Fatal("register failed")
	}
	server.unregisterSession(flow.sessionID, sessionFlow)
	frame := <-joinPendingSession(server, owner, flow.sessionID, flow.flowID)
	if frame.Header.Type != protocol.TypeReset || !strings.Contains(string(frame.Payload), sessionEndedAnswer) {
		t.Fatalf("ended session answered %q", frame.Payload)
	}
	if got := registry.Snapshot().LaneJoinRefusals[metrics.LaneJoinSessionEnded]; got != 1 {
		t.Fatalf("session_ended = %d, want 1", got)
	}
	if _, ok := server.beginPendingSession(flow.sessionID, flow.flowID, owner); ok {
		t.Fatal("an OPEN for an ended session was admitted")
	}
	// Another device, or another flow, learns nothing about the session.
	other := owner
	other.DeviceID = "other"
	for _, join := range []struct {
		who  identity.Principal
		flow uint64
	}{{other, flow.flowID}, {owner, flow.flowID + 1}} {
		answer := <-joinPendingSession(server, join.who, flow.sessionID, join.flow)
		if strings.Contains(string(answer.Payload), sessionEndedAnswer) {
			t.Fatalf("a JOIN by %s for flow %d was told the session ended", join.who.DeviceID, join.flow)
		}
	}
	never := <-joinPendingSession(server, owner, [16]byte{0xee}, 1)
	if strings.Contains(string(never.Payload), sessionEndedAnswer) {
		t.Fatalf("never-held session answered %q", never.Payload)
	}
	// Before OPEN_OK, "session ended" is still the gateway saying it held the
	// session, and must not be counted as an open it never admitted.
	unconfirmed := newIsolationTestFlow(t, false)
	unconfirmed.requireOpenConfirmation()
	ended := fmt.Errorf("%w: unknown session: %s", errLaneJoinRejected, sessionEndedAnswer)
	if errors.Is(classifyUnadmittedRefusal(unconfirmed, ended), errLaneJoinOpenUnadmitted) {
		t.Fatal("a session-ended answer was blamed on the open")
	}
}

// A JOIN that wins the race against the original OPEN's acknowledgement
// confirms the flow, and the late OPEN_OK that may still follow is accepted.
func TestJoinConfirmsAnOptimisticOpen(t *testing.T) {
	flow := newIsolationTestFlow(t, false)
	flow.requireOpenConfirmation()
	flow.confirmOpenByJoin()
	if flow.openConfirmationRequired.Load() {
		t.Fatal("an admitted JOIN left the open unconfirmed")
	}
	late := protocol.Frame{Header: protocol.Header{Type: protocol.TypeOpenOK, SessionID: flow.sessionID, FlowID: flow.flowID}}
	if err := flow.acceptOpenConfirmation(late); err != nil {
		t.Fatalf("late OPEN_OK after a JOIN confirmed the open: %v", err)
	}
	if err := flow.acceptOpenConfirmation(late); err == nil {
		t.Fatal("a second late OPEN_OK was accepted")
	}
	unconfirmed := newIsolationTestFlow(t, false)
	if err := unconfirmed.acceptOpenConfirmation(late); err == nil {
		t.Fatal("an OPEN_OK nobody asked for was accepted")
	}
}

// heldSOCKS5 is an outbound SOCKS5 proxy whose CONNECT answer waits for
// release, which holds the gateway's destination dial for as long as a test
// needs. After answering it echoes.
func heldSOCKS5(t *testing.T, release <-chan struct{}) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				greeting := make([]byte, 3)
				if _, err := io.ReadFull(conn, greeting); err != nil {
					return
				}
				_, _ = conn.Write([]byte{5, 0})
				request := make([]byte, 4)
				if _, err := io.ReadFull(conn, request); err != nil {
					return
				}
				var skip int
				switch request[3] {
				case 1:
					skip = 4 + 2
				case 4:
					skip = 16 + 2
				case 3:
					length := make([]byte, 1)
					if _, err := io.ReadFull(conn, length); err != nil {
						return
					}
					skip = int(length[0]) + 2
				}
				if _, err := io.ReadFull(conn, make([]byte, skip)); err != nil {
					return
				}
				<-release
				_, _ = conn.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0})
				_, _ = io.Copy(conn, conn)
			}(conn)
		}
	}()
	return listener.Addr().String()
}

// The stream that carried OPEN dies while the gateway is dialling, and the
// client's replacement JOIN arrives during the dial. The JOIN must be
// admitted to a running flow: before, the failed OPEN_OK write closed the
// whole flow, replacement included.
func TestOpenLaneLostDuringDialIsRescuedByJoin(t *testing.T) {
	release := make(chan struct{})
	certificate, roots := testCertificate(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0", Credentials: certificate, EnableQUIC: true, ChunkSize: 4 * 1024, Logger: logger,
		DestinationPolicy: DestinationPolicy{AllowPrivate: true, OutboundSOCKS5: heldSOCKS5(t, release)},
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(ClientConfig{
		ListenAddr: "127.0.0.1:0", RemoteAddr: packetConn.LocalAddr().String(), Credentials: roots, Transport: TransportQUIC, ChunkSize: 4 * 1024, Logger: logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = server.ServePacketConn(ctx, packetConn) }()

	sessionID, err := session.NewSessionID()
	if err != nil {
		t.Fatal(err)
	}
	const flowID = 77
	payload, err := session.EncodeDestination("127.0.0.1:9")
	if err != nil {
		t.Fatal(err)
	}
	openLane, err := client.dialJoinLane(ctx, TransportQUIC, sessionID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := openLane.fc.Write(protocol.Frame{Header: protocol.Header{
		Version: protocol.Version, Type: protocol.TypeOpen, SessionID: sessionID, FlowID: flowID, Class: protocol.ClassNew,
	}, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, pending := server.lookupLiveOrPending(sessionID)
		if pending != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the OPEN never became pending")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// The OPEN's lane dies mid-dial, as a pool reset would kill it.
	_ = openLane.fc.Close()
	_ = openLane.outer.Close()

	joined := make(chan error, 1)
	var joinLane *mpLane
	go func() {
		joinCtx := context.WithValue(ctx, laneReplacementContextKey{}, laneReplacementHint{id: 0})
		lane, err := client.dialJoinLane(joinCtx, TransportQUIC, sessionID, 1)
		if err == nil {
			joinLane, err = client.completeLaneJoin(joinCtx, lane, flowID, 0)
		}
		joined <- err
	}()
	time.Sleep(200 * time.Millisecond)
	close(release)
	select {
	case err := <-joined:
		if err != nil {
			t.Fatalf("JOIN during the dial: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("JOIN during the dial was never answered")
	}
	defer joinLane.fc.Close()
	// The flow is alive and carries data over the replacement.
	if err := joinLane.fc.Write(protocol.Frame{Header: protocol.Header{
		Version: protocol.Version, Type: protocol.TypeData, SessionID: sessionID, FlowID: flowID, Class: protocol.ClassInteractive,
	}, Payload: []byte("ping")}); err != nil {
		t.Fatal(err)
	}
	for {
		frame, err := joinLane.fc.Read()
		if err != nil {
			t.Fatalf("replacement lane closed: %v", err)
		}
		if frame.Header.Type == protocol.TypeReset {
			t.Fatalf("flow reset: %q", frame.Payload)
		}
		if frame.Header.Type == protocol.TypeData {
			if string(frame.Payload) != "ping" {
				t.Fatalf("echo = %q", frame.Payload)
			}
			return
		}
	}
}

// Registered is not yet answered: until the OPEN's own OPEN_OK has gone out, a
// JOIN still waits, so it cannot retire the OPEN's lane under it.
func TestJoinWaitsForOpenOKEvenOnceRegistered(t *testing.T) {
	owner := identity.Principal{ProviderID: "provider", AccountID: "account", DeviceID: "owner"}
	server, _ := pendingTestServer()
	flow := newIsolationTestFlow(t, false)
	flow.lanes[0] = isolationLane(t, 0)
	pending, _ := server.beginPendingSession(flow.sessionID, flow.flowID, owner)
	if !server.registerSession(flow.sessionID, newServerFlow(flow, owner, TransportQUIC, 4)) {
		t.Fatal("register failed")
	}
	response := joinPendingSession(server, owner, flow.sessionID, flow.flowID)
	select {
	case frame := <-response:
		t.Fatalf("JOIN answered with type %d before the OPEN_OK was written", frame.Header.Type)
	case <-time.After(100 * time.Millisecond):
	}
	server.endPendingSession(flow.sessionID, pending)
	if frame, ok := <-response; !ok || frame.Header.Type != protocol.TypeOpenOK {
		t.Fatalf("JOIN after settlement answered %+v", frame.Header)
	}
}

func TestOpenRefusalsKeepTheirMeaning(t *testing.T) {
	flow := newIsolationTestFlow(t, false)
	flow.requireOpenConfirmation()
	// Only "unknown session" says the session is not there.
	for _, other := range []error{
		fmt.Errorf("%w: invalid control lane replacement", errLaneJoinRejected),
		errLaneJoinRejected,
	} {
		if errors.Is(classifyUnadmittedRefusal(flow, other), errLaneJoinOpenUnadmitted) {
			t.Fatalf("%v was blamed on the open", other)
		}
	}
	if !errors.Is(classifyUnadmittedRefusal(flow, fmt.Errorf("%w: unknown session", errLaneJoinRejected)), errLaneJoinOpenUnadmitted) {
		t.Fatal("unknown session before OPEN_OK was not classified")
	}
	// A failed OPEN is what the flow fails with.
	flow.noteRefusalCause(errLaneJoinOpenFailed)
	flow.resumeRefused.Store(true)
	if err := flow.resumeRefusal(); !errors.Is(err, errOpenFailed) {
		t.Fatalf("failed open ended the flow with %v", err)
	}
	if kind := replacementFailureKind(errLaneJoinPending); kind != "open_pending" {
		t.Fatalf("pending kind = %q", kind)
	}
}

// "session pending" is not the lane ceiling: a client must not stop trying.
func TestCompleteLaneJoinMapsSessionPendingApartFromCapacity(t *testing.T) {
	client := &Client{cfg: ClientConfig{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), HandshakeTimeout: time.Second}}
	local, remote := net.Pipe()
	defer remote.Close()
	sessionID := [16]byte{6}
	go func() {
		fc := newFrameConn(remote)
		if _, err := fc.Read(); err != nil {
			return
		}
		_ = fc.Write(protocol.Frame{Header: protocol.Header{
			Version: protocol.Version, Type: protocol.TypeReset, SessionID: sessionID, FlowID: 3, Class: protocol.ClassBulk,
		}, Payload: session.ResetPayload(session.ResetFlowLimit, sessionPendingAnswer)})
	}()
	lane := &authenticatedLane{fc: newFrameConn(local), outer: local, sessionID: sessionID, laneID: 1, kind: TransportQUIC}
	_, err := client.completeLaneJoin(context.Background(), lane, 3, 0)
	if !errors.Is(err, errLaneJoinPending) || errors.Is(err, errLaneJoinCapacity) || errors.Is(err, errLaneJoinRejected) {
		t.Fatalf("session pending = %v", err)
	}
}

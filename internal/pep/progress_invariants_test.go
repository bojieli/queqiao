package pep

import (
	"bytes"
	"context"
	"errors"
	"github.com/bojieli/queqiao/internal/identity"
	"github.com/bojieli/queqiao/internal/memlimit"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/protocol"
)

func TestLaneReaderReachesACKBehindFullDataQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inner, app := net.Pipe()
	defer app.Close()
	flow := newMultipathFlow(ctx, inner, [16]byte{1}, 7, 1024, protocol.FlagAckUp, protocol.FlagAckDown, nil, nil)
	defer flow.closeAll()
	local, peer := net.Pipe()
	defer peer.Close()
	lane := &mpLane{id: 0, fc: newFrameConn(local)}
	flow.lanes[0] = lane
	flow.noteSent(0, 8)
	for len(flow.events) < cap(flow.events) {
		flow.events <- inboundEvent{}
	}
	go flow.readLane(lane)
	sent := make(chan error, 1)
	go func() {
		fc := newFrameConn(peer)
		h := protocol.Header{Version: protocol.Version, SessionID: flow.sessionID, FlowID: flow.flowID, Type: protocol.TypeData}
		if err := fc.Write(protocol.Frame{Header: h, Payload: []byte("response")}); err != nil {
			sent <- err
			return
		}
		h.Type = protocol.TypeAck
		h.Flags = flow.sendAckFlag
		h.Sequence = 8
		sent <- fc.Write(protocol.Frame{Header: h})
	}()
	select {
	case err := <-sent:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("lane reader cannot reach ACK behind DATA")
	}
	deadline := time.Now().Add(time.Second)
	for flow.pendingOutbound() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if flow.pendingOutbound() {
		t.Fatal("opposite direction still blocked")
	}
	// The overflow DATA must be delivered, not silently discarded.
	for i := 0; i < cap(flow.events); i++ {
		<-flow.events
	}
	select {
	case ev := <-flow.events:
		if string(ev.frame.Payload) != "response" {
			t.Fatalf("lost data: %q", ev.frame.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("buffered DATA did not resume")
	}
}

func TestRepeatedSACKDoesNotPretendProgress(t *testing.T) {
	flow := newAckTestFlow(newAckCaptureConn(0, nil))
	flow.sendAckFlag = protocol.FlagAckUp
	flow.ackTrack = newAckTracker()
	flow.noteSent(0, 32)
	payload, err := protocol.EncodeAckRanges([][2]uint64{{16, 24}})
	if err != nil {
		t.Fatal(err)
	}
	frame := protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeAck, SessionID: flow.sessionID, FlowID: flow.flowID, Flags: flow.sendAckFlag | protocol.FlagAckRanges}, Payload: payload}
	lane := flow.lanes[0]
	if err := flow.receiveProgressACK(lane, frame); err != nil {
		t.Fatal(err)
	}
	flow.lastAckProgressNS.Store(1)
	lane.suspected.Store(true)
	if err := flow.receiveProgressACK(lane, frame); err != nil {
		t.Fatal(err)
	}
	if flow.lastAckProgressNS.Load() != 1 || !lane.suspected.Load() {
		t.Fatal("duplicate SACK resets stall progress/suspicion")
	}
}

func TestSACKCannotAcknowledgeUnsentBytes(t *testing.T) {
	flow := newAckTestFlow(newAckCaptureConn(0, nil))
	flow.sendAckFlag = protocol.FlagAckUp
	flow.ackTrack = newAckTracker()
	flow.noteSent(0, 8)
	payload, _ := protocol.EncodeAckRanges([][2]uint64{{16, 24}})
	err := flow.receiveProgressACK(flow.lanes[0], protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeAck, SessionID: flow.sessionID, FlowID: flow.flowID, Flags: flow.sendAckFlag | protocol.FlagAckRanges}, Payload: payload})
	if err == nil {
		t.Fatal("SACK accepted unsent bytes")
	}
	if flow.ackTrack.Covered(16, 24) {
		t.Fatal("invalid SACK mutated delivery state")
	}
}

func TestFrameWriteDeadlineIncludesWriterWait(t *testing.T) {
	local, peer := net.Pipe()
	defer local.Close()
	defer peer.Close()
	fc := newFrameConn(local)
	fc.writeMu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- fc.WriteContext(ctx, protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeAck}})
	}()
	select {
	case err := <-result:
		fc.writeMu.Unlock()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		fc.writeMu.Unlock()
		<-result
		t.Fatal("deadline did not bound writer-lock wait")
	}
}

func TestPendingWorkHasOneRecoveryBudget(t *testing.T) {
	for _, refusal := range []bool{false, true} {
		name := "silent lane"
		if refusal {
			name = "refused rescue with old lane still open"
		}
		t.Run(name, func(t *testing.T) {
			flow := newGraceTestFlow(t)
			flow.stallScan = time.Millisecond
			flow.stallGrace = 2 * time.Millisecond
			flow.progressGrace = 30 * time.Millisecond
			flow.noteSent(0, 8)
			flow.resumeRefused.Store(refusal)
			stop := make(chan struct{})
			defer close(stop)
			go flow.stallWatchdog(stop)
			select {
			case err := <-flow.ackErr:
				want := errFlowNoProgress
				if refusal {
					want = errResumeRefused
				}
				if !errors.Is(err, want) {
					t.Fatalf("got %v want %v", err, want)
				}
			case <-time.After(time.Second):
				t.Fatal("pending flow never terminated")
			}
		})
	}
}

func TestIdleConnectionHasNoProgressDeadline(t *testing.T) {
	flow := newGraceTestFlow(t)
	flow.stallScan = time.Millisecond
	flow.stallGrace = time.Millisecond
	flow.progressGrace = 10 * time.Millisecond
	stop := make(chan struct{})
	defer close(stop)
	go flow.stallWatchdog(stop)
	select {
	case err := <-flow.ackErr:
		t.Fatalf("idle connection failed: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
}

func TestApplicationBackpressureIsBoundedAndResumes(t *testing.T) {
	for _, resume := range []bool{false, true} {
		name := "stopped"
		if resume {
			name = "resumes"
		}
		t.Run(name, func(t *testing.T) {
			flow := newGraceTestFlow(t)
			inner, app := net.Pipe()
			defer app.Close()
			defer inner.Close()
			flow.inner = inner
			flow.progressGrace = 100 * time.Millisecond
			done := make(chan error, 1)
			go func() { done <- flow.writeApplication([]byte("payload")) }()
			if resume {
				time.Sleep(10 * time.Millisecond)
				buf := make([]byte, 7)
				if _, err := io.ReadFull(app, buf); err != nil {
					t.Fatal(err)
				}
				if string(buf) != "payload" {
					t.Fatal("corrupt payload")
				}
			}
			select {
			case err := <-done:
				if resume && err != nil {
					t.Fatal(err)
				}
				if !resume && !errors.Is(err, errApplicationBackpressure) {
					t.Fatalf("got %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("application write hung")
			}
		})
	}
}

func TestCancelledFrameWriteReleasesWriter(t *testing.T) {
	local, peer := net.Pipe()
	defer local.Close()
	defer peer.Close()
	fc := newFrameConn(local)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- fc.WriteContext(ctx, protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeAck}})
	}()
	// The peer deliberately never reads. Cancellation must interrupt the
	// socket write, not just the wait for the framing lock.
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("blocked write succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation left writer blocked")
	}
}

func TestReceiveBacklogBudgetReleasedOnClose(t *testing.T) {
	flow := newGraceTestFlow(t)
	flow.receiveMemory = memlimit.New(1024)
	flow.memoryLimits.maxReceiveBytes = 1024
	for len(flow.events) < cap(flow.events) {
		flow.events <- inboundEvent{}
	}
	event := inboundEvent{frame: protocol.Frame{Payload: make([]byte, 512)}}
	// At most one frame can be in the pump; the remaining frames must meet
	// the shared limit. Overflow must be an explicit error, never a wait.
	rejected := false
	for i := 0; i < 5; i++ {
		if !flow.enqueueInbound(event) {
			rejected = true
			break
		}
	}
	if !rejected {
		t.Fatal("backlog exceeded budget")
	}
	select {
	case err := <-flow.ackErr:
		if !errors.Is(err, errReceiveBacklog) {
			t.Fatal(err)
		}
	default:
		t.Fatal("overflow silently dropped data")
	}
	flow.closeAll()
	deadline := time.Now().Add(time.Second)
	for flow.receiveMemory.Snapshot().Used != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if flow.receiveMemory.Snapshot().Used != 0 {
		t.Fatal("backlog retained memory after close")
	}
}

func TestSuspectedYoungLaneDoesNotBlockReplacement(t *testing.T) {
	flow := newIsolationTestFlow(t, false)
	session := newServerFlow(flow, identity.Principal{}, TransportTCP, 1)
	old := isolationLane(t, 0)
	old.kind = TransportTCP
	if err := session.addLane(old); err != nil {
		t.Fatal(err)
	}
	old.suspected.Store(true)
	fresh := isolationLane(t, 1)
	fresh.kind = TransportTCP
	if err := session.addLane(fresh); err != nil {
		t.Fatalf("stalled lane prevented replacement: %v", err)
	}
	if !old.closed.Load() || flow.laneCount() != 1 {
		t.Fatal("replacement was not bounded")
	}
}

func TestBidirectionalFlowSurvivesPausedReader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	left, appLeft := net.Pipe()
	right, appRight := net.Pipe()
	defer appLeft.Close()
	defer appRight.Close()
	a := newMultipathFlow(ctx, left, [16]byte{1}, 7, 4096, protocol.FlagAckUp, protocol.FlagAckDown, nil, nil)
	b := newMultipathFlow(ctx, right, [16]byte{1}, 7, 4096, protocol.FlagAckDown, protocol.FlagAckUp, nil, nil)
	defer a.closeAll()
	defer b.closeAll()
	// A tiny event channel makes this deterministic with a modest payload.
	a.events = make(chan inboundEvent, 2)
	b.events = make(chan inboundEvent, 2)
	outerA, outerB := net.Pipe()
	if err := a.addLane(&mpLane{id: 0, kind: TransportTCP, fc: newFrameConn(outerA)}); err != nil {
		t.Fatal(err)
	}
	if err := b.addLane(&mpLane{id: 0, kind: TransportTCP, fc: newFrameConn(outerB)}); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	go func() { _, err := a.run(ctx); results <- err }()
	go func() { _, err := b.run(ctx); results <- err }()
	payload := bytes.Repeat([]byte("abcdefgh"), 128*1024)
	writes := make(chan error, 2)
	go func() { _, err := appLeft.Write(payload); writes <- err }()
	go func() { _, err := appRight.Write(payload); writes <- err }()
	// Left deliberately does not read. Right must nevertheless receive the
	// entire upload, including ACK-dependent chunks, over this same lane.
	received := make(chan error, 1)
	go func() {
		buf := make([]byte, len(payload))
		_, err := io.ReadFull(appRight, buf)
		if err == nil && !bytes.Equal(buf, payload) {
			err = errors.New("upload mismatch")
		}
		received <- err
	}()
	select {
	case err := <-received:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("paused download blocked upload")
	}
	buf := make([]byte, len(payload))
	_ = appLeft.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(appLeft, buf); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, payload) {
		t.Fatal("resumed download mismatch")
	}
	for i := 0; i < 2; i++ {
		select {
		case err := <-writes:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("writer failed to finish")
		}
	}
	cancel()
	for i := 0; i < 2; i++ {
		select {
		case <-results:
		case <-time.After(time.Second):
			t.Fatal("flow worker did not stop")
		}
	}
}

func TestNoProgressClosesFlowWithSilentPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	inner, app := net.Pipe()
	defer app.Close()
	flow := newMultipathFlow(ctx, inner, [16]byte{1}, 7, 1024, protocol.FlagAckUp, protocol.FlagAckDown, nil, nil)
	defer flow.closeAll()
	flow.stallScan = time.Millisecond
	flow.stallGrace = 2 * time.Millisecond
	flow.progressGrace = 30 * time.Millisecond
	peer := newAckCaptureConn(0, nil) // accepts all writes but returns no ACKs
	if err := flow.addLane(&mpLane{id: 0, kind: TransportTCP, fc: newFrameConn(peer)}); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { _, err := flow.run(ctx); result <- err }()
	if _, err := app.Write([]byte("pending")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, errFlowNoProgress) {
			t.Fatalf("unrecoverable flow returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("silent peer retained logical flow")
	}
	if !flow.doneChanClosed() {
		t.Fatal("flow remained active")
	}
	_ = app.SetWriteDeadline(time.Now().Add(20 * time.Millisecond))
	if _, err := app.Write([]byte("more")); err == nil {
		t.Fatal("application socket remained open")
	}
}

func TestUDPRelayReaderStopsBeforeSocketReuse(t *testing.T) {
	relay, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	sender, err := net.DialUDP("udp", nil, relay.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	oldPackets := make(chan udpDatagram, 32)
	oldErrors := make(chan error, 1)
	stopOld := startUDPRelayReader(context.Background(), relay, oldPackets, oldErrors)
	stopOld()
	packets := make(chan udpDatagram, 32)
	errs := make(chan error, 1)
	stopNew := startUDPRelayReader(context.Background(), relay, packets, errs)
	defer stopNew()
	for i := 0; i < 16; i++ {
		if _, err := sender.Write([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		select {
		case p := <-packets:
			if len(p.payload) != 1 || p.payload[0] != byte(i) {
				t.Fatalf("unexpected packet %v", p.payload)
			}
		case p := <-oldPackets:
			t.Fatalf("old reader stole replacement packet %v", p.payload)
		case err := <-errs:
			t.Fatal(err)
		case <-time.After(time.Second):
			t.Fatal("replacement did not receive reply")
		}
	}
}

type progressTestLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *progressTestLog) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}
func (b *progressTestLog) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.buf.String() }

func TestUnconfirmedOpenDoesNotRaceRescueJoin(t *testing.T) {
	flow := newGraceTestFlow(t)
	flow.stallScan = time.Millisecond
	flow.stallGrace = 2 * time.Millisecond
	flow.progressGrace = time.Second
	flow.requireOpenConfirmation()
	flow.noteSent(0, 8)
	stop := make(chan struct{})
	defer close(stop)
	go flow.stallWatchdog(stop)
	select {
	case <-flow.stallSignals():
		t.Fatal("JOIN raced unconfirmed OPEN")
	case <-time.After(25 * time.Millisecond):
	}
	flow.confirmOpen()
	select {
	case <-flow.stallSignals():
	case <-time.After(time.Second):
		t.Fatal("confirmed stalled flow did not request rescue")
	}
}

func TestRemoteAbortBypassesPausedDelivery(t *testing.T) {
	flow := newGraceTestFlow(t)
	for len(flow.events) < cap(flow.events) {
		flow.events <- inboundEvent{}
	}
	capture := newAckCaptureConn(0, nil)
	lane := &mpLane{id: 0, fc: newFrameConn(capture)}
	flow.lanes[0] = lane
	frame := protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeClose, Flags: protocol.FlagFin | protocol.FlagCloseAbort, SessionID: flow.sessionID, FlowID: flow.flowID}}
	done := make(chan bool, 1)
	go func() { done <- flow.deliverInbound(lane, frame) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("remote cancellation waited for application queue")
	}
	if !flow.remoteAbort.Load() || !flow.doneChanClosed() {
		t.Fatal("remote cancellation did not terminate flow")
	}
	ack := waitAckFrame(t, capture)
	if ack.Header.Type != protocol.TypeAck || ack.Header.Flags&protocol.FlagAckFinal == 0 {
		t.Fatal("missing abort acknowledgement")
	}
}

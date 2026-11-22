package pep

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/multipath"
	"github.com/bojieli/queqiao/internal/protocol"
)

func TestDuplicateDataReacknowledgesLostACK(t *testing.T) {
	for _, ranges := range []bool{false, true} {
		name := "cumulative"
		if ranges {
			name = "selective"
		}
		t.Run(name, func(t *testing.T) {
			conn := newAckCaptureConn(0, nil)
			flow := newAckTestFlow(conn)
			flow.ackRanges.Store(ranges)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go flow.ackLoop(ctx)
			r := multipath.NewReassembler(multipath.DefaultConfig())
			defer r.Close()
			if _, _, err := r.Insert(multipath.Segment{Sequence: 0, Payload: []byte("response")}); err != nil {
				t.Fatal(err)
			}
			last := flow.acknowledgeArrival(r, 0)
			first := waitAckFrame(t, conn) // Accepted by old transport, lost before peer delivery.
			if first.Header.Sequence != 8 {
				t.Fatalf("initial ACK: %+v", first.Header)
			}
			if out, _, err := r.Insert(multipath.Segment{Sequence: 0, Payload: []byte("resp")}); err != nil || len(out) != 0 {
				t.Fatalf("replay: %q, %v", out, err)
			}
			flow.acknowledgeArrival(r, last)
			ack := waitAckFrame(t, conn)
			if ack.Header.Type != protocol.TypeAck || ack.Header.Sequence != 8 {
				t.Fatalf("replay ACK: %+v", ack.Header)
			}
		})
	}
}

func TestRecoveryLaneReceivesCurrentACK(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inner, app := net.Pipe()
	defer inner.Close()
	defer app.Close()
	flow := newMultipathFlow(ctx, inner, [16]byte{1}, 7, 1024, protocol.FlagAckUp, protocol.FlagAckDown, nil, nil)
	defer flow.closeAll()
	old := newAckCaptureConn(0, nil)
	flow.lanes[0] = &mpLane{id: 0, fc: newFrameConn(old)}
	go flow.ackLoop(ctx)
	flow.scheduleACK(8)
	waitAckFrame(t, old)
	fresh := newAckCaptureConn(0, nil)
	lane := &mpLane{id: 1, fc: newFrameConn(fresh)}
	if err := flow.addLane(lane); err != nil {
		t.Fatal(err)
	}
	ack := waitAckFrame(t, fresh)
	if ack.Header.Type != protocol.TypeAck || ack.Header.Sequence != 8 {
		t.Fatalf("recovery ACK: %+v", ack.Header)
	}
}

func TestACKAvoidsSuspectedLaneWhenFreshLaneExists(t *testing.T) {
	old := newAckCaptureConn(0, nil)
	flow := newAckTestFlow(old)
	flow.lanes[0].suspected.Store(true)
	fresh := newAckCaptureConn(0, nil)
	flow.lanes[1] = &mpLane{id: 1, fc: newFrameConn(fresh)}
	if err := flow.writeACK(context.Background(), 8, protocol.FlagAckDown, false); err != nil {
		t.Fatal(err)
	}
	ack := waitAckFrame(t, fresh)
	if ack.Header.Sequence != 8 {
		t.Fatalf("ACK sequence %d", ack.Header.Sequence)
	}
	select {
	case <-old.frames:
		t.Fatal("ACK routed onto suspected lane")
	default:
	}
	// If every lane is suspected, the flow still has a usable fallback.
	flow.lanes[1].suspected.Store(true)
	if err := flow.writeACK(context.Background(), 9, protocol.FlagAckDown, false); err != nil {
		t.Fatal(err)
	}
	if ack := waitAckFrame(t, old); ack.Header.Sequence != 9 {
		t.Fatalf("fallback ACK sequence %d", ack.Header.Sequence)
	}
}

// A reader paused in one application direction must not stop the ACK clock
// for the opposite direction, even when the DATA delivery queue is full.
func TestACKProgressBypassesBlockedApplicationDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inner, app := net.Pipe()
	defer inner.Close()
	defer app.Close()
	flow := newMultipathFlow(ctx, inner, [16]byte{1}, 7, 1024, protocol.FlagAckUp, protocol.FlagAckDown, nil, nil)
	defer flow.closeAll()
	flow.noteSent(0, 8)
	// Occupy the bounded receive queue as if application delivery is blocked.
	for len(flow.events) < cap(flow.events) {
		flow.events <- inboundEvent{}
	}
	done := make(chan bool, 1)
	go func() {
		done <- flow.deliverInbound(&mpLane{id: 0}, protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeAck, SessionID: flow.sessionID, FlowID: flow.flowID, Flags: flow.sendAckFlag, Sequence: 8}})
	}()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("valid ACK rejected")
		}
	case <-time.After(time.Second):
		t.Fatal("ACK blocked behind application DATA queue")
	}
	if flow.pendingOutbound() {
		t.Fatal("ACK did not release outbound work")
	}
}

func TestFastACKPathRejectsInvalidPeerState(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*protocol.Frame)
	}{
		{"wrong session", func(f *protocol.Frame) { f.Header.SessionID = [16]byte{9} }},
		{"wrong flow", func(f *protocol.Frame) { f.Header.FlowID++ }},
		{"wrong direction", func(f *protocol.Frame) { f.Header.Flags = protocol.FlagAckDown }},
		{"unsent bytes", func(f *protocol.Frame) { f.Header.Sequence = 9 }},
		{"malformed ranges", func(f *protocol.Frame) { f.Header.Flags |= protocol.FlagAckRanges; f.Payload = []byte{255} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inner, app := net.Pipe()
			defer inner.Close()
			defer app.Close()
			flow := newMultipathFlow(context.Background(), inner, [16]byte{1}, 7, 1024, protocol.FlagAckUp, protocol.FlagAckDown, nil, nil)
			flow.noteSent(0, 8)
			frame := protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeAck, SessionID: flow.sessionID, FlowID: flow.flowID, Flags: flow.sendAckFlag, Sequence: 8}}
			tc.mutate(&frame)
			if flow.deliverInbound(&mpLane{id: 0}, frame) {
				t.Fatal("invalid ACK admitted")
			}
			select {
			case err := <-flow.ackErr:
				if err == nil {
					t.Fatal("missing error")
				}
			default:
				t.Fatal("coordinator not notified")
			}
			flow.replayMu.Lock()
			acked := flow.acked
			flow.replayMu.Unlock()
			if acked != 0 {
				t.Fatalf("invalid ACK advanced cursor to %d", acked)
			}
		})
	}
}

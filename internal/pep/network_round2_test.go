package pep

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/coded"
	"github.com/bojieli/queqiao/internal/memlimit"
	"github.com/bojieli/queqiao/internal/pathmodel"
	"github.com/bojieli/queqiao/internal/protocol"
	"github.com/bojieli/queqiao/internal/stripe"
)

func TestRound2ClassificationDoesNotInventActivity(t *testing.T) {
	f := newGraceTestFlow(t)
	old := time.Now().Add(-time.Hour).UnixNano()
	f.lastActivity.Store(old)
	f.lastPayload.Store(old)
	f.observe(0, false)
	if f.lastActivity.Load() != old || f.lastPayload.Load() != old {
		t.Fatal("maintenance refreshed activity")
	}
	f.observe(1, true)
	if f.lastActivity.Load() <= old || f.lastPayload.Load() <= old {
		t.Fatal("payload did not refresh activity")
	}
}

func TestRound2SingleTCPStartsRecovery(t *testing.T) {
	rig := newJoinTestRig(t, TransportTCP, TransportTCP, 1)
	flow := newGraceTestFlow(t)
	initial := isolationLaneKind(t, 0, TransportTCP)
	if err := flow.addLane(initial); err != nil {
		t.Fatal(err)
	}
	flow.closeFailedLane(initial)
	defer flow.closeAll()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		rig.client.manageLanes(ctx, flow, rig.serverFlow.sessionID, rig.serverFlow.flowID, TransportTCP)
	}()
	deadline := time.NewTicker(10 * time.Millisecond)
	defer deadline.Stop()
	for flow.laneCount() == 0 {
		select {
		case <-deadline.C:
		case <-ctx.Done():
			t.Fatal("single TCP manager did not join a replacement")
		}
	}
	cancel()
	<-done
}

func TestRound2HandshakeCancellation(t *testing.T) {
	for _, udp := range []bool{false, true} {
		t.Run(map[bool]string{false: "join", true: "udp"}[udp], func(t *testing.T) {
			local, peer := net.Pipe()
			defer local.Close()
			defer peer.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			received := make(chan struct{})
			go func() { _, _ = newFrameConn(peer).Read(); close(received) }()
			client := &Client{cfg: ClientConfig{Transport: TransportTCP, HandshakeTimeout: time.Second}}
			lane := &authenticatedLane{fc: newFrameConn(local), outer: local, kind: TransportTCP, sessionID: [16]byte{1}, laneID: 1}
			client.dialAuthenticatedLaneForTest = func(context.Context, TransportKind) (*authenticatedLane, error) { return lane, nil }
			done := make(chan error, 1)
			go func() {
				if udp {
					_, err := client.openUDPAssociationMode(ctx, nil, false, false)
					done <- err
				} else {
					_, err := client.completeLaneJoin(ctx, lane, 7, 0)
					done <- err
				}
			}()
			<-received
			cancel()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("cancelled handshake succeeded")
				}
			case <-time.After(200 * time.Millisecond):
				t.Fatal("handshake ignored cancellation")
			}
		})
	}
}

func TestRound2OpenDeadlineWithoutApplicationPayload(t *testing.T) {
	f := newGraceTestFlow(t)
	f.openAckPending = true
	f.requireOpenConfirmation()
	f.openDeadline = time.Now().Add(30 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	_, err := f.run(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 300*time.Millisecond {
		t.Fatalf("OPEN result=%v elapsed=%v", err, time.Since(start))
	}
}

func TestRound2FINAcceptsOnlyHistoricalCopies(t *testing.T) {
	inner, app := net.Pipe()
	defer inner.Close()
	defer app.Close()
	flow := newMultipathFlow(context.Background(), inner, [16]byte{1}, 7, 1024, protocol.FlagAckUp, protocol.FlagAckDown, nil, nil)
	capture := newAckCaptureConn(0, nil)
	defer capture.Close()
	flow.lanes[0] = &mpLane{id: 0, fc: newFrameConn(capture)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- flow.receiveInner(ctx) }()
	send := func(typ protocol.Type, seq uint64, flags uint16, data string) {
		flow.events <- inboundEvent{frame: protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: typ, SessionID: [16]byte{1}, FlowID: 7, Sequence: seq, Flags: flags}, Payload: []byte(data)}}
	}
	send(protocol.TypeData, 0, 0, "abc")
	if _, err := io.ReadFull(app, make([]byte, 3)); err != nil {
		t.Fatal(err)
	}
	send(protocol.TypeClose, 3, protocol.FlagFin, "")
	waitAckFrame(t, capture)
	send(protocol.TypeData, 0, 0, "abc")
	waitAckFrame(t, capture)
	select {
	case err := <-result:
		t.Fatalf("duplicate ended reverse direction: %v", err)
	default:
	}
	send(protocol.TypeData, ^uint64(0), 0, "x")
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("invalid data accepted")
		}
	case <-time.After(time.Second):
		t.Fatal("invalid post-FIN data not rejected")
	}
}

func TestRound2ACKProgressWhileApplicationDoesNotRead(t *testing.T) {
	inner, app := net.Pipe()
	defer inner.Close()
	defer app.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	flow := newMultipathFlow(ctx, inner, [16]byte{1}, 7, 1024, protocol.FlagAckUp, protocol.FlagAckDown, nil, nil)
	flow.noteSent(0, 10)
	lane := &mpLane{}
	result := make(chan error, 1)
	go func() { result <- flow.receiveInner(ctx) }()
	frame := protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeData, SessionID: [16]byte{1}, FlowID: 7}, Payload: []byte("blocked response")}
	flow.deliverInbound(lane, frame)
	frame.Header.Type = protocol.TypeAck
	frame.Header.Flags = protocol.FlagAckUp
	frame.Header.Sequence = 10
	frame.Payload = nil
	if !flow.deliverInbound(lane, frame) {
		t.Fatal("ACK rejected")
	}
	flow.replayMu.Lock()
	acked := flow.acked
	flow.replayMu.Unlock()
	if acked != 10 {
		t.Fatalf("ACK blocked behind application delivery: %d", acked)
	}
	cancel()
	inner.Close()
	<-result
}

func TestRound2SlowUDPNameDoesNotBlockAnotherDestination(t *testing.T) {
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	target, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	ctx, cancel := context.WithCancel(context.Background())
	var lookups atomic.Int32
	entered := make(chan struct{})
	sent := make(chan struct{}, 1)
	forward := newUDPForwarder(ctx, socket, func(ctx context.Context, name string) ([]*net.UDPAddr, error) {
		if name == "slow:53" {
			if lookups.Add(1) == 1 {
				close(entered)
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return []*net.UDPAddr{target.LocalAddr().(*net.UDPAddr)}, nil
	}, func(int) { notifyActivity(sent) })
	defer func() { cancel(); forward.wg.Wait() }()
	forward.enqueue("slow:53", []byte("slow"))
	<-entered
	for range 100 {
		forward.enqueue("slow:53", []byte("coalesced"))
	}
	forward.enqueue("numeric:53", []byte("quick"))
	target.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	buf := make([]byte, 16)
	n, _, err := target.ReadFromUDP(buf)
	if err != nil || string(buf[:n]) != "quick" {
		t.Fatalf("healthy target blocked: %v", err)
	}
	if lookups.Load() != 1 {
		t.Fatal("same name was resolved concurrently")
	}
}

func TestRound2PooledPathProbePreservesSibling(t *testing.T) {
	rig := newJoinTestRig(t, TransportQUIC, TransportQUIC, 1)
	defer rig.client.closeQUICPool()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	sibling, err := rig.client.dialPooledQUICLane(ctx, congestionConfig{kind: defaultCongestion()})
	if err != nil {
		t.Fatal(err)
	}
	defer sibling.Close()
	g := sibling.(*controlPoolStreamConn).generation
	g.probeMu.Lock()
	g.verified = time.Time{}
	g.probeMu.Unlock()
	if err := rig.client.verifyControlPath(ctx, g); err != nil {
		t.Fatalf("real echo probe failed: %v", err)
	}
	if g.conn.Context().Err() != nil {
		t.Fatal("probe closed sibling connection")
	}
	g.probeMu.Lock()
	verified := g.verified
	g.probeMu.Unlock()
	if verified.IsZero() {
		t.Fatal("probe did not record remote proof")
	}
}

func TestRound2QueuedReliableChunkCannotTurnIntoDatagram(t *testing.T) {
	model := pathmodel.NewPathModel()
	model.Report(1, pathmodel.Observation{Erasure: .2, BurstFactor: 1, ObservedSamples: 5000, Delivered: 2e6})
	path := coded.New(newPipeCarrier(), coded.Config{Path: model})
	defer path.Close()
	capture := newAckCaptureConn(0, nil)
	defer capture.Close()
	fc := newSplitFrameConn(capture, path)
	var useCoding atomic.Bool
	fc.setCodingPolicy(useCoding.Load)
	sched := stripe.New(bytes.NewReader([]byte("payload")), stripe.Config{Reliable: func(uint64) bool { return !fc.codesData() }})
	defer sched.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	chunk, err := sched.Next(ctx, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	flow := newGraceTestFlow(t)
	flow.scheduler.Store(sched)
	lane := &mpLane{id: 1, fc: fc, writeQ: make(chan laneFrame, 1), writeDone: make(chan struct{})}
	if err := flow.sendChunk(ctx, lane, chunk, true); err != nil {
		t.Fatal(err)
	}
	queued := <-lane.writeQ
	useCoding.Store(true)
	if !fc.codesData() {
		t.Fatal("test path did not switch to coding")
	}
	if err := fc.writeContextMode(ctx, queued.frame, queued.forceReliable); err != nil {
		t.Fatal(err)
	}
	if queued.onWritten != nil {
		queued.onWritten()
	}
	frame := waitAckFrame(t, capture)
	if string(frame.Payload) != "payload" {
		t.Fatal("pinned chunk did not reach stream")
	}
	datagrams, stream := fc.DataSubstrates()
	if datagrams != 0 || stream != 1 {
		t.Fatalf("queued chunk changed substrate: datagram=%d stream=%d", datagrams, stream)
	}
}

func TestRound2DuplexUploadBeforeReadingResponse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	clientInner, app := net.Pipe()
	defer app.Close()
	serverInner, destination := net.Pipe()
	defer destination.Close()
	a, b := net.Pipe()
	client := newMultipathFlow(ctx, clientInner, [16]byte{1}, 7, defaultChunkSize, protocol.FlagAckUp, protocol.FlagAckDown, nil, nil)
	server := newMultipathFlow(ctx, serverInner, [16]byte{1}, 7, defaultChunkSize, protocol.FlagAckDown, protocol.FlagAckUp, nil, nil)
	defer client.closeAll()
	defer server.closeAll()
	if err := client.addLane(&mpLane{id: 0, kind: TransportTCP, fc: newFrameConn(a)}); err != nil {
		t.Fatal(err)
	}
	if err := server.addLane(&mpLane{id: 0, kind: TransportTCP, fc: newFrameConn(b)}); err != nil {
		t.Fatal(err)
	}
	go client.run(ctx)
	go server.run(ctx)
	const size = 32 * 1024 * 1024
	payload := bytes.Repeat([]byte{'x'}, size)
	results := make(chan error, 3)
	go func() { _, err := destination.Write(payload); results <- err }()
	go func() { _, err := io.CopyN(io.Discard, destination, size); results <- err }()
	go func() {
		_, err := app.Write(payload)
		if err == nil {
			_, err = io.CopyN(io.Discard, app, size)
		}
		results <- err
	}()
	for range 3 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("bidirectional flow deadlocked")
		}
	}
}

func TestRound2InboundMemoryIsBoundedAndReleased(t *testing.T) {
	f := newGraceTestFlow(t)
	f.memoryLimits.maxReceiveBytes = 8192
	f.receiveMemory = memlimit.New(8192)
	event := inboundEvent{frame: protocol.Frame{Header: protocol.Header{Type: protocol.TypeData}, Payload: make([]byte, 4096)}}
	for range 2 {
		if !f.queueInbound(event) {
			t.Fatal("available receive budget refused")
		}
	}
	if f.queueInbound(event) {
		t.Fatal("receive budget exceeded")
	}
	f.closeAll()
	until := time.Now().Add(time.Second)
	for f.receiveMemory.Snapshot().Used != 0 {
		if time.Now().After(until) {
			t.Fatalf("queued payload leaked: %+v", f.receiveMemory.Snapshot())
		}
		time.Sleep(time.Millisecond)
	}
}

package pep

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/coded"
	"github.com/bojieli/queqiao/internal/pathmodel"
	"github.com/bojieli/queqiao/internal/protocol"
	"github.com/bojieli/queqiao/internal/stripe"
)

// segmentedReader hands out one segment per Read, so each becomes its own
// chunk.
type segmentedReader struct{ segments []string }

func (r *segmentedReader) Read(p []byte) (int, error) {
	if len(r.segments) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.segments[0])
	r.segments = r.segments[1:]
	return n, nil
}

// codedStallLane is a QUIC-like lane whose coded path erases nothing on its
// own (a pipe carrier) and whose stream records what is written to it, with
// a sticky coded-retry scheduler on a fake clock.
func codedStallLane(t *testing.T, flow *multipathFlow, now *time.Time, segments ...string) (*mpLane, *frameConn, *ackCaptureConn, *stripe.Scheduler) {
	t.Helper()
	model := pathmodel.NewPathModel()
	model.Report(1, pathmodel.Observation{Erasure: .2, BurstFactor: 1, ObservedSamples: 5000})
	path := coded.New(newPipeCarrier(), coded.Config{Path: model})
	t.Cleanup(func() { _ = path.Close() })
	capture := newAckCaptureConn(0, nil)
	t.Cleanup(func() { _ = capture.Close() })
	fc := newSplitFrameConn(capture, path)
	fc.setCodingPolicy(flow.prefersCodingOverRetransmission)
	lane := &mpLane{id: 0, kind: TransportQUIC, fc: fc, writeQ: make(chan laneFrame, 4), writeDone: make(chan struct{})}
	lane.ready.Store(true)
	flow.lanes[0] = lane
	sched := stripe.New(&segmentedReader{segments: segments}, stripe.Config{
		Reliable:        func(uint64) bool { return !fc.codesData() },
		RetransmitAfter: func() time.Duration { return time.Millisecond },
		CodedRetry:      true,
		Now:             func() time.Time { return *now },
	})
	t.Cleanup(sched.Close)
	flow.scheduler.Store(sched)
	return lane, fc, capture, sched
}

// dispatchCoded sends a chunk the scheduler hands out coded, reports its
// write as coded and its datagrams as registered at the current fake time,
// as the coded path would.
func dispatchCoded(t *testing.T, ctx context.Context, flow *multipathFlow, lane *mpLane, sched *stripe.Scheduler, now time.Time) *stripe.Chunk {
	t.Helper()
	chunk, err := sched.Next(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if chunk.Reliable {
		t.Fatal("fixture must begin on coded datagrams")
	}
	if err := flow.sendChunk(ctx, lane, chunk, true); err != nil {
		t.Fatal(err)
	}
	sent := <-lane.writeQ
	if sent.onCoded == nil {
		t.Fatal("a coded chunk is not observed")
	}
	sent.onWritten(false)
	sent.onCoded(coded.Registered, now)
	return chunk
}

// A lost coded response escapes onto the already authenticated stream, on
// the gateway as on the client: the gateway has no lane manager to consume a
// stall signal. Since the watchdog's coded frontier was deleted (design
// §8.1, §14 stage 11) the escape is the chunk's own: once the registration
// deadline passes it is retried on the stream, with no stall signal on a
// QUIC lane, and fresh exchanges stay coded.
func TestCodedSendStallRecoversOnExistingStream(t *testing.T) {
	flow := newStallTestFlow(t, nil)
	flow.stallGrace = 10 * time.Millisecond
	flow.stallScan = time.Millisecond
	now := time.Now()
	lane, fc, capture, sched := codedStallLane(t, flow, &now, "response")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	dispatchCoded(t, ctx, flow, lane, sched, now)
	// The datagram was registered but erased before delivery. The watchdog
	// runs, and on a QUIC lane it asks the lane manager for nothing.
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { flow.stallWatchdog(stop); close(done) }()
	defer func() { close(stop); <-done }()
	now = now.Add(time.Second)
	if sched.ReissueExpired() != 1 {
		t.Fatal("lost response was not reissued at its registration deadline")
	}
	retry, err := sched.Next(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !retry.Reliable {
		t.Fatal("stalled response keeps retrying unreliable datagrams; no stream escape")
	}
	if !fc.codesData() {
		t.Fatal("recovering old bytes disabled coding for fresh exchanges")
	}
	if err = flow.sendChunk(ctx, lane, retry, true); err != nil {
		t.Fatal(err)
	}
	queued := <-lane.writeQ
	// A delayed acknowledgement may arrive after dispatch but before the
	// writer runs; this retry's reliable snapshot stays consistent with the
	// scheduler.
	if err = flow.acknowledgeReplay(retry.End(), false); err != nil {
		t.Fatal(err)
	}
	if !fc.codesData() {
		t.Fatal("acknowledged recovery permanently disabled coding for later exchanges")
	}
	if err = fc.writeContextMode(ctx, queued.frame, queued.forceReliable); err != nil {
		t.Fatal(err)
	}
	queued.onWritten(true)
	frame := waitAckFrame(t, capture)
	if lane.closed.Load() {
		t.Fatal("recovery closed the existing lane")
	}
	if frame.Header.Type != protocol.TypeData || string(frame.Payload) != "response" {
		t.Fatal("response not delivered on stream")
	}
	select {
	case <-flow.stallSignals():
		t.Fatal("a stall on a QUIC lane signalled the lane manager")
	default:
	}
}

// The sticky reliable bit replaces the watchdog's byte frontier (design
// §8.1, §15.1): a chunk whose coded attempt missed its deadline is retried on
// the stream, every later attempt of that chunk stays on the stream, and a
// fresh chunk after it is coded again.
func TestCodedRecoveryEndsAtTheStalledByteFrontier(t *testing.T) {
	flow := newStallTestFlow(t, nil)
	now := time.Now()
	lane, _, _, sched := codedStallLane(t, flow, &now, "stalled!", "fresh")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stalled := dispatchCoded(t, ctx, flow, lane, sched, now)
	now = now.Add(time.Second)
	if sched.ReissueExpired() != 1 {
		t.Fatal("the coded attempt was not retired at its deadline")
	}
	// The retry is on the stream: the sticky reliable bit.
	retry, err := sched.Next(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if retry.Offset != stalled.Offset || !retry.Reliable {
		t.Fatalf("the stalled chunk's retry: offset %d reliable %v, want offset %d on the stream", retry.Offset, retry.Reliable, stalled.Offset)
	}
	if err := flow.sendChunk(ctx, lane, retry, true); err != nil {
		t.Fatal(err)
	}
	sent := <-lane.writeQ
	if !sent.forceReliable {
		t.Fatal("the stalled chunk's retry was not pinned to the stream")
	}
	sent.onWritten(true)
	// The stream attempt outlives its timer: the reliable stream delivers the
	// chunk or the lane dies, so the same lane is not given a second copy.
	now = now.Add(time.Second)
	sched.ReissueExpired()
	// What the lane is offered next is the fresh exchange behind the stalled
	// chunk, and it goes coded: the escape covered the stalled bytes only.
	fresh, err := sched.Next(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Offset != stalled.End() || fresh.Reliable {
		t.Fatalf("next chunk at %d reliable %v, want the fresh one at %d coded", fresh.Offset, fresh.Reliable, stalled.End())
	}
	// And should the stalled chunk be offered again, it stays on the stream.
	sched.Complete(0, fresh)
	now = now.Add(time.Second)
	sched.ReissueExpired()
	nextCtx, nextCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer nextCancel()
	if again, err := sched.Next(nextCtx, 0, 0); err == nil && again != nil && again.Offset == stalled.Offset && !again.Reliable {
		t.Fatal("the sticky bit was lost")
	}
}

// Validated selective coverage: ranges beyond the sent bytes are refused and
// change nothing; ranges above a gap, together with a cumulative ACK that
// stops below them, cover the bytes.
func TestCodedRecoveryUsesValidatedSelectiveCoverage(t *testing.T) {
	flow := newStallTestFlow(t, nil)
	flow.noteSent(0, 8)
	if _, err := flow.acknowledgeCoverage(0, false, [][2]uint64{{4, 9}}); err == nil {
		t.Fatal("accepted acknowledgement beyond the sent bytes")
	}
	if flow.ackTrack != nil && flow.ackTrack.Covered(4, 8) {
		t.Fatal("an invalid acknowledgement recorded coverage")
	}
	if _, err := flow.acknowledgeCoverage(0, false, [][2]uint64{{4, 8}}); err != nil {
		t.Fatal(err)
	}
	if flow.ackTrack == nil || flow.ackTrack.Covered(0, 8) {
		t.Fatal("selective coverage above a gap covered the gap")
	}
	if _, err := flow.acknowledgeCoverage(4, false, nil); err != nil {
		t.Fatal(err)
	}
	if !flow.ackTrack.Covered(0, 8) {
		t.Fatal("the range and the cumulative ACK together did not cover the bytes")
	}
}

// A coded attempt's registration report racing the chunk's acknowledgement
// never leaves an acknowledged chunk owed a retry.
func TestCodedRecoveryCannotRaceAnAcknowledgement(t *testing.T) {
	for range 100 {
		flow := newStallTestFlow(t, nil)
		now := time.Now()
		lane, _, _, sched := codedStallLane(t, flow, &now, "response")
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		chunk, err := sched.Next(ctx, 0, 0)
		if err != nil || chunk.Reliable {
			cancel()
			t.Fatalf("fixture: %v reliable=%v", err, chunk != nil && chunk.Reliable)
		}
		if err := flow.sendChunk(ctx, lane, chunk, true); err != nil {
			cancel()
			t.Fatal(err)
		}
		sent := <-lane.writeQ
		sent.onWritten(false)
		start, done := make(chan struct{}), make(chan struct{})
		go func() {
			<-start
			sent.onCoded(coded.Registered, now)
			close(done)
		}()
		close(start)
		sched.Complete(lane.id, chunk)
		<-done
		now = now.Add(time.Second)
		if n := sched.ReissueExpired(); n != 0 {
			cancel()
			t.Fatalf("an acknowledged chunk was reissued (%d)", n)
		}
		cancel()
	}
}

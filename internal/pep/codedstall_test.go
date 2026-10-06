package pep

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/coded"
	"github.com/bojieli/queqiao/internal/pathmodel"
	"github.com/bojieli/queqiao/internal/protocol"
	"github.com/bojieli/queqiao/internal/stripe"
)

// A lost coded response must escape onto the already authenticated stream.
// In particular the server has no client lane manager to consume stallSignal.
func TestCodedSendStallRecoversOnExistingStream(t *testing.T) {
	flow := newStallTestFlow(t, nil)
	flow.stallGrace = 10 * time.Millisecond
	flow.stallScan = time.Millisecond
	model := pathmodel.NewPathModel()
	model.Report(1, pathmodel.Observation{Erasure: .2, BurstFactor: 1, ObservedSamples: 5000, Delivered: 2e6})
	path := coded.New(newPipeCarrier(), coded.Config{Path: model})
	defer path.Close()
	capture := newAckCaptureConn(0, nil)
	defer capture.Close()
	fc := newSplitFrameConn(capture, path)
	fc.setCodingPolicy(flow.prefersCodingOverRetransmission)
	lane := &mpLane{id: 0, kind: TransportQUIC, fc: fc, writeQ: make(chan laneFrame, 2), writeDone: make(chan struct{})}
	lane.ready.Store(true)
	flow.lanes[0] = lane
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	now := time.Now()
	sched := stripe.New(bytes.NewReader([]byte("response")), stripe.Config{
		Reliable:        func(uint64) bool { return !fc.codesData() },
		ForceReliable:   flow.recoverChunkReliably,
		RetransmitAfter: func() time.Duration { return time.Millisecond }, Now: func() time.Time { return now },
	})
	defer sched.Close()
	flow.scheduler.Store(sched)
	first, err := sched.Next(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if first.Reliable {
		t.Fatal("fixture must begin on coded datagrams")
	}
	if err = flow.sendChunk(ctx, lane, first, true); err != nil {
		t.Fatal(err)
	}
	sent := <-lane.writeQ
	// The datagram was accepted by the carrier but erased before delivery.
	sent.onWritten()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { flow.stallWatchdog(stop); close(done) }()
	defer func() { close(stop); <-done }()
	select {
	case <-flow.stallSignals():
	case <-ctx.Done():
		t.Fatal("no stall detected")
	}
	if !fc.codesData() {
		t.Fatal("recovering old bytes disabled coding for fresh exchanges")
	}
	now = now.Add(time.Second)
	if sched.ReissueExpired() != 1 {
		t.Fatal("lost response was not reissued")
	}
	retry, err := sched.Next(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !retry.Reliable {
		t.Fatal("stalled response keeps retrying unreliable datagrams; no stream escape")
	}
	if err = flow.sendChunk(ctx, lane, retry, true); err != nil {
		t.Fatal(err)
	}
	queued := <-lane.writeQ
	// A delayed acknowledgement may arrive after dispatch but before the
	// writer runs. It clears the byte-scoped recovery, but this retry's
	// reliable snapshot must remain consistent with the scheduler.
	if err = flow.acknowledgeReplay(retry.End(), false); err != nil {
		t.Fatal(err)
	}
	if !fc.codesData() {
		t.Fatal("acknowledged recovery permanently disabled coding for later exchanges")
	}
	if err = fc.writeContextMode(ctx, queued.frame, queued.forceReliable); err != nil {
		t.Fatal(err)
	}
	queued.onWritten()
	frame := waitAckFrame(t, capture)
	if lane.closed.Load() {
		t.Fatal("recovery closed the existing lane")
	}
	if frame.Header.Type != protocol.TypeData || string(frame.Payload) != "response" {
		t.Fatal("response not delivered on stream")
	}

}

func TestCodedRecoveryEndsAtTheStalledByteFrontier(t *testing.T) {
	flow := newStallTestFlow(t, nil)
	flow.noteSent(0, 8)
	flow.escapePendingCoding()
	if got := flow.codingRecoveryEnd.Load(); got != 8 {
		t.Fatalf("recovery frontier = %d, want 8", got)
	}
	if !flow.recoverChunkReliably(0) || !flow.recoverChunkReliably(7) {
		t.Fatal("stalled bytes were not pinned to stream recovery")
	}
	if flow.recoverChunkReliably(8) || !flow.prefersCodingOverRetransmission() {
		t.Fatal("delayed feedback suppressed coding for a fresh exchange")
	}
	// Subsequent sends and repeated stall observations cannot move the
	// recovery boundary indefinitely ahead of the acknowledgement.
	flow.noteSent(8, 8)
	flow.escapePendingCoding()
	if got := flow.codingRecoveryEnd.Load(); got != 8 {
		t.Fatalf("new traffic extended recovery frontier to %d", got)
	}
	if _, err := flow.acknowledgeCoverage(4, false, nil); err != nil {
		t.Fatal(err)
	}
	if !flow.recoverChunkReliably(4) {
		t.Fatal("a partial acknowledgement abandoned the outstanding recovery")
	}
	if _, err := flow.acknowledgeCoverage(8, false, nil); err != nil {
		t.Fatal(err)
	}
	if flow.recoverChunkReliably(0) {
		t.Fatal("acknowledged recovery still forces reliable dispatch")
	}
	// A later, genuinely stalled exchange can enter recovery independently.
	flow.escapePendingCoding()
	if got := flow.codingRecoveryEnd.Load(); got != 16 {
		t.Fatalf("next exchange's recovery frontier = %d, want 16", got)
	}
}

func TestCodedRecoveryUsesValidatedSelectiveCoverage(t *testing.T) {
	flow := newStallTestFlow(t, nil)
	flow.noteSent(0, 8)
	flow.escapePendingCoding()
	if _, err := flow.acknowledgeCoverage(0, false, [][2]uint64{{4, 9}}); err == nil {
		t.Fatal("accepted acknowledgement beyond the sent bytes")
	}
	if got := flow.codingRecoveryEnd.Load(); got != 8 {
		t.Fatal("invalid acknowledgement changed coded recovery")
	}
	if _, err := flow.acknowledgeCoverage(0, false, [][2]uint64{{4, 8}}); err != nil {
		t.Fatal(err)
	}
	if !flow.recoverChunkReliably(0) {
		t.Fatal("selective coverage above a gap abandoned recovery")
	}
	// The range and the cumulative ACK together cover the original frontier,
	// even though the cumulative field itself does not reach that frontier.
	if _, err := flow.acknowledgeCoverage(4, false, nil); err != nil {
		t.Fatal(err)
	}
	if flow.recoverChunkReliably(0) {
		t.Fatal("complete selective coverage did not end coded recovery")
	}
	flow.escapePendingCoding()
	if flow.codingRecoveryEnd.Load() != 0 {
		t.Fatal("a stale watchdog scan restarted an already acknowledged recovery")
	}
}

func TestCodedRecoveryCannotRaceAnAcknowledgement(t *testing.T) {
	for range 100 {
		flow := newStallTestFlow(t, nil)
		flow.noteSent(0, 8)
		start, done := make(chan struct{}), make(chan struct{})
		go func() {
			<-start
			flow.escapePendingCoding()
			close(done)
		}()
		close(start)
		if err := flow.acknowledgeReplay(8, false); err != nil {
			t.Fatal(err)
		}
		<-done
		if flow.codingRecoveryEnd.Load() != 0 {
			t.Fatal("racing acknowledgement left completed recovery latched")
		}
	}
}

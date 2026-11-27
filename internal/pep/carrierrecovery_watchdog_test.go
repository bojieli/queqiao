package pep

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/metrics"
	"github.com/bojieli/queqiao/internal/protocol"
)

// Behaviour-equivalent replacements, under recovery=carrier, for the tests of
// the mechanisms stage 8 disables (§15.1): TestStallWatchdogCountsTheWarmSpare
// and TestSuspectedLaneIsDemotedNotKilled (lane demotion),
// TestStallWatchdogDetectsPendingWorkWithoutProgress (the rescue request) and
// TestCodedSendStallRecoversOnExistingStream (the watchdog's coded
// frontier). The originals keep running against the legacy bundle.

// The watchdog still sees a stall, counts it and keeps the no-progress
// budget, but demotes no lane, sets no replacement identity, asks for no
// rescue and pins no coded frontier (§10.4).
func TestCarrierRecoveryWatchdogObservesAStallAndDoesNothingElse(t *testing.T) {
	registry := metrics.New()
	flow := newStallTestFlow(t, registry)
	flow.carrierRecovery = true
	flow.codedRetrySticky = true
	flow.reserveControlLane = true
	controlLocal, controlRemote := net.Pipe()
	dataLocal, dataRemote := net.Pipe()
	t.Cleanup(func() {
		_ = controlLocal.Close()
		_ = controlRemote.Close()
		_ = dataLocal.Close()
		_ = dataRemote.Close()
	})
	control := &mpLane{id: 0, kind: TransportQUIC, fc: newFrameConn(controlLocal), control: true}
	data := &mpLane{id: 1, kind: TransportQUIC, fc: newFrameConn(dataLocal)}
	if err := flow.addLane(control); err != nil {
		t.Fatal(err)
	}
	if err := flow.addLane(data); err != nil {
		t.Fatal(err)
	}
	flow.noteSent(0, 512)
	flow.stallScan = 5 * time.Millisecond
	flow.stallGrace = 20 * time.Millisecond
	flow.progressGrace = 800 * time.Millisecond
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	go flow.stallWatchdog(stop)
	deadline := time.Now().Add(2 * time.Second)
	for registry.Snapshot().FlowStallsDetected == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the watchdog never saw the stall")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Well past several thresholds: it stays one episode with no remedy.
	select {
	case <-flow.stallSignals():
		t.Fatal("the watchdog asked for a rescue under recovery=carrier")
	case <-time.After(200 * time.Millisecond):
	}
	snapshot := registry.Snapshot()
	if snapshot.FlowStallsDetected != 1 || snapshot.StallSpareAttaches != 0 {
		t.Fatalf("stalls %d, spare attaches %d; want 1 and 0", snapshot.FlowStallsDetected, snapshot.StallSpareAttaches)
	}
	if control.suspected.Load() || data.suspected.Load() {
		t.Fatal("a lane was demoted under recovery=carrier")
	}
	if _, ok := flow.rescueReplacement(); ok {
		t.Fatal("a replacement identity was set for a live stream")
	}
	if flow.codingRecoveryEnd.Load() != 0 {
		t.Fatal("the watchdog pinned a coded frontier under recovery=carrier")
	}
	candidates, err := flow.laneCandidates(true)
	if err != nil || len(candidates) != 1 || candidates[0] != data {
		t.Fatalf("data candidates after the stall = %v, %v, want the data lane unchanged", candidates, err)
	}
	// The no-progress budget still ends a flow that never progresses.
	select {
	case <-flow.doneChan():
	case <-time.After(2 * time.Second):
		t.Fatal("the no-progress budget no longer ends the flow")
	}
}

// A coded tail erased on a live carrier moves to the stream by its own
// registration deadline (coded-retry=sticky), with the watchdog running
// under recovery=carrier beside it: the bytes are retried on the existing
// stream, nothing is demoted, nothing is requested, and fresh exchanges stay
// coded. This is what TestCodedSendStallRecoversOnExistingStream checks for
// the legacy watchdog's frontier.
func TestCarrierRecoveryCodedTailMovesToTheExistingStream(t *testing.T) {
	x := newCodedRetryFixture(t, true, false)
	x.flow.carrierRecovery = true
	x.flow.stallScan = time.Millisecond
	x.flow.stallGrace = 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sched := x.scheduler(t, nil)
	go x.flow.writeLane(x.lane)
	t.Cleanup(func() { x.flow.signalDone(); <-x.lane.writeDone })
	go x.flow.superviseChunks(ctx, sched)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { x.flow.stallWatchdog(stop); close(done) }()
	defer func() { close(stop); <-done }()
	go func() {
		for {
			chunk, err := sched.Next(ctx, 0, 0)
			if err != nil || chunk == nil {
				return
			}
			if x.flow.sendChunk(ctx, x.lane, chunk, true) != nil {
				return
			}
		}
	}()
	frame := waitFrameWithin(t, x.capture, x.flow.reissueDelay(false, 0)+chunkReissueInterval+time.Second)
	if frame.Header.Type != protocol.TypeData || string(frame.Payload) != "response" {
		t.Fatalf("stream carried type %d %q, want the response", frame.Header.Type, frame.Payload)
	}
	if tagged, _, _ := x.carrier.counts(); tagged == 0 {
		t.Fatal("the first attempt never went to the code")
	}
	if x.flow.codingRecoveryEnd.Load() != 0 {
		t.Fatal("recovery leaned on the watchdog's frontier")
	}
	if x.lane.suspected.Load() {
		t.Fatal("the lane was demoted")
	}
	select {
	case <-x.flow.stallSignals():
		t.Fatal("recovery signalled the lane manager")
	default:
	}
	if !x.fc.codesData() {
		t.Fatal("recovering one chunk stopped coding fresh exchanges")
	}
}

// The carrier manager owns QUIC bindings only. A flow on TCP -- opened on
// it, or handed off to it -- keeps its bundle's stall rescue under
// recovery=carrier.
func TestCarrierRecoveryLeavesTCPStallRescueAlone(t *testing.T) {
	flow := newStallTestFlow(t, nil)
	flow.carrierRecovery = true
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
	if err := flow.addLane(&mpLane{id: 0, kind: TransportTCP, fc: newFrameConn(local)}); err != nil {
		t.Fatal(err)
	}
	flow.noteSent(0, 512)
	flow.stallScan = 5 * time.Millisecond
	flow.stallGrace = 20 * time.Millisecond
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	go flow.stallWatchdog(stop)
	select {
	case <-flow.stallSignals():
	case <-time.After(2 * time.Second):
		t.Fatal("a stalled TCP flow raised no rescue request under recovery=carrier")
	}
}

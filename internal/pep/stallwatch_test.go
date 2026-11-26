package pep

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/metrics"
	"github.com/bojieli/queqiao/internal/protocol"
)

func newStallTestFlow(t *testing.T, registry *metrics.Registry) *multipathFlow {
	t.Helper()
	inner, peer := net.Pipe()
	t.Cleanup(func() { _ = inner.Close(); _ = peer.Close() })
	return newMultipathFlow(context.Background(), inner, [16]byte{4}, 7, defaultChunkSize, protocol.FlagAckUp, protocol.FlagAckDown, nil, registry)
}

// The threshold is three minimum round trips, clamped: a fast path never
// waits less than the floor, a slow one never more than the cap, and a flow
// with no RTT sample yet gets the conservative default.
func TestStallThresholdClamp(t *testing.T) {
	flow := newStallTestFlow(t, nil)
	if got := flow.stallThreshold(); got != stallThresholdDefault {
		t.Fatalf("no RTT sample: threshold = %s, want %s", got, stallThresholdDefault)
	}
	for _, test := range []struct {
		rtt  time.Duration
		want time.Duration
	}{
		{50 * time.Millisecond, stallThresholdFloor},
		{83 * time.Millisecond, stallThresholdFloor},
		{200 * time.Millisecond, 600 * time.Millisecond},
		{500 * time.Millisecond, 1500 * time.Millisecond},
		{time.Second, stallThresholdCap},
		{5 * time.Second, stallThresholdCap},
	} {
		flow.minRTTNS.Store(test.rtt.Nanoseconds())
		if got := flow.stallThreshold(); got != test.want {
			t.Fatalf("minRTT %s: threshold = %s, want %s", test.rtt, got, test.want)
		}
	}
	// A controller that reports no minimum still leaves the smoothed reading.
	flow.minRTTNS.Store(0)
	flow.currentRTTNS.Store((200 * time.Millisecond).Nanoseconds())
	if got := flow.stallThreshold(); got != 600*time.Millisecond {
		t.Fatalf("smoothed fallback: threshold = %s, want 600ms", got)
	}
	// The test override wins over every sample.
	flow.stallGrace = 10 * time.Millisecond
	if got := flow.stallThreshold(); got != 10*time.Millisecond {
		t.Fatalf("override: threshold = %s, want 10ms", got)
	}
}

// The gate is strict: nothing pending never stalls, and a stall is measured
// from when the work appeared (or last progressed), never from before it.
func TestScanStallGating(t *testing.T) {
	threshold := 100 * time.Millisecond
	now := time.Now()
	var since time.Time
	if scanStall(false, 0, now, threshold, &since) {
		t.Fatal("an idle flow stalled")
	}
	// First observation of pending work starts the clock rather than firing,
	// however old the last progress is.
	stale := now.Add(-time.Hour).UnixNano()
	if scanStall(true, stale, now, threshold, &since) {
		t.Fatal("work pending for one scan stalled on an ancient clock")
	}
	// Progress newer than the clock restarts it.
	if scanStall(true, now.Add(10*time.Millisecond).UnixNano(), now.Add(20*time.Millisecond), threshold, &since) {
		t.Fatal("work that just progressed stalled")
	}
	if !scanStall(true, 0, now.Add(20*time.Millisecond+threshold), threshold, &since) {
		t.Fatal("work pending a full threshold without progress did not stall")
	}
	// Going idle resets the clock, so the next burst gets a full threshold.
	scanStall(false, 0, now, threshold, &since)
	if scanStall(true, 0, now.Add(50*time.Millisecond), threshold, &since) {
		t.Fatal("fresh work inherited the previous burst's clock")
	}
}

// A transport ACK proves delivery, not that the application owes a reply.
// A one-way message or a slow origin must not trigger lane churn.
func TestAcknowledgedApplicationMessageDoesNotTriggerRescue(t *testing.T) {
	flow := newStallTestFlow(t, metrics.New())
	// A TCP bundle is the only binding the watchdog still asks to rescue.
	lane, _ := rescueTestLane(t, 0)
	if err := flow.addLane(lane); err != nil {
		t.Fatal(err)
	}
	flow.stallScan = time.Millisecond
	flow.stallGrace = 20 * time.Millisecond
	flow.observe(512, true)
	flow.bytesUp.Add(512)
	flow.noteSent(0, 512)
	if err := flow.acknowledgeReplay(512, false); err != nil {
		t.Fatal(err)
	}
	if flow.pendingOutbound() {
		t.Fatal("setup has unacknowledged transport data")
	}
	stop := make(chan struct{})
	defer close(stop)
	go flow.stallWatchdog(stop)
	select {
	case <-flow.stallSignals():
		t.Fatal("fully acknowledged application message triggered transport rescue")
	case <-time.After(100 * time.Millisecond):
	}
}

// Demotion, which only a TCP bundle still uses (a QUIC binding is the carrier
// manager's), is not death: a suspected lane is passed over for new writes while
// a healthier lane exists, keeps its place in the healthy set, and is fully
// eligible again the moment it is the only thing left.
func TestSuspectedLaneIsDemotedNotKilled(t *testing.T) {
	flow := newStallTestFlow(t, nil)
	flow.reserveControlLane = true
	controlLocal, controlRemote := net.Pipe()
	dataLocal, dataRemote := net.Pipe()
	t.Cleanup(func() {
		_ = controlLocal.Close()
		_ = controlRemote.Close()
		_ = dataLocal.Close()
		_ = dataRemote.Close()
	})
	control := &mpLane{id: 0, kind: TransportTCP, fc: newFrameConn(controlLocal), control: true}
	data := &mpLane{id: 1, kind: TransportTCP, fc: newFrameConn(dataLocal)}
	if err := flow.addLane(control); err != nil {
		t.Fatal(err)
	}
	if err := flow.addLane(data); err != nil {
		t.Fatal(err)
	}
	candidates, err := flow.laneCandidates(true)
	if err != nil || len(candidates) != 1 || candidates[0] != data {
		t.Fatalf("data candidates before stall = %v, %v", candidates, err)
	}
	spare, demoted := flow.suspectDataLanes()
	if !spare {
		t.Fatal("demoting the data lane found no warm spare in the control lane")
	}
	if demoted != data {
		t.Fatalf("demoted lane = %v, want the data lane: the shadow observer looks at the lane the watchdog marked", demoted)
	}
	if data.closed.Load() {
		t.Fatal("the watchdog closed a suspected lane")
	}
	if len(flow.healthyLanes()) != 2 {
		t.Fatal("a suspected lane left the healthy set")
	}
	candidates, err = flow.laneCandidates(true)
	if err != nil || len(candidates) != 1 || candidates[0] != control {
		t.Fatalf("data candidates after demotion = %v, %v, want the control lane", candidates, err)
	}
	// With every healthy lane suspected there is nothing healthier, and the
	// set is used exactly as before.
	control.suspected.Store(true)
	candidates, err = flow.laneCandidates(true)
	if err != nil || len(candidates) != 1 || candidates[0] != data {
		t.Fatalf("all-suspected candidates = %v, %v, want the data lane again", candidates, err)
	}
	// An acknowledgement carrying new delivery information clears the mark.
	data.suspected.Store(false)
	control.suspected.Store(false)
}

// On a TCP bundle the watchdog fires once per episode, demotes the data lane,
// and asks the bundle manager for a rescue; an idle flow with the same timing
// never fires.
func TestStallWatchdogDetectsPendingWorkWithoutProgress(t *testing.T) {
	registry := metrics.New()
	flow := newStallTestFlow(t, registry)
	lane, _ := rescueTestLane(t, 0)
	if err := flow.addLane(lane); err != nil {
		t.Fatal(err)
	}
	flow.stallScan = 5 * time.Millisecond
	flow.stallGrace = 20 * time.Millisecond
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	go flow.stallWatchdog(stop)

	// Nothing pending: the scan runs and stays silent.
	select {
	case <-flow.stallSignals():
		t.Fatal("an idle flow raised a stall")
	case <-time.After(100 * time.Millisecond):
	}

	flow.noteSent(0, 512)
	select {
	case <-flow.stallSignals():
	case <-time.After(2 * time.Second):
		t.Fatal("pending work with no progress raised no stall")
	}
	if got := registry.Snapshot().FlowStallsDetected; got != 1 {
		t.Fatalf("stall detections = %d, want 1", got)
	}

	// Acknowledging the work is progress; the episode ends and a quiet flow
	// with nothing outstanding does not re-fire.
	if err := flow.acknowledgeReplay(512, false); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if got := registry.Snapshot().FlowStallsDetected; got != 1 {
		t.Fatalf("stall detections after progress = %d, want still 1", got)
	}
}

// A warm spare is counted when the demotion leaves a non-suspected healthy
// lane, and not when the stalled lane was the only one.
func TestStallWatchdogCountsTheWarmSpare(t *testing.T) {
	registry := metrics.New()
	flow := newStallTestFlow(t, registry)
	flow.reserveControlLane = true
	controlLocal, controlRemote := net.Pipe()
	dataLocal, dataRemote := net.Pipe()
	t.Cleanup(func() {
		_ = controlLocal.Close()
		_ = controlRemote.Close()
		_ = dataLocal.Close()
		_ = dataRemote.Close()
	})
	if err := flow.addLane(&mpLane{id: 0, kind: TransportTCP, fc: newFrameConn(controlLocal), control: true}); err != nil {
		t.Fatal(err)
	}
	if err := flow.addLane(&mpLane{id: 1, kind: TransportTCP, fc: newFrameConn(dataLocal)}); err != nil {
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
		t.Fatal("no stall signal")
	}
	snapshot := registry.Snapshot()
	if snapshot.FlowStallsDetected != 1 || snapshot.StallSpareAttaches != 1 {
		t.Fatalf("stalls = %d, spare attaches = %d, want 1 and 1", snapshot.FlowStallsDetected, snapshot.StallSpareAttaches)
	}
}

func rescueTestLane(t *testing.T, id uint64) (*mpLane, net.Conn) {
	t.Helper()
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
	return &mpLane{id: id, kind: TransportTCP, fc: newFrameConn(local)}, remote
}

// A JOIN can succeed without restoring data delivery. The next request in
// the same stall episode must name the current lane, not omit the hint or
// keep naming the lane the previous round already retired.
func TestStallWatchdogRefreshesReplacementAfterRescue(t *testing.T) {
	flow := newStallTestFlow(t, nil)
	old, _ := rescueTestLane(t, 0)
	if err := flow.addLane(old); err != nil {
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
	case <-time.After(time.Second):
		t.Fatal("no initial rescue request")
	}
	flow.rescueInFlight.Store(true)
	flow.retireLaneByID(old.id)
	replacement, _ := rescueTestLane(t, 1)
	if err := flow.addLane(replacement); err != nil {
		t.Fatal(err)
	}
	flow.clearRescueReplacement()
	flow.rescueInFlight.Store(false)
	select {
	case <-flow.stallSignals():
	case <-time.After(time.Second):
		t.Fatal("no retry for the continuing stall")
	}
	if id, ok := flow.rescueReplacement(); !ok || id != replacement.id {
		t.Fatalf("retry replacement = (%d, %t), want (%d, true)", id, ok, replacement.id)
	}
}

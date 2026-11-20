package pep

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/metrics"
)

// A QUIC flow rescued onto TCP is handed off: the QUIC lane manager retires
// the QUIC lanes and returns, and with the default single TCP fallback lane no
// bundle manager takes over. Its return marks replacement abandoned, which is
// true -- nobody will open another lane -- but the flow is not without one: the
// TCP rescue lane is healthy and is replaying the bytes that stalled. The
// watchdog used to treat "abandoned" as terminal whenever bytes were still
// unacknowledged, so it closed the flow on its next scan, a few hundred
// milliseconds after the rescue it had asked for succeeded. On a live client
// 2,904 of 2,982 "no replacement lane will be attempted" failures in eleven
// hours ended within two seconds of a TCP handoff, interactive SSH sessions
// among them.
func TestAbandonedReplacementDoesNotKillAFlowRescuedOntoTCP(t *testing.T) {
	registry := metrics.New()
	flow := newStallTestFlow(t, registry)
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
	if err := flow.addLane(&mpLane{id: 5, kind: TransportTCP, fc: newFrameConn(local)}); err != nil {
		t.Fatal(err)
	}
	// Bytes sent before the stall are still unacknowledged when the handoff
	// completes and the lane manager returns.
	flow.noteSent(0, 4096)
	flow.noteReplacementAbandoned()

	flow.stallScan = 5 * time.Millisecond
	flow.stallGrace = 20 * time.Millisecond
	flow.progressGrace = 5 * time.Second
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	go flow.stallWatchdog(stop)

	// Well past the stall threshold, far short of the no-progress budget:
	// the replay on the rescue lane has its chance to be acknowledged.
	time.Sleep(300 * time.Millisecond)
	if got := registry.Snapshot().FlowStallsDetected; got == 0 {
		t.Fatal("the watchdog never saw the stall, so its survival proves nothing")
	}
	if flow.doneChanClosed() {
		t.Fatal("the watchdog closed a flow whose TCP rescue lane was still healthy")
	}
	select {
	case err := <-flow.ackErr:
		t.Fatalf("the watchdog failed a flow with a healthy lane: %v", err)
	default:
	}

	// The replay is acknowledged on the rescue lane; the flow carries on.
	if err := flow.acknowledgeReplay(4096, false); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if flow.doneChanClosed() {
		t.Fatal("the flow was closed after its replay was acknowledged")
	}
}

// Abandonment is still the prompt answer for a flow with no lane at all: that
// is the silence the flag exists to cut short.
func TestAbandonedReplacementStillEndsAFlowWithNoLane(t *testing.T) {
	flow := newStallTestFlow(t, metrics.New())
	flow.noteSent(0, 4096)
	flow.noteReplacementAbandoned()
	flow.stallScan = 5 * time.Millisecond
	flow.stallGrace = 20 * time.Millisecond
	flow.progressGrace = 5 * time.Second
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	go flow.stallWatchdog(stop)

	select {
	case err := <-flow.ackErr:
		if !errors.Is(err, errReplacementAbandoned) {
			t.Fatalf("watchdog error = %v, want %v", err, errReplacementAbandoned)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a flow with no lane and nobody to replace it waited out its whole budget")
	}
}

// A healthy lane that never delivers is still bounded by the flow's overall
// no-progress budget; sparing it from abandonment does not make it immortal.
func TestAbandonedFlowWithAHealthyButSilentLaneIsStillBounded(t *testing.T) {
	flow := newStallTestFlow(t, metrics.New())
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
	if err := flow.addLane(&mpLane{id: 5, kind: TransportTCP, fc: newFrameConn(local)}); err != nil {
		t.Fatal(err)
	}
	flow.noteSent(0, 4096)
	flow.noteReplacementAbandoned()
	flow.stallScan = 5 * time.Millisecond
	flow.stallGrace = 20 * time.Millisecond
	flow.progressGrace = 150 * time.Millisecond
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	go flow.stallWatchdog(stop)

	select {
	case err := <-flow.ackErr:
		if !errors.Is(err, errFlowNoProgress) {
			t.Fatalf("watchdog error = %v, want %v", err, errFlowNoProgress)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a lane that never delivered held the flow past its no-progress budget")
	}
}

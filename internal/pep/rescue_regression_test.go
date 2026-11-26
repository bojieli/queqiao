package pep

import (
	"testing"
	"time"
)

// A delivered request can legitimately wait for application work or a push
// message. Delivery acknowledgements, not application response timing, are
// the evidence available to a transport watchdog.
func TestAcknowledgedRequestWaitingForResponseDoesNotRescue(t *testing.T) {
	flow := newStallTestFlow(t, nil)
	// A TCP bundle is the only binding the watchdog still asks to rescue.
	lane, _ := rescueTestLane(t, 0)
	if err := flow.addLane(lane); err != nil {
		t.Fatal(err)
	}
	flow.observe(64, true)
	flow.bytesUp.Add(64)
	flow.noteSent(0, 64)
	if err := flow.acknowledgeReplay(64, false); err != nil {
		t.Fatal(err)
	}
	if flow.pendingOutbound() {
		t.Fatal("request was not acknowledged")
	}
	flow.stallScan = 5 * time.Millisecond
	flow.stallGrace = 20 * time.Millisecond
	stop := make(chan struct{})
	defer close(stop)
	go flow.stallWatchdog(stop)
	select {
	case <-flow.stallSignals():
		t.Fatal("an acknowledged request waiting on its application triggered lane rescue")
	case <-time.After(120 * time.Millisecond):
	}
}

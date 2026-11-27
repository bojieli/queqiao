package pep

import (
	"context"
	"testing"
	"time"
)

// A TCP lane the handoff dialled and then discarded, because a QUIC carrier
// came up while it waited, must give the gateway's connection slot back at
// once: the device's connection budget is held for the wait and no longer,
// not for the gateway's whole handshake bound (two seconds here).
func TestADiscardedTCPHandoffLaneReleasesItsGatewaySlot(t *testing.T) {
	rig := newJoinTestRig(t, TransportTCP, TransportTCP, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lane, err := rig.client.dialJoinLane(ctx, TransportTCP, rig.serverFlow.sessionID, 7)
	if err != nil {
		t.Fatal(err)
	}
	// Which of the gateway's admission bounds a TLS/TCP connection holds
	// depends on its admission mode; the connection holds one of them.
	held := func() int { return len(rig.server.semaphore) + len(rig.server.handshakes) }
	// The gateway holds the connection while it waits for the JOIN.
	deadline := time.Now().Add(time.Second)
	for held() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if held() != 1 {
		discardLane(lane)
		t.Fatalf("gateway holds %d connection slots for the waiting lane, want 1", held())
	}
	started := time.Now()
	discardLane(lane)
	for held() != 0 && time.Since(started) < time.Second {
		time.Sleep(5 * time.Millisecond)
	}
	if held() != 0 {
		t.Fatalf("gateway still holds %d connection slots %v after the lane was discarded", held(), time.Since(started))
	}
	t.Logf("gateway released the slot %v after the discard", time.Since(started))
}

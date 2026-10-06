package pep

import (
	"errors"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/identity"
)

func TestRejectedJoinPreservesExistingLanes(t *testing.T) {
	for _, kind := range []TransportKind{TransportQUIC, TransportTCP} {
		t.Run(string(kind), func(t *testing.T) {
			flow := newIsolationTestFlow(t, true)
			session := newServerFlow(flow, identity.Principal{}, TransportQUIC, 4)
			first, second := isolationLane(t, 0), isolationLane(t, 1)
			if err := session.addLane(first); err != nil {
				t.Fatal(err)
			}
			if err := session.addLane(second); err != nil {
				t.Fatal(err)
			}
			first.admitted = time.Now().Add(-2 * laneDeadPathDetection)
			duplicate := isolationLaneKind(t, second.id, kind)
			if err := session.addLaneReplacing(duplicate, first.id, true); !errors.Is(err, errLaneDuplicateID) {
				t.Fatalf("got %v", err)
			}
			if first.closed.Load() || second.closed.Load() || flow.laneCount() != 2 || session.tcpMode {
				t.Fatal("rejected JOIN changed the existing flow")
			}
		})
	}
}

package pathmodel

import (
	"math"
	"testing"
	"time"
)

// The erasure is pooled so that four lanes reach a usable estimate on four
// lanes' worth of samples rather than each waiting for its own, and so that
// they agree. Lanes that disagree compensate differently and the aggregate
// stops being predictable.
func TestTheFloorIsPooledAcrossLanes(t *testing.T) {
	m := NewPathModel()
	// Three lanes with plenty of samples agree on 0.42; a fourth has just
	// started and has seen almost nothing.
	m.Report(1, Observation{Erasure: 0.42, BurstFactor: 1, ObservedSamples: 5000, RoundTrip: 0})
	m.Report(2, Observation{Erasure: 0.42, BurstFactor: 1, ObservedSamples: 5000, RoundTrip: 0})
	m.Report(3, Observation{Erasure: 0.42, BurstFactor: 1, ObservedSamples: 5000, RoundTrip: 0})
	erasure := m.Report(4, Observation{Erasure: 0.05, BurstFactor: 1, ObservedSamples: 20}).Erasure

	if math.Abs(erasure-0.42) > 0.01 {
		t.Fatalf("pooled erasure %.3f, want the weight of the measured lanes at about 0.42", erasure)
	}
	// And the new lane is handed that estimate rather than its own, so it does
	// not have to rediscover the path.
	if erasure < 0.4 {
		t.Fatalf("a joining lane was left with its own uninformed erasure of %.3f", erasure)
	}
}

func TestObservationProgressDoesNotWeightAnUntrustedFloor(t *testing.T) {
	m := NewPathModel()
	m.Report(1, Observation{Erasure: 0.42, BurstFactor: 1, ObservedSamples: 5000, RoundTrip: 0})
	state := m.Report(2, Observation{Erasure: 0, BurstFactor: 1, ObservedSamples: 120, RoundTrip: 0})
	if state.ObservedSamples != 5120 {
		t.Fatalf("observed samples = %.0f, want 5120", state.ObservedSamples)
	}
	if math.Abs(state.Erasure-0.42) > 0.02 {
		t.Fatalf("a contributor with few samples diluted the pooled erasure to %.3f", state.Erasure)
	}
}

func TestPathKnowledgeOutlivesTheConnectionThatMeasuredIt(t *testing.T) {
	m := NewPathModel()
	m.Report(1, Observation{Erasure: 0.42, BurstFactor: 1, ObservedSamples: 5000, RoundTrip: 240 * time.Millisecond})

	// Membership governs only concurrent bandwidth sharing. Simulate a lane
	// going idle without making a five-second unit test: the measurement must
	// remain until the owner explicitly forgets this path.
	m.mu.Lock()
	m.members[1].at = time.Now().Add(-memberIdle - time.Second)
	m.mu.Unlock()

	state := m.Current()
	if state.Erasure != 0.42 {
		t.Fatalf("retained erasure = %.3f, want 0.420", state.Erasure)
	}
	if state.ObservedSamples != 5000 {
		t.Fatalf("retained observations = %.0f, want 5000", state.ObservedSamples)
	}
	if state.RoundTrip != 240*time.Millisecond {
		t.Fatalf("retained round trip = %v, want 240ms", state.RoundTrip)
	}
	if m.Members() != 0 {
		t.Fatalf("idle measuring connection still counts as an active member")
	}

	// Retained knowledge is a handoff, not a permanent verdict. Once a new
	// connection supplies trusted evidence, it becomes the path's knowledge.
	changed := m.Report(2, Observation{Erasure: 0.55, BurstFactor: 1, ObservedSamples: 5000, RoundTrip: 300 * time.Millisecond})
	if changed.Erasure != 0.55 {
		t.Fatalf("new generation erasure = %.3f, want 0.550", changed.Erasure)
	}
}

// A lane that stops sending stops counting, or its share is held against the
// lanes that are still working. There is no close hook on a congestion
// controller, so membership has to expire.
func TestAnIdleLaneStopsCounting(t *testing.T) {
	m := NewPathModel()
	m.Report(1, Observation{Erasure: 0.42, BurstFactor: 1, ObservedSamples: 5000, RoundTrip: 0})
	m.Report(2, Observation{Erasure: 0.42, BurstFactor: 1, ObservedSamples: 5000, RoundTrip: 0})
	if m.Members() != 2 {
		t.Fatalf("members = %d, want 2", m.Members())
	}

	m.mu.Lock()
	m.members[1].at = time.Now().Add(-2 * memberIdle)
	m.mu.Unlock()

	m.Report(2, Observation{Erasure: 0.42, BurstFactor: 1, ObservedSamples: 5000, RoundTrip: 0})
	if m.Members() != 1 {
		t.Fatalf("members = %d after one went idle, want 1", m.Members())
	}
}

// Lanes to the same peer share; lanes to different peers must not, or one
// path's bottleneck would cap the other's.
func TestSharedPathIsPerPeer(t *testing.T) {
	a, b := Shared("peer-a"), Shared("peer-b")
	if a == b {
		t.Fatal("different peers were given the same model")
	}
	if again := Shared("peer-a"); again != a {
		t.Fatal("the same peer was given two models")
	}
}

// What a path measured outlives the connection that measured it, but not for
// ever. A gateway keys these by client address for the life of its process, so
// a client back hours later was seeded with the erasure and the minimum round
// trip of its last visit -- the round trip a floor that only moves down.
func TestPathKnowledgeExpiresWhenNothingHasReportedForLong(t *testing.T) {
	now := time.Now()
	m := NewPathModel()
	m.now = func() time.Time { return now }
	m.Report(1, Observation{Erasure: 0.3, BurstFactor: 1, ObservedSamples: 5000, RoundTrip: 40 * time.Millisecond})

	// A minute later the member has gone idle, but the path is remembered.
	now = now.Add(time.Minute)
	if got := m.Current(); got.Erasure != 0.3 || got.RoundTrip != 40*time.Millisecond {
		t.Fatalf("a path idle for a minute forgot its measurements: %+v", got)
	}
	// Long after, it is not.
	now = now.Add(11 * time.Minute)
	if got := m.Current(); got.Erasure != 0 || got.RoundTrip != 0 || got.ObservedSamples != 0 {
		t.Fatalf("a path nothing has reported on for %v still answers %+v", knowledgeLifetime, got)
	}
	// And a new report starts from what it measures, not from the old floor.
	got := m.Report(2, Observation{Erasure: 0.05, BurstFactor: 1, ObservedSamples: 200, RoundTrip: 210 * time.Millisecond})
	if got.RoundTrip != 210*time.Millisecond {
		t.Fatalf("a fresh report on an expired path got round trip %v, want its own 210ms", got.RoundTrip)
	}
}

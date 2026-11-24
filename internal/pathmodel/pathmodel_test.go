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
	m.Report(1, Observation{Erasure: 0.42, BurstFactor: 1, ObservedSamples: 5000, Delivered: 1e6, RoundTrip: 0})
	m.Report(2, Observation{Erasure: 0.42, BurstFactor: 1, ObservedSamples: 5000, Delivered: 1e6, RoundTrip: 0})
	m.Report(3, Observation{Erasure: 0.42, BurstFactor: 1, ObservedSamples: 5000, Delivered: 1e6, RoundTrip: 0})
	erasure := m.Report(4, Observation{Erasure: 0.05, BurstFactor: 1, ObservedSamples: 20, Delivered: 1e6}).Erasure

	if math.Abs(erasure-0.42) > 0.01 {
		t.Fatalf("pooled erasure %.3f, want the weight of the measured lanes at about 0.42", erasure)
	}
	// And the new lane is handed that estimate rather than its own, so it does
	// not have to rediscover the path.
	if erasure < 0.4 {
		t.Fatalf("a joining lane was left with its own uninformed erasure of %.3f", erasure)
	}
}

// The share is what stops lanes compounding: each is capped at the endpoint
// pair's bottleneck divided by the number of lanes, so the aggregate on the
// wire is what one sender would have put there.
func TestTheShareDividesTheBottleneck(t *testing.T) {
	m := NewPathModel()
	const perLane = 1e6 // bytes per second
	m.Report(1, Observation{Erasure: 0.42, BurstFactor: 1, ObservedSamples: 5000, Delivered: perLane, RoundTrip: 0})
	m.Report(2, Observation{Erasure: 0.42, BurstFactor: 1, ObservedSamples: 5000, Delivered: perLane, RoundTrip: 0})
	m.Report(3, Observation{Erasure: 0.42, BurstFactor: 1, ObservedSamples: 5000, Delivered: perLane, RoundTrip: 0})
	share := m.Report(4, Observation{Erasure: 0.42, BurstFactor: 1, ObservedSamples: 5000, Delivered: perLane, RoundTrip: 0}).Share

	if m.Members() != 4 {
		t.Fatalf("model counts %d lanes, want 4", m.Members())
	}
	// The aggregate is 4 MB/s over four lanes, and each share carries the
	// probe gain, because a cap with no headroom is a cap computed from its
	// own effect.
	want := shareProbeGain * perLane
	if math.Abs(share-want) > want*0.05 {
		t.Fatalf("share %.0f of a 4 MB/s aggregate over 4 lanes, want about %.0f", share, want)
	}
	// Four lanes each capped at their share put on the wire what one BBR
	// sender probing on its own would have.
	if total := share * 4; math.Abs(total-shareProbeGain*4*perLane) > perLane*0.2 {
		t.Fatalf("four shares total %.0f, want %.0f", total, shareProbeGain*4*perLane)
	}
}

// A lane on its own must not be capped by a bottleneck nobody has measured.
func TestAnUnknownBottleneckDoesNotCap(t *testing.T) {
	m := NewPathModel()
	if share := m.Report(1, Observation{Erasure: 0, BurstFactor: 1, ObservedSamples: 0, Delivered: 0, RoundTrip: 0}).Share; share != 0 {
		t.Fatalf("share %.0f before any delivered rate was reported, want 0 for no cap", share)
	}
}

func TestObservationProgressDoesNotWeightAnUntrustedFloor(t *testing.T) {
	m := NewPathModel()
	m.Report(1, Observation{Erasure: 0.42, BurstFactor: 1, ObservedSamples: 5000, Delivered: 1e6, RoundTrip: 0})
	state := m.Report(2, Observation{Erasure: 0, BurstFactor: 1, ObservedSamples: 120, Delivered: 0, RoundTrip: 0})
	if state.ObservedSamples != 5120 {
		t.Fatalf("observed samples = %.0f, want 5120", state.ObservedSamples)
	}
	if math.Abs(state.Erasure-0.42) > 0.02 {
		t.Fatalf("a contributor with few samples diluted the pooled erasure to %.3f", state.Erasure)
	}
}

func TestPathKnowledgeOutlivesTheConnectionThatMeasuredIt(t *testing.T) {
	m := NewPathModel()
	m.Report(1, Observation{Erasure: 0.42, BurstFactor: 1, ObservedSamples: 5000, Delivered: 1e6, RoundTrip: 240 * time.Millisecond})

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
	changed := m.Report(2, Observation{Erasure: 0.55, BurstFactor: 1, ObservedSamples: 5000, Delivered: 1e6, RoundTrip: 300 * time.Millisecond})
	if changed.Erasure != 0.55 {
		t.Fatalf("new generation erasure = %.3f, want 0.550", changed.Erasure)
	}
}

// A lane that stops sending stops counting, or its share is held against the
// lanes that are still working. There is no close hook on a congestion
// controller, so membership has to expire.
func TestAnIdleLaneStopsCounting(t *testing.T) {
	m := NewPathModel()
	m.Report(1, Observation{Erasure: 0.42, BurstFactor: 1, ObservedSamples: 5000, Delivered: 1e6, RoundTrip: 0})
	m.Report(2, Observation{Erasure: 0.42, BurstFactor: 1, ObservedSamples: 5000, Delivered: 1e6, RoundTrip: 0})
	if m.Members() != 2 {
		t.Fatalf("members = %d, want 2", m.Members())
	}

	m.mu.Lock()
	m.members[1].at = time.Now().Add(-2 * memberIdle)
	m.mu.Unlock()

	state := m.Report(2, Observation{Erasure: 0.42, BurstFactor: 1, ObservedSamples: 5000, Delivered: 1e6, RoundTrip: 0})
	if m.Members() != 1 {
		t.Fatalf("members = %d after one went idle, want 1", m.Members())
	}
	// The remaining lane has the whole bottleneck rather than half, and being
	// alone it is no longer capped at all.
	if state.Seed < 1.5e6 {
		t.Fatalf("seed %.0f for the only live lane, want the whole aggregate", state.Seed)
	}
	if state.Share != 0 {
		t.Fatalf("share %.0f for a lane that is now alone, want no cap", state.Share)
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

// A shared controller must still work when it is the only lane, because that
// is the common case and the one measured fastest live.
func TestASingleSharedLaneIsNotPenalised(t *testing.T) {
	m := NewPathModel()
	const rate = 2e6
	state := m.Report(1, Observation{Erasure: 0.42, BurstFactor: 1, ObservedSamples: 5000, Delivered: rate, RoundTrip: 0})
	// Not capped at its own rate: not capped at all. A lone lane has nothing
	// to compound with, and a ceiling equal to what it has already delivered
	// is one it can never probe past.
	if state.Share != 0 {
		t.Fatalf("a lone lane was capped at %.0f, want no cap", state.Share)
	}
	// It still learns the path, so a lane replacing it can start from what it
	// measured rather than from the initial window.
	if state.Seed < rate*0.95 {
		t.Fatalf("seed %.0f from a lane delivering %.0f, want about the same", state.Seed, rate)
	}
}

// The share is measured from what the members deliver, and the members deliver
// what the share allows. Without headroom that is a loop whose only stable
// point is the rate it started at, and whose every disturbance moves it down:
// one application-limited interval lowers the windowed maximum, which lowers
// the share, which lowers what can be delivered, and nothing ever measures the
// path upward again.
//
// Live, that was a transport that ran at 143 Mbit/s for six 20-second windows
// and then moved no bytes at all until its process was restarted
// (docs/archive/2026-08-development/MEASUREMENTS-20260816.md). On the emulator it is one trial at 37
// Mbit/s followed by four at 0.8.
func TestTheShareDoesNotRatchetDown(t *testing.T) {
	const capacity = 8e6 // what the path would actually carry, bytes/second
	for _, lanes := range []int{1, 2, 4} {
		t.Run("", func(t *testing.T) {
			m := NewPathModel()
			// Each lane delivers what its own cap allows, which is the loop
			// the real controller runs: bandwidth() takes the smaller of its
			// estimate and its share.
			delivered := make([]float64, lanes)
			for i := range delivered {
				delivered[i] = capacity / float64(lanes)
			}
			// One application-limited interval: every lane briefly delivers a
			// tenth of what it could. This is a gap between requests, not a
			// property of the path.
			for i := range delivered {
				delivered[i] /= 10
			}
			var share float64
			for round := 0; round < 40; round++ {
				for i := 0; i < lanes; i++ {
					share = m.Report(Member(i+1), Observation{Erasure: 0.1, BurstFactor: 1, ObservedSamples: 5000, Delivered: delivered[i], RoundTrip: 0}).Share
					// The next round delivers what this one is allowed to,
					// bounded by the path itself.
					next := capacity / float64(lanes)
					if share > 0 && share < next {
						next = share
					}
					delivered[i] = next
				}
			}
			total := 0.0
			for _, d := range delivered {
				total += d
			}
			if total < capacity*0.9 {
				t.Fatalf("%d lane(s) settled at %.0f B/s after one idle interval, want the path's %.0f",
					lanes, total, capacity)
			}
		})
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
	m.Report(1, Observation{Erasure: 0.3, BurstFactor: 1, ObservedSamples: 5000, Delivered: 1e6, RoundTrip: 40 * time.Millisecond})

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
	got := m.Report(2, Observation{Erasure: 0.05, BurstFactor: 1, ObservedSamples: 200, Delivered: 1e6, RoundTrip: 210 * time.Millisecond})
	if got.RoundTrip != 210*time.Millisecond {
		t.Fatalf("a fresh report on an expired path got round trip %v, want its own 210ms", got.RoundTrip)
	}
}

// A member that has measured no delivery yet reports zero. It still counts as
// a member, so it takes its place in the split, but it must neither lower the
// bottleneck the others measured nor be able to produce one on its own.
func TestAZeroDeliveryReportCountsAsAMemberAndNothingMore(t *testing.T) {
	m := NewPathModel()
	if got := m.Report(1, Observation{Delivered: 0}); got.Seed != 0 || got.Share != 0 {
		t.Fatalf("a model with only unmeasured members produced seed %.0f, share %.0f", got.Seed, got.Share)
	}
	m.Report(2, Observation{Delivered: 2e6})
	got := m.Report(1, Observation{Delivered: 0})
	if got.Seed != 1e6 {
		t.Fatalf("seed %.0f, want the measured 2e6 split two ways", got.Seed)
	}
}

// The earned compensation is pooled by sample count, ignores members that
// have not decided, and outlives its members as path knowledge.
func TestArrivalIsPooledBySamplesAndRemembered(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	m := NewPathModel()
	m.now = func() time.Time { return now }
	m.Report(1, Observation{ObservedSamples: 3000, Arrival: 0.6})
	m.Report(2, Observation{ObservedSamples: 1000, Arrival: 1})
	m.Report(3, Observation{ObservedSamples: 5000})
	if got := m.Current().Arrival; got < 0.699 || got > 0.701 {
		t.Fatalf("pooled arrival %.3f, want 0.7", got)
	}
	now = now.Add(time.Minute)
	if got := m.Current().Arrival; got < 0.699 || got > 0.701 {
		t.Fatalf("arrival after the members went quiet %.3f, want the remembered 0.7", got)
	}
	now = now.Add(11 * time.Minute)
	if got := m.Current().Arrival; got != 0 {
		t.Fatalf("arrival %.3f survived knowledgeLifetime", got)
	}
}

// The samples behind the erasure are the ones behind it now, not the most the
// path ever observed: those may belong to a member that has expired.
func TestErasureSamplesAreTheOnesBehindTheCurrentEstimate(t *testing.T) {
	now := time.Now()
	m := NewPathModelClocked(func() time.Time { return now })
	m.Report(1, Observation{Erasure: 0.1, BurstFactor: 1, ObservedSamples: 5000, Delivered: 1e6})
	now = now.Add(2 * memberIdle)
	state := m.Report(2, Observation{Erasure: 0, BurstFactor: 1, ObservedSamples: 10, Delivered: 1e6})
	if state.ObservedSamples != 5000 || state.ErasureSamples != 10 {
		t.Fatalf("observed %.0f, erasure samples %.0f; want 5000 and 10", state.ObservedSamples, state.ErasureSamples)
	}
	if got := m.Members(); got != 1 {
		t.Fatalf("Members reads the model's clock: %d, want 1", got)
	}
}

// An estimator that forgets rests its figure on fewer outcomes than it has
// ever decided; the pool reports the former.
func TestErasureSamplesAreTheEstimatorsEffectiveCount(t *testing.T) {
	m := NewPathModel()
	state := m.Report(1, Observation{Erasure: 0.15, BurstFactor: 1, ObservedSamples: 2e6, ErasureSamples: 8000, Delivered: 1e6})
	if state.ErasureSamples != 8000 || state.ObservedSamples != 2e6 {
		t.Fatalf("erasure samples %.0f, observed %.0f; want 8000 and 2000000", state.ErasureSamples, state.ObservedSamples)
	}
}

// An old lane that dominates the pooled erasure's weights is not made more
// precise by new lanes' samples beside it.
func TestErasureSamplesFollowTheWeightsBehindTheEstimate(t *testing.T) {
	m := NewPathModel()
	m.Report(1, Observation{Erasure: 0.15, BurstFactor: 1, ObservedSamples: 1e6, ErasureSamples: 100, Delivered: 1e6})
	state := m.Report(2, Observation{Erasure: 0.15, BurstFactor: 1, ObservedSamples: 8000, ErasureSamples: 8000, Delivered: 1e6})
	if state.ErasureSamples > 200 {
		t.Fatalf("an estimate weighted 99%% by a lane resting on 100 samples claims %.0f", state.ErasureSamples)
	}
}

// A lane beside members that are not using the path has nothing to compound
// with, and is not capped: the last download of a burst, beside the pooled
// control connection, was held to 5/8 of what it delivered and then 5/8 of
// that. Two busy lanes are capped as before, idle member and all.
func TestALaneBesideIdleMembersIsNotCapped(t *testing.T) {
	now := time.Now()
	m := NewPathModelClocked(func() time.Time { return now })
	m.Report(1, Observation{Delivered: 60e3, ObservedSamples: 100})
	state := m.Report(2, Observation{Delivered: 8e6, ObservedSamples: 1000})
	if state.Share != 0 {
		t.Fatalf("one busy lane beside an idle member is capped at %.0f", state.Share)
	}
	state = m.Report(3, Observation{Delivered: 8e6, ObservedSamples: 1000})
	if want := shareProbeGain * 16.06e6 / 3; math.Abs(state.Share-want) > 1e3 {
		t.Fatalf("two busy lanes and an idle member: share %.0f, want the even split's %.0f", state.Share, want)
	}
	// A joiner beside one busy lane takes a share: it will be the second.
	if got := m.Current(); got.Share == 0 {
		t.Fatal("a lane joining a busy one is not capped")
	}
}

// A lane that stops reporting stops being busy within a few round trips, so
// the lane left behind is uncapped long before its siblings expire; the split
// itself still counts them until then.
func TestAFinishedLaneStopsCountingAsBusy(t *testing.T) {
	now := time.Now()
	m := NewPathModelClocked(func() time.Time { return now })
	m.Report(1, Observation{Delivered: 6e6, ObservedSamples: 1000, RoundTrip: 150 * time.Millisecond})
	if state := m.Report(2, Observation{Delivered: 6e6, ObservedSamples: 1000, RoundTrip: 150 * time.Millisecond}); state.Share == 0 {
		t.Fatal("two busy lanes are not capped")
	}
	now = now.Add(time.Second)
	state := m.Report(2, Observation{Delivered: 6e6, ObservedSamples: 1000, RoundTrip: 150 * time.Millisecond})
	if state.Share != 0 {
		t.Fatalf("a second after its sibling stopped reporting, the lane is capped at %.0f", state.Share)
	}
	if want := 12e6 / 2; state.Seed != want {
		t.Fatalf("the split stopped counting the quiet lane before it expired: seed %.0f, want %.0f", state.Seed, want)
	}
	if got := m.Members(); got != 2 {
		t.Fatalf("a quiet lane expired after a second: %d members", got)
	}
}

// The pooled erasure carries the weight it was averaged by, so a member can
// take its own report back out of it.
func TestErasureWeightIsTheObservationsBehindIt(t *testing.T) {
	m := NewPathModel()
	m.Report(1, Observation{Erasure: 0.9, BurstFactor: 1, ObservedSamples: 1000, Delivered: 1e6})
	state := m.Report(2, Observation{Erasure: 0, BurstFactor: 1, ObservedSamples: 200, Delivered: 1e6})
	if state.ErasureWeight != 1200 {
		t.Fatalf("erasure weight %.0f, want 1200", state.ErasureWeight)
	}
	if others := (state.Erasure*state.ErasureWeight - 0.9*1000) / (state.ErasureWeight - 1000); math.Abs(others) > 1e-9 {
		t.Fatalf("with member 1 taken out, the erasure is %.3f, want member 2's 0", others)
	}
}

// Each member is judged active by its own round trip: a long-haul lane
// acknowledged once a round trip is not gone between acknowledgements
// because a short-haul member sets the node's minimum.
func TestALongHaulMemberIsActiveByItsOwnRoundTrip(t *testing.T) {
	now := time.Now()
	m := NewPathModelClocked(func() time.Time { return now })
	m.Report(1, Observation{Delivered: 6e6, ObservedSamples: 1000, RoundTrip: 800 * time.Millisecond})
	now = now.Add(600 * time.Millisecond)
	state := m.Report(2, Observation{Delivered: 6e6, ObservedSamples: 1000, RoundTrip: 30 * time.Millisecond})
	if state.Share == 0 {
		t.Fatal("a long-haul lane 600 ms into its 800 ms round trip was taken for gone")
	}
}

// A shared drop in capacity is not every sibling leaving: lanes that all
// deliver a fifth of the bottleneck's peak, reporting each round, are still
// busy and still capped.
func TestASharedCapacityDropKeepsTheCap(t *testing.T) {
	now := time.Now()
	m := NewPathModelClocked(func() time.Time { return now })
	m.Report(1, Observation{Delivered: 10e6, ObservedSamples: 1000, RoundTrip: 150 * time.Millisecond})
	m.Report(2, Observation{Delivered: 10e6, ObservedSamples: 1000, RoundTrip: 150 * time.Millisecond})
	now = now.Add(100 * time.Millisecond)
	m.Report(1, Observation{Delivered: 2e6, ObservedSamples: 1000, RoundTrip: 150 * time.Millisecond})
	state := m.Report(2, Observation{Delivered: 2e6, ObservedSamples: 1000, RoundTrip: 150 * time.Millisecond})
	if state.Share == 0 {
		t.Fatal("both lanes cut to a fifth of the peak, still reporting, and the cap came off")
	}
}

// Every stall rescue leaves a lane behind that carried a few chunks and stays
// connected. One idle member keeps its split, as the brake against summed
// peaks; more do not take a share of a path they are not using.
func TestIdleRescueLanesDoNotDiluteTheShare(t *testing.T) {
	now := time.Now()
	m := NewPathModelClocked(func() time.Time { return now })
	for member := Member(10); member < 15; member++ {
		m.Report(member, Observation{Delivered: 60e3, ObservedSamples: 100, RoundTrip: 200 * time.Millisecond})
	}
	var state State
	for member := Member(1); member <= 4; member++ {
		state = m.Report(member, Observation{Delivered: 6e6, ObservedSamples: 1000, RoundTrip: 200 * time.Millisecond})
	}
	bottleneck := 4*6e6 + 5*60e3
	if want := shareProbeGain * bottleneck / 5; math.Abs(state.Share-want) > 1 {
		t.Fatalf("four busy lanes beside five idle ones: share %.0f, want %.0f (busy plus one idle)", state.Share, want)
	}
	if want := bottleneck / 9; math.Abs(state.Seed-want) > 1 {
		t.Fatalf("the seed %.0f no longer splits over every member (%.0f)", state.Seed, want)
	}
}

// The bottleneck sample is what the members deliver now. A member that has
// stopped reporting keeps its last rate until it expires, and summed beside
// the lanes still sending it is a peak nobody delivers.
func TestAQuietMembersLastRateIsNotSummedIntoTheBottleneck(t *testing.T) {
	now := time.Now()
	m := NewPathModelClocked(func() time.Time { return now })
	m.Report(1, Observation{Delivered: 10e6, ObservedSamples: 1000, RoundTrip: 100 * time.Millisecond})
	m.Report(2, Observation{Delivered: 2e6, ObservedSamples: 1000, RoundTrip: 100 * time.Millisecond})
	var state State
	for step := 0; step < 23; step++ {
		now = now.Add(500 * time.Millisecond)
		state = m.Report(2, Observation{Delivered: 2e6, ObservedSamples: 1000, RoundTrip: 100 * time.Millisecond})
	}
	// Eleven and a half seconds on: the samples taken while member 1 was still
	// sending have aged out of the window, and those taken since did not
	// count it.
	if state.Seed > 2e6+1 {
		t.Fatalf("the bottleneck still holds a quiet member's rate: seed %.0f, want 2000000", state.Seed)
	}
}

// Only the sole busy member goes uncapped. A lane beside it that has slowed
// keeps a share: uncapped, it would read the vanished cap as being left alone
// and ramp into a path its sibling still fills.
func TestOnlyTheSoleBusyMemberIsUncapped(t *testing.T) {
	now := time.Now()
	m := NewPathModelClocked(func() time.Time { return now })
	m.Report(1, Observation{Delivered: 6e6, ObservedSamples: 1000, RoundTrip: 200 * time.Millisecond})
	m.Report(2, Observation{Delivered: 6e6, ObservedSamples: 1000, RoundTrip: 200 * time.Millisecond})
	slowed := m.Report(2, Observation{Delivered: 100e3, ObservedSamples: 1000, RoundTrip: 200 * time.Millisecond})
	if slowed.Share == 0 {
		t.Fatal("a lane that slowed beside a busy sibling lost its cap")
	}
	if busy := m.Report(1, Observation{Delivered: 6e6, ObservedSamples: 1000, RoundTrip: 200 * time.Millisecond}); busy.Share != 0 {
		t.Fatalf("the only busy member is capped at %.0f", busy.Share)
	}
}

// A lane about to join takes the same split the members report: the coded
// path sizes its window from Current's share, and a share that still divided
// by every idle rescue lane would size it for a policy the lanes no longer
// follow.
func TestAJoinersShareFollowsTheBoundedSplit(t *testing.T) {
	now := time.Now()
	m := NewPathModelClocked(func() time.Time { return now })
	for member := Member(10); member < 15; member++ {
		m.Report(member, Observation{Delivered: 60e3, ObservedSamples: 100, RoundTrip: 200 * time.Millisecond})
	}
	for member := Member(1); member <= 4; member++ {
		m.Report(member, Observation{Delivered: 6e6, ObservedSamples: 1000, RoundTrip: 200 * time.Millisecond})
	}
	bottleneck := 4*6e6 + 5*60e3
	state := m.Current()
	if want := shareProbeGain * bottleneck / 6; math.Abs(state.Share-want) > 1 {
		t.Fatalf("a joiner beside four busy lanes and five idle ones: share %.0f, want %.0f (five busy with it, plus one idle)", state.Share, want)
	}
	if want := bottleneck / 10; math.Abs(state.Seed-want) > 1 {
		t.Fatalf("seed %.0f, want the split over every member, %.0f", state.Seed, want)
	}
}

// Whether a lane is busy is judged against what the members reporting now
// deliver, between them: rescue lanes that have gone silent but not yet
// expired do not lower the bar until the control connection clears it.
func TestSilentRescueLanesDoNotMakeTheControlLaneBusy(t *testing.T) {
	now := time.Now()
	m := NewPathModelClocked(func() time.Time { return now })
	for member := Member(10); member < 18; member++ {
		m.Report(member, Observation{Delivered: 0, ObservedSamples: 100, RoundTrip: 200 * time.Millisecond})
	}
	now = now.Add(2 * time.Second)
	m.Report(2, Observation{Delivered: 40e3, ObservedSamples: 100, RoundTrip: 200 * time.Millisecond})
	state := m.Report(1, Observation{Delivered: 1e6, ObservedSamples: 1000, RoundTrip: 200 * time.Millisecond})
	if state.Share != 0 {
		t.Fatalf("the only busy lane beside a control lane and eight silent rescue lanes is capped at %.0f", state.Share)
	}
}

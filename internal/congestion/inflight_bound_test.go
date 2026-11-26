package congestion

import (
	"math/rand"
	"testing"
	"time"

	quiccongestion "github.com/apernet/quic-go/congestion"
	"github.com/apernet/quic-go/monotime"
)

const (
	boundMSS    = quiccongestion.ByteCount(1200)
	boundMinRTT = 53 * time.Millisecond
	boundBDP    = quiccongestion.ByteCount(2_000_000)
)

// boundRound feeds one round of n packets, lost of them lost, at a queue,
// and returns whether it closed the previous round as congestion.
func boundRound(b *inflightBound, now monotime.Time, round uint64, n, lost int, queue time.Duration, inFlight quiccongestion.ByteCount) bool {
	congested := b.observe(now, round, 0, 0, queue, boundMinRTT, inFlight, boundMSS, 2*boundBDP, boundBDP, false)
	b.observe(now, round, n-lost, lost, queue, boundMinRTT, inFlight, boundMSS, 2*boundBDP, boundBDP, false)
	return congested
}

// A clean path whose shared buffer overflows: the bound cuts to 0.7 of what
// was in flight, holds through the probe wait, then grows until the law's
// window, where it stays until the next probe.
func TestInflightBoundCutsAnOverflowingQueueAndHoldsThenProbes(t *testing.T) {
	b := &inflightBound{}
	now := base()
	round := uint64(0)
	next := func(n, lost int, queue time.Duration, flight quiccongestion.ByteCount) bool {
		round++
		now = now.Add(boundMinRTT + queue)
		return boundRound(b, now, round, n, lost, queue, flight)
	}
	for range 10 {
		next(2000, 0, 2*time.Millisecond, boundBDP)
	}
	for range 5 {
		next(2000, 0, 55*time.Millisecond, 2*boundBDP)
	}
	if b.hi != 0 {
		t.Fatalf("a full queue without loss set a bound of %d", b.hi)
	}
	next(2000, 300, 55*time.Millisecond, 2*boundBDP) // 15% loss at a full queue
	if !next(2000, 0, 55*time.Millisecond, 2*boundBDP) {
		t.Fatal("15% loss at a full queue on a clean path was not judged congestion")
	}
	want := quiccongestion.ByteCount(inflightBeta * float64(2*boundBDP))
	if b.hi != want {
		t.Fatalf("bound = %d after the cut, want 0.7 of the flight, %d", b.hi, want)
	}
	held := b.hi
	until := now.Add(inflightProbeWait)
	for now.Add(boundMinRTT + 5*time.Millisecond).Before(until) {
		next(2000, 0, 5*time.Millisecond, held)
	}
	if b.hi != held {
		t.Fatalf("the bound moved from %d to %d inside the probe wait", held, b.hi)
	}
	for range 200 {
		next(2000, 0, 5*time.Millisecond, b.limit(2*boundBDP))
		if b.hi == 2*boundBDP {
			return
		}
	}
	t.Fatalf("the bound never grew back to the law's window after the probe wait; it stands at %d", b.hi)
}

// A channel that erases at random loses the same fraction at any queue: the
// base learns it from the low-queue rounds, and no round at a full queue is
// congestion, however lossy.
func TestInflightBoundIgnoresRandomErasureAtAnyQueue(t *testing.T) {
	for i, erasure := range []float64{0.01, 0.03, 0.03, 0.1, 0.2, 0.42} {
		b := &inflightBound{}
		rng := rand.New(rand.NewSource(int64(7 + i)))
		now := base()
		for round := uint64(1); round <= 400; round++ {
			// Rounds of 300 packets, a 25 Mbit/s path's worth: the noisiest
			// case the threshold has to stand above.
			const n = 300
			lost := 0
			for range n {
				if rng.Float64() < erasure {
					lost++
				}
			}
			queue := time.Duration(rng.Int63n(int64(55 * time.Millisecond)))
			now = now.Add(boundMinRTT + queue)
			prevLost, prevN, prevQ, prevBase := b.lost, b.acked+b.lost, b.peakQueue, b.base
			if boundRound(b, now, round, n, lost, queue, boundBDP) {
				t.Logf("judged round: %d/%d lost at peak queue %v, deepest %v, base before %.3f", prevLost, prevN, prevQ, b.deepest, prevBase)
				t.Fatalf("erasure %.2f: round %d with %d/%d lost at a %v queue was judged congestion (base %.3f)",
					erasure, round, lost, n, queue, b.base)
			}
		}
		if got, ok := b.erasure(); !ok || got < erasure*0.8 || got > erasure*1.2 {
			t.Fatalf("erasure %.2f: learned base %.3f (ok %v)", erasure, got, ok)
		}
	}
}

// A policer drops with no queue: nothing it does is near a full queue, so
// nothing it does sets the bound.
func TestInflightBoundLeavesAPolicerToTheCompensationCheck(t *testing.T) {
	b := &inflightBound{}
	now := base()
	for round := uint64(1); round <= 200; round++ {
		now = now.Add(boundMinRTT)
		if boundRound(b, now, round, 2000, 600, time.Millisecond, boundBDP) {
			t.Fatalf("round %d of policer loss with no queue was judged congestion", round)
		}
	}
	if b.hi != 0 {
		t.Fatalf("a policer set a bound of %d", b.hi)
	}
}

// boundWarm teaches b the erasure base on an empty queue and then shows it
// the path's full queue without loss, so that later rounds can be judged.
func boundWarm(b *inflightBound, now *monotime.Time, round *uint64, n, lost int) {
	for range 10 {
		*round++
		*now = now.Add(boundMinRTT)
		boundRound(b, *now, *round, n, lost, time.Millisecond, boundBDP)
	}
}

// A sender holding exactly the pipe and the buffer loses a fraction of a
// percent to the overflow, far under two percent: one such round is not
// congestion, a queue that has stood full through three of them is.
func TestInflightBoundCutsAStandingFullQueueAtLittleLoss(t *testing.T) {
	b := &inflightBound{}
	now, round := base(), uint64(0)
	boundWarm(b, &now, &round, 2000, 0)
	cutAt := 0
	for i := 1; i <= 6 && cutAt == 0; i++ {
		round++
		now = now.Add(2 * boundMinRTT)
		// 0.5% lost: ten of 2000.
		if boundRound(b, now, round, 2000, 10, 55*time.Millisecond, 2*boundBDP) {
			cutAt = i - 1 // the round closed is the one before
		}
	}
	if cutAt != inflightStandingRounds {
		t.Fatalf("0.5%% loss on a standing full queue cut after %d rounds, want %d", cutAt, inflightStandingRounds)
	}
	if want := quiccongestion.ByteCount(inflightBeta * float64(2*boundBDP)); b.hi != want {
		t.Fatalf("bound = %d, want 0.7 of the flight, %d", b.hi, want)
	}
}

// One reduction per round trip: what is lost in the rounds after a cut was
// sent before it.
func TestInflightBoundCutsOncePerRoundTrip(t *testing.T) {
	b := &inflightBound{}
	now, round := base(), uint64(0)
	boundWarm(b, &now, &round, 2000, 0)
	cuts := 0
	for range 2 + inflightSettleRounds {
		round++
		now = now.Add(2 * boundMinRTT)
		if boundRound(b, now, round, 2000, 300, 55*time.Millisecond, 2*boundBDP) {
			cuts++
		}
	}
	if cuts != 1 {
		t.Fatalf("%d cuts in the %d rounds after the first lossy one, want 1", cuts, 1+inflightSettleRounds)
	}
	if want := quiccongestion.ByteCount(inflightBeta * float64(2*boundBDP)); b.hi != want {
		t.Fatalf("bound = %d after one episode, want %d", b.hi, want)
	}
}

// ProbeRTT empties the flight on purpose. The losses decided inside it were
// sent before, the smoothed round trip is still the old queue's, and the
// flight is four packets: a cut taken there set the bound to four packets and
// held it for the probe wait, which is how two path authorities sharing a
// link fell from 275 to 97 Mbit/s between them.
func TestInflightBoundDoesNotJudgeADrainedFlight(t *testing.T) {
	b := &inflightBound{}
	now, round := base(), uint64(0)
	boundWarm(b, &now, &round, 2000, 0)
	round++
	now = now.Add(2 * boundMinRTT)
	boundRound(b, now, round, 2000, 0, 55*time.Millisecond, 2*boundBDP)
	for range 4 {
		round++
		now = now.Add(2 * boundMinRTT)
		// Ten acknowledged and sixty late losses at a round trip not yet
		// re-measured, with four packets in flight.
		if b.observe(now, round, 0, 0, 55*time.Millisecond, boundMinRTT, 4*boundMSS, boundMSS, 2*boundBDP, boundBDP, true) {
			t.Fatal("a round of ProbeRTT was judged congestion")
		}
		b.observe(now, round, 10, 60, 55*time.Millisecond, boundMinRTT, 4*boundMSS, boundMSS, 2*boundBDP, boundBDP, true)
	}
	// The rounds straight after it are not judged either.
	round++
	if boundRound(b, now.Add(boundMinRTT), round, 2000, 0, time.Millisecond, boundBDP) || b.hi != 0 {
		t.Fatalf("ProbeRTT's late losses set a bound of %d", b.hi)
	}
}

// A cut never takes the bound below 0.7 of one product of the sender's own
// estimate, whatever was in flight when it was judged.
func TestInflightBoundIsFlooredAtItsOwnProduct(t *testing.T) {
	b := &inflightBound{}
	now, round := base(), uint64(0)
	boundWarm(b, &now, &round, 2000, 0)
	floor := quiccongestion.ByteCount(inflightBeta * float64(boundBDP))
	for range 40 {
		round++
		now = now.Add(2 * boundMinRTT)
		boundRound(b, now, round, 2000, 300, 55*time.Millisecond, boundBDP/10)
	}
	if b.congestionRounds < 2 {
		t.Fatalf("only %d congestion rounds in forty lossy ones", b.congestionRounds)
	}
	if b.hi != floor {
		t.Fatalf("bound = %d after repeated cuts from a small flight, want the floor %d", b.hi, floor)
	}
}

// A probe's bump is a deep queue for part of one round, not a standing one:
// its loss is not judged, however deep the bump.
func TestInflightBoundDoesNotJudgeAQueueThatDoesNotStand(t *testing.T) {
	b := &inflightBound{}
	now, round := base(), uint64(0)
	boundWarm(b, &now, &round, 2000, 0)
	for range 50 {
		round++
		now = now.Add(2 * boundMinRTT)
		congested := b.observe(now, round, 0, 0, 20*time.Millisecond, boundMinRTT, boundBDP, boundMSS, 2*boundBDP, boundBDP, false)
		b.observe(now, round, 1900, 100, 55*time.Millisecond, boundMinRTT, boundBDP, boundMSS, 2*boundBDP, boundBDP, false)
		if congested || b.hi != 0 {
			t.Fatalf("a queue that stood at 20 ms of a 55 ms peak was judged full (bound %d)", b.hi)
		}
	}
}

// The loss bound and the ProbeRTT skip (#210) compose: a path controller
// skips ProbeRTT only for a sender that was application-limited and never at
// its window, and the window its admissions are refused at is the bounded
// one. A bulk sender the bound has cut is held at the bound, so it is
// window-limited, keeps a queue of its own, and still takes every ProbeRTT.
func TestPathControllerHeldAtTheLossBoundStillTakesProbeRTT(t *testing.T) {
	// A clean path with a buffer of half a bandwidth-delay product: startup
	// overflows it, which is the congestion the bound exists for.
	path := pcSimPath{rate: 2e6, rtt: 200 * time.Millisecond, buffer: 2e6 * 0.1, seed: 3}
	const duration = 35 * time.Second
	res, ctl := runPCSim(path, []pcSimLane{{bulk: true}}, duration, 5*time.Second, true)
	law, bound := ctl.law.inner, ctl.law.bound
	rate := res.delivered / (duration - 5*time.Second).Seconds()
	t.Logf("%d congestion rounds, bound %d, %d ProbeRTTs, %d skipped, %.2f of the path's rate",
		bound.congestionRounds, bound.hi, law.probeRTTs, law.probeRTTsSkipped, rate/path.rate)
	if bound.congestionRounds == 0 {
		t.Fatal("the bound never judged the overflowing queue congestion; the test exercises nothing")
	}
	if law.probeRTTs < 3 || law.probeRTTsSkipped != 0 {
		t.Errorf("a bulk sender held at the loss bound took %d ProbeRTTs in %v and skipped %d",
			law.probeRTTs, duration, law.probeRTTsSkipped)
	}
	// ProbeRTT at half a product must still find the path's round trip.
	if law.minRTT > path.rtt*11/10 {
		t.Errorf("minimum round trip %v after three ProbeRTTs on a %v path", law.minRTT, path.rtt)
	}
	if rate < 0.8*path.rate {
		t.Errorf("the bounded sender delivered %.2f of the path's rate", rate/path.rate)
	}
}

package congestion

import (
	"math"
	"time"

	quiccongestion "github.com/apernet/quic-go/congestion"
	"github.com/apernet/quic-go/monotime"
)

// inflightBound is the path authority's loss bound on what it keeps in
// flight: BBRv2's inflight_hi and inflight_lo, with the one change an erasure
// channel needs.
//
// The erasure law answers loss with compensation and congestion with the
// delay bound. Neither works at a shared bottleneck with a buffer of about one
// bandwidth-delay product, which is an ordinary one. The delay bound permits a
// round trip of twice the minimum, so it permits the whole buffer, and the law
// keeps two products in flight, so the buffer is full and overflowing. The
// overflow is then read as erasure: the loss estimator's floor is the lowest
// loss of its last eight 2000-packet rounds -- about half a second at 300
// Mbit/s -- so a queue that overflows in every round raises the floor to the
// overflow rate, and the law compensates for its own tail drops by sending
// more. Measured on an emulated 300 Mbit/s, 53 ms link with a one-product
// buffer, two path authorities held it full with 16 to 24% of packets
// dropped, and one path authority took 99% of the link from a CUBIC
// competitor and 77% from today's per-connection sender.
//
// So loss is split by when it happens rather than by what it looks like. A
// drop-tail queue drops only when it is full, so a loss seen while the path
// holds little queue is the channel's, whatever its pattern, and the erasure
// base is learned from those rounds alone. Loss beyond that base while a queue
// stands near the deepest this path has shown is congestion, by either of two
// tests. One round is congestion when its excess clears BBRv2's two percent
// and three standard errors of its own sampling. And a queue that has stood
// near full through three consecutive rounds is congestion at any excess its
// sampling cannot explain: a flight parked on a full queue is not probing
// anything, and BBRv2 answers any loss outside a probe (inflight_lo). The
// second test is what sees a sender holding exactly the pipe and the buffer,
// where the overflow is a fraction of a percent of its own packets and most
// of what a slower competitor sends.
//
// "Stands" is the round's shallowest queue, not its deepest: one probe's bump
// is deep for a moment and gone, and a standing queue is the one that is
// there through the whole round trip.
//
// On congestion the bound falls to 0.7 of what was in flight, as CUBIC's
// window does, but never below 0.7 of one product of the sender's own
// estimate, as BBRv2's; no further round is judged for two, one reduction per
// round trip; and it is held for a probe wait of two to three seconds before
// growing again, as BBRv2 waits so that a Reno or CUBIC flow sharing the
// queue gets back what it gave up. Growth is then exponential in packets per
// round, as BBRv2's PROBE_UP, until a loss or the law's own window ends the
// probe; the bound then persists, as inflight_hi does, until the next one. A
// first cut also ends startup: a queue this sender has overflowed is a pipe
// it has filled.
//
// A channel that erases at random reads the same loss at any queue depth, so
// its base absorbs it and no round is congestion; a policer drops with no
// queue at all, so nothing it does is near a full one, and the compensation's
// own check that sending harder delivers more remains its brake. A queue
// other traffic holds for good stops being a queue to this sender when its
// minimum round trip is next measured, and its loss is then the channel's.
type inflightBound struct {
	// hi is the bound in bytes, zero while none applies.
	hi quiccongestion.ByteCount
	// step is how much the next clean round may add to hi.
	step quiccongestion.ByteCount
	// holdUntil is when growth may resume after a congestion round.
	holdUntil monotime.Time
	// jitter spreads the probe wait over a second so that senders cut by one
	// overflow do not all probe again in the same round.
	jitter func() time.Duration

	// The round being accumulated.
	round        uint64
	acked, lost  int
	peakQueue    time.Duration // the deepest queue seen in this round
	standing     time.Duration // the shallowest: what stood through all of it
	events       int           // acknowledgement events accounted
	peakInFlight quiccongestion.ByteCount

	// The run of consecutive rounds through which a queue stood near full.
	runRounds         int
	runAcked, runLost int
	runInFlight       quiccongestion.ByteCount
	// settle is how many rounds remain in which the losses still arriving
	// belong to what was sent before the last cut.
	settle int

	// base is the erasure learned from low-queue rounds, valid once
	// baseRounds is positive.
	base       float64
	baseRounds int
	// deepest is the deepest queue recently seen, decayed per round so that
	// one transient does not set "near full" for good.
	deepest time.Duration

	// congestionRounds counts the rounds judged congestion.
	congestionRounds uint64
}

const (
	// inflightLossThreshold is BBRv2's BBRLossThresh: loss in a round above
	// this, beyond what the channel erases, is the queue overflowing.
	inflightLossThreshold = 0.02
	// inflightBeta is the multiplicative decrease on a congestion round,
	// BBRv2's and CUBIC's 0.7.
	inflightBeta = 0.7
	// inflightProbeWait is the least time between a congestion round and the
	// next increase of the bound; inflightProbeJitter is added at random.
	inflightProbeWait   = 2 * time.Second
	inflightProbeJitter = time.Second
	// inflightMinRoundPackets is the fewest decided packets a round needs to
	// be judged: below it one loss is several percent.
	inflightMinRoundPackets = 50
	// inflightBaseWeight is the EWMA weight of one low-queue round in the
	// erasure base.
	inflightBaseWeight = 1.0 / 8
	// inflightNearFull is how close to the deepest recent queue a round's
	// queue must stand to be one in which a drop-tail buffer overflows, and
	// inflightLowQueue how shallow it must be to teach the erasure base.
	inflightNearFull = 0.75
	inflightLowQueue = 0.25
	// inflightNoiseSigmas is how many standard errors of the round's own
	// sampling the excess must also clear: a 300-packet round on a 3%
	// channel loses 22 often enough, across hundreds of rounds, to cut an
	// erasure path at two; an overflowing queue clears any such margin.
	inflightNoiseSigmas = 3
	// inflightBaseEvidence is how many low-queue rounds must have taught the
	// erasure base before any round is judged against it.
	inflightBaseEvidence = 4
	// inflightDeepestDecay is the per-round decay of the deepest queue.
	inflightDeepestDecay = 0.98
	// inflightStandingRounds is how many consecutive rounds a queue must have
	// stood near full for their loss to be judged together, without the two
	// percent: BBRv2 answers any loss outside a probe (inflight_lo), and a
	// flight parked on a full queue is not probing anything.
	inflightStandingRounds = 3
	// inflightSettleRounds is how many rounds after a cut are not judged: one
	// reduction per round trip, as every loss-based sender, since what is lost
	// in them was sent before the cut.
	inflightSettleRounds = 2
)

// observe accounts one acknowledgement event: its acknowledged and lost
// packets, the queue the path is holding now, what is in flight, and the law's
// round counter. It closes the previous round when the counter has moved, and
// returns true when that round was congestion.
func (b *inflightBound) observe(now monotime.Time, round uint64, acked, lost int, queue, minRTT time.Duration, inFlight, mss, window, bdp quiccongestion.ByteCount, drained bool) bool {
	congested := false
	if drained {
		// The sender has emptied its own flight on purpose (ProbeRTT). What
		// is decided now was sent before, the smoothed round trip is that of
		// a queue no longer there, and the flight is four packets: nothing
		// here measures the queue, and a cut from it would be a cut to
		// nothing.
		b.settle = inflightSettleRounds
	}
	if round != b.round {
		congested = b.closeRound(now, minRTT, mss, window, bdp)
		b.round, b.acked, b.lost, b.peakQueue, b.peakInFlight = round, 0, 0, 0, 0
		b.standing, b.events = 0, 0
	}
	b.acked += max(acked, 0)
	b.lost += max(lost, 0)
	b.peakQueue = max(b.peakQueue, queue)
	if b.events == 0 || queue < b.standing {
		b.standing = queue
	}
	b.events++
	b.peakInFlight = max(b.peakInFlight, inFlight)
	return congested
}

func (b *inflightBound) closeRound(now monotime.Time, minRTT time.Duration, mss, window, bdp quiccongestion.ByteCount) bool {
	b.deepest = max(time.Duration(float64(b.deepest)*inflightDeepestDecay), b.peakQueue)
	n := b.acked + b.lost
	if b.acked == 0 && b.lost > 0 {
		// Nothing of the round arrived: the path was away, which is not a
		// queue overflowing -- that still delivers what it does not drop --
		// and not the channel's erasure either. The round teaches nothing,
		// and what is reported lost after it was sent into the same hole.
		b.settle = inflightSettleRounds
		b.endRun()
		return false
	}
	if b.settle > 0 {
		b.settle--
		b.endRun()
		return false
	}
	if n < inflightMinRoundPackets || minRTT <= 0 {
		b.endRun()
		b.grow(now, mss, window)
		return false
	}
	loss := float64(b.lost) / float64(n)
	// A queue is something to judge against only once it is a real fraction
	// of the round trip: a path with none (a policer, an idle link) has no
	// overflow to find, only jitter.
	queued := b.deepest >= minRTT/8
	switch {
	case !queued || b.peakQueue <= time.Duration(float64(b.deepest)*inflightLowQueue):
		if b.baseRounds == 0 {
			b.base = loss
		} else {
			b.base += inflightBaseWeight * (loss - b.base)
		}
		b.baseRounds++
		b.endRun()
	case b.baseRounds >= inflightBaseEvidence &&
		b.standing >= time.Duration(float64(b.deepest)*inflightNearFull):
		// Judged only against a base the channel has shown: until then
		// there is no telling the queue's loss from the channel's, and the
		// law runs as it did without the bound. And only where the queue
		// stood near full through the whole round, which is what a standing
		// queue is: one probe's bump is deep for a moment and gone.
		b.runRounds++
		b.runAcked += b.acked
		b.runLost += b.lost
		b.runInFlight = max(b.runInFlight, b.peakInFlight)
		// One round is congestion past BBRv2's two percent; a queue that has
		// stood full for several is congestion at any loss its sampling
		// cannot explain.
		if b.lost > 0 && loss-b.base > inflightLossThreshold+b.noise(n) {
			b.cut(now, mss, b.peakInFlight, bdp)
			return true
		}
		if run := b.runAcked + b.runLost; b.runRounds >= inflightStandingRounds && b.runLost > 0 &&
			float64(b.runLost)/float64(run)-b.base > b.noise(run) {
			b.cut(now, mss, b.runInFlight, bdp)
			return true
		}
	default:
		b.endRun()
	}
	b.grow(now, mss, window)
	return false
}

// noise is inflightNoiseSigmas standard errors of the loss of n packets
// sampled from the erasure base.
func (b *inflightBound) noise(n int) float64 {
	return inflightNoiseSigmas * math.Sqrt(max(b.base, 1/float64(n))*(1-b.base)/float64(n))
}

func (b *inflightBound) endRun() {
	b.runRounds, b.runAcked, b.runLost, b.runInFlight = 0, 0, 0, 0
}

// cut answers a congestion round.
func (b *inflightBound) cut(now monotime.Time, mss, inFlight, bdp quiccongestion.ByteCount) {
	b.congestionRounds++
	b.settle = inflightSettleRounds
	b.endRun()
	// Never below 0.7 of one bandwidth-delay product of the sender's own
	// estimate, as BBRv2's inflight_hi: below that the bound would be
	// throttling the rate the path has just been measured to deliver, and
	// repeated cuts while someone else holds the queue would walk it to
	// nothing. If the path now delivers less, the estimate says so and the
	// floor follows it.
	floor := quiccongestion.ByteCount(inflightBeta * float64(bdp))
	cut := max(quiccongestion.ByteCount(inflightBeta*float64(inFlight)), floor)
	if b.hi > 0 {
		// Already bounded: the bound falls from where it was, not from a
		// flight that overshot it.
		cut = min(cut, max(quiccongestion.ByteCount(inflightBeta*float64(b.hi)), floor))
	}
	b.hi = max(cut, 4*mss)
	b.step = mss
	b.holdUntil = now.Add(inflightProbeWait)
	if b.jitter != nil {
		b.holdUntil = b.holdUntil.Add(b.jitter())
	}
}

// grow raises the bound after a clean round once the probe wait is over. The
// bound is not lifted when it stops binding, which is what BBRv1 does and what
// left a CUBIC competitor no room: the law's own window is two products of an
// estimate that is, once this sender has taken the link, the whole link. It
// persists, as BBRv2's inflight_hi does, and grows only in a probe, which a
// loss ends; a probe that reaches the law's window without loss ends there.
func (b *inflightBound) grow(now monotime.Time, mss, window quiccongestion.ByteCount) {
	if b.hi == 0 || now.Before(b.holdUntil) {
		return
	}
	if window > 0 && b.hi >= window {
		// The probe found no loss up to the law's own window: stop growing
		// and wait for the next probe, holding the bound where the window is.
		b.hi, b.step = window, mss
		b.holdUntil = now.Add(inflightProbeWait)
		if b.jitter != nil {
			b.holdUntil = b.holdUntil.Add(b.jitter())
		}
		return
	}
	b.hi += b.step
	// Doubling each round, but never more than a quarter of the bound in one:
	// the next round must be able to show whether the last step overflowed.
	b.step = min(2*b.step, max(mss, b.hi/4))
}

// restored is told that the path has returned from an outage. The probe wait
// is there to give a queue-sharing competitor back what a cut took from it;
// after an outage there is no such queue, everyone sharing the path restarts
// together, and a bound still waiting would hold the sender under a path
// that has just come back empty. The bound itself stands -- it is what the
// path last showed it could hold -- and growth resumes at once; the rounds
// that follow carry the outage's late losses and are not judged.
func (b *inflightBound) restored() {
	b.holdUntil = 0
	b.settle = inflightSettleRounds
	b.endRun()
}

// limit is the window the bound allows, or window itself while none applies.
func (b *inflightBound) limit(window quiccongestion.ByteCount) quiccongestion.ByteCount {
	if b.hi > 0 && b.hi < window {
		return b.hi
	}
	return window
}

// erasure is what compensation may make up for: the erasure the channel
// showed at a low queue, never the part a full queue adds. ok is false until
// a low-queue round has been seen.
func (b *inflightBound) erasure() (float64, bool) {
	return b.base, b.baseRounds > 0
}

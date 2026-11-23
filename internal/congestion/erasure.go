package congestion

import (
	"math"
	"sort"
	"sync/atomic"
	"time"

	quiccongestion "github.com/apernet/quic-go/congestion"
	"github.com/apernet/quic-go/monotime"

	"github.com/bojieli/queqiao/internal/lossmodel"
	"github.com/bojieli/queqiao/internal/pathmodel"
)

var nextErasureMemberID atomic.Uint64

// ErasureSender is BBR on a path that erases packets for reasons that have
// nothing to do with congestion.
//
// Measured on the China-US path this project targets, about 42% of packets are
// dropped independently of the sending rate: at 1 Mbit/s as readily as at 12,
// and ICMP loses 37% at five packets a second
// (docs/PATH-CHARACTER-20260813.md). Every loss-responsive controller gives
// the path away. Across the emulated channel, carrying QUIC datagrams:
//
//	default (Reno/Cubic)          0.13 Mbit/s
//	BBR                           0.39
//	BBR-TUIC                      1.36
//	Brutal, told 25 Mbit/s        13.89
//
// Only Brutal gets the path, and only because it ignores loss and paces at a
// rate a human typed in. This controller is meant to reach the same place
// without being told, which needs two corrections rather than one.
//
// The first is that channel loss must not read as congestion. Which loss is
// which is not a policy but a measurement: the erasure floor is the loss that
// does not respond to sending more slowly, and the excess above it is the part
// a sender caused. Only that excess is passed to BBR.
//
// The second is subtler and is why plain BBR collapses rather than merely
// under-shooting. BBR's bandwidth estimate is the rate that is *delivered*,
// while its pacer governs the rate that is *sent*. On a clean path those are
// the same number. On an erasure channel they differ by the arrival rate, and
// pacing a delivered-rate estimate makes the sending rate its own input:
// sending S delivers S(1-p), which becomes the next estimate, which is paced
// as S(1-p), which delivers S(1-p)^2. Every rate is a fixed point of that loop
// only in the sense that zero is; in practice it walks down to nothing, which
// is the 0.39 Mbit/s above. Dividing the pacing rate by the arrival rate
// restores the property BBR assumes, and the loop then converges on the
// bottleneck rather than on zero: in startup S grows by the gain each round
// until delivery stops growing, which happens exactly when S reaches the
// bottleneck.
//
// What it does not do is ignore congestion. Above the bottleneck the loss
// stops being memoryless -- the live path's loss runs grow from 1.7 packets to
// 5.7 -- the excess over the floor becomes positive, and BBR sees it and backs
// off.
type ErasureSender struct {
	inner *TUICBBRSender
	pacer *pacer

	// estimator watches the explicit packet outcomes QUIC reports, ordered by
	// packet number within each congestion event for the burst statistic.
	estimator *lossmodel.Estimator

	outcomes []packetOutcome

	// compensationRound, deliveredAtCompensation and appliedArrival are the
	// probe behind the compensation: what the path was delivering when this
	// sender last agreed to send faster to make up for erasure, and when.
	//
	// Compensating is a bet that the losses are independent of the sending
	// rate, so that sending 1/arrival times as much delivers a full window.
	// The bet is checkable, and it has to be checked: on a path that drops
	// because it is policed rather than because it erases, sending more
	// delivers no more and simply raises the loss, which lowers the arrival
	// rate, which asks for more compensation again.
	compensationRound       uint64
	deliveredAtCompensation float64
	appliedArrival          float64

	// erasure is the pooled measured erasure of the direction this sender
	// sends into, in parts per million. It is what the code is sized from, and
	// it is published because a floor is not a substitute for it: on the live
	// path the floor read a seventh of what the channel was doing.
	erasure atomic.Uint64

	// rttStats is the connection's round-trip measurements, kept here as well
	// as on the inner controller because the delay bound is this sender's.
	rttStats quiccongestion.RTTStatsProvider

	// delayBrake is how much the delay bound is currently removing from the
	// rate, in parts per million, so a trace can tell a path being held back
	// by its own queue from one that simply measured less.
	delayBrake atomic.Uint64

	// arrival is the last computed arrival rate, published for the pacer and
	// the congestion window, which run outside the callback that computes it.
	arrival atomic.Uint64 // arrival rate in parts per million

	// congestive is the share of loss the estimator attributes to this
	// instant's sending rate rather than to the channel's own erasure, in
	// parts per million. It is the only part a sender can remove by slowing
	// down, and it is published here because the pacer runs outside the
	// callback that computes it.
	congestive atomic.Uint64

	// inFlight is what this connection already has on the wire, recorded from
	// the callbacks that carry it because the pacer, like the fields above,
	// runs outside them.
	inFlight atomic.Uint64

	// samples is how many decided packets the estimator's report rests on.
	// Both gates above read their own zero value as a clean path, so until
	// there is evidence behind that reading, the absence of evidence is
	// indistinguishable from evidence of absence -- the reading this sender
	// refuses everywhere else.
	samples atomic.Uint64

	// passed counts the losses handed to the inner controller, which is now
	// all of them. It stays because the telemetry below is an assertion as
	// much as a measurement: what this sender saw and what the controller was
	// charged must agree, and a counter is how that is checked from outside.
	passed atomic.Uint64

	// path is shared with every other lane to the same endpoint pair, or nil
	// for a lane that is on its own. share is this lane's allowance of the
	// endpoint's bottleneck in bytes per second, zero while it is unknown.
	path   pathmodel.Model
	share  atomic.Uint64
	member pathmodel.Member

	// ramping is whether this lane is still finding its rate, and while it is
	// the share does not cap it. See rampGate. The rest is the gate's own
	// state: the most this lane had in flight during the round in progress; the erasure the pool had measured when this lane joined and
	// the packets behind it, which is the floor its loss is judged against;
	// the round it last judged; the delivery the last growth reached and how
	// many rounds have passed without more; and the packets decided in the
	// round in progress.
	ramping       atomic.Bool
	rampPeak      quiccongestion.ByteCount
	rampFloor     float64
	rampFloorN    float64
	rampRound     uint64
	rampDelivered float64
	rampFlat      int
	rampArrived   uint64
	rampLost      uint64
	// joined is whether this lane joined from a pooled seed; aboveFloor
	// whether its last judged round lost significantly more than the floor it
	// joined with; and poolSamples how many packets the pool's current
	// erasure rests on. See compensationFor.
	joined      bool
	aboveFloor  bool
	poolSamples float64
	// poolAboveSince is when the pool's erasure last rose significantly above
	// the floor, zero while it is not; see rebaseFloor.
	poolAboveSince monotime.Time
	// rampBurst is the pool's measured burst factor when this lane joined,
	// and lastGate when the gate last ran; see aboveTheFloor and rebaseFloor.
	rampBurst float64
	poolBurst float64
	// rampBaseRTT is the pool's minimum round trip when this lane joined:
	// the baseline a queue that predates the lane is measured against.
	rampBaseRTT time.Duration
	lastGate    monotime.Time
}

type packetOutcome struct {
	number  quiccongestion.PacketNumber
	arrived bool
}

const (
	// erasureMinArrival bounds the compensation. At an arrival rate of 0.15 the
	// sender already puts nearly seven packets on the wire per packet
	// delivered; below that the path is not one to push harder into, and an
	// unbounded divisor would turn a measurement error into a flood.
	// compensationStep is how much of the remaining compensation one round may
	// take. Erasure compensation is a bet that sending more delivers more, and
	// a bet is only checkable if each increase is small enough that the next
	// round's delivery can be attributed to it. A tenth of the remainder per
	// round reaches a 42% channel's 1.72x in about six rounds, which on a long
	// path is under two seconds.
	compensationStep  = 0.9
	erasureMinArrival = 0.15
	partsPerMillion   = 1e6
)

const (
	// rampGrowth and rampFlatRounds are BBR's own full-pipe test: delivery
	// that has not grown by a quarter in three rounds has stopped following
	// the sending rate.
	rampGrowth     = 1.25
	rampFlatRounds = 3
	// rampQueueFraction is how much queue ends a ramp, as a fraction of the
	// path's minimum round trip. It is a quarter of the delay bound's
	// allowance: the ramp is the phase that runs past the share, so it stops
	// at the first sign of a queue rather than at the bound.
	rampQueueFraction = 4
	// rampLossSamples is the fewest packets a round, and the pool's floor,
	// must each rest on before the one is compared with the other, and
	// rampLossDeviations how far above the floor, in standard deviations of
	// the difference, the round must then be. A 32-packet first flight on a
	// 15% channel loses seven packets about one time in four; read without
	// its noise it would end most ramps in their first round.
	rampLossSamples    = 64
	rampLossDeviations = 3
)

// NewErasureSender returns a controller for a path whose loss is mostly not
// congestion, deciding on its own. Use NewErasureSenderOn when more than one
// lane shares an endpoint pair.
func NewErasureSender(initialPacketSize quiccongestion.ByteCount) *ErasureSender {
	return NewErasureSenderOn(initialPacketSize, nil)
}

// NewErasureSenderOn returns a controller that pools its measurements with
// every other lane on the same path.
//
// Deciding alone is what made lanes cost more than they earn: each lane
// measures the erasure floor from only its own packets, and each discovers the
// bottleneck from only its own delivered rate, so the aggregate overshoots by
// however many lanes there are and the path's loss stops being memoryless.
func NewErasureSenderOn(initialPacketSize quiccongestion.ByteCount, path pathmodel.Model) *ErasureSender {
	e := newErasureSender(initialPacketSize)
	e.path = path
	if path != nil {
		// Start where its siblings already are rather than at the initial
		// window. On a path that erases 40% of packets the ramp is the
		// expensive part, and a lane opened to replace one that died would
		// otherwise pay it again on a path nothing has forgotten.
		//
		// What is seeded is the delivered rate; the erasure compensation is
		// applied on top of it, as for any other estimate. BBR sizes
		// both its pacing and its window from that one number, so seeding it
		// moves both; seeding a pacing rate alone leaves the window at the
		// initial one and the pacer waiting on it.
		//
		// The seed and the cap are separate: a lane joining a path that is
		// already occupied takes both, and a lane that is alone takes the
		// seed only. Sharing one number for the two used to mean that the
		// only lane on a path capped itself at whatever the last one managed.
		state := path.Current()
		// The measured erasure is path knowledge, so a replacement lane
		// publishes it, and its code is sized from it, rather than
		// rediscovering it.
		//
		// It is not paced from. The loss alone does not say whether sending
		// more would deliver more, and a policer's loss is as high as an
		// erasure channel's. Pacing and the window are compensated only by
		// what the siblings earned, below, and stay uncompensated until some
		// sibling has decided -- at 85% loss, reading 1-Erasure instead paced
		// 6.7x the seed on nothing anyone had earned.
		if state.Erasure > 0 {
			e.erasure.Store(uint64(state.Erasure * partsPerMillion))
		}
		// The compensation its siblings have earned is path knowledge too, and
		// it is where this lane's compensation gate starts. Starting the gate
		// at none, as a lone sender does, threw the knowledge away at the
		// first loss after the round trip was measured: a lane joining at the
		// arrival floor then paced at 0.43x its share, delivered 0.065x of it,
		// and stayed there, because the seeded estimate was the bar the next
		// step had to clear.
		//
		// It is the earned compensation and not 1-Erasure. The two agree on an
		// erasure channel, where sending more did deliver more. On a policer
		// they do not: the loss is high and the siblings were refused the
		// compensation. A joiner that took 1/arrival from the raw loss
		// instead peaked at 11.5x the emulated policer's rate in
		// internal/pep/case4_test.go, against 8.6x taking the earned value.
		// Further compensation is still earned round by round, as for any
		// lane.
		if state.Arrival > 0 {
			e.arrival.Store(uint64(state.Arrival * partsPerMillion))
			e.appliedArrival = e.arrivalRate()
		}
		if state.Seed > 0 {
			if state.Share > 0 {
				e.share.Store(uint64(state.Share))
			}
			// The seed is the pooled delivered rate, the inner filter is a
			// delivered rate, and bandwidth() divides by the arrival rate once
			// to put it on the wire. So it is seeded as it is: dividing it here
			// compensated twice (#73), and multiplying compensated zero times.
			e.inner.seedBandwidth(uint64(state.Seed), state.RoundTrip)
			// A lane that joins from a pooled seed starts by finding its own
			// rate from it; see rampGate. Its loss is judged against the
			// erasure the pool had measured, when the pool had measured any.
			e.ramping.Store(true)
			e.inner.startupHeld = true
			e.rampBaseRTT = state.RoundTrip
			e.joined = true
			e.rampFloor, e.rampFloorN = state.Erasure, state.ErasureSamples
			e.rampBurst = math.Max(state.ErasureBurst, 1)
			// The compensation gate judges each step by whether delivery rose
			// since the last one. Its baseline starts at zero, and the seeded
			// estimate is above zero without this lane having delivered
			// anything, so the first round would grant a step for the seed
			// itself -- and the lane would report that step as earned, for the
			// next replacement to inherit and step again. The seed is the
			// baseline: a step needs delivery above it.
			e.deliveredAtCompensation = float64(e.inner.estimator.estimate())
			e.compensationRound = e.inner.round
		}
	}
	return e
}

func newErasureSender(initialPacketSize quiccongestion.ByteCount) *ErasureSender {
	inner := NewTUICBBRSender(initialPacketSize)
	// Loss is not this path's congestion signal. Erasure scales delivery down
	// proportionally at every rate and cannot produce a knee; congestion
	// produces one, and the delivery-versus-rate curve is what separates them.
	// The brake is the delay bound.
	inner.lossIsCongestion = false
	e := &ErasureSender{
		inner:  inner,
		member: pathmodel.Member(nextErasureMemberID.Add(1)),
		// A reorder tolerance wide enough for QUIC's acknowledgement
		// aggregation: packets are acked in batches, and a batch boundary must
		// not read as a gap.
		estimator: lossmodel.New(lossmodel.Config{ReorderTolerance: 32}),
	}
	e.arrival.Store(partsPerMillion)
	e.pacer = newTUICPacer(e.bandwidth)
	e.pacer.setBurstFloor(e.unmeteredBurst)
	return e
}

// deliveredRate is BBR's estimate of what arrives, bounded by this lane's
// share of the endpoint pair's bottleneck.
//
// The cap is what keeps lanes from compounding. Each lane's own estimate is
// what it alone is receiving, so four lanes each probing above their own
// estimate put four times the overshoot into one bottleneck; capping at the
// share means the aggregate on the wire is what a single sender would have put
// there. The burst allowance is measured against this for the same reason.
//
// A lane that is still ramping is not capped; see rampGate.
func (e *ErasureSender) deliveredRate() float64 {
	delivered := float64(e.inner.bandwidth())
	if share := float64(e.share.Load()); share > 0 && share < delivered && !e.ramping.Load() {
		delivered = share
	}
	return delivered
}

// rampGate decides when a lane joining a shared path has found its rate, and
// until then exempts it from its share.
//
// The share is a fair split of what the pool has measured, and what the pool
// has measured is what its members delivered -- which, for a burst of lanes
// joining a pool that has idled down to a trickle, is the trickle. A share is
// 1.25x an even split of that, so the members together can grow the measured
// bottleneck by a quarter per measurement. That is enough only if every
// member is using its share. One that is not -- the pooled control connection
// a download joins next to, a lane that has finished or stalled and not yet
// expired -- still takes an even split, and four bulk lanes beside one idle
// member grow by 1.25 x 4/5: not at all. Measured live, lanes on a 150 Mbit/s
// path plateaued at a third of it for the whole of a 256 MiB transfer, with
// the loss at the path's floor and no queue. The share was being read as a
// ceiling on a path with nothing to protect.
//
// So a joining lane ramps the way a lone BBR sender does, from its seed, and
// takes up its share only when the path shows the share has something to
// protect. Any of three signals ends the ramp, for good:
//
//   - a queue: the smoothed round trip a quarter above the path's minimum;
//   - loss above the floor: a round losing more than the erasure the pool had
//     measured when this lane joined, by more than the two measurements'
//     sampling noise (lossAboveFloor);
//   - delivery that has stopped following the sending rate: BBR's own
//     full-pipe test, three rounds without a quarter's growth in what this lane
//     measured itself delivering.
//
// The third counts only rounds in which the window or the pacer held the lane
// back, so a lane carrying requests before its transfer keeps its ramp for
// the transfer.
//
// The first two are the path saying it is full. The third is for a path that
// says neither -- a policer whose drops have already been measured as the
// floor queues nothing and loses no more than it did -- but delivers no more
// however fast it is sent into. A lane that ends its ramp has reported what
// it reached, and the pool's share is sized from that, so it hands over to the
// share at the rate it found rather than at the trickle.
//
// Nothing here reports a rate the lane did not measure. The pooled seed and
// the bottleneck stay what was delivered (#73); only the cap moves.
func (e *ErasureSender) rampGate(now monotime.Time, acked []quiccongestion.AckedPacketInfo, lost []quiccongestion.LostPacketInfo) {
	if !e.joined {
		return
	}
	if queue, base := e.rampQueue(); e.ramping.Load() && base > 0 && queue*rampQueueFraction >= base {
		e.endRamp()
	}
	// Judge the round that has just closed, once per round. The inner round
	// advances inside its own callback, after this one has run, so a change
	// seen here is a round that ended on the previous event. The loss is
	// judged for as long as the lane lives, because the compensation gate
	// reads it too.
	if round := e.inner.round; round != e.rampRound {
		e.rampRound = round
		e.aboveFloor = e.lossAboveFloor(e.rampArrived, e.rampLost)
		e.rampArrived, e.rampLost = 0, 0
		if e.ramping.Load() {
			e.judgeRamp()
		}
		e.rampPeak = 0
	}
	e.rebaseFloor(now)
	e.rampArrived += uint64(len(acked))
	e.rampLost += uint64(len(lost))
}

// judgeRamp ends the ramp on a closed round that lost more than the floor,
// or on the third that tried to send faster and delivered no more.
func (e *ErasureSender) judgeRamp() {
	// A round only counts as flat if the lane was sending as if it had a
	// backlog: at some point half its window in flight. One carrying a few
	// requests is not probing anything, and calling it flat would end the
	// ramp before the bulk transfer it was opened for has started. A refused
	// pacing or window poll is not that evidence: quic-go polls after the
	// last packet of a request as readily as in the middle of a transfer.
	limited := 2*e.rampPeak >= e.GetCongestionWindow()
	if e.aboveFloor {
		e.endRamp()
		return
	}
	delivered := float64(e.inner.estimator.measuredEstimate())
	if delivered >= e.rampDelivered*rampGrowth && delivered > 0 {
		e.rampDelivered, e.rampFlat = delivered, 0
		return
	}
	if limited {
		if e.rampFlat++; e.rampFlat >= rampFlatRounds {
			e.endRamp()
		}
	}
}

// lossAboveFloor reports whether a round's loss is significantly above the
// erasure the pool had measured when this lane joined.
//
// Both are estimates, so it is the two-proportion test rather than the
// round's own binomial noise against a fixed floor: a pool that had decided
// only a handful of packets reports an erasure of zero with nothing behind
// it, and a fixed zero has no noise at all, so one ordinary loss would end
// the ramp. A pool with too few packets behind its figure, or a round with
// too few of its own, is not judged.
func (e *ErasureSender) lossAboveFloor(arrived, lost uint64) bool {
	decided := float64(arrived + lost)
	if decided == 0 {
		return false
	}
	return e.aboveTheFloor(float64(lost)/decided, decided)
}

// aboveTheFloor is the two-proportion test of a loss rate measured over
// samples packets against the floor this lane joined with.
func (e *ErasureSender) aboveTheFloor(loss, samples float64) bool {
	// Losses that arrive in runs are fewer independent trials than packets:
	// a channel whose erasures come in runs of b gives about n/b of them, and
	// a single 30-packet burst in a 100-packet round on a 15% floor would
	// otherwise read as overflow. Both sides are deflated by the burst factor
	// the pool had measured.
	burst := math.Max(e.rampBurst, 1)
	samples /= burst
	floorN := e.rampFloorN / burst
	if samples < rampLossSamples/burst || floorN < rampLossSamples/burst {
		return false
	}
	// The normal approximation behind the test says nothing about a handful
	// of losses on a clean floor, where one loss in a hundred would read as
	// significant; so the excess must also be that many packets.
	if (loss-e.rampFloor)*samples < rampLossDeviations {
		return false
	}
	pooled := (loss*samples + e.rampFloor*floorN) / (samples + floorN)
	deviation := math.Sqrt(pooled * (1 - pooled) * (1/samples + 1/floorN))
	return loss-e.rampFloor > rampLossDeviations*deviation
}

// poolAboveFloor reports whether the pool's measured erasure has risen
// significantly above what it was when this lane joined.
func (e *ErasureSender) poolAboveFloor() bool {
	return e.aboveTheFloor(float64(e.erasure.Load())/partsPerMillion, e.poolSamples)
}

// rebaseFloor makes the floor this lane joined with expire.
//
// Loss the pool measures above it is treated as the lanes' own overflow, and
// earns no compensation, until shown otherwise. What shows otherwise is time:
// overflow lasts only while the lanes overdrive, and the shares that let them
// are sized from a bottleneck window that turns over in bottleneckWindow. A
// pool still above the floor a whole window later is a path whose erasure
// has changed, and the lane takes the pool's current figure as its floor and
// earns compensation against it as any lane does.
func (e *ErasureSender) rebaseFloor(now monotime.Time) {
	// A lane that joined before the pool had measured enough to judge
	// against takes the floor once it has: otherwise neither loss guard
	// would ever run for it.
	if e.rampFloorN < rampLossSamples && e.poolSamples >= rampLossSamples {
		e.rampFloor = float64(e.erasure.Load()) / partsPerMillion
		e.rampFloorN, e.rampBurst = e.poolSamples, math.Max(e.poolBurst, 1)
		return
	}
	// The window has to be observed, not merely elapsed: a lane that saw the
	// overflow once and then went quiet has no evidence about the interval it
	// did not watch, and the estimator, which forgets by packets rather than
	// by time, would hand it the same figure when it woke.
	gap := !e.lastGate.IsZero() && now.Sub(e.lastGate) > rebaseGap
	e.lastGate = now
	if gap || !e.poolAboveFloor() {
		e.poolAboveSince = 0
		if gap && e.poolAboveFloor() {
			e.poolAboveSince = now
		}
		return
	}
	if e.poolAboveSince == 0 {
		e.poolAboveSince = now
		return
	}
	if now.Sub(e.poolAboveSince) >= floorRebaseAfter {
		e.rampFloor = float64(e.erasure.Load()) / partsPerMillion
		e.rampFloorN = e.poolSamples
		e.rampBurst = math.Max(e.poolBurst, 1)
		e.poolAboveSince = 0
	}
}

// floorRebaseAfter is the pooled bottleneck's own window
// (pathmodel.bottleneckWindow): the longest a share sized from an overshoot
// can outlive it.
const floorRebaseAfter = 10 * time.Second

// rebaseGap is the longest the gate may go without running and still count
// the time as observed.
const rebaseGap = time.Second

// endRamp hands the lane to its share, and its controller out of startup.
//
// The ramp is this lane's startup, and its end is the full pipe BBR's startup
// is looking for: a queue, loss above the floor, or delivery that stopped
// following. The inner controller cannot find that for itself here, for two
// opposite reasons. While the lane ramps, its own exit would fire on the
// wrong signal -- startup leaves at 2% loss, which an erasure channel exceeds
// every round -- so it is held (TUICBBRSender.startupHeld): measured live,
// lanes left startup on ordinary erasure at a fraction of their share and
// crawled in ProbeBW. And once the share caps a lane it looks
// application-limited at every acknowledgement, and startup's exit tests skip
// those rounds, so it would never leave: pacing at the startup gain over its
// last estimate, with a share sized from the summed peaks of a burst of lanes
// as its only bound -- simulated on a shallow-buffer path, that held the
// bottleneck dropping a third of what was sent.
func (e *ErasureSender) endRamp() {
	e.ramping.Store(false)
	e.inner.startupHeld = false
	e.inner.fullBandwidth = true
}

// rampQueue is the queue the ramp judges: the smoothed round trip over the
// smaller of this lane's minimum and the pool's at join. A lane joining
// behind a standing queue measures its own minimum inside it.
func (e *ErasureSender) rampQueue() (queue, base time.Duration) {
	base = e.inner.minRoundTrip()
	if e.rampBaseRTT > 0 && (base <= 0 || e.rampBaseRTT < base) {
		base = e.rampBaseRTT
	}
	if base <= 0 || e.rttStats == nil || e.rttStats.SmoothedRTT() <= 0 {
		return 0, 0
	}
	if smoothed := e.rttStats.SmoothedRTT(); smoothed > base {
		return smoothed - base, base
	}
	return 0, base
}

// bandwidth is the rate to put on the wire: what arrives, divided by the
// fraction that arrives.
func (e *ErasureSender) bandwidth() quiccongestion.ByteCount {
	// The delay bound is applied after the erasure compensation rather than
	// before it, because the queue holds what was sent and not what arrived.
	return quiccongestion.ByteCount(e.delayBounded(e.deliveredRate() / e.arrivalRate()))
}

// compensationFor decides how much of a proposed compensation to apply, by
// asking whether the last one bought any delivery.
//
// Less compensation is always allowed at once: it can only reduce what goes on
// the wire. More is a claim that the path will deliver proportionally more if
// this sender sends proportionally more, and that claim is tested against the
// delivered rate rather than assumed. If the previous increase did not raise
// delivery, this one is refused and the compensation stands where it is.
//
// This is not the classifier returning. It makes no judgement about the nature
// of the loss and reads no burst statistics; it observes only whether sending
// harder delivered more, which is the question the design's own argument rests
// on -- erasure scales delivery down at every rate and cannot produce a knee,
// congestion produces one. Without it a policed path is a positive feedback
// loop: measured against an emulated policer the sender reached eleven times
// the path's capacity at 63% loss with nothing to brake it, because a policer
// drops rather than queues and so offers the delay bound no signal either.
func (e *ErasureSender) compensationFor(want float64) float64 {
	if want > 1 {
		want = 1
	}
	if want < erasureMinArrival {
		want = erasureMinArrival
	}
	if e.appliedArrival == 0 {
		// Start at no compensation and earn the rest. Accepting the first
		// proposal whole is what made an earlier version of this useless: on a
		// policed path the first measurement is already taken after the sender
		// has burst and been policed, so it asks for several times the rate
		// before there is any evidence to test that request against.
		e.appliedArrival = 1
	}
	// The delivered-rate estimate, not the sender's own bandwidth: that method
	// returns the pacing rate, which this compensation is an input to. Judging
	// the bet against it would be judging the bet against its own effect, and
	// every increase would appear to have worked.
	delivered := float64(e.inner.estimator.estimate())
	round := e.inner.round
	if want >= e.appliedArrival {
		// Asking for no more than is applied can only reduce what goes on the
		// wire, so it is taken at once.
		e.appliedArrival, e.deliveredAtCompensation, e.compensationRound = want, delivered, round
		return want
	}
	// More compensation than is applied. Not while the lane is ramping: the
	// ramp is raising the sending rate on its own, so delivery rises whether
	// or not the last step bought any of it, and a step judged against that
	// would be granted for loss the ramp itself is causing. The baseline
	// follows the ramp, so the first step after it has to beat what the ramp
	// reached.
	//
	// Nor, for a lane that joined knowing the path's erasure, while its last
	// round, or the pool, lost significantly more than that. Compensation is
	// for erasure, and loss above the erasure the pool had measured when the
	// lane joined is the lanes' own overflow until shown otherwise:
	// compensating it raises the rate that caused it. Without this a burst
	// that filled a shallow buffer pooled its overflow as erasure, and the
	// gate, judging by a max filter that drifts up with noise, granted the
	// compensation for it step by step, paced the shares at up to twice the
	// path, and held the bottleneck dropping a third of what was sent. The
	// pool has to be read as well as the lane: a shared tail-drop queue does
	// not share its drops evenly, and in that simulation the lane that took
	// its compensation from 0.85 to 0.43 lost at the floor itself while its
	// siblings lost 35-50%.
	if e.ramping.Load() || e.aboveFloor || (e.joined && e.poolAboveFloor()) {
		e.deliveredAtCompensation, e.compensationRound = delivered, round
		return e.appliedArrival
	}
	// Give the previous step a round to show an effect before judging it,
	// then require that it showed one.
	if round == e.compensationRound {
		return e.appliedArrival
	}
	if delivered <= e.deliveredAtCompensation {
		// Sending harder did not deliver more. Hold, and re-arm against what
		// the path is doing now so a path that later improves is still
		// followed.
		e.deliveredAtCompensation, e.compensationRound = delivered, round
		return e.appliedArrival
	}
	// It did deliver more, so take a step towards what is being asked for
	// rather than the whole of it. A step keeps every increase small enough
	// that the next round's delivery can be attributed to it; a jump to the
	// full request would be one large bet with no way to tell afterwards
	// whether it paid.
	next := e.appliedArrival * compensationStep
	if next < want {
		next = want
	}
	e.appliedArrival, e.deliveredAtCompensation, e.compensationRound = next, delivered, round
	return next
}

func (e *ErasureSender) arrivalRate() float64 {
	rate := float64(e.arrival.Load()) / partsPerMillion
	if rate < erasureMinArrival {
		return erasureMinArrival
	}
	if rate > 1 {
		return 1
	}
	return rate
}

func (e *ErasureSender) SetRTTStatsProvider(provider quiccongestion.RTTStatsProvider) {
	e.rttStats = provider
	e.inner.SetRTTStatsProvider(provider)
}

// queueDelay is how much delay the path is carrying beyond its own minimum,
// which at the bottleneck rate is the data sitting in its queue.
//
// The smoothed round trip is used rather than the latest because a single
// sample carries reordering and one ACK's scheduling; a brake that fires on
// those would back off for a jittery path rather than a full one.
func (e *ErasureSender) queueDelay() (queue, minRTT time.Duration) {
	minRTT = e.inner.minRoundTrip()
	if minRTT <= 0 || e.rttStats == nil {
		return 0, minRTT
	}
	smoothed := e.rttStats.SmoothedRTT()
	if smoothed <= minRTT {
		return 0, minRTT
	}
	return smoothed - minRTT, minRTT
}

// delayBounded scales a rate down while the path is holding more than one
// bandwidth-delay product of queue.
//
// The bound is that the round trip may not exceed twice the path's own
// minimum, which is the same statement as "the queue may hold at most one
// bandwidth-delay product" and the same rule as the controller's 2.0
// congestion-window gain, in the time domain rather than the window domain.
// It is a ratio rather than a duration on purpose: a duration would have to be
// chosen, and any choice is a latency policy smuggled into a congestion
// controller. Interactive latency is protected by the aggregate budget's
// reserve and the lanes' priority queues, which is where an absolute guarantee
// belongs; this bound exists only to stop the path being overdriven.
//
// The scaling is continuous rather than a step. At exactly one round trip of
// queue the factor is one, so a sender that has just reached the bound is not
// punished for arriving there; past it the factor falls as the queue grows,
// which is what makes the response proportional to the overshoot rather than
// to the fact of it.
//
// This is a measurement where the window gain is an estimate. The gain bounds
// the window using a bandwidth this sender believes in, and on an erasure path
// that window is divided by the arrival rate so that what arrives is a full
// one -- which means what is *sent* can be several times the bottleneck's
// worth. The queue is downstream of that division and does not care why the
// bytes were sent.
func (e *ErasureSender) delayBounded(rate float64) float64 {
	queue, minRTT := e.queueDelay()
	if queue <= minRTT || minRTT <= 0 {
		e.delayBrake.Store(0)
		return rate
	}
	factor := float64(minRTT) / float64(queue)
	e.delayBrake.Store(uint64((1 - factor) * partsPerMillion))
	return rate * factor
}

// unmeteredBurst is how much this sender may release without the pacer
// metering it, and it is the answer to a question the pacer was never asking.
//
// Pacing exists to stop a sender building a standing queue at a bottleneck.
// This controller already knows whether one is forming: queueDelay reports what
// the path is holding beyond its own minimum, and the loss estimator separates
// the channel's rate-independent erasure from the part that only appears when
// you push. Neither was wired to the pacer, which metered unconditionally at a
// bandwidth estimate that a request-shaped flow cannot raise, because such a
// flow is application-limited by construction: measured on the datacenter path,
// one flow estimated 42 Mbit/s where sixteen estimated 88, and a 355KB request
// spent 67ms being metered at the former against about 9ms of real wire time.
// That was this transport's entire deficit against a tuned TCP client.
//
// So the rule is the one the controller's own policy already states. The delay
// bound permits a queue of up to one round trip, which is the same statement as
// one bandwidth-delay product. Below that, and with no loss the estimator
// attributes to rate, there is nothing for pacing to protect: the congestion
// window and the acknowledgement clock bound the send, which is what an unpaced
// TCP does and why a tuned one reaches this path's floor. At the bound, or once
// loss starts tracking rate, metering resumes at the estimate.
//
// The response is one round trip away in the worst case, which is the same
// latency every window-based controller accepts, and a burst loss on the way
// there is repaired by the code rather than by a retransmission.
func (e *ErasureSender) unmeteredBurst() quiccongestion.ByteCount {
	// Too little has reported on this channel for the two gates below to be
	// reading anything but their own initial zeroes, which is a clean path. That
	// is how a startup burst was granted on a channel erasing 42%: by the time
	// the erasure ceiling had a measurement to refuse it with, the burst had
	// already built the queue, and the delay brake holds the rate down long
	// after. Measured on the emulated erasure path, closing this window alone
	// was the difference between 9.4 and 10.5 Mbit/s.
	if e.samples.Load() < burstEvidenceSamples {
		return 0
	}
	if float64(e.congestive.Load())/partsPerMillion > 0 {
		return 0
	}
	// A policer is the case neither brake can see: it drops what it cannot
	// pass and holds nothing, so there is no queue for the delay bound to
	// measure, and its loss is sustained enough that the estimator's floor
	// rises to meet it and reports no congestive component. Both signals above
	// therefore read clean on exactly the path where bursting is worst.
	// internal/pep/case4_test.go records that this transport already overdrives
	// such a path; measured against it, gating on those two signals alone took
	// the overdrive from 3.0x to 4.0x and the loss from 32.9% to 54.9%, because
	// metering was the last thing holding the rate down.
	//
	// So the burst also requires the direction being sent into to be delivering
	// essentially everything. That is not a third signal so much as a refusal
	// to act on the absence of the first two: where loss is already high, this
	// sender cannot tell its own contribution from the channel's, and the safe
	// reading of an ambiguous channel is to keep metering. Where loss is
	// absent, a burst that starts causing it shows up as loss within a round
	// trip and closes this gate.
	if float64(e.erasure.Load())/partsPerMillion > burstLossCeiling {
		return 0
	}
	queue, minRTT := e.queueDelay()
	if minRTT <= 0 || queue >= minRTT {
		return 0
	}
	// What is already on the wire has to come out of the allowance, and the
	// paragraph above is what explains why. A burst below the bandwidth-delay
	// product cannot build a standing queue only if it is what the path holds;
	// granted on top of a window that is already in flight it is not that, it
	// is one product of overshoot, and the congestion window is no bound on it
	// because this sender's window is inflated by the erasure compensation.
	//
	// The two flows this has to separate both look uncongested by the signals
	// above. A request-shaped flow arrives with an empty pipe and gets the
	// whole product, which is the case this exists for: 355KB against a 1.09MB
	// product on the 207ms datacenter path, released without paying the 67ms.
	// A saturating flow is already holding a product or more, gets nothing, and
	// stays metered. Measured on the emulated erasure path, granting it the
	// window regardless drove a burst into a 25 Mbit/s bottleneck, the delay
	// brake read the queue that built, and the controller fell from 10.5 to
	// 0.9 Mbit/s of a 14.5 Mbit/s capacity -- worse, monotonically, the larger
	// the burst: 0.53 at 128KB and 0.09 at 1MB.
	product := quiccongestion.ByteCount(e.deliveredRate() * minRTT.Seconds())
	allowance := product - quiccongestion.ByteCount(e.inFlight.Load())
	if allowance <= 0 {
		return 0
	}
	if window := e.GetCongestionWindow(); window < allowance {
		return window
	}
	return allowance
}

// burstLossCeiling is how much loss the sending direction may show and still
// permit an unmetered burst.
//
// The two paths this project has measured sit far apart on either side of it,
// which is the only reason a threshold is defensible here at all: the
// datacenter link's upload direction lost 0 of 41,663 datagrams, and an
// emulated policer shaped to its own capacity loses a third of everything.
// Anything in between is a channel this sender cannot characterize from its
// own traffic, and it meters rather than guess.
const burstLossCeiling = 0.01

// burstEvidenceSamples is how many decided packets must lie behind that
// measurement before it is allowed to open the gate rather than close it. It is
// the ceiling above read backwards: a channel cannot be called cleaner than one
// packet in a hundred on the evidence of fewer than a few hundred of them.
const burstEvidenceSamples = 4 / burstLossCeiling

func (e *ErasureSender) TimeUntilSend(bytesInFlight quiccongestion.ByteCount) monotime.Time {
	e.recordInFlight(bytesInFlight)
	return e.pacer.timeUntilSend()
}

func (e *ErasureSender) recordInFlight(bytesInFlight quiccongestion.ByteCount) {
	if bytesInFlight < 0 {
		bytesInFlight = 0
	}
	e.inFlight.Store(uint64(bytesInFlight))
}

func (e *ErasureSender) HasPacingBudget(now monotime.Time) bool {
	return e.pacer.budget(now) >= e.inner.maxDatagramSize
}

func (e *ErasureSender) OnPacketSent(sentTime monotime.Time, bytesInFlight quiccongestion.ByteCount, number quiccongestion.PacketNumber, bytes quiccongestion.ByteCount, retransmittable bool) {
	e.pacer.sentPacket(sentTime, bytes)
	if bytesInFlight > e.rampPeak {
		e.rampPeak = bytesInFlight
	}
	e.inner.OnPacketSent(sentTime, bytesInFlight, number, bytes, retransmittable)
}

// CanSend uses the compensated window for the same reason the pacer uses the
// compensated rate: the window bounds what is on the wire, and on an erasure
// channel what is on the wire is more than what will arrive.
func (e *ErasureSender) CanSend(bytesInFlight quiccongestion.ByteCount) bool {
	e.recordInFlight(bytesInFlight)
	return bytesInFlight < e.GetCongestionWindow()
}

func (e *ErasureSender) GetCongestionWindow() quiccongestion.ByteCount {
	return quiccongestion.ByteCount(float64(e.inner.GetCongestionWindow()) / e.arrivalRate())
}

func (e *ErasureSender) MaybeExitSlowStart() { e.inner.MaybeExitSlowStart() }

func (e *ErasureSender) OnPacketAcked(number quiccongestion.PacketNumber, ackedBytes, priorInFlight quiccongestion.ByteCount, eventTime monotime.Time) {
	e.inner.OnPacketAcked(number, ackedBytes, priorInFlight, eventTime)
}

func (e *ErasureSender) OnCongestionEvent(number quiccongestion.PacketNumber, lostBytes, priorInFlight quiccongestion.ByteCount) {
	e.inner.OnCongestionEvent(number, lostBytes, priorInFlight)
}

// OnCongestionEventEx measures the channel and passes every fate through.
//
// It used to be where the two regimes were separated: only the share of the
// losses the channel did not explain reached BBR, so that erasure would not be
// read as congestion. That separation is gone. It was a statistical answer to a
// question the delivery-versus-rate curve answers directly -- erasure scales
// delivery down proportionally at every rate and cannot produce a knee, while
// congestion produces one -- and it was least reliable exactly when loss was
// worst, because heavy erasure is bursty and the burst test then refused to
// call it erasure at all.
//
// What replaced it is not a better classifier but a different signal. The inner
// controller no longer enters recovery on loss, and the brake is the delay
// bound: the round trip may not exceed twice the path's own minimum.
//
// Every loss is still handed to the inner controller, which matters for a
// reason unrelated to congestion. Its bytesInFlight is priorInFlight less what
// was acknowledged and what was lost, so a sender told about only a fraction of
// the losses believes the pipe is fuller than it is and throttles itself for
// packets that are already gone.
func (e *ErasureSender) OnCongestionEventEx(priorInFlight quiccongestion.ByteCount, eventTime monotime.Time, acked []quiccongestion.AckedPacketInfo, lost []quiccongestion.LostPacketInfo) {
	// QUIC has a separate packet-number space for Initial, Handshake and
	// 1-RTT packets, but this public callback omits the space. Inferring losses
	// from gaps therefore fabricates them when those numbers overlap. The
	// callback has already resolved every fate, so order that explicit batch
	// for the burst statistic and measure it directly. A handful of equal
	// numbers across spaces may be adjacent, but neither can become a
	// fictitious gap. Retaining the scratch slice avoids allocating on every
	// acknowledgement.
	e.outcomes = e.outcomes[:0]
	for _, packet := range acked {
		if packet.PacketNumber >= 0 {
			e.outcomes = append(e.outcomes, packetOutcome{number: packet.PacketNumber, arrived: true})
		}
	}
	for _, packet := range lost {
		if packet.PacketNumber >= 0 {
			e.outcomes = append(e.outcomes, packetOutcome{number: packet.PacketNumber})
		}
	}
	sort.Slice(e.outcomes, func(i, j int) bool { return e.outcomes[i].number < e.outcomes[j].number })
	for _, outcome := range e.outcomes {
		e.estimator.ObserveOutcome(outcome.arrived)
	}
	snapshot := e.estimator.Snapshot()
	if snapshot.Samples > 0 {
		e.samples.Store(uint64(snapshot.Samples))
	}
	if snapshot.Congestive > 0 {
		e.congestive.Store(uint64(snapshot.Congestive * partsPerMillion))
	} else {
		e.congestive.Store(0)
	}
	erasure := snapshot.Loss
	if e.path != nil {
		// Pool with the other lanes: the estimate converges on all their
		// samples together, and the share is what stops their probes
		// compounding.
		state := e.path.Report(e.id(), pathmodel.Observation{
			Erasure: snapshot.Loss, BurstFactor: snapshot.BurstFactor,
			ObservedSamples: float64(snapshot.Decided),
			// What the loss figure rests on: the estimator forgets, so this
			// stops growing where Decided does not.
			ErasureSamples: snapshot.Samples,
			// Only what this lane has measured itself. The estimate also
			// holds the seed, which came from this pool; reporting it would
			// count the pool's own figure as fresh delivery, and joins inside
			// the bottleneck window would inflate it one seed at a time. Zero
			// until the first sample still counts the lane as a member.
			Delivered: float64(e.inner.estimator.measuredEstimate()),
			RoundTrip: e.inner.minRoundTrip(),
			// What the gate has accepted so far, zero before it has run.
			Arrival: e.appliedArrival,
		})
		erasure = state.Erasure
		e.erasure.Store(uint64(state.Erasure * partsPerMillion))
		e.share.Store(uint64(state.Share))
		e.poolSamples, e.poolBurst = state.ErasureSamples, state.ErasureBurst
		e.rampGate(eventTime, acked, lost)
	}
	// The compensation rides on the measurement rather than on a floor biased
	// low for pacing. Under-compensating is not the safe direction it looks
	// like: pacing a delivered-rate estimate makes the sending rate its own
	// input, and the loop walks down to nothing rather than converging on the
	// bottleneck.
	//
	// But it waits for the delay bound to be able to act. A first flight can
	// overrun a clean path's queue before the controller has found its
	// bottleneck, and compensating for those drops means sending twice as fast
	// into a queue that is already overflowing -- the exact positive feedback
	// the removed classifier was there to prevent. The bound would stop it,
	// except that at that moment there is no minimum round trip to bound
	// against, so the brake is inert.
	//
	// Gating on the minimum being measured is not a classifier returning. It
	// makes no judgement about what kind of loss this is; it says only that
	// compensation may not run ahead of the brake that bounds it.
	if _, minRTT := e.queueDelay(); minRTT > 0 {
		e.arrival.Store(uint64(e.compensationFor(1-erasure) * partsPerMillion))
	}

	e.passed.Add(uint64(len(lost)))
	e.inner.OnCongestionEventEx(priorInFlight, eventTime, acked, lost)
}

func (e *ErasureSender) OnRetransmissionTimeout(retransmitted bool) {
	e.inner.OnRetransmissionTimeout(retransmitted)
}

func (e *ErasureSender) SetMaxDatagramSize(size quiccongestion.ByteCount) {
	e.inner.SetMaxDatagramSize(size)
	e.pacer.setMaxDatagramSize(size)
}

func (e *ErasureSender) InSlowStart() bool { return e.inner.InSlowStart() }
func (e *ErasureSender) InRecovery() bool  { return e.inner.InRecovery() }

// Telemetry reports the inner controller's state with this one's compensation
// applied, so a trace shows the rate actually being put on the wire.
func (e *ErasureSender) Telemetry() ControllerTelemetry {
	t := e.inner.Telemetry()
	t.Kind = "erasure"
	// The inner controller was charged only the congestive share, so its own
	// figure cannot answer what the path did. Both halves are reported: what
	// this sender saw, and what it withheld as erasure.
	// Every loss now reaches the inner controller, so what this sender saw and
	// what the controller was charged are the same number. Publishing it from
	// this sender's own counter rather than copying the controller's keeps the
	// two independently derived, which is what lets a test assert they agree.
	t.PacketsLostObserved = e.passed.Load()
	t.Erasure = float64(e.erasure.Load()) / partsPerMillion
	// Read, never recomputed. This method runs on the flow telemetry goroutine
	// while quic-go invokes the controller on its packet goroutine, which is
	// why everything else here comes from an atomic. Recomputing the brake
	// reached through to the inner sender's minimum round trip, which the
	// packet goroutine writes: a data race, and one that surfaced on a single
	// CI architecture and on none of eight local runs.
	//
	// The cost is that a trace taken while nothing is sending reports the last
	// brake rather than a fresh one. That is the right trade -- a stale
	// diagnostic is a smaller problem than an unsynchronised read of the
	// controller's state.
	t.DelayBrake = float64(e.delayBrake.Load()) / partsPerMillion
	arrival := e.arrivalRate()
	t.PacingRate = uint64(float64(t.PacingRate) / arrival)
	t.CongestionWindow = uint64(float64(t.CongestionWindow) / arrival)
	return t
}

// id identifies this lane within its shared model. It avoids pointer-derived
// identity so a report never exposes an address, even inside the process.
func (e *ErasureSender) id() pathmodel.Member { return e.member }

// Share is this lane's allowance of the endpoint pair's bottleneck in bytes
// per second, or zero when it is deciding alone or the bottleneck is not yet
// known.
func (e *ErasureSender) Share() float64 { return float64(e.share.Load()) }

// Channel reports what this sender has measured about the path and how many
// losses it has handed to the controller.
func (e *ErasureSender) Channel() (lossmodel.Snapshot, uint64) {
	return e.estimator.Snapshot(), e.passed.Load()
}

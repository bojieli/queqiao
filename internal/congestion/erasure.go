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

	// limitPeak, limitHeld, limitRound and limitIdle judge whether the lane is
	// application-limited, for the pool (see judgeAppLimited): the most it
	// had in flight in the round in progress, whether a refused poll held an
	// ack-eliciting packet back in it, the round last judged, and how many
	// closed rounds in a row were not send-limited. appLimited is the
	// verdict, and starts false: a lane is capped as before until it has
	// shown it has less to send.
	limitPeak  quiccongestion.ByteCount
	limitHeld  bool
	limitRound uint64
	limitIdle  int
	appLimited bool

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

	// pacing is the rate the pacer last put on the wire, after the share and
	// the delay brake, published for Telemetry.
	pacing atomic.Uint64

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
	ramping  atomic.Bool
	rampPeak quiccongestion.ByteCount
	// Trace counters; see the matching ControllerTelemetry fields. denied is
	// whether a poll has been refused since the last ack-eliciting send.
	capBound                     atomic.Bool
	appLimitedTrace              atomic.Bool
	ackOnlyBytes, ackOnlyCharged atomic.Uint64
	ackedBytes                   atomic.Uint64
	pacingDenials, windowDenials atomic.Uint64
	heldSends                    atomic.Uint64
	uncappedPacing               atomic.Uint64
	denied                       bool
	rampFloor                    float64
	rampFloorN                   float64
	rampRound                    uint64
	rampDelivered                float64
	rampFlat                     int
	rampArrived                  uint64
	rampLost                     uint64
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
	// ownFloor is the loss of this lane's own first rounds, over ownFloorN
	// packets, and ownArrived/ownLost what it is being gathered from; see
	// floor. windowArrived/windowLost are this lane's own outcomes since the
	// pool rose above the floor; see rebaseFloor.
	ownFloor, ownFloorN       float64
	ownBurst                  float64
	ownArrived, ownLost       uint64
	ownRoundMax               uint64
	ownConfirmed              bool
	windowArrived, windowLost uint64
	// earnArrived/earnLost are the outcomes of the ramp rounds in which
	// delivery grew with the rate and the loss stayed at the floor, and
	// earnRounds how many there were; see earnFromRamp.
	earnArrived, earnLost uint64
	earnRounds            int
	// rearmAfter is the first round a ramp may be re-armed, rearmBackoff
	// the rounds the next re-arm waits, rearmFrom the delivery a re-armed
	// ramp started from (zero for a first ramp), lastShare the share at the
	// last round a lane was capped, and departed whether it has since been
	// left uncapped without re-arming. poolRTT is the pool's current minimum
	// round trip, the baseline a re-armed ramp's queue is judged against.
	// See maybeRearm.
	rearmAfter   uint64
	rearmBackoff uint64
	rearmFrom    float64
	lastShare    uint64
	departed     bool
	poolRTT      time.Duration
	// selfSamples, selfLoss and selfObserved are this lane's own report to
	// the pool -- the part of the pool's erasure that is not independent
	// evidence about its rounds -- and poolWeight the observations the
	// pool's figure is weighted by. See siblingsErasure.
	selfSamples, selfLoss, selfObserved, poolWeight, poolErasure float64
	// selfBurst is this lane's own measured burst factor, what its rounds'
	// losses are deflated by; the floor keeps the one measured with it.
	selfBurst float64
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
	// rampConfirmDeviations is how close, in standard deviations, a faster
	// round's loss must be to a lane's first rounds for them to be its
	// floor; see confirmOwnFloor.
	rampConfirmDeviations = 2
	// rampRearmShare is how far a lane's share must stand above what it
	// delivers before it ramps again, and rampRearmRounds the fewest rounds
	// between the end of one ramp and the next re-arm; it doubles, up to
	// rampRearmMaxRounds, each time a re-armed ramp finds nothing more.
	rampRearmShare     = 2
	rampRearmRounds    = 4
	rampRearmMaxRounds = 64
	// appLimitedRounds is how many closed rounds in a row a lane must go
	// without being send-limited before the pool stops capping it; one
	// send-limited round caps it again.
	appLimitedRounds = 3
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
			// The leaf's minimum, not the chain's: an ancestor pools other
			// destinations, and a shorter one's round trip read as this
			// path's queue would end every ramp at once.
			e.rampBaseRTT = state.LeafRoundTrip
			e.rearmBackoff = rampRearmRounds
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
	e.uncappedPacing.Store(uint64(delivered))
	capped := false
	if share := float64(e.share.Load()); share > 0 && share < delivered && !e.ramping.Load() {
		delivered, capped = share, true
	}
	e.capBound.Store(capped)
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
		arrived, lost := e.rampArrived, e.rampLost
		e.rampArrived, e.rampLost = 0, 0
		// The lane's first rounds are its own floor (see floor): they are
		// what it loses at the lowest rate it will send, so they are not
		// judged against it.
		if e.ownFloorN == 0 {
			e.gatherOwnFloor(arrived, lost)
			e.aboveFloor = false
		} else {
			pending := false
			if e.ramping.Load() && e.rampDelivered > 0 && float64(e.inner.estimator.measuredEstimate()) >= e.rampDelivered*rampGrowth {
				pending = e.confirmOwnFloor(arrived, lost)
			}
			e.aboveFloor = !pending && e.lossAboveFloor(arrived, lost)
		}
		if e.ramping.Load() {
			e.judgeRamp(arrived, lost)
		} else {
			e.maybeRearm()
		}
		e.rampPeak = 0
	}
	e.rebaseFloor(now)
	e.rampArrived += uint64(len(acked))
	e.rampLost += uint64(len(lost))
	if e.poolAboveSince != 0 {
		e.windowArrived += uint64(len(acked))
		e.windowLost += uint64(len(lost))
	}
}

// gatherOwnFloor adds a closed round to the lane's own floor until there are
// enough packets behind it to judge against.
func (e *ErasureSender) gatherOwnFloor(arrived, lost uint64) {
	e.ownArrived += arrived
	e.ownLost += lost
	if arrived+lost > e.ownRoundMax {
		e.ownRoundMax = arrived + lost
	}
	decided := float64(e.ownArrived + e.ownLost)
	if burst := e.laneBurst(); decided >= rampLossSamples*burst {
		e.ownFloor, e.ownFloorN, e.ownBurst = float64(e.ownLost)/decided, decided, burst
	}
}

// laneBurst is the burst factor this lane's own outcomes are deflated by: its
// own measured one once its estimator has enough behind it, the pool's until
// then. A floor and the rounds judged against it may come from channels with
// different runs, and counting a run of sixteen as sixteen trials is how an
// overflow could pass for erasure.
func (e *ErasureSender) laneBurst() float64 {
	if e.selfSamples >= rampLossSamples && e.selfBurst > 0 {
		return math.Max(e.selfBurst, 1)
	}
	return math.Max(e.rampBurst, 1)
}

// confirmOwnFloor accepts the lane's own floor once a round sent at a higher
// rate lost the same.
//
// The first rounds alone do not show that their loss is erasure. A lane that
// joins a full queue puts its first flight into it, and in simulation rescue
// joiners lost 98% of their first few hundred packets to a buffer their
// siblings kept full; taken as a floor, that read every later round as clean
// and lifted the overflow veto for good, and the pool's erasure climbed from
// 0.15 to 0.5 as the gate compensated it. What erasure does that overflow does
// not is stay put as the rate rises. So the own floor counts only once a ramp
// round carrying at least twice the packets of any first round, and
// delivering a quarter more than the round before, lost the same -- by the
// same two-proportion test at a lower bar -- and from then on it rests on
// both. A lane wedged behind a full queue loses as much at every rate too,
// but delivers no more for sending more; one confirmed a floor of 0.97 before
// delivery growth was required.
//
// It reports whether the confirmation is pending: the round grew and lost
// what the first rounds did, but is not yet the evidence that confirms them
// -- too few independent samples, or less than twice the packets. Such a
// round is not judged against the pool's floor either: a joiner whose rounds
// grew by 30% at a constant 40% loss over an inherited 15% floor would
// otherwise end its ramp before any round was large enough to confirm what
// it was showing.
func (e *ErasureSender) confirmOwnFloor(arrived, lost uint64) (pending bool) {
	decided := float64(arrived + lost)
	if e.ownConfirmed || decided == 0 {
		return false
	}
	n, m := decided/e.laneBurst(), e.ownFloorN/math.Max(e.ownBurst, 1)
	loss := float64(lost) / decided
	pooled := (loss*n + e.ownFloor*m) / (n + m)
	deviation := math.Sqrt(pooled * (1 - pooled) * (1/n + 1/m))
	if math.Abs(loss-e.ownFloor) > rampConfirmDeviations*deviation {
		return false
	}
	// Enough independent trials, counted after the burst deflation -- a
	// 64-packet round on a channel losing in runs of 16 is four trials.
	if n < rampLossSamples || decided < 2*float64(e.ownRoundMax) {
		return true
	}
	e.ownConfirmed = true
	e.ownFloor = (e.ownFloor*e.ownFloorN + float64(lost)) / (e.ownFloorN + decided)
	// The confirmed floor rests on the independent trials of both, n + m,
	// which the burst factor stored with it has to reproduce: the rounds may
	// have lost in runs of different lengths.
	e.ownFloorN += decided
	e.ownBurst = e.ownFloorN / (n + m)
	return false
}

// floor is the erasure this lane's loss is judged against, and the packets
// behind it.
//
// The pool's floor at join is history: measured by other connections, at
// other rates, possibly at another hour. Measured live on a path losing 22%,
// joiners inherited a floor of about 15%; every well-sampled round then read
// as overflow, the ramp ended at its first, and the compensation veto held
// for good, so the lanes paced their delivered rate uncompensated and walked
// down -- simulated, 0.35 of the path and falling. The re-base that would have
// rescued them needs ten observed seconds, which a rescue joiner never lives.
//
// So the question the ramp asks is the one its own rounds can answer: does
// this lane lose more as it sends faster? Its first rounds, at the lowest rate
// it will send, are its own floor, and loss above the higher of that and the
// pool's is loss that rose with its rate. The pool's figure still counts when
// it is the higher one, which is the case it is evidence for: a lane whose
// first rounds were lucky is not let off by them. An own floor that is not
// confirmed is not used at all, even where the pool has too little to judge
// by: it may be a first flight's overflow (see confirmOwnFloor).
func (e *ErasureSender) floor() (rate, samples, burst float64) {
	poolJudged := e.rampFloorN >= rampLossSamples
	if e.ownConfirmed && (!poolJudged || e.ownFloor > e.rampFloor) {
		return e.ownFloor, e.ownFloorN, math.Max(e.ownBurst, 1)
	}
	return e.rampFloor, e.rampFloorN, math.Max(e.rampBurst, 1)
}

// judgeRamp ends the ramp on a closed round that lost more than the floor,
// or on the third that tried to send faster and delivered no more.
func (e *ErasureSender) judgeRamp(arrived, lost uint64) {
	// A round only counts as flat if the lane was sending as if it had a
	// backlog (see backlogged). One carrying a few requests is not probing
	// anything, and calling it flat would end the ramp before the bulk
	// transfer it was opened for has started. A refused pacing or window
	// poll is not that evidence: quic-go polls after the last packet of a
	// request as readily as in the middle of a transfer.
	limited := e.backlogged()
	if e.aboveFloor {
		e.endRamp()
		return
	}
	// A ProbeRTT round holds the window down on purpose, and its delivery
	// says nothing about whether the path is full: three such rounds fit in
	// one probe on a short path.
	if e.inner.mode == tuicBbrProbeRTT {
		return
	}
	delivered := float64(e.inner.estimator.measuredEstimate())
	if delivered >= e.rampDelivered*rampGrowth && delivered > 0 {
		// Delivery rose with the rate and the loss did not: the round is
		// evidence that sending more delivers proportionally more.
		if e.rampDelivered > 0 {
			e.earnArrived += arrived
			e.earnLost += lost
			e.earnRounds++
		}
		e.rampDelivered, e.rampFlat = delivered, 0
		return
	}
	if !limited {
		// Not probing, so not evidence either way -- and the baseline
		// follows what the lane delivers now. A baseline that only rose
		// would hold the transfer after a lull to the peak of the requests
		// before it, and count its growth back from the lull as flat.
		e.rampDelivered, e.rampFlat = delivered, 0
		return
	}
	if e.rampFlat++; e.rampFlat >= rampFlatRounds {
		e.endRamp()
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
	// The lane's own burst factor once its estimator has enough behind it
	// to have one, the inherited one until then.
	return e.aboveTheFloor(float64(lost)/decided, decided, e.laneBurst())
}

// aboveTheFloor is the two-proportion test of a loss rate measured over
// samples packets, whose losses arrive in runs of burst, against the floor
// this lane judges by.
func (e *ErasureSender) aboveTheFloor(loss, samples, burst float64) bool {
	// Losses that arrive in runs are fewer independent trials than packets:
	// a channel whose erasures come in runs of b gives about n/b of them, and
	// a single 30-packet burst in a 100-packet round on a 15% floor would
	// otherwise read as overflow. Each side is deflated by its own burst
	// factor: the floor by the one measured with it, the figure under test
	// by the one it was measured with.
	burst = math.Max(burst, 1)
	floor, floorN, floorBurst := e.floor()
	samples /= burst
	floorN /= floorBurst
	if samples < rampLossSamples/burst || floorN < rampLossSamples/floorBurst {
		return false
	}
	// The normal approximation behind the test says nothing about a handful
	// of losses on a clean floor, where one loss in a hundred would read as
	// significant; so the excess must also be that many packets.
	if (loss-floor)*samples < rampLossDeviations {
		return false
	}
	pooled := (loss*samples + floor*floorN) / (samples + floorN)
	deviation := math.Sqrt(pooled * (1 - pooled) * (1/samples + 1/floorN))
	return loss-floor > rampLossDeviations*deviation
}

// poolAboveFloor reports whether the pool's measured erasure has risen
// significantly above what it was when this lane joined.
func (e *ErasureSender) poolAboveFloor() bool {
	return e.aboveTheFloor(float64(e.erasure.Load())/partsPerMillion, e.poolSamples, e.poolBurst)
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
	// would ever run for it. Enough besides this lane's own packets, or a
	// lane alone in a thin pool would adopt its own first flight -- which
	// may be overflow -- as soon as its reports had filled the pool.
	if e.rampFloorN < rampLossSamples {
		if floor, samples, ok := e.siblingsErasure(); ok && samples >= rampLossSamples {
			e.rampFloor, e.rampFloorN, e.rampBurst = floor, samples, e.siblingsBurst()
			return
		}
		// A lane with a confirmed floor of its own has something to judge
		// the pool against, and its floor must be able to expire like any
		// other: a lane that outlives the siblings it joined beside would
		// otherwise keep its veto however long the path's erasure has risen.
		if !e.ownConfirmed {
			return
		}
	}
	// The window has to be observed, not merely elapsed: a lane that saw the
	// overflow once and then went quiet has no evidence about the interval it
	// did not watch, and the estimator, which forgets by packets rather than
	// by time, would hand it the same figure when it woke.
	gap := !e.lastGate.IsZero() && now.Sub(e.lastGate) > rebaseGap
	e.lastGate = now
	if gap || !e.poolAboveFloor() {
		e.poolAboveSince = 0
		e.windowArrived, e.windowLost = 0, 0
		if gap && e.poolAboveFloor() {
			e.poolAboveSince = now
		}
		return
	}
	if e.poolAboveSince == 0 {
		e.poolAboveSince = now
		e.windowArrived, e.windowLost = 0, 0
		return
	}
	// And the window has to have been measured, not merely watched. The
	// pool's estimator forgets by packets, not time, so an overflow burst
	// that no packet has since diluted holds its figure for as long as
	// nobody sends; a lane re-basing on it would take the burst for the
	// path. Its own outcomes over the window are fresh by construction, and
	// a path whose erasure rose shows it in them too.
	if now.Sub(e.poolAboveSince) >= floorRebaseAfter && e.lossAboveFloor(e.windowArrived, e.windowLost) {
		e.rampFloor = float64(e.erasure.Load()) / partsPerMillion
		e.rampFloorN = e.poolSamples
		e.rampBurst = math.Max(e.poolBurst, 1)
		e.poolAboveSince = 0
		e.windowArrived, e.windowLost = 0, 0
		// The last round was judged against the floor that just expired.
		e.aboveFloor = false
	}
}

// siblingsErasure is the pool's erasure with this lane's own report taken out
// of it, and the samples behind what is left. The pool weighs each member's
// figure by the outcomes it has observed (State.ErasureWeight), so the
// lane's part is removed exactly; ok is false when nothing is left.
//
// The samples are Kish's effective count, W^2 / sum(w^2/n) over the members
// (pathmodel), which is not additive: a lane that has observed 100,000
// packets on an estimate resting on 1,000 dominates the weights, and
// subtracting its 1,000 from the pool's would leave a sibling's 100 fresh
// samples reading as 2. So the lane's term is removed from the sums and the
// count recomputed.
func (e *ErasureSender) siblingsErasure() (erasure, samples float64, ok bool) {
	weight := e.poolWeight - e.selfObserved
	if weight <= 0 || e.poolSamples <= 0 {
		return 0, 0, false
	}
	spread := e.poolWeight * e.poolWeight / e.poolSamples
	if e.selfSamples > 0 {
		spread -= e.selfObserved * e.selfObserved / e.selfSamples
	}
	if spread <= 0 {
		return 0, 0, false
	}
	samples = weight * weight / spread
	// The pool's figure at full precision: the subtraction scales its
	// rounding by poolWeight/weight, which for a long-lived lane beside a
	// new sibling turns a part per million into points of loss.
	erasure = (e.poolErasure*e.poolWeight - e.selfLoss*e.selfObserved) / weight
	return math.Min(math.Max(erasure, 0), 1), samples, true
}

// siblingsBurst is the pool's burst factor with this lane's report taken out,
// weighted as the erasure is, so the floor siblingsErasure gives and the
// runs its samples are deflated by describe the same observations: a bursty
// joiner dominating the weights would otherwise discount its siblings'
// independent losses as if they came in its runs.
func (e *ErasureSender) siblingsBurst() float64 {
	weight := e.poolWeight - e.selfObserved
	if weight <= 0 {
		return math.Max(e.poolBurst, 1)
	}
	return math.Max((e.poolBurst*e.poolWeight-e.selfBurst*e.selfObserved)/weight, 1)
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
	// The cap the lane hands over to, so that losing it before the next
	// round is still seen as being left alone (maybeRearm).
	e.lastShare = e.share.Load()
	e.earnFromRamp()
	// A re-armed ramp that found no more than it started from was a false
	// alarm, and the next waits twice as long; one that found more resets
	// the wait.
	if e.rearmFrom > 0 {
		if float64(e.inner.estimator.measuredEstimate()) >= e.rearmFrom*rampGrowth {
			e.rearmBackoff = rampRearmRounds
		} else if e.rearmBackoff *= 2; e.rearmBackoff > rampRearmMaxRounds {
			e.rearmBackoff = rampRearmMaxRounds
		}
	}
	e.rearmAfter = e.inner.round + e.rearmBackoff
}

// earnFromRamp grants the compensation the ramp has shown to pay.
//
// A lane takes the compensation its siblings earned. When none of them has
// earned any -- the pooled control connection carries requests and never
// sees a step pay; rescue joiners die before theirs do -- it starts at none,
// and on a path that erases a fifth of what is sent, a lane paced at its
// delivered rate delivers four fifths of it, which is the next estimate.
// Simulated, that walked a 90 Mbit/s path down to a third of itself.
//
// The ramp is the evidence the compensation gate looks for, and more of it
// than one step gets: round after round the lane sent faster, delivered
// proportionally more, and lost no more for it. What those rounds lost is
// what this path erases at the rates the lane tried, and the lane leaves its
// ramp compensating for that. It never takes more than the gate would allow
// (the pooled erasure still bounds it in compensationFor), and rounds that
// lost above the floor or did not grow are not counted.
func (e *ErasureSender) earnFromRamp() {
	decided := float64(e.earnArrived + e.earnLost)
	if !e.ownConfirmed || e.earnRounds < 2 || decided < rampLossSamples*math.Max(e.rampBurst, 1) {
		return
	}
	// The lesser of what the growth rounds lost and the lane's confirmed
	// floor: a growth round can carry some overflow under the test's noise,
	// and the floor is the loss shown not to rise with the rate.
	earned := math.Max(math.Max(float64(e.earnArrived)/decided, 1-e.ownFloor), erasureMinArrival)
	if e.appliedArrival == 0 || earned < e.appliedArrival {
		e.appliedArrival = earned
		e.arrival.Store(uint64(earned * partsPerMillion))
	}
	e.deliveredAtCompensation = float64(e.inner.estimator.estimate())
	e.compensationRound = e.inner.round
	e.earnArrived, e.earnLost, e.earnRounds = 0, 0, 0
}

// maybeRearm starts the ramp again for a lane that has room it is not using.
//
// A ramp ends for good, and with it startup. When a lane's siblings then
// finish, the capacity they free is the remaining lanes', and a lane in
// ProbeBW grows into it a quarter per gain cycle: live, about ten seconds to
// refill a 150 Mbit/s path, and most of what was left between this change and
// main on the fast path. So a lane ramps again, as it did when it joined, on
// either of two signals that it has room:
//
//   - its share stands rampRearmShare times above what it delivers, as it
//     does once departed siblings leave the split;
//   - it was capped and no longer is: the pool has no other busy member
//     (pathmodel.splitLocked), so the path is the lane's alone. This one
//     stays pending through the backoff, since the share cannot stand in for
//     it -- a lane left alone has none.
//
// Not on a bare drop in the number of members. Every survivor of a burst
// then re-entered startup at once into a path the others still filled:
// simulated, one lane of four finishing had the three left lose a quarter of
// what they sent for two seconds, and rescue joiners coming and going re-armed
// the long lanes 26 times in twelve seconds.
//
// Only while it has a backlog -- half of what its pacing rate or its window,
// whichever is smaller, allows in flight -- since a lane carrying requests
// has no use for a ramp. And not sooner than rearmBackoff rounds after the
// last ramp ended, doubling each time a re-armed ramp finds nothing, so a
// lane held below its share by something else -- cross traffic, a narrower
// path the window has not yet forgotten -- probes it on a widening interval
// rather than every few rounds.
func (e *ErasureSender) maybeRearm() {
	// Pending only while the lane stays uncapped: a sibling that starts
	// again before the backoff ends takes the room back.
	if share := e.share.Load(); share == 0 {
		e.departed = e.departed || e.lastShare > 0
	} else {
		e.departed, e.lastShare = false, share
	}
	// Not while ProbeRTT holds the window down on purpose: filling a probe
	// window is not demand, and the departure waits for the probe to end.
	if e.inner.round < e.rearmAfter || e.inner.mode == tuicBbrProbeRTT || !e.backlogged() {
		return
	}
	delivered := float64(e.inner.estimator.measuredEstimate())
	if delivered <= 0 {
		return
	}
	if share := float64(e.share.Load()); !e.departed && share < rampRearmShare*delivered {
		return
	}
	// Not into a queue the ramp would end on at its first event: a sibling
	// that departs while its queue is still draining leaves the room, not
	// yet the path. The departure stays pending until the queue is gone.
	if queue, base := e.queueAgainst(e.poolRTT); base > 0 && queue*rampQueueFraction >= base {
		return
	}
	e.rearm(delivered)
}

// queueAgainst is the smoothed round trip's excess over the smaller of this
// lane's minimum and the given baseline; see rampQueue.
func (e *ErasureSender) queueAgainst(baseline time.Duration) (queue, base time.Duration) {
	base = e.inner.minRoundTrip()
	if baseline > 0 && (base <= 0 || baseline < base) {
		base = baseline
	}
	if base <= 0 || e.rttStats == nil || e.rttStats.SmoothedRTT() <= 0 {
		return 0, 0
	}
	if smoothed := e.rttStats.SmoothedRTT(); smoothed > base {
		return smoothed - base, base
	}
	return 0, base
}

// backlogged reports whether the round just closed had at some point half of
// what the lane was allowed in flight: by its window or by its pacing rate
// over a round trip, whichever is smaller. A lane carrying requests has much
// less. Either limit alone misreads one case: a share that rose above what a
// lane delivers raises its pacing long before its window, which follows the
// delivery; and a window sized from a seed's longer round trip can stay far
// above what the pacer lets out, so a pacer-bound lane on a policer would
// never count a flat round.
func (e *ErasureSender) backlogged() bool {
	allowed := float64(e.GetCongestionWindow())
	rtt := e.inner.minRoundTrip()
	if rtt <= 0 {
		rtt = e.rampBaseRTT
	}
	if rtt > 0 {
		allowed = math.Min(allowed, float64(e.bandwidth())*rtt.Seconds())
	}
	return 2*float64(e.rampPeak) >= allowed
}

// judgeAppLimited decides, once per closed round, whether the lane was
// send-limited in it, and from that whether the pool should cap it at all
// (pathmodel.Observation.AppLimited). It runs from the congestion event, not
// from the rate calculation, and reads only the inner controller's rate and
// the window, neither of which depends on the share.
//
// A round is send-limited when both hold:
//   - a refused pacing or window poll held an ack-eliciting packet back: the
//     lane had something to send and was made to wait;
//   - it had in flight at least a quarter of what its own window and its own
//     pacing rate, before any share, would allow: its demand is a rate the
//     path has to make room for, not a trickle.
//
// The second is what keeps the verdict from making itself true. A cap below
// a lane's demand makes every round of it pacer-limited, so the first alone
// would keep capping the client's flow-acknowledgement lane at 1.25x the
// acknowledgements it had carried, which is the live failure. Held to a
// share, a bulk lane still fills its share's worth of the path, which is a
// fair split of a bottleneck its own estimate is part of, and keeps its cap.
func (e *ErasureSender) judgeAppLimited() {
	round := e.inner.round
	if round == e.limitRound {
		return
	}
	e.limitRound = round
	peak, held := float64(e.limitPeak), e.limitHeld
	e.limitPeak, e.limitHeld = 0, false
	allowed := float64(e.GetCongestionWindow())
	rtt := e.inner.minRoundTrip()
	if rtt <= 0 {
		rtt = e.rampBaseRTT
	}
	if rtt > 0 {
		allowed = math.Min(allowed, float64(e.inner.bandwidth())/e.arrivalRate()*rtt.Seconds())
	}
	if held && 4*peak >= allowed {
		e.limitIdle, e.appLimited = 0, false
	} else if e.limitIdle++; e.limitIdle >= appLimitedRounds {
		e.appLimited = true
	}
	e.appLimitedTrace.Store(e.appLimited)
}

// rearm puts a lane back into its ramp, and its controller into startup.
func (e *ErasureSender) rearm(delivered float64) {
	e.ramping.Store(true)
	e.inner.startupHeld = true
	e.inner.reenterStartup()
	e.rampDelivered, e.rampFlat = delivered, 0
	e.earnArrived, e.earnLost, e.earnRounds = 0, 0, 0
	e.rearmFrom, e.departed, e.lastShare = delivered, false, 0
	// The pool's minimum now, not at join. Kept from the join, a route whose
	// round trip rose would read as standing queue to every later ramp and
	// end it on its first event; the pool's current figure is the least of
	// its members' own expiring minima, so it follows the route. Dropped
	// altogether, a lane re-armed behind a standing queue -- which it has
	// measured its own minimum inside -- sees none: simulated with rescue
	// joiners keeping a 50 ms buffer full, the caused loss rose from 0.14
	// to 0.21.
	e.rampBaseRTT = e.poolRTT
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
	ok := e.pacer.budget(now) >= e.inner.maxDatagramSize
	if !ok {
		e.pacingDenials.Add(1)
		e.denied = true
	}
	return ok
}

func (e *ErasureSender) OnPacketSent(sentTime monotime.Time, bytesInFlight quiccongestion.ByteCount, number quiccongestion.PacketNumber, bytes quiccongestion.ByteCount, retransmittable bool) {
	if retransmittable {
		if e.denied {
			e.heldSends.Add(1)
			e.denied = false
			e.limitHeld = true
		}
		if bytesInFlight > e.limitPeak {
			e.limitPeak = bytesInFlight
		}
	} else {
		e.ackOnlyBytes.Add(uint64(bytes))
		e.ackOnlyCharged.Add(uint64(bytes))
	}
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
	ok := bytesInFlight < e.GetCongestionWindow()
	if !ok {
		e.windowDenials.Add(1)
		e.denied = true
	}
	return ok
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
	var ackedBytes uint64
	for _, packet := range acked {
		ackedBytes += uint64(packet.BytesAcked)
	}
	e.ackedBytes.Add(ackedBytes)
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
		e.judgeAppLimited()
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
			Arrival:    e.appliedArrival,
			AppLimited: e.appLimited,
		})
		erasure = state.Erasure
		e.erasure.Store(uint64(state.Erasure * partsPerMillion))
		e.share.Store(uint64(state.Share))
		e.poolSamples, e.poolBurst = state.ErasureSamples, state.ErasureBurst
		e.selfSamples, e.selfLoss, e.selfObserved = snapshot.Samples, snapshot.Loss, float64(snapshot.Decided)
		e.selfBurst = snapshot.BurstFactor
		e.poolWeight, e.poolErasure = state.ErasureWeight, state.Erasure
		e.poolRTT = state.LeafRoundTrip
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
	// What the pacer will use, published for readers outside this goroutine.
	e.pacing.Store(uint64(e.bandwidth()))
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
	// The rate the pacer uses, after the share and the delay brake, once the
	// controller has run: the inner rate over the arrival is what it would
	// send uncapped, and a lane admitted against that (quicLaneOutstanding)
	// could take on several times what it sends in a round trip.
	if pacing := e.pacing.Load(); pacing > 0 {
		t.PacingRate = pacing
	}
	t.CongestionWindow = uint64(float64(t.CongestionWindow) / arrival)
	t.Share, t.ShareCapped = e.share.Load(), e.capBound.Load()
	t.UncappedPacing = uint64(float64(e.uncappedPacing.Load()) / arrival)
	t.AppLimited = e.appLimitedTrace.Load()
	t.AckOnlyBytes, t.AckOnlyCharged = e.ackOnlyBytes.Load(), e.ackOnlyCharged.Load()
	t.AckedBytes = e.ackedBytes.Load()
	t.PacingDenials, t.WindowDenials, t.HeldSends = e.pacingDenials.Load(), e.windowDenials.Load(), e.heldSends.Load()
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

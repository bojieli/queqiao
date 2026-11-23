package congestion

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	quiccongestion "github.com/apernet/quic-go/congestion"
	"github.com/apernet/quic-go/monotime"

	"github.com/bojieli/queqiao/internal/pathmodel"
)

// This file is a deterministic, packet-level simulation of a burst of lanes
// joining one pooled path model, which is the shape of every live download
// trial in the #146 A/B: a pool that has idled down to a trickle, then four
// bulk lanes opened at once.
//
// It runs the real ErasureSender and the real PathModel. Only the network is
// simulated: one FIFO bottleneck with a finite buffer, erasure drawn from a
// seeded generator after the queue, and a fixed propagation round trip. Time
// is simulated too -- the model reads it through NewPathModelClocked and the
// senders through the event times they are handed -- so a run is the same run
// on every machine.

// simRTT is the round-trip provider a simulated lane hands its sender, kept
// the way quic-go keeps it: a minimum, the latest sample and a smoothed one.
type simRTT struct{ min, latest, smoothed time.Duration }

func (r *simRTT) MinRTT() time.Duration        { return r.min }
func (r *simRTT) LatestRTT() time.Duration     { return r.latest }
func (r *simRTT) SmoothedRTT() time.Duration   { return r.smoothed }
func (r *simRTT) MeanDeviation() time.Duration { return 0 }
func (r *simRTT) MaxAckDelay() time.Duration   { return 0 }
func (r *simRTT) PTO(bool) time.Duration       { return 2 * r.smoothed }
func (r *simRTT) UpdateRTT(_, _ time.Duration) {}
func (r *simRTT) SetMaxAckDelay(time.Duration) {}
func (r *simRTT) SetInitialRTT(time.Duration)  {}

func (r *simRTT) observe(sample time.Duration) {
	r.latest = sample
	if r.min == 0 || sample < r.min {
		r.min = sample
	}
	if r.smoothed == 0 {
		r.smoothed = sample
		return
	}
	r.smoothed = (7*r.smoothed + sample) / 8
}

// joinBurst describes one simulated burst.
type joinBurst struct {
	// The path: bottleneck rate in bytes per second, propagation round trip,
	// rate-independent erasure, and the bottleneck's buffer in bytes.
	rate    float64
	rtt     time.Duration
	erasure float64
	buffer  float64
	// lanes join at the start, stagger apart, and run for duration.
	lanes    int
	stagger  time.Duration
	duration time.Duration
	// idle is the delivered rate of a member that is already in the pool and
	// stays there without using its share -- the pooled control connection a
	// bulk download joins next to. Zero means the lanes join an empty model.
	idle float64
	// policer makes the bottleneck a token bucket holding this many seconds
	// of its rate, which drops what it cannot pass and queues nothing.
	policer time.Duration
	seed    int64
}

type joinBurstResult struct {
	// delivered is every byte acknowledged, and perSecond the same by second.
	delivered float64
	perSecond []float64
	// sent, erased and dropped count packets: dropped are the ones the
	// bottleneck's buffer refused, which is the loss a sender causes.
	sent, erased, dropped uint64
	// queue is the bottleneck's queueing delay as seen by each packet that
	// entered it.
	queue []time.Duration
}

func (r joinBurstResult) goodput(d time.Duration) float64 { return r.delivered / d.Seconds() }

// queueP90 is the 90th percentile queueing delay at the bottleneck.
func (r joinBurstResult) queueP90() time.Duration {
	if len(r.queue) == 0 {
		return 0
	}
	q := append([]time.Duration(nil), r.queue...)
	sort.Slice(q, func(i, j int) bool { return q[i] < q[j] })
	return q[len(q)*9/10]
}

// causedLoss is the fraction of sent packets the bottleneck dropped.
func (r joinBurstResult) causedLoss() float64 {
	if r.sent == 0 {
		return 0
	}
	return float64(r.dropped) / float64(r.sent)
}

type simPacket struct {
	number quiccongestion.PacketNumber
	sent   time.Duration
	due    time.Duration
	lost   bool
}

type simLane struct {
	e        *ErasureSender
	rtt      simRTT
	next     quiccongestion.PacketNumber
	inFlight quiccongestion.ByteCount
	credit   float64
	pending  []simPacket
}

const (
	simPacketSize = 1200
	simTick       = time.Millisecond
)

func runJoinBurst(cfg joinBurst) joinBurstResult {
	base := monotime.Now()
	wall := time.Now()
	var now time.Duration
	model := pathmodel.NewPathModelClocked(func() time.Time { return wall.Add(now) })
	rng := rand.New(rand.NewSource(cfg.seed))
	const idleMember = pathmodel.Member(1 << 62)
	reportIdle := func() {
		model.Report(idleMember, pathmodel.Observation{
			Erasure: cfg.erasure, BurstFactor: 1, ObservedSamples: 5000,
			Delivered: cfg.idle, RoundTrip: cfg.rtt, Arrival: math.Max(1-cfg.erasure, erasureMinArrival),
		})
	}
	if cfg.idle > 0 {
		reportIdle()
	}

	var result joinBurstResult
	result.perSecond = make([]float64, int(cfg.duration/time.Second)+1)
	var busyUntil, refilled time.Duration
	tokens := 0.0
	lanes := make([]*simLane, 0, cfg.lanes)
	for now = 0; now < cfg.duration; now += simTick {
		if cfg.idle > 0 && now%(100*time.Millisecond) == 0 {
			reportIdle()
		}
		if len(lanes) < cfg.lanes && now >= time.Duration(len(lanes))*cfg.stagger {
			lane := &simLane{e: NewErasureSenderOn(simPacketSize, model), next: 1}
			// The gain cycle's phase is drawn from the clock; fix it so the run
			// is reproducible.
			lane.e.inner.randomState = uint64(len(lanes)+1) * 0x9e3779b97f4a7c15
			lane.e.SetRTTStatsProvider(&lane.rtt)
			lanes = append(lanes, lane)
		}
		at := base.Add(now)
		for _, lane := range lanes {
			// Acknowledgements and losses that have come due.
			var acked []quiccongestion.AckedPacketInfo
			var lost []quiccongestion.LostPacketInfo
			done := 0
			for _, p := range lane.pending {
				if p.due > now {
					break
				}
				done++
				if p.lost {
					lost = append(lost, quiccongestion.LostPacketInfo{PacketNumber: p.number, BytesLost: simPacketSize})
					continue
				}
				acked = append(acked, quiccongestion.AckedPacketInfo{PacketNumber: p.number, BytesAcked: simPacketSize, ReceivedTime: at})
				lane.rtt.observe(p.due - p.sent)
				result.delivered += simPacketSize
				result.perSecond[int(now/time.Second)] += simPacketSize
			}
			if done > 0 {
				lane.pending = lane.pending[done:]
				prior := lane.inFlight
				lane.inFlight -= quiccongestion.ByteCount(done * simPacketSize)
				lane.e.OnCongestionEventEx(prior, at, acked, lost)
			}

			// Send what the pacer and the window allow. The credit is capped
			// at a pacer's burst, or two ticks of the rate when that is more,
			// so an idle lane does not bank a flood and a fast one is not
			// limited by the tick.
			rate := float64(lane.e.bandwidth())
			lane.credit += rate * simTick.Seconds()
			if limit := math.Max(tuicMaxBurstPackets*simPacketSize, 2*rate*simTick.Seconds()); lane.credit > limit {
				lane.credit = limit
			}
			for lane.credit >= simPacketSize && lane.e.CanSend(lane.inFlight) {
				lane.credit -= simPacketSize
				lane.inFlight += simPacketSize
				number := lane.next
				lane.next++
				lane.e.OnPacketSent(at, lane.inFlight, number, simPacketSize, true)
				result.sent++

				start := now
				if busyUntil > start {
					start = busyUntil
				}
				p := simPacket{number: number, sent: now}
				if cfg.policer > 0 {
					tokens = math.Min(tokens+(now-refilled).Seconds()*cfg.rate, cfg.policer.Seconds()*cfg.rate)
					refilled = now
					if tokens < simPacketSize {
						result.dropped++
						p.lost, p.due = true, now+cfg.rtt
					} else {
						tokens -= simPacketSize
						p.due = now + cfg.rtt
						if rng.Float64() < cfg.erasure {
							result.erased++
							p.lost = true
						}
					}
				} else if backlog := (start - now).Seconds() * cfg.rate; backlog+simPacketSize > cfg.buffer {
					// Tail drop. It is detected with the packets around it,
					// which is when the queue would have delivered it.
					result.dropped++
					p.lost, p.due = true, start+cfg.rtt
				} else {
					result.queue = append(result.queue, start-now)
					busyUntil = start + time.Duration(simPacketSize/cfg.rate*float64(time.Second))
					p.due = busyUntil + cfg.rtt
					if rng.Float64() < cfg.erasure {
						result.erased++
						p.lost = true
					}
				}
				lane.pending = append(lane.pending, p)
			}
		}
	}
	return result
}

// The fast path the live A/B ran on: about 150 Mbit/s, a 170 ms round trip,
// a 15% erasure floor, and a shallow buffer -- the A arm never saw more than a
// few milliseconds of queue there. The pool holds one member that is not
// using its share, as the pooled control connection is not, delivering what
// the live pool had idled down to between trials.
func fastModerateErasurePath() joinBurst {
	return joinBurst{
		rate: 18e6, rtt: 170 * time.Millisecond, erasure: 0.15, buffer: 18e6 * 0.012,
		lanes: 4, stagger: 20 * time.Millisecond, duration: 8 * time.Second,
		idle: 60e3, seed: 1,
	}
}

func (cfg joinBurst) report(t *testing.T, name string, r joinBurstResult) float64 {
	t.Helper()
	capacity := cfg.rate * (1 - cfg.erasure)
	ratio := r.goodput(cfg.duration) / capacity
	t.Logf("%s: goodput %.2f MB/s (%.2f of capacity), caused loss %.3f, queue p90 %v, MB per second %v",
		name, r.goodput(cfg.duration)/1e6, ratio, r.causedLoss(), r.queueP90().Round(100*time.Microsecond), mbps(r.perSecond))
	return ratio
}

// The regression the live A/B found, reproduced. Four lanes join a pool that
// has idled down to one member using 60 kB/s of a path that delivers 15 MB/s.
// The share is 1.25x an even split of what the pool measured, the idle member
// takes a fifth of it, and the lanes are left exactly what they delivered:
// before the ramp they reached 0.14 of the path in twelve seconds, rising by
// about a quarter a second, with no loss above the floor and no queue --
// which is the live trace, a path under-used rather than overdriven.
func TestABurstOfJoinersBesideAnIdleMemberFindsAFastPath(t *testing.T) {
	cfg := fastModerateErasurePath()
	r := runJoinBurst(cfg)
	if got := cfg.report(t, "fast path, idle pool", r); got < 0.6 {
		t.Errorf("the lanes delivered %.2f of a path with room for all of it; the share is acting as a ceiling", got)
	}
	// The path's own brakes still hold the burst. This is not a tight bound:
	// on a buffer this shallow the controller has no queue to read, and the
	// pool's own regime at capacity -- a burst into an empty pool, which this
	// change does not touch -- loses more (see docs/CONTROL-REDESIGN.md).
	if got := r.causedLoss(); got > 0.5 {
		t.Errorf("the bottleneck dropped %.2f of what was sent; the ramp is not ending", got)
	}
}

// The path the change must not cost: heavy erasure, a deep buffer, a slower
// bottleneck -- the Mac leg of the A/B, where #146 cut the queue p90 from
// about a second to 168 ms. The lanes stop ramping at the first quarter
// round trip of queue and are held to their shares from there.
func TestJoinersOnAHighErasureDeepBufferPathDoNotOverdriveIt(t *testing.T) {
	cfg := fastModerateErasurePath()
	cfg.rate, cfg.erasure, cfg.buffer, cfg.rtt = 2.6e6, 0.4, 2.6e6*0.3, 200*time.Millisecond
	r := runJoinBurst(cfg)
	cfg.report(t, "high-erasure path, idle pool", r)
	if got := r.causedLoss(); got > 0.01 {
		t.Errorf("the bottleneck dropped %.3f of what was sent on a buffer holding 300 ms", got)
	}
	// The shares alone hold about 245 ms of queue here, as they do without
	// the ramp; the bound is what the ramp may add to that.
	if got := r.queueP90(); got > 280*time.Millisecond {
		t.Errorf("queue p90 %v; the ramp did not stop at the queue", got)
	}
}

// A policer queues nothing and its drops are part of what the pool measured,
// so the queue and the floor are both blind to it. What ends the ramp there is
// the policer refusing more than the floor and delivery that stops following.
func TestJoinersOnAPolicerAreBounded(t *testing.T) {
	cfg := fastModerateErasurePath()
	cfg.rate, cfg.erasure, cfg.policer, cfg.rtt = 2e6, 0, 8*time.Millisecond, 300*time.Millisecond
	r := runJoinBurst(cfg)
	if got := cfg.report(t, "policer, idle pool", r); got < 0.5 {
		t.Errorf("the lanes delivered %.2f of the policer's rate", got)
	}
	if got := r.causedLoss(); got > 0.3 {
		t.Errorf("the policer dropped %.2f of what was sent", got)
	}
}

// A lane joining a path its sibling already fills meets the queue or the loss
// within a round or two and takes its share from there.
func TestAJoinerBesideALaneThatFillsThePathBacksOff(t *testing.T) {
	cfg := fastModerateErasurePath()
	cfg.lanes, cfg.stagger, cfg.buffer = 2, 5*time.Second, cfg.rate*0.085
	r := runJoinBurst(cfg)
	cfg.report(t, "joiner beside a full lane", r)
	if got := r.causedLoss(); got > 0.05 {
		t.Errorf("the bottleneck dropped %.3f of what was sent", got)
	}
	if got := r.queueP90(); got > cfg.rtt/2 {
		t.Errorf("queue p90 %v on a %v path", got, cfg.rtt)
	}
}

// The gate itself.

func joiningProbe() *seedObservationProbe {
	return &seedObservationProbe{State: pathmodel.State{
		Seed: 1e6, Share: 1.25e6, Erasure: 0.15, ObservedSamples: 5000, Arrival: 0.85, RoundTrip: joiningRoundTrip,
	}}
}

func TestAJoinerIsNotCappedByItsShareUntilItsRampEnds(t *testing.T) {
	if NewErasureSenderOn(1200, &seedObservationProbe{}).ramping.Load() {
		t.Fatal("a lane joining a model with nothing in it ramps; it has no share to be exempt from")
	}
	sender := NewErasureSenderOn(1200, joiningProbe())
	if !sender.ramping.Load() {
		t.Fatal("a lane joining from a pooled seed is not ramping")
	}
	inner := float64(sender.inner.bandwidth())
	if inner <= 1.25e6 {
		t.Fatalf("the seeded startup rate %.0f is not above the share; the test cannot tell", inner)
	}
	if got := sender.deliveredRate(); got != inner {
		t.Errorf("a ramping lane is held at %.0f, want its own %.0f", got, inner)
	}
	sender.endRamp()
	if got := sender.deliveredRate(); got != 1.25e6 {
		t.Errorf("after its ramp the lane runs at %.0f, want its share", got)
	}
	if !sender.inner.fullBandwidth {
		t.Error("the ramp ended and the controller is still in startup")
	}
}

func TestARampEndsAtAQuarterRoundTripOfQueue(t *testing.T) {
	for _, tc := range []struct {
		smoothed time.Duration
		ramping  bool
	}{{240 * time.Millisecond, true}, {250 * time.Millisecond, false}} {
		sender := NewErasureSenderOn(1200, joiningProbe())
		sender.inner.minRTT = joiningRoundTrip
		sender.SetRTTStatsProvider(&fixedRTT{min: joiningRoundTrip, smoothed: tc.smoothed})
		ackEvent(sender)
		if got := sender.ramping.Load(); got != tc.ramping {
			t.Errorf("smoothed %v over a %v minimum: ramping %v, want %v", tc.smoothed, joiningRoundTrip, got, tc.ramping)
		}
	}
}

// closeRound hands the gate one round's outcomes and then the event that
// closes it, with delivery still growing so that only the loss is judged.
func closeRound(e *ErasureSender, arrived, lost int) {
	e.rampGate(make([]quiccongestion.AckedPacketInfo, arrived), make([]quiccongestion.LostPacketInfo, lost))
	e.inner.round++
	e.inner.estimator.measured.updateMax(e.inner.round, monotime.Time(0), uint64(e.rampDelivered*rampGrowth)+1)
	e.rampGate(nil, nil)
}

func TestARampEndsOnLossAboveTheFloorButNotOnItsNoise(t *testing.T) {
	for _, tc := range []struct {
		arrived, lost int
		ramping       bool
	}{
		{85, 15, true}, // the floor
		{78, 22, true}, // two deviations above it at this count
		{22, 10, true}, // a first flight: too few to judge
		{70, 30, false},
		{60, 40, false},
	} {
		sender := NewErasureSenderOn(1200, joiningProbe())
		closeRound(sender, tc.arrived, tc.lost)
		if got := sender.ramping.Load(); got != tc.ramping {
			t.Errorf("%d of %d lost on a 0.15 floor: ramping %v, want %v", tc.lost, tc.arrived+tc.lost, got, tc.ramping)
		}
	}
}

func TestARampEndsWhenDeliveryStopsGrowing(t *testing.T) {
	sender := NewErasureSenderOn(1200, joiningProbe())
	sender.inner.estimator.measured.updateMax(1, monotime.Time(0), 1e6)
	for round := 1; round <= rampFlatRounds+1; round++ {
		if !sender.ramping.Load() {
			// The first round grows from nothing; the next three do not.
			if round != rampFlatRounds+1 {
				t.Fatalf("the ramp ended after %d rounds, want %d without growth", round-2, rampFlatRounds)
			}
			return
		}
		sender.inner.round++
		sender.rampGate(nil, nil)
	}
	if sender.ramping.Load() {
		t.Fatalf("delivery flat for %d rounds and the lane is still ramping", rampFlatRounds)
	}
}

// Delivery rises during a ramp because the ramp raises the rate, so it is no
// evidence that compensation bought anything.
func TestCompensationIsNotEarnedDuringARamp(t *testing.T) {
	probe := joiningProbe()
	probe.Erasure, probe.Arrival = 0.42, 1
	sender := NewErasureSenderOn(1200, probe)
	grow := func() {
		sender.inner.round++
		sender.inner.estimator.maxFilter.updateMax(sender.inner.round, monotime.Time(0), sender.inner.estimator.estimate()*2)
	}
	for i := 0; i < 4; i++ {
		grow()
		if got := sender.compensationFor(0.58); got != 1 {
			t.Fatalf("round %d of a ramp granted compensation to %.3f", i+1, got)
		}
	}
	sender.endRamp()
	grow()
	if got := sender.compensationFor(0.58); got >= 1 {
		t.Fatalf("after the ramp, a round that delivered more was refused compensation (%.3f)", got)
	}
}

func mbps(xs []float64) []string {
	out := make([]string, len(xs))
	for i, x := range xs {
		out[i] = fmt.Sprintf("%.1f", x/1e6)
	}
	return out
}

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
	// sentPerSecond and droppedPerSecond count packets by the second they
	// were sent in, so an interval can be judged on its own.
	sentPerSecond, droppedPerSecond []uint64
	// joined and rampEnded are when each lane joined and when it stopped
	// ramping, rampEnded negative for a lane that never ramped or never
	// stopped.
	joined, rampEnded []time.Duration
}

// causedLossBetween is the fraction of packets sent in [from, to) seconds
// that the bottleneck dropped.
func (r joinBurstResult) causedLossBetween(from, to int) float64 {
	var sent, dropped uint64
	for i := from; i < to && i < len(r.sentPerSecond); i++ {
		sent += r.sentPerSecond[i]
		dropped += r.droppedPerSecond[i]
	}
	if sent == 0 {
		return 0
	}
	return float64(dropped) / float64(sent)
}

// goodputBetween is what was delivered in [from, to) seconds, per second.
func (r joinBurstResult) goodputBetween(from, to int) float64 {
	var delivered float64
	for i := from; i < to && i < len(r.perSecond); i++ {
		delivered += r.perSecond[i]
	}
	return delivered / float64(to-from)
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
	seconds := int(cfg.duration/time.Second) + 1
	result.perSecond = make([]float64, seconds)
	result.sentPerSecond = make([]uint64, seconds)
	result.droppedPerSecond = make([]uint64, seconds)
	var busyUntil, refilled time.Duration
	tokens := 0.0
	lanes := make([]*simLane, 0, cfg.lanes)
	for now = 0; now < cfg.duration; now += simTick {
		if cfg.idle > 0 && now%(100*time.Millisecond) == 0 {
			reportIdle()
		}
		at := base.Add(now)
		if len(lanes) < cfg.lanes && now >= time.Duration(len(lanes))*cfg.stagger {
			lane := &simLane{e: NewErasureSenderOn(simPacketSize, model), next: 1}
			// The gain cycle's phase is drawn from the clock; fix it so the run
			// is reproducible.
			lane.e.inner.randomState = uint64(len(lanes)+1) * 0x9e3779b97f4a7c15
			lane.e.SetRTTStatsProvider(&lane.rtt)
			// The seed was stamped with the wall clock; restamp it with the
			// simulated one, or a lane joining seconds into a run starts with
			// a seed that already looks stale.
			if seed := lane.e.inner.estimator.maxFilter.get(); seed > 0 {
				filter := newTUICMinMax()
				filter.setRoundTrip(cfg.rtt)
				filter.updateMax(lane.e.inner.round, at, seed)
				lane.e.inner.estimator.maxFilter = filter
			}
			lanes = append(lanes, lane)
			result.joined = append(result.joined, now)
			result.rampEnded = append(result.rampEnded, -1)
		}
		for i, lane := range lanes {
			if result.rampEnded[i] < 0 && lane.e.rampRound > 0 && !lane.e.ramping.Load() {
				result.rampEnded[i] = now
			}
			// Acknowledgements and losses that have come due.
			var acked []quiccongestion.AckedPacketInfo
			var lost []quiccongestion.LostPacketInfo
			done := 0
			var sample time.Duration
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
				sample = p.due - p.sent
				result.delivered += simPacketSize
				result.perSecond[int(now/time.Second)] += simPacketSize
			}
			// QUIC takes one round-trip sample per acknowledgement, from its
			// largest newly acknowledged packet, not one per packet it covers.
			if sample > 0 {
				lane.rtt.observe(sample)
			}
			if done > 0 {
				lane.pending = lane.pending[done:]
				prior := lane.inFlight
				lane.inFlight -= quiccongestion.ByteCount(done * simPacketSize)
				lane.e.OnCongestionEventEx(prior, at, acked, lost)
			}

			// Send what the window and the controller's own pacer allow, as
			// quic-go asks them: the pacer's burst allowance, including the
			// unmetered burst, is part of what is being simulated.
			for lane.e.CanSend(lane.inFlight) && lane.e.HasPacingBudget(at) {
				lane.inFlight += simPacketSize
				number := lane.next
				lane.next++
				lane.e.OnPacketSent(at, lane.inFlight, number, simPacketSize, true)
				result.sent++
				result.sentPerSecond[int(now/time.Second)]++

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
						result.droppedPerSecond[int(now/time.Second)]++
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
					result.droppedPerSecond[int(now/time.Second)]++
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
	// And it is not bought by overdriving it. A buffer this shallow gives the
	// controller no queue to read, so loss above the floor is the only brake;
	// the same burst into an empty pool, which this change does not touch,
	// has the bottleneck drop 0.55 of what is sent.
	if got := r.causedLoss(); got > 0.2 {
		t.Errorf("the bottleneck dropped %.2f of what was sent", got)
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

// A lane joining a path its sibling already fills meets the loss or the
// queue within a few rounds and takes its share from there. Judged on the
// joiner and on the interval after it joined, not averaged with the five
// seconds before it.
func TestAJoinerBesideALaneThatFillsThePathBacksOff(t *testing.T) {
	cfg := fastModerateErasurePath()
	cfg.lanes, cfg.stagger, cfg.buffer = 2, 5*time.Second, cfg.rate*0.085
	cfg.duration = 9 * time.Second
	r := runJoinBurst(cfg)
	cfg.report(t, "joiner beside a full lane", r)
	joined, ended := r.joined[1], r.rampEnded[1]
	t.Logf("the joiner joined at %v and stopped ramping at %v; after it joined, caused loss %.3f",
		joined, ended, r.causedLossBetween(5, 9))
	if ended < 0 || ended-joined > 8*cfg.rtt {
		t.Errorf("the joiner was still ramping %v after joining a full path", ended-joined)
	}
	if got := r.causedLossBetween(5, 9); got > 0.1 {
		t.Errorf("after the join the bottleneck dropped %.3f of what was sent", got)
	}
	// Both lanes together still use the path.
	if got := r.goodputBetween(6, 9) / (cfg.rate * (1 - cfg.erasure)); got < 0.8 {
		t.Errorf("after the join the lanes delivered %.2f of the path", got)
	}
}

// The gate itself.

func joiningProbe() *seedObservationProbe {
	return &seedObservationProbe{State: pathmodel.State{
		Seed: 1e6, Share: 1.25e6, Erasure: 0.15, ObservedSamples: 5000, ErasureSamples: 5000, Arrival: 0.85, RoundTrip: joiningRoundTrip,
		BurstFactor: 1, ErasureBurst: 1,
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
	e.rampGate(base(), make([]quiccongestion.AckedPacketInfo, arrived), make([]quiccongestion.LostPacketInfo, lost))
	e.inner.round++
	e.inner.estimator.measured.updateMax(e.inner.round, monotime.Time(0), uint64(e.rampDelivered*rampGrowth)+1)
	e.rampGate(base(), nil, nil)
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
	// A pool that has decided a handful of packets reports an erasure of zero
	// with nothing behind it. One ordinary loss is not loss above that.
	// The high-water mark of what the pool ever observed may belong to a
	// member that has expired; it is not what stands behind this figure.
	thin := joiningProbe()
	thin.Erasure, thin.ErasureSamples = 0, 10
	sender := NewErasureSenderOn(1200, thin)
	closeRound(sender, 63, 1)
	if !sender.ramping.Load() {
		t.Error("one loss in 64 ended a ramp against a floor measured from 10 packets")
	}
	// Against a floor of zero that does have packets behind it, a couple of
	// losses are too few to judge and a tenth of the round is not.
	measured := joiningProbe()
	measured.Erasure = 0
	for _, tc := range []struct {
		lost    int
		ramping bool
	}{{1, true}, {2, true}, {10, false}} {
		sender := NewErasureSenderOn(1200, measured)
		closeRound(sender, 100-tc.lost, tc.lost)
		if got := sender.ramping.Load(); got != tc.ramping {
			t.Errorf("%d of 100 lost on a measured clean path: ramping %v, want %v", tc.lost, got, tc.ramping)
		}
	}
}

func TestARampEndsWhenDeliveryStopsGrowing(t *testing.T) {
	sender := NewErasureSenderOn(1200, joiningProbe())
	sender.inner.estimator.measured.updateMax(1, monotime.Time(0), 1e6)
	// The first round grows from nothing; the next three do not, and in each
	// the lane had its window in flight.
	for round := 1; round <= rampFlatRounds+1; round++ {
		if !sender.ramping.Load() {
			t.Fatalf("the ramp ended after %d flat rounds, want %d", round-2, rampFlatRounds)
		}
		sendAt(sender, sender.GetCongestionWindow())
		sender.inner.round++
		sender.rampGate(base(), nil, nil)
	}
	if sender.ramping.Load() {
		t.Fatalf("delivery flat for %d rounds and the lane is still ramping", rampFlatRounds)
	}
}

// sendAt records one packet sent with inFlight on the wire.
func sendAt(e *ErasureSender, inFlight quiccongestion.ByteCount) {
	e.OnPacketSent(monotime.Now(), inFlight, quiccongestion.PacketNumber(e.inner.round+1)*1000, 1200, true)
}

// A lane carrying a few requests before its bulk transfer is not probing the
// path, and its flat rounds must not spend the ramp the transfer needs --
// even when the last packet of each request exhausts the pacer, which is
// why the evidence is what was in flight rather than a refused poll.
func TestRequestRoundsDoNotEndARamp(t *testing.T) {
	sender := NewErasureSenderOn(1200, joiningProbe())
	sender.inner.estimator.measured.updateMax(1, monotime.Time(0), 1e6)
	for round := 0; round < 4*rampFlatRounds; round++ {
		sendAt(sender, 1200)
		sender.inner.round++
		sender.rampGate(base(), nil, nil)
	}
	if !sender.ramping.Load() {
		t.Fatal("rounds in which nothing held the lane back ended its ramp")
	}
	// Then the backlog arrives, half the window and more is in flight, and
	// delivery still does not grow: that is a full path.
	for round := 0; round < rampFlatRounds; round++ {
		sendAt(sender, sender.GetCongestionWindow())
		sender.inner.round++
		sender.rampGate(base(), nil, nil)
	}
	if sender.ramping.Load() {
		t.Fatal("a lane held back by its window for three flat rounds is still ramping")
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

// Loss the pool measures above the erasure it had when a lane joined is the
// burst's own overflow until shown otherwise, and the lane that is not losing
// it is the one most likely to be causing it: simulated, a lane whose own
// loss sat at the floor took its compensation from 0.85 to 0.43 while its
// siblings lost 35-50%.
func TestCompensationIsNotEarnedWhileThePoolLosesAboveItsFloor(t *testing.T) {
	for _, tc := range []struct {
		pooled  float64
		granted bool
	}{{0.15, true}, {0.30, false}} {
		probe := joiningProbe()
		probe.Erasure, probe.Arrival = 0.15, 1
		sender := NewErasureSenderOn(1200, probe)
		sender.endRamp()
		sender.poolSamples = 5000
		sender.erasure.Store(uint64(tc.pooled * partsPerMillion))
		sender.inner.round++
		sender.inner.estimator.maxFilter.updateMax(sender.inner.round, monotime.Time(0), sender.inner.estimator.estimate()*2)
		if got := sender.compensationFor(1 - tc.pooled); (got < 1) != tc.granted {
			t.Errorf("pool erasure %.2f on a 0.15 floor: compensation %.3f, granted %v, want %v", tc.pooled, got, got < 1, tc.granted)
		}
	}
}

func mbps(xs []float64) []string {
	out := make([]string, len(xs))
	for i, x := range xs {
		out[i] = fmt.Sprintf("%.1f", x/1e6)
	}
	return out
}

// The floor a lane joined with is not forever. Loss the pool measures above
// it earns no compensation until shown otherwise, and a whole bottleneck
// window of it shows otherwise: the path's erasure has changed.
func TestAJoinedLanesFloorFollowsAPathWhoseErasureRose(t *testing.T) {
	probe := joiningProbe()
	probe.Erasure, probe.Arrival = 0.05, 1
	sender := NewErasureSenderOn(1200, probe)
	sender.endRamp()
	sender.rampBurst = 3
	sender.poolSamples, sender.poolBurst = 5000, 1
	sender.erasure.Store(uint64(0.30 * partsPerMillion))
	grant := func() float64 {
		sender.inner.round++
		sender.inner.estimator.maxFilter.updateMax(sender.inner.round, monotime.Time(0), sender.inner.estimator.estimate()*2)
		return sender.compensationFor(0.70)
	}
	start := base()
	for at := start; at.Sub(start) < floorRebaseAfter; at = at.Add(time.Second) {
		sender.rampGate(at, nil, nil)
		if got := grant(); got != 1 {
			t.Fatalf("%v after the pool rose above the floor, compensation was granted (%.3f)", at.Sub(start), got)
		}
	}
	sender.rampGate(start.Add(floorRebaseAfter), nil, nil)
	if got := sender.rampFloor; got != 0.30 {
		t.Fatalf("after a whole window above it, the floor is still %.3f", got)
	}
	if got := sender.rampBurst; got != 1 {
		t.Fatalf("the rebased floor kept a burst factor of %.1f", got)
	}
	if got := grant(); got >= 1 {
		t.Fatal("against the rebased floor, a round that delivered more was refused compensation")
	}
}

// Erasure that arrives in runs is fewer independent trials than packets. A
// 30-packet run in a 100-packet round on a 15% floor is a burst, not
// overflow, on a pool that measured its losses arriving in runs of four.
func TestABurstOfErasureOnABurstyFloorDoesNotEndARamp(t *testing.T) {
	for _, tc := range []struct {
		burst   float64
		ramping bool
	}{{1, false}, {4, true}} {
		probe := joiningProbe()
		probe.BurstFactor, probe.ErasureBurst = tc.burst, tc.burst
		sender := NewErasureSenderOn(1200, probe)
		closeRound(sender, 70, 30)
		if got := sender.ramping.Load(); got != tc.ramping {
			t.Errorf("30 of 100 lost on a 0.15 floor with burst factor %.0f: ramping %v, want %v", tc.burst, got, tc.ramping)
		}
	}
}

// The window before a floor is rebased has to be watched. A lane that saw
// the pool above its floor and then went quiet for longer than the window
// has no evidence about it, and must start the interval again.
func TestAnIdleGapDoesNotRebaseTheFloor(t *testing.T) {
	probe := joiningProbe()
	probe.Erasure = 0.05
	sender := NewErasureSenderOn(1200, probe)
	sender.endRamp()
	sender.poolSamples = 5000
	sender.erasure.Store(uint64(0.30 * partsPerMillion))
	start := base()
	sender.rampGate(start, nil, nil)
	sender.rampGate(start.Add(floorRebaseAfter+time.Second), nil, nil)
	if got := sender.rampFloor; got != 0.05 {
		t.Fatalf("one observation either side of an idle gap rebased the floor to %.3f", got)
	}
}

// Only a lane that reached its share has found a full pipe. One that ends its
// ramp well below the share stays in startup, held by the share as before.
func TestARampEndedBelowItsShareStaysInStartup(t *testing.T) {
	for _, tc := range []struct {
		share float64
		left  bool
	}{{1.25e6, true}, {5e6, false}} {
		probe := joiningProbe()
		probe.Share = tc.share
		sender := NewErasureSenderOn(1200, probe)
		sender.endRamp()
		if got := sender.inner.fullBandwidth; got != tc.left {
			t.Errorf("a lane at 1e6 with a %.2g share left startup: %v, want %v", tc.share, got, tc.left)
		}
	}
}

// A lane that joined before the pool could judge a floor takes it once the
// pool can, so its loss guards are not off for life.
func TestAnUnderSampledFloorMatures(t *testing.T) {
	probe := joiningProbe()
	probe.ErasureSamples = 10
	sender := NewErasureSenderOn(1200, probe)
	sender.poolSamples, sender.poolBurst = 5000, 1
	sender.erasure.Store(uint64(0.15 * partsPerMillion))
	sender.rampGate(base(), nil, nil)
	if sender.rampFloorN != 5000 || sender.rampFloor != 0.15 {
		t.Fatalf("floor %.3f on %.0f samples, want 0.15 on 5000", sender.rampFloor, sender.rampFloorN)
	}
	closeRound(sender, 60, 40)
	if sender.ramping.Load() {
		t.Fatal("against the matured floor, 40% loss did not end the ramp")
	}
}

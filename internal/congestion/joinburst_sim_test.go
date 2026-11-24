package congestion

import (
	"fmt"
	"math"
	"math/rand"
	"os"
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
	//
	// What it reports is what such a connection measures. It carries
	// requests, so its compensation gate never sees delivery rise with a step
	// and it has earned nothing: it reports an arrival of 1, unless
	// idleArrival says otherwise. Its loss estimate rests on the few
	// packets it decides (idleSamples, 2000 if zero) and reads idleErasure,
	// which is the path's erasure unless set: a floor measured at a calmer
	// hour, or from a connection whose losses were never the bulk lanes',
	// is the stale floor a joiner inherits.
	idle        float64
	idleArrival float64
	idleErasure float64
	idleSamples float64
	// plan, when set, replaces lanes and stagger: each lane's own join time,
	// when it stops sending, and a request phase before its bulk transfer.
	plan []simLanePlan
	// policer makes the bottleneck a token bucket holding this many seconds
	// of its rate, which drops what it cannot pass and queues nothing.
	policer time.Duration
	seed    int64
}

// simLanePlan is one lane's life in a simulated run.
type simLanePlan struct {
	// join is when the lane opens, and stop when its flow ends: from then it
	// sends nothing and, once what it had in flight has been decided, reports
	// nothing, as a closed connection does. Zero stop runs to the end.
	join, stop time.Duration
	// bulkAt is when the bulk transfer starts. Before it the lane carries
	// requestPackets packets once a round trip, the way a lane carries the
	// requests of a page before the download it was opened for.
	bulkAt         time.Duration
	requestPackets int
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
	// rearms counts ramps started again after one had ended.
	rearms int
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
	plan     simLanePlan
	// requestAt is when the lane last sent a request, in its request phase.
	requestAt time.Duration
	// wasRamping is whether the lane was ramping at the last tick.
	wasRamping bool
}

var simTrace = os.Getenv("QUEQIAO_SIM_TRACE") != ""

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
	idleErasure, idleArrival, idleSamples := cfg.erasure, 1.0, 2000.0
	if cfg.idleErasure > 0 {
		idleErasure = cfg.idleErasure
	}
	if cfg.idleArrival > 0 {
		idleArrival = cfg.idleArrival
	}
	if cfg.idleSamples > 0 {
		idleSamples = cfg.idleSamples
	}
	reportIdle := func() {
		model.Report(idleMember, pathmodel.Observation{
			Erasure: idleErasure, BurstFactor: 1, ObservedSamples: idleSamples, ErasureSamples: idleSamples,
			Delivered: cfg.idle, RoundTrip: cfg.rtt, Arrival: idleArrival,
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
	plan := cfg.plan
	if plan == nil {
		for i := 0; i < cfg.lanes; i++ {
			plan = append(plan, simLanePlan{join: time.Duration(i) * cfg.stagger})
		}
	}
	lanes := make([]*simLane, 0, len(plan))
	for now = 0; now < cfg.duration; now += simTick {
		if cfg.idle > 0 && now%(100*time.Millisecond) == 0 {
			reportIdle()
		}
		at := base.Add(now)
		if simTrace && now%(500*time.Millisecond) == 0 {
			st := model.Current()
			line := fmt.Sprintf("%v pool erasure %.3f/%.0f share %.0f |", now, st.Erasure, st.ErasureSamples, st.Share)
			for i, l := range lanes {
				if l.plan.stop > 0 && now >= l.plan.stop {
					continue
				}
				fl, fn, _ := l.e.floor()
				line += fmt.Sprintf(" %d:arr=%.2f fl=%.2f/%.0f conf=%v ramp=%v above=%v sh=%.1f est=%.1f;", i, l.e.arrivalRate(), fl, fn, l.e.ownConfirmed, l.e.ramping.Load(), l.e.aboveFloor, l.e.Share()/1e6, float64(l.e.inner.estimator.estimate())/1e6)
			}
			fmt.Println(line)
		}
		for len(lanes) < len(plan) && now >= plan[len(lanes)].join {
			lane := &simLane{e: NewErasureSenderOn(simPacketSize, model), next: 1, plan: plan[len(lanes)]}
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
			if ramping := lane.e.ramping.Load(); ramping != lane.wasRamping {
				if ramping && result.rampEnded[i] >= 0 {
					result.rearms++
					if simTrace {
						fmt.Printf("%v lane %d rearm share=%.0f measured=%d alone=%v\n", now, i, lane.e.Share(), lane.e.inner.estimator.measuredEstimate(), lane.e.departed)
					}
				} else if !ramping && simTrace {
					fl, fn, _ := lane.e.floor()
					fmt.Printf("%v lane %d ramp end above=%v floor=%.3f/%.0f own=%.3f pool=%.3f srtt=%v min=%v arrival=%.3f measured=%d\n", now, i, lane.e.aboveFloor, fl, fn, lane.e.ownFloor, lane.e.rampFloor, lane.rtt.smoothed, lane.e.inner.minRTT, lane.e.arrivalRate(), lane.e.inner.estimator.measuredEstimate())
				}
				lane.wasRamping = ramping
			}
			stopped := lane.plan.stop > 0 && now >= lane.plan.stop
			if stopped && len(lane.pending) == 0 {
				continue
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
			budget := -1
			if stopped {
				budget = 0
			} else if now < lane.plan.bulkAt {
				budget = 0
				if now-lane.requestAt >= cfg.rtt {
					budget, lane.requestAt = lane.plan.requestPackets, now
				}
			}
			for budget != 0 && lane.e.CanSend(lane.inFlight) && lane.e.HasPacingBudget(at) {
				budget--
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
		LeafRoundTrip: joiningRoundTrip, BurstFactor: 1, ErasureBurst: 1,
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

// primeOwnFloor closes the lane's first round, which becomes its own floor
// rather than being judged against one: 64 packets losing lost of them, or as
// many more as the burst factor deflates them by.
func primeOwnFloor(e *ErasureSender, lost int) {
	scale := int(math.Ceil(e.laneBurst()))
	closeRound(e, (64-lost)*scale, lost*scale)
	if e.ownFloorN == 0 {
		panic("the first round did not gather the lane's own floor")
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
		primeOwnFloor(sender, 10)
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
	primeOwnFloor(sender, 0)
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
		primeOwnFloor(sender, 0)
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
	// The lane's own packets over the window lose what the pool does: the
	// window was measured, not only watched.
	acked, lost := make([]quiccongestion.AckedPacketInfo, 70), make([]quiccongestion.LostPacketInfo, 30)
	start := base()
	for at := start; at.Sub(start) < floorRebaseAfter; at = at.Add(time.Second) {
		sender.rampGate(at, acked, lost)
		if got := grant(); got != 1 {
			t.Fatalf("%v after the pool rose above the floor, compensation was granted (%.3f)", at.Sub(start), got)
		}
	}
	sender.rampGate(start.Add(floorRebaseAfter), acked, lost)
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
		sender.selfSamples, sender.selfBurst = 1000, tc.burst
		primeOwnFloor(sender, 10)
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

// While a lane ramps, the ramp decides the full pipe. Startup's own loss exit
// would end it on ordinary erasure: on a 23% channel every round has more
// than the 2% and eight loss events it looks for.
func TestStartupDoesNotEndOnErasureWhileRamping(t *testing.T) {
	sender := NewErasureSenderOn(1200, joiningProbe())
	if !sender.inner.startupHeld {
		t.Fatal("a ramping lane's startup is not held")
	}
	sender.inner.lossEventsInRound, sender.inner.bytesLostInRound = 20, 1e6
	sender.inner.checkFullBandwidth(tuicSendState{valid: true, bytesInFlight: 1e6})
	if sender.inner.fullBandwidth {
		t.Fatal("startup's loss exit ended a ramping lane's startup")
	}
	sender.endRamp()
	if sender.inner.startupHeld {
		t.Fatal("the ramp ended and startup is still held")
	}
}

// A lane joining behind a standing queue measures its own minimum inside it,
// so the ramp judges the queue against the pool's minimum as well.
func TestARampSeesAQueueThatPredatesTheJoin(t *testing.T) {
	sender := NewErasureSenderOn(1200, joiningProbe())
	sender.inner.minRTT = 300 * time.Millisecond
	sender.SetRTTStatsProvider(&fixedRTT{min: 300 * time.Millisecond, smoothed: 300 * time.Millisecond})
	ackEvent(sender)
	if sender.ramping.Load() {
		t.Fatalf("a lane whose every sample sat %v above the pool's %v minimum is still ramping", 100*time.Millisecond, joiningRoundTrip)
	}
}

// A lane that joined before the pool could judge a floor takes it once the
// pool can, so its loss guards are not off for life.
func TestAnUnderSampledFloorMatures(t *testing.T) {
	probe := joiningProbe()
	probe.ErasureSamples = 10
	sender := NewErasureSenderOn(1200, probe)
	sender.poolSamples, sender.poolWeight, sender.poolBurst = 5000, 5000, 1
	sender.poolErasure = 0.15
	sender.erasure.Store(uint64(0.15 * partsPerMillion))
	sender.rampGate(base(), nil, nil)
	if sender.rampFloorN != 5000 || sender.rampFloor != 0.15 {
		t.Fatalf("floor %.3f on %.0f samples, want 0.15 on 5000", sender.rampFloor, sender.rampFloorN)
	}
	primeOwnFloor(sender, 10)
	closeRound(sender, 60, 40)
	if sender.ramping.Load() {
		t.Fatal("against the matured floor, 40% loss did not end the ramp")
	}
}

// The Mac leg of the second A/B: about 90 Mbit/s, a 180 ms round trip, 22%
// loss that does not respond to rate, and a 50 ms buffer. The floor a burst
// inherits was measured lower than that -- by a connection that decides few
// packets, at another hour -- and nobody in the pool has earned any
// compensation. Judged against the inherited floor, every round of every lane
// reads as overflow: the ramp ends at its first well-sampled round, the veto
// refuses all compensation, and the lanes pace their delivered rate
// uncompensated, which on this path walks down a fifth a round trip.
func staleFloorLossyPath() joinBurst {
	return joinBurst{
		rate: 11.25e6, rtt: 180 * time.Millisecond, erasure: 0.22, buffer: 11.25e6 * 0.05,
		lanes: 4, stagger: 20 * time.Millisecond, duration: 12 * time.Second,
		idle: 60e3, idleErasure: 0.15, seed: 3,
	}
}

func TestJoinersOnAStaleFloorWithNothingEarnedFillTheLossyPath(t *testing.T) {
	cfg := staleFloorLossyPath()
	r := runJoinBurst(cfg)
	if got := cfg.report(t, "stale floor, nothing earned", r); got < 0.7 {
		t.Errorf("the lanes delivered %.2f of a path with room for all of it", got)
	}
	// The second half, so a run that fills the path and then decays fails.
	if got := r.goodputBetween(6, 12) / (cfg.rate * (1 - cfg.erasure)); got < 0.75 {
		t.Errorf("over the last six seconds the lanes delivered %.2f of the path", got)
	}
	if got := r.causedLoss(); got > 0.15 {
		t.Errorf("the bottleneck dropped %.3f of what was sent", got)
	}
}

// The tail of every live trial: four downloads of different sizes share the
// fast path, three finish, and the last carries on alone. When its siblings
// stop, the capacity they leave is the last lane's, and it must take it up in
// round trips, not the ten seconds ProbeBW's quarter-per-cycle takes.
func TestTheLastLaneRefillsThePathItsSiblingsLeave(t *testing.T) {
	cfg := fastModerateErasurePath()
	cfg.duration = 14 * time.Second
	cfg.plan = []simLanePlan{
		{join: 0, stop: 3 * time.Second},
		{join: 20 * time.Millisecond, stop: 3500 * time.Millisecond},
		{join: 40 * time.Millisecond, stop: 4 * time.Second},
		{join: 60 * time.Millisecond},
	}
	r := runJoinBurst(cfg)
	cfg.report(t, "siblings finish", r)
	capacity := cfg.rate * (1 - cfg.erasure)
	tail := r.goodputBetween(6, 14) / capacity
	t.Logf("after the last sibling finished: goodput %.2f of capacity, caused loss %.3f", tail, r.causedLossBetween(4, 14))
	if tail < 0.75 {
		t.Errorf("alone on the path, the last lane delivered %.2f of it", tail)
	}
	// The lane is alone and uncapped, as any first flow is: one lane on its
	// own on this 12 ms buffer has the bottleneck drop 0.24 of what it sends
	// over the same seconds, from ProbeBW's probes. The bound is that the
	// refill costs no more than that.
	if got := r.causedLossBetween(4, 14); got > 0.15 {
		t.Errorf("refilling the path, the bottleneck dropped %.3f of what was sent", got)
	}
}

// Stall rescue on the lossy path: two lanes carry the transfer, and a new lane
// joins every half second and lives a second and a half -- the 26 rescue
// joiners per trial the Mac traces showed. None of them lives long enough to
// re-base a floor on a ten-second window, so each has to judge the path from
// its own rounds, and none may knock the long lanes off it.
func TestShortLivedRescueJoinersDoNotStarveTheLossyPath(t *testing.T) {
	cfg := staleFloorLossyPath()
	cfg.plan = []simLanePlan{{join: 0}, {join: 20 * time.Millisecond}}
	for at := time.Second; at < cfg.duration-time.Second; at += 500 * time.Millisecond {
		cfg.plan = append(cfg.plan, simLanePlan{join: at, stop: at + 1500*time.Millisecond})
	}
	r := runJoinBurst(cfg)
	if got := cfg.report(t, "rescue churn", r); got < 0.65 {
		t.Errorf("with rescue joiners coming and going, the lanes delivered %.2f of the path", got)
	}
	if got := r.causedLoss(); got > 0.15 {
		t.Errorf("the bottleneck dropped %.3f of what was sent", got)
	}
}

// Lanes that carry a page's requests before the download they were opened
// for: four packets a round trip for two seconds, then the bulk transfer.
// The request rounds must neither spend the ramp nor become the baseline the
// transfer is held to.
func TestRequestsBeforeABulkTransferDoNotSpendItsRamp(t *testing.T) {
	cfg := fastModerateErasurePath()
	cfg.duration = 10 * time.Second
	for i := 0; i < 4; i++ {
		cfg.plan = append(cfg.plan, simLanePlan{join: time.Duration(i) * 20 * time.Millisecond, bulkAt: 2 * time.Second, requestPackets: 4})
	}
	r := runJoinBurst(cfg)
	cfg.report(t, "requests then bulk", r)
	capacity := cfg.rate * (1 - cfg.erasure)
	if got := r.goodputBetween(3, 10) / capacity; got < 0.75 {
		t.Errorf("after the bulk transfer started, the lanes delivered %.2f of the path", got)
	}
	if got := r.causedLossBetween(2, 10); got > 0.2 {
		t.Errorf("the bottleneck dropped %.3f of what was sent", got)
	}
}

// The window before a re-base must also have been measured. The pool's
// estimator forgets by packets, so an overflow burst nobody has sent since
// holds its figure indefinitely; a lane that watched it for ten seconds
// without a packet of its own to show the path changed keeps its floor.
func TestAFloorIsNotRebasedOnAFigureWithNoFreshSamples(t *testing.T) {
	probe := joiningProbe()
	probe.Erasure = 0.05
	sender := NewErasureSenderOn(1200, probe)
	sender.endRamp()
	sender.poolSamples, sender.poolBurst = 5000, 1
	sender.erasure.Store(uint64(0.30 * partsPerMillion))
	start := base()
	for at := start; at.Sub(start) <= floorRebaseAfter; at = at.Add(500 * time.Millisecond) {
		sender.rampGate(at, nil, nil)
	}
	if got := sender.rampFloor; got != 0.05 {
		t.Fatalf("ten seconds watching a pool figure with no fresh samples re-based the floor to %.3f", got)
	}
	// Nor on fresh samples that lose at the old floor: the pool's figure is
	// someone else's overflow.
	acked, lost := make([]quiccongestion.AckedPacketInfo, 95), make([]quiccongestion.LostPacketInfo, 5)
	for at := start.Add(floorRebaseAfter); at.Sub(start) <= 3*floorRebaseAfter; at = at.Add(500 * time.Millisecond) {
		sender.rampGate(at, acked, lost)
	}
	if got := sender.rampFloor; got != 0.05 {
		t.Fatalf("a lane losing at its floor re-based it to the pool's %.3f", got)
	}
}

// A lane's own first rounds are its floor once a faster round that delivered
// more lost the same: the stale-floor case, where the pool's inherited figure
// is lower than what the path erases.
func TestAStaleFloorGivesWayToTheLanesOwnRounds(t *testing.T) {
	probe := joiningProbe()
	probe.Erasure = 0.15
	sender := NewErasureSenderOn(1200, probe)
	closeRound(sender, 50, 14) // 22%: the lane's first round, its own floor
	if sender.ownFloorN == 0 || sender.ownConfirmed {
		t.Fatalf("first round: own floor %.3f on %.0f, confirmed %v", sender.ownFloor, sender.ownFloorN, sender.ownConfirmed)
	}
	// 22% of 800, at twelve times the packets: against the pool's 0.15 that
	// is far outside the noise, against the lane's own first round it is not.
	closeRound(sender, 624, 176)
	if !sender.ownConfirmed {
		t.Fatal("a faster round that lost the same did not confirm the lane's floor")
	}
	if !sender.ramping.Load() {
		t.Fatal("a round losing what the lane's own first round lost ended its ramp")
	}
	if floor, _, _ := sender.floor(); math.Abs(floor-0.22) > 0.01 {
		t.Fatalf("the floor is %.3f, want the lane's own 0.22", floor)
	}
	// Loss that does rise with the rate still ends it.
	closeRound(sender, 1600, 800)
	if sender.ramping.Load() {
		t.Fatal("a round losing a third, against a confirmed 0.22 floor, did not end the ramp")
	}
}

// A lane whose first flight met a full queue loses most of it, and loses as
// much at every rate; that is not a floor, and it is not confirmed unless
// delivery grew with the rate.
func TestAFirstFlightIntoAFullQueueIsNotAFloor(t *testing.T) {
	sender := NewErasureSenderOn(1200, joiningProbe())
	closeRound(sender, 4, 60)
	// The same loss at twice the packets, but delivery flat.
	sender.rampGate(base(), make([]quiccongestion.AckedPacketInfo, 8), make([]quiccongestion.LostPacketInfo, 120))
	sender.inner.round++
	sender.rampGate(base(), nil, nil)
	if sender.ownConfirmed {
		t.Fatal("a round that delivered no more confirmed a 94% floor")
	}
	if floor, _, _ := sender.floor(); floor != 0.15 {
		t.Fatalf("judged against %.3f, want the pool's 0.15", floor)
	}
}

// A ramp that showed loss independent of rate leaves the lane compensating
// for it, when nothing in the pool had been earned.
func TestARampEarnsTheCompensationItShowed(t *testing.T) {
	probe := joiningProbe()
	probe.Erasure, probe.Arrival = 0.15, 0
	sender := NewErasureSenderOn(1200, probe)
	closeRound(sender, 50, 14)
	closeRound(sender, 156, 44)
	closeRound(sender, 468, 132)
	closeRound(sender, 1404, 396)
	if !sender.ownConfirmed || sender.earnRounds < 2 {
		t.Fatalf("confirmed %v after %d growth rounds", sender.ownConfirmed, sender.earnRounds)
	}
	sender.endRamp()
	if got := sender.arrivalRate(); math.Abs(got-0.78) > 0.01 {
		t.Fatalf("after a ramp that lost 22%% at every rate, arrival %.3f, want 0.78", got)
	}
	// Without the evidence -- no confirmed floor -- nothing is earned.
	sender = NewErasureSenderOn(1200, probe)
	closeRound(sender, 50, 14)
	sender.endRamp()
	if got := sender.arrivalRate(); got != 1 {
		t.Fatalf("a ramp with no confirmed floor earned arrival %.3f", got)
	}
}

// A request lull drops what the lane delivers. Growth back from it is growth,
// not three flat rounds against the peak before the lull.
func TestGrowthAfterALullIsNotFlat(t *testing.T) {
	sender := NewErasureSenderOn(1200, joiningProbe())
	primeOwnFloor(sender, 10)
	setMeasured := func(rate uint64) {
		sender.inner.estimator.measured = newTUICMinMax()
		sender.inner.estimator.measured.updateMax(sender.inner.round, monotime.Time(0), rate)
	}
	setMeasured(4e6)
	sender.inner.round++
	sender.rampGate(base(), nil, nil)
	// A lull: requests only, delivery down to a tenth.
	setMeasured(4e5)
	sendAt(sender, 1200)
	sender.inner.round++
	sender.rampGate(base(), nil, nil)
	// The transfer: every round has the window in flight and delivery
	// grows by half -- below the old peak for the first few.
	rate := 4e5
	for round := 0; round < 4; round++ {
		rate *= 1.5
		setMeasured(uint64(rate))
		sendAt(sender, sender.GetCongestionWindow())
		sender.inner.round++
		sender.rampGate(base(), nil, nil)
		if !sender.ramping.Load() {
			t.Fatalf("round %d after the lull, growing by half, ended the ramp as flat", round+1)
		}
	}
}

// Siblings that finish free their capacity, and the lane left behind ramps
// into it rather than growing a quarter per gain cycle. Left alone it is
// uncapped, so the share cannot say so; the change itself is the signal, and
// it waits out the backoff rather than being lost to it.
func TestALaneRampsAgainWhenItIsLeftAlone(t *testing.T) {
	sender := NewErasureSenderOn(1200, joiningProbe())
	sender.inner.minRTT = joiningRoundTrip
	sender.SetRTTStatsProvider(&fixedRTT{min: joiningRoundTrip, smoothed: joiningRoundTrip})
	sender.endRamp()
	sender.inner.mode = tuicBbrProbeBW
	closeRound := func(inFlight quiccongestion.ByteCount) {
		sender.inner.estimator.measured.updateMax(sender.inner.round, monotime.Time(0), 1e6)
		sendAt(sender, inFlight)
		sender.inner.round++
		sender.rampGate(base(), nil, nil)
	}
	for sender.inner.round+2 < sender.rearmAfter {
		closeRound(sender.GetCongestionWindow())
	}
	if sender.ramping.Load() {
		t.Fatal("a lane at its share with its siblings all busy ramped again")
	}
	// The last sibling finishes one round before the backoff is over.
	sender.share.Store(0)
	closeRound(sender.GetCongestionWindow())
	if sender.ramping.Load() {
		t.Fatal("the lane ramped again inside its backoff")
	}
	closeRound(sender.GetCongestionWindow())
	if !sender.ramping.Load() || sender.inner.mode != tuicBbrStartup || sender.inner.fullBandwidth {
		t.Fatalf("left alone, the lane is ramping %v in mode %v", sender.ramping.Load(), sender.inner.mode)
	}
	if sender.rampBaseRTT != sender.poolRTT {
		t.Fatalf("a re-armed ramp judges queue against %v, not the pool's current %v", sender.rampBaseRTT, sender.poolRTT)
	}
	// A re-armed ramp that finds nothing more waits twice as long next time.
	sender.endRamp()
	if sender.rearmBackoff != 2*rampRearmRounds {
		t.Fatalf("after a fruitless re-arm the backoff is %d rounds, want %d", sender.rearmBackoff, 2*rampRearmRounds)
	}
	// A share well above what the lane delivers re-arms it too, once the
	// backoff has passed -- and not with the lane carrying only requests.
	sender.share.Store(3e6)
	for sender.inner.round <= sender.rearmAfter {
		closeRound(1200)
	}
	if sender.ramping.Load() {
		t.Fatal("a lane carrying requests was re-armed")
	}
	closeRound(sender.GetCongestionWindow())
	if !sender.ramping.Load() {
		t.Fatal("a backlogged lane with a share three times its delivery did not ramp again")
	}
}

// In hierarchical mode a lane can join a destination its leaf has not
// measured, seeded from an ancestor that pools other destinations. Their
// round trips are not this path's: a shorter one taken as the baseline read
// the whole of this path's extra length as queue and ended every ramp at once.
func TestAJoinerOnAnUnmeasuredLeafHasNoPooledQueueBaseline(t *testing.T) {
	pathmodel.ResetTree()
	defer pathmodel.ResetTree()
	near := pathmodel.SharedChain(pathmodel.Key{Egress: "uplink-ramp", Dest: "near"})
	for member := pathmodel.Member(1); member <= 2; member++ {
		near.Report(member, pathmodel.Observation{Erasure: 0.1, BurstFactor: 1, ObservedSamples: 5000, Delivered: 4e6, RoundTrip: 30 * time.Millisecond})
	}
	far := pathmodel.SharedChain(pathmodel.Key{Egress: "uplink-ramp", Dest: "far"})
	sender := NewErasureSenderOn(1200, far)
	if !sender.ramping.Load() {
		t.Skip("the chain gave this joiner no seed; nothing to judge")
	}
	if sender.rampBaseRTT != 0 {
		t.Fatalf("a joiner to an unmeasured destination judges queue against %v, another destination's round trip", sender.rampBaseRTT)
	}
	sender.inner.minRTT = 180 * time.Millisecond
	sender.SetRTTStatsProvider(&fixedRTT{min: 180 * time.Millisecond, smoothed: 185 * time.Millisecond})
	ackEvent(sender)
	if !sender.ramping.Load() {
		t.Fatal("a joiner on a 180 ms path with 5 ms of queue ended its ramp")
	}
}

// Over a pool too thin to judge by, a lane's unconfirmed first flight is not
// its floor either -- it may be overflow -- and the pool's floor matures only
// from what its siblings measured, this lane's own report taken out.
func TestAThinPoolDoesNotMakeAFirstFlightAFloor(t *testing.T) {
	probe := joiningProbe()
	probe.ErasureSamples = 10
	sender := NewErasureSenderOn(1200, probe)
	closeRound(sender, 4, 60)
	if floor, n, _ := sender.floor(); n >= rampLossSamples {
		t.Fatalf("over a thin pool, the lane judges against %.3f on %.0f samples: its own unconfirmed first flight", floor, n)
	}
	// The joiner's first flight lost 90% of 1,000 packets; one sibling lost
	// none of 100. The pool reads 0.82.
	sender.selfLoss, sender.selfObserved, sender.selfSamples = 0.9, 1000, 1000
	sender.poolWeight, sender.poolSamples, sender.poolBurst = 1100, 1040, 1
	sender.poolErasure = 0.9 * 1000 / 1100
	sender.rampGate(base(), nil, nil)
	if sender.rampFloorN >= rampLossSamples {
		t.Fatalf("the floor matured to %.3f on %.0f samples, 1,000 of them this lane's own", sender.rampFloor, sender.rampFloorN)
	}
	sender.poolWeight, sender.poolSamples = 1200, 1100
	sender.poolErasure = 0.9 * 1000 / 1200
	sender.rampGate(base(), nil, nil)
	if sender.rampFloorN < rampLossSamples || sender.rampFloor > 0.01 {
		t.Fatalf("from 200 clean sibling outcomes the floor matured to %.3f on %.0f, want 0", sender.rampFloor, sender.rampFloorN)
	}
}

// A lane the pacer holds back is backlogged even when its window is far
// larger -- a window sized from a seed's longer round trip -- or its flat
// rounds would never count, and on a queue-free policer whose drops are the
// floor nothing else would end its ramp.
func TestAPacerBoundLaneCountsFlatRounds(t *testing.T) {
	sender := NewErasureSenderOn(1200, joiningProbe())
	sender.inner.minRTT = 20 * time.Millisecond
	sender.inner.cwnd *= 20
	sender.inner.estimator.measured.updateMax(1, monotime.Time(0), 1e6)
	paced := quiccongestion.ByteCount(float64(sender.bandwidth()) * 0.02)
	if 2*paced >= sender.GetCongestionWindow() {
		t.Fatalf("the window %d does not dwarf the pacing allowance %d; the test cannot tell", sender.GetCongestionWindow(), paced)
	}
	for round := 0; round <= rampFlatRounds; round++ {
		sendAt(sender, paced)
		sender.inner.round++
		sender.rampGate(base(), nil, nil)
	}
	if sender.ramping.Load() {
		t.Fatal("a lane with its pacing allowance in flight for three flat rounds is still ramping")
	}
}

// A lane left alone re-arms when the backoff allows -- unless a sibling took
// the room back first.
func TestAPendingReArmLapsesWhenTheCapReturns(t *testing.T) {
	sender := NewErasureSenderOn(1200, joiningProbe())
	sender.inner.minRTT = joiningRoundTrip
	sender.SetRTTStatsProvider(&fixedRTT{min: joiningRoundTrip, smoothed: joiningRoundTrip})
	sender.endRamp()
	sender.inner.mode = tuicBbrProbeBW
	closeRound := func() {
		sender.inner.estimator.measured.updateMax(sender.inner.round, monotime.Time(0), 1e6)
		sendAt(sender, sender.GetCongestionWindow())
		sender.inner.round++
		sender.rampGate(base(), nil, nil)
	}
	closeRound()
	sender.share.Store(0)
	closeRound()
	if !sender.departed {
		t.Fatal("left alone during its backoff, the lane has nothing pending")
	}
	sender.share.Store(1.25e6)
	for sender.inner.round <= sender.rearmAfter {
		closeRound()
	}
	if sender.ramping.Load() {
		t.Fatal("the cap came back before the backoff ended, and the lane ramped anyway")
	}
}

// The siblings' effective sample count is recomputed with the lane's own term
// taken out, not subtracted: a lane that dominates the pool's weights would
// otherwise hide a sibling's fresh samples.
func TestSiblingSamplesAreRecomputedNotSubtracted(t *testing.T) {
	m := pathmodel.NewPathModel()
	m.Report(1, pathmodel.Observation{Erasure: 0.5, BurstFactor: 1, ObservedSamples: 100000, ErasureSamples: 1000, Delivered: 1e6})
	state := m.Report(2, pathmodel.Observation{Erasure: 0.1, BurstFactor: 1, ObservedSamples: 100, ErasureSamples: 100, Delivered: 1e6})
	sender := NewErasureSenderOn(1200, joiningProbe())
	sender.selfLoss, sender.selfObserved, sender.selfSamples = 0.5, 100000, 1000
	sender.poolWeight, sender.poolSamples, sender.poolErasure = state.ErasureWeight, state.ErasureSamples, state.Erasure
	erasure, samples, ok := sender.siblingsErasure()
	if !ok || math.Abs(samples-100) > 1 || math.Abs(erasure-0.1) > 0.001 {
		t.Fatalf("siblings: erasure %.3f on %.1f samples (ok %v); want 0.1 on 100", erasure, samples, ok)
	}
}

// A ramp that ends at a cap hands the lane to it, and the cap vanishing before
// the next round is still the lane being left alone.
func TestALaneLeftAloneRightAfterItsRampEnds(t *testing.T) {
	sender := NewErasureSenderOn(1200, joiningProbe())
	sender.inner.minRTT = joiningRoundTrip
	sender.SetRTTStatsProvider(&fixedRTT{min: joiningRoundTrip, smoothed: joiningRoundTrip})
	sender.endRamp()
	sender.inner.mode = tuicBbrProbeBW
	sender.share.Store(0)
	for sender.inner.round <= sender.rearmAfter && !sender.ramping.Load() {
		sender.inner.estimator.measured.updateMax(sender.inner.round, monotime.Time(0), 1e6)
		sendAt(sender, sender.GetCongestionWindow())
		sender.inner.round++
		sender.rampGate(base(), nil, nil)
	}
	if !sender.ramping.Load() {
		t.Fatal("left alone before its first round at the cap, the lane never ramped again")
	}
}

// A lane that confirmed its own floor over a thin pool, and outlived the
// siblings that could have matured the pool's, still has its floor expire
// after a measured window above it.
func TestAConfirmedFloorOverAThinPoolCanRebase(t *testing.T) {
	probe := joiningProbe()
	probe.ErasureSamples = 10
	sender := NewErasureSenderOn(1200, probe)
	closeRound(sender, 60, 4)
	closeRound(sender, 188, 12)
	if !sender.ownConfirmed {
		t.Fatal("setup: the lane did not confirm its own floor")
	}
	sender.endRamp()
	sender.poolSamples, sender.poolBurst = 5000, 1
	sender.erasure.Store(uint64(0.30 * partsPerMillion))
	acked, lost := make([]quiccongestion.AckedPacketInfo, 70), make([]quiccongestion.LostPacketInfo, 30)
	start := base()
	for at := start; at.Sub(start) <= floorRebaseAfter; at = at.Add(500 * time.Millisecond) {
		sender.rampGate(at, acked, lost)
	}
	if floor, _, _ := sender.floor(); math.Abs(floor-0.30) > 0.001 {
		t.Fatalf("after a measured window above it, the floor is still %.3f", floor)
	}
}

// ProbeRTT holds the window down on purpose. Its rounds are not flat rounds,
// and a lane left alone during one re-arms after it, not inside it.
func TestProbeRTTRoundsAreNotRampEvidence(t *testing.T) {
	sender := NewErasureSenderOn(1200, joiningProbe())
	primeOwnFloor(sender, 10)
	sender.inner.estimator.measured.updateMax(sender.inner.round, monotime.Time(0), 1e6)
	sender.inner.mode = tuicBbrProbeRTT
	for round := 0; round < 2*rampFlatRounds; round++ {
		sendAt(sender, sender.GetCongestionWindow())
		sender.inner.round++
		sender.rampGate(base(), nil, nil)
	}
	if !sender.ramping.Load() {
		t.Fatal("ProbeRTT's held-down rounds ended a ramp as flat")
	}

	sender = NewErasureSenderOn(1200, joiningProbe())
	sender.inner.minRTT = joiningRoundTrip
	sender.SetRTTStatsProvider(&fixedRTT{min: joiningRoundTrip, smoothed: joiningRoundTrip})
	sender.endRamp()
	sender.inner.mode = tuicBbrProbeRTT
	sender.share.Store(0)
	closeRound := func() {
		sender.inner.estimator.measured.updateMax(sender.inner.round, monotime.Time(0), 1e6)
		sendAt(sender, sender.GetCongestionWindow())
		sender.inner.round++
		sender.rampGate(base(), nil, nil)
	}
	for sender.inner.round <= sender.rearmAfter+2 {
		closeRound()
	}
	if sender.ramping.Load() || !sender.departed {
		t.Fatalf("inside ProbeRTT: ramping %v, departure pending %v; want false, true", sender.ramping.Load(), sender.departed)
	}
	sender.inner.mode = tuicBbrProbeBW
	closeRound()
	if !sender.ramping.Load() {
		t.Fatal("after ProbeRTT the pending departure did not re-arm the lane")
	}
}

// The siblings' erasure is taken from the pool's figure at full precision; a
// long-lived lane's weight would otherwise magnify the published figure's
// rounding into points of loss.
func TestSiblingErasureIsExactBesideALongLivedLane(t *testing.T) {
	m := pathmodel.NewPathModel()
	m.Report(1, pathmodel.Observation{Erasure: 0.2000004, BurstFactor: 1, ObservedSamples: 1e7, ErasureSamples: 8000, Delivered: 1e6})
	state := m.Report(2, pathmodel.Observation{Erasure: 0.15, BurstFactor: 1, ObservedSamples: 100, ErasureSamples: 100, Delivered: 1e6})
	sender := NewErasureSenderOn(1200, joiningProbe())
	sender.selfLoss, sender.selfObserved, sender.selfSamples = 0.2000004, 1e7, 8000
	sender.poolWeight, sender.poolSamples, sender.poolErasure = state.ErasureWeight, state.ErasureSamples, state.Erasure
	if erasure, _, ok := sender.siblingsErasure(); !ok || math.Abs(erasure-0.15) > 0.001 {
		t.Fatalf("the sibling's erasure reads %.4f (ok %v), want 0.15", erasure, ok)
	}
}

// Rounds that grow gradually -- less than twice the first rounds' packets --
// at the loss the first rounds showed are a confirmation pending, not loss
// above an inherited floor.
func TestGradualGrowthAtAConstantLossKeepsTheRamp(t *testing.T) {
	sender := NewErasureSenderOn(1200, joiningProbe()) // floor 0.15 on 5000
	closeRound(sender, 60, 40)
	closeRound(sender, 78, 52)
	if !sender.ramping.Load() {
		t.Fatal("a round that grew 30% at the first round's 40% loss ended the ramp against the inherited 15%")
	}
	closeRound(sender, 120, 80)
	if !sender.ownConfirmed || !sender.ramping.Load() {
		t.Fatalf("at twice the packets and the same loss: confirmed %v, ramping %v", sender.ownConfirmed, sender.ramping.Load())
	}
	// A round that did not grow is judged against the floor as before.
	sender = NewErasureSenderOn(1200, joiningProbe())
	closeRound(sender, 60, 40)
	sender.rampGate(base(), make([]quiccongestion.AckedPacketInfo, 78), make([]quiccongestion.LostPacketInfo, 52))
	sender.inner.round++
	sender.rampGate(base(), nil, nil)
	if sender.ramping.Load() {
		t.Fatal("a round at 40% loss that delivered no more did not end the ramp against a 15% floor")
	}
}

// Confirmation needs enough independent trials after the burst deflation: on
// a channel losing in runs of 16, a round of 1,000 packets is 62 trials and
// does not confirm, one of 1,040 is 65 and does.
func TestConfirmationCountsIndependentTrials(t *testing.T) {
	for _, tc := range []struct {
		packets   int
		confirmed bool
	}{{1000, false}, {1040, true}} {
		probe := joiningProbe()
		probe.BurstFactor, probe.ErasureBurst = 16, 16
		sender := NewErasureSenderOn(1200, probe)
		// The first rounds: 16 x 64 packets at 25%, gathered in one round so
		// that the synthetic delivery stays bounded.
		closeRound(sender, 768, 256)
		if sender.ownFloorN == 0 {
			t.Fatal("setup: the first round did not gather the own floor")
		}
		sender.ownRoundMax = 400 // twice is below either round under test
		closeRound(sender, tc.packets*3/4, tc.packets/4)
		if sender.ownConfirmed != tc.confirmed {
			t.Errorf("%d packets in runs of 16 at the first rounds' loss: confirmed %v, want %v", tc.packets, sender.ownConfirmed, tc.confirmed)
		}
	}
}

// The pool's figure is deflated by the pool's burst factor, the floor by its
// own: long loss runs in the pool now are fewer trials than packets.
func TestThePoolIsJudgedByItsOwnBurstFactor(t *testing.T) {
	sender := NewErasureSenderOn(1200, joiningProbe()) // 0.15 on 5000, burst 1
	sender.endRamp()
	sender.erasure.Store(uint64(0.25 * partsPerMillion))
	sender.poolSamples, sender.poolBurst = 400, 1
	if !sender.poolAboveFloor() {
		t.Fatal("setup: 0.25 on 400 independent samples is not above 0.15")
	}
	sender.poolBurst = 20
	if sender.poolAboveFloor() {
		t.Fatal("0.25 on 400 packets lost in runs of 20 read as above the floor")
	}
}

// A lane's rounds are deflated by the burstiness it measures itself, not the
// floor's: a lane inheriting an independent-loss floor that now loses in long
// runs is seeing a burst, not overflow.
func TestARoundIsDeflatedByTheLanesOwnBurstFactor(t *testing.T) {
	sender := NewErasureSenderOn(1200, joiningProbe()) // floor 0.15, burst 1
	sender.selfSamples, sender.selfBurst = 1000, 8
	primeOwnFloor(sender, 10)
	closeRound(sender, 140, 60)
	if !sender.ramping.Load() {
		t.Fatal("60 of 200 lost in runs of 8 -- 25 trials -- ended the ramp")
	}
}

// Telemetry reports the rate the pacer uses. A lane held at its share paces
// at the share, and anything sized from its reported rate -- a QUIC lane's
// admission is -- must see that, not the uncapped estimate.
func TestTelemetryReportsTheCappedPacingRate(t *testing.T) {
	probe := joiningProbe()
	probe.Share = 200_000
	sender := NewErasureSenderOn(1200, probe)
	sender.endRamp()
	passGate(sender)
	if uncapped := float64(sender.inner.bandwidth()) / sender.arrivalRate(); uncapped <= 1.5*200_000 {
		t.Fatalf("setup: the uncapped rate %.0f is not well above the share", uncapped)
	}
	if got := sender.Telemetry().PacingRate; got != uint64(sender.bandwidth()) {
		t.Fatalf("telemetry reports pacing %d, the pacer uses %d", got, sender.bandwidth())
	}
}

// A lane's own rounds are deflated by its own burst factor, not the pool's:
// a 256-packet round lost in runs of 16 is sixteen trials, not 256, and does
// not confirm a floor however well it matches.
func TestALanesOwnBurstsDecideItsConfirmation(t *testing.T) {
	sender := NewErasureSenderOn(1200, joiningProbe()) // pool burst 1
	closeRound(sender, 48, 16)
	sender.selfSamples, sender.selfBurst = 1000, 16
	closeRound(sender, 192, 64)
	if sender.ownConfirmed {
		t.Fatal("a round of 256 packets lost in runs of 16 confirmed the lane's floor")
	}
}

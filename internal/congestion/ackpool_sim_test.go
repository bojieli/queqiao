package congestion

import (
	"math/rand"
	"sort"
	"testing"
	"time"

	quiccongestion "github.com/apernet/quic-go/congestion"
	"github.com/apernet/quic-go/monotime"

	"github.com/bojieli/queqiao/internal/pathmodel"
)

// The reverse direction of a download burst, simulated like the forward one in
// joinburst_sim_test.go: one pooled path model, the real ErasureSender on each
// lane, a FIFO bottleneck with erasure after it. What differs is the traffic.
// Each lane carries two kinds, as the client's upstream connection of a
// download does:
//
//   - QUIC's own ACK-only packets for the download, many and small. quic-go
//     sends them whatever the pacer and the window say, but still reports
//     each through OnPacketSent, as not ack-eliciting; they are never in
//     flight, so nothing acknowledges them and the delivery sampler never
//     sees them. The simulation does the same.
//   - the download's flow acknowledgements: sparse, small stream frames,
//     which have to pass CanSend and HasPacingBudget like any other data.
//
// A pool member like this has nothing more to send than it offers. Its
// measured delivery is the trickle of flow acknowledgements, so a share of
// 1.25x the pooled delivery caps it at about that trickle, the ACK-only
// packets are charged against the same small budget, and each flow
// acknowledgement waits for a full datagram's worth of it. The gateway's
// sender is clocked by these acknowledgements, so that wait is what the
// gateway sees as a stalled flow.
type ackPool struct {
	rate, erasure, buffer float64
	rtt                   time.Duration
	lanes                 int
	stagger, duration     time.Duration
	// Each lane offers flow acknowledgements at low bytes per second until
	// stepAt, then high, in packets of ackPacketSize; and ackOnly QUIC
	// ACK-only packets a second of ackOnlySize bytes.
	low, high float64
	stepAt    time.Duration
	ackOnly   float64
	seed      int64
}

type ackPoolResult struct {
	// delays is how long each acknowledgement offered after measureFrom waited
	// for the sender to put it on the wire.
	delays []time.Duration
	// capped counts lane-ticks after the step on which a share capped the
	// lane's rate.
	capped, ticks int
	offered, sent uint64
}

const (
	ackPacketSize = 100
	ackOnlySize   = 60
)

func (r ackPoolResult) delay(p float64) time.Duration {
	if len(r.delays) == 0 {
		return 0
	}
	d := append([]time.Duration(nil), r.delays...)
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	return d[int(p*float64(len(d)-1))]
}

func runAckPool(cfg ackPool, measureFrom time.Duration) ackPoolResult {
	base := monotime.Now()
	wall := time.Now()
	var now time.Duration
	model := pathmodel.NewPathModelClocked(func() time.Time { return wall.Add(now) })
	rng := rand.New(rand.NewSource(cfg.seed))
	type ackLane struct {
		e        *ErasureSender
		rtt      simRTT
		next     quiccongestion.PacketNumber
		inFlight quiccongestion.ByteCount
		pending  []simPacket
		credit   float64
		acks     float64
		queue    []time.Duration // offer time of each waiting acknowledgement
	}
	var result ackPoolResult
	var lanes []*ackLane
	var busyUntil time.Duration
	for now = 0; now < cfg.duration; now += simTick {
		at := base.Add(now)
		for len(lanes) < cfg.lanes && now >= time.Duration(len(lanes))*cfg.stagger {
			lane := &ackLane{e: NewErasureSenderOn(ackPacketSize, model), next: 1}
			lane.e.inner.randomState = uint64(len(lanes)+1) * 0x9e3779b97f4a7c15
			lane.e.SetRTTStatsProvider(&lane.rtt)
			if seed := lane.e.inner.estimator.maxFilter.get(); seed > 0 {
				filter := newTUICMinMax()
				filter.setRoundTrip(cfg.rtt)
				filter.updateMax(lane.e.inner.round, at, seed)
				lane.e.inner.estimator.maxFilter = filter
			}
			lanes = append(lanes, lane)
		}
		demand := cfg.low
		if now >= cfg.stepAt {
			demand = cfg.high
		}
		for _, lane := range lanes {
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
					lost = append(lost, quiccongestion.LostPacketInfo{PacketNumber: p.number, BytesLost: ackPacketSize})
					continue
				}
				acked = append(acked, quiccongestion.AckedPacketInfo{PacketNumber: p.number, BytesAcked: ackPacketSize, ReceivedTime: at})
				sample = p.due - p.sent
			}
			if sample > 0 {
				lane.rtt.observe(sample)
			}
			if done > 0 {
				lane.pending = lane.pending[done:]
				prior := lane.inFlight
				lane.inFlight -= quiccongestion.ByteCount(done * ackPacketSize)
				lane.e.OnCongestionEventEx(prior, at, acked, lost)
			}
			// The download's acknowledgements arrive at the offered rate.
			lane.credit += demand * simTick.Seconds()
			for lane.credit >= ackPacketSize {
				lane.credit -= ackPacketSize
				lane.queue = append(lane.queue, now)
				if now >= measureFrom {
					result.offered++
				}
			}
			// The download's QUIC acknowledgements: sent past the pacer and
			// the window, reported, never in flight.
			lane.acks += cfg.ackOnly * simTick.Seconds()
			for ; lane.acks >= 1; lane.acks-- {
				number := lane.next
				lane.next++
				lane.e.OnPacketSent(at, lane.inFlight, number, ackOnlySize, false)
			}
			if now >= cfg.stepAt {
				result.ticks++
				if share := lane.e.Share(); share > 0 && share < float64(lane.e.inner.bandwidth()) && !lane.e.ramping.Load() {
					result.capped++
				}
			}
			for len(lane.queue) > 0 && lane.e.CanSend(lane.inFlight) && lane.e.HasPacingBudget(at) {
				offeredAt := lane.queue[0]
				lane.queue = lane.queue[1:]
				if offeredAt >= measureFrom {
					result.delays = append(result.delays, now-offeredAt)
					result.sent++
				}
				lane.inFlight += ackPacketSize
				number := lane.next
				lane.next++
				lane.e.OnPacketSent(at, lane.inFlight, number, ackPacketSize, true)
				start := now
				if busyUntil > start {
					start = busyUntil
				}
				p := simPacket{number: number, sent: now}
				if backlog := (start - now).Seconds() * cfg.rate; backlog+ackPacketSize > cfg.buffer {
					p.lost, p.due = true, start+cfg.rtt
				} else {
					busyUntil = start + time.Duration(ackPacketSize/cfg.rate*float64(time.Second))
					p.due = busyUntil + cfg.rtt
					p.lost = rng.Float64() < cfg.erasure
				}
				lane.pending = append(lane.pending, p)
			}
		}
	}
	// An acknowledgement still waiting at the end waited at least this long.
	for _, lane := range lanes {
		for _, offeredAt := range lane.queue {
			if offeredAt >= measureFrom {
				result.delays = append(result.delays, cfg.duration-offeredAt)
			}
		}
	}
	return result
}

// The Beijing host's upstream as traced: four download lanes on a 2 MB/s,
// 200 ms, 1.5%-erasure path, each sending about 175 ACK-only packets a second
// (700 across the four, as measured) and its flow acknowledgements, ten a
// second while the download starts and twenty-five once it runs. The path
// has a hundred times the room all of that needs, so no flow acknowledgement
// should wait on the pacer.
func reverseAckPoolPath() ackPool {
	return ackPool{
		rate: 2e6, erasure: 0.015, buffer: 2e6 * 0.1, rtt: 200 * time.Millisecond,
		lanes: 4, stagger: 20 * time.Millisecond, duration: 10 * time.Second,
		low: 10 * ackPacketSize, high: 25 * ackPacketSize, stepAt: 3 * time.Second,
		ackOnly: 175, seed: 1,
	}
}

// A download's flow acknowledgements go out as they are offered: neither the
// share nor the ACK-only packets beside them hold them behind the pacer.
func TestFlowAcksOnAReverseAckOnlyPoolDoNotWaitOnThePacer(t *testing.T) {
	cfg := reverseAckPoolPath()
	measureFrom := cfg.stepAt + time.Second
	r := runAckPool(cfg, measureFrom)
	p50, p90 := r.delay(0.5), r.delay(0.9)
	t.Logf("reverse ACK-only pool: offered %d sent %d, pacer wait p50 %v p90 %v, capped %d of %d lane-ticks after the step",
		r.offered, r.sent, p50, p90, r.capped, r.ticks)
	if p90 > cfg.rtt/4 {
		t.Errorf("flow acknowledgements waited %v (p90) for the pacer on a path with a hundred times the room they need", p90)
	}
	if r.sent+r.sent/50 < r.offered {
		t.Errorf("sent %d of %d acknowledgements offered after the step", r.sent, r.offered)
	}
}

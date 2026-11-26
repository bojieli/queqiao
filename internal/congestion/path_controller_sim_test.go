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

// A deterministic packet-level simulation of several connections on one
// path, in the style of the join-burst simulation of #161: the real control
// law, a simulated FIFO bottleneck with a finite buffer, erasure drawn from a
// seeded generator after the queue, and a fixed propagation round trip. Time
// is simulated, so a run is the same run on every machine.
//
// Each connection is driven either as an attachment of one PathController
// (congestion=path) or, for comparison, as today's per-connection
// ErasureSender pooled through a shared path model with shares and seeds.

const (
	pcSimTick       = time.Millisecond
	pcSimPacketSize = 1200
)

type pcSimPath struct {
	rate    float64 // bottleneck bytes per second
	rtt     time.Duration
	erasure float64
	buffer  float64 // bottleneck buffer, bytes
	seed    int64
}

// pcSimLane is one connection's life and traffic.
type pcSimLane struct {
	join, stop time.Duration // stop zero runs to the end
	// bulk lanes always have data. Otherwise the lane offers flow
	// acknowledgements of ackSize bytes at ackRate bytes per second (ackStep
	// from stepAt), each a small ack-eliciting packet, and sends ackOnly
	// QUIC ACK-only packets per millisecond besides.
	bulk             bool
	ackRate, ackStep float64
	stepAt           time.Duration
	ackSize          quiccongestion.ByteCount
	ackOnly          int
	// requests, for a lane that is neither: one small packet a round trip.
	requests bool
	// pauseEvery, for a bulk lane, pauses it for pauseFor that often.
	pauseEvery, pauseFor time.Duration
}

type pcSimResult struct {
	delivered       float64 // bulk bytes acknowledged
	perSecond       []float64
	sent, dropped   uint64
	queue           []time.Duration
	ackWaits        []time.Duration // flow acknowledgements' wait to be sent, after measureFrom
	ackOffered      uint64
	ackSent         uint64
	nonElicitingHit bool // a non-eliciting packet was debited
}

func (r pcSimResult) goodputBetween(from, to int) float64 {
	var d float64
	for i := from; i < to && i < len(r.perSecond); i++ {
		d += r.perSecond[i]
	}
	return d / float64(to-from)
}

func (r pcSimResult) causedLoss() float64 {
	if r.sent == 0 {
		return 0
	}
	return float64(r.dropped) / float64(r.sent)
}

func pcPercentile(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), d...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[int(p*float64(len(s)-1))]
}

type pcSimPacket struct {
	number quiccongestion.PacketNumber
	bytes  quiccongestion.ByteCount
	sent   time.Duration
	due    time.Duration
	lost   bool
	bulk   bool
}

// pcSimRTT is a connection's own round-trip statistics, as quic-go keeps them.
type pcSimRTT struct{ min, latest, smoothed time.Duration }

func (r *pcSimRTT) MinRTT() time.Duration        { return r.min }
func (r *pcSimRTT) LatestRTT() time.Duration     { return r.latest }
func (r *pcSimRTT) SmoothedRTT() time.Duration   { return r.smoothed }
func (r *pcSimRTT) MeanDeviation() time.Duration { return 0 }
func (r *pcSimRTT) MaxAckDelay() time.Duration   { return 0 }
func (r *pcSimRTT) PTO(bool) time.Duration       { return 2 * r.smoothed }
func (r *pcSimRTT) UpdateRTT(_, _ time.Duration) {}
func (r *pcSimRTT) SetMaxAckDelay(time.Duration) {}
func (r *pcSimRTT) SetInitialRTT(time.Duration)  {}

func (r *pcSimRTT) observe(sample time.Duration) {
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

// pcSimConn is one simulated connection: its packet numbers, its flight and
// its controller, whichever kind.
type pcSimConn struct {
	plan     pcSimLane
	rtt      pcSimRTT
	next     quiccongestion.PacketNumber
	inFlight quiccongestion.ByteCount
	pending  []pcSimPacket
	queue    []time.Duration // offer times of waiting flow acknowledgements
	credit   float64
	open     bool
	lastReq  time.Duration

	att    *PathAttachment // congestion=path
	legacy *ErasureSender  // per-connection, on its own
}

func (c *pcSimConn) admit(at monotime.Time, class quiccongestion.SendClass) bool {
	if c.att != nil {
		var d quiccongestion.Demand
		d.Eligible[class] = true
		c.att.OnDemand(d)
		return c.att.Admit(at, pcSimPacketSize).OK
	}
	return c.legacy.CanSend(c.inFlight) && c.legacy.HasPacingBudget(at)
}

func (c *pcSimConn) idle() {
	if c.att != nil {
		c.att.OnDemand(quiccongestion.Demand{})
	}
}

func (c *pcSimConn) sent(at monotime.Time, n quiccongestion.PacketNumber, bytes quiccongestion.ByteCount, eliciting bool, class quiccongestion.SendClass) {
	if c.att != nil {
		var cb [quiccongestion.NumSendClasses]quiccongestion.ByteCount
		if eliciting {
			cb[class] = bytes
		}
		kind := quiccongestion.KindOrdinary
		if !eliciting {
			kind = quiccongestion.KindAckOnly
		}
		c.att.OnSent(quiccongestion.SentPacket{Space: quiccongestion.SpaceApplication, Number: n, Bytes: bytes,
			AckEliciting: eliciting, InFlightCounted: eliciting, Exempt: !eliciting, Kind: kind,
			ClassBytes: cb, InFlight: c.inFlight, Time: at})
		return
	}
	c.legacy.OnPacketSent(at, c.inFlight, n, bytes, eliciting)
}

func (c *pcSimConn) event(at monotime.Time, prior quiccongestion.ByteCount, acked, lost []pcSimPacket, sample time.Duration) {
	if c.att != nil {
		ev := quiccongestion.AckEvent{Time: at, InFlight: c.inFlight, RTTSample: sample}
		for _, p := range acked {
			ev.Acked = append(ev.Acked, quiccongestion.PacketRef{Space: quiccongestion.SpaceApplication, Number: p.number, Bytes: p.bytes})
		}
		for _, p := range lost {
			ev.Lost = append(ev.Lost, quiccongestion.PacketRef{Space: quiccongestion.SpaceApplication, Number: p.number, Bytes: p.bytes})
		}
		c.att.OnAckEvent(ev)
		return
	}
	var a []quiccongestion.AckedPacketInfo
	var l []quiccongestion.LostPacketInfo
	for _, p := range acked {
		a = append(a, quiccongestion.AckedPacketInfo{PacketNumber: p.number, BytesAcked: p.bytes, ReceivedTime: at})
	}
	for _, p := range lost {
		l = append(l, quiccongestion.LostPacketInfo{PacketNumber: p.number, BytesLost: p.bytes})
	}
	c.legacy.OnCongestionEventEx(prior, at, a, l)
}

// runPCSim runs lanes on path for duration, through one PathController when
// authority is set and through one sender per connection otherwise.
func runPCSim(path pcSimPath, lanes []pcSimLane, duration, measureFrom time.Duration, authority bool) (pcSimResult, *PathController) {
	base := monotime.Now()
	var now time.Duration
	// The path model ages members by the wall clock, which this simulation
	// outruns; the pooled comparison is informational.
	model := pathmodel.NewPathModel()
	rng := rand.New(rand.NewSource(path.seed))
	var ctl *PathController
	if authority {
		ctl = NewPathController(pcSimPacketSize, model, DefaultClassWeights)
		ctl.law.inner.randomState = 0x9e3779b97f4a7c15
		ctl.clock = func() monotime.Time { return base.Add(now) }
	}
	conns := make([]*pcSimConn, len(lanes))
	for i, plan := range lanes {
		conns[i] = &pcSimConn{plan: plan, next: 1}
	}
	var r pcSimResult
	r.perSecond = make([]float64, int(duration/time.Second)+1)
	var busyUntil time.Duration
	for now = 0; now < duration; now += pcSimTick {
		at := base.Add(now)
		for i, c := range conns {
			if !c.open && now >= c.plan.join && (c.plan.stop == 0 || now < c.plan.stop) {
				c.open = true
				if authority {
					c.att = ctl.Attach(nil, nil)
					c.att.OnAttach(at, 0, pcSimPacketSize, func() {})
					c.att.SetRTTStatsProvider(&c.rtt)
				} else {
					c.legacy = NewErasureSender(pcSimPacketSize)
					c.legacy.inner.randomState = uint64(i+1) * 0x9e3779b97f4a7c15
					c.legacy.SetRTTStatsProvider(&c.rtt)
				}
			}
			if c.open && c.plan.stop > 0 && now >= c.plan.stop && len(c.pending) == 0 && c.att != nil {
				c.att.OnDetach(at)
				c.att = nil
				c.open = false
			}
		}
		// Outcomes.
		for _, c := range conns {
			if c.att == nil && c.legacy == nil {
				continue
			}
			var acked, lost []pcSimPacket
			done := 0
			var sample time.Duration
			for _, p := range c.pending {
				if p.due > now {
					break
				}
				done++
				if p.lost {
					lost = append(lost, p)
					continue
				}
				acked = append(acked, p)
				sample = p.due - p.sent
				if p.bulk {
					r.delivered += float64(p.bytes)
					r.perSecond[int(now/time.Second)] += float64(p.bytes)
				}
			}
			if done == 0 {
				continue
			}
			if sample > 0 {
				c.rtt.observe(sample)
			}
			c.pending = c.pending[done:]
			prior := c.inFlight
			for _, p := range acked {
				c.inFlight -= p.bytes
			}
			for _, p := range lost {
				c.inFlight -= p.bytes
			}
			c.event(at, prior, acked, lost, sample)
		}
		// Sends, each connection in a rotating order.
		for j := range conns {
			c := conns[(j+int(now/pcSimTick))%len(conns)]
			if !c.open || (c.plan.stop > 0 && now >= c.plan.stop) {
				if c.att != nil {
					c.idle()
				}
				continue
			}
			send := func(bytes quiccongestion.ByteCount, eliciting, bulk bool, class quiccongestion.SendClass) {
				n := c.next
				c.next++
				if eliciting {
					c.inFlight += bytes
				}
				c.sent(at, n, bytes, eliciting, class)
				if !eliciting {
					return
				}
				r.sent++
				start := max(busyUntil, now)
				p := pcSimPacket{number: n, bytes: bytes, sent: now, bulk: bulk}
				if backlog := (start - now).Seconds() * path.rate; backlog+float64(bytes) > path.buffer {
					p.lost, p.due = true, start+path.rtt
					r.dropped++
				} else {
					r.queue = append(r.queue, start-now)
					busyUntil = start + time.Duration(float64(bytes)/path.rate*float64(time.Second))
					p.due = busyUntil + path.rtt
					p.lost = rng.Float64() < path.erasure
				}
				c.pending = append(c.pending, p)
			}
			switch {
			case c.plan.bulk && c.plan.pauseEvery > 0 && now%c.plan.pauseEvery < c.plan.pauseFor:
				c.idle()
			case c.plan.bulk:
				for c.admit(at, quiccongestion.ClassBulk) {
					send(pcSimPacketSize, true, true, quiccongestion.ClassBulk)
				}
			case c.plan.requests:
				if now-c.lastReq >= path.rtt && c.admit(at, quiccongestion.ClassInteractive) {
					c.lastReq = now
					send(300, true, false, quiccongestion.ClassInteractive)
				}
				c.idle()
			default:
				for range c.plan.ackOnly {
					send(50, false, false, quiccongestion.ClassExempt)
				}
				rate := c.plan.ackRate
				if now >= c.plan.stepAt {
					rate = c.plan.ackStep
				}
				c.credit += rate * pcSimTick.Seconds()
				for c.credit >= float64(c.plan.ackSize) {
					c.credit -= float64(c.plan.ackSize)
					c.queue = append(c.queue, now)
					if now >= measureFrom {
						r.ackOffered++
					}
				}
				for len(c.queue) > 0 && c.admit(at, quiccongestion.ClassControl) {
					if c.queue[0] >= measureFrom {
						r.ackWaits = append(r.ackWaits, now-c.queue[0])
						r.ackSent++
					}
					c.queue = c.queue[1:]
					send(c.plan.ackSize, true, false, quiccongestion.ClassControl)
				}
				// A lane with acknowledgements still waiting keeps its
				// demand: only an empty queue is an unused opportunity.
				if len(c.queue) == 0 {
					c.idle()
				}
			}
		}
	}
	for _, c := range conns {
		for _, offered := range c.queue {
			if offered >= measureFrom {
				r.ackWaits = append(r.ackWaits, duration-offered)
			}
		}
	}
	if authority {
		tel := ctl.Telemetry()
		r.nonElicitingHit = tel.NonElicitingDebited != 0
	}
	return r, ctl
}

// The China-US access path the erasure law was built for, scaled: 2 MB/s,
// 200 ms, 15% erasure, a quarter of a bandwidth-delay product of buffer.
func pcErasurePath() pcSimPath {
	return pcSimPath{rate: 2e6, rtt: 200 * time.Millisecond, erasure: 0.15, buffer: 2e6 * 0.05, seed: 1}
}

func bulkLanes(n int, stagger time.Duration) []pcSimLane {
	lanes := make([]pcSimLane, n)
	for i := range lanes {
		lanes[i] = pcSimLane{join: time.Duration(i) * stagger, bulk: true}
	}
	return lanes
}

// A burst of bulk connections joining one path, the shape of every download
// trial of the #146 A/B. Under one authority, four connections deliver what
// one does, with the queue and the self-inflicted loss of one: nothing is
// divided, so there is nothing to seed and nothing to ramp, and the
// aggregate cannot overshoot by the number of connections.
func TestPathControllerJoinBurstBehavesAsOneConnection(t *testing.T) {
	path := pcErasurePath()
	const duration = 12 * time.Second
	one, _ := runPCSim(path, bulkLanes(1, 0), duration, 0, true)
	four, ctl := runPCSim(path, bulkLanes(4, 20*time.Millisecond), duration, 0, true)
	pooled, _ := runPCSim(path, bulkLanes(4, 20*time.Millisecond), duration, 0, false)
	capacity := path.rate * (1 - path.erasure)
	for _, c := range []struct {
		name string
		r    pcSimResult
	}{{"one attachment", one}, {"four attachments", four}, {"four pooled connections (today)", pooled}} {
		t.Logf("%-32s goodput %5.2f MB/s (%.2f of capacity) in 4-12 s, caused loss %.3f, queue p90 %v",
			c.name, c.r.goodputBetween(4, 12)/1e6, c.r.goodputBetween(4, 12)/capacity, c.r.causedLoss(), pcPercentile(c.r.queue, 0.9))
	}
	g1, g4 := one.goodputBetween(4, 12), four.goodputBetween(4, 12)
	if ratio := g4 / g1; ratio < 0.9 || ratio > 1.1 {
		t.Errorf("four attachments delivered %.2fx what one did", ratio)
	}
	if four.causedLoss() > one.causedLoss()+0.02 {
		t.Errorf("four attachments caused loss %.3f where one caused %.3f", four.causedLoss(), one.causedLoss())
	}
	if q1, q4 := pcPercentile(one.queue, 0.9), pcPercentile(four.queue, 0.9); q4 > q1+path.rtt/10 {
		t.Errorf("four attachments queued %v (p90) where one queued %v", q4, q1)
	}
	if g4 < 0.6*capacity {
		t.Errorf("four attachments delivered %.2f of capacity", g4/capacity)
	}
	if s := ctl.Stats(); s.SamplerDelivered == 0 || s.Expired > s.Samples/100 {
		t.Errorf("sampler delivered %d bytes; %d of %d samples expired", s.SamplerDelivered, s.Expired, s.Samples)
	}
}

// The #146 shape: an idle control connection, carrying a request a round
// trip, is on the path when four bulk connections join. Under shares, an
// honest share beside an idle member capped the bulk lanes; under one
// authority the idle connection holds nothing, and the bulk connections
// deliver what bulk alone does.
func TestPathControllerIdleSiblingTakesNothing(t *testing.T) {
	path := pcErasurePath()
	const duration = 12 * time.Second
	alone, _ := runPCSim(path, bulkLanes(4, 20*time.Millisecond), duration, 0, true)
	lanes := append([]pcSimLane{{requests: true}}, bulkLanes(4, 20*time.Millisecond)...)
	for i := 1; i < len(lanes); i++ {
		lanes[i].join += 2 * time.Second
	}
	withIdle, _ := runPCSim(path, lanes, duration, 0, true)
	// The same absolute interval in both: both runs take their ProbeRTT at
	// about ten seconds, when the path's first minimum expires.
	ga, gi := alone.goodputBetween(4, 10), withIdle.goodputBetween(4, 10)
	t.Logf("bulk alone %.2f MB/s, bulk beside an idle control connection %.2f MB/s", ga/1e6, gi/1e6)
	if gi < 0.9*ga {
		t.Errorf("the idle connection cost the bulk connections %.0f%%", 100*(1-gi/ga))
	}
}

// #160's tail regrowth: when one bulk connection ends and another starts, a
// per-connection controller must rediscover the path. Under one authority
// the path's estimate belongs to the path, so the next connection runs at
// once at the rate the last one had reached.
func TestPathControllerNextConnectionNeedsNoRegrowth(t *testing.T) {
	path := pcErasurePath()
	lanes := []pcSimLane{
		{bulk: true, stop: 6 * time.Second},
		{bulk: true, join: 6 * time.Second},
	}
	r, ctl := runPCSim(path, lanes, 10*time.Second, 0, true)
	before, after := r.goodputBetween(4, 6), r.goodputBetween(7, 9)
	t.Logf("first connection %.2f MB/s in 4-6 s, next %.2f MB/s in 7-9 s", before/1e6, after/1e6)
	if after < 0.85*before {
		t.Errorf("the next connection delivered %.2fx what the last one had reached", after/before)
	}
	if s := ctl.Stats(); s.Attachments != 1 {
		t.Errorf("%d attachments at the end, want 1", s.Attachments)
	}
}

// The reverse direction of a download: four connections carrying only flow
// acknowledgements and QUIC ACK-only packets, whose load steps tenfold as
// the downloads ramp. The path has thirty times the room. Under one
// authority nothing caps an application-limited connection at its own past
// load, the ACK-only packets are not debited, and the flow acknowledgements
// do not wait for the pacer.
func TestPathControllerReverseAckOnlyIsNotThrottled(t *testing.T) {
	path := pcSimPath{rate: 2e6, rtt: 200 * time.Millisecond, erasure: 0.015, buffer: 2e6 * 0.1, seed: 1}
	lanes := make([]pcSimLane, 4)
	for i := range lanes {
		lanes[i] = pcSimLane{join: time.Duration(i) * 20 * time.Millisecond, ackRate: 6e3, ackStep: 60e3,
			stepAt: 3 * time.Second, ackSize: 200, ackOnly: 2}
	}
	measureFrom := 4 * time.Second
	r, ctl := runPCSim(path, lanes, 10*time.Second, measureFrom, true)
	p50, p90 := pcPercentile(r.ackWaits, 0.5), pcPercentile(r.ackWaits, 0.9)
	s := ctl.Stats()
	t.Logf("reverse ACK channel: offered %d, sent %d, pacer wait p50 %v p90 %v; ACK-only packets %d", r.ackOffered, r.ackSent, p50, p90, s.NonEliciting)
	if p90 > path.rtt/4 {
		t.Errorf("flow acknowledgements waited %v (p90) on a path with thirty times their rate", p90)
	}
	if r.ackSent+r.ackSent/50 < r.ackOffered {
		t.Errorf("sent %d of %d flow acknowledgements offered after the step", r.ackSent, r.ackOffered)
	}
	if r.nonElicitingHit || s.NonEliciting == 0 {
		t.Errorf("ACK-only packets: %d registered, debited %v", s.NonEliciting, r.nonElicitingHit)
	}
	pooled, _ := runPCSim(path, lanes, 10*time.Second, measureFrom, false)
	t.Logf("today's pooled connections: pacer wait p50 %v p90 %v", pcPercentile(pooled.ackWaits, 0.5), pcPercentile(pooled.ackWaits, 0.9))
}

// The reverse direction of a download again, past the ten seconds at which
// the path's minimum RTT expires (#210). Once the step in demand is behind
// them the connections are application-limited, so their flight builds no
// queue of its own; a ProbeRTT would cap the window at four packets for a
// round trip and 200 ms, holding every flow acknowledgement and datagram
// behind it, which on a real uplink also stalls the download those
// acknowledgements release. The first expiry follows the tenfold step,
// which the pacer held back, so it is taken; its catch-up is the ProbeRTT's
// own backlog and does not count, so the later two are skipped. A bulk
// path, which always has work, still takes its ProbeRTTs.
func TestPathControllerApplicationLimitedPathSkipsProbeRTT(t *testing.T) {
	path := pcSimPath{rate: 2e6, rtt: 200 * time.Millisecond, erasure: 0.015, buffer: 2e6 * 0.1, seed: 1}
	lanes := make([]pcSimLane, 4)
	for i := range lanes {
		lanes[i] = pcSimLane{join: time.Duration(i) * 20 * time.Millisecond, ackRate: 6e3, ackStep: 60e3,
			stepAt: 3 * time.Second, ackSize: 200, ackOnly: 2}
	}
	const duration = 35 * time.Second
	measureFrom := 12 * time.Second // after the first expiry's ProbeRTT
	r, ctl := runPCSim(path, lanes, duration, measureFrom, true)
	law := ctl.law.inner
	worst := pcPercentile(r.ackWaits, 1)
	t.Logf("application-limited path: %d ProbeRTTs, %d skipped; flow acknowledgement wait after %v p99 %v, max %v",
		law.probeRTTs, law.probeRTTsSkipped, measureFrom, pcPercentile(r.ackWaits, 0.99), worst)
	if law.probeRTTs != 1 || law.probeRTTsSkipped < 2 {
		t.Errorf("application-limited path: %d ProbeRTTs, %d skipped; want the first taken and the later two skipped",
			law.probeRTTs, law.probeRTTsSkipped)
	}
	if worst > path.rtt/4 {
		t.Errorf("a flow acknowledgement waited %v on an application-limited path", worst)
	}

	_, bulk := runPCSim(pcErasurePath(), bulkLanes(1, 0), duration, 0, true)
	if bulk.law.inner.probeRTTs < 3 || bulk.law.inner.probeRTTsSkipped != 0 {
		t.Errorf("a bulk path took %d ProbeRTTs in %v and skipped %d", bulk.law.inner.probeRTTs, duration, bulk.law.inner.probeRTTsSkipped)
	}
}

// A steady producer at half the path's capacity: application-limited, with
// less spare service to clear a backlog than the sparse lanes above, so
// after a ProbeRTT its catch-up takes many rounds. The catch-up is the
// ProbeRTT's backlog, however long it takes to drain, so it must not become
// the evidence for the next ProbeRTT (#210).
func TestPathControllerLoadedProducerSkipsProbeRTT(t *testing.T) {
	path := pcSimPath{rate: 2e6, rtt: 200 * time.Millisecond, erasure: 0.015, buffer: 2e6 * 0.1, seed: 1}
	lanes := make([]pcSimLane, 4)
	for i := range lanes {
		lanes[i] = pcSimLane{ackRate: 0.5 * 2e6 / 4, ackStep: 0.5 * 2e6 / 4, ackSize: pcSimPacketSize}
	}
	measureFrom := 12 * time.Second
	r, ctl := runPCSim(path, lanes, 45*time.Second, measureFrom, true)
	law := ctl.law.inner
	worst := pcPercentile(r.ackWaits, 1)
	t.Logf("producer at 0.5 of the path: %d ProbeRTTs, %d skipped; wait after %v p99 %v, max %v",
		law.probeRTTs, law.probeRTTsSkipped, measureFrom, pcPercentile(r.ackWaits, 0.99), worst)
	if law.probeRTTs > 1 || law.probeRTTsSkipped < 3 {
		t.Errorf("producer at 0.5 of the path: %d ProbeRTTs, %d skipped; want at most the first taken and the later three skipped",
			law.probeRTTs, law.probeRTTsSkipped)
	}
	if worst > path.rtt {
		t.Errorf("a packet waited %v behind the producer's ProbeRTT", worst)
	}
}

// A bulk sender that pauses for a few milliseconds every second is briefly
// out of work, but resumes at the path's rate with its flight well above
// the bottleneck's BDP: it keeps a queue of its own, below its window. It
// must take its ProbeRTTs.
func TestPathControllerBrieflyIdleBulkTakesProbeRTT(t *testing.T) {
	path := pcErasurePath()
	// Pauses once a second, and pauses more often than once a round trip,
	// which keeps every packet's application-limited marker set: the
	// pacer's refusals between them are the evidence then.
	for _, pause := range []struct{ every, length time.Duration }{
		{time.Second, 5 * time.Millisecond}, {50 * time.Millisecond, 2 * time.Millisecond},
	} {
		lanes := []pcSimLane{{bulk: true, pauseEvery: pause.every, pauseFor: pause.length}}
		const duration = 35 * time.Second
		_, ctl := runPCSim(path, lanes, duration, 0, true)
		law := ctl.law.inner
		t.Logf("bulk idle %v every %v: %d ProbeRTTs, %d skipped", pause.length, pause.every, law.probeRTTs, law.probeRTTsSkipped)
		if law.probeRTTs < 3 || law.probeRTTsSkipped != 0 {
			t.Errorf("a bulk sender idle %v every %v took %d ProbeRTTs in %v and skipped %d",
				pause.length, pause.every, law.probeRTTs, duration, law.probeRTTsSkipped)
		}
	}
}

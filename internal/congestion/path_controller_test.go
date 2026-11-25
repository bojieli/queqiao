package congestion

import (
	"math/rand/v2"
	"net"
	"testing"
	"time"

	quiccongestion "github.com/apernet/quic-go/congestion"
	"github.com/apernet/quic-go/monotime"

	"github.com/bojieli/queqiao/internal/lossmodel"
)

type lossSnapshot = lossmodel.Snapshot

// pcConn is a test stand-in for one connection attached to a path
// controller: its own packet numbers and its own bytes in flight, reported
// exactly as the fork reports them.
type pcConn struct {
	t        *testing.T
	a        *PathAttachment
	next     quiccongestion.PacketNumber
	inFlight quiccongestion.ByteCount
	sizes    map[quiccongestion.PacketNumber]quiccongestion.ByteCount
	wakes    int
}

func newPCController() *PathController {
	return NewPathController(1200, nil, DefaultClassWeights)
}

func attachPC(t *testing.T, c *PathController, now monotime.Time) *pcConn {
	t.Helper()
	p := &pcConn{t: t, a: c.Attach(nil, nil), sizes: make(map[quiccongestion.PacketNumber]quiccongestion.ByteCount)}
	p.a.OnAttach(now, 0, 1200, func() { p.wakes++ })
	p.a.SetRTTStatsProvider(&fixedRTT{min: 100 * time.Millisecond, smoothed: 100 * time.Millisecond})
	return p
}

// send registers one ordinary bulk packet and returns its number.
func (p *pcConn) send(now monotime.Time, bytes quiccongestion.ByteCount) quiccongestion.PacketNumber {
	return p.sendKind(now, bytes, quiccongestion.KindOrdinary, true)
}

func (p *pcConn) sendKind(now monotime.Time, bytes quiccongestion.ByteCount, kind quiccongestion.PacketKind, eliciting bool) quiccongestion.PacketNumber {
	n := p.next
	p.next++
	if eliciting {
		p.inFlight += bytes
		p.sizes[n] = bytes
	}
	var cb [quiccongestion.NumSendClasses]quiccongestion.ByteCount
	if kind == quiccongestion.KindOrdinary {
		cb[quiccongestion.ClassBulk] = bytes
	}
	p.a.OnSent(quiccongestion.SentPacket{
		Space: quiccongestion.SpaceApplication, Number: n, Bytes: bytes,
		AckEliciting: eliciting, InFlightCounted: eliciting, Exempt: kind != quiccongestion.KindOrdinary,
		Kind: kind, ClassBytes: cb, InFlight: p.inFlight, Time: now,
	})
	return n
}

func (p *pcConn) refs(numbers []quiccongestion.PacketNumber) []quiccongestion.PacketRef {
	var refs []quiccongestion.PacketRef
	for _, n := range numbers {
		refs = append(refs, quiccongestion.PacketRef{Space: quiccongestion.SpaceApplication, Number: n, Bytes: p.sizes[n]})
	}
	return refs
}

// event reports acknowledgements and losses of the given packets.
func (p *pcConn) event(now monotime.Time, acked, lost []quiccongestion.PacketNumber) {
	for _, n := range append(append([]quiccongestion.PacketNumber(nil), acked...), lost...) {
		p.inFlight -= p.sizes[n]
	}
	p.a.OnAckEvent(quiccongestion.AckEvent{Time: now, InFlight: p.inFlight, RTTSample: 100 * time.Millisecond,
		Acked: p.refs(acked), Lost: p.refs(lost)})
}

func numbers(from, to quiccongestion.PacketNumber) []quiccongestion.PacketNumber {
	var n []quiccongestion.PacketNumber
	for i := from; i < to; i++ {
		n = append(n, i)
	}
	return n
}

// The round-5 review's P1-4. TUIC's sampler drops every packet state below
// a watermark derived from the largest acknowledgement. Under one send order
// across attachments, B's acknowledgement arriving before A's would erase
// the states of A's packets, still in flight; A's acknowledgement then finds
// no state and its bytes are never counted as delivered. The path controller
// retires states by fate, so the delivered total is exact whatever order the
// attachments' acknowledgements arrive in.
func TestDeliveredBytesSurviveCrossAttachmentAckReordering(t *testing.T) {
	run := func(watermark bool) PathControllerStats {
		c := newPCController()
		c.law.inner.estimator.retireByFate = !watermark
		now := base()
		a, b := attachPC(t, c, now), attachPC(t, c, now)
		// A sends first, then B sends many more: B's packets carry the
		// higher path sequence numbers.
		for range 10 {
			a.send(now, 1200)
		}
		for range 20 {
			b.send(now, 1200)
		}
		now = now.Add(100 * time.Millisecond)
		b.event(now, numbers(0, 20), nil)
		now = now.Add(5 * time.Millisecond)
		a.event(now, numbers(0, 10), nil)
		return c.Stats()
	}
	got := run(false)
	if want := uint64(30 * 1200); got.SamplerDelivered != want {
		t.Fatalf("sampler counted %d delivered bytes, want %d", got.SamplerDelivered, want)
	}
	if got.Delivered != 30 || got.Pending != 0 || got.SamplerStates != 0 {
		t.Fatalf("fates delivered %d pending %d, sampler states %d; want 30, 0, 0", got.Delivered, got.Pending, got.SamplerStates)
	}
	// The same events with the single-connection watermark lose A's bytes.
	if legacy := run(true); legacy.SamplerDelivered >= got.SamplerDelivered {
		t.Fatalf("the watermark kept %d delivered bytes; this test no longer demonstrates the defect", legacy.SamplerDelivered)
	}
}

// Test 4 of §15: alternating losses, delivered as an all-delivered batch from
// one attachment and an all-lost batch from the other, give the burst
// statistic of in-order delivery, in either order. The per-batch estimator
// of one connection, fed the same two batches, sees two long runs instead.
func TestBurstStatisticIsIndependentOfCrossAttachmentAckOrder(t *testing.T) {
	const pairs = 300
	type stat struct{ loss, lossAfterArrival, meanBurst float64 }
	of := func(s lossSnapshot) stat { return stat{s.Loss, s.LossAfterArrival, s.MeanBurst} }
	run := func(bFirst bool) stat {
		c := newPCController()
		now := base()
		a, b := attachPC(t, c, now), attachPC(t, c, now)
		for range pairs {
			a.send(now, 1200) // delivered
			b.send(now, 1200) // lost
		}
		now = now.Add(100 * time.Millisecond)
		if bFirst {
			b.event(now, nil, numbers(0, pairs))
			a.event(now, numbers(0, pairs), nil)
		} else {
			a.event(now, numbers(0, pairs), nil)
			b.event(now, nil, numbers(0, pairs))
		}
		return of(c.law.estimator.Snapshot())
	}
	reference := NewErasureSender(1200).estimator
	for range pairs {
		reference.ObserveOutcome(true)
		reference.ObserveOutcome(false)
	}
	want := of(reference.Snapshot())
	for _, bFirst := range []bool{false, true} {
		if got := run(bFirst); got != want {
			t.Errorf("lost batch first=%v: %+v, in-order %+v", bFirst, got, want)
		}
	}
	// One connection's per-batch feeding of the same two batches.
	legacy := NewErasureSender(1200).estimator
	for range pairs {
		legacy.ObserveOutcome(true)
	}
	for range pairs {
		legacy.ObserveOutcome(false)
	}
	if of(legacy.Snapshot()) == want {
		t.Fatal("per-batch feeding gives the in-order statistic; this test no longer demonstrates the defect")
	}
}

// Test 4: every sample record ends in exactly one fate, each gauge is its
// connection's flight, the path's flight is their sum and never negative,
// and the sampler holds no state once everything is resolved -- through
// acknowledgements, losses, spurious corrections, discards (PTO-probe
// declaration, migration), expiry and detach, on several attachments.
func TestEverySampleEndsInExactlyOneFate(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	c := newPCController()
	now := base()
	conns := []*pcConn{attachPC(t, c, now), attachPC(t, c, now), attachPC(t, c, now)}
	outstanding := make([]map[quiccongestion.PacketNumber]bool, len(conns))
	lostOnce := make([]map[quiccongestion.PacketNumber]bool, len(conns))
	for i := range conns {
		outstanding[i] = map[quiccongestion.PacketNumber]bool{}
		lostOnce[i] = map[quiccongestion.PacketNumber]bool{}
	}
	check := func(step int) {
		s := c.Stats()
		var sum quiccongestion.ByteCount
		for i, p := range conns {
			if p == nil {
				continue
			}
			if g := s.Gauges[p.a.epoch]; g != p.inFlight {
				t.Fatalf("step %d: attachment %d gauge %d, connection flight %d", step, i, g, p.inFlight)
			}
			if s.Counted[p.a.epoch] {
				sum += p.inFlight
			}
		}
		if s.Flight != sum || s.Flight < 0 {
			t.Fatalf("step %d: path flight %d, sum of counted gauges %d", step, s.Flight, sum)
		}
		if resolved := s.Delivered + s.Lost + s.Discarded + s.Expired; resolved+uint64(len(conns))*0 > s.Samples {
			t.Fatalf("step %d: %d fates for %d samples", step, resolved, s.Samples)
		}
	}
	for step := range 3000 {
		now = now.Add(time.Millisecond)
		i := rng.IntN(len(conns))
		p := conns[i]
		if p == nil {
			continue
		}
		switch r := rng.IntN(20); {
		case r < 10:
			outstanding[i][p.send(now, 1200)] = true
		case r < 16:
			var acked, lost []quiccongestion.PacketNumber
			for n := range outstanding[i] {
				if rng.IntN(3) == 0 {
					continue
				}
				delete(outstanding[i], n)
				if rng.IntN(4) == 0 {
					lost = append(lost, n)
					lostOnce[i][n] = true
				} else {
					acked = append(acked, n)
				}
			}
			p.event(now, acked, lost)
		case r < 17:
			// A spurious correction for something reported lost.
			for n := range lostOnce[i] {
				delete(lostOnce[i], n)
				p.a.OnAckEvent(quiccongestion.AckEvent{Time: now, InFlight: p.inFlight, Spurious: p.refs([]quiccongestion.PacketNumber{n})})
				break
			}
		case r < 19:
			// Discard some outstanding packets, as a PTO-probe declaration
			// or a migration does.
			var gone []quiccongestion.PacketNumber
			for n := range outstanding[i] {
				if rng.IntN(2) == 0 {
					delete(outstanding[i], n)
					gone = append(gone, n)
					p.inFlight -= p.sizes[n]
				}
			}
			reason := quiccongestion.DiscardPTOProbe
			if rng.IntN(2) == 0 {
				reason = quiccongestion.DiscardMigration
			}
			p.a.OnDiscarded(now, p.inFlight, p.refs(gone), false, reason)
		default:
			if step > 2000 && i == 0 {
				p.a.OnDetach(now)
				conns[0] = nil
			}
		}
		check(step)
	}
	// Resolve everything still outstanding, then let the horizon pass.
	for i, p := range conns {
		if p == nil {
			continue
		}
		var acked []quiccongestion.PacketNumber
		for n := range outstanding[i] {
			acked = append(acked, n)
		}
		p.event(now, acked, nil)
	}
	now = now.Add(time.Minute)
	conns[1].send(now, 1200)
	conns[1].event(now, []quiccongestion.PacketNumber{conns[1].next - 1}, nil)
	s := c.Stats()
	if resolved := s.Delivered + s.Lost + s.Discarded + s.Expired; resolved != s.Samples || s.Pending != 0 || s.Held != 0 {
		t.Fatalf("%d samples, fates delivered %d lost %d discarded %d expired %d (pending %d, held %d)",
			s.Samples, s.Delivered, s.Lost, s.Discarded, s.Expired, s.Pending, s.Held)
	}
	if s.SamplerStates != 0 {
		t.Fatalf("the sampler holds %d states with every fate final", s.SamplerStates)
	}
	if s.Spurious == 0 || s.Discarded == 0 {
		t.Fatalf("the run exercised no spurious correction (%d) or discard (%d)", s.Spurious, s.Discarded)
	}
}

// A spurious correction restores the packet's bytes to the delivered total,
// and flips its fate while the estimator has not yet observed it.
func TestSpuriousLossIsCorrected(t *testing.T) {
	c := newPCController()
	now := base()
	a, b := attachPC(t, c, now), attachPC(t, c, now)
	b.send(now, 1200) // pseq 1, pending: holds the estimator's cursor
	a.send(now, 1200) // pseq 2
	now = now.Add(100 * time.Millisecond)
	a.event(now, nil, []quiccongestion.PacketNumber{0})
	a.a.OnAckEvent(quiccongestion.AckEvent{Time: now, InFlight: 0, Spurious: a.refs([]quiccongestion.PacketNumber{0})})
	b.event(now, []quiccongestion.PacketNumber{0}, nil)
	s := c.Stats()
	if s.Spurious != 1 || s.SamplerDelivered != 2400 {
		t.Fatalf("spurious %d, delivered %d bytes; want 1 and 2400", s.Spurious, s.SamplerDelivered)
	}
	if snap := c.law.estimator.Snapshot(); snap.Loss != 0 {
		t.Fatalf("the corrected loss still counts: estimator loss %.3f", snap.Loss)
	}
}

// Packets that are not samples -- an MTU probe, a liveness PING, an ACK
// packet that gained a PING -- are debited when they elicit an
// acknowledgement and never enter the estimator; packets that elicit none
// are counted and not debited.
func TestOnlySamplesEnterTheEstimatorAndOnlyElicitingPacketsAreDebited(t *testing.T) {
	c := newPCController()
	now := base()
	a := attachPC(t, c, now)
	budget := func() quiccongestion.ByteCount { return c.law.pacer.budget(now) }
	start := budget()
	probe := a.sendKind(now, 1200, quiccongestion.KindMTUProbe, true)
	a.sendKind(now, 60, quiccongestion.KindLiveness, true)
	a.sendKind(now, 60, quiccongestion.KindAckPing, true)
	if got := start - budget(); got != 1320 {
		t.Fatalf("eliciting non-samples debited %d bytes, want 1320", got)
	}
	before := budget()
	for range 50 {
		a.sendKind(now, 60, quiccongestion.KindAckOnly, false)
	}
	if budget() != before {
		t.Fatal("ACK-only packets were debited")
	}
	now = now.Add(100 * time.Millisecond)
	a.event(now, nil, []quiccongestion.PacketNumber{probe})
	s := c.Stats()
	if s.Samples != 0 || c.law.estimator.Snapshot().Samples != 0 {
		t.Fatalf("non-samples entered the estimator: %d samples", s.Samples)
	}
	if tel := c.Telemetry(); tel.NonElicitingBytes != 50*60 || tel.NonElicitingDebited != 0 {
		t.Fatalf("non-eliciting %d debited %d, want %d and 0", tel.NonElicitingBytes, tel.NonElicitingDebited, 50*60)
	}
}

// Exempt eliciting packets are debited even past the balance, and the debt
// is not forgiven: ordinary admission waits for refill to repay it.
func TestExemptDebtIsNotForgiven(t *testing.T) {
	c := newPCController()
	now := base()
	a := attachPC(t, c, now)
	a.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassBulk: true}})
	for c.law.pacer.budget(now) > 0 {
		a.sendKind(now, 1200, quiccongestion.KindAckPing, true)
	}
	for range 20 {
		a.sendKind(now, 1200, quiccongestion.KindLiveness, true)
	}
	if b := c.law.pacer.budget(now); b > -20*1200+1200 {
		t.Fatalf("balance %d after 20 exempt packets past zero; the debt was forgiven", b)
	}
	if p := a.a.Admit(now, 1200); p.OK {
		t.Fatal("admitted an ordinary packet against a negative balance")
	} else if p.RetryAt.IsZero() {
		t.Fatal("a balance refusal has no RetryAt")
	}
}

// Detaching a connection takes its flight out of the path's sum, discards
// its unresolved records, and wakes an attachment the window was holding.
func TestDetachReconcilesFlight(t *testing.T) {
	c := newPCController()
	now := base()
	a, b := attachPC(t, c, now), attachPC(t, c, now)
	b.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassBulk: true}})
	window := c.law.GetCongestionWindow()
	for a.inFlight < window {
		a.send(now, 1200)
	}
	if p := b.a.Admit(now, 1200); p.OK || !p.RetryAt.IsZero() {
		t.Fatalf("admitted, or refused for pacing, with the window full of a sibling's flight: %+v", p)
	}
	b.send(now, 1200) // an exempt-path packet the window did not gate
	wakes := b.wakes
	a.a.OnDetach(now)
	s := c.Stats()
	if s.Flight != b.inFlight || s.Attachments != 1 {
		t.Fatalf("after detach the path's flight is %d with %d attachments; want b's %d with 1", s.Flight, s.Attachments, b.inFlight)
	}
	if s.Discarded == 0 || s.SamplerStates != 1 {
		t.Fatalf("detach discarded %d records and left %d sampler states; want all of a's discarded and b's one state", s.Discarded, s.SamplerStates)
	}
	if b.wakes == wakes {
		t.Fatal("the attachment held by the window was not woken")
	}
	if p := b.a.Admit(now.Add(time.Second), 1200); !p.OK {
		t.Fatalf("still refused once the sibling's flight left: %+v", p)
	}
}

// An attachment without demand in the path's turn defers to one that has it
// for one packet time; if that one does not send, its claim lapses and the
// asking attachment is served from its own demand.
func TestTurnPrecedenceAcrossAttachmentsAndLapse(t *testing.T) {
	c := newPCController()
	now := base()
	bulk, ctrl := attachPC(t, c, now), attachPC(t, c, now)
	bulk.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassBulk: true}})
	ctrl.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassControl: true}})
	p := bulk.a.Admit(now, 1200)
	if p.OK || p.RetryAt.IsZero() {
		t.Fatalf("a bulk attachment was not deferred to a sibling's control turn: %+v", p)
	}
	if ctrl.wakes == 0 {
		t.Fatal("the attachment holding the turn was not woken")
	}
	if q := ctrl.a.Admit(now, 1200); !q.OK || q.Turn != quiccongestion.ClassControl {
		t.Fatalf("the control attachment was not given its turn: %+v", q)
	}
	// The control attachment never sends. Its claim lapses.
	now = p.RetryAt
	q := bulk.a.Admit(now, 1200)
	if !q.OK || q.Turn != quiccongestion.ClassBulk {
		t.Fatalf("after the claim lapsed the bulk attachment got %+v", q)
	}
	if c.Stats().LapsedClaims == 0 {
		t.Fatal("no lapse was recorded")
	}
}

// A CONTROL turn may exceed a full window by the control debt, and nothing
// but CONTROL rides on that borrowed budget.
func TestControlTurnBorrowsTheControlDebtOnly(t *testing.T) {
	c := newPCController()
	now := base()
	a := attachPC(t, c, now)
	window := c.law.GetCongestionWindow()
	for a.inFlight < window {
		a.send(now, 1200)
	}
	now = now.Add(time.Second) // refill the balance
	a.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassControl: true, quiccongestion.ClassBulk: true}})
	p := a.a.Admit(now, 1200)
	if !p.OK || p.Turn != quiccongestion.ClassControl {
		t.Fatalf("a control turn on a full window: %+v", p)
	}
	if p.Fill != [4]quiccongestion.ByteCount{} {
		t.Fatalf("a permit on borrowed window lets other classes fill: %+v", p.Fill)
	}
	for a.inFlight < window+pathControlDebtPackets*1200 {
		a.sendKind(now, 1200, quiccongestion.KindOrdinary, true)
	}
	if p := a.a.Admit(now, 1200); p.OK {
		t.Fatalf("admitted past the window plus the control debt: %+v", p)
	}
}

// A connection whose PTO probes go unacknowledged is PROBING: its flight
// leaves the sum, so the path's window is not held shut for its siblings,
// and its packets are not samples. Its first progress returns it, with its
// current flight.
func TestUnacknowledgedAttachmentIsDemotedAndReturns(t *testing.T) {
	c := newPCController()
	now := base()
	dead, live := attachPC(t, c, now), attachPC(t, c, now)
	live.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassBulk: true}})
	window := c.law.GetCongestionWindow()
	for dead.inFlight < window {
		dead.send(now, 1200)
	}
	if live.a.Admit(now, 1200).OK {
		t.Fatal("admitted beside a full window")
	}
	for range pathProbingPTOProbes {
		now = now.Add(200 * time.Millisecond)
		dead.sendKind(now, 1200, quiccongestion.KindPTOProbe, true)
	}
	s := c.Stats()
	if s.Counted[dead.a.epoch] || s.Flight != live.inFlight {
		t.Fatalf("after %d unacknowledged PTO probes: counted %v, path flight %d", pathProbingPTOProbes, s.Counted[dead.a.epoch], s.Flight)
	}
	if p := live.a.Admit(now, 1200); !p.OK {
		t.Fatalf("the live sibling is still refused: %+v", p)
	}
	samples := s.Samples
	dead.send(now, 1200)
	if c.Stats().Samples != samples {
		t.Fatal("a PROBING attachment's packet was a sample")
	}
	dead.event(now, []quiccongestion.PacketNumber{0}, nil)
	s = c.Stats()
	if !s.Counted[dead.a.epoch] || s.Flight != dead.inFlight+live.inFlight {
		t.Fatalf("progress did not return the attachment: counted %v, flight %d, want %d", s.Counted[dead.a.epoch], s.Flight, dead.inFlight+live.inFlight)
	}
}

// Test 1 of §15 at the controller: k = 1, 2 and 4 attachments, each
// saturated, asking in turn, with acknowledgements turning the window over.
// Two bounds, each in its own domain, neither growing with k beyond the
// other attachments' packets: the path's flight never exceeds the window by
// more than k packets, and the bytes registered in any interval are at most
// the rate integrated over it, plus the burst allowance, plus k packets.
func TestSharedAdmissionBoundsDoNotGrowWithAttachments(t *testing.T) {
	for _, k := range []int{1, 2, 4} {
		c := newPCController()
		c.law.inner.randomState = 1
		now := base()
		conns := make([]*pcConn, k)
		for i := range conns {
			conns[i] = attachPC(t, c, now)
			conns[i].a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassBulk: true}})
		}
		const tick = 100 * time.Microsecond
		const window = 50 * time.Millisecond
		var inWindow, allowed float64
		var burst quiccongestion.ByteCount
		worst := 0.0
		end := now.Add(time.Second)
		step := 0
		for ; now.Before(end); now = now.Add(tick) {
			step++
			if step%1000 == 0 {
				// A round trip: everything outstanding is acknowledged.
				for _, p := range conns {
					var acked []quiccongestion.PacketNumber
					for n := range p.sizes {
						acked = append(acked, n)
					}
					p.event(now, acked, nil)
					clear(p.sizes)
				}
			}
			if step%int(window/tick) == 0 {
				if limit := allowed + float64(burst) + float64(k*1200); inWindow > limit {
					t.Fatalf("k=%d: %.0f bytes registered in %v, more than one balance allows (%.0f)", k, inWindow, window, limit)
				}
				worst = max(worst, inWindow/(allowed+float64(burst)))
				inWindow, allowed, burst = 0, 0, 0
			}
			burst = max(burst, c.law.pacer.maxBurstSize())
			allowed += float64(c.law.bandwidth()) * tick.Seconds()
			for j := range k {
				p := conns[(j+step)%k]
				for p.a.Admit(now, 1200).OK {
					w := c.law.GetCongestionWindow()
					p.send(now, 1200)
					inWindow += 1200
					if f := c.Stats().Flight; f > w+quiccongestion.ByteCount(k)*1200 {
						t.Fatalf("k=%d: flight %d exceeds the window %d by more than %d packets", k, f, w, k)
					}
				}
			}
		}
		t.Logf("k=%d: worst interval used %.2f of rate plus burst", k, worst)
	}
}

// Per-attachment fairness and no starvation: attachments with the same
// class of demand share the path's service about equally whatever their
// number, and a sparse interactive attachment beside saturated bulk ones is
// served at once.
func TestAttachmentsShareServiceAndNoneStarves(t *testing.T) {
	c := newPCController()
	now := base()
	const k = 4
	conns := make([]*pcConn, k)
	for i := range conns {
		conns[i] = attachPC(t, c, now)
		conns[i].a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassBulk: true}})
	}
	inter := attachPC(t, c, now)
	served := make([]quiccongestion.ByteCount, k)
	var interDelays []time.Duration
	var interQueued monotime.Time
	// overtaken counts bulk packets admitted while an interactive packet
	// was waiting.
	overtaken, worst := 0, 0
	end := now.Add(2 * time.Second)
	step := 0
	for ; now.Before(end); now = now.Add(time.Millisecond) {
		step++
		// Acknowledge everything a round trip old, so the window turns over.
		for _, p := range append(conns, inter) {
			var acked []quiccongestion.PacketNumber
			for n := range p.sizes {
				acked = append(acked, n)
			}
			if step%100 == 0 && len(acked) > 0 {
				p.event(now, acked, nil)
				clear(p.sizes)
			}
		}
		if step%50 == 0 && interQueued.IsZero() {
			interQueued = now
			inter.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassInteractive: true}})
		}
		// Every attachment asks in a rotating order, as independent run
		// loops would.
		for j := range k + 1 {
			i := (j + step) % (k + 1)
			if i == k {
				if !interQueued.IsZero() {
					if p := inter.a.Admit(now, 1200); p.OK {
						inter.sendKind(now, 300, quiccongestion.KindOrdinary, true)
						interDelays = append(interDelays, now.Sub(interQueued))
						interQueued = 0
						worst = max(worst, overtaken)
						overtaken = 0
						inter.a.OnDemand(quiccongestion.Demand{})
					}
				}
				continue
			}
			if p := conns[i].a.Admit(now, 1200); p.OK {
				conns[i].send(now, 1200)
				served[i] += 1200
				if !interQueued.IsZero() {
					overtaken++
				}
			}
		}
	}
	var lo, hi quiccongestion.ByteCount = served[0], served[0]
	for _, s := range served {
		lo, hi = min(lo, s), max(hi, s)
	}
	t.Logf("served per attachment %v; interactive delays %v", served, interDelays)
	if lo == 0 || float64(hi)/float64(lo) > 1.25 {
		t.Errorf("attachments with equal demand were served %v", served)
	}
	if len(interDelays) < 30 {
		t.Fatalf("the interactive attachment was served %d times", len(interDelays))
	}
	// It may wait for the shared window to open, but no bulk packet is
	// admitted ahead of it once it is queued.
	if worst > 0 {
		t.Errorf("%d bulk packets were admitted ahead of a waiting interactive packet", worst)
	}
}

// A connection that moves to another path's controller takes nothing of
// itself with it in the old one: its held losses are retired there, so the
// old controller never touches the attachment's records again (which the new
// controller's lock now guards).
func TestMigratingAttachmentLeavesNoHeldLossBehind(t *testing.T) {
	old, next := newPCController(), newPCController()
	now := base()
	a := &pcConn{t: t, sizes: map[quiccongestion.PacketNumber]quiccongestion.ByteCount{}}
	a.a = old.Attach(func(_, _, _, _ net.Addr) *PathController { return next }, nil)
	a.a.OnAttach(now, 0, 1200, func() {})
	sibling := attachPC(t, old, now)
	a.send(now, 1200)
	now = now.Add(100 * time.Millisecond)
	a.event(now, nil, []quiccongestion.PacketNumber{0})
	if old.Stats().Held != 1 {
		t.Fatal("no held loss to leave behind")
	}
	a.a.OnPathMigrated(now, nil, nil, nil, nil)
	if a.a.Controller() != next {
		t.Fatal("the attachment did not move")
	}
	if s := old.Stats(); s.Held != 0 || s.SamplerStates != 0 {
		t.Fatalf("the old controller kept %d held losses and %d sampler states of a departed attachment", s.Held, s.SamplerStates)
	}
	sibling.send(now.Add(time.Minute), 1200)
}

// An acknowledgement past the fate horizon is not credited, whether or not
// any other callback came first to expire it.
func TestAnAcknowledgementPastTheHorizonIsNotCredited(t *testing.T) {
	c := newPCController()
	now := base()
	a := attachPC(t, c, now)
	a.send(now, 1200)
	now = now.Add(time.Minute)
	a.event(now, []quiccongestion.PacketNumber{0}, nil)
	if s := c.Stats(); s.Expired != 1 || s.Delivered != 0 || s.SamplerDelivered != 0 {
		t.Fatalf("expired %d delivered %d (%d bytes); want the packet expired and nothing credited", s.Expired, s.Delivered, s.SamplerDelivered)
	}
}

// A controller nothing ever attached to is idle from its creation, so a
// registry can forget it.
func TestANeverAttachedControllerIsIdle(t *testing.T) {
	c := newPCController()
	if _, idle := c.IdleSince(); !idle {
		t.Fatal("a controller with no attachment ever is not idle")
	}
	a := attachPC(t, c, base())
	if _, idle := c.IdleSince(); idle {
		t.Fatal("an attached controller is idle")
	}
	a.a.OnDetach(base())
	if _, idle := c.IdleSince(); !idle {
		t.Fatal("a controller whose last attachment detached is not idle")
	}
}

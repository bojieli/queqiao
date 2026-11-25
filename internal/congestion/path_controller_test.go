package congestion

import (
	"math/rand/v2"
	"net"
	"slices"
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
	c := NewPathController(1200, nil, DefaultClassWeights)
	// These tests run on a fixed clock: the latest event's time stands for
	// a demand change's.
	c.clock = func() monotime.Time { return c.now }
	return c
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

// Per-attachment fairness and no starvation, with a fixed caller order: the
// first attachment always asks first when the window opens, as the
// connection whose acknowledgement opened it would, and an attachment that
// is woken asks at once, as its run loop would. Attachments with the same
// class of demand still share the path's service about equally, and a
// sparse interactive attachment beside saturated bulk ones is served ahead
// of every bulk packet once it is queued.
func TestAttachmentsShareServiceAndNoneStarves(t *testing.T) {
	c := newPCController()
	now := base()
	const k = 4
	conns := make([]*pcConn, k+1)
	var woken []int
	for i := range conns {
		p := &pcConn{t: t, sizes: map[quiccongestion.PacketNumber]quiccongestion.ByteCount{}}
		p.a = c.Attach(nil, nil)
		// Wake-ups coalesce, as a connection's scheduleSending does.
		p.a.OnAttach(now, 0, 1200, func() {
			if !slices.Contains(woken, i) {
				woken = append(woken, i)
			}
		})
		p.a.SetRTTStatsProvider(&fixedRTT{min: 100 * time.Millisecond, smoothed: 100 * time.Millisecond})
		conns[i] = p
		if i < k {
			p.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassBulk: true}})
		}
	}
	inter := conns[k]
	served := make([]quiccongestion.ByteCount, k)
	var interQueued monotime.Time
	interServed, overtaken, worst := 0, 0, 0
	// run is one attachment's send loop: it sends while admitted.
	run := func(i int) {
		p := conns[i]
		if i == k {
			if interQueued.IsZero() {
				return
			}
			if p.a.Admit(now, 1200).OK {
				p.sendKind(now, 300, quiccongestion.KindOrdinary, true)
				interServed++
				interQueued = 0
				worst = max(worst, overtaken)
				overtaken = 0
				p.a.OnDemand(quiccongestion.Demand{})
			}
			return
		}
		for p.a.Admit(now, 1200).OK {
			p.send(now, 1200)
			served[i] += 1200
			if !interQueued.IsZero() {
				overtaken++
			}
		}
	}
	end := now.Add(2 * time.Second)
	for step := 1; now.Before(end); now, step = now.Add(time.Millisecond), step+1 {
		if step%100 == 0 {
			// A round trip: every connection's outstanding packets are
			// acknowledged, the first's first.
			for _, p := range conns {
				var acked []quiccongestion.PacketNumber
				for n := range p.sizes {
					acked = append(acked, n)
				}
				if len(acked) > 0 {
					p.event(now, acked, nil)
					clear(p.sizes)
				}
			}
		}
		if step%50 == 0 && interQueued.IsZero() {
			interQueued = now
			inter.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassInteractive: true}})
		}
		for i := range conns {
			run(i)
			for len(woken) > 0 {
				w := woken[0]
				woken = woken[1:]
				run(w)
			}
		}
	}
	lo, hi := served[0], served[0]
	for _, s := range served {
		lo, hi = min(lo, s), max(hi, s)
	}
	t.Logf("served per attachment %v; interactive served %d times", served, interServed)
	if lo == 0 || float64(hi)/float64(lo) > 1.25 {
		t.Errorf("attachments with equal demand were served %v", served)
	}
	if interServed < 15 {
		t.Fatalf("the interactive attachment was served %d times", interServed)
	}
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

// A correction past its own deadline is refused even if an older held loss,
// recorded while the round trip was longer, still holds the front of the
// queue.
func TestACorrectionPastItsOwnDeadlineIsRefused(t *testing.T) {
	c := newPCController()
	now := base()
	a := attachPC(t, c, now)
	a.send(now, 1200) // 0: lost while the RTT is long
	a.send(now, 1200) // 1: lost after it falls
	c.mu.Lock()
	c.rtt.observe(2 * time.Second)
	c.mu.Unlock()
	a.event(now, nil, []quiccongestion.PacketNumber{0})
	c.mu.Lock()
	c.rtt = pathRTT{fallback: c.rtt.fallback}
	c.rtt.observe(10 * time.Millisecond)
	c.mu.Unlock()
	a.event(now, nil, []quiccongestion.PacketNumber{1})
	now = now.Add(time.Second)
	a.a.OnAckEvent(quiccongestion.AckEvent{Time: now, InFlight: 0, Spurious: a.refs([]quiccongestion.PacketNumber{1})})
	if s := c.Stats(); s.Spurious != 0 || s.SamplerDelivered != 0 {
		t.Fatalf("a correction past its deadline was accepted: spurious %d, delivered %d", s.Spurious, s.SamplerDelivered)
	}
}

// A claimant that asks but cannot use the turn for a reason of its own -- a
// PROBING attachment with its probe window full -- does not hold back a
// healthy sibling, and a lapsed claim stays lapsed until the claimant sends.
func TestAStalledClaimantDoesNotThrottleItsSibling(t *testing.T) {
	c := newPCController()
	now := base()
	stuck, healthy := attachPC(t, c, now), attachPC(t, c, now)
	for range pathProbingPTOProbes {
		stuck.sendKind(now, 1200, quiccongestion.KindPTOProbe, true)
	}
	stuck.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassControl: true}})
	healthy.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassBulk: true}})
	if p := stuck.a.Admit(now, 1200); p.OK {
		t.Fatal("a PROBING attachment with a full probe window was admitted")
	}
	sent := 0
	for range 20 {
		now = now.Add(1200 * time.Microsecond) // about a packet time: budget for one packet
		if healthy.a.Admit(now, 1200).OK {
			healthy.send(now, 1200)
			sent++
		}
	}
	if sent < 18 {
		t.Fatalf("the healthy sibling sent %d of 20 packets beside a stalled claimant", sent)
	}
}

// Each attachment publishes its own share of the path's cumulative
// counters, so connections on one path that each publish count every loss
// and sample once between them.
func TestAttachmentCountersSumToThePaths(t *testing.T) {
	c := newPCController()
	now := base()
	a, b := attachPC(t, c, now), attachPC(t, c, now)
	for range 10 {
		a.send(now, 1200)
		b.send(now, 1200)
	}
	now = now.Add(100 * time.Millisecond)
	a.event(now, numbers(0, 7), numbers(7, 10))
	b.event(now, numbers(0, 9), numbers(9, 10))
	path, ta, tb := c.Telemetry(), a.a.Telemetry(), b.a.Telemetry()
	if ta.PacketsLost+tb.PacketsLost != path.PacketsLost || ta.PacketsLost != 3 || tb.PacketsLost != 1 {
		t.Fatalf("losses: path %d, attachments %d and %d", path.PacketsLost, ta.PacketsLost, tb.PacketsLost)
	}
	if ta.Samples+tb.Samples != path.Samples {
		t.Fatalf("samples: path %d, attachments %d and %d", path.Samples, ta.Samples, tb.Samples)
	}
}

// A packet must fit the window: one byte short of a full window admits no
// full-size packet, and a CONTROL packet that fits only on the debt carries
// nothing else.
func TestAPacketMustFitTheWindow(t *testing.T) {
	c := newPCController()
	now := base()
	a := attachPC(t, c, now)
	window := c.law.GetCongestionWindow()
	for a.inFlight+1200 <= window-1 {
		a.send(now, 1200)
	}
	a.send(now, window-1-a.inFlight)
	now = now.Add(time.Second)
	a.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassBulk: true}})
	if p := a.a.Admit(now, 1200); p.OK {
		t.Fatalf("a full packet was admitted with one byte of window left: %+v", p)
	}
	a.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassControl: true, quiccongestion.ClassBulk: true}})
	if p := a.a.Admit(now, 1200); !p.OK || p.Turn != quiccongestion.ClassControl || p.Fill != [4]quiccongestion.ByteCount{} {
		t.Fatalf("a CONTROL packet on the debt: %+v", p)
	}
}

// Test 7 of §15: application-limited is an unused admission opportunity,
// measured by the controller from demand. A continuously backlogged sender
// held back only by the path's pacer and window, and one whose queue empties
// only between writes, produce no application-limited samples; one with no
// eligible demand while the window is open -- blocked on peer flow control,
// say -- does.
func TestApplicationLimitedIsAnUnusedOpportunity(t *testing.T) {
	run := func(gap func(step int) bool, blink bool) PathControllerStats {
		c := newPCController()
		now := base()
		a := attachPC(t, c, now)
		for step := 1; step <= 3000; step++ {
			now = now.Add(time.Millisecond)
			if step%100 == 0 {
				var acked []quiccongestion.PacketNumber
				for n := range a.sizes {
					acked = append(acked, n)
				}
				a.event(now, acked, nil)
				clear(a.sizes)
			}
			if gap(step) {
				a.a.OnDemand(quiccongestion.Demand{})
				continue
			}
			if blink {
				// The queue empties for an instant between two writes.
				a.a.OnDemand(quiccongestion.Demand{})
			}
			a.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassBulk: true}})
			for a.a.Admit(now, 1200).OK {
				a.send(now, 1200)
			}
		}
		return c.Stats()
	}
	appSamples := func(c PathControllerStats) uint64 { return c.AppSamples }
	if s := run(func(int) bool { return false }, false); appSamples(s) != 0 {
		t.Errorf("a backlogged sender produced %d application-limited samples", appSamples(s))
	}
	if s := run(func(int) bool { return false }, true); appSamples(s) != 0 {
		t.Errorf("a sender idle only between writes produced %d application-limited samples", appSamples(s))
	}
	// No eligible demand for half of every round trip, with the window open.
	if s := run(func(step int) bool { return step%100 >= 50 }, false); appSamples(s) == 0 {
		t.Error("a sender without demand while the window was open produced no application-limited samples")
	}
}

// The path's delay model takes the connection's ACK-delay-corrected RTT,
// not the raw time to ACK receipt the event carries: a delayed ACK is not
// queueing.
func TestPathRTTIgnoresThePeersAckDelay(t *testing.T) {
	c := newPCController()
	now := base()
	a := attachPC(t, c, now) // its connection's latest RTT is 100 ms
	a.send(now, 1200)
	now = now.Add(300 * time.Millisecond)
	a.inFlight = 0
	a.a.OnAckEvent(quiccongestion.AckEvent{Time: now, RTTSample: 300 * time.Millisecond, Acked: a.refs([]quiccongestion.PacketNumber{0})})
	c.mu.Lock()
	srtt := c.rtt.SmoothedRTT()
	c.mu.Unlock()
	if srtt != 100*time.Millisecond {
		t.Fatalf("the path's smoothed RTT is %v after a 300 ms raw sample whose corrected value is 100 ms", srtt)
	}
}

// sendClass registers one ordinary packet carrying bytes of class.
func (p *pcConn) sendClass(now monotime.Time, bytes quiccongestion.ByteCount, class quiccongestion.SendClass) {
	n := p.next
	p.next++
	p.inFlight += bytes
	p.sizes[n] = bytes
	var cb [quiccongestion.NumSendClasses]quiccongestion.ByteCount
	cb[class] = bytes
	p.a.OnSent(quiccongestion.SentPacket{
		Space: quiccongestion.SpaceApplication, Number: n, Bytes: bytes,
		AckEliciting: true, InFlightCounted: true, Kind: quiccongestion.KindOrdinary,
		ClassBytes: cb, InFlight: p.inFlight, Time: now,
	})
}

// A stalled claimant -- PROBING, its probe window full, holding CONTROL
// demand and credit -- beside a healthy sibling with two backlogged classes:
// the sibling's classes are replenished between themselves, so neither
// starves while the claimant stays stalled.
func TestALapsedClaimStarvesNoClassOfTheSibling(t *testing.T) {
	c := newPCController()
	now := base()
	stuck, healthy := attachPC(t, c, now), attachPC(t, c, now)
	for range pathProbingPTOProbes {
		stuck.sendKind(now, 1200, quiccongestion.KindPTOProbe, true)
	}
	stuck.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassControl: true}})
	healthy.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassInteractive: true, quiccongestion.ClassBulk: true}})
	stuck.a.Admit(now, 1200) // refused for its own probe window
	for step := 1; step <= 2000; step++ {
		now = now.Add(time.Millisecond)
		if step%100 == 0 {
			var acked []quiccongestion.PacketNumber
			for n := range healthy.sizes {
				acked = append(acked, n)
			}
			healthy.event(now, acked, nil)
			clear(healthy.sizes)
		}
		if p := healthy.a.Admit(now, 1200); p.OK {
			healthy.sendClass(now, 1200, p.Turn)
		}
	}
	served := c.Stats().Classes.Served
	t.Logf("served: interactive %d, bulk %d", served[quiccongestion.ClassInteractive], served[quiccongestion.ClassBulk])
	if served[quiccongestion.ClassBulk] == 0 || 4*served[quiccongestion.ClassBulk] < served[quiccongestion.ClassInteractive] {
		t.Fatalf("BULK starved beside a stalled claimant: interactive %d, bulk %d", served[quiccongestion.ClassInteractive], served[quiccongestion.ClassBulk])
	}
}

// Unused admission time is time in which a packet could have been sent. A
// sender whose queue empties on a full window, and whose next ACK opens it,
// had no opportunity in between and is not application-limited; nor does a
// due ACK -- an EXEMPT obligation -- count as demand that hides one.
func TestUnusedTimeCountsOnlyWhileAPacketCouldBeSent(t *testing.T) {
	t.Run("window-blocked", func(t *testing.T) {
		c := newPCController()
		now := base()
		a := attachPC(t, c, now)
		window := c.law.GetCongestionWindow()
		for a.inFlight+1200 <= window {
			a.send(now, 1200)
		}
		a.a.OnDemand(quiccongestion.Demand{})
		now = now.Add(300 * time.Millisecond) // within the fate horizon
		a.event(now, numbers(0, 3), nil)
		if c.law.inner.estimator.appLimited {
			t.Fatal("a window-blocked interval was read as an unused opportunity")
		}
	})
	t.Run("exempt only", func(t *testing.T) {
		c := newPCController()
		now := base()
		a := attachPC(t, c, now)
		a.send(now, 1200)
		a.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassExempt: true}})
		now = now.Add(300 * time.Millisecond) // within the fate horizon
		a.event(now, numbers(0, 1), nil)
		if !c.law.inner.estimator.appLimited {
			t.Fatal("EXEMPT obligations hid an interval with nothing to send")
		}
	})
}

// PROBING follows the transport's published progress: a late ACK of a
// packet retired for a PTO probe produces no packet event, but moves the
// progress epoch, and the attachment is COUNTED again at its next callback.
func TestProbingFollowsTransportProgress(t *testing.T) {
	c := newPCController()
	now := base()
	var tp TransportProgress
	a := &pcConn{t: t, sizes: map[quiccongestion.PacketNumber]quiccongestion.ByteCount{}}
	a.a = c.Attach(nil, nil)
	a.a.SetTransportProgress(func() TransportProgress { return tp })
	a.a.OnAttach(now, 0, 1200, func() {})
	a.send(now, 1200)
	tp.PTOExpiries = pathProbingPTOExpiries
	a.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassBulk: true}})
	if c.Stats().Counted[a.a.epoch] {
		t.Fatal("not PROBING after its transport reported probe timeouts without progress")
	}
	tp = TransportProgress{Epoch: 1}
	a.a.Admit(now, 1200)
	if !c.Stats().Counted[a.a.epoch] {
		t.Fatal("still PROBING after its transport made progress")
	}
}

// The coded reader's burst statistic is the datagrams' own: here the
// datagram-bearing packets lose independently while another attachment's
// stream packets lose in long runs, and only the path-wide figure shows it.
func TestTheDatagramBurstStatisticIsTheDatagramsOwn(t *testing.T) {
	c := newPCController()
	now := base()
	dg, bulk := attachPC(t, c, now), attachPC(t, c, now)
	var dgAcked, dgLost, bulkAcked, bulkLost []quiccongestion.PacketNumber
	for i := range 600 {
		n := dg.next
		dg.next++
		dg.inFlight += 1200
		dg.sizes[n] = 1200
		dg.a.OnSent(quiccongestion.SentPacket{Space: quiccongestion.SpaceApplication, Number: n, Bytes: 1200,
			AckEliciting: true, InFlightCounted: true, HasDatagram: true, InFlight: dg.inFlight, Time: now})
		if i%5 == 4 {
			dgLost = append(dgLost, n)
		} else {
			dgAcked = append(dgAcked, n)
		}
	}
	for i := range 600 {
		n := bulk.send(now, 1200)
		if i%40 < 10 {
			bulkLost = append(bulkLost, n)
		} else {
			bulkAcked = append(bulkAcked, n)
		}
	}
	now = now.Add(100 * time.Millisecond)
	dg.event(now, dgAcked, dgLost)
	bulk.event(now, bulkAcked, bulkLost)
	b, ok := c.DatagramBurstFactor()
	path := c.law.estimator.Snapshot().BurstFactor
	t.Logf("datagram burst %.2f (%v), path-wide %.2f", b, ok, path)
	if !ok || b > 1.1 || path < 1.4 {
		t.Fatalf("datagram burst %.2f (%v) against path-wide %.2f", b, ok, path)
	}
}

// An admitted turn that builds nothing -- its work does not fit the permit,
// demand unchanged -- is passed over for that attachment: the connection is
// woken and its next permit serves another class it has; once every class
// is passed over it is refused until the earliest hold ends, and a send
// clears the holds.
func TestAnEmptyBuildPassesTheTurnToAnotherClass(t *testing.T) {
	c := newPCController()
	now := base()
	a := attachPC(t, c, now)
	a.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassInteractive: true, quiccongestion.ClassBulk: true}})
	p := a.a.Admit(now, 1200)
	if !p.OK || p.Turn != quiccongestion.ClassInteractive {
		t.Fatalf("first permit %+v", p)
	}
	wakes := a.wakes
	a.a.OnEmptyBuild(now, p)
	if a.wakes == wakes {
		t.Fatal("the connection was not woken to take another decision")
	}
	q := a.a.Admit(now, 1200)
	if !q.OK || q.Turn != quiccongestion.ClassBulk || q.Fill[quiccongestion.ClassInteractive] != 0 {
		t.Fatalf("after INTERACTIVE built nothing the permit is %+v; want BULK with no INTERACTIVE fill", q)
	}
	a.a.OnEmptyBuild(now, q)
	r := a.a.Admit(now, 1200)
	if r.OK || r.RetryAt.IsZero() {
		t.Fatalf("with every class passed over the permit is %+v; want a refusal with a retry", r)
	}
	if s := a.a.Admit(r.RetryAt, 1200); !s.OK {
		t.Fatalf("still refused once the holds ended: %+v", s)
	}
	a.a.OnEmptyBuild(r.RetryAt, quiccongestion.Permit{Turn: quiccongestion.ClassInteractive})
	a.sendClass(r.RetryAt, 1200, quiccongestion.ClassBulk)
	if u := a.a.Admit(r.RetryAt, 1200); !u.OK || u.Turn != quiccongestion.ClassInteractive {
		t.Fatalf("a send did not clear the hold: %+v", u)
	}
}

// A debt-keeping balance is settled at the rate it was earned at: a rate
// that rises after most of an interval has passed does not apply to the
// whole interval and repay a debt the old rate had not.
func TestTheBalanceIsSettledBeforeTheRateChanges(t *testing.T) {
	rate := quiccongestion.ByteCount(100_000)
	p := newTUICPacer(func() quiccongestion.ByteCount { return rate })
	p.keepDebt = true
	t0 := base()
	p.sentPacket(t0, 40_000) // deep in debt: the bucket starts at its burst allowance
	t1 := t0.Add(100 * time.Millisecond)
	p.settle(t1) // 10 kB earned at the old rate
	rate = 10_000_000
	if b := p.budget(t1); b > 0 {
		t.Fatalf("a rate raised at %v repaid the debt earned before it: balance %d", t1.Sub(t0), b)
	}
}

// The retry after every class is passed over is the earliest hold still in
// the future among the classes with demand, not an expired one.
func TestTheEmptyBuildRetryIsAFutureHold(t *testing.T) {
	c := newPCController()
	now := base()
	a := attachPC(t, c, now)
	a.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassInteractive: true}})
	a.a.OnEmptyBuild(now, quiccongestion.Permit{Turn: quiccongestion.ClassInteractive})
	later := now.Add(10 * time.Millisecond) // INTERACTIVE's hold has expired
	a.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassBulk: true}})
	a.a.OnEmptyBuild(later, quiccongestion.Permit{Turn: quiccongestion.ClassBulk})
	p := a.a.Admit(later, 1200)
	if p.OK || !p.RetryAt.After(later) {
		t.Fatalf("with BULK held the permit is %+v; want a refusal until BULK's hold ends", p)
	}
}

// Every attachment reports the path's undivided window and rate -- what its
// connection's flow control may use -- with the controller's identity, so a
// reader summing over connections counts the path once.
func TestAttachmentsReportTheUndividedPathWithItsIdentity(t *testing.T) {
	c := newPCController()
	now := base()
	a, b := attachPC(t, c, now), attachPC(t, c, now)
	ta, tb, path := a.a.Telemetry(), b.a.Telemetry(), c.Telemetry()
	if ta.CongestionWindow != path.CongestionWindow || tb.PacingRate != path.PacingRate {
		t.Fatalf("an attachment reports a divided window or rate: %d/%d against %d/%d", ta.CongestionWindow, tb.PacingRate, path.CongestionWindow, path.PacingRate)
	}
	if ta.Authority == 0 || ta.Authority != tb.Authority {
		t.Fatalf("authority %d and %d", ta.Authority, tb.Authority)
	}
}

// An attachment with no ordinary work asks from its send loop all the same;
// beside a busy sibling it neither defers, nor wakes the sibling, nor
// registers as a waiter.
func TestAnIdleAttachmentDoesNotContend(t *testing.T) {
	c := newPCController()
	now := base()
	idle, busy := attachPC(t, c, now), attachPC(t, c, now)
	busy.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassBulk: true}})
	window := c.law.GetCongestionWindow()
	for busy.inFlight+1200 <= window {
		busy.send(now, 1200)
	}
	wakes := busy.wakes
	for range 10 {
		if p := idle.a.Admit(now, 1200); p.OK || !p.RetryAt.IsZero() {
			t.Fatalf("an idle attachment beside a full window got %+v", p)
		}
	}
	busy.event(now.Add(100*time.Millisecond), numbers(0, 3), nil)
	if busy.wakes != wakes || idle.wakes != 0 {
		t.Fatalf("idle asking woke the sibling (%d) or registered itself as a waiter (%d)", busy.wakes-wakes, idle.wakes)
	}
	if s := c.Stats(); s.RefusedTurn != 0 {
		t.Fatalf("the idle attachment deferred %d times", s.RefusedTurn)
	}
}

// After an unused opportunity the marker is applied before the first packet
// of the resumed burst is registered: on a drained path no ACK comes between
// the idle interval and that packet.
func TestTheFirstPacketAfterAnUnusedOpportunityIsApplicationLimited(t *testing.T) {
	c := newPCController()
	now := base()
	a := attachPC(t, c, now)
	a.a.OnDemand(quiccongestion.Demand{})
	now = now.Add(200 * time.Millisecond)
	c.mu.Lock()
	c.now = now
	c.mu.Unlock()
	a.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassBulk: true}})
	a.send(now, 1200)
	c.mu.Lock()
	state, ok := c.law.inner.estimator.packetStates[1]
	c.mu.Unlock()
	if !ok || !state.appLimited {
		t.Fatalf("the first packet after an unused opportunity was registered as a path measurement (state %v)", ok)
	}
}

// The datagram burst override rests on transition evidence that decays with
// the statistic: once datagram-bearing samples stop sitting next to each
// other for long enough, it is withdrawn and the path-wide figure stands.
func TestTheDatagramOverrideIsWithdrawnWhenItsEvidenceDecays(t *testing.T) {
	c := newPCController()
	now := base()
	a := attachPC(t, c, now)
	send := func(datagram bool) quiccongestion.PacketNumber {
		n := a.next
		a.next++
		a.inFlight += 1200
		a.sizes[n] = 1200
		a.a.OnSent(quiccongestion.SentPacket{Space: quiccongestion.SpaceApplication, Number: n, Bytes: 1200,
			AckEliciting: true, InFlightCounted: true, HasDatagram: datagram, InFlight: a.inFlight, Time: now})
		return n
	}
	var batch []quiccongestion.PacketNumber
	for range 400 {
		batch = append(batch, send(true))
	}
	a.event(now, batch, nil)
	if _, ok := c.DatagramBurstFactor(); !ok {
		t.Fatal("no override after 400 adjacent datagram samples")
	}
	for range 10 {
		batch = batch[:0]
		for range 1000 {
			batch = append(batch, send(true), send(false))
		}
		a.event(now, batch, nil)
	}
	if b, ok := c.DatagramBurstFactor(); ok {
		t.Fatalf("the override %.2f stands on evidence that has decayed away", b)
	}
}

// Time is judged against the flight before an attachment leaves: a full
// window that a detach opens was not an unused opportunity before it.
func TestADetachDoesNotMakeThePastLookUnused(t *testing.T) {
	c := newPCController()
	now := base()
	full, other := attachPC(t, c, now), attachPC(t, c, now)
	window := c.law.GetCongestionWindow()
	for full.inFlight+1200 <= window {
		full.send(now, 1200)
	}
	other.send(now, 1200)
	other.a.OnDemand(quiccongestion.Demand{})
	now = now.Add(300 * time.Millisecond)
	full.a.OnDetach(now)
	other.event(now, numbers(0, 1), nil)
	if c.law.inner.estimator.appLimited {
		t.Fatal("the time before the detach was counted as an unused opportunity")
	}
}

// On a fast path an unused opportunity is measured in packet times, not in
// the pacing timer's 1 ms floor: at about 100 MB/s a packet takes some
// 12 µs, and half a millisecond with nothing to send and the window open is
// many missed packets.
func TestAnUnusedOpportunityIsMeasuredInPacketTimesOnAFastPath(t *testing.T) {
	c := newPCController()
	now := base()
	p := &pcConn{t: t, a: c.Attach(nil, nil), sizes: make(map[quiccongestion.PacketNumber]quiccongestion.ByteCount)}
	p.a.OnAttach(now, 0, 1200, func() {})
	p.a.SetRTTStatsProvider(&fixedRTT{min: time.Millisecond, smoothed: time.Millisecond})
	if pt := c.serialTime(1200); pt >= 100*time.Microsecond {
		t.Fatalf("the test needs a fast path; a packet takes %v", pt)
	}
	p.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassBulk: true}})
	p.send(now, 1200)
	p.a.OnDemand(quiccongestion.Demand{})
	now = now.Add(500 * time.Microsecond)
	p.event(now, numbers(0, 1), nil)
	if !c.law.inner.estimator.appLimited {
		t.Fatal("half a millisecond of open window with nothing to send, at many packets per millisecond, was not an unused opportunity")
	}
}

// Admission never visits idle attachments: with thousands of idle siblings
// on the path, a busy attachment's permit costs what it costs alone.
func TestAdmissionCostDoesNotGrowWithIdleSiblings(t *testing.T) {
	permitCost := func(idle int) time.Duration {
		c := newPCController()
		now := base()
		busy := attachPC(t, c, now)
		for range idle {
			attachPC(t, c, now)
		}
		busy.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassBulk: true}})
		const n = 2000
		start := time.Now()
		for range n {
			now = now.Add(time.Millisecond)
			if busy.a.Admit(now, 1200).OK {
				busy.send(now, 1200)
			}
			if busy.inFlight > 20*1200 {
				busy.event(now, numbers(busy.next-20, busy.next), nil)
			}
		}
		return time.Since(start) / n
	}
	alone, crowded := permitCost(0), permitCost(5000)
	t.Logf("per packet: %v alone, %v beside 5000 idle siblings", alone, crowded)
	if crowded > 10*alone+20*time.Microsecond {
		t.Fatalf("admission beside 5000 idle siblings costs %v per packet against %v alone", crowded, alone)
	}
}

func BenchmarkAdmissionBesideIdleSiblings(b *testing.B) {
	c := NewPathController(1200, nil, DefaultClassWeights)
	c.clock = func() monotime.Time { return c.now }
	now := base()
	t := &testing.T{}
	busy := attachPC(t, c, now)
	for range 5000 {
		attachPC(t, c, now)
	}
	busy.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassBulk: true}})
	b.ResetTimer()
	for range b.N {
		now = now.Add(time.Millisecond)
		if busy.a.Admit(now, 1200).OK {
			busy.send(now, 1200)
		}
		if busy.inFlight > 20*1200 {
			busy.event(now, numbers(busy.next-20, busy.next), nil)
		}
	}
}

// A CONTROL-only attachment beside a sibling holding the BULK turn, with the
// window full: the sibling cannot send BULK, so its claim gives it nothing,
// and the CONTROL packet goes on the control debt at once instead of
// waiting for the flight to drain.
func TestControlUsesItsDebtWhenTheBulkClaimantCannotSend(t *testing.T) {
	c := newPCController()
	now := base()
	bulk, ctrl := attachPC(t, c, now), attachPC(t, c, now)
	bulk.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassBulk: true}})
	window := c.law.GetCongestionWindow()
	for bulk.inFlight+1200 <= window {
		bulk.sendClass(now, 1200, quiccongestion.ClassBulk)
	}
	now = now.Add(time.Second) // the balance refills; the window stays full
	ctrl.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassControl: true}})
	if turn := c.arb.Select(); turn != quiccongestion.ClassBulk {
		// Make BULK the path's turn: CONTROL has spent its credit.
		c.arb.OnSent([4]quiccongestion.ByteCount{quiccongestion.ClassControl: 1 << 20})
	}
	if c.arb.Select() != quiccongestion.ClassBulk {
		t.Fatal("setup: BULK is not the path's turn")
	}
	p := ctrl.a.Admit(now, 1200)
	if !p.OK || p.Turn != quiccongestion.ClassControl || p.Fill != [4]quiccongestion.ByteCount{} {
		t.Fatalf("CONTROL beside a BULK claimant that cannot send got %+v; want CONTROL on the debt", p)
	}
}

// A loss corrected before the estimator observes it moves between the fate
// counts: the totals agree with what the estimator counts.
func TestACorrectedLossMovesBetweenTheFateCounts(t *testing.T) {
	c := newPCController()
	now := base()
	a, b := attachPC(t, c, now), attachPC(t, c, now)
	b.send(now, 1200) // an unresolved predecessor holds the record in the ring
	a.send(now, 1200)
	now = now.Add(100 * time.Millisecond)
	a.event(now, nil, []quiccongestion.PacketNumber{0})
	a.a.OnAckEvent(quiccongestion.AckEvent{Time: now, Spurious: a.refs([]quiccongestion.PacketNumber{0})})
	if s := c.Stats(); s.Lost != 0 || s.Delivered != 1 {
		t.Fatalf("after a correction: lost %d, delivered %d; want 0 and 1", s.Lost, s.Delivered)
	}
}

// Sustained CONTROL beside a live BULK claimant, the window full: each ACK
// frees one packet. CONTROL may go past the claim once, on its debt, while
// BULK cannot send; after that the claimant gets the next opening, so BULK
// keeps a share however long CONTROL keeps asking.
func TestRepeatedControlBorrowingDoesNotStarveALiveClaim(t *testing.T) {
	c := newPCController()
	now := base()
	bulk, ctrl := attachPC(t, c, now), attachPC(t, c, now)
	bulk.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassBulk: true}})
	ctrl.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassControl: true}})
	type pkt struct {
		conn *pcConn
		n    quiccongestion.PacketNumber
	}
	var queue []pkt
	window := c.law.GetCongestionWindow()
	for bulk.inFlight+1200 <= window {
		bulk.sendClass(now, 1200, quiccongestion.ClassBulk)
		queue = append(queue, pkt{bulk, bulk.next - 1})
	}
	// BULK owns the turn: CONTROL has spent its credit.
	c.arb.OnSent([4]quiccongestion.ByteCount{quiccongestion.ClassControl: 1 << 20})
	sent := map[quiccongestion.SendClass]int{}
	try := func(p *pcConn) {
		if permit := p.a.Admit(now, 1200); permit.OK {
			p.sendClass(now, 1200, permit.Turn)
			queue = append(queue, pkt{p, p.next - 1})
			sent[permit.Turn]++
		}
	}
	for range 200 {
		now = now.Add(5 * time.Millisecond)
		// One packet is acknowledged: the oldest.
		head := queue[0]
		queue = queue[1:]
		head.conn.event(now, []quiccongestion.PacketNumber{head.n}, nil)
		try(ctrl) // the CONTROL sender asks first every time
		try(bulk)
	}
	t.Logf("sent: control %d, bulk %d", sent[quiccongestion.ClassControl], sent[quiccongestion.ClassBulk])
	if sent[quiccongestion.ClassBulk] < 60 {
		t.Fatalf("a live BULK claim was starved by repeated CONTROL borrowing: control %d, bulk %d", sent[quiccongestion.ClassControl], sent[quiccongestion.ClassBulk])
	}
	if sent[quiccongestion.ClassControl] == 0 {
		t.Fatal("CONTROL never went past a claim its holder could not use")
	}
}

// The claimant is judged by its own packet size: with exactly one of its
// packets of window left, a 1,200-byte BULK claimant can use its turn, and a
// CONTROL caller building 1,500-byte packets is not let past it.
func TestTheClaimantIsJudgedByItsOwnPacketSize(t *testing.T) {
	c := newPCController()
	now := base()
	bulk := attachPC(t, c, now)
	ctrl := &pcConn{t: t, a: c.Attach(nil, nil), sizes: make(map[quiccongestion.PacketNumber]quiccongestion.ByteCount)}
	ctrl.a.OnAttach(now, 0, 1500, func() { ctrl.wakes++ })
	bulk.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassBulk: true}})
	window := c.law.GetCongestionWindow()
	for bulk.inFlight+2400 <= window {
		bulk.sendClass(now, 1200, quiccongestion.ClassBulk)
	}
	bulk.sendClass(now, window-1200-bulk.inFlight, quiccongestion.ClassBulk) // exactly 1,200 bytes left
	now = now.Add(time.Second)
	c.arb.OnSent([4]quiccongestion.ByteCount{quiccongestion.ClassControl: 1 << 20})
	ctrl.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassControl: true}})
	wakes := bulk.wakes
	if p := ctrl.a.Admit(now, 1500); p.OK {
		t.Fatalf("CONTROL went past a claimant that could send its own packet: %+v", p)
	} else if p.RetryAt.IsZero() || !p.RetryAt.After(now) {
		t.Fatalf("the caller was refused as for the window, not deferred to the claimant: %+v", p)
	}
	if bulk.wakes == wakes {
		t.Fatal("the claimant that can send was not woken")
	}
	if p := bulk.a.Admit(now, 1200); !p.OK || p.Turn != quiccongestion.ClassBulk {
		t.Fatalf("the claimant could not use its turn: %+v", p)
	}
}

// The idle interval is marked once, when demand resumes: the burst's first
// ACK does not mark it again, so packets sent after it, with demand
// continuous since the resume, are measurements of the path.
func TestAResumedBurstIsMarkedOnce(t *testing.T) {
	c := newPCController()
	now := base()
	a := attachPC(t, c, now)
	a.a.OnDemand(quiccongestion.Demand{})
	now = now.Add(200 * time.Millisecond)
	c.mu.Lock()
	c.now = now
	c.mu.Unlock()
	a.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassBulk: true}})
	for range 5 {
		a.send(now, 1200)
	}
	now = now.Add(50 * time.Millisecond)
	a.event(now, numbers(0, 1), nil)
	a.send(now, 1200)
	c.mu.Lock()
	state, ok := c.law.inner.estimator.packetStates[6]
	c.mu.Unlock()
	if !ok || state.appLimited {
		t.Fatalf("a packet sent after the burst's first ACK, with demand continuous, was marked application-limited (state %v)", ok)
	}
}

// A CONTROL attachment that cannot use a bypass -- PROBING, its probe
// window full -- is not charged one: the claimant's single bypass is still
// there for a sibling that can.
func TestABypassIsChargedOnlyWhenGranted(t *testing.T) {
	c := newPCController()
	now := base()
	bulk, stuck, ctrl := attachPC(t, c, now), attachPC(t, c, now), attachPC(t, c, now)
	bulk.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassBulk: true}})
	window := c.law.GetCongestionWindow()
	for bulk.inFlight+1200 <= window {
		bulk.sendClass(now, 1200, quiccongestion.ClassBulk)
	}
	for range pathProbingPTOProbes {
		stuck.sendKind(now, 1200, quiccongestion.KindPTOProbe, true)
	}
	now = now.Add(time.Second)
	c.arb.OnSent([4]quiccongestion.ByteCount{quiccongestion.ClassControl: 1 << 20})
	stuck.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassControl: true}})
	if p := stuck.a.Admit(now, 1200); p.OK {
		t.Fatalf("a PROBING attachment with a full probe window was admitted: %+v", p)
	}
	ctrl.a.OnDemand(quiccongestion.Demand{Eligible: [4]bool{quiccongestion.ClassControl: true}})
	if p := ctrl.a.Admit(now, 1200); !p.OK || p.Turn != quiccongestion.ClassControl {
		t.Fatalf("the healthy CONTROL attachment found the bypass spent by one that got no permit: %+v", p)
	}
}

// Migration leaves an attachment's bypass standing behind: it starts on the
// new controller with none.
func TestABypassDoesNotFollowAMigration(t *testing.T) {
	old, next := newPCController(), newPCController()
	now := base()
	a := &pcConn{t: t, sizes: map[quiccongestion.PacketNumber]quiccongestion.ByteCount{}}
	a.a = old.Attach(func(_, _, _, _ net.Addr) *PathController { return next }, nil)
	a.a.OnAttach(now, 0, 1200, func() {})
	old.mu.Lock()
	a.a.bypassed = true
	old.mu.Unlock()
	a.a.OnPathMigrated(now, nil, nil, nil, nil)
	next.mu.Lock()
	defer next.mu.Unlock()
	if a.a.bypassed {
		t.Fatal("a bypass charged on the old controller followed the attachment to the new one")
	}
}

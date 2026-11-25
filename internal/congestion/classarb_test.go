package congestion

import (
	"math"
	"testing"

	quiccongestion "github.com/apernet/quic-go/congestion"
	"github.com/apernet/quic-go/monotime"
)

const (
	arbPacket   = 1252 // packet size the simulated builder fills
	arbOverhead = 30   // header and AEAD, charged to the turn
)

func demandOf(classes ...quiccongestion.SendClass) quiccongestion.Demand {
	var d quiccongestion.Demand
	for _, c := range classes {
		d.Eligible[c] = true
	}
	return d
}

// simBuilder executes a permit the way the fork's builder does: the turn's
// class first, then each class in Fill up to its allowance, in priority
// order. INTERACTIVE work is indivisible datagrams of datagramSize bytes when
// datagramSize is set; everything else divides freely.
func simBuilder(p quiccongestion.Permit, demand quiccongestion.Demand, datagramSize quiccongestion.ByteCount) [quiccongestion.NumSendClasses]quiccongestion.ByteCount {
	var out [quiccongestion.NumSendClasses]quiccongestion.ByteCount
	remaining := quiccongestion.ByteCount(arbPacket - arbOverhead)
	order := []quiccongestion.SendClass{p.Turn}
	for _, c := range scheduledClasses {
		if c != p.Turn {
			order = append(order, c)
		}
	}
	for _, c := range order {
		if !demand.Eligible[c] || remaining <= 0 {
			continue
		}
		limit := remaining
		if c != p.Turn {
			limit = min(limit, p.Fill[c])
		}
		if c == quiccongestion.ClassInteractive && datagramSize > 0 {
			for datagramSize <= limit {
				out[c] += datagramSize
				limit -= datagramSize
				remaining -= datagramSize
			}
			continue
		}
		out[c] += limit
		remaining -= limit
	}
	for _, c := range order {
		if out[c] > 0 {
			out[p.Turn] += arbOverhead
			break
		}
	}
	return out
}

// round runs n packets through an arbiter with fixed demand.
func runArbiter(a *ClassArbiter, demand quiccongestion.Demand, n int, datagramSize quiccongestion.ByteCount) {
	a.OnDemand(demand)
	for range n {
		p := a.Permit(arbPacket)
		if !p.OK {
			panic("the arbiter refused")
		}
		a.OnSent(simBuilder(p, demand, datagramSize))
	}
}

// Review round 5, finding 2: the arbiter can have eligible work and no
// selectable class. CONTROL holds the turn with a positive deficit while
// INTERACTIVE is in debt; CONTROL's demand disappears between the permit and
// the build, the build carries nothing, and no later packet or timer comes.
// The sole indebted class must still be selectable at once.
func TestClassArbiterSoleIndebtedClassIsSelectable(t *testing.T) {
	a := NewClassArbiter(DefaultClassWeights, arbPacket)
	a.OnDemand(demandOf(quiccongestion.ClassControl, quiccongestion.ClassInteractive))
	// Put INTERACTIVE in debt: a full packet of it under CONTROL's turn.
	p := a.Permit(arbPacket)
	if p.Turn != quiccongestion.ClassControl {
		t.Fatalf("turn %v, want CONTROL first", p.Turn)
	}
	a.OnSent([quiccongestion.NumSendClasses]quiccongestion.ByteCount{quiccongestion.ClassInteractive: 20 * arbPacket})
	st := a.Stats()
	if st.Deficit[quiccongestion.ClassInteractive] > 0 || st.Deficit[quiccongestion.ClassControl] <= 0 {
		t.Fatalf("setup: deficits %v, want CONTROL positive and INTERACTIVE indebted", st.Deficit)
	}

	// CONTROL takes a permit, then its demand disappears before the build.
	if p := a.Permit(arbPacket); p.Turn != quiccongestion.ClassControl {
		t.Fatalf("turn %v, want CONTROL", p.Turn)
	}
	a.OnDemand(demandOf(quiccongestion.ClassInteractive))
	// The build found nothing to send: no OnSent. The next admission:
	p = a.Permit(arbPacket)
	if !p.OK || p.Turn != quiccongestion.ClassInteractive {
		t.Fatalf("permit %+v: the sole indebted class with demand is not selectable", p)
	}

	// An idle class that becomes eligible after the last settlement, in
	// debt, is selectable too.
	a.OnDemand(quiccongestion.Demand{})
	a.OnSent([quiccongestion.NumSendClasses]quiccongestion.ByteCount{quiccongestion.ClassBulk: 50 * arbPacket})
	a.OnDemand(demandOf(quiccongestion.ClassBulk))
	if p := a.Permit(arbPacket); p.Turn != quiccongestion.ClassBulk {
		t.Fatalf("turn %v, want the newly eligible indebted BULK", p.Turn)
	}
}

// With every class saturated, served bytes follow the weights; a class alone
// gets everything; CONTROL with little demand is served first.
func TestClassArbiterServesTheWeights(t *testing.T) {
	a := NewClassArbiter(DefaultClassWeights, arbPacket)
	all := demandOf(quiccongestion.ClassControl, quiccongestion.ClassInteractive, quiccongestion.ClassBulk)
	runArbiter(a, all, 7000, 0)
	s := a.Stats().Served
	total := float64(s[quiccongestion.ClassControl] + s[quiccongestion.ClassInteractive] + s[quiccongestion.ClassBulk])
	for c, w := range map[quiccongestion.SendClass]float64{
		quiccongestion.ClassControl: 4.0 / 7, quiccongestion.ClassInteractive: 2.0 / 7, quiccongestion.ClassBulk: 1.0 / 7,
	} {
		if got := float64(s[c]) / total; math.Abs(got-w) > 0.01 {
			t.Errorf("class %d served %.3f of the bytes, want %.3f", c, got, w)
		}
	}

	alone := NewClassArbiter(DefaultClassWeights, arbPacket)
	runArbiter(alone, demandOf(quiccongestion.ClassBulk), 100, 0)
	if got := alone.Stats().Served[quiccongestion.ClassBulk]; got != 100*arbPacket {
		t.Fatalf("BULK alone served %d bytes, want every byte (%d)", got, 100*arbPacket)
	}
}

// At a rate so low that control demand exceeds its share, BULK is still
// served its weight: CONTROL saturated against BULK gets 4/5, BULK 1/5, and
// BULK is never more than a few packets from its next turn.
func TestClassArbiterNeverStarvesBulk(t *testing.T) {
	a := NewClassArbiter(DefaultClassWeights, arbPacket)
	d := demandOf(quiccongestion.ClassControl, quiccongestion.ClassBulk)
	a.OnDemand(d)
	gap, maxGap := 0, 0
	for range 5000 {
		p := a.Permit(arbPacket)
		sent := simBuilder(p, d, 0)
		a.OnSent(sent)
		if sent[quiccongestion.ClassBulk] > 0 {
			gap = 0
		} else if gap++; gap > maxGap {
			maxGap = gap
		}
	}
	s := a.Stats().Served
	share := float64(s[quiccongestion.ClassBulk]) / float64(s[quiccongestion.ClassBulk]+s[quiccongestion.ClassControl])
	if math.Abs(share-0.2) > 0.01 {
		t.Fatalf("BULK share %.3f under saturated CONTROL, want 0.2", share)
	}
	if maxGap > 5 {
		t.Fatalf("BULK went %d packets without service", maxGap)
	}
}

// Test 2 of §15.1: with 9:1 weights, 900-byte datagrams and a saturated bulk
// stream, served bytes stay within one packet per round of the weights.
func TestClassArbiterIndivisibleDatagramsStayWithinOnePacketPerRound(t *testing.T) {
	w := ClassWeights{Control: 1, Interactive: 9, Bulk: 1}
	a := NewClassArbiter(w, arbPacket)
	d := demandOf(quiccongestion.ClassInteractive, quiccongestion.ClassBulk)
	a.OnDemand(d)
	const datagram = 903 // 900 bytes of payload and the frame's header
	for n := 1; n <= 20000; n++ {
		a.OnSent(simBuilder(a.Permit(arbPacket), d, datagram))
		if n%1000 != 0 {
			continue
		}
		s := a.Stats().Served
		served := float64(s[quiccongestion.ClassInteractive] + s[quiccongestion.ClassBulk])
		// Rounds of 10 quanta each, so far.
		rounds := served / (10 * arbPacket)
		if s[quiccongestion.ClassBulk] == 0 {
			t.Fatalf("after %d packets BULK has not been served", n)
		}
		for c, weight := range map[quiccongestion.SendClass]float64{quiccongestion.ClassInteractive: 0.9, quiccongestion.ClassBulk: 0.1} {
			if dev := math.Abs(float64(s[c]) - weight*served); dev > rounds*arbPacket {
				t.Fatalf("after %d packets class %d deviates from its weight by %.0f bytes, more than one packet per round (%.0f rounds)", n, c, dev, rounds)
			}
		}
	}
	s := a.Stats().Served
	if share := float64(s[quiccongestion.ClassInteractive]) / float64(s[quiccongestion.ClassInteractive]+s[quiccongestion.ClassBulk]); math.Abs(share-0.9) > 0.02 {
		t.Fatalf("INTERACTIVE share %.3f, want 0.9", share)
	}
}

// The erasure sender schedules classes only when enabled, and then answers a
// permitted admission with the arbiter's turn; pacing and the window still
// decide whether a packet goes at all.
func TestErasurePathSenderClassScheduling(t *testing.T) {
	plain := NewErasurePathSender(NewErasureSenderOn(1252, nil), nil)
	if plain.SchedulesClasses() {
		t.Fatal("class scheduling is on by default")
	}
	p := NewErasurePathSender(NewErasureSenderOn(1252, nil), nil)
	p.EnableClassScheduling(DefaultClassWeights)
	if !p.SchedulesClasses() {
		t.Fatal("class scheduling did not turn on")
	}
	p.OnAttach(0, 0, 1252, func() {})
	p.OnDemand(demandOf(quiccongestion.ClassControl, quiccongestion.ClassBulk))
	permit := p.Admit(1, 1252)
	if !permit.OK || permit.Turn != quiccongestion.ClassControl || permit.Fill[quiccongestion.ClassBulk] <= 0 {
		t.Fatalf("permit %+v, want CONTROL's turn with BULK filling", permit)
	}
	p.OnSent(quiccongestion.SentPacket{Bytes: 1252, AckEliciting: true, InFlightCounted: true, InFlight: 1252, Time: 1,
		ClassBytes: [quiccongestion.NumSendClasses]quiccongestion.ByteCount{quiccongestion.ClassBulk: 1252}})
	if st, ok := p.ClassStats(); !ok || st.Served[quiccongestion.ClassBulk] != 1252 {
		t.Fatalf("stats %+v, want the packet settled", st)
	}
}

// A class that becomes eligible while another holds credit joins the current
// round: an ACK arriving during BULK-only service gets the next turn instead
// of waiting out BULK's allocation, and toggling its demand earns no more
// than one quantum per round.
func TestClassArbiterNewlyEligibleControlJoinsTheRound(t *testing.T) {
	a := NewClassArbiter(ClassWeights{Control: 4, Interactive: 2, Bulk: 50}, arbPacket)
	bulk := demandOf(quiccongestion.ClassBulk)
	a.OnDemand(bulk)
	a.OnSent(simBuilder(a.Permit(arbPacket), bulk, 0)) // BULK now has most of its large credit left
	both := demandOf(quiccongestion.ClassControl, quiccongestion.ClassBulk)
	a.OnDemand(both)
	if p := a.Permit(arbPacket); p.Turn != quiccongestion.ClassControl {
		t.Fatalf("turn %v: newly eligible CONTROL waits behind BULK's credit", p.Turn)
	}
	before := a.Stats().Deficit[quiccongestion.ClassControl]
	for range 10 {
		a.OnDemand(bulk)
		a.OnDemand(both)
	}
	if after := a.Stats().Deficit[quiccongestion.ClassControl]; after > before {
		t.Fatalf("toggling demand raised CONTROL's credit from %d to %d", before, after)
	}
}

// A demand toggle after a replenishment earns no second allocation in the
// same round: bursty CONTROL against backlogged BULK still gets 4:1.
func TestClassArbiterTogglesAfterReplenishmentKeepTheWeights(t *testing.T) {
	a := NewClassArbiter(DefaultClassWeights, arbPacket)
	both := demandOf(quiccongestion.ClassControl, quiccongestion.ClassBulk)
	bulk := demandOf(quiccongestion.ClassBulk)
	a.OnDemand(both)
	for range 5000 {
		p := a.Permit(arbPacket)
		a.OnSent(simBuilder(p, both, 0))
		if a.Stats().Deficit[quiccongestion.ClassControl] <= 0 {
			// CONTROL has spent its allocation; its queue empties and
			// refills before BULK's turn.
			a.OnDemand(bulk)
			a.OnDemand(both)
		}
	}
	s := a.Stats().Served
	share := float64(s[quiccongestion.ClassBulk]) / float64(s[quiccongestion.ClassBulk]+s[quiccongestion.ClassControl])
	if share < 0.19 {
		t.Fatalf("BULK share %.3f under toggling CONTROL, want its weight 0.2", share)
	}
}

// A class that spends part of its allocation, goes idle and returns within
// the same round keeps the rest: a sparse ACK after a small one still gets
// the turn while INTERACTIVE holds credit. It gets no more than that.
func TestClassArbiterIdleClassKeepsItsUnspentShareForTheRound(t *testing.T) {
	a := NewClassArbiter(DefaultClassWeights, arbPacket)
	ci := demandOf(quiccongestion.ClassControl, quiccongestion.ClassInteractive)
	i := demandOf(quiccongestion.ClassInteractive)
	a.OnDemand(ci)
	if p := a.Permit(arbPacket); p.Turn != quiccongestion.ClassControl {
		t.Fatalf("turn %v, want CONTROL", p.Turn)
	}
	a.OnSent([quiccongestion.NumSendClasses]quiccongestion.ByteCount{quiccongestion.ClassControl: 100}) // a small ACK
	left := a.Stats().Deficit[quiccongestion.ClassControl]
	a.OnDemand(i)
	a.OnSent(simBuilder(a.Permit(arbPacket), i, 0)) // INTERACTIVE, still in credit
	a.OnDemand(ci)
	if p := a.Permit(arbPacket); p.Turn != quiccongestion.ClassControl {
		t.Fatalf("turn %v: CONTROL lost its unspent share by going idle", p.Turn)
	}
	if got := a.Stats().Deficit[quiccongestion.ClassControl]; got > left {
		t.Fatalf("CONTROL came back with %d, more than the %d it left", got, left)
	}
}

// Bytes charged to a class while it is idle -- a PTO probe retransmitting
// its data -- come off the credit it gets back: 500 held, 1000 sent while
// idle, leaves a 500-byte debt.
func TestClassArbiterIdleDebitComesOffHeldCredit(t *testing.T) {
	a := NewClassArbiter(DefaultClassWeights, arbPacket)
	ci := demandOf(quiccongestion.ClassControl, quiccongestion.ClassInteractive)
	a.OnDemand(ci)
	a.Permit(arbPacket)
	spend := quiccongestion.ByteCount(a.Stats().Deficit[quiccongestion.ClassControl] - 500)
	a.OnSent([quiccongestion.NumSendClasses]quiccongestion.ByteCount{quiccongestion.ClassControl: spend})
	a.OnDemand(demandOf(quiccongestion.ClassInteractive)) // 500 held
	a.OnSent([quiccongestion.NumSendClasses]quiccongestion.ByteCount{quiccongestion.ClassControl: 1000})
	a.OnDemand(ci)
	if got := a.Stats().Deficit[quiccongestion.ClassControl]; got > -500 {
		t.Fatalf("CONTROL deficit %d after a 1000-byte idle charge against 500 held, want -500 or less", got)
	}
}

// An idle class's retransmission is paid from the credit it held for the
// round, so when the round ends before it returns, it comes back without a
// debt it never ran up.
func TestClassArbiterIdleChargeWithinHeldCreditLeavesNoDebtAcrossRounds(t *testing.T) {
	a := NewClassArbiter(DefaultClassWeights, arbPacket)
	cb := demandOf(quiccongestion.ClassControl, quiccongestion.ClassBulk)
	bulk := demandOf(quiccongestion.ClassBulk)
	a.OnDemand(cb)
	a.Permit(arbPacket)
	spend := quiccongestion.ByteCount(a.Stats().Deficit[quiccongestion.ClassControl] - 500)
	a.OnSent([quiccongestion.NumSendClasses]quiccongestion.ByteCount{quiccongestion.ClassControl: spend})
	a.OnDemand(bulk) // 500 held
	a.OnSent([quiccongestion.NumSendClasses]quiccongestion.ByteCount{quiccongestion.ClassControl: 300})
	// BULK runs the round out and replenishment starts another.
	for range 20 {
		a.OnSent(simBuilder(a.Permit(arbPacket), bulk, 0))
	}
	a.OnDemand(cb)
	if got := a.Stats().Deficit[quiccongestion.ClassControl]; got < 0 {
		t.Fatalf("CONTROL returned with a %d-byte debt it never ran up", -got)
	}
	if p := a.Permit(arbPacket); p.Turn != quiccongestion.ClassControl {
		t.Fatalf("turn %v: the returning ACK waits behind BULK", p.Turn)
	}
}

// With no class demand the turn is EXEMPT with no fill: a run of ACK-only
// packets charges no class, and the next CONTROL frame gets the turn at once.
func TestClassArbiterAckOnlyPacketsChargeNoClass(t *testing.T) {
	a := NewClassArbiter(DefaultClassWeights, arbPacket)
	a.OnDemand(quiccongestion.Demand{})
	for range 1000 {
		p := a.Permit(arbPacket)
		if p.Turn != quiccongestion.ClassExempt || p.Fill != ([quiccongestion.NumSendClasses]quiccongestion.ByteCount{}) {
			t.Fatalf("permit %+v with no demand, want an EXEMPT turn and no fill", p)
		}
		// The fork charges overhead to the turn: here, to EXEMPT.
		a.OnSent([quiccongestion.NumSendClasses]quiccongestion.ByteCount{quiccongestion.ClassExempt: 60})
	}
	cb := demandOf(quiccongestion.ClassControl, quiccongestion.ClassBulk)
	a.OnDemand(cb)
	if p := a.Permit(arbPacket); p.Turn != quiccongestion.ClassControl {
		t.Fatalf("turn %v: ACK-only traffic left CONTROL in debt", p.Turn)
	}
}

// A turn that builds nothing because its work does not fit the permit is
// passed over: the next permit goes to another class with demand; once
// every class with demand is passed over, admission is refused until the
// earliest mark expires -- no spin, and no wait for an unrelated event --
// after which the class gets its turn again; a sent packet clears the marks.
func TestClassArbiterPassesOverAStuckTurn(t *testing.T) {
	a := NewClassArbiter(DefaultClassWeights, arbPacket)
	now := monotime.Now()
	a.OnDemand(demandOf(quiccongestion.ClassInteractive, quiccongestion.ClassBulk))
	p := a.PermitAt(now, arbPacket)
	if p.Turn != quiccongestion.ClassInteractive {
		t.Fatalf("turn %v, want INTERACTIVE", p.Turn)
	}
	if !a.OnEmptyBuild(now, p.Turn) {
		t.Fatal("a first empty build allows no other decision")
	}
	if p := a.PermitAt(now, arbPacket); p.Turn != quiccongestion.ClassBulk || p.Fill[quiccongestion.ClassInteractive] != 0 {
		t.Fatalf("permit %+v, want BULK with no INTERACTIVE fill", p)
	}
	a.OnEmptyBuild(now, quiccongestion.ClassBulk)
	p = a.PermitAt(now, arbPacket)
	if p.OK || p.RetryAt.IsZero() || !now.Before(p.RetryAt) {
		t.Fatalf("permit %+v with every class passed over, want a refusal with a retry time", p)
	}
	if a.OnEmptyBuild(now, quiccongestion.ClassExempt) {
		t.Fatal("an EXEMPT empty build asks for another decision: the connection would spin")
	}
	if p := a.PermitAt(p.RetryAt, arbPacket); !p.OK || p.Turn != quiccongestion.ClassInteractive {
		t.Fatalf("permit %+v at the retry time, want INTERACTIVE's turn back", p)
	}
	a.OnEmptyBuild(now, quiccongestion.ClassInteractive)
	a.OnSent([quiccongestion.NumSendClasses]quiccongestion.ByteCount{quiccongestion.ClassExempt: 40})
	if p := a.PermitAt(now, arbPacket); p.Turn != quiccongestion.ClassInteractive {
		t.Fatalf("turn %v after a packet was sent, want INTERACTIVE back", p.Turn)
	}
}

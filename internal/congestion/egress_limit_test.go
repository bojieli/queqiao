package congestion

import (
	"math"
	"testing"
	"time"

	"github.com/apernet/quic-go/monotime"
)

// egressRig drives a group's members' counters directly, a window at a time.
type egressRig struct {
	g       *EgressLimit
	now     monotime.Time
	members []*EgressMember
}

type egressLoad struct {
	rate, want float64 // bytes per second sent, and the law's pacing rate
	loss       float64 // of the decided packets
	idle       bool    // application-limited
}

func newEgressRig(cfg EgressConfig, members int) *egressRig {
	r := &egressRig{g: NewEgressLimit(cfg), now: monotime.Time(time.Second)}
	for range members {
		r.members = append(r.members, r.g.Join())
	}
	return r
}

// window advances one egress window with the given load per member.
func (r *egressRig) window(loads ...egressLoad) {
	// Every member's counters move before any reports, as in a window where
	// events interleave; the tick then sees the whole window.
	for i, l := range loads {
		m := r.members[i]
		m.sent.Add(uint64(l.rate * egressWindow.Seconds()))
		decided := uint64(l.rate * egressWindow.Seconds() / 1200)
		m.decided.Add(decided)
		m.lost.Add(uint64(float64(decided) * l.loss))
		m.want.Store(int64(l.want))
		if l.idle {
			m.unused.Add(int64(egressWindow) * 9 / 10)
		}
	}
	for i, l := range loads {
		m := r.members[i]
		m.report(r.now, int64(l.want), 50*time.Millisecond, time.Duration(m.unused.Load()))
	}
	r.now = r.now.Add(egressWindow)
}

func (r *egressRig) run(windows int, loads ...egressLoad) {
	for range windows {
		r.window(loads...)
	}
}

const mbit = 125_000.0

func TestEgressLimitDividesMaxMin(t *testing.T) {
	k := 1000 * mbit
	r := newEgressRig(EgressConfig{Configured: uint64(k)}, 3)
	loaded := egressLoad{rate: 450 * mbit, want: 900 * mbit}
	light := egressLoad{rate: 5 * mbit, want: 900 * mbit, idle: true}
	r.run(6, loaded, loaded, light)
	if got := r.members[2].Share(); got != 0 {
		t.Errorf("the light member is capped at %.0f Mbit/s, want uncapped", float64(got)/mbit)
	}
	want := (egressHeadroom*k - 1.25*5*mbit - 2*1200/0.05) / 2
	for i := range 2 {
		got := float64(r.members[i].Share())
		if got < 0.97*want || got > 1.03*want {
			t.Errorf("loaded member %d capped at %.0f Mbit/s, want %.0f", i, got/mbit, want/mbit)
		}
	}
}

func TestEgressLimitHasNoEffectUnderTheCeiling(t *testing.T) {
	r := newEgressRig(EgressConfig{Configured: uint64(1000 * mbit)}, 3)
	l := egressLoad{rate: 300 * mbit, want: 320 * mbit}
	r.run(10, l, l, l)
	for i, m := range r.members {
		if m.Share() != 0 {
			t.Errorf("member %d capped at %d under the ceiling", i, m.Share())
		}
	}
}

func TestEgressLimitSingleMemberOnlyUnderAConfiguredCeiling(t *testing.T) {
	l := egressLoad{rate: 900 * mbit, want: 2000 * mbit}
	for _, tc := range []struct {
		name string
		cfg  EgressConfig
		cap  bool
	}{
		{"link speed", EgressConfig{LinkSpeed: uint64(1000 * mbit)}, false},
		{"configured", EgressConfig{Configured: uint64(1000 * mbit)}, true},
		{"nothing known", EgressConfig{}, false},
	} {
		r := newEgressRig(tc.cfg, 1)
		r.run(10, l)
		if got := r.members[0].Share() != 0; got != tc.cap {
			t.Errorf("%s: capped=%v, want %v", tc.name, got, tc.cap)
		}
	}
}

func TestEgressLimitLinkSpeedIsTheCeilingWithAMargin(t *testing.T) {
	r := newEgressRig(EgressConfig{LinkSpeed: uint64(1000 * mbit)}, 2)
	l := egressLoad{rate: 500 * mbit, want: 2000 * mbit}
	r.run(6, l, l)
	k, src := r.g.Ceiling()
	if src != EgressSourceLink || float64(k) != egressLinkMargin*1000*mbit {
		t.Fatalf("ceiling %.0f Mbit/s from source %d, want %.0f from the link", float64(k)/mbit, src, egressLinkMargin*1000)
	}
	want := egressHeadroom * egressLinkMargin * 1000 * mbit / 2
	if got := float64(r.members[0].Share()); got < 0.99*want || got > 1.01*want {
		t.Errorf("share %.0f Mbit/s, want %.0f", got/mbit, want/mbit)
	}
}

// offered returns the loads of n members each sending base bytes per second
// with a slow wobble of their own, as members' probe cycles do, whose loss is
// lossOf of the aggregate they send. The wobbles differ, so the aggregate
// moves.
func (r *egressRig) offered(windows, n int, base float64, lossOf func(aggregate float64) float64) {
	for w := range windows {
		loads := make([]egressLoad, n)
		var aggregate float64
		for i := range n {
			phase := float64(w)/(7+3*float64(i)) + float64(i)
			loads[i] = egressLoad{rate: base * (1 + 0.15*math.Sin(2*math.Pi*phase)), want: 2 * base}
			aggregate += loads[i].rate
		}
		for i := range loads {
			loads[i].loss = lossOf(aggregate)
		}
		r.window(loads...)
	}
}

func TestEgressLimitIgnoresLossThatDoesNotRiseWithRate(t *testing.T) {
	// Nothing known: a gateway on a host whose link speed is unreadable,
	// whose members all see the same random loss whatever they send.
	r := newEgressRig(EgressConfig{}, 2)
	r.offered(600, 2, 450*mbit, func(float64) float64 { return 0.30 })
	if k, src := r.g.Ceiling(); k != 0 {
		t.Fatalf("random loss alone learned a ceiling of %.0f Mbit/s (source %d)", float64(k)/mbit, src)
	}
	for i, m := range r.members {
		if m.Share() != 0 {
			t.Errorf("member %d capped at %.0f Mbit/s by random loss", i, float64(m.Share())/mbit)
		}
	}
}

func TestEgressLimitLearnsAPolicedEgress(t *testing.T) {
	capacity := 1000 * mbit
	policer := func(a float64) float64 { return math.Max(0, 1-capacity/a) }
	r := newEgressRig(EgressConfig{}, 2)
	// Two members offering 1.3 times the egress: the loss rises with what
	// they send, and the group learns where it stops.
	r.offered(400, 2, 650*mbit, policer)
	k, src := r.g.Ceiling()
	if src != EgressSourceLearned {
		t.Fatalf("no ceiling learned from a policed egress (source %d)", src)
	}
	if float64(k) < 0.85*capacity || float64(k) > 1.15*capacity {
		t.Errorf("learned %.0f Mbit/s, want about %.0f", float64(k)/mbit, capacity/mbit)
	}
	// Once members are held to their shares nothing is lost, and what they
	// send stays under the capacity.
	var sum float64
	for _, m := range r.members {
		sum += float64(m.Share())
	}
	if sum > 1.15*capacity {
		t.Errorf("shares sum to %.0f Mbit/s over a %.0f Mbit/s egress", sum/mbit, capacity/mbit)
	}
}

func TestEgressLimitOneActiveMemberLearnsNothing(t *testing.T) {
	capacity := 1000 * mbit
	r := newEgressRig(EgressConfig{}, 2)
	for w := range 400 {
		a := 1300 * mbit * (1 + 0.15*math.Sin(float64(w)/5))
		r.window(egressLoad{rate: a, want: 2 * a, loss: math.Max(0, 1-capacity/a)}, egressLoad{})
	}
	if k, _ := r.g.Ceiling(); k != 0 {
		t.Fatalf("a single active member learned a ceiling of %.0f Mbit/s", float64(k)/mbit)
	}
}

func TestEgressLimitProbesAStaleCeilingUp(t *testing.T) {
	capacity := 600 * mbit
	r := newEgressRig(EgressConfig{LinkSpeed: uint64(1000 * mbit)}, 2)
	r.offered(400, 2, 500*mbit, func(a float64) float64 { return math.Max(0, 1-capacity/a) })
	cut, src := r.g.Ceiling()
	if src != EgressSourceLearned || float64(cut) >= egressLinkMargin*1000*mbit {
		t.Fatalf("not cut: %.0f Mbit/s from source %d", float64(cut)/mbit, src)
	}
	// The limiter has gone: members held by their shares lose nothing.
	r.offered(int(egressHold/egressWindow)-25, 2, 300*mbit, func(float64) float64 { return 0 })
	if k, _ := r.g.Ceiling(); k != cut {
		t.Errorf("probed during the hold: %.0f -> %.0f Mbit/s", float64(cut)/mbit, float64(k)/mbit)
	}
	r.offered(40+int(time.Second/egressWindow)*3, 2, 300*mbit, func(float64) float64 { return 0 })
	k, _ := r.g.Ceiling()
	if float64(k) < cut2(cut, 3) {
		t.Errorf("after three probe periods the ceiling is %.0f Mbit/s, want at least %.0f", float64(k)/mbit, cut2(cut, 3)/mbit)
	}
	// And all the way back to the link's bound, where it stops being learned.
	r.offered(int(30*time.Second/egressWindow), 2, 300*mbit, func(float64) float64 { return 0 })
	if k, src := r.g.Ceiling(); src != EgressSourceLink {
		t.Errorf("ceiling %.0f Mbit/s from source %d, want the link's", float64(k)/mbit, src)
	}
}

func cut2(cut uint64, probes int) float64 {
	v := float64(cut)
	for range probes {
		v *= egressProbeGain
	}
	return v * 0.98
}

func TestEgressLimitUnreadableLinkSpeedLeavesNoBound(t *testing.T) {
	g := NewEgressLimit(EgressConfig{LinkSpeed: 0})
	if k, src := g.Ceiling(); k != 0 || src != EgressSourceNone {
		t.Fatalf("a group with nothing known has ceiling %d from source %d", k, src)
	}
}

package congestion

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apernet/quic-go/monotime"
)

// EgressLimit is the joint limit of the path authorities of one gateway that
// send out one network interface (docs/DESIGN-EGRESS-JOINT-LIMIT.md §3).
//
// Each authority is correct about its own path and blind to its siblings'.
// Where they share a point that drops without queueing, every one of them
// reads its share of the drop as channel erasure, and together they overdrive
// the point. The group knows a ceiling, divides it max-min among the
// authorities that are sending, and caps each one's pacing rate and window at
// its share. It has no effect while the aggregate is under the ceiling, and
// none on one authority alone unless the ceiling was configured.
//
// The group never calls into a member: members add to atomic counters on
// their own events and read their own share. The window tick runs under the
// group's lock when the first event past the window arrives.
type EgressLimit struct {
	cfg EgressConfig

	mu      sync.Mutex
	members map[*EgressMember]struct{}
	// windowAt is when the current window began; nextTick is the same as
	// nanoseconds, read without the lock by the members' events.
	windowAt monotime.Time
	nextTick atomic.Int64

	// learned is the ceiling learned from the aggregate, zero while none.
	// It falls only through the learn-down rule, and rises only by probing.
	learned float64
	// holdUntil is when probing may resume after a cut, nextProbe the
	// earliest next probe, settleUntil when evidence is judged again.
	holdUntil, nextProbe, settleUntil monotime.Time
	// bound is whether some member was capped in the last window, which is
	// when a probe has something to find.
	binding bool
	// samples is the last egressRing windows' aggregate sent rate and loss,
	// the evidence the group learns from (§3.2). It is emptied by a cut.
	samples [egressRing]egressSample
	nSample int
	nextSmp int
	jitter  func() time.Duration

	// ceiling and source are the effective ceiling in bytes per second and
	// where it came from, published for telemetry.
	ceiling atomic.Uint64
	source  atomic.Uint32
	// cuts counts learn-down events.
	cuts atomic.Uint64
}

// EgressConfig is a group's configuration.
type EgressConfig struct {
	// Configured is the operator's ceiling in bytes per second out of the
	// interface; zero when none. It is exact and is enforced for one
	// active member too.
	Configured uint64
	// LinkSpeed is the interface's speed in bytes per second, zero when it
	// cannot be read. The ceiling takes egressLinkMargin of it.
	LinkSpeed uint64
	// Jitter, if set, spreads the probe wait.
	Jitter func() time.Duration
}

// Egress ceiling sources, for telemetry.
const (
	EgressSourceNone uint32 = iota
	EgressSourceConfigured
	EgressSourceLink
	EgressSourceLearned
)

const (
	// egressWindow is the group's tick.
	egressWindow = 100 * time.Millisecond
	// egressHeadroom is the share of the ceiling that is divided, leaving the
	// rest to bursts of members that are below their share.
	egressHeadroom = 0.97
	// egressLinkMargin is the share of the link speed that is the ceiling.
	egressLinkMargin = 0.95
	// egressActiveRate is the sent rate, in bytes per second, from which a
	// member is active (1 Mbit/s).
	egressActiveRate = 125_000
	// egressMinDecided is the fewest decided packets a window needs.
	egressMinDecided = 500
	// egressProbeGain is the ceiling's growth per probe, and egressProbeEvery
	// the time between probes.
	egressProbeGain  = 1.05
	egressProbeEvery = time.Second
	// egressHold is the wait after a cut before probing, as
	// inflightProbeWait.
	egressHold = 2 * time.Second
	// egressSettleWindows is how many windows (and round trips, if longer)
	// after a cut are not judged: what is reported lost then was sent before
	// the cut.
	egressSettleWindows = 3
	// egressAppLimited is the share of a window a member must have had
	// unused opportunities in to be application-limited.
	egressAppLimited = 0.25
	// egressRing is how many windows the group's regression looks at, and
	// egressMinWindows the fewest it needs. egressMinSpread is the least
	// relative spread of the aggregate rate across them that makes them an
	// experiment at all.
	egressRing       = 40
	egressMinWindows = 20
	egressMinSpread  = 0.03
	// egressSlope and egressSlopeSigmas flag loss that rises with the
	// aggregate rate: at least 0.1 (ten percent more offered, one point more
	// lost) and four standard errors of the fit, the margin that a variance
	// estimated from tens of windows needs.
	egressSlope       = 0.1
	egressSlopeSigmas = 4.0
	// egressMinCeiling is the lowest ceiling the group learns.
	egressMinCeiling = 1_000_000
)

// NewEgressLimit returns a group.
func NewEgressLimit(cfg EgressConfig) *EgressLimit {
	g := &EgressLimit{cfg: cfg, members: make(map[*EgressMember]struct{}), jitter: cfg.Jitter}
	g.publish()
	return g
}

// EgressMember is a controller's seat in a group.
type EgressMember struct {
	g *EgressLimit

	// Published by the controller on its own events.
	want    atomic.Int64  // pacing rate before the share, bytes per second
	rtt     atomic.Int64  // minimum round trip, ns
	sent    atomic.Uint64 // wire bytes debited, cumulative
	decided atomic.Uint64 // sample packets with a fate, cumulative
	lost    atomic.Uint64
	unused  atomic.Int64 // application-limited time, cumulative ns

	// share is the cap in bytes per second, zero while uncapped.
	share atomic.Int64

	// The cumulative values at the last tick, owned by the tick.
	lastSent, lastDecided, lastLost uint64
	lastUnused                      int64
}

// Join returns the seat of c in g. A controller sits in one group.
func (g *EgressLimit) Join() *EgressMember {
	m := &EgressMember{g: g}
	g.mu.Lock()
	g.members[m] = struct{}{}
	g.mu.Unlock()
	return m
}

// Leave removes the seat.
func (m *EgressMember) Leave() {
	m.g.mu.Lock()
	delete(m.g.members, m)
	m.g.mu.Unlock()
	m.share.Store(0)
}

// Share is the member's cap in bytes per second, zero while uncapped.
func (m *EgressMember) Share() int64 { return m.share.Load() }

// Ceiling is the group's ceiling in bytes per second and its source.
func (g *EgressLimit) Ceiling() (uint64, uint32) { return g.ceiling.Load(), g.source.Load() }

// Cuts is how many times the group has learned its ceiling down.
func (g *EgressLimit) Cuts() uint64 { return g.cuts.Load() }

// ceilingNow is the effective ceiling: the least of the sources that are
// known, zero when none is.
func (g *EgressLimit) ceilingNow() (float64, uint32) {
	k, src := 0.0, EgressSourceNone
	take := func(v float64, s uint32) {
		if v > 0 && (k == 0 || v < k) {
			k, src = v, s
		}
	}
	take(float64(g.cfg.Configured), EgressSourceConfigured)
	take(egressLinkMargin*float64(g.cfg.LinkSpeed), EgressSourceLink)
	take(g.learned, EgressSourceLearned)
	return k, src
}

func (g *EgressLimit) publish() {
	k, src := g.ceilingNow()
	g.ceiling.Store(uint64(k))
	g.source.Store(src)
}

// report is called by a member's controller after an acknowledgement event.
func (m *EgressMember) report(now monotime.Time, want int64, rtt time.Duration, unused time.Duration) {
	m.want.Store(want)
	m.rtt.Store(int64(rtt))
	m.unused.Store(int64(unused))
	if int64(now) >= m.g.nextTick.Load() {
		m.g.tick(now)
	}
}

type egressSnap struct {
	m                  *EgressMember
	rate, want, demand float64
	decided, lost      uint64
	appLimited, active bool
	rtt                time.Duration
}

type egressSample struct{ rate, loss, decided float64 }

// regress says whether the loss of the windows in the ring rises with the
// aggregate rate sent in them, by least squares of loss on relative rate: the
// fit's slope must be at least egressSlope and egressSlopeSigmas of its own
// standard error, and the windows must have spread over at least
// egressMinSpread of their mean rate, or they are no experiment. It returns
// the mean rate and the loss over the ring.
func (g *EgressLimit) regress() (correlated bool, rate, loss float64) {
	n := g.nSample
	if n < egressMinWindows {
		return false, 0, 0
	}
	var sumA, sumL, sumD float64
	for i := range n {
		w := g.samples[i]
		sumA += w.rate
		sumL += w.loss * w.decided
		sumD += w.decided
	}
	rate, loss = sumA/float64(n), sumL/sumD
	if rate <= 0 {
		return false, rate, loss
	}
	var meanL float64
	for i := range n {
		meanL += g.samples[i].loss
	}
	meanL /= float64(n)
	var sxx, sxy, syy float64
	for i := range n {
		x, y := g.samples[i].rate/rate-1, g.samples[i].loss-meanL
		sxx += x * x
		sxy += x * y
		syy += y * y
	}
	if sxx <= 0 || math.Sqrt(sxx/float64(n)) < egressMinSpread {
		return false, rate, loss
	}
	slope := sxy / sxx
	resid := math.Max(syy-slope*sxy, 0) / float64(n-2)
	// The loss of a window is a count, so its sampling alone has a variance.
	resid = math.Max(resid, loss*(1-loss)/(sumD/float64(n)))
	se := math.Sqrt(resid / sxx)
	return slope >= egressSlope && slope >= egressSlopeSigmas*se, rate, loss
}

func (g *EgressLimit) tick(now monotime.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.windowAt.IsZero() {
		g.windowAt = now
		g.nextTick.Store(int64(now.Add(egressWindow)))
		// Take the baselines.
		for m := range g.members {
			m.lastSent, m.lastDecided, m.lastLost, m.lastUnused = m.sent.Load(), m.decided.Load(), m.lost.Load(), m.unused.Load()
		}
		return
	}
	dt := now.Sub(g.windowAt)
	if dt < egressWindow {
		return
	}
	g.windowAt = now
	g.nextTick.Store(int64(now.Add(egressWindow)))
	secs := dt.Seconds()

	snaps := make([]egressSnap, 0, len(g.members))
	var decided, lost uint64
	var maxRTT time.Duration
	active := 0
	for m := range g.members {
		s := egressSnap{m: m}
		cs, cd, cl, cu := m.sent.Load(), m.decided.Load(), m.lost.Load(), m.unused.Load()
		s.rate = float64(cs-m.lastSent) / secs
		s.decided, s.lost = cd-m.lastDecided, cl-m.lastLost
		s.appLimited = float64(cu-m.lastUnused) > egressAppLimited*float64(dt)
		m.lastSent, m.lastDecided, m.lastLost, m.lastUnused = cs, cd, cl, cu
		s.want = float64(m.want.Load())
		s.rtt = time.Duration(m.rtt.Load())
		s.active = s.rate >= egressActiveRate
		if s.active {
			active++
			maxRTT = max(maxRTT, s.rtt)
		}
		decided += s.decided
		lost += s.lost
		snaps = append(snaps, s)
	}
	var aggregate float64
	for _, s := range snaps {
		aggregate += s.rate
	}

	// What the group learns of its aggregate (§3.2): whether the loss of the
	// windows it has seen rises with the rate the members sent in them.
	settle := time.Duration(egressSettleWindows) * egressWindow
	if r := egressSettleWindows * maxRTT; r > settle {
		settle = r
	}
	if active >= 2 && decided >= egressMinDecided && !now.Before(g.settleUntil) {
		g.samples[g.nextSmp] = egressSample{rate: aggregate, loss: float64(lost) / float64(decided), decided: float64(decided)}
		g.nextSmp = (g.nextSmp + 1) % egressRing
		g.nSample = min(g.nSample+1, egressRing)
	}
	correlated, rate, loss := g.regress()
	switch {
	case correlated:
		// The aggregate delivered in those windows is what the egress
		// passed. It is the ceiling of the wire rate when the channel past
		// it erases nothing, and a lower bound of it when it does, which a
		// probe then finds.
		fit := rate * (1 - loss)
		if k, _ := g.ceilingNow(); fit < k || k == 0 {
			g.learned = math.Max(fit, egressMinCeiling)
			g.cuts.Add(1)
			g.holdUntil = now.Add(egressHold)
			if g.jitter != nil {
				g.holdUntil = g.holdUntil.Add(g.jitter())
			}
			g.nextProbe = g.holdUntil
		}
		g.settleUntil = now.Add(settle)
		g.nSample, g.nextSmp = 0, 0
	case g.learned > 0 && g.binding && !now.Before(g.holdUntil) && !now.Before(g.nextProbe):
		g.learned *= egressProbeGain
		g.nextProbe = now.Add(egressProbeEvery)
		// A probe that reaches the other sources' bound is no longer a
		// learned ceiling.
		bound := math.Inf(1)
		if g.cfg.Configured > 0 {
			bound = math.Min(bound, float64(g.cfg.Configured))
		}
		if g.cfg.LinkSpeed > 0 {
			bound = math.Min(bound, egressLinkMargin*float64(g.cfg.LinkSpeed))
		}
		if g.learned >= bound {
			g.learned = 0
		}
	}
	g.publish()

	// The division (§3.3). The learned and link ceilings need two active
	// members; a configured one binds one as well.
	k, _ := g.ceilingNow()
	enforce := k > 0 && (g.cfg.Configured > 0 || active >= 2)
	g.binding = false
	if !enforce {
		for _, s := range snaps {
			s.m.share.Store(0)
		}
		return
	}
	var demands []*egressSnap
	for i := range snaps {
		s := &snaps[i]
		if !s.active {
			continue
		}
		s.demand = s.want
		if s.appLimited {
			// It demands its use, not its line rate: a member that fetches a
			// few hundred kilobytes now and then reserves nothing it does
			// not use.
			packets := 0.0
			if s.rtt > 0 {
				packets = 2 * 1200 / s.rtt.Seconds()
			}
			s.demand = math.Min(s.want, 1.25*s.rate+packets)
		}
		if s.demand <= 0 {
			s.demand = s.rate
		}
		demands = append(demands, s)
	}
	sort.Slice(demands, func(i, j int) bool { return demands[i].demand < demands[j].demand })
	remaining := egressHeadroom * k
	level := math.Inf(1)
	for i, s := range demands {
		fair := remaining / float64(len(demands)-i)
		if s.demand <= fair {
			remaining -= s.demand
			continue
		}
		level = fair
		break
	}
	for _, s := range snaps {
		s.m.share.Store(0)
	}
	if !math.IsInf(level, 1) {
		for _, s := range demands {
			if s.demand > level {
				s.m.share.Store(int64(level))
				g.binding = true
			}
		}
	}
}

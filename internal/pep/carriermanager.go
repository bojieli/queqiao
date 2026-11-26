package pep

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apernet/quic-go"
	quiccongestion "github.com/apernet/quic-go/congestion"
	"github.com/apernet/quic-go/monotime"

	wancongestion "github.com/bojieli/queqiao/internal/congestion"
	"github.com/bojieli/queqiao/internal/metrics"
	"github.com/bojieli/queqiao/internal/protocol"
)

// The carrier manager is stage 8 of the path-authority design
// (docs/DESIGN-PATH-AUTHORITY.md §10, §14 recovery=carrier). On a client it
// is the single owner of the pooled QUIC carrier, of which flows are bound to
// it, and of its replacement:
//
//   - A carrier is judged only on its own transport evidence, the fork's
//     progress export (§10.2): k_s probe timeouts without an acknowledged
//     eliciting packet make it SUSPECT, which takes no new flows, makes its
//     attachment PROBING and dials one standby; nothing moves. A verified
//     standby is promoted once k_p expiries have passed with the progress
//     epoch unchanged since SUSPECT. Progress first returns the carrier to
//     ACTIVE; after promotion nothing returns: the old carrier is DRAINING
//     until no flow is bound to it.
//   - A flow whose stream failed is unbound, and is rebound by one JOIN on the
//     eligible carrier, the same one if it is live (§10.3, §10.4). Missing
//     flow credit on a live stream has no carrier remedy at all.
//   - Every unbound flow, idle or not, carries a binding deadline derived from
//     the gateway's grace less the detection skew S, and a latest handoff
//     start that leaves the handoff budget J for the JOIN to arrive (§10.3
//     step 4).
//   - Every dial round is charged to one path-level attempt history whose
//     cooldown doubles, so a flapping carrier cannot produce a dial storm.
//   - The adaptive keep-alive schedule (fork change 11) is set on each carrier
//     while it has open flows.
//
// Flows never dial QUIC for themselves under this mode; the per-flow rescue
// race and the stall watchdog's lane demotion are not reached.

// CarrierRecoveryConfig holds the carrier manager's thresholds. Every value
// is provisional: the design calibrates k_s, k_p and the keep-alive floor from
// the carrier-shadow capture (§10.2, §16 Q4) and J from measured handoff
// times (§10.3 step 4), so each is configurable and zero selects the design's
// provisional default.
type CarrierRecoveryConfig struct {
	// SuspectPTOs is k_s: probe timeouts without transport progress before a
	// carrier is SUSPECT.
	SuspectPTOs uint32
	// ReplacePTOs is k_p: probe timeouts, the progress epoch unchanged since
	// SUSPECT, before a verified standby is promoted.
	ReplacePTOs uint32
	// DetectionSkew is S: how much later than the gateway the client may
	// observe the loss of a binding (main budgets 15 s of the 45 s grace for
	// it, mpflow.go laneReplacementWait).
	DetectionSkew time.Duration
	// HandoffBudget is J: the whole interval from starting a JOIN, on QUIC or
	// by TCP handoff, to the gateway parsing it.
	HandoffBudget time.Duration
	// Interval is how often each carrier's progress is sampled. Tests shorten
	// it.
	Interval time.Duration

	// grace is the gateway's replacement grace G; zero is
	// laneReplacementWait. Tests shorten it.
	grace time.Duration
	// clock reads the time binding deadlines and the attempt history are
	// judged by; nil is time.Now. Tests inject one.
	clock func() time.Time
}

const (
	// Provisional, as the shadow observer's labels (carriershadow.go) until
	// the stage-8 calibration capture (#196) sets them.
	defaultCarrierSuspectPTOs = defaultCarrierShadowSuspectPTOs
	defaultCarrierReplacePTOs = defaultCarrierShadowReplacePTOs
	defaultCarrierInterval    = 50 * time.Millisecond
	// defaultDetectionSkew is main's own split of the grace (§10.3 step 4).
	defaultDetectionSkew = laneDeadPathDetection
	// defaultHandoffBudget is a provisional J: a QUIC stream open and JOIN
	// on a live carrier takes a round trip; a TCP handoff a dial, TCP and
	// TLS handshakes and the frame's transit. Five seconds covers several
	// round trips on the slowest measured path.
	defaultHandoffBudget = 5 * time.Second
	// standbyWarmFor is how long a standby is kept after its suspect carrier
	// recovered before promotion (§10.2).
	standbyWarmFor = 30 * time.Second
	// keepAliveFloorMin and keepAliveFloorRTTs give the keep-alive floor
	// max(4×SRTT, 500 ms) while a carrier has open flows (§10.2).
	keepAliveFloorMin  = 500 * time.Millisecond
	keepAliveFloorRTTs = 4
	// carrierRoundCooldownMax caps the attempt history's doubling cooldown,
	// as today's per-flow recovery backoff is capped.
	carrierRoundCooldownMax = 15 * time.Second
	// joinTarget is T of §10.3 step 3: the time a JOIN queue should drain in.
	joinTarget = 3 * time.Second
	// minJoinConcurrency is the floor of §10.3 step 3.
	minJoinConcurrency = 16
)

// ValidateCarrierRecovery reports whether a set of carrier-recovery
// thresholds is usable. Zero selects a default.
func ValidateCarrierRecovery(cfg CarrierRecoveryConfig) error {
	return cfg.resolve()
}

func (c *CarrierRecoveryConfig) resolve() error {
	if c.SuspectPTOs == 0 {
		c.SuspectPTOs = defaultCarrierSuspectPTOs
	}
	if c.ReplacePTOs == 0 {
		c.ReplacePTOs = max(defaultCarrierReplacePTOs, c.SuspectPTOs)
	}
	if c.SuspectPTOs > maxCarrierShadowPTOs || c.ReplacePTOs > maxCarrierShadowPTOs {
		return fmt.Errorf("carrier recovery thresholds must be at most %d probe timeouts", maxCarrierShadowPTOs)
	}
	if c.ReplacePTOs < c.SuspectPTOs {
		return fmt.Errorf("carrier recovery k_p (%d) must not be below k_s (%d)", c.ReplacePTOs, c.SuspectPTOs)
	}
	if c.DetectionSkew == 0 {
		c.DetectionSkew = defaultDetectionSkew
	}
	if c.HandoffBudget == 0 {
		c.HandoffBudget = defaultHandoffBudget
	}
	if c.Interval <= 0 {
		c.Interval = defaultCarrierInterval
	}
	if c.grace <= 0 {
		c.grace = laneReplacementWait
	}
	if c.DetectionSkew < 0 || c.HandoffBudget < 0 {
		return errors.New("carrier recovery skew and handoff budget must not be negative")
	}
	if c.grace-c.DetectionSkew-c.HandoffBudget <= 0 {
		return fmt.Errorf("carrier recovery skew (%v) and handoff budget (%v) leave no time inside the gateway's %v grace", c.DetectionSkew, c.HandoffBudget, c.grace)
	}
	return nil
}

func (c *CarrierRecoveryConfig) now() time.Time {
	if c.clock != nil {
		return c.clock()
	}
	return time.Now()
}

// bindingDeadlines gives the two deadlines of a binding whose loss the
// client observed at observed (§10.3 step 4): the JOIN must reach the gateway
// by deadline, observed + G − S; a JOIN, on either substrate, must start by
// latestStart, deadline − J.
func (c *CarrierRecoveryConfig) bindingDeadlines(observed time.Time) (latestStart, deadline time.Time) {
	deadline = observed.Add(c.grace - c.DetectionSkew)
	return deadline.Add(-c.HandoffBudget), deadline
}

// carrierState is a carrier's state in the client's carrier manager (§10.2).
type carrierState uint32

const (
	carrierActive carrierState = iota + 1
	carrierSuspect
	// carrierDraining is irreversible: a standby was promoted over it.
	carrierDraining
	carrierDead
	carrierClosed
)

func (s carrierState) String() string {
	switch s {
	case carrierActive:
		return "active"
	case carrierSuspect:
		return "suspect"
	case carrierDraining:
		return "draining"
	case carrierDead:
		return "dead"
	case carrierClosed:
		return "closed"
	}
	return "unknown"
}

func (s carrierState) terminal() bool { return s == carrierDead || s == carrierClosed }

// carrierEvidence is one reading of a carrier's transport.
type carrierEvidence struct {
	Epoch uint64
	PTOs  uint32
	// Ended is why the connection is gone, nil while it lives.
	Ended error
}

// carrierFSM is the §10.2 state machine of one carrier. It is pure: no clock,
// no I/O, so every transition is tested deterministically.
type carrierFSM struct {
	suspectPTOs, replacePTOs uint32
	state                    carrierState
	started                  bool
	epoch                    uint64
	// suspectEpoch is the progress epoch when the carrier became SUSPECT.
	suspectEpoch uint64
}

func newCarrierFSM(ks, kp uint32) carrierFSM {
	return carrierFSM{suspectPTOs: ks, replacePTOs: kp, state: carrierActive}
}

// step folds one reading in. promote says a standby, if verified, should be
// promoted over this carrier now.
func (f *carrierFSM) step(ev carrierEvidence) (from, to carrierState, promote bool) {
	from = f.state
	if f.state.terminal() {
		return from, f.state, false
	}
	if ev.Ended != nil {
		if carrierEndState(ev.Ended) == metrics.CarrierShadowClosed {
			f.state = carrierClosed
		} else {
			f.state = carrierDead
		}
		return from, f.state, false
	}
	if !f.started {
		f.started, f.epoch = true, ev.Epoch
	}
	if ev.Epoch > f.epoch {
		f.epoch = ev.Epoch
		if f.state == carrierSuspect {
			// Progress before promotion: back to ACTIVE.
			f.state = carrierActive
		}
	}
	if f.state == carrierDraining {
		// Nothing returns after promotion, and nothing else moves it.
		return from, f.state, false
	}
	if f.state == carrierActive && ev.PTOs >= f.suspectPTOs {
		f.state, f.suspectEpoch = carrierSuspect, f.epoch
	}
	if f.state == carrierSuspect && ev.PTOs >= f.replacePTOs && f.epoch == f.suspectEpoch {
		promote = true
	}
	return from, f.state, promote
}

// drain marks the carrier DRAINING: a standby was promoted over it.
func (f *carrierFSM) drain() {
	if !f.state.terminal() {
		f.state = carrierDraining
	}
}

// attemptHistory is the path-level record every dial round is charged to
// (§10.2): the cooldown doubles with each charged round, replacement rounds
// are capped at today's per-flow cap, and only a sustained healthy dwell
// resets it.
type attemptHistory struct {
	mu           sync.Mutex
	charged      int
	cooldown     time.Duration
	next         time.Time
	healthySince time.Time
}

// admit charges one round at now if the cooldown has passed and, for a
// replacement round, the cap is not spent. Otherwise it returns when to ask
// again; a zero time means not until the history resets.
func (h *attemptHistory) admit(now time.Time, replacement bool) (bool, time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if replacement && h.charged >= maxLaneRecoveryAttempts {
		return false, time.Time{}
	}
	if now.Before(h.next) {
		return false, h.next
	}
	h.charged++
	if h.cooldown == 0 {
		h.cooldown = time.Second
	} else {
		h.cooldown = min(2*h.cooldown, carrierRoundCooldownMax)
	}
	h.next = now.Add(h.cooldown)
	h.healthySince = time.Time{}
	return true, time.Time{}
}

// observe resets the history after a sustained healthy dwell.
func (h *attemptHistory) observe(now time.Time, healthy bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !healthy {
		h.healthySince = time.Time{}
		return
	}
	if h.healthySince.IsZero() {
		h.healthySince = now
		return
	}
	if h.charged > 0 && now.Sub(h.healthySince) >= laneRecoveryResetAfter {
		h.charged, h.cooldown, h.next = 0, 0, time.Time{}
	}
}

func (h *attemptHistory) rounds() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.charged
}

// managedCarrier is one pooled connection as the carrier manager sees it.
type managedCarrier struct {
	gen *controlQUICGeneration
	// state mirrors fsm.state for lock-free readers (the pool's acquire
	// path holds quicMu, which the manager takes after its own lock).
	state   atomic.Uint32
	standby bool
	// Guarded by carrierManager.mu.
	fsm       carrierFSM
	verified  bool
	warmUntil time.Time
	keepAlive time.Duration
	// rebinding counts the flows queued for or running a JOIN onto this
	// carrier: a new OPEN does not take a stream slot one of them needs
	// (§10.3 step 2).
	rebinding atomic.Int64
}

func (mc *managedCarrier) current() carrierState { return carrierState(mc.state.Load()) }

// carrierManager is one client's carrier manager. The client has one remote
// gateway address, so one path, except across an uplink change, which is a
// new path whose old carriers are DEAD (§10.2).
type carrierManager struct {
	c   *Client
	cfg CarrierRecoveryConfig
	log *slog.Logger

	mu       sync.Mutex
	carriers map[*controlQUICGeneration]*managedCarrier
	standby  *managedCarrier
	dialing  bool // a standby dial round is running
	running  bool
	// generation is the path's generation, incremented by each promotion;
	// a binding names the carrier it is on, so a late report about a
	// superseded one changes nothing.
	generation uint64
	// wire is the version of the path's carriers, kept while it has flows
	// (§7 one wire version per path).
	wire    byte
	wireSet bool
	// replacing says a break-before-make dial is running.
	replacing atomic.Bool

	history attemptHistory
	changed atomic.Pointer[chan struct{}]
	joins   joinQueue

	// Counters, for tests and the client's log.
	promotions, standbyDials, standbyFailures, replacementDials      atomic.Int64
	sameCarrierJoins, newCarrierJoins, deadlineFailures, tcpHandoffs atomic.Int64
}

func newCarrierManager(c *Client, cfg CarrierRecoveryConfig) *carrierManager {
	m := &carrierManager{c: c, cfg: cfg, log: c.cfg.Logger, carriers: make(map[*controlQUICGeneration]*managedCarrier)}
	ch := make(chan struct{})
	m.changed.Store(&ch)
	m.joins.init()
	return m
}

func (m *carrierManager) now() time.Time { return m.cfg.now() }

// changes is closed at the next change of any carrier's state.
func (m *carrierManager) changes() <-chan struct{} { return *m.changed.Load() }

func (m *carrierManager) broadcast() {
	next := make(chan struct{})
	old := m.changed.Swap(&next)
	close(*old)
}

// watch starts managing a freshly dialled generation. standby says it is a
// standby, which carries no flow until it is promoted.
func (m *carrierManager) watch(g *controlQUICGeneration, standby bool) {
	mc := &managedCarrier{gen: g, standby: standby, fsm: newCarrierFSM(m.cfg.SuspectPTOs, m.cfg.ReplacePTOs)}
	mc.state.Store(uint32(carrierActive))
	g.managed = mc
	m.mu.Lock()
	m.carriers[g] = mc
	if !standby {
		m.noteWireLocked(g)
	}
	if !m.running {
		m.running = true
		go m.run()
	}
	m.mu.Unlock()
}

func (m *carrierManager) noteWireLocked(g *controlQUICGeneration) {
	if wire, ok := protocol.VersionForALPN(g.conn.ConnectionState().TLS.NegotiatedProtocol); ok {
		m.wire, m.wireSet = wire, true
	}
}

// dialWires is the offer for a replacement or standby: while the path has
// flows, exactly the wire its carriers speak, because a flow can only JOIN
// on a connection of its own wire (§7). A path without flows adopts the
// configured offer.
func (m *carrierManager) dialWires() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.wireSet && m.c.quicPoolActive.Load() > 0 {
		return []byte{m.wire}
	}
	return m.c.cfg.WireVersions
}

// accepting reports whether a carrier takes new flows: it is ACTIVE. It
// reads no lock the pool's acquire path holds.
func (m *carrierManager) accepting(g *controlQUICGeneration) bool {
	return g.managed == nil || g.managed.current() == carrierActive
}

// eligible is the carrier a flow should be bound to now: the pooled
// generation, if it is ACTIVE and alive.
func (m *carrierManager) eligible() *controlQUICGeneration {
	c := m.c
	c.quicMu.Lock()
	g := c.quicGeneration
	c.quicMu.Unlock()
	if g == nil || g.conn.Context().Err() != nil || !m.accepting(g) {
		return nil
	}
	return g
}

// superseded reports whether a flow on g must move: g was promoted away from
// or has ended.
func (m *carrierManager) superseded(g *controlQUICGeneration) bool {
	if g == nil {
		return false
	}
	if g.conn.Context().Err() != nil {
		return true
	}
	if mc := g.managed; mc != nil {
		s := mc.current()
		return s == carrierDraining || s.terminal()
	}
	return false
}

// suspect reports whether the pooled carrier is SUSPECT: a flow that lost
// its stream waits for it to recover or be replaced.
func (m *carrierManager) suspect() bool {
	c := m.c
	c.quicMu.Lock()
	g := c.quicGeneration
	c.quicMu.Unlock()
	return g != nil && g.managed != nil && g.managed.current() == carrierSuspect && g.conn.Context().Err() == nil
}

// admitDial is the gate a pooled dial passes before it starts: one dial
// round charged to the attempt history. A replacement round (a flow is
// waiting to be rebound) is also capped. It returns how long to wait
// otherwise, zero meaning until the next change.
func (m *carrierManager) admitDial(replacement bool) (bool, time.Duration) {
	now := m.now()
	ok, next := m.history.admit(now, replacement)
	if ok {
		if replacement {
			m.replacementDials.Add(1)
		}
		return true, 0
	}
	if next.IsZero() {
		return false, 0
	}
	return false, max(next.Sub(now), time.Millisecond)
}

// requestCarrier starts a break-before-make dial for flows that have no
// carrier at all (DEAD with no standby, §10.2). Only one runs at a time; the
// dial itself is the pool's singleflight, gated by the attempt history.
func (m *carrierManager) requestCarrier() {
	if !m.replacing.CompareAndSwap(false, true) {
		return
	}
	serving := m.c.serving.Load()
	if serving == nil || (*serving).Err() != nil {
		m.replacing.Store(false)
		return
	}
	go func() {
		defer m.replacing.Store(false)
		ctx, cancel := context.WithTimeout(withCarrierRebind(*serving), m.c.cfg.DialTimeout)
		defer cancel()
		g, err := m.c.acquireControlQUICGeneration(ctx, m.c.pooledCongestionConfig())
		if err != nil {
			m.log.Debug("carrier replacement unavailable", "error", err)
			return
		}
		g.releaseBorrow()
	}()
}

// run samples every carrier until none is left.
func (m *carrierManager) run() {
	ticker := time.NewTicker(m.cfg.Interval)
	defer ticker.Stop()
	for range ticker.C {
		if !m.tick() {
			return
		}
	}
}

// tick samples each carrier once and acts. It reports whether any carrier
// is left to watch.
func (m *carrierManager) tick() bool {
	c := m.c
	m.mu.Lock()
	// Read under the manager's lock (manager before pool, never the
	// reverse): a promotion cannot move the pool between this read and the
	// loop below.
	c.quicMu.Lock()
	pooled := c.quicGeneration
	c.quicMu.Unlock()
	changed := false
	var closeLater []*controlQUICGeneration
	now := m.now()
	for g, mc := range m.carriers {
		st := g.conn.ConnectionStats()
		ev := carrierEvidence{Epoch: st.ProgressEpoch, PTOs: st.PTOExpiriesSinceProgress}
		if ctx := g.conn.Context(); ctx.Err() != nil {
			ev.Ended = context.Cause(ctx)
			if ev.Ended == nil {
				ev.Ended = ctx.Err()
			}
		}
		from, to, promote := mc.fsm.step(ev)
		mc.state.Store(uint32(to))
		if from != to {
			changed = true
			m.log.Info("carrier state", "carrier", g.id, "from", from.String(), "to", to.String(),
				"pto_since_progress", ev.PTOs, "transport_epoch", ev.Epoch, "standby", mc.standby,
				"flows", g.flows.Load(), "path_generation", m.generation)
		}
		switch {
		case to.terminal():
			delete(m.carriers, g)
			if m.standby == mc {
				m.standby = nil
			}
			if g == pooled && !mc.standby {
				// DEAD: a verified standby takes over at once; otherwise
				// the flows are unbound and share the path's one grace.
				if sb := m.standby; sb != nil && sb.verified && sb.gen.conn.Context().Err() == nil {
					m.promoteLocked(mc, sb, "carrier dead")
					pooled = sb.gen
				} else {
					c.retireControlQUICGeneration(g, "queqiao carrier dead")
				}
			}
			continue
		case mc.standby:
			if !mc.warmUntil.IsZero() && now.After(mc.warmUntil) {
				// Kept warm after the suspect carrier recovered; nothing
				// was promoted onto it.
				delete(m.carriers, g)
				if m.standby == mc {
					m.standby = nil
				}
				closeLater = append(closeLater, g)
				changed = true
			}
			continue
		}
		if to == carrierDraining {
			continue
		}
		if g != pooled {
			// Retired from the pool by the pool itself (a failed path
			// probe or a pool reset): it is draining what it carries.
			mc.fsm.drain()
			mc.state.Store(uint32(carrierDraining))
			changed = true
			continue
		}
		m.history.observe(now, to == carrierActive)
		m.setKeepAliveLocked(mc, st.SmoothedRTT)
		switch {
		case from == carrierActive && to == carrierSuspect:
			// SUSPECT takes no new flows, makes the attachment PROBING
			// and dials one standby. Nothing moves.
			if a, ok := g.controller.(*wancongestion.PathAttachment); ok {
				a.SetProbing(true)
			}
			m.dialStandbyLocked()
		case from == carrierSuspect && to == carrierActive:
			if sb := m.standby; sb != nil && sb.warmUntil.IsZero() {
				sb.warmUntil = now.Add(standbyWarmFor)
			}
		case to == carrierSuspect:
			if m.standby == nil {
				// The first round failed; the history paces another.
				m.dialStandbyLocked()
			}
		}
		if promote {
			if sb := m.standby; sb != nil && sb.verified && sb.gen.conn.Context().Err() == nil {
				m.promoteLocked(mc, sb, "suspect carrier made no progress")
				pooled = sb.gen
				changed = true
			}
		}
	}
	left := len(m.carriers) > 0
	if !left {
		m.running = false
	}
	m.mu.Unlock()
	for _, g := range closeLater {
		g.close("queqiao standby no longer needed")
	}
	if changed {
		m.broadcast()
	}
	return left
}

// setKeepAliveLocked applies the adaptive keep-alive schedule (fork change
// 11): a floor of max(4×SRTT, 500 ms) while the carrier has open flows; the
// fixed interval otherwise.
func (m *carrierManager) setKeepAliveLocked(mc *managedCarrier, srtt time.Duration) {
	floor := time.Duration(0)
	if mc.gen.flows.Load() > 0 {
		floor = keepAliveFloor(srtt)
	}
	// Re-applying the floor restarts the schedule at it, so it is set only
	// when it moved materially.
	if floor == mc.keepAlive || (floor > 0 && mc.keepAlive > 0 && absDuration(floor-mc.keepAlive) < mc.keepAlive/4) {
		return
	}
	mc.keepAlive = floor
	mc.gen.conn.SetKeepAliveSchedule(floor)
}

func keepAliveFloor(srtt time.Duration) time.Duration {
	return max(keepAliveFloorRTTs*srtt, keepAliveFloorMin)
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// dialStandbyLocked starts the one standby dial round of a SUSPECT carrier,
// if the attempt history allows it.
func (m *carrierManager) dialStandbyLocked() {
	if m.standby != nil || m.dialing {
		return
	}
	if ok, _ := m.admitDial(true); !ok {
		return
	}
	serving := m.c.serving.Load()
	if serving == nil || (*serving).Err() != nil {
		return
	}
	m.dialing = true
	m.standbyDials.Add(1)
	go m.dialStandby(*serving)
}

func (m *carrierManager) dialStandby(serving context.Context) {
	c := m.c
	ctx, cancel := context.WithTimeout(serving, c.cfg.DialTimeout)
	defer cancel()
	g, err := c.dialControlGeneration(ctx, 0, c.pooledCongestionConfig(), m.dialWires(), true)
	if err == nil {
		probeCtx, cancelProbe := context.WithTimeout(g.conn.Context(), c.cfg.HandshakeTimeout)
		probe := probeStandbyGeneration
		if c.probeControlGenerationForTest != nil {
			probe = c.probeControlGenerationForTest
		}
		err = probe(probeCtx, g, c.cfg.HandshakeTimeout)
		cancelProbe()
		if err != nil {
			g.close("queqiao standby failed verification")
		}
	}
	m.mu.Lock()
	m.dialing = false
	if err != nil {
		m.standbyFailures.Add(1)
		m.mu.Unlock()
		m.log.Info("carrier standby unavailable", "error", err)
		return
	}
	mc := g.managed
	mc.verified = true
	m.standby = mc
	m.log.Info("carrier standby verified", "carrier", g.id)
	// Promotion is the next tick's decision: it needs the old carrier's
	// expiries and unchanged epoch, read fresh.
	c.quicMu.Lock()
	pooled := c.quicGeneration
	c.quicMu.Unlock()
	if pooled == nil {
		// The suspect carrier died while this one was dialled: there is
		// nothing to drain, and the flows are waiting.
		m.installLocked(mc, "carrier dead before standby verified")
	} else if pooled.managed != nil && pooled.managed.current() == carrierActive {
		mc.warmUntil = m.now().Add(standbyWarmFor)
	}
	m.mu.Unlock()
	m.broadcast()
}

// promoteLocked makes the verified standby sb the path's carrier over old,
// which becomes DRAINING. It is single-flight: only the tick holding the
// manager's lock does it, once per standby.
func (m *carrierManager) promoteLocked(old, sb *managedCarrier, reason string) {
	m.installLocked(sb, reason)
	if !old.fsm.state.terminal() {
		old.fsm.drain()
		old.state.Store(uint32(carrierDraining))
	}
	if a, ok := old.gen.controller.(*wancongestion.PathAttachment); ok {
		// Nothing returns after promotion (§4.4).
		a.SetDraining()
	}
	old.gen.stopBorrowing(errPoolRetired)
	go m.c.drainControlQUICGeneration(old.gen)
	m.promotions.Add(1)
	m.log.Info("carrier promoted", "old", old.gen.id, "new", sb.gen.id, "reason", reason,
		"path_generation", m.generation, "flows_to_move", old.gen.flows.Load())
}

// installLocked makes sb the pooled carrier.
func (m *carrierManager) installLocked(sb *managedCarrier, reason string) {
	c := m.c
	if m.standby == sb {
		m.standby = nil
	}
	sb.standby, sb.warmUntil = false, time.Time{}
	m.generation++
	m.noteWireLocked(sb.gen)
	c.quicMu.Lock()
	// A break-before-make dial still in flight would install over the
	// promoted carrier; supersede it, and its waiters retry on this one.
	dial := c.quicDial
	if dial != nil {
		c.quicEpoch++
		c.quicDial = nil
	}
	c.quicGeneration = sb.gen
	c.quicMu.Unlock()
	if dial != nil {
		dial.cancel()
	}
	if reason != "" && sb.gen != nil {
		m.log.Debug("carrier installed", "carrier", sb.gen.id, "reason", reason)
	}
}

// pathReset ends every carrier of the old path: a new uplink is a new path
// key, so its carriers are DEAD (§10.2).
func (m *carrierManager) pathReset(reason string) {
	m.mu.Lock()
	var gens []*controlQUICGeneration
	for g := range m.carriers {
		gens = append(gens, g)
	}
	m.standby = nil
	m.mu.Unlock()
	for _, g := range gens {
		g.close(reason)
	}
}

// probeStandbyGeneration verifies a standby by today's bounded PROBE
// exchange, under a verification grant, so that it completes while the
// suspect carrier's flight holds the path's window and balance (§4.3,
// §10.2).
func probeStandbyGeneration(ctx context.Context, g *controlQUICGeneration, budget time.Duration) error {
	return probePooledQUICGranted(ctx, g.conn, budget, g, true)
}

// probeGrantSeq numbers verification grants; the fork requires them nonzero.
var probeGrantSeq atomic.Uint64

// bindProbeGrant binds a verification grant to the first bytes a PROBE
// exchange writes on stream: the probe frames, never anything else.
func bindProbeGrant(conn *quic.Conn, stream quicBidiStream, start int64, budget time.Duration) {
	qs, ok := stream.(*quic.Stream)
	if conn == nil || !ok || qs == nil {
		return
	}
	span := quiccongestion.ByteCount(maxPathProbeFrames*protocol.HeaderSize + maxPathProbeBytes)
	_ = conn.BindVerificationGrant(qs, quiccongestion.VerificationGrant{
		ID:    probeGrantSeq.Add(1),
		Start: quiccongestion.ByteCount(start), End: quiccongestion.ByteCount(start) + span,
		// A few full packets: the frames and their retransmissions.
		Budget: 8 * 1500,
		Expiry: monotime.FromTime(time.Now().Add(budget)),
	})
}

// runKeepAliveSchedule keeps a gateway connection's adaptive keep-alive
// floor at max(4×SRTT, 500 ms) while active reports open flows, and the
// fixed interval otherwise (§10.2, fork change 11), until it closes.
func runKeepAliveSchedule(conn *quic.Conn, active func() bool, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	var set time.Duration
	for {
		select {
		case <-conn.Context().Done():
			return
		case <-ticker.C:
		}
		floor := time.Duration(0)
		if active() {
			floor = keepAliveFloor(conn.ConnectionStats().SmoothedRTT)
		}
		if floor == set || (floor > 0 && set > 0 && absDuration(floor-set) < set/4) {
			continue
		}
		set = floor
		conn.SetKeepAliveSchedule(floor)
	}
}

// carrierRebindKey marks a pooled borrow made to rebind an existing flow:
// it is not held back for flows waiting to rebind, and a dial it needs is a
// replacement round.
type carrierRebindKey struct{}

func withCarrierRebind(ctx context.Context) context.Context {
	return context.WithValue(ctx, carrierRebindKey{}, true)
}

func isCarrierRebind(ctx context.Context) bool {
	v, _ := ctx.Value(carrierRebindKey{}).(bool)
	return v
}

// joinQueue admits rebinding JOINs earliest-deadline-first within the order
// of §10.3 step 3 (pending FIN or ACK, then INTERACTIVE, then BULK), at a
// concurrency of max(16, N×r/T) for N flows to move and JOIN round trip r.
type joinQueue struct {
	mu       sync.Mutex
	inflight int
	waiters  joinHeap
	seq      uint64
	// rtt is the JOIN round trip estimate r, read when a JOIN is admitted.
	rtt func() time.Duration
}

type joinWaiter struct {
	priority int
	deadline time.Time
	seq      uint64
	ready    chan struct{}
	granted  bool
	index    int
}

type joinHeap []*joinWaiter

func (h joinHeap) Len() int { return len(h) }
func (h joinHeap) Less(i, j int) bool {
	if h[i].priority != h[j].priority {
		return h[i].priority < h[j].priority
	}
	if !h[i].deadline.Equal(h[j].deadline) {
		return h[i].deadline.Before(h[j].deadline)
	}
	return h[i].seq < h[j].seq
}
func (h joinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *joinHeap) Push(x any)   { w := x.(*joinWaiter); w.index = len(*h); *h = append(*h, w) }
func (h *joinHeap) Pop() any {
	old := *h
	n := len(old)
	w := old[n-1]
	old[n-1] = nil
	w.index = -1
	*h = old[:n-1]
	return w
}

func (q *joinQueue) init() {}

// limitLocked is the concurrency for the JOINs queued or running now.
func (q *joinQueue) limitLocked() int {
	n := q.inflight + len(q.waiters)
	r := 100 * time.Millisecond
	if q.rtt != nil {
		if measured := q.rtt(); measured > 0 {
			r = measured
		}
	}
	return max(minJoinConcurrency, int((time.Duration(n)*r+joinTarget-1)/joinTarget))
}

func (q *joinQueue) grantLocked() {
	for len(q.waiters) > 0 && q.inflight < q.limitLocked() {
		w := heap.Pop(&q.waiters).(*joinWaiter)
		w.granted = true
		q.inflight++
		close(w.ready)
	}
}

// acquire waits for a JOIN slot. The returned release must be called once.
func (q *joinQueue) acquire(ctx context.Context, priority int, deadline time.Time) (func(), error) {
	q.mu.Lock()
	q.seq++
	w := &joinWaiter{priority: priority, deadline: deadline, seq: q.seq, ready: make(chan struct{})}
	heap.Push(&q.waiters, w)
	q.grantLocked()
	q.mu.Unlock()
	release := func() {
		q.mu.Lock()
		q.inflight--
		q.grantLocked()
		q.mu.Unlock()
	}
	select {
	case <-w.ready:
		return release, nil
	case <-ctx.Done():
		q.mu.Lock()
		if w.granted {
			q.mu.Unlock()
			release()
		} else {
			heap.Remove(&q.waiters, w.index)
			q.mu.Unlock()
		}
		return nil, ctx.Err()
	}
}

// joinPriority is a flow's place in the JOIN order of §10.3 step 3.
func joinPriority(flow *multipathFlow) int {
	switch {
	case flow.finSent.Load() || flow.remoteFinSeen.Load():
		return 0
	case !flow.carriesBulk():
		return 1
	}
	return 2
}

// randomJitter spreads a retry by up to a quarter of d.
func randomJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return d + time.Duration(rand.Int64N(int64(d)/4+1))
}

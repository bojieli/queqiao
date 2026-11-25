package congestion

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	quiccongestion "github.com/apernet/quic-go/congestion"
	"github.com/apernet/quic-go/monotime"

	"github.com/bojieli/queqiao/internal/pathmodel"
)

// PathController is the one congestion authority of a path and sending
// direction (docs/DESIGN-PATH-AUTHORITY.md §4; stage 6 of the migration,
// congestion=path). Connections attach to it and ask it to admit each
// packet. It owns the delivery-rate estimate, the minimum round trip, the
// erasure estimate and its compensation, the delay brake, the pacing balance,
// the window and the choice of which class is served next. It has no
// members, shares, seeds or ramps, because nothing is divided: every
// attachment draws on the same balance and the same window.
//
// The control law is the erasure sender, unchanged, applied once to the
// path. What changes is what it is fed:
//
//   - One send order. Every sample packet of every attachment gets a path
//     sequence number when it is registered, and the law sees only those
//     numbers. A fate record per sequence number resolves to delivered,
//     lost, discarded or expired; the erasure estimator observes the fates in
//     sequence order, so its loss-run statistic does not depend on the order
//     in which the attachments' acknowledgements arrive, and an unknown fate
//     breaks a run instead of joining its neighbours.
//   - Sampler states retire by fate. The delivery sampler of a connection
//     drops every state below a watermark derived from the largest
//     acknowledgement; under a shared send order that would erase the states
//     of the other attachments' packets still in flight. Here a state leaves
//     when its packet is acknowledged, or when its fate is final without
//     one: discarded, expired, or lost past the window in which a spurious
//     correction may still restore it.
//   - Flight is a sum of gauges. Each attachment reports its connection's
//     own bytes in flight; the path's flight is the sum over COUNTED
//     attachments. Nothing is subtracted, so it cannot go negative, and a
//     detached connection's flight leaves the sum with it.
//   - The balance keeps debt. Every ack-eliciting packet, exempt or not, is
//     debited when it is registered; packets that elicit nothing are counted
//     and not debited.
//
// All methods are safe for concurrent use: the attachments' run loops call
// in under one mutex, and wake-ups are delivered after it is released.
type PathController struct {
	mu  sync.Mutex
	law *ErasureSender
	arb *ClassArbiter
	rtt pathRTT

	atts map[*PathAttachment]struct{}
	// flight is the sum of the COUNTED attachments' gauges.
	flight quiccongestion.ByteCount
	// now is the latest event time; event times are clamped to it so the
	// law sees a non-decreasing clock across the attachments' run loops.
	now monotime.Time

	// The send order: ring holds the fate records of path sequence numbers
	// [ringBase, nextPseq) that the estimator has not yet observed.
	nextPseq quiccongestion.PacketNumber
	ringBase quiccongestion.PacketNumber
	ring     []fateRecord
	// lostHeld are the sampler states of packets reported lost, kept for a
	// spurious correction until their deadline.
	lostHeld []heldLoss

	maxPacket quiccongestion.ByteCount
	// sendSeq counts registered packets, so a refused attachment can tell
	// whether the attachment holding a claim on the turn has sent since.
	sendSeq uint64
	// permitSeq counts permits issued.
	permitSeq uint64

	// Scratch for the law's batches.
	acked []quiccongestion.AckedPacketInfo
	lost  []quiccongestion.LostPacketInfo

	stats pathControllerCounters
	// lastDetach is when the last attachment detached, for a registry
	// deciding how long the path's knowledge stands.
	attached   atomic.Int64
	lastDetach atomic.Int64 // UnixNano
	created    time.Time
}

type fate uint8

const (
	fatePending fate = iota
	fateDelivered
	fateLost
	fateDiscarded
	fateExpired
)

type packetKey struct {
	space  quiccongestion.PacketSpace
	number quiccongestion.PacketNumber
}

type fateRecord struct {
	att  *PathAttachment
	key  packetKey
	sent monotime.Time
	fate fate
}

type heldLoss struct {
	att      *PathAttachment
	key      packetKey
	pseq     quiccongestion.PacketNumber
	deadline monotime.Time
}

// sampleRef is an attachment's record of one of its sample packets.
type sampleRef struct {
	pseq  quiccongestion.PacketNumber
	bytes quiccongestion.ByteCount
	lost  bool
	// correctable is, for a lost packet, until when a correction may
	// restore it. Held losses expire in report order, which is not deadline
	// order when the round trip falls, so it is checked again here.
	correctable monotime.Time
}

type pathControllerCounters struct {
	delivered, lost, discarded, expired, spurious atomic.Uint64
	samples, unsampledDebited, nonEliciting       atomic.Uint64
	exemptBytes                                   atomic.Uint64
	refusedWindow, refusedPacing, refusedTurn     atomic.Uint64
	lapsedClaims, debtPermits                     atomic.Uint64
	demotions, promotions                         atomic.Uint64
}

const (
	// pathMaxRecords bounds the fate records awaiting observation; the
	// oldest is expired when it is full.
	pathMaxRecords = 8192
	// pathMaxHeldLosses bounds the sampler states held for a spurious
	// correction.
	pathMaxHeldLosses = 8192
	// pathFateHorizonRTTs is how many smoothed round trips a record may stay
	// unresolved before it expires, and how long a loss stays correctable.
	pathFateHorizonRTTs = 4
	// pathControlDebtPackets is the control debt D (§4.3) in packets: a
	// CONTROL turn may exceed the window and the balance by this much, with
	// nothing else in the packet.
	pathControlDebtPackets = 2
	// pathProbeWindowPackets is a PROBING attachment's own window for
	// ordinary packets (§4.4).
	pathProbeWindowPackets = 2
	// pathProbingPTOProbes is how many PTO probes an attachment may send
	// without transport progress before it is PROBING: its flight then
	// leaves the sum, so a connection whose packets are no longer being
	// acknowledged does not hold the path's window shut for the others.
	pathProbingPTOProbes = 4
)

// NewPathController returns the authority of one path. path, if not nil, is
// the path model the law reports its measurements to, for the readers that
// size FEC from them; the controller takes nothing from it but the erasure
// prior of its knowledge. weights are the class arbitration weights.
func NewPathController(initialPacketSize quiccongestion.ByteCount, path pathmodel.Model, weights ClassWeights) *PathController {
	law := newPathLaw(initialPacketSize, path)
	c := &PathController{
		law:       law,
		arb:       NewClassArbiter(weights, law.inner.maxDatagramSize),
		atts:      make(map[*PathAttachment]struct{}),
		nextPseq:  1,
		ringBase:  1,
		maxPacket: law.inner.maxDatagramSize,
		created:   time.Now(),
	}
	law.SetRTTStatsProvider(&c.rtt)
	return c
}

// Attach returns a new attachment of this controller, to be installed on one
// connection. rekey, if not nil, is asked after a migration (and at
// attachment) which controller the connection's path now belongs to; it
// returns nil to stay. addrs, if not nil, reports the connection's current
// addresses at attachment.
func (c *PathController) Attach(rekey func(oldLocal, newLocal, oldRemote, newRemote net.Addr) *PathController, addrs func() (local, remote net.Addr)) *PathAttachment {
	a := &PathAttachment{rekey: rekey, addrs: addrs, pkts: make(map[packetKey]sampleRef)}
	a.ctl.Store(c)
	return a
}

// Attachments is the number of attachments currently attached.
func (c *PathController) Attachments() int { return int(c.attached.Load()) }

// Touch marks the controller as just handed out, so a registry does not
// count the time before an attachment installs as idleness.
func (c *PathController) Touch() {
	if c.attached.Load() == 0 {
		c.lastDetach.Store(time.Now().UnixNano())
	}
}

// IdleSince reports when the last attachment detached -- or, if none ever
// attached, when the controller was created -- and false while any is
// attached. A controller whose installation never happened (the connection
// closed first, or chose another path) is therefore idle from its creation.
func (c *PathController) IdleSince() (time.Time, bool) {
	if c.attached.Load() > 0 {
		return time.Time{}, false
	}
	ns := c.lastDetach.Load()
	if ns == 0 {
		return c.created, true
	}
	return time.Unix(0, ns), true
}

// Telemetry is the law's state, which is the path's.
func (c *PathController) Telemetry() ControllerTelemetry {
	t := c.law.Telemetry()
	t.Kind = "erasure-path"
	return t
}

// PathControllerStats is a snapshot of a controller's accounting.
type PathControllerStats struct {
	Attachments int
	// Flight is the path's flight, and Gauges each attachment's own, by
	// attachment epoch; Counted says whether each is in the sum.
	Flight  quiccongestion.ByteCount
	Gauges  map[uint64]quiccongestion.ByteCount
	Counted map[uint64]bool
	// Fates of sample packets.
	Delivered, Lost, Discarded, Expired, Spurious uint64
	// Pending is the fate records not yet observed, and Held the losses
	// still correctable.
	Pending, Held int
	// Samples is the sample packets registered; UnsampledDebited the
	// ack-eliciting packets debited without being samples; NonEliciting the
	// packets that elicit nothing, which are not debited.
	Samples, UnsampledDebited, NonEliciting uint64
	// SamplerDelivered is the bytes the delivery sampler has counted as
	// acknowledged, and SamplerStates the states it holds.
	SamplerDelivered uint64
	SamplerStates    int
	// Refusals by reason.
	RefusedWindow, RefusedPacing, RefusedTurn, LapsedClaims, DebtPermits uint64
	Demotions, Promotions                                                uint64
	Classes                                                              ClassArbiterStats
}

// Stats returns a snapshot.
func (c *PathController) Stats() PathControllerStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := PathControllerStats{
		Attachments: len(c.atts),
		Flight:      c.flight,
		Gauges:      make(map[uint64]quiccongestion.ByteCount, len(c.atts)),
		Counted:     make(map[uint64]bool, len(c.atts)),
		Delivered:   c.stats.delivered.Load(), Lost: c.stats.lost.Load(),
		Discarded: c.stats.discarded.Load(), Expired: c.stats.expired.Load(),
		Spurious: c.stats.spurious.Load(),
		Pending:  len(c.ring), Held: len(c.lostHeld),
		Samples: c.stats.samples.Load(), UnsampledDebited: c.stats.unsampledDebited.Load(),
		NonEliciting:     c.stats.nonEliciting.Load(),
		SamplerDelivered: c.law.inner.estimator.totalAcked,
		SamplerStates:    len(c.law.inner.estimator.packetStates),
		RefusedWindow:    c.stats.refusedWindow.Load(), RefusedPacing: c.stats.refusedPacing.Load(),
		RefusedTurn: c.stats.refusedTurn.Load(), LapsedClaims: c.stats.lapsedClaims.Load(),
		DebtPermits: c.stats.debtPermits.Load(),
		Demotions:   c.stats.demotions.Load(), Promotions: c.stats.promotions.Load(),
		Classes: c.arb.Stats(),
	}
	for a := range c.atts {
		s.Gauges[a.epoch] = a.gauge
		s.Counted[a.epoch] = !a.probing
	}
	return s
}

// clamp makes an event time non-decreasing across attachments.
func (c *PathController) clamp(t monotime.Time) monotime.Time {
	if t.IsZero() {
		t = monotime.Now()
	}
	if t.Before(c.now) {
		return c.now
	}
	c.now = t
	return t
}

// horizon is how long a fate may stay unresolved, and a loss correctable.
func (c *PathController) horizon() time.Duration {
	srtt := c.rtt.SmoothedRTT()
	if srtt <= 0 {
		srtt = c.law.inner.rtt()
	}
	return pathFateHorizonRTTs * srtt
}

// record returns the fate record of pseq while it awaits observation.
func (c *PathController) record(pseq quiccongestion.PacketNumber) *fateRecord {
	i := pseq - c.ringBase
	if i < 0 || i >= quiccongestion.PacketNumber(len(c.ring)) {
		return nil
	}
	return &c.ring[i]
}

// resolve sets the fate of a pending record, counting it.
func (c *PathController) resolve(pseq quiccongestion.PacketNumber, f fate) {
	r := c.record(pseq)
	if r == nil || r.fate != fatePending {
		return
	}
	r.fate = f
	switch f {
	case fateDelivered:
		c.stats.delivered.Add(1)
	case fateLost:
		c.stats.lost.Add(1)
	case fateDiscarded:
		c.stats.discarded.Add(1)
	case fateExpired:
		c.stats.expired.Add(1)
	}
}

// advance expires what has waited past the horizon and lets the estimator
// observe every resolved fate at the front of the send order, in order.
func (c *PathController) advance(now monotime.Time) {
	horizon := c.horizon()
	for len(c.ring) > 0 {
		r := &c.ring[0]
		if r.fate == fatePending {
			if len(c.ring) <= pathMaxRecords && now.Sub(r.sent) <= horizon {
				break
			}
			// No outcome within the horizon, or the ring is full: the
			// packet's fate is unknowable as far as the path is concerned.
			c.resolve(c.ringBase, fateExpired)
			if ref, ok := r.att.pkts[r.key]; ok && ref.pseq == c.ringBase {
				delete(r.att.pkts, r.key)
			}
			c.law.inner.estimator.retire(c.ringBase)
		}
		switch r.fate {
		case fateDelivered:
			c.law.estimator.ObserveOutcome(true)
		case fateLost:
			c.law.estimator.ObserveOutcome(false)
		default:
			c.law.estimator.Break()
		}
		c.ring[0] = fateRecord{}
		c.ring = c.ring[1:]
		c.ringBase++
	}
	for len(c.lostHeld) > 0 {
		h := c.lostHeld[0]
		if len(c.lostHeld) <= pathMaxHeldLosses && now.Before(h.deadline) {
			break
		}
		// The loss stands: no correction can restore its state now.
		if ref, ok := h.att.pkts[h.key]; ok && ref.pseq == h.pseq {
			delete(h.att.pkts, h.key)
		}
		c.law.inner.estimator.retire(h.pseq)
		c.lostHeld[0] = heldLoss{}
		c.lostHeld = c.lostHeld[1:]
	}
}

// discard ends a sample packet of a without a fate.
func (c *PathController) discard(a *PathAttachment, key packetKey) {
	ref, ok := a.pkts[key]
	if !ok {
		return
	}
	delete(a.pkts, key)
	if !ref.lost {
		c.resolve(ref.pseq, fateDiscarded)
	}
	c.law.inner.estimator.retire(ref.pseq)
}

// setGauge records an attachment's flight and keeps the sum.
func (c *PathController) setGauge(a *PathAttachment, inFlight quiccongestion.ByteCount) {
	if inFlight < 0 {
		inFlight = 0
	}
	if !a.probing {
		c.flight += inFlight - a.gauge
	}
	a.gauge = inFlight
}

// demand is the union of the attachments' demand.
func (c *PathController) demand() quiccongestion.Demand {
	var d quiccongestion.Demand
	for a := range c.atts {
		for i, e := range a.demand.Eligible {
			d.Eligible[i] = d.Eligible[i] || e
		}
	}
	return d
}

// collectBlocked returns the wakers of attachments refused for the window,
// except a, and clears their flag: flight fell or the window grew.
func (c *PathController) collectBlocked(a *PathAttachment, wakes []func()) []func() {
	for o := range c.atts {
		if o != a && o.windowBlocked && o.wake != nil {
			o.windowBlocked = false
			wakes = append(wakes, o.wake)
		}
	}
	return wakes
}

// collectWaiting returns the wakers of attachments refused for precedence,
// except a: the claim they waited on may have cleared.
func (c *PathController) collectWaiting(a *PathAttachment, wakes []func()) []func() {
	for o := range c.atts {
		if o != a && o.deferred && o.wake != nil {
			wakes = append(wakes, o.wake)
		}
	}
	return wakes
}

func runWakes(wakes []func()) {
	for _, w := range wakes {
		w()
	}
}

func (c *PathController) attach(a *PathAttachment, now monotime.Time, inFlight, maxPacket quiccongestion.ByteCount) {
	c.clamp(now)
	c.atts[a] = struct{}{}
	c.attached.Store(int64(len(c.atts)))
	a.probing = false
	a.gauge = 0
	c.setGauge(a, inFlight)
	c.notePacketSize(maxPacket)
	c.arb.OnDemand(c.demand())
}

// detach takes a out of the controller: its gauge leaves the sum and its
// unresolved records are discarded.
func (c *PathController) detach(a *PathAttachment, now monotime.Time) []func() {
	if _, ok := c.atts[a]; !ok {
		return nil
	}
	now = c.clamp(now)
	for key := range a.pkts {
		c.discard(a, key)
	}
	// Its held losses go too: after a migration the attachment's map is
	// written under another controller's lock, so nothing here may keep a
	// reference to it.
	kept := c.lostHeld[:0]
	for _, h := range c.lostHeld {
		if h.att == a {
			c.law.inner.estimator.retire(h.pseq)
			continue
		}
		kept = append(kept, h)
	}
	clear(c.lostHeld[len(kept):])
	c.lostHeld = kept
	if !a.probing {
		c.flight -= a.gauge
	}
	a.gauge = 0
	a.windowBlocked = false
	a.claimDeadline = 0
	a.waiting, a.deferred, a.unable = false, false, false
	delete(c.atts, a)
	c.attached.Store(int64(len(c.atts)))
	if len(c.atts) == 0 {
		c.lastDetach.Store(time.Now().UnixNano())
	}
	c.arb.OnDemand(c.demand())
	c.advance(now)
	wakes := c.collectBlocked(a, nil)
	return c.collectWaiting(a, wakes)
}

// notePacketSize follows the largest packet size an attachment reports.
// Sizes only grow: a smaller one on another attachment would rescale the
// law's window for everyone.
func (c *PathController) notePacketSize(size quiccongestion.ByteCount) {
	if size <= c.maxPacket {
		return
	}
	c.maxPacket = size
	c.law.SetMaxDatagramSize(size)
	c.arb.SetQuantum(size)
}

// packetTime is how long the path's rate takes to send n bytes.
func (c *PathController) packetTime(n quiccongestion.ByteCount) time.Duration {
	rate := c.law.bandwidth()
	if rate <= 0 {
		return time.Millisecond
	}
	return max(time.Duration(float64(n)/float64(rate)*float64(time.Second)), quiccongestion.MinPacingDelay)
}

func (c *PathController) admit(a *PathAttachment, now monotime.Time, maxBytes quiccongestion.ByteCount) (quiccongestion.Permit, []func()) {
	now = c.clamp(now)
	c.advance(now)
	var wakes []func()

	// The turn. An attachment defers to another that has a claim on it:
	// one with demand in the path's turn when this one has none, or, among
	// attachments with demand in the turn, one that was refused and has been
	// served less recently, so the connection whose acknowledgement opened
	// the window cannot take every opening from a woken sibling.
	//
	// A claim is held by its claimant, not by the attachment deferring: the
	// first deferral to it gives it one packet time to use the turn. If it
	// sends, the claim is live; if it does not, the claim lapses and stays
	// lapsed until the claimant sends again. A claimant that asks and cannot
	// use the turn for a reason of its own (a PROBING attachment's full
	// probe window) holds no claim until it next sends either.
	turn := c.arb.Select()
	if turn != quiccongestion.ClassExempt && !a.demand.Eligible[turn] && !c.claimed(a, turn, now) {
		for o := range c.atts {
			if o != a && o.demand.Eligible[turn] {
				// Another attachment has demand in the turn but no live
				// claim: its claim lapsed.
				c.stats.lapsedClaims.Add(1)
				break
			}
		}
		turn = c.arb.SelectAmong(a.demand.Eligible)
	}
	// The window and the balance, for the turn the path would serve now.
	borrowed, refusal := c.admissible(a, now, turn)
	if refusal != nil {
		a.waiting = true
		return *refusal, wakes
	}
	if c.claimed(a, turn, now) {
		var retry monotime.Time
		for o := range c.atts {
			if !c.claims(o, a, turn, now) {
				continue
			}
			if o.claimDeadline.IsZero() {
				o.claimDeadline = now.Add(c.packetTime(maxBytes))
			}
			if retry.IsZero() || o.claimDeadline.Before(retry) {
				retry = o.claimDeadline
			}
			if o.wake != nil {
				wakes = append(wakes, o.wake)
			}
		}
		c.stats.refusedTurn.Add(1)
		a.waiting, a.deferred = true, true
		return quiccongestion.Permit{RetryAt: retry}, wakes
	}
	if !a.demand.Eligible[turn] && turn != quiccongestion.ClassExempt {
		// Every claim on the path's turn lapsed: this attachment is served
		// from its own demand, and the class keeps its deficit.
		turn = c.arb.SelectAmong(a.demand.Eligible)
		if borrowed, refusal = c.admissible(a, now, turn); refusal != nil {
			a.waiting = true
			return *refusal, wakes
		}
	}
	if borrowed {
		c.stats.debtPermits.Add(1)
	}
	a.windowBlocked = false
	a.waiting, a.deferred = false, false
	c.permitSeq++
	a.lastPermit = c.permitSeq
	return c.arb.PermitTurn(turn, maxBytes, !borrowed), wakes
}

// claims says whether o holds a claim on turn against a.
func (c *PathController) claims(o, a *PathAttachment, turn quiccongestion.SendClass, now monotime.Time) bool {
	if o == a || !o.demand.Eligible[turn] || o.unable {
		return false
	}
	if !o.claimDeadline.IsZero() && !now.Before(o.claimDeadline) {
		return false // lapsed: it was given a packet time and did not ask
	}
	if !a.demand.Eligible[turn] {
		return true
	}
	return o.waiting && o.lastPermit < a.lastPermit
}

// claimed says whether any attachment holds a claim on turn against a.
func (c *PathController) claimed(a *PathAttachment, turn quiccongestion.SendClass, now monotime.Time) bool {
	if turn == quiccongestion.ClassExempt {
		return false
	}
	for o := range c.atts {
		if c.claims(o, a, turn, now) {
			return true
		}
	}
	return false
}

// admissible checks the window, against the path's flight or a PROBING
// attachment's own, and the balance, for a packet of the given turn. A
// CONTROL turn may borrow the control debt; borrowed says it did.
func (c *PathController) admissible(a *PathAttachment, now monotime.Time, turn quiccongestion.SendClass) (bool, *quiccongestion.Permit) {
	debt := quiccongestion.ByteCount(0)
	if turn == quiccongestion.ClassControl {
		debt = pathControlDebtPackets * c.maxPacket
	}
	borrowed := false
	window := c.law.GetCongestionWindow()
	c.law.recordInFlight(c.flight)
	if a.probing {
		if a.gauge >= pathProbeWindowPackets*c.maxPacket {
			a.windowBlocked = true
			a.unable = true
			c.stats.refusedWindow.Add(1)
			return false, &quiccongestion.Permit{}
		}
	} else if c.flight >= window {
		if c.flight >= window+debt {
			a.windowBlocked = true
			c.stats.refusedWindow.Add(1)
			return false, &quiccongestion.Permit{}
		}
		borrowed = true
	}
	if budget, need := c.law.pacer.budget(now), c.law.inner.maxDatagramSize; budget < need {
		if budget < need-debt {
			c.stats.refusedPacing.Add(1)
			retry := c.law.pacer.timeUntilSend()
			if retry.IsZero() || retry.Before(now) {
				retry = now
			}
			return false, &quiccongestion.Permit{RetryAt: retry}
		}
		borrowed = true
	}
	return borrowed, nil
}

func (c *PathController) sent(a *PathAttachment, s quiccongestion.SentPacket) []func() {
	now := c.clamp(s.Time)
	var wakes []func()
	before := c.flight
	c.setGauge(a, s.InFlight)
	c.arb.OnSent(s.ClassBytes)
	c.sendSeq++
	a.lastSentSeq = c.sendSeq
	a.unable = false
	a.claimDeadline = 0
	if s.Exempt {
		c.stats.exemptBytes.Add(byteTotal(s.Bytes))
	}
	if s.Kind == quiccongestion.KindPTOProbe {
		a.ptoSinceProgress++
		if !a.probing && a.ptoSinceProgress >= pathProbingPTOProbes {
			c.demote(a)
			before = c.flight
			// Its flight left the sum: the siblings it held may send.
			wakes = c.collectBlocked(a, wakes)
		}
	}
	c.law.recordInFlight(c.flight)
	sample := !a.probing && s.Space == quiccongestion.SpaceApplication && s.AckEliciting && s.InFlightCounted &&
		(s.Kind == quiccongestion.KindOrdinary || s.Kind == quiccongestion.KindPTOProbe)
	switch {
	case sample:
		pseq := c.nextPseq
		c.nextPseq++
		key := packetKey{space: s.Space, number: s.Number}
		c.ring = append(c.ring, fateRecord{att: a, key: key, sent: now})
		a.pkts[key] = sampleRef{pseq: pseq, bytes: s.Bytes}
		c.stats.samples.Add(1)
		// The sampler's pre-send flight is the path's before this packet.
		c.law.OnPacketSent(now, before+s.Bytes, pseq, s.Bytes, true)
	case s.AckEliciting:
		// Debited, never gated by an estimate it is not part of.
		c.stats.unsampledDebited.Add(1)
		c.law.pacer.sentPacket(now, s.Bytes)
	default:
		c.stats.nonEliciting.Add(1)
		c.law.nonElicitingUndebited.Add(byteTotal(s.Bytes))
	}
	c.advance(now)
	return wakes
}

// demote makes a PROBING: its flight leaves the sum.
func (c *PathController) demote(a *PathAttachment) {
	if a.probing {
		return
	}
	c.flight -= a.gauge
	a.probing = true
	c.stats.demotions.Add(1)
	// Its sample packets still in flight are not measurements of the path
	// any more than its next ones are.
	for key, ref := range a.pkts {
		if !ref.lost {
			c.discard(a, key)
		}
	}
}

// promote makes a COUNTED again with its current gauge.
func (c *PathController) promote(a *PathAttachment) {
	if !a.probing {
		return
	}
	a.probing = false
	c.flight += a.gauge
	c.stats.promotions.Add(1)
}

func (c *PathController) ackEvent(a *PathAttachment, ev quiccongestion.AckEvent) []func() {
	now := c.clamp(ev.Time)
	// Deadlines are enforced before outcomes are accepted, so an
	// acknowledgement past the horizon is not credited merely because no
	// other callback came first.
	c.advance(now)
	before := c.flight
	windowBefore := c.law.GetCongestionWindow()
	c.setGauge(a, ev.InFlight)
	c.rtt.observe(ev.RTTSample)
	c.acked, c.lost = c.acked[:0], c.lost[:0]
	var resolved quiccongestion.ByteCount
	progress := false
	for _, r := range ev.Acked {
		if r.Space == quiccongestion.SpaceApplication {
			progress = true
		}
		key := packetKey{space: r.Space, number: r.Number}
		ref, ok := a.pkts[key]
		if !ok {
			continue
		}
		delete(a.pkts, key)
		if ref.lost && !now.Before(ref.correctable) {
			// Past its correction window: the loss stands.
			c.law.inner.estimator.retire(ref.pseq)
			continue
		}
		if !ref.lost {
			c.resolve(ref.pseq, fateDelivered)
		} else if rec := c.record(ref.pseq); rec != nil && rec.fate == fateLost {
			rec.fate = fateDelivered
			c.stats.spurious.Add(1)
		}
		c.acked = append(c.acked, quiccongestion.AckedPacketInfo{PacketNumber: ref.pseq, BytesAcked: ref.bytes})
		resolved += ref.bytes
	}
	for _, r := range ev.Spurious {
		if r.Space == quiccongestion.SpaceApplication {
			progress = true
		}
		key := packetKey{space: r.Space, number: r.Number}
		ref, ok := a.pkts[key]
		if !ok || !ref.lost {
			continue
		}
		delete(a.pkts, key)
		if !now.Before(ref.correctable) {
			// Past its correction window: the loss stands.
			c.law.inner.estimator.retire(ref.pseq)
			continue
		}
		c.stats.spurious.Add(1)
		// A correction flips the fate if the estimator has not observed it
		// yet; beyond that the loss stands there. The sampler still has the
		// state, so the bytes count as delivered.
		if rec := c.record(ref.pseq); rec != nil && rec.fate == fateLost {
			rec.fate = fateDelivered
		}
		c.acked = append(c.acked, quiccongestion.AckedPacketInfo{PacketNumber: ref.pseq, BytesAcked: ref.bytes})
		resolved += ref.bytes
	}
	for _, r := range ev.Lost {
		key := packetKey{space: r.Space, number: r.Number}
		ref, ok := a.pkts[key]
		if !ok || ref.lost {
			continue
		}
		ref.lost = true
		ref.correctable = now.Add(c.horizon())
		a.pkts[key] = ref
		c.resolve(ref.pseq, fateLost)
		c.lostHeld = append(c.lostHeld, heldLoss{att: a, key: key, pseq: ref.pseq, deadline: ref.correctable})
		c.lost = append(c.lost, quiccongestion.LostPacketInfo{PacketNumber: ref.pseq, BytesLost: ref.bytes})
		resolved += ref.bytes
	}
	if progress {
		a.ptoSinceProgress = 0
		c.promote(a)
	}
	c.advance(now)
	c.law.recordInFlight(c.flight)
	if len(c.acked) > 0 || len(c.lost) > 0 {
		// The law's flight after the event is the path's: its prior is
		// that plus what this event resolved of its samples.
		c.law.afterOutcomes(c.flight+resolved, now, c.acked, c.lost)
	}
	if c.flight < before || c.law.GetCongestionWindow() > windowBefore {
		return c.collectBlocked(a, nil)
	}
	return nil
}

func (c *PathController) discarded(a *PathAttachment, now monotime.Time, inFlight quiccongestion.ByteCount, refs []quiccongestion.PacketRef) []func() {
	now = c.clamp(now)
	before := c.flight
	c.setGauge(a, inFlight)
	for _, r := range refs {
		c.discard(a, packetKey{space: r.Space, number: r.Number})
	}
	c.advance(now)
	c.law.recordInFlight(c.flight)
	if c.flight < before {
		return c.collectBlocked(a, nil)
	}
	return nil
}

func (c *PathController) setDemand(a *PathAttachment, d quiccongestion.Demand) []func() {
	a.demand = d
	c.arb.OnDemand(c.demand())
	return c.collectWaiting(a, nil)
}

// PathAttachment is one connection's attachment to a PathController. It is
// the PathSender installed on the connection; it forwards every event to the
// controller under the controller's lock. An attachment has an epoch and a
// state, COUNTED or PROBING, which only the controller changes.
type PathAttachment struct {
	ctl   atomic.Pointer[PathController]
	rekey func(oldLocal, newLocal, oldRemote, newRemote net.Addr) *PathController
	addrs func() (local, remote net.Addr)

	// Under the controller's lock.
	epoch         uint64
	gauge         quiccongestion.ByteCount
	probing       bool
	demand        quiccongestion.Demand
	pkts          map[packetKey]sampleRef
	windowBlocked bool
	lastSentSeq   uint64
	// claimDeadline is when this attachment's claim on a turn lapses unless
	// it sends first; zero while nobody has deferred to it since it last
	// sent.
	claimDeadline monotime.Time
	// unable says it asked and could not use the path's opportunity for a
	// reason of its own; it holds no claim until it next sends.
	unable bool
	// deferred says its last Admit deferred to a claim.
	deferred bool
	// waiting says the attachment's last Admit was refused; lastPermit is
	// the controller's permit count at its last permit. Together they rank
	// attachments with demand in the same class.
	waiting    bool
	lastPermit uint64
	// ptoSinceProgress counts PTO probes sent since the connection's packets
	// were last acknowledged.
	ptoSinceProgress int

	// Run loop only.
	wake      func()
	attached  bool
	maxPacket quiccongestion.ByteCount
	connRTT   quiccongestion.RTTStatsProvider
}

var attachmentEpochs atomic.Uint64

var (
	_ quiccongestion.PathSender     = (*PathAttachment)(nil)
	_ quiccongestion.ClassScheduler = (*PathAttachment)(nil)
	_ TelemetryProvider             = (*PathAttachment)(nil)
)

// Controller returns the controller the attachment is attached to.
func (a *PathAttachment) Controller() *PathController { return a.ctl.Load() }

// SchedulesClasses is true: a path controller always arbitrates classes, so
// the connection's builder executes its permits.
func (a *PathAttachment) SchedulesClasses() bool { return true }

// Telemetry is the path's.
func (a *PathAttachment) Telemetry() ControllerTelemetry { return a.ctl.Load().Telemetry() }

// ClassStats returns the path's arbiter state.
func (a *PathAttachment) ClassStats() (ClassArbiterStats, bool) {
	return a.ctl.Load().arb.Stats(), true
}

func (a *PathAttachment) attachTo(c *PathController, now monotime.Time, inFlight quiccongestion.ByteCount) {
	a.ctl.Store(c)
	c.mu.Lock()
	a.epoch = attachmentEpochs.Add(1)
	if a.connRTT != nil && c.rtt.fallback == nil {
		c.rtt.fallback = a.connRTT
	}
	c.attach(a, now, inFlight, a.maxPacket)
	c.mu.Unlock()
}

// OnAttach is the first call.
func (a *PathAttachment) OnAttach(now monotime.Time, inFlight, maxPacket quiccongestion.ByteCount, wake func()) {
	a.wake = wake
	a.maxPacket = maxPacket
	a.attached = true
	c := a.ctl.Load()
	// The connection may have migrated between choosing the controller and
	// this installation on the run loop: revalidate the path.
	if a.addrs != nil && a.rekey != nil {
		local, remote := a.addrs()
		if next := a.rekey(local, local, remote, remote); next != nil {
			c = next
		}
	}
	a.attachTo(c, now, inFlight)
}

func (a *PathAttachment) OnDemand(d quiccongestion.Demand) {
	c := a.ctl.Load()
	c.mu.Lock()
	wakes := c.setDemand(a, d)
	c.mu.Unlock()
	runWakes(wakes)
}

func (a *PathAttachment) Admit(now monotime.Time, maxBytes quiccongestion.ByteCount) quiccongestion.Permit {
	c := a.ctl.Load()
	c.mu.Lock()
	p, wakes := c.admit(a, now, maxBytes)
	c.mu.Unlock()
	runWakes(wakes)
	return p
}

func (a *PathAttachment) OnSent(s quiccongestion.SentPacket) {
	c := a.ctl.Load()
	c.mu.Lock()
	wakes := c.sent(a, s)
	c.mu.Unlock()
	runWakes(wakes)
}

func (a *PathAttachment) OnAckEvent(ev quiccongestion.AckEvent) {
	c := a.ctl.Load()
	c.mu.Lock()
	wakes := c.ackEvent(a, ev)
	c.mu.Unlock()
	runWakes(wakes)
}

func (a *PathAttachment) OnDiscarded(now monotime.Time, inFlight quiccongestion.ByteCount, refs []quiccongestion.PacketRef, _ bool, _ quiccongestion.DiscardReason) {
	c := a.ctl.Load()
	c.mu.Lock()
	wakes := c.discarded(a, now, inFlight, refs)
	c.mu.Unlock()
	runWakes(wakes)
}

// OnPathMigrated keeps the attachment unless the path itself changed; then
// the connection moves, with its flight, to the new path's controller.
func (a *PathAttachment) OnPathMigrated(now monotime.Time, oldLocal, newLocal, oldRemote, newRemote net.Addr) {
	if a.rekey == nil || !a.attached {
		return
	}
	next := a.rekey(oldLocal, newLocal, oldRemote, newRemote)
	old := a.ctl.Load()
	if next == nil || next == old {
		return
	}
	old.mu.Lock()
	gauge := a.gauge
	wakes := old.detach(a, now)
	old.mu.Unlock()
	runWakes(wakes)
	a.attachTo(next, now, gauge)
}

func (a *PathAttachment) OnDetach(now monotime.Time) {
	if !a.attached {
		return
	}
	a.attached = false
	c := a.ctl.Load()
	c.mu.Lock()
	wakes := c.detach(a, now)
	c.mu.Unlock()
	runWakes(wakes)
}

// SetProbing sets the attachment's state from its endpoint's own evidence of
// transport progress (§4.4): PROBING takes its flight out of the sum and
// stops its packets being samples; COUNTED returns its current gauge.
func (a *PathAttachment) SetProbing(on bool) {
	c := a.ctl.Load()
	c.mu.Lock()
	var wakes []func()
	if on {
		before := c.flight
		c.demote(a)
		if c.flight < before {
			wakes = c.collectBlocked(a, nil)
		}
	} else {
		c.promote(a)
	}
	c.mu.Unlock()
	runWakes(wakes)
}

// The CongestionControl methods a connection still calls on a PathSender.

func (a *PathAttachment) SetRTTStatsProvider(provider quiccongestion.RTTStatsProvider) {
	a.connRTT = provider
	c := a.ctl.Load()
	c.mu.Lock()
	if c.rtt.fallback == nil {
		c.rtt.fallback = provider
	}
	c.mu.Unlock()
}

func (a *PathAttachment) SetMaxDatagramSize(size quiccongestion.ByteCount) {
	a.maxPacket = size
	c := a.ctl.Load()
	c.mu.Lock()
	c.notePacketSize(size)
	c.mu.Unlock()
}

func (a *PathAttachment) GetCongestionWindow() quiccongestion.ByteCount {
	c := a.ctl.Load()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.law.GetCongestionWindow()
}

func (a *PathAttachment) InSlowStart() bool {
	c := a.ctl.Load()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.law.InSlowStart()
}

func (a *PathAttachment) InRecovery() bool {
	c := a.ctl.Load()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.law.InRecovery()
}

// OnRetransmissionTimeout is one connection's probe timeout, which says
// nothing about the path; the law ignores it anyway.
func (a *PathAttachment) OnRetransmissionTimeout(bool) {}

// The per-packet methods are not called on a PathSender. They answer from
// the path's state and register nothing.

func (a *PathAttachment) TimeUntilSend(quiccongestion.ByteCount) monotime.Time {
	c := a.ctl.Load()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.law.pacer.timeUntilSend()
}

func (a *PathAttachment) HasPacingBudget(now monotime.Time) bool {
	c := a.ctl.Load()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.law.pacer.budget(now) >= c.law.inner.maxDatagramSize
}

func (a *PathAttachment) CanSend(quiccongestion.ByteCount) bool {
	c := a.ctl.Load()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flight < c.law.GetCongestionWindow()
}

func (a *PathAttachment) OnPacketSent(monotime.Time, quiccongestion.ByteCount, quiccongestion.PacketNumber, quiccongestion.ByteCount, bool) {
}
func (a *PathAttachment) MaybeExitSlowStart() {}
func (a *PathAttachment) OnPacketAcked(quiccongestion.PacketNumber, quiccongestion.ByteCount, quiccongestion.ByteCount, monotime.Time) {
}
func (a *PathAttachment) OnCongestionEvent(quiccongestion.PacketNumber, quiccongestion.ByteCount, quiccongestion.ByteCount) {
}

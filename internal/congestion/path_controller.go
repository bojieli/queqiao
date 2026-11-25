package congestion

import (
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	quiccongestion "github.com/apernet/quic-go/congestion"
	"github.com/apernet/quic-go/monotime"

	"github.com/bojieli/queqiao/internal/lossmodel"
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
	// byClass holds, per class, the attachments with demand in it: the only
	// ones that can hold a claim on that class's turn, so admission visits
	// them and never the idle ones. windowWaiters, pacingWaiters and
	// deferredWaiters are the attachments refused for the window, the
	// balance and a claim, to wake when that may have cleared.
	byClass                                       [quiccongestion.NumSendClasses]map[*PathAttachment]struct{}
	windowWaiters, pacingWaiters, deferredWaiters map[*PathAttachment]struct{}
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
	// noDemandSince is when the path last ran out of eligible demand, zero
	// while some attachment has demand. An interval without demand, at
	// least a packet time of it with the window and the balance open, is an
	// unused admission opportunity (§4.2).
	noDemandSince monotime.Time
	// unusedTime is how much of the time since noDemandSince the path could
	// have admitted a packet -- window open and balance covering one --
	// accrued up to accruedAt. Time window-blocked or repaying debt is not
	// unused.
	unusedTime time.Duration
	accruedAt  monotime.Time
	// clock reads the time of a demand change, which carries none. It is
	// monotime.Now; a simulation substitutes its own.
	clock func() monotime.Time
	// unpublished says the estimator observed fates since its figures were
	// last published.
	unpublished bool
	// flightGauge mirrors flight for telemetry readers.
	flightGauge atomic.Int64
	// datagrams is the loss estimator over datagram-bearing samples only,
	// for the coded path's parity (§9); the law's is path-wide.
	// datagramBurst publishes its burst factor (math.Float64bits), zero
	// until datagramsSeen is enough to say.
	datagrams     *lossmodel.Estimator
	datagramBurst atomic.Uint64

	// Scratch for the law's batches.
	acked []quiccongestion.AckedPacketInfo
	lost  []quiccongestion.LostPacketInfo

	stats pathControllerCounters
	// lastDetach is when the last attachment detached, for a registry
	// deciding how long the path's knowledge stands.
	attached   atomic.Int64
	lastDetach atomic.Int64 // UnixNano
	created    time.Time
	// id names the controller in telemetry (ControllerTelemetry.Authority).
	id uint64
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
	// datagram says the packet carried a DATAGRAM frame.
	datagram bool
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
	lapsedClaims, debtPermits, controlBypasses    atomic.Uint64
	// publications counts measurements published outside an ACK event.
	publications          atomic.Uint64
	demotions, promotions atomic.Uint64
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
		law:             law,
		arb:             NewClassArbiter(weights, law.inner.maxDatagramSize),
		atts:            make(map[*PathAttachment]struct{}),
		windowWaiters:   make(map[*PathAttachment]struct{}),
		pacingWaiters:   make(map[*PathAttachment]struct{}),
		deferredWaiters: make(map[*PathAttachment]struct{}),
		nextPseq:        1,
		ringBase:        1,
		maxPacket:       law.inner.maxDatagramSize,
		created:         time.Now(),
		id:              pathControllerIDs.Add(1),
		clock:           monotime.Now,
		datagrams:       lossmodel.New(lossmodel.Config{}),
	}
	for cl := range c.byClass {
		c.byClass[cl] = make(map[*PathAttachment]struct{})
	}
	law.SetRTTStatsProvider(&c.rtt)
	law.inner.appLimitedBy = c.appLimited
	return c
}

// unused says whether the path, without demand since noDemandSince, has had
// an admission opportunity of at least one packet time by now: the window
// open for a packet, and the balance accrued over that time.
func (c *PathController) unused() bool {
	return !c.noDemandSince.IsZero() && c.unusedTime >= c.serialTime(c.maxPacket)
}

// accrue adds to unusedTime the part of [accruedAt, now] in which, with no
// demand, an ordinary admission would have succeeded. The window and flight
// change only at events, so they are as they were at accruedAt; the balance
// refills continuously and covers a packet from a computable time. It is
// called at every event before the event changes anything.
func (c *PathController) accrue(now monotime.Time) {
	if c.noDemandSince.IsZero() {
		c.accruedAt = now
		return
	}
	from := c.accruedAt
	if from.Before(c.noDemandSince) {
		from = c.noDemandSince
	}
	if now.After(from) && c.flight+c.maxPacket <= c.law.GetCongestionWindow() {
		start := from
		if fit := c.law.pacer.budgetReachedAt(c.maxPacket); !fit.IsZero() && fit.After(start) {
			start = fit
		}
		if now.After(start) {
			c.unusedTime += now.Sub(start)
		}
	}
	if now.After(c.accruedAt) {
		c.accruedAt = now
	}
}

// appLimited is the law's application-limited marker under a path
// controller: an unused opportunity in progress. One that ended when demand
// resumed was marked then (noteDemand). A sender that always has eligible demand -- held back only by
// the path's window, its pacer or a sibling's turn -- is never marked; one
// whose queue empties only between writes for less than a packet time is
// not either. A sender blocked on peer flow control or a full send queue
// has no eligible demand, and is.
func (c *PathController) appLimited() bool {
	return c.unused()
}

var pathControllerIDs atomic.Uint64

// pathDatagramEvidence is how many transitions between adjacent
// datagram-bearing samples must have been observed before their own burst
// statistic replaces the path-wide one. Datagram packets that never sit next
// to each other in the send order give none, however many there are.
const pathDatagramEvidence = 200

// DatagramBurstFactor is the burst factor of the loss of datagram-bearing
// packets, measured over adjacent datagram-bearing samples, and false until
// enough of them have resolved. The coded path sizes its parity from it: on
// a path shared with stream traffic, the path-wide statistic is dominated by
// packets whose loss pattern is not the datagrams'.
func (c *PathController) DatagramBurstFactor() (float64, bool) {
	bits := c.datagramBurst.Load()
	if bits == 0 {
		return 0, false
	}
	return math.Float64frombits(bits), true
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
	t.Authority = c.id
	// The counted flight, not the sampler's last view of it.
	t.BytesInFlight = uint64(max(c.flightGauge.Load(), 0))
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
	// AppSamples and NonAppSamples are the sampler's delivery samples by
	// whether they were application-limited.
	AppSamples, NonAppSamples uint64
	Demotions, Promotions     uint64
	// Publications counts measurements published outside an ACK event.
	Publications uint64
	Classes      ClassArbiterStats
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
		Publications: c.stats.publications.Load(),
		Classes:      c.arb.Stats(),
		AppSamples:   c.law.inner.estimator.appSamples, NonAppSamples: c.law.inner.estimator.nonAppSamples,
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

// correct flips a lost record the estimator has not yet observed to
// delivered, moving it between the fate counts with it.
func (c *PathController) correct(r *fateRecord) {
	r.fate = fateDelivered
	c.stats.lost.Add(^uint64(0))
	c.stats.delivered.Add(1)
}

// publishObserved publishes the estimator's figures if it observed fates
// outside an acknowledgement event -- a discard or a detach that unblocked
// the send order -- so the erasure the coded readers size from is current
// without waiting for the next ACK.
//
// The balance is settled at the old rate first, as an ACK event does, and
// siblings refused for the balance are woken if the rate rose.
func (c *PathController) publishObserved(a *PathAttachment, now monotime.Time, wakes []func()) []func() {
	if !c.unpublished {
		return wakes
	}
	c.unpublished = false
	c.stats.publications.Add(1)
	c.law.pacer.settle(now)
	rateBefore, windowBefore := c.law.bandwidth(), c.law.GetCongestionWindow()
	c.law.publishMeasurement()
	if c.law.bandwidth() > rateBefore {
		wakes = c.collectPacing(a, wakes)
	}
	if c.law.GetCongestionWindow() > windowBefore {
		wakes = c.collectBlocked(a, wakes)
	}
	return wakes
}

// collectPacing returns the wakers of attachments refused for the balance,
// except a, and clears their flag: the rate rose, so their retry is late.
func (c *PathController) collectPacing(a *PathAttachment, wakes []func()) []func() {
	for o := range c.pacingWaiters {
		if o != a && o.wake != nil {
			c.markPacing(o, false)
			wakes = append(wakes, o.wake)
		}
	}
	return wakes
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
			c.unpublished = true
		case fateLost:
			c.law.estimator.ObserveOutcome(false)
			c.unpublished = true
		default:
			c.law.estimator.Break()
		}
		// The datagram statistic (§4.2) counts a transition only between
		// adjacent records that both carried a datagram: anything else
		// between them breaks the run.
		switch {
		case !r.datagram || (r.fate != fateDelivered && r.fate != fateLost):
			c.datagrams.Break()
		default:
			c.datagrams.ObserveOutcome(r.fate == fateDelivered)
		}
		c.ring[0] = fateRecord{}
		c.ring = c.ring[1:]
		c.ringBase++
	}
	// The evidence decays with the statistic: transitions the estimator
	// still weighs, not every pair ever seen. Below the threshold the
	// override is withdrawn and the path-wide figure stands.
	if c.datagrams.Transitions() >= pathDatagramEvidence {
		b := c.datagrams.Snapshot().BurstFactor
		if b < 1 {
			b = 1
		}
		c.datagramBurst.Store(math.Float64bits(b))
	} else {
		c.datagramBurst.Store(0)
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
		c.flightGauge.Store(int64(c.flight))
	}
	a.gauge = inFlight
	a.gaugeView.Store(int64(inFlight))
}

// noteDemand tracks intervals in which the path has no eligible demand.
// Only ordinary work counts: EXEMPT obligations (a due ACK, a path probe)
// are not demand for a permit, and must not interrupt an idle interval.
func (c *PathController) noteDemand(union quiccongestion.Demand, now monotime.Time) {
	any := false
	for _, cl := range scheduledClasses {
		any = any || union.Eligible[cl]
	}
	c.accrue(now)
	switch {
	case any && !c.noDemandSince.IsZero():
		if c.unused() {
			// Marked now, before the first packet of the resumed burst is
			// registered: the sampler takes a packet's marker at send, and
			// on a drained path no ACK may come between. Marked once: the
			// opportunity is consumed here, so the burst's first ACK does
			// not mark it again and extend the phase over the burst.
			c.law.inner.estimator.markAppLimited()
		}
		c.noDemandSince, c.unusedTime = 0, 0
	case !any && c.noDemandSince.IsZero():
		c.noDemandSince, c.accruedAt, c.unusedTime = now, now, 0
	}
}

func markIn(set map[*PathAttachment]struct{}, a *PathAttachment, on bool) {
	if on {
		set[a] = struct{}{}
	} else {
		delete(set, a)
	}
}

func (c *PathController) markWindow(a *PathAttachment, on bool) {
	a.windowBlocked = on
	markIn(c.windowWaiters, a, on)
}

func (c *PathController) markPacing(a *PathAttachment, on bool) {
	a.pacingBlocked = on
	markIn(c.pacingWaiters, a, on)
}

func (c *PathController) markDeferred(a *PathAttachment, on bool) {
	a.deferred = on
	markIn(c.deferredWaiters, a, on)
}

// demand is the union of the attachments' demand.
func (c *PathController) demand() quiccongestion.Demand {
	var d quiccongestion.Demand
	for cl := range c.byClass {
		d.Eligible[cl] = len(c.byClass[cl]) > 0
	}
	return d
}

// collectBlocked returns the wakers of attachments refused for the window,
// except a, and clears their flag: flight fell or the window grew.
func (c *PathController) collectBlocked(a *PathAttachment, wakes []func()) []func() {
	for o := range c.windowWaiters {
		if o != a && o.wake != nil {
			c.markWindow(o, false)
			wakes = append(wakes, o.wake)
		}
	}
	return wakes
}

// collectWaiting returns the wakers of attachments refused for precedence,
// except a: the claim they waited on may have cleared.
func (c *PathController) collectWaiting(a *PathAttachment, wakes []func()) []func() {
	for o := range c.deferredWaiters {
		if o != a && o.wake != nil {
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
	// Time so far is judged against the flight before this attachment
	// joins it.
	c.accrue(c.clamp(now))
	c.atts[a] = struct{}{}
	// Nothing of its standing on another controller carries over.
	a.bypassed = false
	for cl, e := range a.demand.Eligible {
		if e {
			c.byClass[cl][a] = struct{}{}
		}
	}
	c.attached.Store(int64(len(c.atts)))
	a.probing = false
	a.gauge = 0
	c.setGauge(a, inFlight)
	c.notePacketSize(maxPacket)
	a.ptoSinceProgress = 0
	union := c.demand()
	c.noteDemand(union, c.now)
	c.arb.OnDemand(union)
}

// detach takes a out of the controller: its gauge leaves the sum and its
// unresolved records are discarded.
func (c *PathController) detach(a *PathAttachment, now monotime.Time) []func() {
	if _, ok := c.atts[a]; !ok {
		return nil
	}
	now = c.clamp(now)
	// Time so far is judged against the flight before this attachment
	// leaves it.
	c.accrue(now)
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
		c.flightGauge.Store(int64(c.flight))
	}
	a.gauge = 0
	a.gaugeView.Store(0)
	c.markWindow(a, false)
	c.markPacing(a, false)
	c.markDeferred(a, false)
	for cl := range c.byClass {
		delete(c.byClass[cl], a)
	}
	a.claimDeadline = 0
	a.waiting, a.unable, a.bypassed = false, false, false
	delete(c.atts, a)
	if len(c.atts) == 0 {
		// Published before the count, so a sweep that sees no attachment
		// reads this detach's time.
		c.lastDetach.Store(time.Now().UnixNano())
	}
	c.attached.Store(int64(len(c.atts)))
	union := c.demand()
	c.noteDemand(union, now)
	c.arb.OnDemand(union)
	c.advance(now)
	wakes := c.publishObserved(a, now, nil)
	wakes = c.collectBlocked(a, wakes)
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
// packetTime is how long the path's rate takes to send n bytes, floored at
// the pacing timer's granularity: it times a wake-up (a claim's deadline).
func (c *PathController) packetTime(n quiccongestion.ByteCount) time.Duration {
	return max(c.serialTime(n), quiccongestion.MinPacingDelay)
}

// serialTime is how long the path's rate takes to send n bytes, unfloored:
// what an unused send opportunity is measured in. At 100 Mbit/s a full
// packet takes about 96 µs, a tenth of the timer's floor.
func (c *PathController) serialTime(n quiccongestion.ByteCount) time.Duration {
	rate := c.law.bandwidth()
	if rate <= 0 {
		return time.Millisecond
	}
	return time.Duration(float64(n) / float64(rate) * float64(time.Second))
}

func (c *PathController) admit(a *PathAttachment, now monotime.Time, maxBytes quiccongestion.ByteCount) (quiccongestion.Permit, []func()) {
	now = c.clamp(now)
	c.accrue(now)
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
	//
	// A class whose turn this attachment could not build (OnEmptyBuild) is
	// passed over for it as if it had no demand in it, for a short hold or
	// until something changes; once every class it has is passed over it is
	// refused until the earliest hold ends.
	if !hasDemand(a.demand) {
		// No ordinary work: the connection asks from its send loop all the
		// same. It takes no turn, defers to no one and registers no
		// waiter or timer; whatever the demand query missed may go as
		// CONTROL if the path has room for it now, and otherwise only
		// exempt obligations are sent.
		if !a.probing && c.flight+maxBytes <= c.law.GetCongestionWindow() && c.law.pacer.budget(now) >= maxBytes {
			return quiccongestion.Permit{OK: true, Turn: quiccongestion.ClassControl, MaxBytes: maxBytes}, wakes
		}
		return quiccongestion.Permit{}, wakes
	}
	own := a.usable(now)
	if !hasDemand(quiccongestion.Demand{Eligible: own}) {
		return quiccongestion.Permit{RetryAt: a.earliestUnstick(now)}, wakes
	}
	turn := c.arb.Select()
	var bypassed []*PathAttachment
	if turn != quiccongestion.ClassExempt && !own[turn] && !c.claimed(a, turn, now) {
		for o := range c.byClass[turn] {
			if o != a && o.usable(now)[turn] {
				// Another attachment has demand in the turn but no live
				// claim: its claim lapsed.
				c.stats.lapsedClaims.Add(1)
				break
			}
		}
		turn = c.arb.SelectAmong(own)
	} else if bypassed = c.controlBypass(a, own, turn, now, maxBytes); len(bypassed) > 0 {
		// CONTROL goes past a live claim its holders cannot use now; the
		// deficits are left as they are, and nothing else rides on it.
		turn = quiccongestion.ClassControl
	}
	if len(bypassed) == 0 && turn != quiccongestion.ClassExempt && !own[turn] && c.claimantReady(a, turn, now) {
		// A claimant can send the path's turn now with its own packet, even
		// if this attachment's would not fit: that is a deferral to it --
		// it is woken and has a packet time -- not a window refusal.
		return c.deferTo(a, turn, now, maxBytes, wakes)
	}
	// The window and the balance, for the turn the path would serve now.
	borrowed, refusal := c.admissible(a, now, turn, maxBytes)
	if refusal != nil {
		a.waiting = true
		// It asked and the path had no opportunity for it: its own claim,
		// if anyone deferred to it, is renewed rather than spent. A
		// claimant that is admitted keeps its deadline until it sends.
		if !a.unable {
			a.claimDeadline = 0
		}
		return *refusal, wakes
	}
	if c.claimed(a, turn, now) {
		return c.deferTo(a, turn, now, maxBytes, wakes)
	}
	if !own[turn] && turn != quiccongestion.ClassExempt {
		// Every claim on the path's turn lapsed: this attachment is served
		// from its own demand, and the class keeps its deficit.
		turn = c.arb.SelectAmong(own)
		if borrowed, refusal = c.admissible(a, now, turn, maxBytes); refusal != nil {
			a.waiting = true
			return *refusal, wakes
		}
	}
	if borrowed {
		c.stats.debtPermits.Add(1)
	}
	c.markWindow(a, false)
	c.markPacing(a, false)
	a.waiting = false
	c.markDeferred(a, false)
	c.permitSeq++
	a.lastPermit = c.permitSeq
	// Admitted: a bypass of its claim has been followed by its opportunity.
	a.bypassed = false
	if len(bypassed) > 0 {
		c.commitBypass(bypassed)
	}
	p := c.arb.PermitTurn(turn, maxBytes, !borrowed && len(bypassed) == 0)
	for cl := range p.Fill {
		if !own[cl] {
			// Nothing of a class this attachment cannot build fills it.
			p.Fill[cl] = 0
		}
	}
	return p, wakes
}

// usable is the attachment's demand less the classes it is passed over in.
func (a *PathAttachment) usable(now monotime.Time) [quiccongestion.NumSendClasses]bool {
	own := a.demand.Eligible
	for cl, until := range a.stuckUntil {
		if !until.IsZero() && now.Before(until) {
			own[cl] = false
		}
	}
	return own
}

// earliestUnstick is the earliest future hold among the classes with demand
// that are blocking admission now.
func (a *PathAttachment) earliestUnstick(now monotime.Time) monotime.Time {
	var at monotime.Time
	for _, cl := range scheduledClasses {
		until := a.stuckUntil[cl]
		if !a.demand.Eligible[cl] || until.IsZero() || !now.Before(until) {
			continue
		}
		if at.IsZero() || until.Before(at) {
			at = until
		}
	}
	return at
}

func hasDemand(d quiccongestion.Demand) bool {
	for _, cl := range scheduledClasses {
		if d.Eligible[cl] {
			return true
		}
	}
	return false
}

// claims says whether o holds a claim on turn against a.
func (c *PathController) claims(o, a *PathAttachment, turn quiccongestion.SendClass, now monotime.Time) bool {
	if o == a || !o.usable(now)[turn] || o.unable {
		return false
	}
	if !o.claimDeadline.IsZero() && !now.Before(o.claimDeadline) {
		return false // lapsed: it was given a packet time and did not ask
	}
	if !a.usable(now)[turn] {
		return true
	}
	return o.waiting && o.lastPermit < a.lastPermit
}

// claimed says whether any attachment holds a claim on turn against a.
func (c *PathController) claimed(a *PathAttachment, turn quiccongestion.SendClass, now monotime.Time) bool {
	if turn == quiccongestion.ClassExempt {
		return false
	}
	for o := range c.byClass[turn] {
		if c.claims(o, a, turn, now) {
			return true
		}
	}
	return false
}

// admissible checks the window, against the path's flight or a PROBING
// attachment's own, and the balance, for a packet of the given turn. A
// CONTROL turn may borrow the control debt; borrowed says it did.
// controlBypass decides whether a's CONTROL work may go past the live claim
// of the path's turn, and records it if so. Two properties, no more:
//
//   - CONTROL is not held behind claimants that cannot use the turn now:
//     none of them could send a packet of its own size, against its own
//     window (a PROBING claimant's probe window) and the balance, while
//     CONTROL fits on the control debt. A claimant whose ability is unknown
//     keeps its claim.
//   - A live claim is not starved: each claimant is bypassed at most once
//     until it is next admitted, so CONTROL then waits for the window like
//     anyone else and the claimant gets the opening it was waiting for. A
//     dead claimant -- one that never asks again -- costs CONTROL that one
//     drain: with the window open, its claim lapses after a packet time
//     like any other unused claim.
//
// It decides only; the bypass is charged (commitBypass) when the permit is
// actually granted.
func (c *PathController) controlBypass(a *PathAttachment, own [quiccongestion.NumSendClasses]bool, turn quiccongestion.SendClass, now monotime.Time, maxBytes quiccongestion.ByteCount) []*PathAttachment {
	if turn == quiccongestion.ClassExempt || turn == quiccongestion.ClassControl || own[turn] || !own[quiccongestion.ClassControl] {
		return nil
	}
	if !c.fits(now, quiccongestion.ClassControl, maxBytes) {
		return nil
	}
	var claimants []*PathAttachment
	for o := range c.byClass[turn] {
		if !c.claims(o, a, turn, now) {
			continue
		}
		if o.bypassed || o.maxPacket <= 0 || c.claimantCanSend(o, now, turn) {
			return nil
		}
		claimants = append(claimants, o)
	}
	return claimants
}

// commitBypass charges a granted bypass to the claimants it went past.
func (c *PathController) commitBypass(claimants []*PathAttachment) {
	for _, o := range claimants {
		o.bypassed = true
	}
	c.stats.controlBypasses.Add(1)
}

// deferTo refuses a for the claim others hold on turn: each claimant is
// woken and its claim deadline started, and a retries at the earliest one.
func (c *PathController) deferTo(a *PathAttachment, turn quiccongestion.SendClass, now monotime.Time, maxBytes quiccongestion.ByteCount, wakes []func()) (quiccongestion.Permit, []func()) {
	var retry monotime.Time
	for o := range c.byClass[turn] {
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
	a.waiting = true
	c.markDeferred(a, true)
	a.claimDeadline = 0
	return quiccongestion.Permit{RetryAt: retry}, wakes
}

// claimantReady says whether some claimant of turn against a could send
// its own packet now.
func (c *PathController) claimantReady(a *PathAttachment, turn quiccongestion.SendClass, now monotime.Time) bool {
	for o := range c.byClass[turn] {
		if c.claims(o, a, turn, now) && o.maxPacket > 0 && c.claimantCanSend(o, now, turn) {
			return true
		}
	}
	return false
}

// claimantCanSend says whether o could send a packet of its own size of the
// turn now, with the allowance admission would give it: its window -- the
// path's, or a PROBING attachment's own probe window -- and the balance,
// both less the control debt on a CONTROL turn.
func (c *PathController) claimantCanSend(o *PathAttachment, now monotime.Time, turn quiccongestion.SendClass) bool {
	size := o.maxPacket
	debt := quiccongestion.ByteCount(0)
	if turn == quiccongestion.ClassControl {
		debt = pathControlDebtPackets * c.maxPacket
	}
	if o.probing {
		if o.gauge+size > pathProbeWindowPackets*c.maxPacket {
			return false
		}
	} else if c.flight+size > c.law.GetCongestionWindow()+debt {
		return false
	}
	return c.law.pacer.budget(now) >= size-debt
}

// fits says whether an ordinary packet of the given turn would pass the
// path's window and balance now, as admissible decides for a COUNTED
// attachment, without recording anything.
func (c *PathController) fits(now monotime.Time, turn quiccongestion.SendClass, maxBytes quiccongestion.ByteCount) bool {
	debt := quiccongestion.ByteCount(0)
	if turn == quiccongestion.ClassControl {
		debt = pathControlDebtPackets * c.maxPacket
	}
	if c.flight+maxBytes > c.law.GetCongestionWindow()+debt {
		return false
	}
	return c.law.pacer.budget(now) >= maxBytes-debt
}

func (c *PathController) admissible(a *PathAttachment, now monotime.Time, turn quiccongestion.SendClass, maxBytes quiccongestion.ByteCount) (bool, *quiccongestion.Permit) {
	debt := quiccongestion.ByteCount(0)
	if turn == quiccongestion.ClassControl {
		debt = pathControlDebtPackets * c.maxPacket
	}
	borrowed := false
	window := c.law.GetCongestionWindow()
	c.law.recordInFlight(c.flight)
	// The packet must fit: refused if the flight after it would exceed the
	// window (§4.3), or, on a CONTROL turn, the window plus the debt.
	if a.probing {
		if a.gauge+maxBytes > pathProbeWindowPackets*c.maxPacket {
			c.markWindow(a, true)
			a.unable = true
			c.stats.refusedWindow.Add(1)
			return false, &quiccongestion.Permit{}
		}
	} else if c.flight+maxBytes > window {
		if c.flight+maxBytes > window+debt {
			c.markWindow(a, true)
			c.stats.refusedWindow.Add(1)
			return false, &quiccongestion.Permit{}
		}
		borrowed = true
	}
	// The balance must cover this packet, of the size the asking
	// connection builds.
	if budget := c.law.pacer.budget(now); budget < maxBytes {
		if budget < maxBytes-debt {
			c.stats.refusedPacing.Add(1)
			c.markPacing(a, true)
			retry := c.law.pacer.timeUntilBudget(maxBytes - debt)
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
	c.accrue(now)
	if a.staleRTT && a.movedFrom < 0 && s.Space == quiccongestion.SpaceApplication {
		a.movedFrom = s.Number
	}
	var wakes []func()
	before := c.flight
	c.setGauge(a, s.InFlight)
	c.arb.OnSent(s.ClassBytes)
	c.sendSeq++
	a.lastSentSeq = c.sendSeq
	a.unable = false
	// Something was built: work that did not fit may now.
	a.stuckUntil = [quiccongestion.NumSendClasses]monotime.Time{}
	a.claimDeadline = 0
	if s.Exempt {
		c.stats.exemptBytes.Add(byteTotal(s.Bytes))
	}
	if s.Kind == quiccongestion.KindPTOProbe && a.progress == nil {
		// Without the transport's own progress export, PTO probes sent
		// since the last acknowledgement stand in for its expiry count.
		a.ptoSinceProgress++
		if !a.probing && a.ptoSinceProgress >= pathProbingPTOProbes {
			wakes = append(wakes, c.demote(a)...)
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
		c.ring = append(c.ring, fateRecord{att: a, key: key, sent: now, datagram: s.HasDatagram})
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
		a.bankNonEliciting(byteTotal(s.Bytes))
	}
	c.advance(now)
	return wakes
}

// demote makes a PROBING: its flight leaves the sum.
//
// Its discards can release siblings' resolved outcomes held behind its
// records; those are observed and published now, and the wakers of siblings
// a rate or window rise frees are returned.
func (c *PathController) demote(a *PathAttachment) []func() {
	if a.probing {
		return nil
	}
	c.accrue(c.now)
	c.flight -= a.gauge
	c.flightGauge.Store(int64(c.flight))
	a.probing = true
	c.stats.demotions.Add(1)
	// Its sample packets still in flight are not measurements of the path
	// any more than its next ones are.
	for key, ref := range a.pkts {
		if !ref.lost {
			c.discard(a, key)
		}
	}
	// Demotion can be found from transport progress before the callback's
	// own time is clamped in: read the clock, so a publication settles the
	// balance at the demotion's time, not the previous event's.
	now := c.clamp(c.clock())
	c.advance(now)
	return c.publishObserved(a, now, nil)
}

// promote makes a COUNTED again with its current gauge.
func (c *PathController) promote(a *PathAttachment) {
	if !a.probing {
		return
	}
	c.accrue(c.now)
	a.probing = false
	c.flight += a.gauge
	c.flightGauge.Store(int64(c.flight))
	c.stats.promotions.Add(1)
}

func (c *PathController) ackEvent(a *PathAttachment, ev quiccongestion.AckEvent) []func() {
	now := c.clamp(ev.Time)
	// What the path could have sent until now is judged on the state before
	// this event changes it, and the balance is settled at the rate it was
	// earned at before the event moves the rate.
	c.accrue(now)
	c.law.pacer.settle(now)
	// Deadlines are enforced before outcomes are accepted, so an
	// acknowledgement past the horizon is not credited merely because no
	// other callback came first.
	c.advance(now)
	before := c.flight
	windowBefore := c.law.GetCongestionWindow()
	rateBefore := c.law.bandwidth()
	c.setGauge(a, ev.InFlight)
	// The flight and the window moved: work that did not fit may now.
	a.stuckUntil = [quiccongestion.NumSendClasses]monotime.Time{}
	c.acked, c.lost = c.acked[:0], c.lost[:0]
	var resolved quiccongestion.ByteCount
	// largestOwn says the largest application packet this event newly
	// acknowledges -- the one its RTT sample times -- was sent after the
	// move. Ownership is by packet number, not by the fate map, whose
	// records can expire before a long round trip's ACK arrives; and it is
	// worked out only after a move.
	largestOwn := false
	if a.staleRTT && ev.RTTSample > 0 && a.movedFrom >= 0 {
		largestPN := quiccongestion.PacketNumber(-1)
		for _, r := range ev.Acked {
			if r.Space == quiccongestion.SpaceApplication && r.Number > largestPN {
				largestPN = r.Number
			}
		}
		largestOwn = largestPN >= a.movedFrom
	}
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
			c.correct(rec)
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
			c.correct(rec)
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
	// After a move to another controller, an RTT sample may time a packet
	// sent on the old uplink. The path's delay model takes a sample only if
	// it times one of this attachment's own packets on this controller --
	// the sample is the largest newly acknowledged packet's -- and the
	// boundary stays: packets on both sides of a move can share a send
	// time, so a reordered ACK of an old one can still carry a sample after
	// a new one has been timed.
	if ev.RTTSample > 0 && (!a.staleRTT || largestOwn) {
		// The event's sample is raw: send to ACK receipt. The connection's
		// own statistics have already removed the peer's reported ACK
		// delay, so their latest sample is what the path's delay model
		// takes; a delayed ACK is not queueing.
		sample := ev.RTTSample
		if a.connRTT != nil {
			if latest := a.connRTT.LatestRTT(); latest > 0 {
				sample = latest
			}
		}
		c.rtt.observe(sample)
	}
	if progress && a.progress == nil {
		a.ptoSinceProgress = 0
		c.promote(a)
	}
	c.advance(now)
	c.law.recordInFlight(c.flight)
	if len(c.acked) > 0 || len(c.lost) > 0 {
		// The law's flight after the event is the path's: its prior is
		// that plus what this event resolved of its samples.
		before := c.law.Telemetry()
		c.law.afterOutcomes(c.flight+resolved, now, c.acked, c.lost)
		c.unpublished = false
		// The event's movement of the cumulative counters is this
		// attachment's, and only its: a connection's published counters
		// must count each loss and sample of the path once.
		after := c.law.Telemetry()
		a.bankCounters(&after, &before)
	}
	var wakes []func()
	if c.flight < before || c.law.GetCongestionWindow() > windowBefore {
		wakes = c.collectBlocked(a, wakes)
	}
	if c.law.bandwidth() > rateBefore {
		// A higher rate moves every pacing refusal's retry earlier.
		for o := range c.pacingWaiters {
			if o != a && o.wake != nil {
				c.markPacing(o, false)
				wakes = append(wakes, o.wake)
			}
		}
	}
	return wakes
}

func (c *PathController) discarded(a *PathAttachment, now monotime.Time, inFlight quiccongestion.ByteCount, refs []quiccongestion.PacketRef) []func() {
	now = c.clamp(now)
	c.accrue(now)
	before := c.flight
	c.setGauge(a, inFlight)
	for _, r := range refs {
		c.discard(a, packetKey{space: r.Space, number: r.Number})
	}
	c.advance(now)
	wakes := c.publishObserved(a, now, nil)
	c.law.recordInFlight(c.flight)
	if c.flight < before {
		wakes = c.collectBlocked(a, wakes)
	}
	return wakes
}

func (c *PathController) setDemand(a *PathAttachment, d quiccongestion.Demand) []func() {
	a.demand = d
	if _, attached := c.atts[a]; attached {
		for cl, e := range d.Eligible {
			if e {
				c.byClass[cl][a] = struct{}{}
			} else {
				delete(c.byClass[cl], a)
			}
		}
	}
	union := c.demand()
	// Demand carries no time: it is read from the clock, without moving
	// the event clock.
	now := c.clock()
	if now.Before(c.now) {
		now = c.now
	}
	c.noteDemand(union, now)
	c.arb.OnDemand(union)
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
	// generation reads the endpoint's uplink generation; seenGeneration is
	// the one this attachment last resolved its key with. Run loop only.
	generation     func() uint64
	seenGeneration uint64
	// progress reads the connection's transport progress; progressEpoch is
	// the epoch last seen.
	progress      func() TransportProgress
	progressEpoch uint64
	// staleRTT says the attachment moved here from another controller:
	// RTT samples count only if they time a packet numbered from movedFrom,
	// its first application packet on this controller (-1 until it sends
	// one). It stays for the attachment's life on this controller.
	staleRTT  bool
	movedFrom quiccongestion.PacketNumber
	// bypassed says CONTROL went past this attachment's claim and it has
	// not been admitted since: no further bypass until it is.
	bypassed bool
	// stuckUntil is, per class, until when a turn this attachment could not
	// build is passed over for it (OnEmptyBuild).
	stuckUntil [quiccongestion.NumSendClasses]monotime.Time
	// pacingBlocked says its last refusal was for the balance.
	pacingBlocked bool
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

	// gaugeView mirrors gauge for telemetry readers.
	gaugeView atomic.Int64
	// counters are this attachment's share of the path's cumulative
	// counters, read from any goroutine.
	countersMu sync.Mutex
	counters   ControllerTelemetry

	// Run loop only.
	wake      func()
	attached  bool
	maxPacket quiccongestion.ByteCount
	connRTT   quiccongestion.RTTStatsProvider
}

var attachmentEpochs atomic.Uint64

var (
	_ quiccongestion.PathSender         = (*PathAttachment)(nil)
	_ quiccongestion.ClassScheduler     = (*PathAttachment)(nil)
	_ quiccongestion.EmptyBuildObserver = (*PathAttachment)(nil)
	_ TelemetryProvider                 = (*PathAttachment)(nil)
)

// Controller returns the controller the attachment is attached to.
func (a *PathAttachment) Controller() *PathController { return a.ctl.Load() }

// SchedulesClasses is true: a path controller always arbitrates classes, so
// the connection's builder executes its permits.
func (a *PathAttachment) SchedulesClasses() bool { return true }

// Telemetry is the path's.
// Its gauges and estimates are the path's; its cumulative counters are the
// attachment's own share of the path's, so connections on one path that
// each publish their counters count every loss and sample once.
func (a *PathAttachment) Telemetry() ControllerTelemetry {
	t := a.ctl.Load().Telemetry()
	a.countersMu.Lock()
	own := a.counters
	a.countersMu.Unlock()
	t.Samples, t.NonAppSamples, t.AppSamples = own.Samples, own.NonAppSamples, own.AppSamples
	t.StateMisses, t.ZeroSamples = own.StateMisses, own.ZeroSamples
	t.BytesLost, t.PacketsLost, t.PacketsLostObserved = own.BytesLost, own.PacketsLost, own.PacketsLostObserved
	t.NonElicitingBytes, t.NonElicitingDebited = own.NonElicitingBytes, own.NonElicitingDebited
	// Flight is the connection's own; the window, pacing rate and bandwidth
	// are the path's, undivided -- they are what this connection's flow
	// control may use, and the controller admits against them whatever the
	// number of attachments. Readers that sum them over connections count
	// each Authority once.
	t.BytesInFlight = uint64(max(a.gaugeView.Load(), 0))
	return t
}

// bankCounters adds the counters' movement from before to after.
func (a *PathAttachment) bankCounters(after, before *ControllerTelemetry) {
	d := func(x, y uint64) uint64 {
		if x < y {
			return 0
		}
		return x - y
	}
	a.countersMu.Lock()
	c := &a.counters
	c.Samples += d(after.Samples, before.Samples)
	c.NonAppSamples += d(after.NonAppSamples, before.NonAppSamples)
	c.AppSamples += d(after.AppSamples, before.AppSamples)
	c.StateMisses += d(after.StateMisses, before.StateMisses)
	c.ZeroSamples += d(after.ZeroSamples, before.ZeroSamples)
	c.BytesLost += d(after.BytesLost, before.BytesLost)
	c.PacketsLost += d(after.PacketsLost, before.PacketsLost)
	c.PacketsLostObserved += d(after.PacketsLostObserved, before.PacketsLostObserved)
	a.countersMu.Unlock()
}

func (a *PathAttachment) bankNonEliciting(bytes uint64) {
	a.countersMu.Lock()
	a.counters.NonElicitingBytes += bytes
	a.countersMu.Unlock()
}

// ClassStats returns the path's arbiter state.
func (a *PathAttachment) ClassStats() (ClassArbiterStats, bool) {
	return a.ctl.Load().arb.Stats(), true
}

func (a *PathAttachment) attachTo(c *PathController, now monotime.Time, inFlight quiccongestion.ByteCount) {
	a.ctl.Store(c)
	c.mu.Lock()
	a.epoch = attachmentEpochs.Add(1)
	if a.connRTT != nil && c.rtt.fallback == nil && !a.staleRTT {
		// A connection that moved here from another path does not seed
		// this one's round trip: its statistics are the old path's.
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
		if next := a.rekey(local, local, remote, remote); next != nil && next != c {
			// The path changed between choosing the controller and this
			// installation: the packets already in flight were sent on
			// the old one, as after a migration.
			c = next
			a.staleRTT, a.movedFrom = true, -1
		}
	}
	a.attachTo(c, now, inFlight)
}

func (a *PathAttachment) OnDemand(d quiccongestion.Demand) {
	tp := a.readProgress()
	c := a.ctl.Load()
	c.mu.Lock()
	early := c.applyProgress(a, tp)
	wakes := c.setDemand(a, d)
	wakes = append(wakes, early...)
	c.mu.Unlock()
	runWakes(wakes)
}

func (a *PathAttachment) Admit(now monotime.Time, maxBytes quiccongestion.ByteCount) quiccongestion.Permit {
	a.followGeneration(now)
	tp := a.readProgress()
	c := a.ctl.Load()
	c.mu.Lock()
	early := c.applyProgress(a, tp)
	p, wakes := c.admit(a, now, maxBytes)
	wakes = append(wakes, early...)
	c.mu.Unlock()
	runWakes(wakes)
	return p
}

func (a *PathAttachment) OnSent(s quiccongestion.SentPacket) {
	// A packet built without admission (a PTO probe) after an uplink change
	// is charged to the current path's controller too.
	a.followGeneration(s.Time)
	tp := a.readProgress()
	c := a.ctl.Load()
	c.mu.Lock()
	early := c.applyProgress(a, tp)
	wakes := c.sent(a, s)
	wakes = append(wakes, early...)
	c.mu.Unlock()
	runWakes(wakes)
}

func (a *PathAttachment) OnAckEvent(ev quiccongestion.AckEvent) {
	a.followGeneration(ev.Time)
	tp := a.readProgress()
	c := a.ctl.Load()
	c.mu.Lock()
	early := c.applyProgress(a, tp)
	wakes := c.ackEvent(a, ev)
	wakes = append(wakes, early...)
	c.mu.Unlock()
	runWakes(wakes)
}

func (a *PathAttachment) OnDiscarded(now monotime.Time, inFlight quiccongestion.ByteCount, refs []quiccongestion.PacketRef, _ bool, _ quiccongestion.DiscardReason) {
	tp := a.readProgress()
	c := a.ctl.Load()
	c.mu.Lock()
	early := c.applyProgress(a, tp)
	wakes := c.discarded(a, now, inFlight, refs)
	wakes = append(wakes, early...)
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
	// Its packets in flight were sent on the path it left.
	a.staleRTT, a.movedFrom = true, -1
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

// OnEmptyBuild is told that a packet admitted under a permit could not be
// built although the connection's demand is unchanged: the turn's work does
// not fit. That class is passed over for this attachment for a short hold or
// until it sends or is acknowledged, and the connection is woken to take
// another decision -- another class it has, or, once all are passed over, a
// refusal until the earliest hold ends.
func (a *PathAttachment) OnEmptyBuild(now monotime.Time, p quiccongestion.Permit) {
	cl := p.Turn
	if cl == quiccongestion.ClassExempt || int(cl) >= quiccongestion.NumSendClasses {
		return
	}
	c := a.ctl.Load()
	c.mu.Lock()
	now = c.clamp(now)
	if until := a.stuckUntil[cl]; !until.IsZero() && now.Before(until) {
		c.mu.Unlock()
		return
	}
	a.stuckUntil[cl] = now.Add(emptyBuildHold)
	c.mu.Unlock()
	if a.wake != nil {
		a.wake()
	}
}

// TransportProgress is a connection's published transport progress: an
// epoch that moves whenever an ack-eliciting application packet is newly
// acknowledged, and the probe timeouts that expired since it last moved.
type TransportProgress struct {
	Epoch       uint64
	PTOExpiries uint32
}

// pathProbingPTOExpiries is k_s (§10.2): the probe timeouts without transport
// progress after which an attachment is PROBING.
const pathProbingPTOExpiries = 2

// SetGeneration gives the attachment its endpoint's uplink generation. The
// generation is part of the path key, and an uplink change that keeps the
// connection's addresses produces no migration: so the attachment compares
// it at its own callbacks, on its run loop, and moves to the path's current
// controller when it changed. It must be called before installation.
func (a *PathAttachment) SetGeneration(read func() uint64) {
	a.generation = read
	a.seenGeneration = read()
}

// followGeneration re-attaches a, on its run loop, if its endpoint's uplink
// generation moved since it last looked.
func (a *PathAttachment) followGeneration(now monotime.Time) {
	if a.generation == nil || !a.attached {
		return
	}
	g := a.generation()
	if g == a.seenGeneration {
		return
	}
	a.seenGeneration = g
	if a.addrs == nil {
		return
	}
	local, remote := a.addrs()
	a.OnPathMigrated(now, local, local, remote, remote)
}

// SetTransportProgress gives the attachment its connection's progress
// export. With it, PROBING follows the transport's own evidence -- which
// also sees a late acknowledgement of a packet retired for a PTO probe,
// although no packet event reports one -- rather than being inferred from
// packet callbacks. It must be called before installation.
func (a *PathAttachment) SetTransportProgress(read func() TransportProgress) { a.progress = read }

func (a *PathAttachment) readProgress() (tp TransportProgress) {
	if a.progress != nil {
		tp = a.progress()
	}
	return tp
}

// applyProgress moves the attachment between COUNTED and PROBING on its
// transport's progress. It is called, with a fresh reading, at every
// callback of the connection's run loop.
func (c *PathController) applyProgress(a *PathAttachment, tp TransportProgress) []func() {
	if a.progress == nil {
		return nil
	}
	if tp.Epoch != a.progressEpoch {
		a.progressEpoch = tp.Epoch
		c.promote(a)
		return nil
	}
	if !a.probing && tp.PTOExpiries >= pathProbingPTOExpiries {
		before := c.flight
		wakes := c.demote(a)
		if c.flight < before {
			wakes = c.collectBlocked(a, wakes)
		}
		return wakes
	}
	return nil
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
		wakes = c.demote(a)
		if c.flight < before {
			wakes = c.collectBlocked(a, wakes)
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
	if c.rtt.fallback == nil && !a.staleRTT {
		// A connection that came here from another path does not seed
		// this one's round trip.
		c.rtt.fallback = provider
	}
	c.mu.Unlock()
}

func (a *PathAttachment) SetMaxDatagramSize(size quiccongestion.ByteCount) {
	c := a.ctl.Load()
	c.mu.Lock()
	// Under the lock: a sibling's admission reads it (controlBypass).
	a.maxPacket = size
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

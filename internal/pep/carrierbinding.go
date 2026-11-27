package pep

import (
	"context"
	"errors"
	"time"

	"github.com/bojieli/queqiao/internal/metrics"
)

// errBindingDeadline ends a flow that could not be rebound before the
// gateway's grace for it can have run out (§10.3 step 4). In QUIC-only mode
// it is the actual boundary on wires 1 and 2.
var errBindingDeadline = errors.New("flow was not rebound before its binding deadline")

// carrierRebindPoll bounds how long a flow's binding loop sleeps between
// looks at its own lanes; carrier transitions wake it at once.
const carrierRebindPoll = 250 * time.Millisecond

// carrierRTT is the JOIN round trip estimate r of §10.3 step 3: the eligible
// carrier's smoothed round trip.
func (c *Client) carrierRTT() time.Duration {
	if g := c.carriers.eligible(); g != nil {
		return g.conn.ConnectionStats().SmoothedRTT
	}
	return 0
}

// manageCarrierBoundLanes keeps one QUIC flow bound to the path's carrier
// under recovery=carrier (§10.3, §10.4). It never dials QUIC itself: the
// carrier manager owns the carrier and its replacement. The flow's binding
// is reconciled from its own lanes, which cannot be missed the way an event
// can:
//
//   - bound to the eligible carrier: nothing to do;
//   - unbound (its stream failed): one JOIN on the eligible carrier, the same
//     carrier when it is live; while the carrier is SUSPECT the flow waits
//     for its recovery or a promotion, whichever comes first;
//   - bound to a superseded carrier (promoted away from): one JOIN on the new
//     carrier, then the old stream is closed;
//   - either way under a binding deadline: AUTO hands off to TCP no later
//     than the latest handoff start, and a flow still unbound at the
//     deadline fails.
func (c *Client) manageCarrierBoundLanes(ctx context.Context, flow *multipathFlow, sessionID [16]byte, flowID uint64) {
	m := c.carriers
	manageCtx, manageCancel := context.WithCancel(ctx)
	defer manageCancel()
	// As in manageQUICLanes: once this returns nothing will replace a lost
	// lane, so the flow's grace should stop rather than wait in silence.
	defer flow.noteReplacementAbandoned()
	go func() {
		select {
		case <-flow.doneChan():
			manageCancel()
		case <-manageCtx.Done():
		}
	}()
	ticker := time.NewTicker(carrierRebindPoll)
	defer ticker.Stop()
	m.flows.Add(1)
	counted := true
	uncount := func() {
		if counted {
			counted = false
			m.flows.Add(-1)
		}
	}
	defer uncount()
	unbound := false
	setUnbound := func(on bool) {
		if on != unbound {
			unbound = on
			if on {
				m.unbound.Add(1)
			} else {
				m.unbound.Add(-1)
			}
		}
	}
	defer setUnbound(false)
	var boundTo *managedCarrier
	bindTo := func(g *controlQUICGeneration) {
		var next *managedCarrier
		if g != nil {
			next = g.managed
		}
		if next == boundTo {
			return
		}
		if boundTo != nil {
			boundTo.bound.Add(-1)
		}
		if next != nil {
			next.bound.Add(1)
		}
		boundTo = next
	}
	defer bindTo(nil)
	var (
		observed      time.Time
		lastCarrier   *controlQUICGeneration
		attempts      int
		backoff       time.Duration
		nextAttempt   time.Time
		lastAttemptAt time.Time
		tcpTried      bool
		// tcpNow asks for the TCP handoff at once: the gateway said the
		// flow is already in TCP mode. tcpNextAt paces retries of a failed
		// handoff within the binding budget.
		tcpNow     bool
		tcpNextAt  time.Time
		tcpBackoff time.Duration
		gate       tcpHandoffGate
	)
	// wake fires at the next time this loop must act on its own: the latest
	// handoff start, the binding deadline, or the end of a backoff. The
	// ticker alone could start a handoff a poll late.
	var wake <-chan time.Time
	for {
		select {
		case <-flow.doneChan():
			return
		case <-manageCtx.Done():
			return
		case <-m.changes():
		case <-ticker.C:
		case <-wake:
		case <-flow.stallSignals():
			// Nothing sends these under this mode; drained so a stale one
			// cannot accumulate.
		}
		if flow.finSent.Load() && flow.remoteFinSeen.Load() {
			flow.closeAll()
			return
		}
		if hasTCPLane(flow) {
			// The flow leaves the carrier manager: it holds no QUIC
			// reservation and no wire pin while it lives on TCP.
			setUnbound(false)
			bindTo(nil)
			uncount()
			retired := flow.retireLanesExcept(TransportTCP)
			c.cfg.Logger.Info("flow handed off to TCP fallback", "flow_id", flowID, "retired_quic_lanes", retired, "tcp_lanes", flow.laneCount())
			c.manageTCPBundle(manageCtx, flow, sessionID, flowID)
			return
		}
		healthy := flow.healthyLanes()
		current := flowGeneration(flow)
		if current != nil {
			lastCarrier = current
		}
		bindTo(current)
		superseded := len(healthy) > 0 && current != nil && m.superseded(current)
		// An unbound flow holds new OPENs back while it is due a JOIN; one
		// backing off after a refusal does not need a stream slot now, and
		// must not stall every new flow for its whole backoff.
		setUnbound(len(healthy) == 0 && !m.now().Before(nextAttempt) && attempts < maxLaneRecoveryAttempts)
		if len(healthy) > 0 && !superseded {
			observed, tcpTried, tcpNextAt, tcpBackoff, wake = time.Time{}, false, time.Time{}, 0, nil
			gate = tcpHandoffGate{}
			// The per-flow budget resets only after a sustained healthy
			// dwell, as today's does: a stream that fails again just after
			// each JOIN cannot JOIN without bound.
			if attempts > 0 && m.now().Sub(lastAttemptAt) >= laneRecoveryResetAfter {
				attempts, backoff, nextAttempt = 0, 0, time.Time{}
			}
			continue
		}
		now := m.now()
		if observed.IsZero() {
			// The client's failure-observation time t_c: the deadlines run
			// from here, for an idle flow too (§10.3 step 4).
			observed = now
			latest, deadline := m.cfg.bindingDeadlines(observed)
			c.cfg.Logger.Info("flow binding lost", "flow_id", flowID, "superseded", superseded,
				"latest_handoff_start", latest.Sub(now), "binding_deadline", deadline.Sub(now))
		}
		latest, deadline := m.cfg.bindingDeadlines(observed)
		wake = nextWake(now, latest, deadline, nextAttempt, tcpNextAt, observed.Add(c.fallbackGrace()))
		if !now.Before(deadline) {
			m.count(&m.deadlineFailures, metrics.CarrierBindingDeadlineFailure)
			c.cfg.Logger.Warn("flow binding deadline passed", "flow_id", flowID, "attempts", attempts,
				"since_observed", now.Sub(observed))
			flow.failBinding(errBindingDeadline)
			return
		}
		// An unconfirmed OPEN is not rebound while its carrier is only
		// SUSPECT (§10.3 step 7): the binding is not lost then, so this point
		// is not reached. Once the carrier has been superseded, it was
		// promoted away from for making no progress through k_p probe
		// timeouts, which is the carrier manager's own evidence that the lane
		// has failed, so the failed-lane rule applies now: the JOIN is
		// attempted on the new carrier, and the gateway holds it for a
		// pending OPEN or refuses it, ending the flow at once rather than at
		// the dead carrier's idle timeout.
		target := m.eligible()
		if c.cfg.Transport == TransportAuto && !tcpTried {
			// AUTO's TCP handoff starts no later than the latest handoff
			// start; when there is no QUIC carrier to JOIN at all it starts
			// once the fallback grace has passed, as today's rescue does.
			due := tcpNow || !now.Before(latest) || (target == nil && now.Sub(observed) >= c.fallbackGrace())
			held := false
			if due && !now.Before(tcpNextAt) {
				// A handoff that timed out found TCP down as well: the path
				// was away, not UDP. When it returns, this retry and the
				// QUIC replacement race, and a TCP JOIN that lands first
				// commits the flow to TCP for good (tcpHandoffGate).
				var until time.Time
				held, until = gate.hold(now, latest, tcpNow || !now.Before(latest), target == nil && m.quicComing(), c.quicHandshakeWait())
				if held {
					wake = nextWake(now, latest, deadline, nextAttempt, until)
				}
			}
			if due && !held && !now.Before(tcpNextAt) {
				tcpNow = false
				herr := c.carrierTCPHandoff(manageCtx, flow, sessionID, flowID, healthy, superseded, deadline, latest, &gate)
				if herr != nil && isTimeoutError(herr) {
					gate.timedOut = true
				}
				if errors.Is(herr, errHandoffYielded) {
					// A QUIC carrier came up while the TCP lane waited: the
					// flow JOINs it below, and a later handoff starts
					// afresh.
					tcpNextAt = now
					continue
				}
				switch {
				case herr == nil:
					// The gateway has committed the flow to TCP: the TCP
					// bundle manager owns it from here, even if the new lane
					// fails before the next look.
					m.count(&m.tcpHandoffs, metrics.CarrierTCPHandoff)
					setUnbound(false)
					bindTo(nil)
					uncount()
					c.manageTCPBundle(manageCtx, flow, sessionID, flowID)
					return
				case errors.Is(herr, errLaneJoinRejected) && !errors.Is(herr, errLaneJoinTCPMode):
					// The gateway does not hold this session: terminal, and
					// the flow ends now, idle or not.
					flow.noteRefusalCause(herr)
					flow.resumeRefused.Store(true)
					flow.closeAll()
					return
				case manageCtx.Err() != nil || flow.doneChanClosed():
					return
				default:
					// A transient failure: try again, paced, until the
					// binding deadline.
					tcpBackoff = max(min(2*tcpBackoff, carrierRoundCooldownMax), time.Second)
					tcpNextAt = now.Add(randomJitter(tcpBackoff))
				}
				continue
			}
		}
		if target == nil {
			if !m.suspect() {
				// DEAD with no standby: break-before-make. The flows share
				// the path's one dial, and one grace.
				m.requestCarrier()
			}
			continue
		}
		if now.Before(nextAttempt) {
			continue
		}
		if attempts >= maxLaneRecoveryAttempts {
			// The QUIC budget is spent: no more QUIC JOINs, but AUTO still
			// has its TCP handoff, and the binding deadline still ends the
			// flow.
			if c.cfg.Transport == TransportAuto && !tcpTried {
				tcpNow = true
			}
			continue
		}
		attempts++
		lastAttemptAt = now
		attempt := flow.noteReplacementAttempt()
		// In AUTO a QUIC JOIN, queued or running, must leave room for the
		// TCP handoff: it gives up at the latest handoff start.
		cutoff := deadline
		if c.cfg.Transport == TransportAuto && !tcpTried {
			cutoff = latest
		}
		err := c.rebindFlow(manageCtx, flow, sessionID, flowID, target, healthy, superseded, cutoff, deadline)
		if err == nil {
			if target == lastCarrier {
				m.count(&m.sameCarrierJoins, metrics.CarrierRebindSameCarrier)
			} else {
				m.count(&m.newCarrierJoins, metrics.CarrierRebindNewCarrier)
			}
			continue
		}
		if manageCtx.Err() != nil || flow.doneChanClosed() {
			return
		}
		err = classifyUnconfirmedRefusal(flow, err)
		flow.noteReplacementFailure(err)
		c.logLaneRecoveryFailure(manageCtx, flowID, attempt, err)
		if errors.Is(err, errLaneJoinTCPMode) {
			// The gateway already committed this flow to TCP: a handoff
			// whose acknowledgement was lost. AUTO retries the TCP JOIN
			// within the binding budget; QUIC-only cannot follow it.
			if c.cfg.Transport == TransportAuto {
				tcpTried, tcpNow = false, true
				continue
			}
		}
		if errors.Is(err, errLaneJoinRejected) {
			// The gateway does not hold this session: terminal.
			flow.noteRefusalCause(err)
			flow.resumeRefused.Store(true)
			if len(flow.healthyLanes()) > 0 {
				flow.closeAll()
			}
			return
		}
		if backoff == 0 {
			backoff = time.Second
		} else {
			backoff = min(2*backoff, carrierRoundCooldownMax)
		}
		next := now.Add(randomJitter(backoff))
		if next.After(latest) && now.Before(latest) {
			// Never back off past the last moment a JOIN can still start.
			next = latest
		}
		nextAttempt = next
	}
}

// nextWake is a timer for the earliest of times still in the future.
func nextWake(now time.Time, times ...time.Time) <-chan time.Time {
	var next time.Time
	for _, t := range times {
		if t.After(now) && (next.IsZero() || t.Before(next)) {
			next = t
		}
	}
	if next.IsZero() {
		return nil
	}
	return time.After(next.Sub(now))
}

// rebindFlow moves flow onto target with one JOIN on a stream of it, with
// RESERVE_CONTROL, through the JOIN queue (§10.3 steps 1-3). On a superseded
// binding the old stream is closed once the replacement's OPEN_OK arrived.
//
// The JOIN, queued or running, gives up at cutoff; the queue orders it by
// deadline.
func (c *Client) rebindFlow(ctx context.Context, flow *multipathFlow, sessionID [16]byte, flowID uint64, target *controlQUICGeneration, healthy []*mpLane, superseded bool, cutoff, deadline time.Time) error {
	m := c.carriers
	if mc := target.managed; mc != nil {
		mc.rebinding.Add(1)
		defer mc.rebinding.Add(-1)
	}
	joinCtx, cancel := context.WithTimeout(withCarrierRebind(ctx), max(cutoff.Sub(m.now()), time.Millisecond))
	defer cancel()
	release, err := m.joins.acquire(joinCtx, joinPriority(flow), deadline)
	if err != nil {
		return err
	}
	defer release()
	var replaced *mpLane
	if superseded {
		for _, lane := range healthy {
			if flow.laneIsControl(lane) {
				replaced = lane
				break
			}
		}
		if replaced == nil && len(healthy) > 0 {
			replaced = healthy[0]
		}
	}
	hint := flow.rescueHint.Load()
	if replaced != nil {
		joinCtx = context.WithValue(joinCtx, laneReplacementContextKey{}, laneReplacementHint{id: replaced.id})
	} else if hint != nil {
		joinCtx = context.WithValue(joinCtx, laneReplacementContextKey{}, *hint)
	}
	laneID, err := flow.allocateJoinID()
	if err != nil {
		return err
	}
	lane, err := c.openControlPoolJoinLane(joinCtx, sessionID, flowID, laneID)
	if err != nil {
		return err
	}
	// Replay is gated on the new binding: READY data waits for the first ACK
	// after it, or one SRTT (§10.3 step 5).
	srtt := c.carrierRTT()
	if srtt <= 0 {
		srtt = 100 * time.Millisecond
	}
	lane.replayGate = &replayGate{wait: srtt}
	if err := c.installRecoveryLane(flow, lane); err != nil {
		return err
	}
	switch {
	case replaced != nil:
		// OPEN_OK has arrived: close the old stream, so a released gateway
		// sees that lane fail and stops scheduling on it (§10.3 step 1).
		flow.retireLaneByID(replaced.id)
	case hint != nil:
		flow.retireLaneByID(hint.id)
		flow.clearRescueReplacementSnapshot(hint)
	}
	c.cfg.Logger.Info("flow rebound", "flow_id", flowID, "lane", lane.id, "superseded", superseded,
		"carrier", flowGenerationID(flow))
	return nil
}

func flowGenerationID(flow *multipathFlow) uint64 {
	if g := flowGeneration(flow); g != nil {
		return g.id
	}
	return 0
}

// tcpHandoffGate holds a flow's TCP handoff back for a QUIC replacement that
// is on its way, within one binding episode.
//
// AUTO hands a flow with no QUIC carrier off to TCP once the fallback grace
// has passed, and retries on a backoff. That is right when UDP is blocked:
// the first attempt succeeds and the flow moves at once. In an outage the
// attempts time out, because TCP is down too. When the path returns, the
// next retry and the carrier manager's QUIC replacement come up within the
// same second or two, and a TCP JOIN the gateway accepts commits the flow to
// TCP for the rest of its life. On a live path dropped for 25 s, every flow
// in flight went to TCP 1.2 to 2.1 s after the restore, a long download
// among them, and it then moved 14 s after the restore, on one TCP lane.
//
// So once a handoff attempt of this episode has timed out, a retry that is
// due waits for a QUIC replacement that is being dialled: for about one
// handshake, and never past the latest handoff start. If QUIC comes up, the
// flow JOINs it; if not, the retry goes ahead.
type tcpHandoffGate struct {
	// timedOut says a handoff attempt of this episode failed by timing out:
	// both transports were down.
	timedOut bool
	// waitUntil is when the retry now held goes ahead; zero while none is.
	waitUntil time.Time
}

// hold reports whether a TCP handoff due at now must wait, and until when.
// forced is a handoff nothing may delay: the latest handoff start has come,
// or the gateway has already committed the flow to TCP. quicComing says a
// QUIC replacement is being dialled or verified.
func (g *tcpHandoffGate) hold(now, latest time.Time, forced, quicComing bool, handshake time.Duration) (bool, time.Time) {
	if forced || !g.timedOut || !quicComing {
		g.waitUntil = time.Time{}
		return false, time.Time{}
	}
	if g.waitUntil.IsZero() {
		g.waitUntil = now.Add(handshake)
		if g.waitUntil.After(latest) {
			g.waitUntil = latest
		}
	}
	if now.Before(g.waitUntil) {
		return true, g.waitUntil
	}
	// Waited out: this retry goes ahead, and the next one waits afresh.
	g.waitUntil = time.Time{}
	return false, time.Time{}
}

const (
	// quicHandshakeRTTs is a QUIC replacement's handshake and the probe that
	// verifies it, in round trips; quicHandshakeWaitMin and Max bound the
	// wait a TCP handoff gives it.
	quicHandshakeRTTs    = 4
	quicHandshakeWaitMin = time.Second
	quicHandshakeWaitMax = 3 * time.Second
)

// quicHandshakeWait is about how long a QUIC replacement takes to become a
// carrier on this path once the path carries packets.
func (c *Client) quicHandshakeWait() time.Duration {
	return quicHandshakeWaitFor(time.Duration(c.observedRTT.Load()))
}

func quicHandshakeWaitFor(rtt time.Duration) time.Duration {
	return min(max(quicHandshakeRTTs*rtt, quicHandshakeWaitMin), quicHandshakeWaitMax)
}

// isTimeoutError reports whether err is an attempt that ran out of time, as
// opposed to one the peer or the network refused.
func isTimeoutError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var timeout interface{ Timeout() bool }
	return errors.As(err, &timeout) && timeout.Timeout()
}

// errHandoffYielded says a TCP handoff stood down before its JOIN because a
// QUIC carrier became eligible: nothing was committed.
var errHandoffYielded = errors.New("TCP handoff yielded to a QUIC carrier")

// carrierTCPHandoff starts AUTO's TCP handoff for a flow that no QUIC JOIN
// can rebind in time. It reports whether the TCP lane was installed; the
// binding loop then hands the flow to the TCP bundle manager.
//
// The handoff has two steps, and only the second commits: the TCP lane is
// dialled and authenticated, and then the JOIN is sent. A dial that began in
// an outage completes when the path returns -- the kernel retransmits its
// SYN, and the handshake above it simply waits -- so it never times out, and
// the gate on starting a retry never sees it. Between the two steps, then:
// if the path was away for TCP as well (an earlier attempt timed out, or
// this dial took longer than a handshake should) and a QUIC replacement is
// on its way, the lane waits for it, for about one handshake and never past
// latest. If a QUIC carrier becomes eligible the lane is closed with no JOIN
// sent, and errHandoffYielded is returned.
func (c *Client) carrierTCPHandoff(ctx context.Context, flow *multipathFlow, sessionID [16]byte, flowID uint64, healthy []*mpLane, superseded bool, deadline, latest time.Time, gate *tcpHandoffGate) error {
	m := c.carriers
	joinCtx, cancel := context.WithTimeout(ctx, max(deadline.Sub(m.now()), time.Millisecond))
	defer cancel()
	if superseded && len(healthy) > 0 {
		joinCtx = context.WithValue(joinCtx, laneReplacementContextKey{}, laneReplacementHint{id: healthy[0].id})
	} else if hint := flow.rescueHint.Load(); hint != nil {
		joinCtx = context.WithValue(joinCtx, laneReplacementContextKey{}, *hint)
	}
	laneID, err := flow.allocateJoinID()
	if err != nil {
		return err
	}
	c.metrics.LaneRescueAttempt()
	lane, err := c.openTCPHandoffLane(joinCtx, sessionID, flowID, laneID, latest, gate)
	if err != nil {
		if !errors.Is(err, errHandoffYielded) {
			c.cfg.Logger.Info("carrier TCP handoff unavailable", "flow_id", flowID, "failure_kind", replacementFailureKind(err), "error", err)
		}
		return err
	}
	if err := c.installRecoveryLane(flow, lane); err != nil {
		return err
	}
	c.metrics.LaneRescueWin(0)
	c.cfg.Logger.Info("flow handed off to TCP before its binding deadline", "flow_id", flowID, "lane", lane.id)
	return nil
}

// openTCPHandoffLane dials the TCP lane, gives a QUIC replacement its chance
// if the path was away (see carrierTCPHandoff), and sends the JOIN.
func (c *Client) openTCPHandoffLane(ctx context.Context, sessionID [16]byte, flowID, laneID uint64, latest time.Time, gate *tcpHandoffGate) (*mpLane, error) {
	if c.openJoinLaneForTest != nil {
		return c.openJoinLaneForTest(ctx, TransportTCP, laneID)
	}
	m := c.carriers
	c.carrierShadow.noteJoinAttempt(ctx, sessionID, flowID, laneID, TransportTCP)
	wait := c.quicHandshakeWait()
	began := m.now()
	lane, err := c.dialJoinLane(ctx, TransportTCP, sessionID, laneID)
	if err != nil {
		return nil, err
	}
	now := m.now()
	if tcpHandoffYields(gate != nil && gate.timedOut, now.Sub(began), wait, now, latest) {
		until := now.Add(wait)
		if until.After(latest) {
			until = latest
		}
		if m.eligible() == nil && !m.suspect() {
			// The path carries packets again: make sure a replacement is
			// being dialled now, not at the manager's next probe.
			m.requestCarrier()
		}
		if c.awaitQUICCarrier(ctx, until) {
			// Nothing was sent on the lane but its authentication: closing
			// it leaves the gateway nothing to clean up but the connection,
			// which it held idle for at most the handshake wait.
			discardLane(lane)
			c.cfg.Logger.Info("TCP handoff stood down for a QUIC carrier", "flow_id", flowID, "dial", now.Sub(began))
			return nil, errHandoffYielded
		}
	}
	return c.completeLaneJoin(ctx, lane, flowID, 0)
}

// discardLane closes an authenticated lane that carried no JOIN. The TLS
// close alert and the FIN go out at once, so the gateway's first-frame read
// ends with an end of stream and releases the connection's admission slot
// without waiting out its handshake bound.
func discardLane(lane *authenticatedLane) {
	_ = lane.fc.Close()
	_ = lane.outer.Close()
}

// tcpHandoffYields reports whether a TCP handoff whose lane is ready should
// wait for QUIC before its JOIN: the path was away for TCP too -- an earlier
// attempt of the episode timed out, or this dial took longer than the
// handshake wait -- and the latest handoff start has not come.
func tcpHandoffYields(timedOut bool, dial, wait time.Duration, now, latest time.Time) bool {
	return (timedOut || dial > wait) && now.Before(latest)
}

// awaitQUICCarrier waits until a QUIC carrier is eligible, a replacement is
// no longer on its way, or until passes; it reports whether one is eligible.
func (c *Client) awaitQUICCarrier(ctx context.Context, until time.Time) bool {
	m := c.carriers
	for {
		if m.eligible() != nil {
			return true
		}
		now := m.now()
		if !m.quicComing() || !now.Before(until) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-m.changes():
		case <-time.After(min(until.Sub(now), carrierRebindPoll/5)):
		}
	}
}

// replayGate holds a rebound lane's READY data until the first flow ACK
// arrives on that lane, or until one SRTT has passed (§10.3 step 5). An ACK
// on the old lane during make-before-break does not open it. ACK and FIN
// frames do not pass through it.
type replayGate struct {
	// wait is one SRTT, counted from when the lane starts carrying data:
	// after OPEN_OK, when the lane is activated.
	wait time.Duration
}

// waitReplayGate blocks the lane's puller until its gate opens.
func (f *multipathFlow) waitReplayGate(ctx context.Context, lane *mpLane) {
	gate := lane.replayGate
	if gate == nil {
		return
	}
	until := time.Now().Add(gate.wait)
	for lane.acksIn.Load() == 0 && time.Now().Before(until) {
		wait := min(time.Until(until), 5*time.Millisecond)
		select {
		case <-ctx.Done():
			return
		case <-f.done:
			return
		case <-time.After(wait):
		}
	}
}

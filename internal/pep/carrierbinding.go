package pep

import (
	"context"
	"errors"
	"time"
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
	var (
		observed      time.Time
		lastCarrier   *controlQUICGeneration
		attempts      int
		backoff       time.Duration
		nextAttempt   time.Time
		lastAttemptAt time.Time
		tcpTried      bool
		// tcpNow asks for the TCP handoff at once: the gateway said the
		// flow is already in TCP mode.
		tcpNow bool
	)
	for {
		select {
		case <-flow.doneChan():
			return
		case <-manageCtx.Done():
			return
		case <-m.changes():
		case <-ticker.C:
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
		superseded := len(healthy) > 0 && current != nil && m.superseded(current)
		// An unbound flow holds new OPENs back while it is due a JOIN; one
		// backing off after a refusal does not need a stream slot now, and
		// must not stall every new flow for its whole backoff.
		setUnbound(len(healthy) == 0 && !m.now().Before(nextAttempt))
		if len(healthy) > 0 && !superseded {
			observed, tcpTried = time.Time{}, false
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
		if !now.Before(deadline) {
			m.deadlineFailures.Add(1)
			c.cfg.Logger.Warn("flow binding deadline passed", "flow_id", flowID, "attempts", attempts,
				"since_observed", now.Sub(observed))
			flow.failBinding(errBindingDeadline)
			return
		}
		if superseded && flow.openConfirmationRequired.Load() {
			// A JOIN can overtake an OPEN the gateway has not admitted; the
			// old carrier's probes keep retransmitting it (§10.3 step 7).
			// Its binding deadline still runs.
			continue
		}
		target := m.eligible()
		if c.cfg.Transport == TransportAuto && !tcpTried {
			// AUTO's TCP handoff starts no later than the latest handoff
			// start; when there is no QUIC carrier to JOIN at all it starts
			// once the fallback grace has passed, as today's rescue does.
			if tcpNow || !now.Before(latest) || (target == nil && now.Sub(observed) >= c.fallbackGrace()) {
				tcpTried, tcpNow = true, false
				if c.carrierTCPHandoff(manageCtx, flow, sessionID, flowID, healthy, superseded, deadline) {
					m.tcpHandoffs.Add(1)
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
				m.sameCarrierJoins.Add(1)
			} else {
				m.newCarrierJoins.Add(1)
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
	lane.replayGate = &replayGate{until: time.Now().Add(srtt)}
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

// carrierTCPHandoff starts AUTO's TCP handoff for a flow that no QUIC JOIN
// can rebind in time. It reports whether the TCP lane was installed; the
// binding loop then hands the flow to the TCP bundle manager.
func (c *Client) carrierTCPHandoff(ctx context.Context, flow *multipathFlow, sessionID [16]byte, flowID uint64, healthy []*mpLane, superseded bool, deadline time.Time) bool {
	joinCtx, cancel := context.WithTimeout(ctx, max(deadline.Sub(c.carriers.now()), time.Millisecond))
	defer cancel()
	if superseded && len(healthy) > 0 {
		joinCtx = context.WithValue(joinCtx, laneReplacementContextKey{}, laneReplacementHint{id: healthy[0].id})
	} else if hint := flow.rescueHint.Load(); hint != nil {
		joinCtx = context.WithValue(joinCtx, laneReplacementContextKey{}, *hint)
	}
	laneID, err := flow.allocateJoinID()
	if err != nil {
		return false
	}
	c.metrics.LaneRescueAttempt()
	lane, err := c.openJoinLane(joinCtx, TransportTCP, sessionID, flowID, laneID)
	if err != nil {
		c.cfg.Logger.Info("carrier TCP handoff unavailable", "flow_id", flowID, "failure_kind", replacementFailureKind(err), "error", err)
		return false
	}
	if err := c.installRecoveryLane(flow, lane); err != nil {
		return false
	}
	c.metrics.LaneRescueWin(0)
	c.cfg.Logger.Info("flow handed off to TCP before its binding deadline", "flow_id", flowID, "lane", lane.id)
	return true
}

// replayGate holds a rebound lane's READY data until the first flow ACK
// arrives on that lane, or until one SRTT has passed (§10.3 step 5). An ACK
// on the old lane during make-before-break does not open it. ACK and FIN
// frames do not pass through it.
type replayGate struct {
	until time.Time
}

// waitReplayGate blocks the lane's puller until its gate opens.
func (f *multipathFlow) waitReplayGate(ctx context.Context, lane *mpLane) {
	gate := lane.replayGate
	if gate == nil {
		return
	}
	for lane.acksIn.Load() == 0 && time.Now().Before(gate.until) {
		wait := min(time.Until(gate.until), 5*time.Millisecond)
		select {
		case <-ctx.Done():
			return
		case <-f.done:
			return
		case <-time.After(wait):
		}
	}
}

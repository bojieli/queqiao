package metrics

import (
	"fmt"
	"io"
	"sync/atomic"
)

// The carrier-shadow counters are written only by an endpoint running the
// carrier-liveness observer (--carrier-shadow). They describe what the
// carrier-owned recovery of the path-authority design (§10) would have
// decided beside today's per-flow watchdog, and they never influence a
// decision. With the observer off every one of them stays zero.

// CarrierShadowState is a carrier state the observer can assign.
type CarrierShadowState int

const (
	// CarrierShadowActive: the transport is being acknowledged, or has missed
	// fewer than k_s probe timeouts since it last was.
	CarrierShadowActive CarrierShadowState = iota
	// CarrierShadowSuspect: k_s probe timeouts without transport progress.
	// The design would dial one standby and move nothing.
	CarrierShadowSuspect
	// CarrierShadowReplace: k_p probe timeouts, the progress epoch unchanged
	// since SUSPECT. The design would promote a verified standby here.
	CarrierShadowReplace
	// CarrierShadowDead: the connection ended on a transport failure (idle
	// timeout, stateless reset, transport error).
	CarrierShadowDead
	// CarrierShadowClosed: the connection was closed by an application on
	// either end. That is not evidence about the path.
	CarrierShadowClosed
	// CarrierShadowStates is how many states there are.
	CarrierShadowStates
)

var carrierShadowStateNames = [CarrierShadowStates]string{"active", "suspect", "replace", "dead", "closed"}

func (s CarrierShadowState) String() string {
	if s < 0 || s >= CarrierShadowStates {
		return "unknown"
	}
	return carrierShadowStateNames[s]
}

// CarrierShadowRemedy is the remedy the design would take for a piece of
// flow-scoped evidence (§0 decision 5, §10.4).
type CarrierShadowRemedy int

const (
	// CarrierShadowReplaceCarrier: the carrier under the flow is silent.
	CarrierShadowReplaceCarrier CarrierShadowRemedy = iota
	// CarrierShadowRebindStream: the flow's stream failed on a live carrier.
	CarrierShadowRebindStream
	// CarrierShadowWaitMoveCoded: missing flow credit on a live carrier with
	// coded bytes pending: move them to the stream, otherwise wait.
	CarrierShadowWaitMoveCoded
	// CarrierShadowWait: missing flow credit on a live carrier: wait.
	CarrierShadowWait
	// CarrierShadowUntracked: the lane is not a QUIC carrier (TLS/TCP, §11.1).
	CarrierShadowUntracked
	// CarrierShadowDialStandby: the carrier is SUSPECT. The design dials one
	// standby and moves nothing; the flow-scoped evidence adds nothing.
	CarrierShadowDialStandby
	// CarrierShadowWaitUnbound: a stream failed on a SUSPECT carrier. The
	// flow is unbound until its carrier recovers or is replaced.
	CarrierShadowWaitUnbound
	// CarrierShadowCarrierClosed: an application on either end had already
	// closed the carrier, so the event is that decision's consequence, not
	// evidence about the path.
	CarrierShadowCarrierClosed
	// CarrierShadowRemedies is how many remedies there are.
	CarrierShadowRemedies
)

var carrierShadowRemedyNames = [CarrierShadowRemedies]string{"replace_carrier", "rebind_stream", "wait_move_coded", "wait", "untracked", "dial_standby", "wait_unbound", "carrier_closed"}

func (r CarrierShadowRemedy) String() string {
	if r < 0 || r >= CarrierShadowRemedies {
		return "unknown"
	}
	return carrierShadowRemedyNames[r]
}

// CarrierShadowPTOBuckets bounds the probe-timeout histogram. Bucket n-1
// counts episodes that reached n expiries; the last bucket also holds every
// episode that went further.
const CarrierShadowPTOBuckets = 8

type carrierShadowCounters struct {
	transitions [CarrierShadowStates]atomic.Uint64
	// falseReplacements counts carriers that reached REPLACE and were then
	// acknowledged again: a promotion the design would have made in vain.
	falseReplacements atomic.Uint64
	ptoReached        [CarrierShadowPTOBuckets]atomic.Uint64
	ptoRecovered      [CarrierShadowPTOBuckets]atomic.Uint64
	stalls            [CarrierShadowRemedies]atomic.Uint64
	stallsAcked       atomic.Uint64
	laneFailures      [CarrierShadowRemedies]atomic.Uint64
}

// CarrierShadowSnapshot is the observer's part of a Snapshot.
type CarrierShadowSnapshot struct {
	// Transitions counts entries into each state. Entries into active are
	// returns from SUSPECT or REPLACE.
	Transitions       [CarrierShadowStates]uint64
	FalseReplacements uint64
	// PTOReached[n-1] counts no-progress episodes that reached n probe
	// timeouts; PTORecovered[n-1] those of them that then saw progress. Their
	// ratio is the conditional false-promotion rate for k_p = n (§10.2).
	PTOReached   [CarrierShadowPTOBuckets]uint64
	PTORecovered [CarrierShadowPTOBuckets]uint64
	// Stalls counts watchdog stall episodes by the remedy the design would
	// take; StallsTransportAcked those during which QUIC still acknowledged.
	Stalls               [CarrierShadowRemedies]uint64
	StallsTransportAcked uint64
	// LaneFailures counts lane failures by the remedy the design would take.
	LaneFailures [CarrierShadowRemedies]uint64
}

// CarrierShadowTransition records a carrier entering a state.
func (r *Registry) CarrierShadowTransition(to CarrierShadowState, falseReplacement bool) {
	if r == nil || to < 0 || to >= CarrierShadowStates {
		return
	}
	r.carrierShadow.transitions[to].Add(1)
	if falseReplacement {
		r.carrierShadow.falseReplacements.Add(1)
	}
}

// CarrierShadowEpisode records one no-progress episode that reached
// maxPTO probe timeouts, and whether the transport then progressed.
func (r *Registry) CarrierShadowEpisode(maxPTO uint32, recovered bool) {
	if r == nil || maxPTO == 0 {
		return
	}
	top := int(min(maxPTO, CarrierShadowPTOBuckets))
	for n := 0; n < top; n++ {
		r.carrierShadow.ptoReached[n].Add(1)
		if recovered {
			r.carrierShadow.ptoRecovered[n].Add(1)
		}
	}
}

// CarrierShadowStall records a watchdog stall episode classified by the
// design's remedy.
func (r *Registry) CarrierShadowStall(remedy CarrierShadowRemedy, transportAcked bool) {
	if r == nil || remedy < 0 || remedy >= CarrierShadowRemedies {
		return
	}
	r.carrierShadow.stalls[remedy].Add(1)
	if transportAcked {
		r.carrierShadow.stallsAcked.Add(1)
	}
}

// CarrierShadowLaneFailure records a lane failure classified by the design's
// remedy.
func (r *Registry) CarrierShadowLaneFailure(remedy CarrierShadowRemedy) {
	if r == nil || remedy < 0 || remedy >= CarrierShadowRemedies {
		return
	}
	r.carrierShadow.laneFailures[remedy].Add(1)
}

func (r *Registry) carrierShadowSnapshot() CarrierShadowSnapshot {
	var s CarrierShadowSnapshot
	c := &r.carrierShadow
	for i := range s.Transitions {
		s.Transitions[i] = c.transitions[i].Load()
	}
	s.FalseReplacements = c.falseReplacements.Load()
	for i := range s.PTOReached {
		s.PTOReached[i] = c.ptoReached[i].Load()
		s.PTORecovered[i] = c.ptoRecovered[i].Load()
	}
	for i := range s.Stalls {
		s.Stalls[i] = c.stalls[i].Load()
		s.LaneFailures[i] = c.laneFailures[i].Load()
	}
	s.StallsTransportAcked = c.stallsAcked.Load()
	return s
}

func writeCarrierShadow(w io.Writer, s CarrierShadowSnapshot) {
	for i, value := range s.Transitions {
		fmt.Fprintf(w, "queqiao_carrier_shadow_transitions_total{to=\"%s\"} %d\n", CarrierShadowState(i), value)
	}
	fmt.Fprintf(w, "queqiao_carrier_shadow_false_replacements_total %d\n", s.FalseReplacements)
	for i := range s.PTOReached {
		label := fmt.Sprint(i + 1)
		if i == CarrierShadowPTOBuckets-1 {
			label += "+"
		}
		fmt.Fprintf(w, "queqiao_carrier_shadow_pto_episodes_total{reached=\"%s\"} %d\n", label, s.PTOReached[i])
		fmt.Fprintf(w, "queqiao_carrier_shadow_pto_recovered_total{reached=\"%s\"} %d\n", label, s.PTORecovered[i])
	}
	for i, value := range s.Stalls {
		fmt.Fprintf(w, "queqiao_carrier_shadow_stalls_total{remedy=\"%s\"} %d\n", CarrierShadowRemedy(i), value)
	}
	fmt.Fprintf(w, "queqiao_carrier_shadow_stalls_transport_acked_total %d\n", s.StallsTransportAcked)
	for i, value := range s.LaneFailures {
		fmt.Fprintf(w, "queqiao_carrier_shadow_lane_failures_total{remedy=\"%s\"} %d\n", CarrierShadowRemedy(i), value)
	}
}

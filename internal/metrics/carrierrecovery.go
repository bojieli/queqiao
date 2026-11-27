package metrics

import (
	"fmt"
	"io"
)

// The carrier-recovery counters are written only by a client running
// --recovery=carrier, whose carrier manager owns QUIC recovery (design §10,
// stage 8). With the bundle off every one of them stays zero.

// CarrierRecoveryEvent is one kind of carrier-manager decision or outcome.
type CarrierRecoveryEvent int

const (
	// CarrierSuspect: the pooled carrier went ACTIVE -> SUSPECT.
	CarrierSuspect CarrierRecoveryEvent = iota
	// CarrierPromotion: a verified standby became the path's carrier.
	CarrierPromotion
	// CarrierStandbyDial: a standby dial round started.
	CarrierStandbyDial
	// CarrierStandbyFailure: a standby dial round ended without a verified
	// standby.
	CarrierStandbyFailure
	// CarrierReplacementDial: a dial round admitted for flows waiting to be
	// rebound.
	CarrierReplacementDial
	// CarrierRebindSameCarrier: a flow rebound by a JOIN on the carrier it
	// was last bound to.
	CarrierRebindSameCarrier
	// CarrierRebindNewCarrier: a flow rebound by a JOIN on another carrier.
	CarrierRebindNewCarrier
	// CarrierBindingDeadlineFailure: a flow reached its binding deadline
	// unbound.
	CarrierBindingDeadlineFailure
	// CarrierTCPHandoff: a flow moved to a TCP lane at its latest handoff
	// start.
	CarrierTCPHandoff
	// CarrierPathProbe: a path-return probe was sent on a silent carrier.
	CarrierPathProbe
	// CarrierSilentDeath: a carrier with flows was ended by the manager's
	// wall-clock silence bound rather than by its transport's idle timeout.
	CarrierSilentDeath
	// CarrierRecoveryEvents is how many events there are.
	CarrierRecoveryEvents
)

// carrierRecoveryMetrics names each event's Prometheus counter; the rebinds
// share one name, labelled by carrier.
var carrierRecoveryMetrics = [CarrierRecoveryEvents]string{
	"queqiao_carrier_suspects_total",
	"queqiao_carrier_promotions_total",
	"queqiao_carrier_standby_dials_total",
	"queqiao_carrier_standby_failures_total",
	"queqiao_carrier_replacement_dials_total",
	`queqiao_carrier_rebinds_total{carrier="same"}`,
	`queqiao_carrier_rebinds_total{carrier="new"}`,
	"queqiao_carrier_binding_deadline_failures_total",
	"queqiao_carrier_tcp_handoffs_total",
	"queqiao_carrier_path_probes_total",
	"queqiao_carrier_silent_deaths_total",
}

// CarrierRecovery records one carrier-manager event.
func (r *Registry) CarrierRecovery(event CarrierRecoveryEvent) {
	if r == nil || event < 0 || event >= CarrierRecoveryEvents {
		return
	}
	r.carrierRecovery[event].Add(1)
}

func writeCarrierRecovery(w io.Writer, s [CarrierRecoveryEvents]uint64) {
	for i, value := range s {
		fmt.Fprintf(w, "%s %d\n", carrierRecoveryMetrics[i], value)
	}
}

`--carrier-shadow` runs a carrier-liveness observer on clients and gateways
beside today's recovery, as groundwork for carrier-owned recovery in the
path-authority migration. For every QUIC connection that carries a flow it
labels the state the design would assign (active, suspect after
`--carrier-shadow-ks` probe timeouts without transport progress, replace after
`--carrier-shadow-kp`, dead on an idle timeout or transport error). For every
flow stall and lane failure it records that state, whether QUIC was still
acknowledging, and the remedy the design would take next to the one taken
today. Both endpoints also log when they first observed a flow's failure, when
the gateway's replacement grace started, and when a rescue JOIN arrived. The
records appear as `carrier shadow ...` log lines and
`queqiao_carrier_shadow_*` metrics, and `scripts/carrier_shadow_summary.py`
turns client and gateway logs into probe-timeout calibration tables, a stall
comparison and recovery-deadline distributions. It changes no decision; the
default is off.

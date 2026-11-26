`--recovery=carrier` makes the client's carrier manager the single owner of
QUIC recovery (stage 8 of the path-authority design). A carrier is replaced
only on its own transport evidence: after `--recovery-ks` probe timeouts
without progress it takes no new flows and one standby is dialled and
verified; after `--recovery-kp` it is promoted and the old carrier drains.
A flow whose stream failed is rebound by one JOIN on the live carrier, and
missing flow credit on a live stream is waited out. Per-flow QUIC rescue
dials and lane demotion are off under the switch, the adaptive keep-alive
schedule is on, and each unbound flow has a binding deadline derived from
the gateway's grace (`--recovery-skew`, `--recovery-handoff-budget`). The
thresholds are provisional until the carrier-shadow calibration. The switch
requires `--congestion-scope=path` and `--coded-retry=sticky`, and on a
client `--topology=single`. Off by default.

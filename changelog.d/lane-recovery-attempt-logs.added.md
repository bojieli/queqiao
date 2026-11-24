Client lane-recovery failure logs now name the flow and attempt on both
transports. The TCP `TCP bundle lane unavailable` line gains `flow_id`,
`attempt`, `replacement`, and `failure_kind`, matching the QUIC
`lane recovery unavailable` line. Every counted replacement failure is now
logged, including timeouts and peer refusals, which are written at info and
were previously dropped. A new `scripts/lane_replacement_split.py` splits
`lane replacement timeout` failures from these logs by what recovery did.

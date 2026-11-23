Client lane-recovery failure logs now name the flow and attempt on both
transports. The TCP `TCP bundle lane unavailable` line gains `flow_id`,
`attempt`, `replacement`, and `failure_kind`, matching the QUIC
`lane recovery unavailable` line, and a QUIC recovery round that times out is
logged at info instead of being dropped. A new
`scripts/lane_replacement_split.py` splits `lane replacement timeout` failures
from these logs by what recovery did.

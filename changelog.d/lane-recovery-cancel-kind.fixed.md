A TCP lane JOIN interrupted because its flow's recovery ended is now reported
as `canceled` rather than `timeout`. Cancellation interrupts the handshake by
moving the socket deadline to now, and the resulting i/o timeout was counted
and logged at info as `failure_kind=timeout` after the flow had already gone,
inflating the timeouts that suggest a path cannot carry a handshake. Such
joins are no longer logged once the flow's recovery has stopped; a handshake
that expires while the flow is live is still a `timeout`.
`scripts/lane_replacement_split.py` now reports its `window` over every
client record with a time instead of only terminal flow records, and keeps
flow keys only when `--gateway` input is given.

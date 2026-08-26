The bandwidth estimator rescanned everything in flight on every congestion
event to drop retired packet states. At 300 Mbit/s over a 200ms round trip
that is about six thousand entries per acknowledgement, and a CPU profile of a
294 Mbit/s transfer attributed 13% of all time to that one loop. Packet
numbers only increase, so it now walks the range being dropped: 23x faster on
a benchmark retiring one window at a realistic acknowledgement cadence.

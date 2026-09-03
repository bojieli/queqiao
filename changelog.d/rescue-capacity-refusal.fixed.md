Rescue races no longer misfire on the peer's lane ceiling. A capacity
refusal of the pooled control JOIN is a benign per-attempt outcome — usually
a sibling attempt already holds the lane — but it fell through to the
fallback paths: an ordinary QUIC join under `--transport quic`, or the AUTO
TCP commit, which could hand the flow to TCP while a QUIC sibling won the
race. Capacity refusals now end the attempt instead. The TCP fallback
metric also moved from commit time to install time, so a TCP commit that
loses its race no longer counts as a fallback, and the stall watchdog stays
silent while a rescue round is in flight rather than queueing another round
behind it.

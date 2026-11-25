`scripts/carrier_shadow_summary.py` ends a client incident when any flow
observes its carrier in a newer transport epoch, not only the flow itself.
It also recognizes an outage that ended in the millisecond it started:
outage records now carry an `outage_id` that their end record repeats.
Correlating stalls with later progress no longer rescans each carrier's
history per stall, so long captures of many flows on one pooled carrier
summarize in near-linear time.

A transfer that stalled once anywhere inside its first `BulkBytes` was
permanently classified interactive on the datacenter profile, so it was coded
and held a single lane for the rest of its life. The idle veto now needs two
separate gaps: one is an event, two is a pattern, and only the pattern says a
flow is a caller rather than a transfer.

`--mem owned` enables the memory-ownership bundle of the path-authority
migration on clients and gateways, and `Session.SetMemoryBundle("owned")`
on mobile: the shared send budget grants one chunk per waiting flow in
arrival order instead of letting waiters race, a flow's retention bound is
tuned from its own validated flow credit instead of summing its lanes'
congestion windows, and bytes read but not yet dispatched (including a
queued end of stream) count as pending work for the no-progress budget. The
default, `--mem legacy`, keeps today's behaviour; any other value is refused
at start-up.

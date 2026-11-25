`--mem owned` enables the memory-ownership bundle of the path-authority
migration on clients and gateways: shared send and receive budgets grant one
chunk per waiting flow in arrival order instead of letting waiters race, a
flow's retention bound is tuned from its own validated flow credit instead
of summing its lanes' congestion windows, and bytes read but not yet
dispatched count as pending work for the no-progress budget. The default,
`--mem legacy`, keeps today's behaviour; any other value is refused at
start-up.

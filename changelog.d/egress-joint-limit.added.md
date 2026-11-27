Under `--congestion-scope=path`, a gateway's path authorities now share a
joint egress limit per network interface (`--egress-capacity`: `auto`, `off`,
or a rate). The ceiling is configured, or the interface's link speed with a
margin, or learned from the aggregate loss when authorities that see their
own loss rise with their own rate overload it together. It is divided
max-min among the active authorities, and has no effect while the aggregate
is under it or when only one authority is active. Each path authority also
stops compensating loss that rises with its own sending rate (up-phase loss
above cruise and down-phase loss), so a policed or overflowing shared point
is no longer read as channel erasure; independent channel loss is
compensated as before.

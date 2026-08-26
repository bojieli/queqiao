`queqiaod doctor` reports whether a host is in the state the deployment
measurements assumed: the selected profile's qualification level and
precondition, the kernel settings that matter, and whether the gateway
answers. It exists because the setting worth the most is a kernel default
nothing else would mention: `tcp_slow_start_after_idle` is 1 on Linux, which
throws a connection's window away after any idle gap, and turning it off was
worth 4.5x on a 300KB burst.

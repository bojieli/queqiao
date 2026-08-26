The advice to set `net.ipv4.tcp_slow_start_after_idle=0` is now qualified by
direction, in the runbook, the design document and `queqiaod doctor`. It is
the most valuable line here on a direction that does not lose packets, and it
makes an erasing one worse: a 100KB synthesis download took 827.7ms on a cold
connection and 2281.2ms on a warm one with the sysctl set, because restoring
the window hands the controller more in flight for a multiplicative decrease
to take away.

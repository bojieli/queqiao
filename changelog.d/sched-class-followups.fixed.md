`--sched=class` no longer blocks MTU discovery when the scheduler grants
exactly the packet size it is asked for: while a probe is due, admission is
requested at the probe's size, falling back to an ordinary packet if refused.
Under class permits, a not-yet-due ACK is no longer spent on an ACK-only
packet for a datagram that cannot fit beside it, a small leftover budget is
offered to a bounded number of streams, and draining control retransmissions
behind an oversized frame takes one pass per packet instead of quadratic
copying.

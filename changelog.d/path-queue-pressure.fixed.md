Under `--congestion=path`, a path's sender no longer holds a shared
bottleneck's queue full. The law keeps two bandwidth-delay products in
flight and its delay bound allows a round trip of twice the minimum, which
together are exactly the pipe and a one-product buffer, so at a shared link
the buffer stood full and overflowing; the overflow was then read as channel
erasure and compensated for by sending more. The law now bounds its flight
by loss the way BBRv2 does, with the erasure channel kept apart: the erasure
it compensates for is learned only from rounds with little queue, and loss
beyond that while a queue stands near full cuts the flight to 0.7, holds it
for two to three seconds and probes back up. Random loss, which is the same
at any queue depth, and a policer, which drops with no queue, do not trigger
it. On an emulated 300 Mbit/s, 53 ms link with a one-product buffer, two
such senders went from 16-24% of packets dropped and a queue pinned at 55 ms
to 0.1-0.2% and a median 26 ms, at a higher combined rate; against the
per-connection sender their share went from 77% to 47%; and a light leaf's
300 KB fetches beside three loaded leaves went from a median 991 ms to
148 ms. The per-connection controllers are unchanged.

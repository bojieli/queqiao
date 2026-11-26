Under `--recovery=carrier`, a client notices a path that comes back from a
blackhole within about a second. It sends a small probe exchange every
second under a verification grant while its carrier is suspect, or while an
ack-eliciting packet (a keep-alive PING counts) has gone unanswered, with
nothing heard, for a second beneath open flows. Before,
it waited for the next exponentially backed-off probe timeout, or, on a
carrier carrying mostly downloads, for the one keep-alive PING sent early in
the outage to come round again. While there is no carrier at all, it starts a
fresh replacement handshake every two seconds instead of leaving one
handshake waiting out its retransmission backoff. On an emulated 20 Mbit/s
path with +150 ms of round trip, traffic moved again on average 0.7 s
(worst 0.8 s) after an 8 s blackhole ended, against 3.2–3.5 s (worst 5.8 s)
before; a download alone resumed after 1.7–2.0 s (worst 2.1 s), against 5.1 s.

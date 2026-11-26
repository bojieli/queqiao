Under `--congestion-scope=path` with `--recovery=carrier`, a burst that
follows a pause no longer starts from a collapsed bandwidth estimate. An idle
carrier still carries keep-alives and the odd small frame, and the path's
estimate ages out after ten round trips. Two things then replaced it with a
trickle. A lone small packet acknowledged during the pause read as a few
hundred bytes a second and became the estimate. And a burst that began while
a keep-alive was in flight was measured from the last acknowledgement before
the pause, because the delivery sampler restarted only when the path had
nothing in flight. On an emulated 20 Mbit/s path with 120 ms of round trip, a
video player fetching a 1.5 MB segment every 4 s saw a segment take 2.4 to
4.8 s instead of 1 s in about two sessions out of five. The sampler now
starts a flight whenever none of its own sample packets is outstanding, and
an application-limited sample replaces an aged-out estimate only if it saw
more than a minimum window delivered.

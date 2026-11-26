Under `--congestion=path`, a path's bandwidth estimate no longer collapses to
a ProbeRTT trickle when it ages out. The estimate expires after ten minimum
round trips of wall time, which on a path with a standing queue is sooner
than ten rounds, so it expired in steady state, and expiry replaced every
sample in the filter with the one that happened to arrive. When that sample
came from ProbeRTT's four-packet window, a 20 Mbit/s path was estimated at
0.3 Mbit/s, and bandwidth probing took about twelve seconds to climb back.
Expiry now forgets only the samples that outlived the window and keeps the
younger ones. On an emulated 20 Mbit/s path with 150 ms of round trip and a
512 KiB queue, a bulk upload beside an interactive exchange held 18 Mbit/s
with prompt responses within 90 ms, where it had fallen to 8 Mbit/s with
responses of up to 1.0 s. The per-connection controllers are unchanged.

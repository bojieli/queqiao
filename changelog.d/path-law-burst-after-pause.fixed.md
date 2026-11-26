Under `--congestion-scope=path` with `--recovery=carrier`, a burst that
follows a pause no longer starts from a collapsed bandwidth estimate. An idle
carrier still carries the odd small frame, and the path's estimate ages out
after ten round trips. A lone small packet acknowledged during the pause then
read as a few hundred bytes a second and became the estimate, and the next
burst was paced from it. On an emulated 20 Mbit/s path with 120 ms of round
trip, a video player fetching a 1.5 MB segment every 4 s saw a segment take
2.4 to 4.8 s instead of 1 s in about two sessions out of five. An
application-limited sample now replaces an aged-out estimate only if it saw
more than a minimum window delivered.

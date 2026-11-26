The stage 9 DC gate can now judge a short path. A request wave counts as
concurrent when the leaves overlap for half of the shortest leaf's time in
the wave (at least 20 ms, at most 0.1 s), instead of a fixed 0.1 s that a
warm request across a 53 ms hop never lasts, so latency and spread were never
measured there. `[dc] calibrate_repeats` runs each leaf's solo several times
and takes the median, so one collapsed run cannot void the binding check, and
`calibrate_arms = all` records every arm's single-connection solo rate. The
report now prints the uplink binding ratio and a table of every solo run.

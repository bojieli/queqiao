On Windows the QUIC stack now reads the performance counter instead of Go's
clock, which advances once per clock interrupt there (15.6 ms by default).
At that granularity a controller measured a short path's round trips as zero
or one whole step and dropped nearly every delivery-rate sample. The path
controller (`--congestion-scope=path`) then settled at a few packets per step
on loopback and LAN paths, and a transfer stalled.

On Windows the QUIC stack now reads the performance counter instead of Go's
clock, which advances once per clock interrupt there (15.6 ms by default).
At that granularity the path controller, the default since this release,
measured a short path's round trips as zero or one whole step and dropped
nearly every delivery-rate sample: on loopback and LAN paths it settled at a
few packets per step and a transfer stalled.

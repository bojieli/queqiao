Two UDP association tests no longer fail on Windows runners. They bound a
loopback port for TCP and then asked for the same number on UDP, which is
what a gateway offering QUIC beside a TCP fallback does, but the two
protocols have independent namespaces and an ephemeral TCP port can land
inside a range Windows has reserved for UDP. They now use the shared helper
that already solved this for the rest of the suite.

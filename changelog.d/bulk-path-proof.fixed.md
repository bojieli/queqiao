Validate expired bulk-pool connections with an authenticated, bounded path
probe before opening another lane. Failed exclusive entries leave the pool
without closing control-pool siblings; fresh handshakes and hot reuses avoid
extra probes. Remote proof freshness is independent of interface polling.

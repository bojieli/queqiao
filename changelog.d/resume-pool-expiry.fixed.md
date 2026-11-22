Expire cached QUIC control and bulk connections after macOS or Linux suspend
exceeds the transport idle budget, even when the local address is unchanged.
The existing uplink watcher and first pool borrower share the same reset, so
concurrent requests rebuild one generation rather than reuse pre-sleep paths.

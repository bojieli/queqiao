`--cc-api=v2`, the first bundle of the path-authority migration, drives each
QUIC connection's erasure controller through the fork's new PathSender API
instead of quic-go's per-packet callbacks. It changes exactly three things.
QUIC ACK-only packets are no longer charged to the pacer, so a connection that
mostly receives no longer spends the budget its own flow acknowledgements need
on traffic its rate estimate never measured. A NAT rebinding or other
connection migration keeps the controller and what it has measured, where
quic-go replaced it with Reno. Acknowledgements, probe-timeout probes, the
keep-alive PING and path validation can still be sent while the connection is
window-blocked or paced. The default stays `v1`, which behaves as before.
`v2` requires `--congestion=erasure` and is refused with the `dc-long-haul`
profile.

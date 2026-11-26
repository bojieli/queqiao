The flags that selected between the per-flow design and the path-authority
architecture are gone, with the code only the per-flow design used:
`--cc-api`, `--sched`, `--congestion-scope`, `--coded-retry`, `--mem`,
`--classifier`, `--recovery`, `--topology`, `--topology-fanout` and
`--quic-pool` on the client, `--gateway-admission` on the gateway, and
`--congestion`, `--brutal-bytes-per-sec`, `--adaptive-min-bytes-per-sec` and
`--adaptive-max-bytes-per-sec` on both. QUIC always runs the erasure law
under its path controller, and the client always keeps one pooled
connection per path. A command line that still names one of these flags is
refused at start-up; remove it before upgrading. The mobile core's
`SetMemoryBundle` is removed as well. To go back to the per-flow design,
install the previous release.

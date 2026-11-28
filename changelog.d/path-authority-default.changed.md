The gateway and the client, desktop and mobile, now run the path-authority
bundle by default (stage 10 of the path-authority design): `--cc-api=v2`,
`--sched=class`, `--congestion-scope=path`, `--coded-retry=sticky`,
`--recovery=carrier`, `--mem=owned` and `--classifier=phased` on both roles,
`--gateway-admission=owned` on a gateway and `--topology=single` on a
client. `dc-long-haul` runs the same bundle. The wire does not change: an
upgraded gateway keeps serving wire-1 clients and clients on the legacy
bundle, so upgrade gateways first. The new `--bundle=legacy` turns every
switch off at once and is the rollback while the new defaults soak, until
the legacy code is deleted. A switch given explicitly still overrides
the bundle, and start-up refuses an unsupported combination.
`--congestion` other than `erasure` and a client's `--quic-pool=false` now
need `--bundle=legacy`, and start-up says so. `queqiaod service install`
takes `--bundle` to install a client service on the legacy bundle.

`--path-profile dc-long-haul` can now run on the path-authority architecture:
`--cc-api=v2 --sched=class --congestion-scope=path --coded-retry=sticky
--recovery=carrier`, with `--topology=single` on the client, and optionally
`--classifier=phased`, `--mem=owned` and `--gateway-admission=owned`. Each
endpoint pair gets one path authority, and the profile's path tree is used
only as knowledge: a new destination starts from the erasure, burst and
round-trip figures its uplink has already measured, until it has measured
enough of its own, and no tree node caps or seeds a rate. The profile still
defaults to the legacy bundle, and start-up refuses any other combination of
these switches with it. The stage 9 DC gate's example campaigns now run this
bundle as their candidate, and `dc_gate.py` voids a campaign whose candidate
or legacy arm ran a different bundle.

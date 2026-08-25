A `--path-profile` flag on the client and gateway selects the deployment a
process is running in. `wan-shared-bottleneck` is the default and is unchanged;
`dc-long-haul` is experimental and adjusts flow classification for a leg where
every flow is a latency-critical request rather than a transfer.

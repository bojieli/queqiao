`--path-profile` selects which deployment a client or gateway is running in.
`wan-shared-bottleneck` is the default and behaves as before; `dc-long-haul` is
experimental and changes how flows are classified on a hop where every flow is
a latency-sensitive request.

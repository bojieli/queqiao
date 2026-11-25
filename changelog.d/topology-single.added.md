`--topology=single`, the seventh bundle of the path-authority migration,
keeps every flow of a client a stream on one QUIC connection per path. A bulk
flow no longer moves to a connection of its own when another flow shares the
pooled one; the path controller's class scheduler keeps control and
interactive work ahead of bulk on the one connection instead. Per-flow rescue
still dials, and a replacement connection is an attachment of the same path
controller. `--topology-fanout=N` is an explicit, off-by-default fan-out to at
most N connections per path for bulk flows. With fixed mobile receive
windows, the one connection's window grows by a stream window for each
secondary connection it no longer opens, so it holds as much in flight as the
isolated connections did. It requires `--congestion-scope=path` and
`--quic-pool`; the default stays `isolated`, which behaves as before.

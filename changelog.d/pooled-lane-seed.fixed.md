A QUIC lane joining a path its siblings have already measured now starts from
the pooled delivered rate with erasure compensation applied once (it was
applied twice), starts its compensation from what its siblings earned rather
than dropping it at the first loss, and reports only delivery it has measured
itself to the shared path model, so a join no longer inflates the pooled
bottleneck with its own seed.

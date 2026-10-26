The coded path keeps reading QUIC datagrams while it decodes and while the
flow consuming its frames is slow. Reading and decoding shared one goroutine,
so either stalled the reads; QUIC then discards received datagrams beyond
128 after acknowledging them, which the sender never learns of and this end
counted as path erasure, inflating the parity the peer sent for a channel
that had delivered everything.

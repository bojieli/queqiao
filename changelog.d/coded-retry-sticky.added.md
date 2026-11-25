`--coded-retry sticky` enables the coded-retry bundle of the path-authority
migration on clients and gateways; it requires `--cc-api v2`. A chunk sent on
coded datagrams is the code's to repair only until a deadline that now starts
when its last datagram is built into a packet, not when the chunk is queued,
so a chunk waiting behind the connection's datagram queue is no longer retried
early. A chunk whose deadline passes, whose datagram the connection discards,
or that the peer's ranges show missing is retried on the flow's existing
stream, and every later attempt of that chunk stays on the stream. A coded
tail lost on a healthy connection is therefore recovered without waiting for
the stall watchdog. The default, `--coded-retry legacy`, keeps today's
behaviour; any other value, or `sticky` without `--cc-api v2`, is refused at
start-up.

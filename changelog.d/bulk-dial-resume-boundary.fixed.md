Reject bulk connections whose handshake spans a system suspend before
publishing them into the pool, so wake-up recovery cannot reuse an old path.

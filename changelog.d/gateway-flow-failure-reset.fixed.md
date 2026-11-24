A gateway flow that ends with an error now sends RESET, with a short reason,
on every lane still open before it closes them. The client used to see only
the streams end, take that for a dead path, and ask to join a replacement lane
to a session the gateway had already dropped, failing the flow about half a
second later with `peer does not hold this session`. It now ends the flow at
once with `peer reset flow: flow ended: <reason>`.

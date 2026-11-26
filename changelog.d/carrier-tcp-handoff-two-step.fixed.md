Under `--recovery=carrier` with `--transport=auto`, a flow is committed to
TCP only when its JOIN is sent, after a bounded wait for QUIC whenever the
path was away for TCP as well. The earlier fix held back a handoff retry
after one had timed out, but a handoff that began during the outage never
times out: its connection waits and completes the moment the path returns,
and its JOIN then committed the flow. The handoff now dials and
authenticates its TCP lane first; if an earlier attempt timed out, or the
dial took longer than a handshake should, it waits about one handshake (and
never past the latest handoff start) for a QUIC carrier, and if one becomes
eligible the lane is closed with no JOIN sent. On an emulated 20 Mbit/s,
360 ms path dropped for 8 s and for 25 s with TCP dropped too, both
downloads were handed to TCP in six trials of six before, and in none of
three after. With UDP alone blocked the handoff still comes 18.0 s after
the block, as before.

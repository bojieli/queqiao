`--gateway-admission owned` makes a gateway's `--max-sessions` count flows
rather than streams: the ceiling is charged at OPEN on QUIC and TLS/TCP, a
JOIN counts against its flow's lane limit, a PROBE against a per-connection
allowance, and a refused OPEN is answered with the flow-limit code. A
RESERVE_CONTROL JOIN retires the lane that held the control role, and a UDP
association whose replacement arrives while it still runs hands over its relay
and its slots. The default, `legacy`, keeps the per-stream charge.

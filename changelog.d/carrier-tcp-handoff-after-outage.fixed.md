Under `--recovery=carrier` with `--transport=auto`, flows in flight across
an outage are no longer handed to TCP as the path returns. A flow with no
QUIC carrier tries a TCP handoff once the fallback grace has passed and
retries it on a backoff; in an outage those attempts time out, because TCP
is down as well. When the path came back, the next retry and the QUIC
replacement came up within the same second or two, and a TCP JOIN that
landed first committed the flow to TCP for the rest of its life: on a live
path dropped for 25 s every flow in flight went to TCP 1.2 to 2.1 s after
the restore, and a long download then moved 14 s after it. Once a handoff
attempt has timed out, a retry now waits for a QUIC replacement that is
being dialled, for about one handshake and never past the latest handoff
start, and goes ahead only if QUIC does not come up. A network that blocks
UDP is unaffected: its first handoff succeeds, so nothing waits.

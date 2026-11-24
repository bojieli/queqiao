A lane join that reaches the gateway while the flow's OPEN is still dialling
its destination now waits for the OPEN to be decided, and its acknowledgement,
instead of being refused as an unknown session. When the OPEN's own stream
dies during the dial and a join is waiting, the gateway keeps the flow for the
replacement instead of closing it, and a join admitted before the OPEN's
acknowledgement now confirms the flow on the client. A join still refused
before OPEN_OK fails the flow with `peer refused this flow before confirming
its open` rather than `peer does not hold this session`. The gateway now tells
a session it held and ended (`session_ended`) from one it never held, and
refuses an OPEN for a session it remembers ending. The failure kinds
`open_unconfirmed`, `open_failed` and `open_pending` and the gateway refusal
reasons `session_pending`, `open_failed` and `session_ended` keep these cases
apart from a lost session.

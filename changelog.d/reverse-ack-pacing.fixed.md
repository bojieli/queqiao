A download's flow acknowledgements no longer queue behind the client's
upstream pacer. QUIC's ACK-only packets, which are sent past the pacer, are no
longer charged to its budget, and a lane that is not sending all it is allowed
to -- such as an upstream connection carrying only acknowledgements -- is no
longer held to a share of the pool sized from its own trickle. Before, a
download's acknowledgements could arrive seconds late on a lossy path, and the
gateway's sender, clocked by them, read the wait as a stalled flow.

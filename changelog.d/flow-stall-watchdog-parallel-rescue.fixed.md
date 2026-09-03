Flows stalled on one-way loss now recover in seconds instead of failing
after a minute. A lane used to be declared dead only on I/O error — fifteen
seconds of total receive silence — so on a path erasing the upstream
direction, downstream keepalives kept every connection nominally alive
while flows with a full send window made no progress at all. A per-flow
stall watchdog now measures forward progress where delivery to the peer is
actually recorded (the acknowledged send offset and payload arrival) and,
finding none for three minimum round trips while work is pending, demotes
the lane and starts a rescue beside it: the suspected lane keeps receiving
and is fully eligible again the moment an acknowledgement arrives on it.
The rescue dials up to three concurrent JOIN attempts sprayed uniformly
across the hop-port pool — or across independent handshakes on the single
port when hopping is off — and the first completed JOIN wins. The gateway
protects a freshly admitted lane from eviction at its lane ceiling, so the
losing JOINs of a parallel rescue can no longer retire the winner's server
side; such a late JOIN is refused by capacity instead, which the client
treats as one lost attempt rather than a lost session. A gateway that sees
a rescue JOIN arrive while a flow is waiting out its replacement grace now
restarts that grace instead of letting it expire mid-handshake, and
--handshake-timeout no longer overrides the client's safer 30-second
default with 10 seconds.

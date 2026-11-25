On macOS, a QUIC dial with no local binding now listens on a socket of the
gateway's own address family rather than a dual-stack one. The kernel could
give the dual-stack socket a port that an IPv4 socket elsewhere on the host
already held, and that socket then received the gateway's answers, so the
handshake retransmitted into silence until the dial timed out. A TCP flow's
lane manager also no longer dials a replacement after the flow has ended,
which it could do when its timer fired as the flow's grace ran out; that dial
was missing from the flow's completion record.

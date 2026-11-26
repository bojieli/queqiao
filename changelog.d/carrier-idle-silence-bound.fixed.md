Under `--recovery=carrier`, connections left idle no longer die after a
stall on the path. A stall leaves round-trip samples of many seconds in an
idle carrier's estimate, and QUIC spaces its keep-alives and sets its idle
timeout by that estimate (1.5 and 3 probe timeouts): in a live idle-wake
session the client's keep-alives were 50 s apart and it noticed a dead
carrier 152 s after the gateway did, long after the gateway's 45 s grace for
the flows, so every idle connection ended. The carrier manager now keeps two
wall-clock bounds on a carrier with open flows. One that has heard nothing
for 7.5 s is probed, which keeps a NAT or relay mapping alive and gives the
estimate a fresh sample; one that has heard nothing for 15 s after its first
unanswered packet is treated as dead and replaced, as QUIC's own idle
timeout does when its estimate is sound. A healthy idle carrier is not
probed and sends what it sent before: one keep-alive every five seconds.
`queqiao_carrier_silent_deaths_total` counts the carriers ended this way.

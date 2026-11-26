Under `--congestion=path`, ProbeRTT now holds half a bandwidth-delay product
in flight, as BBRv2 and BBRv3 do, instead of four packets. At four packets
the path carried nothing for 200 ms and a round trip every time the minimum
round trip was re-measured, and everything on the path waited behind it. On
an emulated 20 Mbit/s, 120 ms path, a call's downlink audio beside a
tunnelled download waited up to 440 ms for about half a second at each
ProbeRTT, which put its 99th-percentile lateness at about 310 ms; it is now
about 80 ms, with a peak of about 145 ms, and late or lost audio fell from
about 1.8% to under 0.1%. Half a product is less than the pipe holds, so the
sender's own queue still drains and the round trip measured is the path's.
The per-connection controllers keep the four-packet window.

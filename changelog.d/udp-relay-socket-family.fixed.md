The gateway's direct UDP relay now uses one socket per address family
instead of one dual-stack socket: an IPv4 socket opened with the relay, and
an IPv6 socket opened on the first IPv6 destination. On macOS a dual-stack
socket could be given a port that an IPv4 socket elsewhere on the host
already held, and that socket then received the destination's replies. The
socket to an upstream SOCKS5 relay takes the upstream's address family.

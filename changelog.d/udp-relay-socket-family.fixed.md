On macOS, the gateway's direct UDP relay now uses one socket per address
family instead of one dual-stack socket: an IPv4 socket opened with the
relay, and an IPv6 socket opened on the first IPv6 destination. A
dual-stack socket there could be given a port that an IPv4 socket elsewhere
on the host already held, and that socket then received the destination's
replies. Other platforms keep the single dual-stack socket. On every
platform, the socket to an upstream SOCKS5 relay takes the upstream's
address family.

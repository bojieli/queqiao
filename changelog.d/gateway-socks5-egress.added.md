The gateway can forward destination traffic to a trusted loopback SOCKS5
router with `--outbound-socks5`. This lets an operator use sing-box or another
local router to send selected services through a designated proxy while other
traffic leaves directly from the gateway. TCP CONNECT and UDP ASSOCIATE retain
original destination names for domain routing, after gateway-side destination
validation. The option is disabled by default, requires no client or wire
protocol change, and never falls back to direct egress if the router fails.

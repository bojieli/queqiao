The gateway can forward destination traffic to a trusted loopback SOCKS5
router with `--outbound-socks5`. This lets an operator use sing-box or another
local router to send selected services through a designated proxy while other
traffic leaves directly from the gateway. TCP CONNECT and UDP ASSOCIATE use
validated destination IPs by default. Private-access services can forward
original names for domain routing. The option is disabled by default, uses
the existing client and wire protocol, and fails closed if the router fails.

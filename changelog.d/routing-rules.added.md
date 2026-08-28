The iOS client routes by rule rather than only by address. A profile carries
a rule list in the `TYPE,VALUE,ACTION` syntax that Clash, mihomo, sing-box and
Shadowrocket all read, so an existing list can be pasted in: `DOMAIN`,
`DOMAIN-SUFFIX`, `DOMAIN-KEYWORD`, `IP-CIDR`, `GEOIP` and `DST-PORT` choosing
between `PROXY`, `DIRECT` and `REJECT`, first match wins. A flow no rule
matches takes the tunnel, so a list that fails to load cannot put traffic on
the open path. The editor counts what loaded and names every line that did
not, by number, before you connect. There is one tunnel, so a rule naming a
proxy group is refused rather than quietly read as `PROXY`.

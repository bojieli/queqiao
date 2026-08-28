The Android debug build reads the same rule list as iOS, from
`routing-rules.conf` in its files directory, and carries the same packed
country set for `GEOIP` rules. It is a development affordance rather than a
shipped feature: the released Android app is not a VPN, declares no
`BIND_VPN_SERVICE`, and continues to hand routing to whichever consumer client
owns the device tunnel.

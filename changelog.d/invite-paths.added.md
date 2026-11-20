`queqiaod provider invite` accepts a repeatable `--path NAME=HOST:PORT` that
issues one invitation per route to the gateway, such as a direct address and
relays in front of it, in a single run. Each invitation pins the same gateway
and root and only dials the named endpoint; the URIs are printed one per line
so a mobile app can take them in one paste. Operators no longer edit an
invitation's endpoint by hand to enroll through a relay.

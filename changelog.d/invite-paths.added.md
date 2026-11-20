`queqiaod provider invite` accepts a repeatable `--path NAME=HOST:PORT` that
issues one invitation per route to the gateway, such as a direct address and
relays in front of it, in a single run. Each invitation pins the same gateway
and root, and the gateway records its route and answers enrollment with it, so
the enrolled profile keeps dialing the relay and existing clients accept it
unchanged. The URIs are printed one per line so a mobile app can take them in
one paste. Before this, an invitation whose endpoint was edited by hand to
point at a relay consumed its token and then failed enrollment.

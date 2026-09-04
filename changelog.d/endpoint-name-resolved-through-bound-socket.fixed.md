A gateway endpoint written as a hostname is now resolved over a socket bound
the same way the lane will be. `--local-address` applies to the sockets the
client creates, but a dialer's `LocalAddr` and `Control` never reach the
lookup that turns the endpoint name into the address it connects to, so that
one question went out over an unbound socket. A host whose transparent proxy
redirects port 53 into this very client could then not answer it: the name
has to resolve before the datapath exists, and the datapath has to exist
before the name resolves, so both transports failed with `no such host` and
the provider never came up. Endpoints written as literal addresses were never
affected, and still take a path with no lookup in it.

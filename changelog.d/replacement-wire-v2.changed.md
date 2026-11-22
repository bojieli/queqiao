Move explicit replacement JOINs to wire version 2 and data ALPN queqiao/2.
TCP and QUIC reject mixed versions during TLS negotiation. Clients and
gateways require paired upgrades; the protocol documentation describes
parallel-endpoint staging and rollback. Preserve version-1 vectors and add
version-2 conformance vectors.

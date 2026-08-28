A new guide, [choosing and placing a gateway](docs/CHOOSING-A-GATEWAY.md),
covers the decision that precedes deployment and previously had no home in
the documentation. It states plainly that both ends run this software, so a
destination someone else operates needs a gateway on a host the reader
controls; it gives the placement rule that follows from the transport
improving the client-to-gateway segment and nothing past it; it works
through the anycast case, where a destination already served from an edge
near the caller is made slower by a gateway on another continent; and it
promotes the free client-side fixes out of the datacenter profile's
appendix, where they applied to every reader but only one audience could
find them.

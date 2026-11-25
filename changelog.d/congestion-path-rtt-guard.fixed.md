`--congestion-scope=path`: after a connection moves to another path
controller -- by migration or because its path changed before installation
-- round-trip samples count only for its own packets on the new path, for as
long as it stays there, and its old round trip does not seed the new
controller. Outcomes released when a connection is demoted for probe
timeouts are published at once, and releasing records without an outcome
publishes nothing.

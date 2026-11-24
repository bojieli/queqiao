Gateways and clients negotiate the wire version per connection by ALPN
instead of requiring a paired upgrade. A gateway accepts versions 1 and 2
(`--accept-wire`, default `1,2`) and a client offers both, preferring 2
(`--wire-versions`, default `2,1`), so gateways can be upgraded ahead of their
clients: v0.12.x clients keep working against a current gateway on version 1,
and a current client works against a v0.12.x gateway. A flow keeps the version
it opened on; a lane joining it on the other version is refused with the
`wire_mismatch` reason. Version-1 rescues use the plain JOIN, since
`REPLACE_LANE` exists only on version 2. `--version` and `BUILDINFO` add
`wire_accept=1,2`, and the gateway reports
`queqiao_data_connections_total{wire}` and
`queqiao_wire_active_flows{wire}`, and a `wire` field on each flow's log
records.

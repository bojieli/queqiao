Name rules work on a packet tunnel, which needed the core to answer lookups
itself. A query is answered from `198.18.0.0/15`, the address becomes that
name's handle, and when the connection arrives the handle is reversed so the
rule list sees the name rather than an address. A proxied flow is then dialled
by name, so the gateway resolves it from the vantage the flow is being sent to
use; a `DIRECT` flow is resolved on the device, which is the vantage that
makes the answer right. This is what the bundled China set could not do on its
own: `docs/KNOWN-LIMITATIONS.md` recorded that a Chinese domain resolved
through the tunnel can answer with a CDN address outside the registry set and
take the tunnel anyway, and the bundled China preset now pairs `DOMAIN-SUFFIX`
rules with `GEOIP,CN` rather than relying on either alone.

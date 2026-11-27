A regression test, `TestGatewayFirstFlightBesideBulkDownloads`, now guards the
gateway's send path: beside two loaded clients' downloads on one gateway, a
light client's 300 KB response must be on the path whole within 45 ms of its
first byte, before any acknowledgement of it can return, under the legacy and
the path-authority bundles. The live DC gate's slow control-leaf probes were
first read as a capped first flight; the gateway's snapshots show every chunk
issued and the whole response in flight, and the slow probes are the ones that
lost a packet and paid a round trip to repair it. A shared-bottleneck variant
only reports. The test bed gains a helper for one gateway with several
enrolled clients.

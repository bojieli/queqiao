The gateway and the client, desktop and mobile, run the path-authority
architecture (docs/DESIGN-PATH-AUTHORITY.md), which replaces the per-flow
design they ran before. The wire does not change: an upgraded gateway still
serves wire-1 clients and released clients, so upgrade gateways first.

- One congestion controller per path and sending direction. Every QUIC
  connection on a path attaches to it and asks it to admit each packet, so
  the rate, window, erasure estimate and minimum round trip are decided once
  for the path; nothing is divided between connections. It drives the erasure
  law through the QUIC fork's PathSender API: acknowledgement-only packets
  are not charged, a NAT rebinding keeps the controller, and acknowledgements,
  probe-timeout probes, keep-alives and path validation can still be sent
  while the window is full. On a gateway every data connection of one device,
  a released client's isolated and rescue connections included, attaches to
  the same controller. A path that has been application-limited since its
  minimum round trip was measured, such as the uplink beside a download,
  takes no ProbeRTT, and at a standing queue the path's flight is bounded by
  loss, so two senders no longer hold a shared bottleneck's queue full. On
  Windows the QUIC stack now reads the performance
  counter: Go's clock there advances once per clock interrupt (15.6 ms by
  default), too coarse for the controller to measure a short path.
- Class scheduling. The controller chooses whether the next packet carries
  CONTROL (flow control frames and QUIC window updates), INTERACTIVE (new and
  interactive flows' data, the datagram substrate) or BULK work, by weighted
  deficit round robin (4:2:1), and a flow whose acknowledgement waits behind
  its own data has that stream boosted to CONTROL.
- One QUIC connection per path on a client. A bulk flow no longer moves to a
  connection of its own; class scheduling keeps control and interactive work
  ahead of it. With fixed mobile receive windows, the connection's window
  grows by one stream window per connection it no longer opens.
- Carrier recovery. The client's carrier manager is the single owner of QUIC
  recovery: a connection is replaced only on its own transport evidence
  (probe timeouts without progress) by promoting one verified standby, a flow
  whose stream failed is rebound by one JOIN on the live connection, and
  missing flow credit on a live stream is waited out rather than rescued with
  a dial. A path back from a blackhole is noticed within about a second. Each
  unbound flow has a binding deadline derived from the gateway's grace
  (`--recovery-ks`, `--recovery-kp`, `--recovery-skew`,
  `--recovery-handoff-budget`), and the counters are exported as
  `queqiao_carrier_*` metrics. TCP flows keep their bundle's own recovery.
- Coded retry. A chunk sent on coded datagrams is the code's to repair until
  a deadline that starts when its last datagram is built into a packet; then,
  or when its datagram is discarded or the peer's ranges show it missing, it
  is retried on the flow's stream and stays there.
- Owned memory. The shared send budget is granted to waiting flows in turn,
  a flow's retention follows its own flow credit, and bytes read but not yet
  sent count as pending work for the no-progress budget. A flow may have one
  path window unacknowledged from its first byte and again after an idle gap,
  and it keeps its bound through a pause of its application.
- Phased classification. A flow that carried a bulk transfer becomes
  interactive again once it stops: its data class, queue and coding follow
  what it does now, and a small write onto an empty queue is sent interactive
  and coded.
- Gateway admission. `--max-sessions` counts flows, charged at OPEN, rather
  than streams: a JOIN counts against its flow's lane limit and a PROBE
  against a per-connection allowance, so a full gateway still admits the
  recovery work that frees it, and a refused OPEN gets the flow-limit code.
- `dc-long-haul` runs the same architecture, with its path tree used only as
  knowledge: a new destination starts from the erasure, burst and round-trip
  figures its uplink has measured.

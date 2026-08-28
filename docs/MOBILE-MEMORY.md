# Mobile memory architecture

Queqiao's mobile data plane treats memory as fixed hardware capacity. More
connections may increase latency or cause bounded admission failure, but they
must not multiply retained payload memory without limit.

## Invariants

- All stream chunks retained for retransmission share one transmit byte
  budget. A producer acquires a full chunk before allocating it and stops
  reading when the budget is full.
- All out-of-order stream segments share one receive byte budget. A receiver
  never blocks every lane while waiting for receive memory: if it cannot retain
  a gap, it fails that flow and releases its reservations.
- A flow also has a smaller ceiling, so one bulk transfer cannot monopolize the
  shared arena.
- Per-flow queues have tiny fixed frame counts and payload sizes, and the
  endpoint has a fixed session ceiling. Coded datagrams have one
  connection-level reader and tiny per-flow mailboxes; overflow is packet loss
  and is repaired by the existing flow protocol when appropriate.
- Control frames have reserved lane slots. Bulk data cannot consume those
  slots, so memory pressure cannot prevent ACK, close, reset, or rescue
  progress.
- QUIC receive windows have explicit maximums on mobile. Auto-tuning cannot
  grow them past the platform profile.
- gVisor endpoint buffers, the TUN link queue, UDP working buffers, active
  sessions, pending opens, and secondary QUIC connections all have explicit
  bounds.
- The Go runtime memory limit is a final safety backstop, not the primary flow
  controller. The byte budgets provide deterministic backpressure before the
  garbage collector is under pressure.

## Capacity profiles

| Resource | iOS packet extension | Android VPN service |
| --- | ---: | ---: |
| Go runtime soft limit | 28 MiB | 72 MiB |
| Shared retained transmit payload | 3 MiB | 8 MiB |
| Shared out-of-order receive payload | 3 MiB | 8 MiB |
| Per-flow transmit ceiling | 1 MiB | 2 MiB |
| Per-flow receive ceiling | 1 MiB | 2 MiB |
| Active TCP/UDP sessions | 1,024 | 128 |
| Pending remote opens | 128 | 32 |
| QUIC connection receive window | 2 MiB fixed | 4 MiB fixed |
| QUIC stream receive window | 64 KiB fixed | 1 MiB fixed |
| QUIC peer-initiated streams | 32 | 64 |
| Secondary bulk QUIC connections | 1 | 2 |
| Coded-path send/receive mailbox | 2 frames each | 4 frames each |
| TUN link descriptors | 64 | 64 |
| Platform-to-Go packet queue | 64 packets / 128 KiB | kernel TUN backpressure |

The limits intentionally favor survival and interactive traffic over maximum
bandwidth-delay-product utilization. They can be changed only as a complete
profile: raising a session or queue count requires redoing the worst-case
memory calculation.

On iOS the Go runtime limit is not the binding constraint. NetworkExtension
enforces a 50 MiB ceiling on the whole packet-tunnel process, and
`SetMemoryLimit` governs Go-owned memory only, so the profile leaves 22 MiB of
that ceiling unclaimed for the resident text pages of the statically linked Go
and gVisor code, runtime metadata, goroutine and thread stacks, the Swift packet
bridge, and CoreFoundation. Crossing the ceiling is not a soft failure: jetsam
SIGKILLs the extension with reason `per-process-limit`, so the provider never
runs `stopTunnel`, never records a diagnostic, and leaves its network settings
installed — the VPN keeps showing connected while no packets move. A Go limit
set too close to the ceiling also makes the collector run continuously against a
limit the process cannot honour, which burns CPU without averting the kill.
`mobile/core/resources_test.go` holds the limit below the ceiling with that
remainder reserved.

## Overload behavior

- TCP and stream producers stop reading, propagating ordinary receive-window
  backpressure to the application.
- New sessions or pending opens beyond their limits are rejected promptly.
- UDP and coded-datagram descriptor overflow drops packets; these substrates
  already permit loss.
- A receive flow that exhausts the shared out-of-order arena is terminated
  rather than deadlocking the connection or growing memory.
- iOS stops calling `readPackets` above its high watermark and resumes below
  its low watermark. Its fixed circular queue never reallocates or retains
  dequeued `Data` values.
- Android responds to system memory-pressure callbacks by returning idle Go
  pages, while correctness continues to rely on the hard budgets.

## Telemetry and validation

Mobile metrics JSON version 2 includes the selected capacity profile, Go heap
allocation/in-use values, and exact capacity/current/peak/waiter values for the
shared transmit and receive budgets. A production soak should exercise many
short flows, stalled flows, UDP bursts, app backgrounding, path changes, and
repeated connect/disconnect cycles while verifying that the peaks plateau.

The unit suite covers concurrent budget admission, cancellation, release,
cross-flow reassembly pressure, fixed QUIC windows, mobile profile invariants,
and packet forwarding. Mobile release builds additionally run the Go race
detector before regenerating each platform binding.

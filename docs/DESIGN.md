# Queqiao design

> [!NOTE]
> **Status:** Current technical design reference
>
> **Applies to:** Public protocol 1
> **Last reviewed:** 2026-08-20

This is the technical deep dive behind the user-facing explanation in the
[repository README](../README.md) and [vision](VISION.md). Queqiao is a WAN
optimization protocol for two known tunnel endpoints
whose shared long-haul segment is the dominant bottleneck for many application
flows. It combines a shared directional path model, erasure-aware congestion
control, selective forward-error correction, byte-offset recovery, and
cross-flow scheduling in one authenticated proxy.

The archived [multipath](archive/2026-08-development/DESIGN-MULTIPATH.md) and
[erasure](archive/2026-08-development/DESIGN-ERASURE.md) notebooks record how
the current design was reached. When they disagree with this document, this
document is current.

We're currently measuring a second deployment: a long hop between two machines
one operator runs, carrying inference requests. The
[datacenter profile design](DESIGN-DC-PROFILE.md) covers when it applies, what
differs, and which of our assumptions the measurements overturned. It's
experimental and you select it explicitly with `--path-profile`. Nothing here
changes for a deployment that doesn't ask for it.

## The topology changes the control problem

A general Internet congestion controller cannot assume two connections share a
bottleneck. They may go to different destinations, leave through different
interfaces, or encounter different queues. Its safe default is therefore
per-connection inference and control.

Queqiao knows more about its deployment:

```text
many application flows
        │
        v
client Queqiao endpoint
        │
        │  one difficult long-haul segment
        │  shared dominant bottleneck
        v
known gateway / relay endpoint
        │
        ├── destination A
        ├── destination B
        └── destination C
```

The destinations diverge after the gateway, but all proxied traffic first
crosses the same endpoint-pair segment. Queqiao can therefore coordinate the
aggregate offered load, share loss/RTT/capacity evidence, and reserve latency
headroom across flows rather than asking each connection to rediscover the same
bottleneck.

This optimization unit is precise, but its applicability is broad:
intercontinental proxies and branch tunnels, remote corporate access, poor
hotel/mobile/residential links to a stable relay, and individual long-haul legs
of an overlay all have this shape. The current implementation exposes client
and provider-gateway roles; a Tailscale-like product can supply discovery and
routing around each optimized pair.

If the dominant bottleneck is beyond
the gateway, differs by destination, or is a public bottleneck where the
operator has no authority to use a non-TCP-friendly policy, the current design
may be inappropriate. The protocol must be evaluated again rather than treating
“high loss” alone as proof that the assumption holds.

## The motivating path has two loss regimes

The path was characterized with an open-loop UDP probe bound to the physical
interface, not by interpreting a transport's own throughput. The full record is
in [PATH-CHARACTER-20260813.md](PATH-CHARACTER-20260813.md).

Downstream, the measured endpoint pair showed:

- roughly 42–45% packet loss even at low offered rate;
- near-independent loss below the capacity knee;
- about 14.5 Mbit/s maximum useful delivery; and
- longer, correlated loss bursts after aggregate offered traffic crossed the
  knee.

Upstream showed no comparable erasure floor and a different capacity limit.
The directions must therefore be modeled independently.

These observations identify two distinct network states:

| Regime | Evidence | Correct response |
| --- | --- | --- |
| Rate-independent erasure | loss persists far below capacity and is approximately memoryless | keep the path fed; repair latency-sensitive gaps with FEC or retransmission |
| Bottleneck overload | additional loss appears near the delivery knee and clusters into bursts | reduce/police aggregate offered load |

One loss event does not say which regime produced it. A controller that treats
every loss as congestion gives away a path whose erasure does not improve when
the rate falls. A sender that ignores every loss overruns the real bottleneck
and turns independent erasure into damaging bursts. Queqiao estimates the loss
floor separately from excess loss and delivery behavior.

## Network design principles

### 1. Treat the endpoint pair as one congestion domain

Connections sharing the client uplink and gateway read a shared path model.
Per-flow byte progress remains separate, but path loss, RTT, delivery rate, and
pacing do not restart from zero for every destination.

The path key includes the local uplink as well as the remote gateway. A switch
from Wi-Fi to cellular creates a new model because it changes the path, even
though the gateway address remains the same.

### 2. Separate congestion from erasure, by what each does to delivery

Congestive behavior is a delivery/queue regime, not a count of individual lost
packets. Erasure scales delivery down proportionally at every rate and cannot
produce a knee; congestion produces one.

Queqiao no longer tries to tell them apart packet by packet. It once classified
each loss and forwarded only the share the channel could not explain, which was
a statistical answer to a question the delivery-versus-rate curve answers
directly, and it was least reliable exactly when loss was worst. Every loss now
reaches the congestion controller, and the brake is a delay bound instead: the
round trip may not exceed twice the path's own minimum. What the measured
erasure is used for is the two things that are proportional to it -- sizing the
code, and compensating the congestion window so that a full one arrives.

The path model still carries two numbers for the channel, because their
consumers fail in opposite directions. A controller must under-estimate
erasure, since mistaking erasure for congestion only makes it slow down; a code
must not, since mistaking erasure for congestion makes it send no parity into a
channel that is erasing. Sizing the code from the controller's conservative
floor is the defect [the control redesign](CONTROL-REDESIGN.md) was written
about.

A policer drops without queueing, so it offers neither signal. That case is
open; see the [known limitations](KNOWN-LIMITATIONS.md).

### 3. Keep total traffic within the bottleneck

On an erasure channel with probability `p`, application-useful delivery cannot
exceed `(1-p)` times the wire rate. Data, parity, and retransmissions all consume
the same bottleneck budget. Queqiao coordinates pacing across the endpoint pair
and reserves part of the bounded queue/rate budget for control and interactive
work.

The paired segment deliberately has no TCP-friendliness requirement. This
permits aggressive recovery through non-congestive erasure, but it does not
remove congestion control: aggregate overload at the measured knee still
requires restraint.

### 4. Choose recovery for the WAN RTT

Automatic repeat request (ARQ) resends only what was lost and is therefore
byte-efficient. On a long path it may also add one or more complete round trips
to useful delivery. Sliding-window FEC spends additional wire bytes so a
receiver can reconstruct some gaps without waiting for feedback.

Queqiao can use coding while avoiding another RTT is worth its parity cost and
return DATA to the reliable stream as a flow grows. This is a cross-cutting
policy decision inside the same logical flow, not a separate “short-flow
protocol” and “bulk protocol.” Residual coded loss is handled by the byte-offset
replay machinery already required for carrier failure.

### 5. Keep feedback from waiting behind data

A QUIC stream is reliable but ordered: one missing transport packet can hold
later stream bytes. Queqiao keeps OPEN, ACK, CLOSE, RESET, and recovery control
on the reliable stream while selected DATA frames use coded datagrams. An ACK
therefore does not wait inside the coded-data queue whose sender it releases.

The same rule applies across contending flows. Priority queueing and reactive
isolation keep a growing data stream from trapping new work and feedback in its
connection congestion window.

### 6. Measure upstream and downstream separately

Each direction has its own logical ACK flag, delivery state, loss estimate,
coding decision, and congestion behavior. Queqiao does not copy a downstream
erasure floor or capacity estimate into the upstream.

## One design, three workload goals

Every TCP flow has the same:

- random session and flow identity;
- OPEN/OPEN_OK lifecycle;
- logical byte-offset sequence space;
- cumulative and selective range acknowledgements;
- bounded replay and out-of-order reassembly;
- carrier replacement and close semantics; and
- authenticated QUIC/TCP transport machinery.

The application does not label its traffic. An internal classifier observes
byte count, rate, directionality, age, and idle gaps, and emits `NEW`,
`INTERACTIVE`, or `BULK` as a scheduling hint. The hint can change queue
priority, aggregate reserve, current coding value, reactive isolation, or TCP
fallback lane admission. It does not create a new flow type or wire state
machine.

The same implementation is evaluated against three workload families:

| Workload | Examples | Design requirement |
| --- | --- | --- |
| Short-lived | `curl`, API call, page resource | pooled setup and recovery must not add avoidable WAN round trips |
| Interactive | SSH, voice, video, small request under load | control and packet latency must remain responsive while another flow fills the path |
| Bulk | download or upload | useful goodput and completion must approach the path budget without unbounded parity, replay, memory, or sockets |

A change is incomplete if it improves one family without reporting what it did
to the other two.

## Unified logical flow and substrate selection

DATA is sequenced by byte offset rather than by arrival. The receiver can place
a later frame immediately even if an earlier frame crossed a different
substrate or replacement lane. Only the contiguous prefix is exposed to the
application.

Each QUIC stream has a reliable control substrate. Where QUIC DATAGRAM is
available, the connection also owns one coded datagram substrate shared and
demultiplexed by flow ID. Selected DATA uses it only while both path coding and
the flow's current policy are active. UDP PACKET uses datagrams whenever
available because the application already chose unordered, unreliable
semantics; it does not wait for the coding policy used by TCP DATA.

This split does not ask the application to select a channel. The sender routes
each frame from the current shared path and flow state; the receiver reconstructs
one unchanged logical flow.

## Why a BBR-based proxy is not the same design

BBR is a congestion controller, not a proxy. A WebSocket/TLS/TCP proxy using
kernel BBR is a complete and valid competing stack. It should be compared as
such, not dismissed because BBR itself lacks proxy framing.

BBR estimates bandwidth and RTT per connection and can behave much better than
classic loss-based TCP on some lossy paths. TCP still exposes an ordered byte
stream: missing data is recovered by retransmission, and later bytes cannot be
delivered past the gap. BBR also does not add cross-flow endpoint-pair state,
application-visible byte-offset recovery, selective FEC, UDP relay resumption,
or proxy scheduling.

Queqiao's claim is therefore architectural and path-scoped: on a paired segment
with a stable erasure floor and long RTT, the proxy can coordinate information
and recovery that a per-connection congestion controller does not own. Whether
that produces a performance advantage on a particular network remains a
measurement question.

## Why QUIC data aggregation was removed

The project began with a multipath hypothesis because several TCP connections
delivered more than one. The observation was real; the diagnosis was wrong.

At high random loss, ordinary TCP goodput is bounded roughly by
`MSS / (RTT * sqrt(p))` per connection. Adding connections multiplied that
loss-limited allowance. Open-loop probing then showed that total path delivery
was unchanged when the same offered load was split across one, two, four, or
eight 4-tuples. There was no per-connection policer or independent path
capacity to aggregate.

After correcting the loss response, extra QUIC connections offered no new
capacity. Instead, their combined offered load crossed the shared knee, made
loss more bursty, and weakened the erasure code. The data aggregator was
deleted.

The current invariant is one active QUIC data connection per logical flow.
Other connections exist for different reasons:

- a pooled connection amortizes handshake cost across flows;
- reactive isolation protects cross-flow latency;
- a replacement connection recovers a failed carrier; and
- an opt-in TCP-only bundle protects fallback tails while every socket remains
  under its kernel congestion controller.

None of these claims additional capacity beyond the endpoint-pair bottleneck.

## Pooling and reactive isolation

Warm flows open a new QUIC stream on an authenticated pooled connection, so
they do not pay a new transport/TLS handshake. The pool is also the common
control connection for path state and recovery.

If a flow grows while another flow shares that connection, Queqiao can move
the growing flow's data to a separately authenticated QUIC connection. The
logical flow retains its IDs, byte offsets, ACK state, and recovery machinery.
Its control role remains protected from the bulk data plane.

Isolation is reactive. Moving a lone growing flow immediately would pay a cold
congestion window without protecting anyone else and was measured to reduce
bulk goodput. This is a contention policy, not workload-specific architecture.

## Fallback and resumption

Automatic mode prefers QUIC. TLS/TCP is a delayed fallback, and only a QUIC
failure paired with a working authenticated TCP connection counts as evidence
that UDP is unavailable. Endpoint, protocol, credential, destination, and
caller-cancellation failures do not globally poison the UDP path.

A TCP flow survives carrier replacement because the sender retains bounded
unacknowledged byte ranges. JOIN attaches a same-principal lane and replays what
the peer has not acknowledged. Once a flow hands off from QUIC to TCP, it stays
on TCP rather than mixing carrier semantics.

For UDP, the gateway can retain a failed association's relay socket briefly
under a random single-use token. A same-device replacement reclaims it and
preserves the source address visible to the destination. Datagrams in flight
during the failure remain lost, as UDP permits.

## Non-goals

- Universal congestion control for unrelated destinations and bottlenecks.
- Aggregating QUIC capacity by opening more connections to the same shared
  bottleneck.
- TCP friendliness on an operator-controlled paired segment.
- Ignoring genuine congestion or exceeding the endpoint pair's aggregate
  capacity.
- Decrypting HTTPS or requiring applications to declare workload type.
- Anonymity, CDN behavior, censorship circumvention, or the discovery/routing
  control plane of a full mesh. The paired data plane can be one leg of such a
  system.

## Evidence and current limits

The path characterization is protocol-independent evidence for the motivating
network model. Historical wire-3 transport campaigns produced causal evidence,
including rejected policies and a live comparison that reached parity—not a
general advantage—against a TUIC-shaped reference.

Those records are kept in [`archive/`](archive/), but they do not qualify the
public protocol-1 tree. Current claim boundaries and missing field coverage are
in [STATUS.md](STATUS.md), and the release gates are in
[PRODUCTION-DESIGN.md](PRODUCTION-DESIGN.md).

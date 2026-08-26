# Datacenter profile design

> [!NOTE]
> **Status:** Experimental. Qualified on one path.
>
> **Applies to:** the `dc-long-haul` profile
> **Evidence:** [PATH-CHARACTER-DC-20260826.md](PATH-CHARACTER-DC-20260826.md)
> **Last reviewed:** 2026-08-26

Queqiao's supported deployment is a client and a trusted gateway whose shared
segment is the dominant bottleneck for every flow crossing it. This profile is
a second deployment: a long leg between two hosts one operator runs, carrying
request/response traffic for inference endpoints.

It exists because inference economics fix the topology. Serving throughput
depends on batch size and cache density, so GPU capacity concentrates; voice
activity detection and turn-taking sit inside a 20ms loop, so gateways
distribute. The leg between them is long, operator-owned, and unavoidable.

## Precondition

Both endpoints are operated by the same party; the leg between them is long;
and every flow on it is a latency-critical request rather than a transfer
seeking throughput.

If the third clause is false the access-link profile is the right one, because
its whole classifier premise — that bulk traffic exists and must be kept away
from interactive traffic — depends on it.

## What the measurements changed

The profile was designed from argument and then measured. Four things the
design assumed turned out to be wrong, and the corrections are the design.

**The transport already helps this workload.** It was expected to hurt. Cold
flows gain 5-10x from the pre-warmed pool alone and the erasing direction gains
13.5x from coding, with residual erasure measured at zero.

**Most of that gain is not this project's.** Against the TUIC-shaped reference
on the same QUIC stack, moving off TCP is worth 4.8-6.9x and coding adds
1.8-2.0x on top. The claim to make is "twice a well-configured QUIC tunnel",
not "fourteen times TCP".

**The tail is not the problem for requests; it is the problem for frames.**
Request p99 stays within 7% of the median at every concurrency tested. Frame
p99 sits 567ms above the path floor with 5% of frames arriving late. Those are
different mechanisms and only the second needs work.

**A node-level relay is not faster than several.** Three independent relays
beat one shared relay by ~2.5x in aggregate, in both orders. Sharing buys
fairness, not throughput.

## Mechanisms

### Profiles

A profile is a named bundle of a precondition stated so it can be found false,
the characterisation document justifying its constants, a release level, and
the policy that differs. `internal/profile`. The default is the access-link
deployment; an unknown name is refused rather than silently replaced, because a
process running a different policy than the operator asked for, with nothing in
the logs saying so, is the failure the package exists to prevent.

### The path as a chain of segments

`PathModel` answers what one endpoint pair is doing, which is right when every
flow crosses the same difficult segment. It stops being right when flows go to
different places: one model per destination makes each rediscover the shared
uplink, and one model for everything lets a congested peer throttle a healthy
one.

`internal/pathmodel/tree.go` composes the existing node type into a chain —
uplink, group, peer — so a chain of one behaves exactly as today and every
existing caller is unchanged. Reduction differs per field because one rule for
all of them is how a hierarchy becomes worse than none:

- **Share and Seed: minimum.** A flow may not exceed the tightest segment it
  crosses.
- **Erasure, burst factor, round trip: maximum.** Erasure accumulates along a
  path and a leaf traverses everything its ancestors do; taking the smaller
  would size a code for part of a channel.
- **Below 100 observed samples a node constrains nothing** — the same hundred
  the prewarm sends, for the same reason — so the first flow to a new
  destination is not throttled by a node that has seen nothing.

Which segments are actually shared is decided by measurement.
`internal/pathmodel/correlate.go` buckets each group's loss and RTT inflation at
200ms and correlates **first differences**. Correlating levels would find that
every pair of paths gets worse in the evening and collapse the tree to one
root; differencing removes the shared trend and leaves the coincidences a
shared queue produces. `SuggestMerges` reports evidence; `MergeGroups` applies
it. They are separate because a topology change re-parents every subsequent
flow's budget.

### Flow classification

On an access link, 128KB is a reasonable place to start suspecting a flow will
starve an interactive one. On a datacenter leg it is one ordinary request. Bulk
here is a checkpoint pull — tens of megabytes, sustained — so the thresholds
say that.

Thresholds alone still latch on a long-lived connection that has carried enough
requests to add up, and neither byte count nor rate separates the cases: a
request burst is briefly *faster* than the bulk floor, not slower. **Duty cycle
separates them.** A flow seen idle is disqualified from bulk permanently.

This change is principled and its benefit is **unproven**: it prevents the
demotion (`class_transitions_2_total` 1-4 → 0) and changes latency by 2.4ms on
a 455ms base. It ships behind an experimental profile for that reason.

### Frames go on datagrams, duplicated

A 300KB request is 200 packets: a block worth coding over, and traffic behind a
loss to reveal it. An 80-byte frame is one packet with neither, so a loss waits
out a timeout.

Three designs were candidates. Coding across a session's frames needs a window,
so it delays every frame to repair the few that are lost. Coding across
sessions couples flows with no other reason to be coupled. **Duplication needs
no window, no delay and no coupling**, and measurement chose it: one copy loses
71 of 400, two lose 10, three lose none, while p50 moves by 1.4ms.

The deeper reason is that over datagrams **the tail is loss, not lateness** —
every delivered frame arrived within 209ms. An ordered stream converts one lost
frame into delay for everything behind it; a datagram converts it into a drop,
which is what a jitter buffer exists for.

Cost: 8 KB/s per session against 4, on a leg whose knee is 333 Mbit/s.

### L7 ingress, narrowly scoped

When the receiver's HTTP/2 window can be raised, raise it — it is worth 20x
when it is the RFC's 65535, though Go and gRPC already default far above that,
so measure before assuming.

When it cannot — a third-party endpoint — an ingress colocated with it
terminates HTTP/2 with generous windows and streams onward. A window is credit
per round trip, so shortening the trip it spans makes it irrelevant without
changing anything on the far side: 1MB goes 3394.9ms → 190.8ms.

Streaming, backpressure and cancellation are structural, not tuned: the body is
copied rather than buffered, the copy reads only as fast as the write side
accepts, and the upstream request derives from the inbound context.

Terminating HTTP/2 inherits its attack surface — HPACK bombs, CONTINUATION
floods, reset storms. That is the reason to keep this scoped to the case that
needs it rather than making it the general shape.

## Deployment

Applications reach the transport over loopback, where the round trip is ~0.05ms
and none of the pathologies above bind. Every constraint discussed is credit
per round trip; putting an RTT-free segment at each end confines them to the
middle, where the protocol is fully controlled. Short-lived connections stop
being expensive without the application changing.

**Relay shape is a fairness decision, not a throughput one.** Several
independent relays deliver ~2.5x the aggregate of one shared relay on this
path, because three tunnels are three congestion controllers probing a wide
path in parallel. One shared relay delivered every flow within 2ms of every
other, against a 3.5x spread between independent groups. Share for tenant
fairness; split for aggregate.

This reverses on an access link, where multipath was measured and retired
because four lanes delivered less than one. Both results are correct about
different paths: splitting cannot multiply a share that does not exist.

## Measurement discipline

The path's erasure moves between roughly zero and seventeen percent within
minutes, and the transport's advantage is a function of it — 13.5x at 14% loss,
6.5x at single digits. Therefore:

- **Every comparison carries a contemporaneous loss measurement.**
- **Every comparison alternates order and pools.** Position in the sequence was
  worth 158ms while one policy under test was worth 2.4ms; running the baseline
  first produced a 53% win that reversed on reversal. `pathmeasure -mode ab`
  does this by construction and reports when the order effect dominates.
- **Comparisons include the QUIC arm.** Without it a result against TCP
  overstates this project's contribution sevenfold.

## Open

- Coding across frames is designed but not implemented; frame traffic does not
  yet route onto the datagram path automatically.
- Whether aggregation replaces synthetic probing is untested — the aggregate
  metrics endpoint reports the idle control connection, so it needs per-lane
  trace evidence.
- Correlation-based regrouping produces evidence and applies merges on demand;
  nothing feeds it in production and the split-on-decay half is manual.
- One path. The profile's release level stays experimental until there are more.

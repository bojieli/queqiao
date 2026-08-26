# Datacenter profile design

> [!NOTE]
> **Status:** Experimental. Tested on one path so far.
>
> **Applies to:** the `dc-long-haul` profile
> **Evidence:** [PATH-CHARACTER-DC-20260826.md](PATH-CHARACTER-DC-20260826.md)
> **Last reviewed:** 2026-08-26

Queqiao's supported deployment is a client talking to a trusted gateway over a
link where that first hop is the bottleneck for everything. This profile covers
a different case: a long hop between two machines the same operator runs,
carrying requests to inference endpoints.

We need it because of how inference gets deployed. GPU serving wants big
batches and warm caches, so capacity ends up concentrated in one or two
regions. Voice activity detection and turn-taking have to answer within about
20ms, so those run close to the user. That leaves a long hop in the middle,
owned by one operator, that nobody can design away.

## When this profile applies

Use it when all three hold:

1. You operate both ends.
2. The hop between them is long.
3. Every flow on it is a latency-sensitive request, not a bulk transfer.

If the third one is false, use the access-link profile instead. Its classifier
is built around separating bulk traffic from interactive traffic, and that
distinction doesn't mean anything here.

## What we got wrong

We designed this profile from first principles and then went and measured it.
Four of our assumptions were wrong. The corrections are most of what follows.

**We expected the transport to hurt this workload. It helps a lot.** Cold flows
run 5-10x faster just from the pre-warmed connection pool, and the lossy
direction runs 13.5x faster with coding. Residual erasure after repair measured
zero.

**Most of that speedup isn't ours.** We compared against the TUIC-shaped
reference in `internal/baseline`, which runs on the same QUIC fork in the same
process. Getting off TCP accounts for 4.8-6.9x. Our coding adds another
1.8-2.0x on top. So the number to quote is "about twice a well-tuned QUIC
tunnel," not "fourteen times TCP."

**The tail is fine for requests and bad for frames.** Request p99 stays within
7% of the median at every concurrency we tried. Frame p99 lands 567ms above the
path's floor, with about 5% of frames arriving late. Two different problems,
and only the second one needs work.

**One relay per node isn't faster than several.** Three independent relays beat
a single shared one by roughly 2.5x on aggregate throughput, and that held with
the test order reversed. Sharing buys fairness, not speed.

## Design

### Profiles

`internal/profile` makes the deployment an explicit choice. Each profile
carries a precondition you can check and disprove, a pointer to the
measurements behind its constants, a release level, and whatever policy
differs. The access-link profile is the default.

An unrecognized profile name is an error. We don't fall back to the default,
because then a process would be running policy the operator didn't ask for with
nothing in the logs to show it.

### Modeling the path as a chain

`PathModel` describes one endpoint pair. That's the right model when every flow
crosses the same bad segment. It breaks down when flows go to different places.
Give each destination its own model and they all have to relearn the shared
uplink separately. Give them one model and a congested peer slows down traffic
to a healthy one.

`internal/pathmodel/tree.go` chains the existing node type: uplink, then group,
then peer. A chain of one behaves exactly like today's single model, so nothing
existing changes. How we combine the nodes depends on the field, because using
one rule everywhere would make the tree worse than no tree:

- **Share and Seed take the minimum.** A flow can't exceed the tightest segment
  it crosses.
- **Erasure, burst factor, and RTT take the maximum.** Loss accumulates along
  the path, and a leaf crosses everything its parents do. Taking the smaller
  value would size the code for part of the channel.
- **A node with fewer than 100 samples doesn't constrain anything.** That's the
  same sample count the prewarm sends, for the same statistical reason. Without
  it, the first flow to a new destination gets throttled by a node that hasn't
  measured anything yet.

We decide which segments are actually shared by measuring, not by assuming.
`internal/pathmodel/correlate.go` buckets each group's loss and RTT inflation
into 200ms windows and correlates the first differences. If you correlate raw
levels instead, you find that every pair of paths gets worse in the evening,
and the tree collapses to a single root for a reason that has nothing to do
with a shared queue. Differencing cancels the common trend.

`SuggestMerges` reports what correlates. `MergeGroups` acts on it. We kept
those separate because merging re-parents the budget for every flow that
follows, and that should be a deliberate call.

### Flow classification

On an access link, 128KB is a sensible point to start suspecting a flow will
starve an interactive one. On a datacenter hop it's one ordinary request. Bulk
traffic here means a checkpoint pull: tens of megabytes, sustained. The
thresholds say so.

Thresholds alone aren't enough, since a long-lived connection eventually
accumulates enough bytes to trip them. Neither byte count nor rate separates
the two cases, because a request burst is briefly *faster* than the bulk rate
floor, not slower. What separates them is duty cycle. A flow we've seen go idle
can't be reclassified as bulk afterward.

Worth being clear: this change is well-reasoned but we haven't shown it helps.
It does stop the demotion (`class_transitions_2_total` drops from 1-4 to 0),
and it moves latency by 2.4ms on a 455ms baseline, which is noise. That's why
it sits behind an experimental profile.

### Frames go over datagrams, duplicated

A 300KB request is about 200 packets. There's a block worth coding over, and
there's traffic behind a loss to expose it quickly. An 80-byte audio frame is a
single packet with neither, so a lost one waits for a retransmit timeout.

We looked at three options:

1. Code across a session's own frames. Needs a window, which delays every frame
   to protect the few that get lost.
2. Code across sessions. Couples flows that otherwise have nothing to do with
   each other.
3. Send each frame more than once.

We measured the third. One copy loses 71 frames out of 400, two copies lose 10,
three copies lose none, and p50 moves by 1.4ms across all three. The windowed
options can't match that, since parity computed over a window delays frames
that were never at risk.

There's a second result underneath. Over datagrams, the tail is dropped frames
rather than late ones. Every frame that arrived did so within 209ms. The 567ms
tail we measured earlier is what an ordered stream does when it loses a frame:
everything behind it waits. A datagram turns that into a drop, which is what a
jitter buffer is for.

Cost is 8 KB/s per session instead of 4, on a hop that doesn't bend until 333
Mbit/s.

### L7 ingress, for receivers you don't control

If you can raise the receiver's HTTP/2 window, do that. It's worth 20x when the
window is actually RFC 7540's 65535. Check first, though: Go's `x/net/http2`
defaults both upload buffers to 1MB, and gRPC-go sizes its window from a
measured BDP, so most Go services are already fine.

When you can't change the receiver, put an ingress next to it. It terminates
HTTP/2 with large windows and streams the body onward. A window is credit per
round trip, so a short hop makes even a small window irrelevant. We measured
1MB going from 3394.9ms to 190.8ms with the far end untouched.

Three things have to hold or the extra hop costs more than it saves, and all
three come from how it's written rather than from tuning. The body streams
instead of buffering. Backpressure propagates, because the copy only reads as
fast as the far side accepts. Cancellation propagates, because the outbound
request inherits the inbound request's context.

Terminating HTTP/2 also means inheriting its attack surface: HPACK
decompression bombs, CONTINUATION floods, reset storms. Keep this on the
specific case that needs it. Don't make it the default shape.

## Deployment

Applications connect over loopback, where the round trip is about 0.05ms and
none of the problems above apply. Everything discussed here is credit per round
trip, so putting a zero-RTT segment on each end confines the problem to the
middle, where we control the protocol. Short-lived connections stop being
expensive without touching the application.

**Choosing a relay layout is a fairness decision, not a throughput one.**
Several independent relays move about 2.5x the aggregate of one shared relay,
because three tunnels mean three congestion controllers probing a wide path at
once. But the shared relay delivered all 24 flows within 2ms of each other,
while the independent groups spread out by 3.5x. Share if you care about
fairness across tenants. Split if you care about total throughput.

This is the opposite of what we found on the access link, where we measured
multipath and dropped it after four lanes came in slower than one. Both results
are right, for different paths. You can't split a share that doesn't exist.

## How we measure

This path's loss rate swings between roughly zero and 17% over a few minutes,
and the transport's advantage tracks it: 13.5x at 14% loss, 6.5x in the single
digits. So:

- **Take a loss measurement alongside every comparison**, in the same minutes.
- **Alternate the order and pool the results.** Position in the test sequence
  was worth 158ms here, while one policy we were testing was worth 2.4ms.
  Running the baseline first showed a 53% win that flipped when we reversed the
  order. `pathmeasure -mode ab` alternates and pools automatically, and warns
  when the order effect is larger than the effect you're testing.
- **Include the QUIC arm.** Comparing only against TCP overstates our
  contribution by about 7x.

## Not done yet

- Frame traffic doesn't route onto the datagram path automatically. The design
  is settled and measured; the plumbing isn't written.
- We don't know whether aggregating flows removes the need for a synthetic
  bandwidth probe. The aggregate metrics endpoint reports the idle control
  connection, so answering this needs per-lane trace data.
- Correlation-based regrouping produces evidence and can apply a merge on
  request, but nothing feeds it in production and splitting a group back apart
  is manual.
- One path. This profile stays experimental until we've run it on more.

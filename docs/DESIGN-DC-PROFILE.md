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

`SuggestMerges` reports what correlates and `MergeGroups` acts on it. Ordinary
reporting feeds the correlator, and the datacenter profile applies suggestions
at a bounded cadence; the access-link profile gathers nothing and rearranges
nothing.

We merge but don't automatically split. That sounds asymmetric and isn't: once
two groups share a node they share a budget, and the shared budget is exactly
what smooths away the congestion signal that would tell them apart again.
Undoing a merge is `SplitGroup` and an operator with a reason. The threshold is
0.8 on differenced short-window signals, which is deliberately higher than the
bar for leaving them apart.

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

### Frames go over UDP, where the transport already handles them

A 300KB request is about 200 packets. There's a block worth coding over, and
there's traffic behind a loss to expose it quickly. An 80-byte audio frame is a
single packet with neither, so we expected it to need something new.

It doesn't. Measured on the live path at 3.6% loss, 16 sessions of 200
messages:

| carrier | lost | p50 | p99 | p99/p50 |
|---|---|---|---|---|
| UDP, direct | 163 of 3200 | 193.6ms | 213.7ms | 1.10 |
| UDP, through Queqiao | 34 of 3200 | 208.2ms | 217.6ms | 1.05 |
| TCP, through Queqiao | 0 | 208.4ms | 728.4ms | 3.49 |

Over UDP the transport removes four fifths of the lost frames and the tail
stays flat. Over TCP nothing is lost and the tail is three and a half times the
median, because reliability turns every gap into delay for the frames behind
it.

So the answer is UDP ASSOCIATE, which has been here since the beginning. Our
earlier reading of this as an open problem came from measuring voice over TCP,
which is not how voice travels. Point a voice application at the UDP path and
it gets both properties at once.

The transport's own counters show the mechanism. Over a 52-second session on
an emulated 14% path the coded substrate carried 2270 symbols, recovered 1708,
and lost 5. Five failures can't make one percent of frames slow by themselves.
They do it by stalling what's queued behind them, which is a property of the
carrier.

One real defect turned up while chasing this. Whether a flow counted as a
series of small exchanges was decided from its lifetime byte total, so a voice
call crossed the budget after about a minute and was demoted to bulk, losing
its coding for the rest of the call. It's now a rate over a recent window: a
transfer never drops under it, and a conversation never reaches it whatever its
age.

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

- We don't know whether aggregating flows removes the need for a synthetic
  bandwidth probe. The aggregate metrics endpoint reports the idle control
  connection, so answering this needs per-lane trace data.
- Splitting a merged group back apart is manual, for the reason above.
- One path. This profile stays experimental until we've run it on more.

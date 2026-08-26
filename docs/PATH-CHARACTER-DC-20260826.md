# What a China-US datacenter path actually is (2026-08-26)

A Huawei Cloud instance in Guiyang and a colocated server in Irvine,
California. This is the first path we've characterized for the datacenter
profile. We picked it because it matches that profile's shape: a long hop
between two machines one operator runs, carrying request/response traffic of a
few hundred kilobytes.

Measured with `pathprobe` (open-loop UDP) and `pathmeasure` (new in this
branch). To re-run any of this, see
[MEASURING-A-DC-PATH.md](MEASURING-A-DC-PATH.md).

## The path

| Property | Value |
|---|---|
| Round trip | 199-207ms, min 185.9ms |
| Jitter at 1 packet/s | mdev 0.58ms over 30 probes |
| Path MTU | 1500, no blackholing |
| Capacity knee, downstream | ~333 Mbit/s |

A round trip whose min and max are 3ms apart isn't queueing anywhere. So
whatever the loss turns out to be, it isn't congestion.

## The two directions behave like different paths

| Direction | Protocol | Loss | Throughput |
|---|---|---|---|
| Upload, CN→US | UDP 50 Mbit/s | **0.0%** (0 of 41,663) | 49.99 Mbit/s |
| Upload, CN→US | TCP cubic | 0.36-2.90% retransmitted | 73-94 Mbit/s |
| Download, US→CN | UDP 1-300 Mbit/s | **~14%** | knee ~333 Mbit/s |
| Download, US→CN | TCP cubic | -- | **0.13-0.47 Mbit/s** |

Our first reading of this data said UDP was being policed 20-40x harder than
TCP. That was wrong. `pathprobe`'s server does the sending, so it measures the
download; `pathmeasure`'s client does the sending, so it measures the upload.
We were comparing directions, not protocols. `pathmeasure -mode udp` exists
because of that mistake.

## Download loss is memoryless and doesn't depend on rate

| offered Mbit/s | delivered | loss | P(loss\|prev ok) | P(ok\|prev lost) | burst factor |
|---|---|---|---|---|---|
| 1 | 0.85 | 14.5% | 0.136 | 0.802 | 1.07 |
| 5 | 4.13 | 17.5% | 0.181 | 0.854 | 1.00 |
| 20 | 17.01 | 14.9% | 0.072 | 0.408 | 2.08 |
| 150 | 129.19 | 13.9% | 0.136 | 0.843 | 1.02 |
| 300 | 256.15 | 14.6% | 0.141 | 0.825 | 1.03 |
| 600 | 333.49 | **44.4%** | 0.490 | 0.613 | 1.00 |

For a memoryless channel with loss `p`, you expect `P(loss|prev ok) = p` and
`P(ok|prev lost) = 1-p`. At 1, 150, and 300 Mbit/s the measured pairs match
within sampling noise. Delivered rate scales linearly all the way to 300
Mbit/s, so nothing is congested below that. At 600 the loss jumps to 44.4% with
a longest run of 795 packets. That's the knee, and it's the only point on this
path where loss actually means congestion.

The high burst factors at 20-80 Mbit/s are an artifact of how the probe
releases packets, not something the path is doing. They disappear at 150 and
300 Mbit/s, where we have two orders of magnitude more samples.

**What TCP does with this.** Mathis gives `MSS/(RTT·√p)` = 1448/(0.2·√0.14) =
**0.155 Mbit/s**. We measured 0.13-0.47. TCP isn't broken. It's responding
correctly to a loss signal that carries no information here, while the
open-loop probe pulls 256 Mbit/s across the same channel.

## Flow completion time

Upload direction, cubic, at the payload sizes an inference call actually sends:

| size | cold | warm-first | warm | floor |
|---|---|---|---|---|
| 100KB | 836-2009ms | 742ms | 186-763ms | ~200ms |
| 300KB | 1172-1198ms | 1029ms | **209ms** | ~200ms |
| 1MB | 1560-1721ms | 1687ms | **212ms** | ~200ms |

A warm connection is worth 5.7x at 300KB, and 209ms on a 200ms path is one
round trip, which is the floor.

The first flow on a warm connection isn't warm: 1029ms, versus 209ms for the
ones after it. Reusing a connection saves the handshake right away and saves
the ramp later.

Small flows have worse tails than bigger ones, which is backwards from what
you'd expect. 100KB is about 70 packets, too few to reliably generate the three
duplicate ACKs that fast retransmit needs, so a loss falls through to a
timeout.

## A long-lived connection doesn't stay warm

Six 300KB bursts on one connection with 3s idle between them, which is roughly
what an interactive session looks like:

| burst | cubic, `ssai=1` | bbr, `ssai=1` | cubic, `ssai=0` |
|---|---|---|---|
| 0 | 941.9ms | 1082.6ms | 1028.5ms |
| 1 | 941.0ms | 317.3ms | **209.1ms** |
| 3 | 941.9ms | 288.5ms | **208.7ms** |
| 5 | 941.0ms | 284.9ms | **209.0ms** |

With Linux's default, cubic runs full slow start on every burst, forever. 941ms
is five round trips, repeated six times, on a connection that stayed open the
whole time.

BBR doesn't reset, because `tcp_slow_start_after_idle_check()` returns early
for any controller that provides `cong_control`. It settles at 285ms instead.
The remaining gap is pacing: BBR paces at a bandwidth estimate it derived from
these same bursts, and `app_limited` reads true from burst 1 on.

Setting `tcp_slow_start_after_idle=0` gets cubic to 209ms. **One sysctl, worth
4.5x**, and tuned cubic then beats BBR because cubic doesn't pace.

**Loopback, for scale.** The same 300KB takes 0.1ms (28.9-31.5 Gbit/s) on the
same server talking to itself, versus 209ms across the path. Every constraint
above is a function of the round trip.

## How much of the win is ours, and how much is just QUIC

`internal/baseline` implements TUIC's data-path shape on the same QUIC fork in
the same process, so any gap between them is design rather than library.
Order-alternated, download direction:

| payload | direct TCP | TUIC-shaped QUIC | Queqiao | QUIC's share | coding adds |
|---|---|---|---|---|---|
| 100KB | 1918.4ms | 401.0ms | **218.5ms** | 4.8x | 1.84x |
| 300KB | 5449.7ms | 791.7ms | **399.1ms** | 6.9x | 2.0x |

Getting off TCP accounts for most of it. The honest claim is that Queqiao is
about twice as fast as a well-tuned QUIC tunnel here, not fourteen times TCP. A
comparison table without the QUIC arm overstates our contribution by about 7x.

The spread is more interesting than the median. Sixteen consecutive 100KB
downloads through Queqiao spanned **16ms** (204-220), on a channel dropping a
seventh of everything. The same sixteen over TCP spanned four seconds.

On the clean upload direction with a warm connection, the tunnel **costs**
50-75ms. That's the price of a userspace proxy plus an extra local hop.
Queqiao is the cheaper of the two tunnels by 21.8ms.

## Concurrency: the two workload shapes disagree

**Requests**, 300KB each, all started at once, against the reference (emulated
path):

| concurrency | queqiao | reference |
|---|---|---|
| 4 | 4/4, p99/p50 **1.07** | 4/4, p50 **35637ms** |
| 16 | 16/16, p99/p50 **1.06** | **0/16 completed** |
| 48 | 48/48, p99/p50 **1.05** | **0/48 completed** |

Every flow completes and the tail stays within 7% of the median. 48x the load
costs 2.7x the latency. The reference falls over at sixteen.

On the live link the same pattern holds, and the tunnel moves 2.5x the
aggregate at 16 and 32 flows (86.1 versus 35.4 Mbit/s).

**Frames**, 80 bytes every 20ms, 24 sessions (emulated): p50 203.3ms against a
200ms floor, p90 208.2ms, then **p99 766.6ms**, with 4.96% of frames arriving
more than one frame interval late.

That's the opposite of the request result, and the cause is payload size. An
80-byte frame is a single packet. There's no block to code over and no traffic
behind it to expose the loss, so it waits out a timeout.

## What fixes the frame tail: using the carrier voice actually uses

The frame tail above was measured over a reliable ordered stream, and that was
the wrong shape. A voice stream is not carried on TCP, and measuring it as if
it were reports a problem the application does not have.

The mechanism is visible in the transport's own counters. Across a 52-second
session on the emulated 14% path, the coded substrate carried 2270 symbols,
recovered 1708 of them, and lost 5. Five failures cannot make one percent of
frames slow on their own. They do it by stalling everything queued behind them,
which is what an ordered stream does and what a datagram does not.

Measured on the live path, 16 sessions of 200 messages, with downstream loss at
3.6% during the run:

| carrier | delivered | lost | p50 | p90 | p99 | p99/p50 | >250ms |
|---|---|---|---|---|---|---|---|
| UDP, direct | 3037 | **163** (5.1%) | 193.6ms | 205.6ms | 213.7ms | **1.10** | 0.00% |
| UDP, through Queqiao | 3166 | **34** (1.1%) | 208.2ms | 213.9ms | 217.6ms | **1.05** | 0.00% |
| TCP, through Queqiao | 3200 | 0 | 208.4ms | 214.0ms | **728.4ms** | **3.49** | 2.16% |

Over UDP the transport removes four fifths of the lost frames, from 5.1% to
1.1% on a path erasing 3.6%, and the tail stays flat: p99 is five percent above
the median rather than three and a half times it. That is the code repairing
gaps inside the round trip that carried them, which is what it was built to do.

Over TCP nothing is lost and the tail is 728ms, because reliability converts
every gap into delay for the frames behind it.

So there is no missing coding scheme here. UDP ASSOCIATE has been in this
transport since the beginning, and a voice application using it gets both
properties at once. The earlier reading of this as an open problem came from
measuring frames over a stream, and the tool now measures both carriers so the
mistake is harder to repeat.

A separate defect did turn up while chasing it, and it was real: whether a flow
counted as a series of small exchanges was decided from its lifetime byte
total, so any long conversation eventually stopped qualifying and was demoted
to bulk, losing its coding after about a minute. That signal is now a rate over
a recent window, which a transfer never drops under and a conversation never
reaches whatever its age.

## HTTP/2 flow control, and an ingress for receivers you don't control

Same 1MB upload to the same server, varying only the window it advertises:

| receiver window | 300KB warm | 1MB warm |
|---|---|---|
| Go `http2.Server` default | 187.5ms | 193.2ms |
| 8MB explicit | 191.3ms | 200.6ms |
| **65535, the RFC default** | **912.7ms** | **3903.0ms** |

Twenty times slower, and it matches the arithmetic: the window is credit per
round trip, so 1MB over 64KB is sixteen round trips. That predicts 3.2s and we
measured 3.9.

But `x/net/http2` defaults both upload buffers to 1MB, and gRPC-go sizes its
window from a measured BDP. 64KB is what RFC 7540 specifies, not what a default
Go service advertises. So the claim to make is narrower: when the window really
is 64KB it dominates everything else by 20x, and whether it's 64KB depends on
the implementation. Check before assuming.

An L7 ingress placed next to an **unmodified** 65535 server, reached across the
200ms path: 300KB warm goes 998.0ms → **184.8ms**, and 1MB warm goes 3394.9ms →
**190.8ms**, a **17.8x** improvement with nothing changed on the far side. A
window is credit per round trip, so shortening the trip makes it irrelevant.
190.8ms is one round trip, which also confirms the ingress streams rather than
buffers.

## One relay or several

24 concurrent 300KB requests, either all through one client or split eight
apiece across three independent clients, run in both orders:

| order | one relay | three relays |
|---|---|---|
| shared first | 1696.9ms, **34.6 Mbit/s** | 750/1176/1302ms, **57.5 Mbit/s** |
| split first | 1963.8ms, **29.8 Mbit/s** | 396/1187/1382ms, **78.7 Mbit/s** |
| split first, repeat | 2540.6ms, **23.1 Mbit/s** | -- |

Three relays beat one by about 2.5x on aggregate, in both orders. Three tunnels
are three congestion controllers probing a 333 Mbit/s path at once. One tunnel
is one controller, and there's no shared bottleneck here for the coordination
to help with.

We retired multipath on the opposite evidence: four lanes delivering 8 Mbit/s
where one delivered 11. Both results are correct, for different paths. On an
access link the bottleneck is the client's own uplink, and you can't split a
share that doesn't exist.

What sharing does buy is fairness. All 24 flows through one relay finished
within 2ms of each other (p99/p50 of 1.00), while the split arm's three groups
spread out by 3.5x.

## The path isn't stationary, which changes what every number above means

Downstream loss, three readings about ten minutes apart:

| when | offered | loss |
|---|---|---|
| t+0 | 2 Mbit/s | **0.0%** |
| t+0 | 20 Mbit/s | **2.4%** |
| t+9min | 20 Mbit/s | **9.1%** |

Earlier the same evening it read 9.8-17.5%. The same download comparison gave
**13.5x** when the path was dropping 14% and **6.5x** when it was in the single
digits. Both numbers are right. The transport repairs erasure, so its advantage
scales with how much erasure there is. The useful answer is the relationship,
not either endpoint.

Two things follow. **Take a loss measurement alongside every comparison.** And
**always alternate the order**: position in the test sequence was worth 158ms
here, while one policy we were testing was worth 2.4ms. Running the baseline
first showed a 53% win that flipped when we reversed the order. `pathmeasure
-mode ab` alternates and pools automatically, and tells you when the order
effect is bigger than the effect you're measuring.

## Two measurements that couldn't answer their question

**Does aggregating flows replace the need to probe?** We watched the
controller's counters across 1/4/16/32 concurrent flows. The non-app-limited
count stayed frozen at 79 and the bandwidth estimate read zero, even during a
sustained 30MB transfer at 285.8 Mbit/s.

That looks like a bug and isn't. `bbr_tuic.go` documents both halves: marking
app-limited on any unused congestion window is deliberate (tightened twice,
reverted twice), and the metrics endpoint reports the pooled control
connection, which really is idle. Answering this needs per-lane trace data.

At the level of outcomes, the claim's shape holds: aggregate throughput went
from 11.7 to 163.8 Mbit/s between one flow and thirty-two, with the tail
staying tight.

**Are frames late more often direct than tunnelled?** Our first live comparison
said 30% versus 2%. It counted lateness against each arm's own floor, so the
arm with the 10ms lower median had 10ms more room under its own bar. Against
fixed thresholds the difference disappears. A listener doesn't have a relative
bar.

## End-to-end validation, both profiles, all three carriers

One run per profile, on the live path, with the loss measured before and after
so each row can be placed against it.

**Datacenter profile** (loss 13.6% falling to 1.6% during the run):

| workload | arm | result |
|---|---|---|
| 16 concurrent 300KB requests | direct | p50 947.1ms, p99 1135.4ms, 22.5 Mbit/s |
| 16 concurrent 300KB requests | tunnel | p50 1852.7ms, p99 1857.8ms, 21.1 Mbit/s |
| 8 UDP frame sessions | direct | 40 of 1200 lost, p99 210.1ms |
| 8 UDP frame sessions | tunnel | **2 of 1200 lost**, p99 213.5ms |
| 8 TCP frame sessions | tunnel | 0 lost, p999 731.7ms, 0.58% over 250ms |

class transitions: interactive 8, **bulk 0**

**Access-link profile** (loss 2.4% rising to 3.0%):

| workload | arm | result |
|---|---|---|
| 16 concurrent 300KB requests | direct | p50 960.3ms, p99 1063.3ms, 17.2 Mbit/s |
| 16 concurrent 300KB requests | tunnel | p50 1713.3ms, p99 1748.4ms, 21.9 Mbit/s |
| 8 UDP frame sessions | direct | 156 of 1200 lost, p99 212.1ms |
| 8 UDP frame sessions | tunnel | 45 of 1200 lost, p99 215.0ms |
| 8 TCP frame sessions | tunnel | 0 lost, p99 599.9ms, 1.67% over 250ms |

class transitions: interactive 8, **bulk 16**

Three things this shows.

**The profile difference is real and visible in the counters.** Sixteen request
flows are demoted to bulk on the access-link thresholds and none on the
datacenter ones, which is exactly what those thresholds were changed for: a
300KB request is one ordinary request on a datacenter leg and a transfer on an
access link. A demoted flow stops preferring coding, which on an erasing path
is the difference between repairing a gap inside the round trip that carried it
and waiting out a timeout.

**Frames over UDP gain in both profiles**, by twenty times in one run and three
and a half in the other. The two runs saw different loss, so the ratio between
them means nothing; what transfers is that the transport repairs most of what
the path erases while leaving the tail flat.

**The tunnel trades median for variance on concurrent requests.** Direct gives a
lower median and a wider spread; the tunnel finishes nearly every flow together
-- p99/p50 of 1.00 and 1.02 against 1.20 and 1.11 -- at a similar aggregate and
wall time. That is the same fairness the relay comparison found, seen from the
other side, and it is a choice rather than a defect.


## Re-validated after the classification and carrier changes

The same run again, with the fixes in place and grouping discovery enabled
(loss 1.5% rising to 4.7% during it):

| workload | arm | result |
|---|---|---|
| 16 concurrent 300KB requests | direct | p50 991.2ms, p99 1062.7ms, 17.8 Mbit/s |
| 16 concurrent 300KB requests | tunnel | p50 1732.6ms, p99 1736.2ms, 22.6 Mbit/s |
| 8 UDP frame sessions | direct | 52 of 1200 lost, p99 212.4ms |
| 8 UDP frame sessions | tunnel | **15 of 1200 lost**, p99 209.8ms |
| 8 TCP frame sessions | tunnel | 0 lost, p99 737.1ms, 2.58% over 250ms |

class transitions: interactive 8, **bulk 0**

Every request flow completes, no session is demoted, frames over UDP lose a
third of what they lose direct while the tail stays flat, and frames over TCP
show the same tail they always do. Letting the tree rearrange itself from
measured correlation changed nothing here, which is what a path with one
destination group should show.

## Does aggregation give the estimator real evidence?

A bandwidth estimate only improves from samples that were not
application-limited. An earlier attempt to answer this read the aggregate
metrics endpoint and saw a frozen sample count and a zero estimate, which is
what that endpoint reports: one lane, and under pooling that is the idle
control connection.

Read from the per-lane trace instead, on the lane carrying data:

| concurrent flows | non-app-limited samples | bandwidth estimate |
|---|---|---|
| 1 | 12 | 6.8 Mbit/s |
| 4 | 30 | 24.6 Mbit/s |
| 16 | 31 | **87.6 Mbit/s** |

The estimate rises thirteenfold with concurrency and the non-app-limited count
accumulates rather than staying at whatever the handshake produced. An
aggregate fed by many flows does give the estimator evidence a single bursty
flow cannot, which is the argument the node-relay shape rests on. The sample
count plateaus while the estimate keeps climbing, which is the max filter
keeping its best sample and is what it is for.

## The whole stack, on the live path

An unmodified application, captured by a netfilter rule and carried through a
local agent into the tunnel:

```
app -> capture -> SOCKS5 -> queqiao client -> WAN -> gateway -> destination
```

| path | 300KB cold | warm |
|---|---|---|
| direct | 1089.3ms | 1001.4ms |
| through the stack | **624.9ms** | **381.2ms** |

The connect leg is the part worth reading: 0.2ms against 187.3ms. The
application's connection terminates on loopback, and the round trip it would
have paid was already paid by a tunnel that stayed warm. Every constraint
measured in this document is credit per round trip, and this is what putting a
zero-round-trip segment at each end does about them.

The application was never configured for any of it.

## The two projects as one system, on the live path

Both real components, no stand-ins: an application inside a captured cgroup,
`tunless` capturing it with eBPF and serving flow attribution, and the queqiao
client asking that agent what opened each flow before deciding what it is.

```
pathmeasure (in a captured cgroup)
  -> tunless eBPF capture -> SOCKS5 -> queqiao client -> WAN -> gateway
                                  \-- metadata lookup --/
```

The client logged, for each flow:

```
flow class declared from attribution  identity="path=/tmp/pathmeasure"  class=1
```

and `queqiao_class_transitions_1_total` reached 2, one per flow. The class was
known before either flow carried a byte, which is the second the classifier
would otherwise have spent inferring it -- and which a request finishing in
200ms spends entirely inside.

300KB cold through the whole stack: 686.1ms, against 1111.1ms for the same
request when the agent was not running.

Two things the components refused, correctly, on the way to this working. The
metadata server would not open its socket in a directory that grants group or
world access, because that socket answers questions about what processes are
doing. And the preflight refused to attach a capture to a cgroup containing
tunless itself, which would have captured the agent's own connection to its
upstream and re-captured every packet it forwarded.

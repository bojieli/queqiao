# What a China-US datacenter path actually is (2026-08-26)

A Huawei Cloud instance in Guiyang and a colocated server in Irvine,
California. The first path characterised for the datacenter profile, chosen
because it is that profile's shape: a long leg between two hosts one operator
runs, carrying request/response traffic of a few hundred kilobytes.

Measured with `pathprobe` (open-loop UDP) and `pathmeasure` (new in this
branch). Reproduce with [MEASURING-A-DC-PATH.md](MEASURING-A-DC-PATH.md).

## The path

| Property | Value |
|---|---|
| Round trip | 199-207ms, min 185.9ms |
| Jitter at 1 packet/s | mdev 0.58ms over 30 probes |
| Path MTU | 1500, no blackholing |
| Capacity knee, downstream | ~333 Mbit/s |

A round trip whose min and max differ by 3ms is not queueing, so whatever the
loss is, it is not congestion.

## The two directions are different paths

| Direction | Protocol | Loss | Throughput |
|---|---|---|---|
| Upload, CN→US | UDP 50 Mbit/s | **0.0%** (0 of 41,663) | 49.99 Mbit/s |
| Upload, CN→US | TCP cubic | 0.36-2.90% retransmitted | 73-94 Mbit/s |
| Download, US→CN | UDP 1-300 Mbit/s | **~14%** | knee ~333 Mbit/s |
| Download, US→CN | TCP cubic | -- | **0.13-0.47 Mbit/s** |

An earlier reading concluded UDP was policed 20-40x harder than TCP. That was
an artefact: `pathprobe`'s server is the sender, so it measures the download,
while `pathmeasure`'s client is the sender. The direction was the variable, not
the protocol. `pathmeasure -mode udp` exists because of that mistake.

## The download erasure is memoryless and rate-independent

| offered Mbit/s | delivered | loss | P(loss\|prev ok) | P(ok\|prev lost) | burst factor |
|---|---|---|---|---|---|
| 1 | 0.85 | 14.5% | 0.136 | 0.802 | 1.07 |
| 5 | 4.13 | 17.5% | 0.181 | 0.854 | 1.00 |
| 20 | 17.01 | 14.9% | 0.072 | 0.408 | 2.08 |
| 150 | 129.19 | 13.9% | 0.136 | 0.843 | 1.02 |
| 300 | 256.15 | 14.6% | 0.141 | 0.825 | 1.03 |
| 600 | 333.49 | **44.4%** | 0.490 | 0.613 | 1.00 |

A memoryless channel with loss `p` has `P(loss|prev ok) = p` and
`P(ok|prev lost) = 1-p`; at 1, 150 and 300 Mbit/s the measured pairs match to
within sampling noise. Delivery scales linearly to 300 Mbit/s, so nothing is
congested below it; at 600 loss jumps to 44.4% with a longest run of 795. That
is the knee, and the only place on this path where loss means congestion. The
elevated burst factors at 20-80 Mbit/s are the probe's own release pattern, not
the path's — they vanish at 150 and 300 where sample counts are two orders of
magnitude larger.

**What TCP does with it.** Mathis gives `MSS/(RTT·√p)` = 1448/(0.2·√0.14) =
**0.155 Mbit/s**. Measured: 0.13-0.47. TCP is not malfunctioning; it is obeying
a loss signal that means nothing here, while the open-loop probe pulls 256
Mbit/s across the same channel.

## Flow completion time

Upload, cubic, payload sizes an inference call sends:

| size | cold | warm-first | warm | floor |
|---|---|---|---|---|
| 100KB | 836-2009ms | 742ms | 186-763ms | ~200ms |
| 300KB | 1172-1198ms | 1029ms | **209ms** | ~200ms |
| 1MB | 1560-1721ms | 1687ms | **212ms** | ~200ms |

A warm connection is worth 5.7x at 300KB, and 209ms on a 200ms path is one
round trip — the floor, reached exactly. The first flow on a warm connection is
not warm: 1029ms against 209ms for the ones after it. And small flows have
worse tails than larger ones, because 100KB is ~70 packets, too few to
reliably produce the three duplicate acknowledgements fast retransmit needs.

## A long-lived connection does not stay warm

Six 300KB bursts on one connection, 3s idle between — the shape of an
interactive session:

| burst | cubic, `ssai=1` | bbr, `ssai=1` | cubic, `ssai=0` |
|---|---|---|---|
| 0 | 941.9ms | 1082.6ms | 1028.5ms |
| 1 | 941.0ms | 317.3ms | **209.1ms** |
| 3 | 941.9ms | 288.5ms | **208.7ms** |
| 5 | 941.0ms | 284.9ms | **209.0ms** |

With Linux's default, cubic pays full slow start on every burst forever: 941ms
is five round trips, repeated, on a connection open the whole time. BBR does
not reset (`tcp_slow_start_after_idle_check` returns early for any controller
providing `cong_control`) but converges to 285ms, because it paces at a
bottleneck estimate derived from its own bursts — `app_limited` reads true from
burst 1. **One sysctl takes cubic to 209ms: a factor of 4.5**, and tuned cubic
then beats BBR, whose pacing is the remaining constraint.

**Loopback, for scale.** 300KB takes 0.1ms (28.9-31.5 Gbit/s) on the same
server to itself, against 209ms across the path. Every constraint above is a
function of the round trip.

## What this transport does, and how much of it is QUIC

`internal/baseline` is TUIC's data-path shape on the same QUIC fork in the same
process, so a gap is the design rather than the library. Order-alternated,
download direction:

| payload | direct TCP | TUIC-shaped QUIC | Queqiao | QUIC's share | coding adds |
|---|---|---|---|---|---|
| 100KB | 1918.4ms | 401.0ms | **218.5ms** | 4.8x | 1.84x |
| 300KB | 5449.7ms | 791.7ms | **399.1ms** | 6.9x | 2.0x |

**Moving off TCP is worth most of it.** The honest claim is that Queqiao is
twice a well-configured QUIC tunnel here, not fourteen times TCP — a benchmark
table without the QUIC arm overstates this project's contribution sevenfold.

The spread matters more than the median: sixteen consecutive 100KB downloads
through Queqiao spanned **16ms** (204-220), on a channel erasing a seventh of
everything. The same sixteen over TCP spanned four seconds.

On the clean upload direction with a warm connection the tunnel **costs**
50-75ms — the price of a userspace proxy and an extra local hop — and Queqiao
is the cheaper of the two tunnels by 21.8ms.

## Concurrency: the two workload shapes disagree

**Requests**, 300KB started together, against the reference (emulated path):

| concurrency | queqiao | reference |
|---|---|---|
| 4 | 4/4, p99/p50 **1.07** | 4/4, p50 **35637ms** |
| 16 | 16/16, p99/p50 **1.06** | **0/16 completed** |
| 48 | 48/48, p99/p50 **1.05** | **0/48 completed** |

Every flow completes and the tail stays within 7% of the median; 48x the load
costs 2.7x the latency. The reference falls over at sixteen. On the live link
the same shape holds, and the tunnel moves 2.5x the aggregate at 16 and 32
flows (86.1 against 35.4 Mbit/s).

**Frames**, 80 bytes every 20ms, 24 sessions (emulated): p50 203.3ms against a
200ms floor, p90 208.2ms — then **p99 766.6ms**, with 4.96% arriving more than
a frame interval late. The reverse of the request result, and the cause is
payload size: an 80-byte frame is one packet with no block to code over and no
following traffic to reveal a loss, so it waits out a timeout.

## What fixes the frame tail

Frames over **datagrams** rather than a stream, 400 per arm, emulated 14% path:

| copies | delivered | p50 | p99 | never arrived |
|---|---|---|---|---|
| 1 | 329/400 | 204.4ms | 208.7ms | **71** |
| 2 | 390/400 | 203.5ms | 207.0ms | **10** |
| 3 | 400/400 | 203.0ms | 207.7ms | **0** |

Duplication removes seven eighths of the losses and **latency does not move**:
p50 within 1.4ms across all arms. A windowed code cannot have that property.
Cost is 8 KB/s per session against 4.

Underneath it: **over datagrams the tail is loss, not lateness.** Every
delivered frame arrived within 209ms. The 567ms tail is what an ordered stream
does when one frame is lost.

## HTTP/2 flow control, and an ingress for receivers you cannot change

Same 1MB upload, same server, differing only in the window advertised:

| receiver window | 300KB warm | 1MB warm |
|---|---|---|
| Go `http2.Server` default | 187.5ms | 193.2ms |
| 8MB explicit | 191.3ms | 200.6ms |
| **65535, the RFC default** | **912.7ms** | **3903.0ms** |

Twenty times, matching credit-per-round-trip arithmetic: 1MB over 64KB is
sixteen round trips, 3.2s predicted against 3.9 measured. **But `x/net/http2`
defaults both upload buffers to 1MB** and gRPC-go tunes from a measured BDP, so
64KB is what RFC 7540 specifies rather than what a default Go service
advertises. The claim that survives: when the window *is* 64KB it dominates by
20x, and whether it is 64KB is a property of the implementation.

An L7 ingress colocated with an **unmodified** 65535 server, reached across the
200ms path: 300KB warm 998.0ms → **184.8ms**, 1MB warm 3394.9ms → **190.8ms**
(**17.8x**). A window is credit per round trip, so shortening the trip it spans
makes it irrelevant. 190.8ms is one round trip, which also proves the ingress
streams rather than buffers.

## One relay, or several

24 concurrent 300KB requests, all through one client or split eight apiece
across three independent clients, both orders:

| order | one relay | three relays |
|---|---|---|
| shared first | 1696.9ms, **34.6 Mbit/s** | 750/1176/1302ms, **57.5 Mbit/s** |
| split first | 1963.8ms, **29.8 Mbit/s** | 396/1187/1382ms, **78.7 Mbit/s** |
| split first, repeat | 2540.6ms, **23.1 Mbit/s** | -- |

Three relays beat one by ~2.5x in aggregate, in both orders. Three tunnels are
three congestion controllers probing a 333 Mbit/s path in parallel; one is one,
and there is no shared bottleneck here for coordination to buy anything at.

This project retired multipath on the opposite evidence — four lanes delivering
8 Mbit/s where one delivered 11 — and **both results are correct about
different paths.** Where the bottleneck is the client's own access link,
splitting cannot multiply a share that does not exist.

What sharing buys is **fairness**: all 24 flows within 2ms of each other
(p99/p50 1.00) against a 3.5x spread between the split arm's groups.

## The path is not stationary, and that shapes every number above

Downstream loss, three readings within about ten minutes:

| when | offered | loss |
|---|---|---|
| t+0 | 2 Mbit/s | **0.0%** |
| t+0 | 20 Mbit/s | **2.4%** |
| t+9min | 20 Mbit/s | **9.1%** |

Earlier the same evening it read 9.8-17.5%. The same download comparison gave
**13.5x** when the path erased 14% and **6.5x** when it erased single digits.
Both are correct: the transport repairs erasure, so its advantage is a function
of how much there is. The honest statement is the relationship, not either
endpoint.

Two consequences. **Every comparison needs a contemporaneous loss measurement**
reported beside it. And **order alternation is not optional**: position in the
measurement sequence was worth 158ms on this path while one policy under test
was worth 2.4ms, and running the baseline first produced a 53% win that
reversed when the order reversed. `pathmeasure -mode ab` alternates and pools
by construction, and says so when the order effect dominates.

## Two measurements that could not answer their question

**Whether aggregation replaces probing.** Watching the controller's counters
across 1/4/16/32 concurrent flows showed the non-app-limited count frozen at 79
and the bandwidth estimate at zero, even during a sustained 30MB transfer at
285.8 Mbit/s. That looks like a bug and is not: `bbr_tuic.go` documents that
marking app-limited on any unused congestion window is deliberate (tightened
twice, reverted twice), and that the metrics endpoint reports the pooled
control connection, which is genuinely idle. Answering this needs per-lane
trace evidence. At the level of outcomes the claim's shape holds: aggregate
rose 11.7 → 163.8 Mbit/s from one flow to thirty-two with the tail staying
tight.

**Whether frames are late more often direct than tunnelled.** The first live
frame comparison showed 30% against 2%. It counted lateness against each arm's
own floor, so the arm with the 10ms lower median had 10ms more room under its
own bar. Against fixed thresholds the difference disappears. A listener does
not have a relative bar.

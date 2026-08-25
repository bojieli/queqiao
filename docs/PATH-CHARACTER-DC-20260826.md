# What a China-US datacenter path actually is (2026-08-26)

The path is between two datacenters rather than an access network: a Huawei
Cloud instance in Guiyang and a colocated server in Tustin, California. It is
the first path characterised for the datacenter profile, and it was chosen
because it is the shape that profile targets -- a long-haul leg between two
hosts the operator runs, carrying request/response traffic whose payloads are
hundreds of kilobytes.

Measurements were taken with `pathprobe` (open-loop UDP, download direction)
and `pathmeasure` (TCP and upload-direction UDP, both new in this branch).

## The path

| Property | Value |
|---|---|
| Round trip | 199-207ms, min 185.9ms |
| Jitter at 1 packet/s | mdev 0.58ms over 30 probes |
| Path MTU | 1500, no blackholing; 1472-byte ICMP payload passes, 1480 does not |
| Capacity knee, downstream | ~333 Mbit/s delivered |

The jitter figure matters more than it looks. A round trip whose minimum and
maximum differ by 3ms across thirty seconds is not queueing anywhere, so
whatever the loss is, it is not congestion.

## The two directions are different paths

This is the finding that governs everything else, and it is the reason the
first version of this measurement was wrong.

| Direction | Protocol | Loss | Throughput |
|---|---|---|---|
| Upload, China to US | UDP, 50 Mbit/s | **0.0%** (0 of 41,663) | 49.99 Mbit/s |
| Upload, China to US | TCP cubic | 0.36-2.90% retransmitted | 73-94 Mbit/s |
| Download, US to China | UDP, 1-300 Mbit/s | **~14%** | knee at ~333 Mbit/s |
| Download, US to China | TCP cubic | -- | **0.13-0.47 Mbit/s** |

The upload direction erases nothing at all. The download direction erases a
seventh of everything. They are the same two hosts, minutes apart.

An earlier reading of this data concluded that UDP was being policed twenty to
forty times harder than TCP. That conclusion was an artefact of comparing
`pathprobe`, whose server is the sender and which therefore measures the
download, against `pathmeasure`, whose client is the sender and which measures
the upload. The protocols were never the variable; the direction was. The
`udp` mode in `pathmeasure` exists because of this mistake, and it settles the
question: UDP upstream loses nothing, so nothing on this path treats UDP
worse than TCP.

## The download erasure is memoryless and rate-independent

Downstream, one connection, 1200-byte payloads:

| offered Mbit/s | delivered | loss | P(loss \| prev ok) | P(ok \| prev lost) | burst factor | longest |
|---|---|---|---|---|---|---|
| 1 | 0.85 | 14.5% | 0.136 | 0.802 | 1.07 | 3 |
| 2 | 1.80 | 9.8% | 0.096 | 0.878 | 1.03 | 3 |
| 5 | 4.13 | 17.5% | 0.181 | 0.854 | 1.00 | 4 |
| 10 | 8.51 | 14.9% | 0.124 | 0.710 | 1.20 | 6 |
| 20 | 17.01 | 14.9% | 0.072 | 0.408 | 2.08 | 10 |
| 40 | 34.38 | 14.0% | 0.062 | 0.380 | 2.26 | 11 |
| 80 | 71.24 | 10.9% | 0.040 | 0.329 | 2.71 | 21 |
| 150 | 129.19 | 13.9% | 0.136 | 0.843 | 1.02 | 26 |
| 300 | 256.15 | 14.6% | 0.141 | 0.825 | 1.03 | 149 |
| 600 | 333.49 | 44.4% | 0.490 | 0.613 | 1.00 | 795 |

A memoryless channel with loss `p` has `P(loss | prev ok) = p` and
`P(ok | prev lost) = 1 - p`. At 1 Mbit/s: 0.136 and 0.802 against a loss of
0.145. At 150: 0.136 and 0.843 against 0.139. At 300: 0.141 and 0.825 against
0.146. The erasure is independent, and it is independent at rates far below any
queue.

Delivered scales linearly with offered right up to 300 Mbit/s -- 0.85, 1.80,
4.13, 8.51, 17.01, 34.38, 71.24, 129.19, 256.15 -- so there is no capacity
constraint below it. At 600 the loss jumps to 44.4% and delivery saturates at
333 Mbit/s with a longest run of 795. That is the knee, and it is the only
place on this path where loss means congestion.

The elevated burst factors at 20-80 Mbit/s are the sender's, not the path's:
they appear where the probe's own release pattern is coarsest relative to the
rate, and they disappear again at 150 and 300 where the sample counts are two
orders of magnitude larger. This is the contamination `pathprobe`'s `-burst`
flag exists to bound, and it is a reminder that a burst factor measured from a
few hundred losses is not yet a measurement.

## What TCP does with a memoryless erasure channel

Mathis gives throughput as `MSS / (RTT * sqrt(p))`. With MSS 1448, RTT 0.2s and
p = 0.14 that is 19.4 KB/s, or **0.155 Mbit/s**.

Measured TCP download: **0.13-0.47 Mbit/s**.

TCP is not malfunctioning. It is doing exactly what its design says to do with
a loss signal, on a path where the loss signal means nothing. Meanwhile the
open-loop probe pulls 256 Mbit/s across the same channel in the same window.
The gap between 0.155 and 256 is not the path. It is the cost of interpreting
erasure as congestion, and it is a factor of about 1,600.

## Flow completion time is the metric, and it says something different

Upload direction, cubic, payload sizes an inference call actually sends:

| size | cold (incl. handshake) | warm-first | warm | floor |
|---|---|---|---|---|
| 100KB | 836-2009ms | 742ms | 186-763ms | ~200ms |
| 300KB | 1172-1198ms | 1029ms | **209ms** | ~200ms |
| 1MB | 1560-1721ms | 1687ms | **212ms** | ~200ms |

Three things are visible here that no throughput number shows.

**A warm connection is worth 5.7x on 300KB** -- 1198ms cold against 209ms warm.
And 209ms on a 200ms path is one round trip: the floor, reached exactly.

**The first flow on a warm connection is not warm.** 300KB warm-first takes
1029ms against 209ms for the ones after it, and against 998ms for the transfer
half of a cold flow. Reusing a connection saves the handshake immediately and
saves the ramp only later.

**Small flows have worse tails than larger ones.** 300KB warm was 209.0 and
209.0ms on two runs; 100KB warm was 186 and 763ms. A 100KB payload is about 70
packets, which is too few to reliably produce the three duplicate
acknowledgements fast retransmit needs, so a loss falls through to a
retransmission timeout. The smallest flows are the ones least able to recover
cheaply, which is the opposite of the usual intuition and is precisely the
regime forward error correction addresses.

## A long-lived connection does not stay warm

Six 300KB bursts on one connection, three seconds idle between them, upload
direction. This is the shape of an interactive inference session.

| burst | cubic, `ssai=1` | bbr, `ssai=1` | cubic, `ssai=0` |
|---|---|---|---|
| 0 | 941.9ms | 1082.6ms | 1028.5ms |
| 1 | 941.0ms | 317.3ms | **209.1ms** |
| 2 | 1515.7ms | 296.1ms | **209.1ms** |
| 3 | 941.9ms | 288.5ms | **208.7ms** |
| 4 | 1538.2ms | 286.1ms | **208.7ms** |
| 5 | 941.0ms | 284.9ms | **209.0ms** |

With Linux's default `tcp_slow_start_after_idle=1`, cubic pays full slow start
on every single burst, forever. Its window returns to 223 packets each time and
941ms is five round trips: the ramp, repeated, six times, on a connection that
has been open the whole while.

BBR does not reset, because `tcp_slow_start_after_idle_check()` returns early
for any controller providing `cong_control`. It converges to 285ms instead --
better, but still 36% above the floor, and the reason is visible in the
`app_limited` column, which reads true from the second burst onward. BBR paces
at a bottleneck estimate it derived from these bursts, and the bursts are all
it has ever sent. 300KB in 285ms is 8.6 Mbit/s, which is what BBR believes the
path can do. The open-loop probe says 256 Mbit/s.

Setting `tcp_slow_start_after_idle=0` takes cubic to 209ms from the second
burst on. **One sysctl, 941ms to 209ms, a factor of 4.5** -- and tuned cubic
then beats BBR on the same path, because cubic does not pace and BBR's pacing
is the remaining constraint.

## Loopback, for comparison

On the same server, to itself:

| size | cold | warm |
|---|---|---|
| 300KB | 0.2-0.3ms | 0.1ms (28.9-31.5 Gbit/s) |
| 1MB | 0.3-0.4ms | 0.2ms |

A 300KB payload that takes 209ms across the path takes 0.1ms across loopback.
Every constraint discussed above is a function of the round trip, and at a
round trip of 0.05ms none of them binds. That is the entire argument for
terminating close to the application and carrying only the long leg on a
transport that knows about the path.

## What this path implies

- **The QUIC-based design is viable here.** UDP is not disadvantaged; upstream
  it is perfect and downstream it matches what any protocol would see.
- **Downstream is the direction that needs the transport.** It carries a
  memoryless 14% erasure that TCP converts into a 1,600-fold throughput
  penalty, and forward error correction is the right response to memoryless
  loss on a 200ms round trip where a retransmission costs more than the
  inference it carries.
- **Upstream needs almost nothing.** Connection reuse and one sysctl reach the
  round-trip floor. A transport that spent parity on this direction would be
  spending it for nothing, which is an argument for measuring and controlling
  the two directions separately rather than copying one model onto both.
- **The measurement instruments have to name their direction.** The first
  version of this document drew the wrong conclusion from correct numbers
  because two tools disagreed about who was sending.

## What this project's own transport does with the path

A gateway was run on the US host and a client on the Guiyang host, isolated from
the production deployment on the same server, and the same instrument measured
the same payloads directly and through the SOCKS5 listener. Completion is an
end-to-end acknowledgement from the far side in both cases: a client measuring
an upload through a proxy by watching its own socket is timing loopback, and
that makes any tunnel look arbitrarily fast.

### Download, the direction that erases 14%

| size | direct, median (range) | through Queqiao, median (range) | gain |
|---|---|---|---|
| 100KB | 1732ms (945-12307) | **396ms** (262-705) | 4.4x |
| 300KB | 11257ms (6377-17147) | **649ms** (288-664) | **17.3x** |

A 300KB response takes between six and seventeen seconds directly, and between
0.3 and 0.7 of a second through the transport. The spread collapses with the
median: direct varies by 2.7x across three runs, the tunnel by 2.3x on a much
smaller base.

The client's own counters say why. `queqiao_erasure_ratio_receive` measured
0.035-0.25 on the receive direction, `queqiao_coded_symbols_recovered_total`
reached 53, and `queqiao_erasure_residual_ratio_receive` stayed **0**. The code
was sized for the erasure the path was doing and repaired all of it, so no gap
ever cost a round trip. That is the mechanism the whole design exists for,
working on a live path, and it is worth noting that it is the fix landed in #52
that makes it work -- an earlier build sized the code from the controller's
deliberately low floor and would have carried almost no parity here.

### Upload, the direction that erases nothing

| size | mode | direct | through Queqiao | gain |
|---|---|---|---|---|
| 100KB | cold | 977-1092ms | **200-208ms** | 5.0x |
| 300KB | cold | 1157-1204ms | **216ms** | 5.4x |
| 1MB | cold | 2240-2381ms | **227-232ms** | **10.3x** |
| 300KB | warm | 193ms | 213ms | 0.91x |
| 1MB | warm | 224ms | 431-452ms | **0.50x** |

Cold flows gain five to ten times, and the reason is not erasure -- this
direction has none. It is that the client holds a pre-warmed pooled connection,
so an application flow costs half a millisecond locally plus one round trip,
paying neither handshake nor ramp. A direct 1MB cold flow spends 2.2 seconds
climbing to a rate it then stops needing.

Warm flows are where the transport costs something. At 300KB it is about 20ms.
At 1MB the first reading showed a factor of two, and that reading did not
survive being checked -- see the next section, which is the more important
result.

### What the baseline actually says

The datacenter plan predicted that today's transport would make this workload
worse. It does not:

- **Cold flows gain 5-10x** in both directions, from connection reuse alone.
- **The erasure direction gains 17x** at 300KB, from coding.
- **Warm flows cost 20ms at 300KB**, and are otherwise unchanged.

## The comparison, measured the way the previous section requires

Re-run with order alternation and pooling, which is now what `pathmeasure -mode
ab` does by construction. Each round pair runs A first and then B first, and the
report prints the order effect beside the arm effect so a comparison that has
not resolved its change says so.

**Download, 300KB, cold, 12 samples per arm:**

| arm | median | p25 | p75 | min | max |
|---|---|---|---|---|---|
| direct | **5449.7ms** | 2654.1 | 9052.9 | 1340.9 | 17116.5 |
| through Queqiao | **405.1ms** | 216.8 | 591.1 | 192.3 | 819.7 |

Arm effect 5044.6ms against an order effect of 548.4ms: the change is worth
nine times the confound, so this comparison resolves. **13.5x on the direction
that erases 14%**, and the tail improves more than the median -- the worst
direct sample is 17.1 seconds and the worst tunnelled one is 0.82.

**Upload, 300KB, warm, 30 samples per arm:**

| arm | median | p25 | p75 | min | max |
|---|---|---|---|---|---|
| direct | **209.8ms** | 198.7 | 215.1 | 192.2 | 765.3 |
| through Queqiao | **276.1ms** | 272.1 | 431.8 | 261.3 | 677.3 |

Arm effect 66.3ms against an order effect of 3.9ms. On a clean path with a warm
connection the tunnel costs **66ms**, about a third of a round trip. That is
the honest price of the framing, the extra local hop and the gateway's
processing, and it is the number to quote against the gains above rather than
the gains alone.

## How much of the win is this project, and how much is not TCP

`internal/baseline` exists to answer exactly this: a TUIC-shaped proxy on the
same QUIC fork, the same congestion controllers, and the same process, so a gap
between it and Queqiao is the design rather than the library. Run as a third
arm, order-alternated:

**Download, the direction that erases 14%, order-alternated:**

| payload | direct TCP | TUIC-shaped QUIC | Queqiao | QUIC's share | coding adds |
|---|---|---|---|---|---|
| 100KB | 1918.4ms | 401.0ms | **218.5ms** | 4.8x | 1.84x |
| 300KB | 5449.7ms | 791.7ms | **399.1ms** | 6.9x | 2.0x |

Arm effects of 392.7ms and 182.5ms against order effects of 13.7ms and 84.4ms,
so both comparisons resolve.

The medians are not the most interesting column. The spread is:

| payload | direct TCP | TUIC-shaped QUIC | Queqiao |
|---|---|---|---|
| 100KB | 806-4722ms | 214-801ms | **204-220ms** |
| 300KB | 1341-17117ms | -- | **192-820ms** |

Sixteen consecutive 100KB downloads through Queqiao spanned sixteen
milliseconds, on a channel erasing a seventh of everything, at 200ms round
trip. That is one round trip, every time: the code repaired every gap in the
round trip that carried it, so no loss ever cost a retransmission. The same
sixteen downloads over TCP spanned four seconds.

Small flows gain the least in the median and the most in the tail, which is the
opposite of the usual expectation and follows from a mechanism worth naming. A
100KB payload is about seventy packets, too few to reliably produce the three
duplicate acknowledgements fast retransmit needs, so a loss falls through to a
retransmission timeout. The smallest flows are the ones least able to recover
cheaply, and they are also the ones an inference call is made of.

The decomposition is the useful part, and it is not the flattering one.
**Moving off TCP is worth 6.9x of the 13.7x.** Most of the opportunity on this
path is available to anything that stops treating rate-independent erasure as
congestion, and QUIC with a modern controller does that without any of this
project. What erasure-aware coding adds on top of good QUIC is a further **2.0x**
-- real, worth having, and the smaller half.

Stated the other way round, which is how it should be stated: Queqiao's claim
on this path is that it is twice as fast as a well-configured QUIC tunnel, not
that it is fourteen times faster than TCP. The second number is true and mostly
belongs to QUIC.

**Upload, 300KB warm, the direction that erases nothing:**

| transport | median |
|---|---|
| direct TCP | 209.8ms |
| Queqiao | **262.2ms** |
| TUIC-shaped QUIC | 284.0ms |

Arm effect 21.8ms against an order effect of 0.1ms. Both tunnels cost 50-75ms
over direct TCP on a clean path with a warm connection -- that is the price of a
userspace proxy and an extra local hop, not of anything either design does --
and Queqiao is the cheaper of the two by 21.8ms.

## An A/B that measured the experiment instead of the change

The one apparent regression -- repeated 1MB requests taking 431-452ms against
224ms direct -- was attributed to flow classification, because the counters
showed a flow demoted to bulk and the classifier's 128KB threshold makes that
inevitable for a megabyte request. A profile was built to prevent the demotion.

It prevents it. `queqiao_class_transitions_2_total` reads 1 to 4 on the
access-link profile and 0 on the datacenter profile, every run. The mechanism
does exactly what it was written to do.

It makes no difference to latency at all.

Three interleaved rounds of eight warm 300KB flows per profile, then three more
with the order reversed, 48 samples per profile:

| | median | p25 | p75 |
|---|---|---|---|
| access-link profile, bulk allowed | 454.1ms | 295.6 | 508.6 |
| datacenter profile, bulk prevented | 456.5ms | 295.9 | 662.7 |

Two and a half milliseconds apart on a base of 455. Sorting the same 96 samples
by position in the measurement sequence instead of by profile:

| | median | p25 | p75 |
|---|---|---|---|
| whichever profile was measured **first** | 304.6ms | 292.3 | 535.4 |
| whichever profile was measured **second** | 463.1ms | 297.4 | 627.3 |

**Run order is worth 158ms and the change under test is worth 2.4ms.** The
first three rounds measured the access-link profile first and appeared to show
it winning by 53%; reversing the order reversed the finding. What was being
measured was position in the sequence.

Two conclusions, and the second is the one worth keeping.

The classifier change is **principled but unproven**. A request that goes quiet
between bursts is not a transfer seeking throughput, and calling it one is
wrong regardless of what it costs -- but on this path it costs nothing
measurable, and the plan's justification for the change, that it removes a
regression, is not supported. It remains behind an experimental profile that no
existing deployment selects, which is the right place for a change whose
benefit has not been demonstrated.

And **this path cannot support an A/B of anything smaller than about 30%
without alternating order and pooling.** Identical warm 300KB flows ranged from
250ms to 2906ms. Any comparison here that runs A then B, once, is measuring the
sequence. The two earlier claims in this document that did not survive checking
-- that UDP was policed harder than TCP, and that classification cost a factor
of two -- were both produced by a confound rather than by wrong numbers, which
is the failure mode worth designing the instruments against.

## The hierarchical path model, and what this path could not tell us about it

The path is modelled as a chain of segments -- the uplink, then the peer --
rather than as a single endpoint pair, with a flow permitted only what the
tighter segment allows, and a node below a hundred observed samples permitted
to constrain nothing.

Enabled on the datacenter profile and measured against the flat model,
order-alternated, 30 samples each, 300KB download:

| model | median | p25 | p75 |
|---|---|---|---|
| flat, one model per endpoint pair | 607.8ms | 559.3 | 802.3 |
| hierarchical | 585.5ms | 461.8 | 816.6 |

Arm effect 22.3ms against an order effect of 11.5ms. The arm wins by a factor
of 1.9, which is thin: **this is a no-regression result and not an
improvement**, and 22ms on a 600ms base with this path's variance is not a
number to build on.

The more useful statement is what the measurement could not cover. This
deployment has **one** provider, so the chain is the uplink node and the peer
node carrying identical traffic -- the tree is degenerate here by construction,
and what was actually measured is the confidence rule, not the hierarchy. The
hierarchy earns its keep where flows go to different places over one uplink,
which is what a multi-provider client or a node relay serving several regional
gateways does, and neither exists on this path.

So the gate the plan set -- no regression before any claim -- is met, and the
claim it was gating remains unmade. The mechanism is in place, tested against
its degenerate cases, and off by default.

## The tail under concurrent requests

The datacenter workload is not one flow. It is many concurrent requests, every
one of them latency-critical, and `docs/COMPARISON.md` records that the tail
under load is this transport's weakest published number: SSH p99 at 940ms
against TUIC's 662ms. On an access link that is a caveat, because the bulk
transfer it competes with is also the thing being won. Here there is no bulk
transfer, so the tail is not a caveat -- it is the result.

Measured on the emulated path (`pathsim.DCLongHaul`, the figures above), 300KB
requests started together, against the TUIC-shaped reference on the same QUIC
fork in the same process:

| concurrency | queqiao | reference |
|---|---|---|
| 1 | 1/1, p50 2395ms, p99/p50 **1.00** | 1/1, p50 5054ms |
| 4 | 4/4, p50 2302ms, p99/p50 **1.07** | 4/4, p50 **35637ms** |
| 16 | 16/16, p50 3269ms, p99/p50 **1.06** | **0/16 completed** |
| 48 | 48/48, p50 6445ms, p99/p50 **1.05** | **0/48 completed** |

Two things, and the second is the one that was not expected.

**Every flow completes, and the tail stays within 7% of the median at every
load.** Forty-eight times the offered load costs 2.7x the latency, which is
sublinear, and the p99/p50 ratio moves from 1.00 to 1.05 rather than diverging.
Whatever the scheduler is doing, it is not starving anyone.

**The reference does not degrade, it falls over.** Four concurrent flows already
cost it 35.6 seconds -- fifteen times its own single-flow figure -- and at
sixteen it completes nothing inside a two-minute deadline. The mechanism is
visible in the shape: one QUIC connection carrying one stream per request,
where a lost packet stalls its stream until a retransmission crosses a 200ms
round trip, and 14% of packets are lost. Queqiao repairs the gap inside the
round trip that carried it, so no stream ever waits.

The datacenter plan expected the interactive tail to be this profile's problem,
on the grounds that the published loss was a tail number and the workload is
nothing but tails. On this path it is the opposite: the tail is the strongest
result in this document.

Three limits on the claim. The absolute figures are bounded by the emulator and
the host it runs on -- 48 requests of 300KB is 14.4MB, which the modelled
333 Mbit/s would carry in about a third of a second, so 6.4s is not the path's
doing and only the comparison transfers. The reference is a measurement control
rather than a tuned product, and a real TUIC deployment would be configured by
someone who wanted it to work. And this is one emulated path: the same
comparison on the live link, where these numbers came from, is the check that
has not been run.

## The other traffic shape, where the tail is a real problem

A voice session sends a 20ms frame of audio fifty times a second and a gateway
carries hundreds at once. Nothing about that resembles the burst above: the
payloads are tens of bytes rather than hundreds of kilobytes, the flow never
ends, and what matters is not when it completes but whether any single frame
arrives late.

Twenty-four sessions, a hundred 80-byte frames each, on the same emulated path:

| | p50 | p90 | p99 | p99.9 |
|---|---|---|---|---|
| frame round trip | 203.3ms | 208.2ms | **766.6ms** | 909.4ms |

The path's own round trip is 200ms, so the median frame is 3.3ms above the
floor and the p90 is 8.2ms above it. Ninety-five percent of frames are as good
as the path allows. **Then 4.96% of them arrive more than a frame interval
late, and the p99 sits 567ms above the floor.**

This is the reverse of the burst result, and the reason is the payload size
rather than the concurrency. A 300KB request is two hundred packets: enough
that a coding block is worth building and enough that three duplicate
acknowledgements arrive to trigger a fast retransmit. An 80-byte frame is one
packet, sent 20ms after the last one. There is no block to code and no
subsequent traffic to produce duplicate acknowledgements, so a lost frame waits
for a retransmission timeout -- and on a 200ms path that is most of the 567ms.

So the two shapes of §2.3 give opposite answers on the same path with the same
transport. Requests: p99 within 7% of the median at every concurrency tested.
Frames: p99 at 3.8x the median, with a twentieth of them audibly late.

The gap is not a scheduling failure and will not be fixed by a fairer queue.
Everything that makes coding work for the burst -- a block to compute parity
over, and traffic behind the loss to reveal it quickly -- is absent for a
single small frame. Closing it means coding across frames from the same
session, which spends parity on a stream that is mostly silence, or across
sessions, which couples flows that have no other reason to be coupled. Both are
real designs and neither is implemented. This is the open problem the
datacenter profile has, and it is a narrower and more specific one than "the
interactive tail", which is what the plan expected to find.

## The live link, and why one number for it is the wrong shape of answer

Everything above was measured across one evening. Re-running the same
comparisons a few hours later produced a different path.

Downstream loss, three measurements within about ten minutes of each other:

| when | offered | loss |
|---|---|---|
| t+0 | 2 Mbit/s | **0.0%** |
| t+0 | 20 Mbit/s | **2.4%** |
| t+9min | 20 Mbit/s | **9.1%** |

Earlier the same evening it read 9.8-17.5% across a decade of offered rates.
**This path's erasure moves between roughly zero and seventeen percent on a
timescale of minutes**, and every figure in this document is a snapshot of
whatever it was doing at the time.

That is not a caveat to append; it changes what a result means. The download
comparison run when the path was erasing 14% gave 13.5x. The same comparison,
same command, run when it was erasing somewhere between 2.4% and 9.1%:

| arm | median | p25 | p75 | min | max |
|---|---|---|---|---|---|
| direct | 1820.1ms | 1123.8 | 2874.5 | 938.7 | 19275.1 |
| through Queqiao | **281.2ms** | 273.6 | 365.5 | 268.7 | 623.3 |

Arm effect 1538.9ms against an order effect of **0.7ms** -- the cleanest
comparison in this document -- and a gain of **6.5x** rather than 13.5x.

Both numbers are correct. The transport repairs erasure, so its advantage is a
function of how much erasure there is, and on this path that is a variable
rather than a constant. The honest statement is the relationship: **6.5x at
single-digit loss, 13.5x in the teens, and by extension very little at zero**,
which is what the clean upload direction already showed, where the tunnel costs
50-75ms and buys nothing.

Two consequences for how this path is used as evidence.

**Every comparison needs a contemporaneous loss measurement**, taken in the same
minutes and reported beside it. A result quoted without one cannot be placed on
the curve above, and two results quoted without one cannot be compared to each
other at all.

**The order alternation is not optional here.** It was already justified by a
158ms position effect; on a path that changes this much between runs, an
unalternated A/B is measuring the weather.

## The concurrent workloads on the live link

Both shapes of §2.3, run on the live path rather than the emulator.

**Requests, upload direction, started together:**

| flows | direct p50/p99 | queqiao p50/p99 | direct aggregate | queqiao aggregate |
|---|---|---|---|---|
| 4 | 937/937ms | 991/992ms | 8.7 Mbit/s | 8.4 Mbit/s |
| 16 | 938/997ms | 837/840ms | 18.4 Mbit/s | **46.4 Mbit/s** |
| 32 | 927/1017ms | 723/904ms | 35.4 Mbit/s | **86.1 Mbit/s** |

Both complete every flow at every level and both keep a tight tail, which
confirms the emulated result on the real path. The aggregate is the new part:
at sixteen and thirty-two concurrent flows the tunnel moves two and a half
times the traffic and finishes the batch in 0.8-0.9s against 2.1-2.2s. This is
the clean direction, so it is connection reuse and aggregation rather than
coding.

**Frames, 80 bytes every 20ms, bidirectional:**

| sessions | arm | p50 | p90 | p99 | >250ms | >400ms |
|---|---|---|---|---|---|---|
| 16 | direct | 195.3ms | 207.1ms | 606.6ms | 3.25% | 3.25% |
| 16 | queqiao | 185.6ms | 192.0ms | **658.8ms** | 2.88% | 2.31% |

Slightly better median and p90, slightly worse p99, and no meaningful
difference in how many frames arrive late. This ran while the path was in its
clean phase, which is the reason: there was almost nothing to repair. It is
consistent with the emulated finding rather than contradicting it -- the
emulator held loss at 14%, where the frame tail was a real problem, and the
live path during this run was not doing that.

The first version of this table reported direct losing 30% of frames to
lateness against Queqiao's 2%, which was an artefact. Lateness was being counted
against each arm's own floor, so the arm with the 10ms lower median had 10ms
more room beneath its own bar and reported fewer late frames for that reason
alone. Counting against fixed thresholds -- 250ms, 400ms, a second -- removes
it, and removes the finding with it. A listener does not have a relative bar.

## What HTTP/2's flow-control window actually costs, and who actually pays it

The datacenter plan claims that HTTP/2 flow control is usually the largest
single component of a slow upload and the one no TCP tuning reaches. The
mechanism is real, the magnitude is larger than claimed, and the word "usually"
was wrong.

The same 1MB upload to the same server on this 200ms path, differing only in
the window it advertises:

| receiver window | 300KB warm | 1MB warm |
|---|---|---|
| Go's `http2.Server` default | 187.5ms | 193.2ms |
| 8MB, set explicitly | 191.3ms | 200.6ms |
| **65535, the RFC's default** | **912.7ms** | **3903.0ms** |

At the RFC value a megabyte takes **twenty times** as long, and the arithmetic
is exactly the one the plan predicted: the window is credit per round trip, so
1MB over 64KB is sixteen round trips, and sixteen times 200ms is 3.2 seconds
against 3.9 measured. 300KB is 4.7 round trips, 940ms predicted against 912.7
measured. The mechanism is confirmed to within the path's own noise.

**But Go's server does not use 65535.** `x/net/http2` defaults both
`MaxUploadBufferPerStream` and `MaxUploadBufferPerConnection` to 1MB, and gRPC-go
goes further and tunes the window from a measured bandwidth-delay product. The
64KB figure is what RFC 7540 specifies, not what a Go service running on a
default configuration advertises -- and the first two rows above are
indistinguishable because both were already far above the ceiling.

So the claim that survives is narrower and more useful than the one that went
in: **when the receiver's window is 64KB this dominates everything else by a
factor of twenty, and whether it is 64KB is a property of the implementation
rather than of HTTP/2.** Measure it before assuming it. The check is cheap and
the two answers are five hundred milliseconds apart on a payload an inference
call actually sends.

The related trap is unchanged and worth keeping: RFC 7540 6.9.2 makes
`SETTINGS_INITIAL_WINDOW_SIZE` govern stream windows only, so a stack that sets
it and forgets the connection window is capped across every stream at once.
Both are set together above for that reason.

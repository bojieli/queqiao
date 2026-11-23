### What the real path showed

The three ruled-out explanations below were all about the *sampler*. The
instrumentation, deployed to the live China-US gateway on a build behaviourally
identical to the one running there, showed the fault was not in the sampler at
all. It was in two paths that put a number into the filter without it ever
having been a sample:

| | estimate | widest sample ever taken | ratio |
| --- | ---: | ---: | ---: |
| Gateway | 2,260,356 | 1,649,254 | 1.37x |
| Client | 19,414,758 | 4,732,767 | **4.1x** |

A maximum over samples cannot exceed its widest sample. Something else was
supplying it.

**`restartFromIdle` re-seeds an all-time peak.** `peakBandwidth` only ever rose,
and the seed fires whenever the pipe empties -- which on a connection that is
application limited essentially always is constantly. Its own comment says the
bet is safe because "the filter's own window disproves it within ten rounds";
it cannot, because the seed is re-armed faster than any window retires it. The
peak now carries the time it was observed and is not put back once a
measurement of that age would have expired.

That is also why the two attempts on the sampler changed nothing. Bounding the
filter's memory fixed how long a *sample* survives, and this path was not
supplying samples.

**The pooled seed is inflated twice, and neither is fixed here.** A joining
lane starts at `(aggregate bottleneck / lanes) / arrival`, and the division is
the wrong way round: `seedBandwidth` feeds the delivered-rate filter, and
`ErasureSender.bandwidth` divides that filter's output by the arrival rate
again when it paces, so a seed meant to produce a given pacing rate should be
multiplied by arrival rather than divided by it. The error is `1/arrival`
squared -- 3x on a channel erasing 42%, and 44x at the 0.15 arrival floor.

Underneath it, what the pool is given as `Delivered` is `TUICBBRSender.bandwidth`,
which returns the pacing rate. That is the same "it measured its own output"
mistake found in the compensation gate and fixed there, still present one level
up: `pathmodel` states that the bottleneck is measured from what contributors
deliver, and it is being handed what they were told to send.

Both predate this work -- v0.2.0 passes the same value, and the division dates
to 2026-08-17 -- so neither is a regression, and both are left alone here for
the reason the rest of this document argues for: one change at a time, measured
on the live path, and the measurement above does not separate their
contribution from the first. They fire only when a lane joins a path other
lanes have already mapped, which is the replacement-lane case rather than the
first-flow case.

### What it cost, and what it bought

On the emulated policer the overdrive falls from 7.3x to **2.4x** and the loss
from 49.8% to 36%. Cumulatively the case has gone 42x, 7.3x, 2.4x.

On the 42% erasure channel it costs about 15%: a median 1.23 s against 1.04 s
measured on the same machine in the same window. That cost is real and it is
the honest price of not pacing from data the filter has already decided not to
trust. Two things temper it. The test transfers 48 KB across a 300 ms path,
which is about four round trips and therefore almost entirely ramp -- a worst
case for any change to seeding. And the alternative bound was measured: the
filter's ten-second ceiling costs the same 15% and leaves the policed path at
9.5x rather than 2.4x, so the cheaper-looking option is not cheaper.

### What is still left

2.4x is not braked. The path remains unbraked in the sense that neither loss nor
delay reaches the controller on a policer; what has changed is that the
controller is no longer being handed a number nothing measured.

### The pacer was never wired to any of this

Everything above is about what the controller believes and what brakes it. The
pacer, which decides when a packet may actually leave, consulted none of it. It
metered every send at the bandwidth estimate, and for a request-shaped flow that
estimate is low precisely because such a flow is application-limited: measured
on the datacenter path, one flow estimated 42 Mbit/s where sixteen estimated 88
in the same minutes. So a flow that had produced no congestion evidence was
given a congestion response, and on that path it cost 67ms of a 299ms request,
which was the whole of this transport's deficit against a tuned TCP client.

It now meters only when the delay bound is reached or the estimator attributes
loss to rate. Below that, the congestion window and the acknowledgement clock
bound the send, which is what an unpaced TCP does.

**And a policer defeats it, for the same reason a policer defeats everything
else here.** It holds no queue, so the delay bound sees nothing, and its loss is
sustained enough that the floor rises to meet it and the congestive component
reads zero. Gating on those two signals alone took the emulated policer from
3.0x overdrive to 4.0x and its loss from 32.9% to 54.9%, because metering had
been the last thing holding the rate down. The burst therefore also requires the
sending direction to be delivering essentially everything, which is a refusal to
act on the absence of the first two rather than a third signal.

That guard is a loss threshold, and this design otherwise refuses thresholds:
the delay bound is a ratio precisely so that no duration has to be chosen. It is
defensible only because the two paths measured sit far apart on either side of
it, the datacenter upload having lost 0 of 41,663 datagrams and the policer a
third of everything. A path in between would make it a guess, and what would
replace it is the same thing the rest of this document is missing.

### Two attempts on the wrong component

**Bounding the filter's memory in wall time.** The filter kept a sample for ten
packet-timed rounds, which on an application-limited connection was sixty-six
minutes; expiring samples in time as well as in rounds fixed that, and it is
worth having. It does nothing for a policer. The bursts recur every refill
period, so there is always a recent high sample and the estimate never has to
fall back to anything. **Age was not the problem.**

**Averaging the sample within a round.** The filter is fed the largest
per-packet delivery rate in an acknowledgement batch, each measured over one
packet's own send-to-ack window; on a path that releases traffic in quanta
those windows land unevenly, so the distribution has an upper tail and a
maximum reports the tail. Feeding one rate per round should have removed the
tail. It measured no better -- 2.7x the shaped rate became 3.6x -- and was not
shipped.

**A synthetic estimator harness** reported 4,000 B/s on a known 250 KB/s
schedule. The harness's own acknowledgement pacing was wrong. That failure is
what made the next one careful enough to be right.

**It accepted the first proposal whole.** By the time erasure is first measured
the sender has already burst and been policed, so the first request is for
several times the rate, with no evidence yet to test it against. Compensation
now starts at none and takes a tenth of the remaining distance per round, so
each step is small enough that the next round's delivery can be attributed to
it.

**It measured its own output.** The bet was judged against
`TUICBBRSender.bandwidth`, which returns the *pacing rate* -- a value this
compensation is an input to. Every increase therefore appeared to have worked.
It now reads the delivered-rate estimate.

With both corrected, the overdrive on the emulated policer falls from **42x the
path to 7.3x** and the loss from 72.5% to 49.8%. The 42% erasure channel is
unaffected at a median 1.18 s against a 1.10 s baseline, which is what says the
bound has not simply been switched off.

### What was left at 7.3x, and what became of it

Two terms were named here when the case stood at 7.3x: the startup gain of
2.77, which is BBR probing and should end when delivery stops growing, and a
bandwidth estimate that read 2.0x the path in the full stack against 1.01x when
the estimator was driven in isolation. That gap was called *the next thing to
measure rather than the next thing to guess*, and measuring it is what produced
the section at the top of this document: the estimate stood at four times the
widest sample the connection had ever taken, which a maximum over samples
cannot do, and the second path putting a number into the filter was
`restartFromIdle`. That took the case to 2.4x. The startup gain has not been
revisited.

Five attempts have now been made on this. The first three reasoned about what
the code should be doing and were each refuted by measurement, the third by a
measurement that was itself broken. The fourth and fifth started by measuring,
and each found the fault somewhere none of the earlier ones had looked.

**At that stage, the policed path was still unbraked at 2.4x.** An earlier
draft of this line said it should not be, which was the right
call while the delay bound was the only brake being built and the compensation
was still feeding itself. It is shipped now because the alternative was worse
in the case that was actually hurting users -- a downstream residual of 11%,
against 0.16% measured on the live path after this work -- and because the
policed overdrive was reduced by a factor of eighteen on the way. Neither of
those makes 2.4x braked. The pooled-seed work below (#73) revisits the case
after replacement lanes became more frequent; the path is still unbraked.

## Sequencing

An earlier draft opened with "aggregate enforcement point driven by the
controller, not a flag". That step does not exist: the enforcement point is the
congestion window capped at the pooled share, and it was already there. What it
needed was an estimate that could fall.

1. **Done.** Bandwidth filter expires on rounds or wall time, whichever first;
   an application-limited sample may replace an expired estimate.
2. **Done.** The shared path model carries the measured erasure and burst
   alongside the controller's floor, and `channel()` reads the measurement
   instead of fabricating a snapshot from one scalar.
3. **Done.** Parity sized by the rate that delivers a block soonest;
   `TargetResidual` and `minCodedLoss` deleted.
4. **Done.** The loss the path caused is exported, not only the share charged
   as congestion.
5. **Done.** Delay bound added: the round trip may not exceed twice the path's
   own minimum.
6. **Done.** Loss suppression deleted and the arrival-rate compensation moved
   onto the measurement, gated on a measured round trip so it cannot run ahead
   of the brake that bounds it.
7. `kindReport` closes the residual loop.

Steps 1-5 have no wire impact. Step 6 is additive and forward-compatible.

Splitting the delay bound from the removal of loss suppression is not cosmetic.
Deleting suppression hands every erasure to BBR as congestion, and on a 42%
erasure channel that is the collapse to 0.39 Mbit/s this controller exists to
avoid. Nothing may remove it until the delay bound is in place to brake
instead. Until then the two consumers read different numbers from the same
model, which is what the design argues for anyway: the controller keeps its
conservative floor, the code reads the measurement.

## Open questions

- **The delay budget is the one remaining constant.** It is a latency policy
  rather than a bandwidth number, so it does not violate the rule that
  bandwidth must be measured. Whether it is fixed, derived from `min_rtt`, or
  exposed is undecided.
- **Bucket detection.** Goodput that is high and then decays at constant `B` is
  a draining token bucket, which argues for lengthening the measurement window.
  Detecting it is a measurement; the threshold for acting on it is not yet
  specified.
- **Mixed-fleet behaviour during step 6.** Until both ends report, half the
  paths run open-loop. Acceptable, or gated behind a capability exchange?
- **A congestion signal a policer cannot hide from.** This is the same gap as
  "2.4x is not braked" above, and it now has a second dependant: the unmetered
  burst is guarded by a loss threshold only because nothing else can see a
  policer. Whatever answers one answers both.

### The pooled seed and pooled delivery (#73)

Three quantities meet when a lane joins a path its siblings have mapped, and
all three are in delivered bytes per second except the last:

- `State.Seed` is the pooled bottleneck split across the lanes, computed from
  what the lanes report as `Observation.Delivered`.
- The inner BBR filter (`seedBandwidth`, then the max filter) is a delivered
  rate.
- `ErasureSender.bandwidth()` divides the inner rate by the arrival rate once,
  which is what turns a delivered rate into a wire rate.

So the consistent seed is `State.Seed` as it is. Main divided it by the arrival
rate (compensation applied twice, the 1/arrival^2 of the issue); the first
draft of this fix multiplied by it (compensation applied zero times). The issue
derived "multiply" while lanes still reported their pacing rate as
`Delivered`, which is no longer true.

Two further faults sat behind the conversion.

**Seed echo.** The inner estimate contains the seed, so a lane that had
delivered nothing reported its seed back as delivery and the pool counted its
own figure twice. With one lane established at B on a clean path, four joins
inside the ten-second bottleneck window took the pooled bottleneck to 1.5B,
2.0B, 2.5B and 3.0B. The estimator now keeps a second max filter fed only by
its own delivery samples, and that is what is reported: zero until the lane
has a sample, which still counts it as a member of the split but adds nothing
to the bottleneck. The same filter also keeps a peak restored by
`restartFromIdle` out of the pool.

**Compensation reset at the gate.** A joiner inherited the pooled erasure as
its arrival rate, but the compensation gate started at none, as a lone sender
does, and the first loss after the round trip was measured set it to none.
At the arrival floor the joiner then paced at 0.43x its seed and delivered
0.065x of it, and stayed there, because the seeded estimate was the bar the
next compensation step had to clear. Starting the gate at 1-Erasure fixes that
on an erasure channel and overdrives a policer, whose loss is high but whose
lanes were refused the compensation (below). So lanes now report the
compensation their gate has accepted (`Observation.Arrival`), the model pools
it by sample count and keeps it as path knowledge, and a joiner starts its
gate, and its pre-gate pacing, from that earned value. Anything further is
earned round by round as before.

Two details keep that from becoming a ratchet (both found in review). The
gate judges a step by whether delivery rose since the last one, from a
baseline of zero; a seeded joiner's estimate is above zero before it has
delivered anything, so the first round after an ACK granted a step for the
seed itself, the lane reported it as earned, and the next replacement
inherited it and stepped again. The seeded estimate is now the gate's
baseline. And while siblings have measured loss but none has decided any
compensation (`Arrival` zero), a joiner publishes the measured erasure for
the code but paces and sizes its window uncompensated; reading 1-Erasure
there paced 6.7x the seed at 85% loss on nothing anyone had earned.

The decision, then: **seed unscaled, report only measured delivery, inherit
earned compensation rather than raw erasure.**

#### Probe

On `a6642134` with Go 1.27.3 on darwin/arm64, a fixed sibling state of
Seed = 1,000,000 B/s and 200 ms RTT, read before any delivery (`wire`), after
one loss event (`report`) and after the sender has its own minimum round trip
and one more loss event, so the compensation gate has run (`gated wire`).
`earned` is the arrival the siblings had been allowed: the arrival rate on an
erasure channel, 1 on a policer.

```sh
go test ./internal/congestion -run '^$' -bench '^BenchmarkJoiningLaneSeed$' -benchtime=200x
```

| Path | Filter / seed | Report / seed | Wire / seed (uncapped / 1.25x share) | Gated wire / seed (uncapped / share) |
| --- | ---: | ---: | ---: | ---: |
| First draft, clean | 1.000 | 1.000 | 2.885 / 1.250 | 2.885 / 1.250 |
| First draft, 42% | 0.580 | 0.580 | 2.885 / 2.155 | 1.673 / 1.250 |
| First draft, 85% (floor) | 0.150 | 0.150 | 2.885 / 2.885 | 0.433 / 0.433 |
| This, clean | 1.000 | 0 | 2.885 / 1.250 | 2.885 / 1.250 |
| This, 42% erasure | 1.000 | 0 | 4.974 / 2.155 | 4.974 / 2.155 |
| This, 85% (floor) | 1.000 | 0 | 19.23 / 8.333 | 19.23 / 8.333 |
| This, 42% policer (earned 1) | 1.000 | 0 | 2.885 / 1.250 | 2.885 / 1.250 |
| This, 85% loss, nothing earned yet | 1.000 | 0 | 2.885 / 1.250 | 2.885 / 1.250 |

The first draft's "2.885 everywhere" was measured before the gate. After it,
an uncapped joiner is now at startup gain / arrival, and stays there through
the gate. The share still caps it at 1.25 seeds of delivery.

The seed-echo test (`TestJoiningLanesDoNotEchoTheirSeedIntoThePool`, one lane
at B and four joins) went from 1.5B, 2.0B, 2.5B, 3.0B to B after every join.

#### Policer (case 4)

`TestCase4APolicedPathIsBoundedNotRunaway`, formerly
`TestCase4APolicedPathIsStillUnbraked`: 250,000 B/s token bucket refilled
every 8 ms, 300 ms RTT, path seed 53, same host. Peaks are controller readings
in units of the policer's sustained rate, not throughput.

| Variant | Runs | Peak pacing | Peak estimate | Final loss |
| --- | ---: | ---: | ---: | ---: |
| Main (from the first draft's notes) | 2 | 9,544-57,266 | 8,590 | 65.8-70.0% |
| First draft (seed x arrival, report estimate) | 4 | 2.9-4.0 | 1.0-1.2 | 14.5-23.1% |
| Unscaled seed, measured report, gate reset to none | 4 | 3.0-4.3 | 1.1-1.6 | 13.3-15.4% |
| Unscaled seed, measured report, gate from 1-Erasure | 6 | 4.2-11.5 | 1.1-1.9 | 16.4-25.5% |
| Gate from earned compensation (`ec51b2b`) | 23 | 3.0-7.6 | 1.0-2.0 | 13.0-22.3% |
| This (plus seeded gate baseline, no pacing on unearned loss) | 32 | 3.1-8.6 | 1.0-2.0 | 12.9-22.0% |

The delay brake stayed at zero in every run: a policer is still unbraked. The
queue stayed at or under 27 ms in every `ec51b2b` run and in 29 of the 32
final runs; three final runs read 80-95 ms (smoothed RTT 380-397 ms against a
300 ms minimum) and failed the queue check, none of them in an interleaved
batch of eight runs per arm. Whether that is the change or host scheduling is
not settled by this sample.

The test asserts a peak estimate under 3x and peak pacing under 12x of the
policer rate, which main's runaway fails by three orders of magnitude. A
collapse is judged by delivery instead, since a sender can clear a peak at
startup and then stall: the bytes echoed over the later half of the run must
be at least 0.1x the policer rate. The exchanges are sequential, so this is
far below line rate; over 24 runs it was 0.17-0.35x. The queue and brake
check is unchanged.

The gate-reset variant is the gentlest on the policer, and it is the one
that collapses a joining lane on an erasure channel, which this test cannot
see. That trade-off, and whether the higher starting rate on a high-erasure
link pays for itself in goodput, is what the live-path A/B in the pull request
has to settle.

### A joining lane ramps before its share caps it

The live A/B for #73 found the honest pool had made the share a ceiling. On a
fast path with a moderate erasure floor (about 150 Mbit/s, 170 ms, 15%) four
concurrent downloads ran at 52.5 Mbit/s against main's 142, slower in every
paired round, with the loss at the path's floor and a queue p90 of 0.5 ms:
under-used, not overdriven. Each trial was a burst of joins onto a pool that
had idled down to about 40 kB/s.

The share is `shareProbeGain` times an even split of the pooled bottleneck,
and the bottleneck is what the members delivered. That lets the members grow
the bottleneck by a quarter per measurement only if every member is using its
share. A member that is not -- the pooled control connection, a lane that
finished or stalled and has not expired -- still takes an even split, so four
bulk lanes beside one idle member get `1.25 x 4/5 = 1.0` times what they
delivered, and the bottleneck does not grow at all. The trace showed exactly
that: lanes plateaued with the reported delivery, the pooled share and the
estimate all equal. Main hid it, because the echoed seeds inflated the share
until it did not bind.

A second fault made it permanent. A lane held below its window by a share
looks application-limited at every acknowledgement, and BBR's startup exit
tests skip application-limited rounds, so every capped lane in the trace sat
in STARTUP for the whole transfer, pacing at the startup gain over its last
estimate, with the share as its only bound.

**The change.** A lane that joins from a pooled seed starts *ramping*
(`ErasureSender.rampGate`). While it ramps the share does not cap it; its own
BBR startup, from its honest seed, does. The ramp ends, for good, at the first
of:

- a quarter round trip of queue (`SmoothedRTT >= 1.25 x minRTT`);
- a round losing more than the erasure the pool had measured when the lane
  joined, by three standard deviations of the two-proportion test between
  them (`lossAboveFloor`) and by at least three packets. The floor's weight
  is `State.ErasureSamples`: the outcomes the current erasure rests on, from
  each estimator's effective (forgetting) count and carried through a
  `Chain` from the node that supplies the erasure -- not the lifetime
  high-water mark `ObservedSamples`, which can belong to an expired member
  and grows without bound. Both sides are deflated by the pool's burst
  factor, since losses in runs of b are about n/b independent trials; Both are estimates:
  a pool that has decided ten packets reports a floor of zero with nothing
  behind it, and against a fixed zero one ordinary loss would read as
  significant. Rounds, or floors, resting on fewer than 64 packets are not
  judged;
- three rounds without a quarter's growth in the delivery this lane measured
  itself, counting only rounds in which it had at least half its window in
  flight -- BBR's own full-pipe test, minus the application-limited skip that
  never let a capped lane leave startup, and minus the rounds in which a lane
  carrying requests before its transfer was not probing anything. (A refused
  pacing poll is not that evidence: quic-go polls after the last packet of a
  request too.) This is what catches a policer whose drops are already the
  floor.

When it ends, the lane takes its share and its controller leaves startup, as
BBR would have at a full pipe. While it ramps, the inner controller's own
startup exits are held (`TUICBBRSender.startupHeld`): their loss exit fires at
2% loss, which an erasure channel exceeds every round, and the first live run
showed lanes leaving startup on ordinary erasure at a fraction of their share
and crawling in ProbeBW. The ramp's queue check also measures against the
pool's minimum round trip at join, so a lane that joins behind a standing
queue -- and so measures its own minimum inside it -- still sees the queue.
Nothing reported to the pool changes:
`Delivered` is still only what the lane measured, so the seed and bottleneck
stay honest. Only the cap moves. A lane that joins an empty model has no seed
and no share, and behaves as before.

**Compensation.** The first simulation of the ramp found it filled the path
and then held a shallow buffer dropping a third of what was sent, for the rest
of the transfer. The ramp was not the cause; it was what let the lanes reach
the regime in which the compensation gate fails. Overflow loss is pooled as
erasure, the gate judges each step by a max filter that drifts up with noise,
and it granted the overflow its compensation a step at a time, until the
shares were paced at twice the path. So a joined lane is granted no increase
in compensation while it ramps (delivery rises because the ramp raised the
rate, which says nothing about the compensation), nor afterwards while its
last round, or the pool's erasure, is significantly above the floor it joined
with. The pool has to be read as well as the lane, because a shared tail-drop
queue does not share its drops evenly: the lane that took its compensation
from 0.85 to 0.43 lost at the floor itself while its siblings lost 35-50%.
Reducing compensation is still immediate. The veto expires: overflow lasts
only while the lanes overdrive, and the shares that let them are sized from a
ten-second bottleneck window, so a pool still above the floor a whole window
later is a path whose erasure changed, and the lane re-bases its floor on the
pool's current erasure and earns compensation against it as any lane does.
The window must be watched, not merely elapse: a gap of more than a second
between the lane's own events restarts it.

**Simulated.** `internal/congestion/joinburst_sim_test.go` runs the real
`ErasureSender`, its own pacer, and the real `PathModel` against a
packet-level bottleneck (finite FIFO buffer, seeded erasure after the queue,
fixed propagation) on simulated time (`pathmodel.NewPathModelClocked`), so a
run is identical on every machine. Four lanes join 20 ms apart beside one
pool member delivering 60 kB/s. Goodput is a fraction of what the path
delivers after erasure; caused loss is the fraction of sent packets the
buffer or policer dropped. #146 is its own `erasure.go` under the same test.
8 s runs, except where noted.

| Path | #146 goodput | This goodput | #146 caused loss | This caused loss | #146 queue p90 | This queue p90 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 18 MB/s, 170 ms, 15%, 12 ms buffer | 0.10 | 0.88 | 0 | 0.15 | 0.1 ms | 11.8 ms |
| same, 12 s | 0.16 | 0.87 | 0 | 0.12 | 0.2 ms | 11.8 ms |
| same, 20 s (after 10 s: caused loss 0.011) | | 0.91 | | 0.07 | | 11.7 ms |
| same, clean (0.1%) | 0.06 | 0.88 | 0 | 0.10 | 0.1 ms | 11.8 ms |
| same, 85 ms buffer | 0.10 | 0.88 | 0 | 0.06 | 0.1 ms | 85 ms |
| 2.6 MB/s, 200 ms, 40%, 300 ms buffer | 0.69 | 0.92 | 0 | 0 | 202 ms | 259 ms |
| same, 12 s | 0.69 | 0.84 | 0 | 0 | 288 ms | 260 ms |
| 2 MB/s policer, 300 ms, 8 ms bucket | 0.30 | 0.89 | 0 | 0.14 | 0 | 0 |
| same, plus 42% erasure | 0.61 | 0.90 | 0.09 | 0.19 | 0 | 0 |
| fast path, four lanes into an empty pool | 0.66 | 0.66 | 0.55 | 0.55 | 11.8 ms | 11.8 ms |

The last row is #146's own regime at capacity, which this change does not
enter: no seed, so no ramp. A late joiner beside a lane already filling the
fast path (85 ms buffer) ends its ramp 0.49 s after joining, and caused loss
over the rest of the run is under 0.001; on #146 the first lane never filled the
path, so the comparison does not arise.

The high-erasure deep-buffer rows are the path #146 improved live (queue p90
992 ms to 168 ms). Over 8 s #146's queue reads lower only because it has not
yet filled the path; over 12 s both hold about the delay bound's queue, and
the ramp's is the lower.

**What it does not fix.** The pooled bottleneck is a sum of per-lane windowed
maxima, which overstates the aggregate when lanes peak at different times, as
they do when they end their ramps a round or two apart. That is most of the
0.12-0.15 caused loss left on the shallow buffer, concentrated in the seconds
after the ramps end and gone once the windows turn over; the same burst into an empty
pool, untouched here, loses 0.55. And the even split itself: one bulk lane
beside one idle member is held to 5/8 of what it delivers, which this change
only postpones by ramping first (simulated on the high-erasure path, 0.08 of
capacity on #146 and 0.62 here). Both are follow-ups.

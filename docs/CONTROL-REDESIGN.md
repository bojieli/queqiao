### What is left, and what it is not

7.3x is not braked, it is less unbraked. The remaining overdrive is a bandwidth
estimate that reads **2.0x the path in the full stack against 1.01x when the
estimator is driven in isolation**, and the pacing gain applied on top of it.

Three explanations for that gap have been measured and ruled out. They are
recorded because each was plausible enough to have been implemented on
reasoning alone.

**It is not the metric folding across lanes.** The measurement is taken with
one lane (`queqiao_quic_lanes` reads 1), so the reported figure is a single
sender's own estimate and not a maximum over several.

**It is not the startup gain.** The controller is in ProbeBW, not startup
(`queqiao_quic_controller_mode` reads 3), so it is not pacing at the 2.77
startup gain and has already decided delivery stopped growing.

**It is not sender burstiness.** A paced sender emits flights, and a flight
partly passing a token bucket leaves the survivors bunched -- a short window
carrying many bytes, which is exactly the shape a maximum filter would
overstate. Driven with flights from one packet to sixty-four, the estimate stays
within two per cent of the path.
`TestBurstinessDoesNotInflateTheEstimate` keeps that negative result.

So the difference is something the harness does not model: QUIC's own
acknowledgement timing, the coded datagram substrate, the emulator policing both
directions, or reverse-path loss on the acknowledgements themselves. **The next
step is the sample trace wired into the running stack rather than into a
harness** -- the hook exists and is nil in production; what it lacks is a way to
install it from an end-to-end test. A harness cannot answer this one, which is
the fourth thing this exercise has established the hard way.

# Control redesign: delay-bounded goodput

> [!IMPORTANT]
> **Status:** Implemented except for the receiver report. The classifier is
> gone, the estimate can fall, the code is sized from the measurement, and the
> delay bound is the brake. Nothing here
> is validated on a real path yet -- see [Falsification](#falsification).
>
> **Wire impact:** None for the controller and coding changes. The receiver
> report in [Closing the residual loop](#closing-the-residual-loop) is additive
> and forward-compatible.

This document replaces the erasure-floor classifier and the bandwidth max
filter with a single objective: **deliver as many bytes as the path will carry
without letting the queue grow past a delay bound.** It records the incident
that motivated it, the two defects it removes, the problems it explicitly does
not solve, and the cases that would falsify it.

It does not supersede [the current design](DESIGN.md) until the falsification
cases in this document pass on a real path. Until then, treat DESIGN.md as the
description of what ships and this as the description of where it is going.

## The incident

A live China-US path degraded over roughly eight hours. The evidence that
matters, all of it from per-flow completion records rather than from the
aggregate counters, which were not trustworthy (see
[Measurement plane](#measurement-plane-and-inference-plane)):

| Direction | Measured at | Erasure | Residual after FEC |
| --- | --- | --- | --- |
| Upstream, client to gateway | gateway decoder | 3.8% p50 | 0.00% p50 |
| Downstream, gateway to client | client decoder | 19.9% p50, 60.1% peak | 11.0% p50, 56.0% peak |

Of 14,792 client flows that saw more than 5% downstream erasure, **100%** ran a
code sized for less than half the erasure actually present, and **97%** ran no
code at all. In the worst record the local estimator's own floor read 77.0%
while the plan it produced was sized for 0.00%.

The transport was neither faster nor slower than direct TCP over the same
segment in the same window: 2.10–2.20 MB/s tunnelled against 2.00–2.25 MB/s
direct, on a path whose round trip is 194–430 ms. It delivered none of the
benefit it exists to provide while re-issuing 11% of the downstream payload.

## Two latched estimators

The incident is not one bug. It is the same mistake in two places, pointing in
opposite directions. Both estimators assume the path has a true value worth
remembering, and both are correct only if that assumption holds.

### The erasure floor ratchets down

`ErasureSender.establishedErasureFloor` keeps a lower envelope for the lifetime
of the connection, and says why:

> The floor is a lower envelope for the lifetime of this connection: allowing a
> completed but congested measurement to increase it creates positive feedback,
> because both the pacer and FEC then add traffic to an already overloaded
> path.

The reasoning is sound given the code underneath it. `coded.Path` has no byte
budget: repair symbols are pure addition on top of whatever the controller
already allows. Raising the erasure estimate therefore genuinely does add load,
and the project has been bitten by that loop before — the repair ratio climbing
from 11% to 27% to 63% of sent bytes.

So the ratchet is a correct defence against a real hazard, and no patch to the
gate, the envelope, or the burst test can be applied without re-opening it.
**The hazard has to go first.** The only escape today is lane death, which
creates a fresh estimator; on flows that live for hours, the floor established
during a clean window survives into a degraded one.

A second defect compounds it. `coded.Path.channel()` collapses the pooled model
to a single scalar and fabricates a snapshot from it:

```go
return lossmodel.Snapshot{
    Loss: floor, Floor: floor, Recent: floor,
    BurstFactor: 1, ArrivalAfterLoss: 1 - floor,
}
```

`fec.Choose` has a designed fallback — `if loss <= 0 { loss = s.Loss }` — and it
is dead code, because `Loss` *is* `Floor`. When the floor is zero both are zero.
`BurstFactor: 1` separately asserts independence, so parity is under-sized
whenever loss clusters.

### The bandwidth filter ratchets up

> [!NOTE]
> Fixed. A sample now expires on rounds or on wall time, whichever comes first,
> with the wall-clock window derived from the measured round trip. An
> application-limited sample may also replace an estimate that has already
> expired, without which an always-application-limited connection would have
> emptied its own model. What follows is what shipped and what it did.


`tuicMinMax` is a three-sample max filter with a **ten-round** window, and
`updateMax` collapses all three samples to the newest whenever it is the
largest, so one high sample latches the filter. App-limited samples are allowed
through in the raising direction, which is textbook BBR.

The window is counted in packet-timed rounds, not wall time, and the observed
connections were 99.98% app-limited, so rounds barely advance:

| Endpoint | Round | Uptime | Rounds/hour | Ten-round window |
| --- | --- | --- | --- | --- |
| Gateway | 220 | 24.1 h | 9.1 | **≈ 66 min** |
| Client | 1401 | 25.2 h | 55.7 | ≈ 11 min |

One burst through a token bucket therefore latches the bandwidth estimate for
the better part of an hour. The gateway held `max_bandwidth` at 519 Mbit/s
against a measured sustained 17.6 Mbit/s: a 29× overestimate, and the source of
the 1.6 Gbit/s pacing rate and 15–28 MB congestion window observed alongside it.

### Why a max filter is the wrong shape

A shaped path does not have a bandwidth. It has a rate and a burst depth, and
they are different numbers. A short probe drains the bucket and measures the
line rate; sustained load measures the shaping rate. Any max filter
structurally measures the first and reports it as the second. Shortening the
window or adding decay is a patch on that premise rather than a correction of
it.

The same is true of an operator-set ceiling, which this project has already
rejected on its own terms: the point of the controller is to *"reach the same
place without being told."* A configured rate cannot track an ISP that
re-shapes, and there may be no stable rate to configure.

## The objective

One function, measured end to end:

```
maximise   G(B, r) = B · k / (n + r/(1−r) · reissue)
subject to RTT ≤ min_rtt + delay_budget
```

`B` is the total byte rate on the wire, `k/n` is the fraction of a block
carrying source rather than repair symbols, `r` is the residual the chosen rate
leaves, and `reissue` is what an unrepairable block costs before its data
arrives, in symbol-times. There is no bandwidth variable, so there is nothing
to latch and nothing to configure.

The `reissue` term is not decoration, and an earlier draft of this document
omitted it. Without it the objective is delivered goodput per byte on the wire,
and **that objective never buys parity at all**: sending `k` of `n` shards as
data always carries less data per byte than sending all `n`, and a
retransmission recovers the difference eventually. Parity only ever buys the
*eventually*. An objective with no time in it cannot express that, so it
declines to code at every erasure rate, which is the incident with the
arithmetic reversed.

The residual is charged `r/(1−r)` round trips rather than one, because a
re-issue crosses the same channel and can be erased in its turn. At a small
residual the difference is nothing. At a large one it is the whole answer:
charging a single round trip makes an uncoded block on a 72% erasure channel
look cheaper than any code, because it quietly assumes the one retransmission
gets through.

Three things follow that are worth stating separately, because each removes a
knob rather than adding one.

**The residual target disappears.** `fec.Choose` currently searches for the
highest rate meeting a `TargetResidual` that the sender can never observe —
open-loop control against an unmeasurable setpoint. Under a fixed byte budget
the objective is self-defining: at `p → 0` the maximum lands at `r = 1` on its
own, so `minCodedLoss` disappears with it.

**The erasure/congestion classifier stops being safety-critical.** Today
misclassification is catastrophic in one direction: no parity into a 50%
erasure channel. Under a fixed budget, getting it wrong costs some goodput and
nothing else.

**The delay bound is the brake that replaces loss-based backoff.** A pure
goodput objective would happily fill a queue.

An earlier draft had it doing double duty, protecting interactive latency as
well, and that was wrong in a way worth recording: it is the same mistake as
the erasure floor serving both the pacer and the code. Interactive latency is
an absolute quantity and is already protected by the aggregate budget's reserve
and the lanes' priority queues. Once that job belongs elsewhere the bound has
one job, and for that job the right frame is relative.

So the bound is `RTT <= 2 x min_rtt`, which is the same statement as "the queue
may hold at most one bandwidth-delay product" and the same rule as the
controller's existing 2.0 congestion-window gain, in the time domain rather
than the window domain. It is a ratio and not a duration on purpose: a duration
would have to be chosen, and the choice would be a latency policy smuggled into
a congestion controller.

It is a measurement where the window gain is an estimate. The gain bounds the
window using a bandwidth the sender believes in; on an erasure path that window
is then divided by the arrival rate so a full one arrives, which means what is
*sent* can be several times the bottleneck's worth. The queue is downstream of
that division and does not care why the bytes were sent, so the bound is
applied after the compensation.

### Why the classifier is redundant

The ErasureSender documentation already contains the argument, without drawing
the conclusion:

> in startup S grows by the gain each round until delivery stops growing, which
> happens exactly when S reaches the bottleneck.

That is the whole discriminator. **Erasure scales delivery down proportionally
at every rate and cannot produce a knee; congestion produces a knee.** The
delivery-versus-rate curve separates them empirically, per path, with no
statistics. A policer at capacity — the case the classifier was built for —
still produces a knee: send more, deliver the same.

The memorylessness test and the burst-factor gate are a second, weaker answer
to a question the probing loop already answers, and unlike the probing loop
they fail precisely when loss is worst. With loss removed as a congestion
input, `ErasureSender.congestive` has no consumer.

What survives is not a classifier but a measurement. Pacing must still be
divided by the arrival rate, or the estimate chases itself down — sending `S`
delivers `S(1−p)`, which becomes the next estimate, which is paced as `S(1−p)`.
Arrival rate is `arrived/sent`. **You do not need to know why packets failed to
arrive in order to know what fraction did.** Deriving it from the classified
floor is what made it inherit the ratchet.

### Decoupling the optimiser

Joint optimisation over `(B, r)` is not defensible on this path, and this
document should not pretend otherwise. `lossmodel.DefaultRoundSamples` records
the noise floor: at 42% loss, 2,000 packets put the standard error near one
percentage point, and the filter wants eight such rounds. At the 17.6 Mbit/s
measured, 1,200-byte packets give roughly 1,830 packets per second — about a
second per usable loss estimate, nine seconds per filter window — while the
path moved 5% → 49% → 20% inside one hour. That budget does not support a
two-dimensional gradient on a noisy, non-stationary objective.

So the knobs are decoupled:

- **`B` keeps BBR's structured probing.** The state machine was never the
  defect; the latched filter feeding it was. Replace the filter with an
  estimator that can fall, and add the delay bound as the brake.
- **`r` is a one-dimensional climb inside `B`.** Cheap, fast, and safe by
  construction, because under a fixed budget it cannot add load.

This keeps the well-understood mechanism and replaces only the part that was
demonstrably wrong.

### Rate-preserving parity

The invariant that makes all of the above safe:

```
B = S + R
```

Congestion control owns how much goes on the wire. Coding owns how that budget
is spent. Raising the erasure estimate then changes the mix -- more parity,
less payload -- and never the total.

**This mechanism already existed, and an earlier draft of this document was
wrong about it.** The draft said parity was additive and that an aggregate
enforcement point had to be built, pointing at `internal/limiter` and the
`aggregate_bytes_per_second` flag that reads zero on the live gateway. That is
an optional application-level pacer and it is not the budget. The budget is the
congestion window:

- `coded.QUICCarrier` runs the coded path over QUIC datagrams, and *"the
  congestion controller, the pacer and the loss detector on the connection all
  still apply"*. A repair symbol crosses the same window as a source symbol.
- `ErasureSender.bandwidth` caps that window at this lane's share of the
  endpoint pair's measured bottleneck, *"so the aggregate on the wire is what a
  single sender would have put there"*. Lanes cannot compound.

What was actually broken is that the window was not fixed in practice. The
bandwidth estimate behind the cap could be latched at a burst rate for the
better part of an hour, so the share was computed from a figure the path had
never sustained, and a cap computed from a fantasy never binds. That is a
defect in the estimate, not a missing mechanism, and it is fixed in the filter.

`TestParityCostsACodeRateAndNotAByteRate` holds the invariant directly: the
same payload through a carrier with a fixed byte allowance puts 408,955 bytes
on the wire uncoded and 409,573 coded for 19.9% erasure, against a 409,600
allowance. The parity was spent out of the window rather than added to it.

The honest cost stands: under heavy erasure, goodput visibly drops, because
budget is openly spent on parity. That cost is already being paid as
retransmissions at one round trip each, invisibly. On a 194-430 ms path that is
the trade this project exists to make.

## Closing the residual loop

The planner chooses a code rate and never learns whether it worked. During the
incident the client knew its residual was 11.0%; the gateway could not find
out. No estimator improvement fixes this: a sender can measure erasure from its
own acknowledgements, but it cannot measure residual after its own FEC, which
is the only number that says whether the rate was right.

The coded datagram layer already ignores unknown kinds:

```go
default:
    return nil
```

So a `kindReport` datagram carrying `{erasure, residual, mean burst}` over a
recent window is forward-compatible. Old peers drop it and behave exactly as
today; new peers close the loop. No version bump and no change to existing
conformance vectors.

`coded.Path.channel()` states that the coded layer *"never infers its outbound
direction from packets received in the reverse direction."* That correctly
rejects **inferring** outbound erasure from inbound loss. A peer **reporting**
what it measured on your outbound direction is not inference; it is a direct
measurement relayed, and the reasoning does not extend to it.

## What removing the classifier actually cost

The classifier was protecting one thing that the delay bound does not, and the
existing test suite caught it rather than a review.

A first flight can overrun a clean path's queue before the controller has found
its bottleneck. Those drops arrive in runs and are congestion, not erasure.
With the compensation riding on the raw measurement, the sender compensates for
its own queue drops and sends twice as fast into a queue that is already
overflowing — the exact positive feedback the classifier existed to prevent.

The delay bound would stop it, except that at that moment there is no minimum
round trip to bound against, so the brake is inert. The two safeguards did not
overlap the way the sequencing assumed.

The resolution is a coupling rather than a restored classifier: **compensation
waits for a measured minimum round trip**, which is exactly when the brake can
act. It makes no judgement about what kind of loss it is seeing. Before that
point the sender is plain BBR that ignores loss — it neither compensates nor
collapses.

`TestClusteredStartupLossDoesNotBecomeAnErasureFloor` predates this work and now
holds the new arrangement unchanged, which is the strongest evidence available
that the property survived the mechanism.

## A limit found while trying to validate it

An attempt to check falsification case 1 end to end did not work, and why it
did not is worth recording.

**Expiry is driven by arriving samples, not by reading the estimate.** A sample
that has outlived its window is replaced by the next one; `get` returns what is
stored. A lane that carried a burst and then went silent keeps its figure until
it sends again. For the lane that is harmless — its estimate only governs what
it puts on the wire, and it is not putting anything there. For a trace it is
not: `queqiao_quic_controller_max_bandwidth_bytes_per_second` folds the maximum
across lanes, so **one idle lane can report a peak the path has not sustained
for minutes.** Reading that metric as the path's bandwidth is the same mistake
the filter was fixed for, one level up.

The attempted test also showed that a shaped path is harder to hold in an
emulator than it looks: a load modest enough to be "sustained" fits inside a
bucket that refills between writes, so the path never shapes it and the
estimate rises rather than settling. Expressing case 1 end to end needs a
runtime change to the shaping *rate*, in the way `SetLossRate` changes erasure.
`PolicerBurstBytes` is in place; the rate knob is not.

The test was removed rather than kept, because it passed — on a comparison wide
enough that it would have passed with the filter disabled too.

## Measurement plane and inference plane

Every defect in the incident, including the ones outside the controller, is the
same mistake: **conclusions were stored, shared, and aggregated as if they were
measurements.**

- The erasure floor stores a conclusion and ratchets it.
- `channel()` collapses a measurement into a conclusion and back.
- `internal/metrics` sums per-flow cumulative counters over *currently live*
  flows and exports the result as a counter. The values fall when flows expire,
  so anything that differences them — Prometheus `rate()`, and
  `tools/visualizer/parser.js` — reads noise. A 60-second window produced
  "45.03% packet loss" against a real downstream rate of ~35 kbit/s.
- `queqiao_quic_packets_lost` is a conclusion with no measurement behind it.
  `connStats.PacketsLost` is incremented only in quic-go's `cubic_sender.go`,
  which Queqiao replaces, so it is structurally pinned at zero. The visualiser
  computes `packet_loss_percent` from exactly that field.

The rule this design adopts:

- Measurements are direction-tagged, endpoint-tagged, monotonic, and never
  destroyed by aggregation.
- Inference is per-consumer, explicit about its bias, and reads measurements —
  never another consumer's conclusion.

Direction in particular must be enforced by the type system rather than by
naming convention. The rename to `fec_receive_*` was a convention fix, and two
separate reviewers still misread the result. An erasure figure without a
direction and a measuring endpoint should be treated as unread.

## What this does not solve

Stated plainly, because the design is easy to over-credit.

**Lane idle timeouts, which caused 79% of observed flow failures.**
`MaxIdleTimeout: 15s` and `KeepAlivePeriod: 5s` are `quic.Config` fields, so
keepalives are QUIC PING frames in QUIC packets. The coded substrate sits
*above* QUIC and carries queqiao frames as datagrams, so **FEC never protects
QUIC's own control traffic.** At 20% downstream erasure, three consecutive PING
losses is 0.79% per opportunity regardless of how good the coding becomes, and
a p90 flow gets roughly 22 opportunities. The 5,684 `unknown_session` lane
rejoins are the downstream symptom. This needs its own fix and is not addressed
here.

**Metrics aggregation, the dead loss counter, and direction typing.** Three
independent fixes, described above but not caused by the controller.

**Fairness.** Not claimed, by decision. Queqiao is deliberately more aggressive
than a loss-responsive flow sharing the same bottleneck. What the objective
does provide is a self-limiting property: the climb stops when delivered
goodput stops improving, which is the bottleneck. The claim is *"it takes what
the path will deliver and stops"* — which is testable, and is tested below —
rather than any fairness guarantee.

## Falsification

These are the point of the document, not an appendix. The coding half is a
root-cause fix supported by the incident evidence. The control half is a
well-motivated redesign whose central mechanism has never run on this path, and
elegance is not a reason to trust it.

Each case belongs in `internal/pathsim` and must fail against the current
implementation before the change lands.

| # | Case | Passes if |
| --- | --- | --- |
| 1 | Token bucket, deep burst allowance | `B` converges on the sustained shaping rate, not the bucket drain rate. **Filter-level test passing.** `internal/pathsim` now has the burst allowance (`PolicerBurstBytes`), but the end-to-end version also needs a runtime *rate* change, which it does not have — see [A limit found while trying to validate it](#a-limit-found-while-trying-to-validate-it) |
| 2 | Step change 5% → 50% erasure mid-flow | The erasure estimate rises within one filter window; the code rate follows. **Passing end to end** — `TestTheCodeFollowsAChannelThatGetsWorseMidFlow` steps an emulated path from 2% to 45% downstream under a live flow, and the measurement the code is sized from moves from 0.034 to 0.224 while the controller's floor stays at 0.031 |
| 3 | Throttle that tightens under load | `B` walks down and settles; no sustained oscillation. **Filter-level test passing** |
| 4 | Shallow-buffered dropper, no queue | **Fails.** See [Case 4 fails](#case-4-fails). Held by `TestCase4APolicedPathIsStillUnbraked` as a characterization test |
| 5 | Clean path, no erasure | `r = 1`; no parity is sent, without a `minCodedLoss` constant |
| 6 | Self-limiting claim | The climb stops; `B` does not grow without bound |
| 7 | Bulk flow alongside interactive | Interactive latency stays inside the delay bound. **Partly covered**: `TestABulkTransferIsHeldBackByADeepQueue` shows the brake engaging on a deeply buffered bottleneck at 415 ms of queue against a 313 ms minimum, removing 24.7% of the rate. The interactive half is not covered |

**Case 4 is the one expected to fail.** A dropper that erases at capacity
without building a queue gives the delay bound no signal, and with loss removed
as a congestion input there is nothing else. If it fails, the honest outcome is
to document the path class as unbraked rather than to reintroduce a classifier.

## Case 4 fails

A policer drops what it cannot pass and holds nothing, so overload produces loss
and no delay. Loss is no longer a congestion signal and there is no queue for
the delay bound to measure, so neither brake can act. This was predicted to
fail. It fails by more than was predicted.

Against an emulated policer shaped to 250 KB/s:

| | |
| --- | --- |
| Peak pacing rate | **10,506,238 B/s — 42x the path** |
| Bandwidth estimate | 665,178 B/s — 2.7x the path |
| Worst queue | 2 ms |
| Strongest brake | **0.0000** |
| Sustained loss | 72.5% |

**This is not a hypothetical path class.** `internal/pathsim` records that the
live path this project targets is a policer: *"at twice the bottleneck rate it
shows arrival runs averaging 2.3 packets and loss runs averaging 5.7 ... a
limiter which passes everything for a while and then drops everything for a
while."*

### Where the overdrive comes from

An earlier reading blamed the bandwidth estimate, on the grounds that a token
bucket passes a burst at line rate and a maximum filter reports that burst as
the path's bandwidth. **Measurement says otherwise.** Driven directly against a
simulated policer -- 9 MB/s offered into a 250 KB/s bucket, every event in
timestamp order -- the estimator reports **252,521 B/s, 1.01x the path**, with
the median sample at 0.99x and acknowledgement intervals at about one round
trip. The estimator is not the fault.

`TestWhatTheSamplerSeesOnAPolicedPath` is that measurement, kept so that the
next reading of this starts from evidence rather than from the same guess. It
was reached only after two attempts on the estimator that changed nothing --
bounding the filter's memory in wall time, which is worth having and is
irrelevant here because a policer's bursts recur every refill period, and
averaging the sample within a round, which measured no better -- and one
synthetic harness whose own ack pacing was wrong and which reported 4,000 B/s
on a 250 KB/s path.

The fault is the erasure compensation. It is a bet that losses are independent
of the sending rate, so that sending 1/arrival times as much delivers a full
window. On a policer the losses are *caused* by the sending rate, so the bet
feeds itself: send more, get dropped more, measure a lower arrival rate, ask to
send more again.

Two things kept the first attempt at bounding that bet from working, and both
were mistakes in the bound rather than in the diagnosis.

Nothing, as it turns out, needed replacing. Two attempts were made on the
estimator before it was measured, and both are recorded below because they are
still true about the estimator and because they are how the diagnosis stayed
wrong for as long as it did.

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

### What is left

7.3x is not braked, it is less unbraked. The remaining terms are the startup
gain of 2.77, which is BBR probing and should end when delivery stops growing,
and a bandwidth estimate that reads 2.0x the path in the full stack against
1.01x when the estimator is driven in isolation. **That gap is the next thing to
measure rather than the next thing to guess**: the two differ by everything the
real stack adds, and which part matters is not established.

Four attempts have now been made on this. The first three reasoned about what
the code should be doing and were each refuted by measurement, the third by a
measurement that was itself broken. The fourth started by measuring and found
the fault somewhere none of the three had looked.
Until then, **a policed path is unbraked**, and this design should not be
deployed on one.

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

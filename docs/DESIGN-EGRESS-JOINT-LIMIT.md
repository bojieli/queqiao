# Egress joint limit and rate-correlated erasure

Status: **design, draft for review.** It extends `docs/DESIGN-PATH-AUTHORITY.md` (§4 the authority, §11.3 the DC gate) and `docs/DESIGN-DC-PROFILE.md`. Scope is `--congestion-scope=path`; `--congestion-scope=connection` (the default) is unchanged. No wire change.

## 1. The problem

A gateway host serves several DC leaves from one network port. Each leaf has its own path authority (`PathController`, one per principal, local uplink and remote host, and sending direction). Nothing joins them.

Live DC diagnostic (generic labels):

| Observation | Value |
| --- | --- |
| Gateway transmit, path authority / legacy | 794-969 / 650-906 Mbit/s |
| Loaded-leaf goodput | 522-542 Mbit/s, so the wire carries 1.46-1.83x its goodput |
| Where packets are lost | in the network just past the gateway's port; gateway qdisc, NIC, socket buffers and leaf hosts show zero drops |
| Erasure the senders estimate on loaded lanes | p50 4.6 %, p90 26.8 %, and the law raises its rate by it |
| Control leaf (300 KB every 0.5 s) p50 | 0.131 s against legacy's 0.113 s: 1.16x, over the gate's 1.10x |

The chain of causes:

1. The shared point past the port drops packets with **no standing queue in the sender's round trip**. `inflightBound` (#249) calls loss without a standing queue erasure and learns its `base` from exactly those rounds, so `base` rises to the overflow rate. (`inflightBound.closeRound`: `!queued` teaches the base.)
2. `publishMeasurement` then makes the law compensate, `arrival = 1 - min(erasure, base)`, and sends `1/arrival` times as much.
3. The only check on the bet is `compensationFor`'s "did delivery rise". Where leaves share the loss point, it does: one leaf's extra packets take capacity from the others, so each leaf's delivery rises when it sends more. The bet is confirmed by a zero-sum game.
4. Each leaf learns its share of the shared drop as channel erasure and covers it. N independent compensators overshoot together. The loss rises, and so does the cost of the control leaf's flows: a lost packet in a 300 KB fetch is an extra round trip.

Two defects, two remedies, in this order of cause:

- **A (per controller).** The law cannot tell loss that rises with its own rate from loss that does not. It must, and compensate only the second kind.
- **B (per gateway egress).** Even a controller that classifies correctly cannot see its siblings. Independent leaves need a joint ceiling and a fair division of it.

A is necessary because B's ceiling is not always known and learning needs A's classification. B is necessary because A is a statement about one sender: the aggregate of N senders each correct about itself can still overload a shared port, and a port-level loss does not distinguish its senders.

## 2. Part A: correlation-aware erasure

### 2.1 Principle

Random channel loss is independent of the sending rate: the erasure sender's whole premise (`erasure.go`). Loss at a limiter or an overflowing buffer is a function of the offered rate and rises with it. The sender controls its rate and already varies it: ProbeBW cycles its pacing gain `[1.25, 0.75, 1 x6]` (`tuicPacingGains`). That is a controlled experiment on a free variable. Compare loss at the high rate with loss at the lower ones.

### 2.2 Evidence unit: the epoch

An epoch is a maximal run of consecutive *sample packets* of the path's send order that share a rate class, ending at a class change or after `max(minRTT, 5 ms)`. The class is read when the packet is sent:

| Class | Condition at send |
| --- | --- |
| `up` | mode ProbeBW and pacing gain > 1 |
| `cruise` | mode ProbeBW and gain = 1 |
| `down` | mode ProbeBW and gain < 1 |
| none | startup, drain, ProbeRTT: not used, as their rate moves for other reasons |

Each fate record carries `(epoch id, class)`. The estimator already observes fates **in send order** (`PathController.advance`), so an epoch is complete exactly when the first record of a later epoch is observed. No lag constant is needed, and a loss declared late still counts for the epoch that sent it. An epoch with fewer than 20 decided packets is discarded.

An epoch is also discarded when it is a round with no acknowledgements at all (every packet lost: a path that is away loses every rate class alike), and when it overlaps a detected outage (#264): on judging a silence an outage, `noteAnswered` drops every epoch sent from two smoothed round trips before the silence's first unanswered packet, so an outage that begins during an up epoch cannot flag a rate correlation. The statistic keeps the last 96 closed epochs, with each epoch's loss fraction `l_e = lost / decided`.

### 2.3 Statistic

Let `U` be the `up` epochs and `B` the `cruise` and `down` epochs, with means `u`, `b`, per-epoch sample variances `s²`.

```
delta = mean(U) - mean(B)
se    = sqrt( var(U)/|U| + var(B)/|B| )          # per-epoch variance, so burstiness and
                                                  # siblings' probes enter as noise
var   = max(var_sample, p(1-p)/mean_n)            # floored at binomial, so 6 identical epochs do not give se = 0
```

The loss is **rate-correlated** (`flagged`) when all hold: `|U| >= 6`, `|B| >= 12`, `delta >= 0.02` and `delta >= (3 + 6/|U|) se` (the allowance covers the heavy tail of a variance estimated from few up epochs: 4 se at six, 3.5 at twelve). It stays flagged (a Schmitt trigger) until `delta < 0.01` or `delta < 1 se`, so the verdict does not flap as compensation is withdrawn and the gap closes.

Why this and not a regression on realised rate: the gain is the only rate variation the sender produces deliberately and independently of load, so the contrast is a designed experiment. It needs no model of the limiter, and it is a statement about *this sender's* rate, not the aggregate's. A siblings' probe adds variance to every epoch alike, because siblings' phases are not aligned with ours.

Power: a gain cycle is eight phases of one round trip each and has one `up` phase. On a 53 ms path six cycles take about 2.5 s; on a 1 ms DC round trip, under 100 ms. Against a limiter, `delta` is about `0.2 C/S` for a sender at rate `S` into capacity `C`: 0.13 at 1.5 times capacity, far above 3 se for epochs of a few hundred packets.

### 2.4 Effect

While `flagged`, the compensation takes as its erasure

```
floor = min( mean(cruise), mean(down) )   over classes with >= 6 epochs
erasure_used = min(erasure, base, floor)
```

The floor is the loss at the lowest rates the sender offers: the part that does not respond to the sender's rate. Against a limiter that is also a contraction, not just a cap. Compensating `floor` instead of the actual loss makes the sending rate `S' = D / (1 - floor)` while delivery is `D = S (1 - p_cruise)`, so `S' < S` whenever `floor < p_cruise`, and the rate falls until the loss in the lower classes has gone. Unflagged, the law is as it is today.

Properties:

- **A false flag is nearly harmless.** Random erasure `e` gives `floor ~ e` (the mean of the lower classes), so withholding the excess over `floor` withholds about nothing. A false flag costs only the noise of one mean, not the compensation.
- **A false negative is today's behaviour.** Too few epochs, an application-limited sender (no real rate change on the up phase, so no contrast), or a path that is never in ProbeBW all leave the existing compensation. Part B is what bounds the aggregate in that case.
- **Real random loss is kept.** Lossy cross-border paths (the 42 % channel, the slow-lossy case) have `delta ~ 0`. The existing `TestBlackhole*`, slow-lossy, policed, M5 and W10 tests are regression gates, and unit tests (section 5) assert compensation is unchanged under independent and bursty loss.
- **Where it acts.** Only on the path authority's law (`e.authority`), in `publishMeasurement`, after the pooled erasure and the `inflightBound` base. It lowers; it never raises.

## 3. Part B: the gateway's joint egress limit

### 3.1 Membership and scope

The *egress group* of a gateway is the set of its path controllers that send out one network interface. The group key is the interface that owns the local source address of the path (`resolvePath` already yields the local address; it is mapped to an interface by `net.Interfaces`, falling back to the address itself). Loopback addresses therefore form one group, as one port would.

- Only controllers whose principal key starts with `principal:` (gateway-side) join, and only when `--congestion-scope=path`. A client's controllers never join; neither do provisional controllers.
- A controller joins when it is attached and leaves when the registry sweeps it (`pathAuthorities.controllerIn`), so a path that has gone quiet stops counting.
- A member is **active** when it sent at least 1 Mbit/s over the last window. Only active members share the ceiling.

### 3.2 The ceiling `K` (wire bytes per second out of the interface)

`K = min` of the sources that are known; none known means no ceiling.

1. **Configured.** `--egress-capacity=<rate>` (`1g`, `940m`, `500mbit`; bits per second, exact). Enforced whenever any member is active. `off` disables Part B. The default is `auto`.
2. **Link speed with a margin.** Under `auto`, `K_link = 0.95 x speed` where speed is `/sys/class/net/<interface>/speed` on Linux. The file is absent or `-1` for virtual, wireless and macOS interfaces. Then there is **no link bound**: this is a normal case (hypervisor NICs), and the ceiling is learned only.
3. **Learned from the aggregate.** The group takes a window `W = 100 ms` (events carry their own times, so a simulation drives it). From the members' sample-packet counters it forms the aggregate sent rate `A`, the decided count `N`, the lost count `L`, the loss `lambda = L/N`, and the weighted channel floor `lambda0 = sum(N_i x floor_i) / N`, where `floor_i` is each member's erasure after Part A. The group's excess loss is `lambda - lambda0`.

   **A learned `K` can only fall through the learn-down rule.** The conditions below are the only way it falls, and they all require loss that a member has found to rise with its own rate. A gateway whose members see random loss alone can therefore never be throttled: their `delta` is about zero, none is flagged, and `K` stays what it was (none, or the link's).

   **Learn down** when all hold: at least 2 members are active, at least one active member is `flagged` by Part A, `N >= 500`, and `lambda - lambda0 > 0.02 + 3 sqrt(lambda0 (1 - lambda0)/N)`. Then, with `q = (lambda - lambda0)/(1 - lambda0)` the fraction the egress dropped of what reached it, the aggregate that fitted is `A (1 - q)`:

   ```
   K <- min(K, A (1 - q))              hold 2 s (+ up to 1 s jitter), as inflightBound
   ```

   Requiring a flag is what keeps this from throttling a gateway whose members each see random loss: the group never learns from loss alone. It iterates. Once the members are throttled their lower-class floors fall, `lambda0` falls and the next window's `q` is larger, so `K` converges on the real capacity from above in a few windows.

   **Probe up** while `K` binds (some member is capped by its share) and the last window had no excess loss: `K <- 1.05 K` once per second after the hold, never above the link bound. A probe that brings excess loss is cut back by the rule above to what fitted. This is `inflight_hi`'s growth and BBR's ProbeBW applied to the aggregate; random loss does not disturb it, because the probe's own experiment (rate up, loss unchanged) leaves `q = 0`.

`K` persists for the life of the process, as the capacity of the host's egress is a property of the host.

### 3.3 Fair division

Every window the group computes each active member's **demand** `d_i` in wire bytes per second:

- `want_i`: the law's pacing rate before the share is applied (`delayBounded(delivered/arrival)`, with its gain), published as an atomic.
- If the member was application-limited for more than a quarter of the window (the unused-opportunity time `PathController.accrue` already measures), `d_i = min(want_i, 1.25 m_i + 2 packets per round trip)`, where `m_i` is its measured sent rate. A control leaf that fetches 300 KB every half second demands its use, not its line rate, and so does not reserve a share it would not use.
- Otherwise `d_i = want_i`.

Max-min fairness over `0.97 K` (3 % headroom for bursts of members below their share): water-fill, giving each member `min(d_i, L)` where `L` is the level at which `sum_i min(d_i, L) = 0.97 K`. A member with `d_i <= L` is **uncapped** (share 0): it keeps its own law. Only a member with `d_i > L` is capped at `L`.

- **No effect under the ceiling.** If `sum d_i <= 0.97 K`, every member is uncapped, and `bandwidth()` and the window are exactly as without Part B.
- **Equal weights.** One authority is one principal's path; there is no weighting.
- **A joiner** in STARTUP demands a large `want` and is capped at the water level, so it takes its fair share at once, not the full port, and the loaded leaves release it by their next window.

### 3.4 Enforcement

A member reads an atomic `share` (zero is uncapped) in two places in `ErasureSender` (the erasure sender the controller hosts):

- the pacing rate: `bandwidth()` is `min(current, share)`;
- the window: `GetCongestionWindow()` is `min(window, 2 x share x minRTT + 4 packets)` when `minRTT` is known.

Both apply after the delay bound and the `inflightBound`; the tightest wins. The group never calls into controllers (no lock order inversion): members add to atomic counters on their own events and read their share; the window tick runs under the group's lock when the first event past `W` arrives. A cut share is immediate, a raised share wakes pacing waiters through the next event, as a rate rise already does.

### 3.5 Behaviour for one client

- With **fewer than two active members** the learned and link ceilings are not enforced, and nothing is learned. One client's behaviour is the path law as before, byte for byte, except for Part A's flag, which acts only on loss that rises with its own rate (against a single policer that is the same correction).
- A configured `--egress-capacity` is enforced even for one, because the operator stated it.
- With **no ceiling known** (no link speed, nothing learned) Part B is inert: the first overload episode on a fresh gateway is the old behaviour until it has been seen.

### 3.6 Switches and validation

- New flag `--egress-capacity` (gateway only): `auto` (default), `off`, or a rate. Under `--congestion-scope=connection` it is inert and a rate is refused, so a configured number is never silently ignored. A client refuses the flag.
- Part A has no flag. It is part of the path authority, which is itself the opt-in (`--congestion-scope=path`, stage 6); the default bundle never reaches it.
- Telemetry (`ControllerTelemetry`): `RateCorrelated` and its `Delta`, the member's `EgressShare`, and the group's `EgressCeiling` and its source (`configured`, `link`, `learned`, none).

## 4. Interactions

- `inflightBound` (#249) is unchanged. It still classifies by queue; Part A adds a second, queue-free reason not to compensate. The group ceiling and the share are applied after it.
- The delay bound, ProbeRTT, `compensationFor`'s delivery check and the burst gates are unchanged.
- The legacy bundle has no gateway-side joint limit; its per-peer models are untouched.
- The client's controllers (uploads) are unchanged by B. A already acts on them.

## 5. Acceptance

All new tests fail before the change and pass after it (the report states which).

1. **Unit, Part A** (`internal/congestion/loss_correlation_test.go`): independent loss at 3 %, 20 % and 42 % and a Gilbert burst model, each under the real gain cycle, leaves the compensation within 5 % of the existing law's erasure and flags in under 2 % of looks; loss `max(0, 1 - C/S)` is flagged within 8 cycles and the compensation falls to the lower-class floor; fewer than 6 `up` epochs, or too few packets per epoch, never flags; the Schmitt trigger does not flap.
2. **Unit, Part B** (`internal/congestion/egress_limit_test.go`): water-filling (max-min, work conservation); no effect under the ceiling; a member below its share is uncapped; one active member is unaffected without a configured ceiling; learn-down needs a flag and two members; random loss alone never learns; probe-up recovers a stale low `K`; link-speed parsing, including `-1` and absence.
3. **Deterministic pathsim, `internal/pep`**: two loaded leaves and a light control leaf (300 KB every 0.5 s) through one shared 1 Gbit/s limiter that drops with no standing queue (`PolicerRefillPeriod`), the DC leg (53 ms), ABBAAB against the legacy arm. The control leaf's p50 is within 1.10x legacy's (plus the existing `dcLatencySlack`), the loaded leaves' aggregate is at least legacy's, and the link drop rate falls. The same test on the unmodified main is expected to fail the first bound; the numbers before and after are recorded in the PR.
4. **Policed regression** (`TestDCPolicedLeafGoodputAndLatency`): one leaf behind a 100 Mbit/s policer, with and without 5 % channel loss, against legacy: goodput at least 0.90x and a fetch beside the download at most 1.10x legacy's at the median (plus the emulator slack), because Part A withholds compensation for policer loss. Goodput and latency before and after are recorded in the PR.
5. **Regressions**, run on the bed host: `TestSharedBottleneckQueuePressure` and the CUBIC bed; `TestDCControlLeafBesideLoadedDownloads`; `TestGatewayFirstFlightBesideBulkDownloads`; `TestBlackhole*`; M5 and W10; `TestDCMultiLeaf*`; the congestion package, `go vet`, and the full suite.
6. **Live (not claimed here).** The stage-9 DC gate's control-leaf p50 contrast, and a full impaired campaign (slow-lossy, blackhole, policed, idle-wake) before the flip, rerun after merge.

## 6. Risks

| Risk | Handling |
| --- | --- |
| The learned ceiling is too low after a transient brownout and starves a path that has recovered | Probe-up 5 % per second after a hold; a gateway that must not learn can set `--egress-capacity` or `off` |
| Part A power is too small on short, low-rate paths | It then does nothing (today's behaviour); B's learning also waits for a flag, so it too does nothing. No path gets worse. |
| A gateway with several interfaces mapped to one port by a bridge or a bond | Link speed is then the bond's or unreadable; configure the ceiling |
| The flag arrives late on a very short DC round trip | The 5 ms epoch floor and the 100 ms window bound reaction to a few hundred milliseconds |
| Groups and the light leaf: a burst of the light leaf lands on a full port | 3 % headroom, and the light leaf is never capped while its demand is below the water level |

## 7. Not in this design

- Weighted shares or priorities between principals.
- Reading the host's queue state (`tc`, NIC counters) or ICMP source-quench style signals.
- Any wire change, or a change to `--congestion-scope=connection`.

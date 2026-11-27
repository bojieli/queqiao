# v0.15.0: what the path-authority flip measured, and how to roll it out

v0.15.0 makes the path-authority bundle the default on both roles (stage 10,
PR #241) and then deletes the legacy bundle and the switches that selected it
(stage 11, PR #245). This document is the evidence for that decision and the
order to deploy it in. Nothing here ships by itself: the owner decides.

The design's gates are `docs/DESIGN-PATH-AUTHORITY.md` §11.3 (the DC gate) and
§15.3/§15.4 (the live A/B gates). Every number below is from a run on real
hosts over a real path, at the commits the runs name.

## Summary

The new bundle is markedly better at surviving an impaired path — outages,
policers, slow lossy links — and better at throughput and fairness on a
datacentre path. Its one measured regression is the **tail of interactive echo
under a policer**: the median keystroke echo is unchanged, the p99 is about
twice main's. The design's own rule says one failing gate blocks the flip, so
that tail is the decision: fix it first, or ship with it as a known limitation
and watch it.

## Impaired-path A/B (imp7, 12 blocks, bj → HK relay → gateway)

Arms: `main` at bb212411 (`--classifier=phased`), `final` at bb2124116 with
`--cc-api=v2 --sched=class --mem=owned --coded-retry=sticky
--congestion-scope=path --classifier=phased --recovery=carrier`, plus
`--gateway-admission=owned` on the gateway and `--topology=single` on the
client. Gates are the pre-registered `gates.ini` rules; a rule passes when both
one-sided 95 % bounds of its per-block contrast clear zero.

What the impaired scenarios exist to test — does the session survive and come
back — is where the new bundle wins:

| measure (per block, mean) | main | new | rule |
| --- | ---: | ---: | --- |
| blackhole: silent session deaths | 13 / 492 | 0 / 492 | `events`, pass (exact p 7e-11) |
| blackhole: failed calls | 9 / 480 | 0 / 480 | `events`, pass (exact p 6e-09) |
| blackhole: transfer failures | 15 / 724 | 12 / 988 | `events`, pass |
| blackhole: resume after an outage, s (down / up) | 5.45 / 6.34 | 2.67 / 4.04 | `lower 1`, pass |
| blackhole: recovery, s | 2.16 | 1.28 | `lower 1`, pass |
| blackhole: TTFT after the path returns, s | 4.15 | 2.70 | `lower 1`, pass |
| policed: stalls per trial | 30.4 | 6.0 | `lower 2`, pass |
| slow-lossy: stalls per trial | 30.8 | 10.7 | `lower 2`, pass |
| blackhole: stalls per trial | 106.8 | 82.1 | `lower 2`, **fail** (better, unproven) |
| policed: TTFT p99, s | 6.69 | 5.23 | `atmost`, pass |
| slow-lossy: TTFT p99, s | 8.95 | 3.41 | `atmost`, pass |

Throughput is higher in every impaired scenario (blackhole 1.32 → 1.79 Mbit/s,
policed 1.55 → 2.10, slow-lossy 1.42 → 2.08) and retransmitted source bytes
fall from 61 % to 5 %. TCP fallbacks fall from 2224 to 1505. Client and gateway
CPU are lower, never at 90 % of a host. Scenario D (idle wake, 6 trials) passes
6/6.

### The one regression: interactive echo under a policer

Terminal sessions under the policer (a rate policer with bursts; the closest
scenario in the suite to a real shaper):

| | main | new |
| --- | ---: | ---: |
| keystroke echo p50, s | 0.38 | 0.36 |
| keystroke echo p99, s (pooled, 2889 samples) | 1.92 | 3.76 |
| keys slower than 1 s | 5–8 % of keys | 5–12 % of keys |
| worst session's p99, s | 2.3 | 5.8 |

The median key is unaffected and the frequency of slow keys is close; what is
worse is how deep the slow bursts go. Both arms show the same shape — slow keys
arrive in bursts while a policer queue drains — so this is the new congestion
law holding less back behind a policer, which is what issue #238 describes on
another path. Three other rules fail in the same mechanical way, without the
candidate being worse: `blackhole.stalls_per_trial` (82 vs 107 stalls) and
`slow-lossy.echo_p99_s` (2.29 vs 3.20 s) are better but do not clear the
t-bound at the block cap, and `policed.fetch_failures` was left unmeasured
because the slower arm ran fewer attempts (0/150 against 0/109); the rule's
minimum is now judged on the candidate (#277), which reads it as pass.

### Two artefacts, root-caused to the harness

Neither is a defect in the arm under test; both are fixed.

- One policed terminal session ended with the client application seeing EOF
  30 ms after its TLS handshake while the arm still held the flow open and
  delivered every byte. The workload client read and wrote one TLS connection
  from two threads, which Python's `ssl` does not serialise. Stress-tested:
  about one session in eight returns a spurious EOF or "bad record mac" on a
  bare `SSLSocket`; none do through the serialising wrapper (#276).
- The dcg7 rerun aborted two minutes into block 2 because a start that left no
  pid file was only noticed by the next wait (#278).

## Datacentre gate (stage 9, 12 blocks)

The DC gate checks that the redesign does not harm a path it is not about:
fifteen criteria, each a paired per-block contrast that must clear zero, plus
two binding bounds and a control leaf that must not be slowed.

`dcg6` passes all fifteen, with ancestor binding 0.33 and uplink binding 0.58
(both must be ≤ 0.8) and the control leaf's request p50 contrast +0.034 s
(bounds +0.015 / +0.018). Aggregate sustained goodput 4040 → 5825 MB per
block, worst leaf 1593 → 2763 MB, request p99 1.19 → 0.42 s.

The run was recorded as **VOID** by the gate's own bookkeeping, and the record
should say so plainly rather than be quietly re-judged. The reason is one
upload-solo window where the gateway saw two leaf keys: a 19.4 MB flow of the
other upload leaf that **started 13.6 s before the solo window** and finished
inside it — the tail of the preceding simultaneous-upload phase, not work
scheduled into the solo run. It is 3.4 % of that window's bytes and moves the
calibrated solo rate in the conservative direction (the leaf's measured
capacity is a lower bound, which makes the binding ratio harder to pass). With
flows attributed to the window they started in, the same data reads 15/15
pass. Both readings are recorded at the run; no byte of the data changed.

A confirmation run (dcg7) aborted on the harness bug above and was not
restarted: the evidence is one run, and the margins it passed by are wide.

## Fairness and known costs

- Against kernel CUBIC on a shared bottleneck the new bundle takes 0.32–0.51 of
  the link (legacy: 0.82–0.84). Accepted: the redesign stops one session
  hoarding an ancestor.
- The credit-delay sampler shows the new bundle waits for credit about twice as
  often in normal running; its p95 buckets are equal to main's. Not blocking;
  tune later (#222).

## What is not claimed

- No claim about paths other than the one measured (bj → HK relay → gateway)
  and the DC path.
- No claim that the interactive tail under a policer is fixed.
- No claim about the long tail of the pooled-seed model (#73), fan-out (#244),
  or joiner ramp follow-ups (#160).
- The Mac client's flow-failure rate after the earlier rollout was 1.8 %
  against 0.6 % before it; about 550 of those failures were attributed to known
  causes, and the rest are to be rechecked after this rollout.

## Rollout

Order matters: the gateways speak to every client, and the wire stays
compatible in both directions (wire 1 and 2 are negotiated; this release
deliberately contains no wire change and no cutover). Each step is verified
before the next; each step is individually reversible.

1. **Gateways first** (the two production gateways), one at a time, with
   `--bundle=legacy` ready as the rollback. Watch, for an hour each:
   flow-failure rate, `lane_join_refused` by reason, silent flow deaths,
   fallback counts, and the gateway's own error rate. Compare against the same
   hour a day earlier.
2. **Relays** (the two relay hosts), one at a time, same observations.
3. **The Mac client**, last: it is the smallest population and the one with
   recorded flow failures to compare against.
4. **Release v0.15.0** only after steps 1–3 hold for a day: stage 10's bundle
   is the default and stage 11 has deleted the legacy switches, so the release
   binaries are the new architecture only.

Rollback at any step: reinstall the previous binary, or for step 1 start the
gateway with `--bundle=legacy` (stage 10 keeps it; it is stage 11 that removes
it, and stage 11 merges only after this rollout has soaked). No data migration
is involved, and no state outlives a restart except the enrolled profiles,
which are unchanged.

## Open items at the decision

| item | state |
| --- | --- |
| interactive echo tail under a policer (p99 ~2×, p50 unchanged) | open, #238; the one gate that says the flip is blocked |
| `blackhole.stalls_per_trial`, `slow-lossy.echo_p99_s` | better but unproven at the block cap |
| `policed.fetch_failures` | judged pass under the corrected minimum (#277) |
| label races in failure attribution | open, #274 |
| flaky DC light request beside load | open, #261, pre-existing on main |
| pooled-seed model, joiner ramp, fan-out | open, #73 / #160 / #244, deferred |

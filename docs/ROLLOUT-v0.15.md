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
worse is how deep the slow bursts go. A deterministic reproduction (a 20 s
test over the emulated path, issue #282) locates it: the delay is not
queueing. On a queueing shaper of the same rate the bundles swap places — the
new bundle's echo p99 is 213 ms against main's 488 — and with no bulk on the
path both are clean. What a policer changes is that a small record can be
dropped at the bucket, and the new bundle brings a dropped interactive record
back later than main does (about 500 ms, on the leg the bulk runs on). The
mechanism is `coded-retry=sticky`'s recovery of a coded interactive record
(design §8.1), not the pacing runaway of #238: on this path the new bundle's
pacing stays near the policer's while main's runs away.

Three other rules fail in the same mechanical way, without the
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

The runbook below is what is actually executed, in two phases: soak the new
defaults on every deployment with `--bundle=legacy` as the rollback, and only
then cut v0.15.0. Nothing in it starts before the stage-10 pull request has
passed its gates and merged, and the owner decides when each phase starts.

It names hosts by role only; the mapping to real hosts, units and paths is kept
in a local, git-ignored file beside this one (`ROLLOUT-v0.15.local.md`).

| Label | Role |
| --- | --- |
| `GW-1`, `GW-2`, `GW-3` | production gateways (`queqiaod server`, systemd `queqiaod.service`) |
| `CLIENT-1` | the desktop client (`queqiaod client`, installed with `queqiaod service install`) |
| `RELAY-1`, `RELAY-2` | relay hosts in front of the gateways (`queqiaod relay run` units) |

## What changes, and what does not

- **Behaviour.** With no switch flags, both roles run the path-authority
  bundle: `cc-api=v2`, `sched=class`, `congestion=path`,
  `coded-retry=sticky`, `recovery=carrier`, `mem=owned` and
  `classifier=phased`, plus `gateway-admission=owned` on a gateway and
  `topology=single` on a client (`docs/DEPLOYING.md`, "The path-authority
  bundle").
- **Wire.** Unchanged. A gateway still accepts wires 1 and 2 by default, and
  serves clients on wire 1, released clients, and clients on the legacy
  bundle. That is why gateways go first: every client, old or new, keeps
  working against an upgraded gateway, and a new client works against a
  gateway not yet upgraded.
- **Admission.** Under `gateway-admission=owned` the `--max-sessions`
  ceiling counts flows rather than streams. A value sized for streams is
  therefore at least as generous as before; it does not need to change.
- **Relays** forward bytes and run no bundle. They are upgraded only to keep
  one binary version per host.

## The two phases

| Phase | Build | Rollback |
| --- | --- | --- |
| 1. Soak | `main` at the stage-10 merge, built by the release-candidate workflow (no published release) | `--bundle=legacy` on the host, or the previous binary |
| 2. Release | v0.15.0, after stage 11 deletes the legacy code | the previous binary only (the soak build, or v0.14.0) |

Phase 1 is the design's soak: the new defaults run in production while the
legacy bundle is still one flag away. Phase 2 starts only after phase 1 has
run clean on every host for at least seven days, and stage 11 has merged.

## Before anything

On every host, record and keep with the local file:

1. `queqiaod version`, and the SHA-256 of the installed binary.
2. The exact service arguments: `systemctl cat queqiaod.service` (gateways),
   the relay units, and on `CLIENT-1` the LaunchAgent or systemd user unit.
3. A metrics baseline over the previous 24 hours, from each endpoint's
   `--metrics-listen`: `queqiao_flows_completed_total`,
   `queqiao_flows_failed_total`, `queqiao_flow_timeouts_total`,
   `queqiao_lane_join_refused_total` by reason,
   `queqiao_account_admission_refused_total` by reason, and
   `queqiao_data_connections_total` by wire. The failure ratio is
   `failed / (completed + failed)` over a window.
4. A backup of provider state on each gateway (`docs/DEPLOYING.md`, "Backup
   and monitoring") and of the enrolled profile on `CLIENT-1`.
5. Copy the current binary beside itself as `queqiaod.prev` on every host.
   It is the binary rollback for the whole rollout.

## Health checks

Run these after every step, and again at the end of each wait. A step is
healthy when all of them hold.

- **H1, process.** The service is active and has not restarted since the
  step (`systemctl show -p ActiveState,NRestarts,ExecMainStartTimestamp`
  on Linux; `launchctl print gui/$UID/<label>` on macOS).
- **H2, configuration.** The newest `runtime configuration` log record
  shows the intended bundle: `bundle_name` is `path-authority` (or `legacy`
  after a flag rollback) and `bundle` lists the switches, for example
  `cc-api=v2,sched=class,congestion=path,recovery=carrier,coded-retry=sticky,mem=owned,gateway-admission=owned,classifier=phased`
  on a gateway. `queqiaod version` reports `wire=2 wire_accept=1,2`.
- **H3, traffic.** Through `CLIENT-1`, an HTTPS request through each
  provider's SOCKS listener succeeds
  (`curl --socks5-hostname 127.0.0.1:<port> -o /dev/null -w '%{http_code}' https://example.com/`),
  and a UDP DNS query through it is answered. Through each relay, the same
  from a client whose profile points at that relay.
- **H4, failures.** Over the window since the step, the flow failure ratio is
  at most twice the baseline's and at most 2 % absolute; flow timeouts and
  `queqiao_lane_join_refused_total` do not grow faster than in the baseline;
  `queqiao_account_admission_refused_total{reason="flow_limit"}` does not
  appear where it was zero.
- **H5, old peers (gateways).** Connections on wire 1, if the baseline had
  any, still arrive (`queqiao_data_connections_total{wire="1"}` keeps
  rising) and their flows complete.
- **H6, resources.** Gateway CPU and resident memory stay within 1.5x of the
  baseline at similar load; on `CLIENT-1`, the process stays within its usual
  memory.

Any failed check is a stop: do not take the next step, and roll back the
host if the check does not recover within 15 minutes.

## Phase 1: soak the new defaults

Each step installs the soak build the same way:

```sh
# on the host, with the new binary uploaded as /tmp/queqiaod.new
/tmp/queqiaod.new version                 # expect the soak build and wire=2 wire_accept=1,2
sudo install -m 0755 /tmp/queqiaod.new /usr/local/bin/queqiaod.new
sudo mv /usr/local/bin/queqiaod.new /usr/local/bin/queqiaod   # atomic replace
```

No service arguments change: the defaults carry the new bundle. If a unit
names `--congestion` other than `erasure`, or a client's `--quic-pool=false`,
the new build refuses to start and says so. Remove that flag, or add
`--bundle=legacy` to keep that host on the old behaviour, before restarting.

1. **`GW-1`.** Install, `sudo systemctl restart queqiaod`. A restart ends
   the flows the gateway carries, as any restart does; clients reconnect on
   their own. Check H1 to H6 at once, then wait one hour under normal traffic
   and check again.
2. **`GW-2`**, then **`GW-3`**: the same, one at a time, each with its
   one-hour wait.
3. **All gateways soak for 24 hours** with clients unchanged. This is the
   mixed fleet of design section 12: released clients, which isolate and
   rescue per flow, against gateways that attach each device's connections to
   one path controller.
4. **`RELAY-1`**, then **`RELAY-2`**: install, restart each relay unit
   (`sudo systemctl restart <relay unit>`), check H1 and H3 through that relay.
5. **`CLIENT-1`.** Replace the binary the service runs and restart it
   (`launchctl kickstart -k gui/$UID/<label>` on macOS, or
   `systemctl --user restart <unit>` on Linux). Check H1 to H4 and H6 at once,
   after one hour, and after 24 hours.
6. **Soak for at least seven days** with every host on the soak build. Check
   H4 and H6 daily on every host.

### Roll back, phase 1

Roll back the host that failed, not the fleet, unless the failure is on every
host. Prefer the flag rollback, which keeps the build and changes only the
bundle; use the binary rollback when the build itself is at fault (a crash,
or a failure that `--bundle=legacy` does not clear).

**Gateway, by flag.** Add `--bundle=legacy` to the service's command line in
a drop-in, so the unit file itself is untouched:

```sh
sudo systemctl edit queqiaod.service
# In the editor, repeat the current ExecStart (from `systemctl cat`) with the flag:
#   [Service]
#   ExecStart=
#   ExecStart=<the recorded ExecStart, unchanged> --bundle=legacy
sudo systemctl restart queqiaod
```

H2 must then show `bundle_name` `legacy` and `bundle` `legacy`. To undo it,
`sudo systemctl revert queqiaod.service` and restart.

**Gateway, by binary.**

```sh
sudo install -m 0755 /usr/local/bin/queqiaod.prev /usr/local/bin/queqiaod
sudo systemctl revert queqiaod.service   # only if a drop-in was added
sudo systemctl restart queqiaod
```

`queqiaod version` must report the recorded previous version.

**`CLIENT-1`, by flag.** Reinstall the service with the recorded arguments
plus the bundle, which re-renders the unit and restarts it:

```sh
queqiaod service install <the recorded install arguments> --bundle legacy
```

To undo it, run the same command without `--bundle legacy`.

**`CLIENT-1`, by binary.** Copy `queqiaod.prev` over the binary the service
names and restart the service as in step 5.

**Relays, by binary.** Copy `queqiaod.prev` back and restart each relay unit.
Relays have no bundle, so there is no flag rollback.

A host rolled back by flag or binary stays compatible with every other host:
the wire is the same, and a gateway on either bundle serves clients on either
bundle.

## Phase 2: v0.15.0

Stage 11 deletes the legacy bundle with its switches and `--bundle`. Before
installing a build from after stage 11 on any host:

- the host must be healthy on the soak build with no switch flags and no
  `--bundle` in its service arguments, because the new build rejects them; and
- no host may still need the legacy bundle. If one does, phase 2 waits.

Then repeat the phase 1 order and checks: `GW-1`, `GW-2`, `GW-3` (one hour
each), 24 hours on gateways, the relays, `CLIENT-1`.

### Roll back, phase 2

There is no flag rollback. Reinstall the previous binary as in phase 1: the
soak build if it was healthy, otherwise v0.14.0 (`queqiaod.prev`). Either one
interoperates with v0.15.0 on both wires, so a single host can be rolled back
alone. A host rolled back to the soak build runs the path-authority bundle by
default; add `--bundle=legacy` there only if the failure needs the legacy
behaviour.

## Record

For every step, write down in the local file: the time, the build's
`queqiaod version` and SHA-256, the result of each health check, and any
rollback with its reason. After phase 2, the soak result and the release
decision go in the stage-11 pull request.

## Open items at the decision

| item | state |
| --- | --- |
| interactive echo tail under a policer (p99 ~2×, p50 unchanged) | open, #282; the one gate that says the flip is blocked |
| `blackhole.stalls_per_trial`, `slow-lossy.echo_p99_s` | better but unproven at the block cap |
| `policed.fetch_failures` | judged pass under the corrected minimum (#277) |
| label races in failure attribution | open, #274 |
| flaky DC light request beside load | open, #261, pre-existing on main |
| pooled-seed model, joiner ramp, fan-out | open, #73 / #160 / #244, deferred |

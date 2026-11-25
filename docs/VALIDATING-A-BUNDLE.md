# Validating a bundle

The path-authority migration ([`DESIGN-PATH-AUTHORITY.md`](DESIGN-PATH-AUTHORITY.md),
section 14) adds its behaviour as bundles behind switches. A switch moves
forward only after it passes a live gate, and there are three kinds of gate:

| Gate | Design | What it decides | Tools |
|---|---|---|---|
| A stage's live A/B | 15.3, 15.4 | whether a stage's bundle is at least 0.90 of the reference on every path | `ab_campaign.py`, `ab_analyze.py` |
| The DC gate (stage 9) | 11.3 | whether `dc-long-haul` moves to the new architecture or keeps the legacy bundle | `ab_campaign.py` with `workload = dc`, `dc_gate.py` |
| The default flip (stage 10) | 14 | whether new defaults ship | the first gate, run on the default build |

All three run on real paths. The correctness gates of section 15.1 are
ordinary Go tests and are not covered here.

The harness replaces the per-campaign shell scripts that earlier A/B runs
used. It keeps their trial shape: concurrent `curl` downloads through a
temporary SOCKS5 client, the completion rule (curl exit 0, HTTP 200, exact
byte count), gateway `/metrics` scraped before and after every trial, and the
lane trace on both ends. On top of that it adds the block design and the
statistics of section 15.4, refuses to start next to production, and tears
everything down.

## The pieces

- `scripts/ab_campaign.py` runs a campaign. Each arm is a build plus a flag
  bundle. The build is a git ref, built locally for every platform the
  campaign needs, or prebuilt binaries. A crossover arm builds its gateway and
  its client from different refs (`gateway_ref`, `client_ref`).
- `scripts/ab_analyze.py` evaluates a campaign against section 15.4 and prints
  a markdown table for a pull-request comment. `--pilot` sizes the campaign.
- `scripts/dc_gate.py` evaluates a DC gate campaign against the frozen bounds.
- `scripts/ab_stats.py` holds the statistics. It uses the standard library
  only, and its tests pin every function to a known answer.
- `scripts/validation/` holds the example campaigns, `gates.ini` (the section
  15.4 constants and the secondary bounds), and `dc_gate.ini` (the stage 9
  bounds, frozen).

## Safety

The campaign runs next to production gateways and relays, so the runner is
built to refuse rather than to trust:

- **No host is named in the repository.** Hosts and addresses live in a config
  file matching `*.local.ini`, which git ignores. Use ssh aliases. Only
  documentation addresses appear in the committed examples.
- **Every host needs a `[production:...]` section.** The section lists the
  host's systemd units, launchd labels, declared ports (include a production
  gateway's whole hop-port range), and a process pattern. The section must
  exist even when it lists nothing, so no host is touched without being looked
  at first.
- **Preflight is read-only, and it refuses.** Before anything is copied or
  started, the runner snapshots each host:
  - each unit's state, MainPID, NRestarts, start time and executable SHA-256;
  - each launchd job;
  - every process that matches the pattern;
  - every listening port.

  It then refuses to start in any of these cases:
  - a planned temporary port is declared or already listening;
  - a host's listening ports could not be read (it needs `ss` on Linux and
    `lsof` elsewhere; an empty list is never trusted);
  - a declared unit or launchd job cannot be observed, or the process table
    cannot be read;
  - a home directory contains anything but `[A-Za-z0-9._/-]`;
  - a process from an earlier campaign with the same name is still running.
- **One directory per host, reserved.** Everything temporary lives under
  `~/qq-ab-<name>`. After the read-only preflight, the runner claims that
  directory on every host with an atomic `mkdir` and an owner token. A second
  invocation with the same name is refused, and teardown and cleanup act only
  on directories whose token is their own. Every process is started from there with a pid file. Teardown signals only
  those pid-file processes, each after checking that its command line contains
  the directory, so a stale pid file can never reach a production process.
  Any other process that mentions the directory, such as an operator's
  `tail -f`, is reported as a leftover, never killed. Nothing uses `pkill`.
  A process counts as ready only when the spawned process itself owns the
  listening port, so a child that failed to bind is never mistaken for
  another campaign's listener on the same port.
- **Production is watched during the run.** The runner re-snapshots every
  `snapshot_every` blocks and once more at the end. Any change to a
  production unit, launchd job or matching process aborts the campaign, tears
  it down, writes `prod-diff-*.txt`, and exits with status 3.
- **Small hosts.** A host marked `small = true` never runs more than one
  temporary client at a time: each trial starts its arm's client and stops it
  afterwards. Earlier campaigns had resident clients OOM-killed on a 769 MB
  relay host. The runner also checks this at run time.
- **Jobs run in their own process groups.** Each trial job (curl, a load
  generator) runs in a process group recorded under `run/`, so teardown stops
  the job and everything it started, even when the runner was interrupted
  while the job's ssh session was still open. The record holds the group
  leader's pid and start time. A group is signalled only if that same
  process still leads it; otherwise the record is reported as stale and left
  alone. A job that does not run to its end (ssh dropped, timed out, the
  generator crashed) is a harness failure, not the arm's, and its block gets
  no verdict.
- **Teardown always runs**, on success, on error, and on SIGINT or SIGTERM.
  It stops every temporary process, checks that no planned port is still
  listening, and removes the work directory (pass `--keep-remote` to keep it).

`--dry-run` prints the whole plan:

- arms, hosts, planned ports and the full schedule;
- every command of the first block, and of setup and teardown.

It contacts no host and builds nothing. Run it first, every time.

## Configure once

```sh
cp scripts/validation/campaign.example.ini scripts/validation/stage6.local.ini
$EDITOR scripts/validation/stage6.local.ini
./scripts/ab_campaign.py --config scripts/validation/stage6.local.ini --dry-run
```

The config has five kinds of section:

- `[campaign]`: blocks, seed, workload, and timing.
- `[gateway]`: one host, and the literal address clients dial.
- One `[client:<path>]` per measured path. On a laptop, set
  `--local-address` so a TUN-mode proxy does not carry the test
  ([`BENCHMARKING.md`](BENCHMARKING.md), "Live campaigns").
- One `[arm:<name>]` per arm, exactly one of them `role = main`.
- One `[production:<name>]` per host.

The `main` arm is the reference the contrasts are taken against. It is
usually today's `origin/main`, but not always: see stage 7 below.

Flags that point a process at state, a listener, a log or a profile
(`--state`, `--listen`, `--metrics-listen`, `--log-file`, `--profile`, and
others) are set by the runner. A config that sets them is rejected, in either
the `-name` or the `--name` spelling, since Go's flag parser accepts both.
`--hop-port-count` is rejected too, because it opens listeners that the
preflight cannot reserve. Enrollment always runs over TCP, so the runner enrolls
each client against a gateway started with `--transport auto`, then restarts
it with the arm's own flags for every block. A client's `--local-address`
is taken from its full flag list in either spelling, and the last occurrence
wins, as the client itself sees it, so enrollment uses the same interface.

## Gate 1: a stage's live A/B

### Choose the arms

Arm flags are the section 14 switch names. Check `queqiaod client -h` on the
candidate build.

| Stage | Reference arm (`role = main`) | Candidate arm | Note |
|---|---|---|---|
| 0, 1 | `origin/main` | `--cc-api=v2` | baseline experiments only; the class scheduler is not available yet (round-5 review) |
| 2 | today's controllers | {isolated, single connection} × {round-robin, `--sched=class`} | the product-configuration screen of 15.3: four arms. It chooses a configuration; it does not attribute a result to topology |
| 6 | `origin/main` | `--cc-api=v2 --sched=class --congestion=path` | works with isolation on |
| 7 | `--congestion=path` with isolation | `--congestion=path --topology=single` | the topology test of 15.3: the reference arm is *not* main |
| 8 | stage 7's bundle | `... --recovery=carrier` | |

More than two arms is fine. Each arm runs twice per block, in a rotated order
followed by its mirror, and every arm is compared with the reference.

### Workloads

`workload` selects one of the section 15.3 workloads:

- `bulk`: four concurrent downloads.
- `single`: one flow.
- `interactive`: the bulk load plus a small-object probe every
  `probe_interval` seconds. It measures interactive p99 under bulk. The probe
  stops as soon as the bulk flows finish, so every sample is taken under
  load. A probe that does not finish counts at its 10-second timeout. Each row
  carries millisecond start and end times on its host's clock (`t_start`,
  `t_end`). A probe is used only if it ran while some load transfer did.
  0.1 s of clock uncertainty (`OVERLAP_MIN`, for NTP-level error between
  hosts) is taken off each end of every load interval, and the probe must
  overlap what remains. So a 20 ms probe in the middle of a download counts,
  and one at the download's edge does not. A probe in an idle gap between
  waves, or without sub-second times, is left out, and a trial with none left
  is unmeasured.

A stage's gate needs all three. Run them as three campaigns with different
names.

### Unit and outcome

One block is four trials on one path for two arms, in ABBA or BAAB order
chosen at random from the seed. With N arms a block has 2N trials. Every
gateway is restarted before every block, so no path knowledge
(`knowledgeLifetime`) carries from one block to the next. An arm's first
trial in a block starts cold. Its second starts warm, from that arm's own
first trial. On a small host the client is restarted for every trial, but the
gateway keeps its state, and the gateway is the sender for these downloads.

The outcome per arm per block is the useful bytes delivered within `duration`
seconds, summed over the arm's two trials and all their flows. Useful bytes
are HTTP 200 body bytes, whole or partial; an error page counts as zero.

Each bulk flow fetches object after object until the window closes, so it
stays backlogged. Otherwise a fast arm would run out of bytes and be capped
at the same total as a slow one. A flow is complete when every object
arrived whole, except the last, which the window may cut. The analyzers
check backlog the same way for the A/B and the DC gate: a flow whose last
object finished while the flow had been active for clearly less than the
window was byte-capped. Any capped flow makes the primary gate VOID, and
the pilot refuses to size from it. A failed flow
counts what it delivered. A flow that printed nothing counts as zero; it is
not dropped.

### Pilot, then freeze the block count

Section 15.4 fixes the sample size in advance from a pilot. Run a 12-block
campaign, then:

```sh
./scripts/ab_campaign.py --config scripts/validation/stage6.local.ini --output ~/campaigns/stage6-pilot
./scripts/ab_analyze.py ~/campaigns/stage6-pilot --pilot
```

The pilot table gives, per path and candidate, the blocks needed for 80%
power when the true ratio is 1, using the pilot's contrast variance and its
mean main outcome. If no block count up to 100,000 reaches that power, the
table says so instead of printing a number. It also withholds the number
for a pilot that is incomplete or shorter than 12 blocks. Freeze the largest number as
`blocks`. Do not reuse the
pilot's blocks, and change the campaign `name` so the new campaign gets its
own directory.

### Run, analyze, post

```sh
./scripts/ab_campaign.py --config scripts/validation/stage6.local.ini --output ~/campaigns/stage6
./scripts/ab_analyze.py ~/campaigns/stage6 > comment.md
gh pr comment <PR> --body-file comment.md
```

A campaign directory holds:

- `plan.json`: arms, commits, binary SHA-256s and the schedule.
- `effective-commands.json`.
- `trials.tsv`: one row per flow.
- `metrics/`: gateway scrapes before and after each trial.
- `logs/`: gateway and client logs, gzipped, and client CPU samples.
- `prod-*.txt`: the production snapshots.
- `status.json`: written last. It says whether the campaign completed,
  whether production was unchanged, which temporary listeners or processes
  survived teardown, and which log files could not be fetched. Either of the
  last two makes the runner keep the remote work directories, and a teardown
  leftover makes it exit with status 5. If the final production snapshot
  cannot be taken, the campaign is recorded as aborted with production
  `unknown`, and the runner exits with status 6. A work directory that cannot
  be removed stays reserved, is listed under `cleanup_failures`, and the
  runner exits with status 7.

The analyzer adds `per_trial.tsv` and `summary.md`.

### Reading the table

For each path, the analyzer prints each arm's medians:

- goodput;
- completion;
- server loss;
- queue p90, from the gateway lane trace as `srtt - minRTT`, deduplicated
  per connection;
- stalls per trial;
- wire bytes per useful byte;
- reissued bytes per source byte;
- flow-ACK transit p50 and p90;
- client CPU p90;
- for the interactive workload, interactive p99.

Flow-ACK transit is matched by sequence number when the build logs per-ACK
`flow ack sent` and `flow ack received` events, as the instrumented builds of
the 2×2 run did. Otherwise it is count-matched from the lane trace's `acksout`
and `acksin`, which assumes one ordered stream per flow. The header says which
method was used.

Then comes the primary gate, per candidate. For each block it takes
`d = candidate − 0.90 × main` and reports:

- the mean contrast;
- the one-sided 95% t lower bound, which the design names as primary;
- the bootstrap lower bound, which the design names as the check.

The gate **passes only when both lower bounds are above zero**. A
disagreement fails rather than being settled in the candidate's favour. The
gate is **VOID** when more than 10% of main's blocks delivered nothing; the
campaign is then rerun. "Point estimate ≥ 0.95 and the interval includes 1"
is not a pass.

Next come the secondary bounds from `gates.ini`. Each one is a per-block
`candidate − main` difference with a one-sided bootstrap bound against a
pre-registered margin. The design requires all of them but does not give
their values. The margins in `gates.ini` are **provisional** and must be
confirmed before the stage's campaign starts.

A secondary is judged only when it was measured in both arms in every block
the primary gate judged (`secondary_min_coverage`). A block counts as
measured only if every trial in it has the measure. A trial in which any flow
fell back to TCP has no QUIC-only wire count, so it leaves wire cost
unmeasured rather than flattering it. Fallback is read from the client's own
`/metrics`, which the runner scrapes around every trial, because the client
is where fallback is counted. A client whose runtime configuration says
`transport=tcp` also has no QUIC wire cost. So does a trial in which a
gateway flow record shows a TCP carrier, or a TCP lane that carried bytes:
an AUTO rescue can hand a flow to TCP without counting a client fallback.
Without those records, QUIC-only cannot be shown, and wire cost is
unmeasured. Sender-side credit delay p95 and the
shadow false-promotion rate need the stage 0 instrumentation. They are listed
as required and reported as not measured, so until that instrumentation
lands, no campaign reaches an overall PASS.

The last line per candidate is the overall verdict:

- **PASS**: the primary gate passes and so does every secondary.
- **FAIL**: any of them fails.
- **INCOMPLETE**: there is no verdict. Either a required secondary could not
  be measured, or the campaign itself is incomplete: `status.json` is missing
  or not `complete`, production changed, teardown left something behind, or a
  planned trial has no rows. An incomplete campaign gets no PASS, and no FAIL
  either. The
  analyzer leaves out any block with a missing trial, so an interrupted block
  cannot pair two candidate trials with one main trial, and it still prints
  the numbers for diagnosis. INCOMPLETE is not a pass.

## Gate 2: the DC gate (stage 9)

Section 11.3 keeps `dc-long-haul` on the legacy bundle through the migration.
It moves the profile only if the profile passes its own gate on the new
architecture. The bounds are in `scripts/validation/dc_gate.ini`. They are
**frozen**: section 11.3 and the round-5 review both require them to be fixed
before the campaign runs. A change must land in its own commit before any
stage 9 data exists.

### The workload

A leaf is an endpoint pair, and the gateway is the sender for downloads. So
the gateway's tree runs from its egress to a group to each client address,
and each client host is one leaf under a shared ancestor. The DC campaign
(`scripts/validation/dc-campaign.example.ini`) has these parts:

- One temporary gateway per arm, with `--path-profile dc-long-haul` on both
  ends.
- **Loaded leaves:** two or more client hosts, each its own leaf, that load
  the shared ancestor at the same time.
- **A control leaf:** the uncongested sibling. It fetches one request-sized
  object every `control_interval` seconds and nothing else, and only while the
  loaded leaves are running.
- **Each trial, two phases, with every leaf at once:**
  - *sustained*: `bulk_flows_per_leaf` downloads per loaded leaf, for
    `duration` seconds;
  - *requests*: `waves` waves of `requests_per_leaf` concurrent 300 KB
    fetches per loaded leaf, the shape of the profile's own relay
    measurement.
- **A shared start:** a phase's jobs start on a barrier, five seconds ahead
  on the hosts' clocks. Request wave *w* starts at the barrier plus
  (*w* − 1) × `request_timeout` on every leaf. The analysis requires every
  loaded leaf to have been in each wave together for at least 0.1 s, on
  millisecond times; otherwise latency and spread are unmeasured. This also
  relies on NTP-synchronised clocks.
- **A shared sustained window:** every loaded leaf must have been
  transferring together for at least 90% of the window. This is measured on
  each leaf's actual transfer intervals, intersected across leaves; a leaf
  that transferred at both ends of the window and idled in between did not
  compete. A leaf whose ssh
  session missed the barrier runs late and alone, and its bytes are not
  service it received while competing. Otherwise that trial's aggregate,
  worst-leaf, fairness and binding measures are unmeasured.
- **Calibration:** at the end of every block, each loaded leaf runs alone on
  the legacy arm. Every judged block needs its own usable calibration of every
  loaded leaf, otherwise the campaign is INCOMPLETE. A calibration is usable
  only if it ran backlogged for the window, because a short burst followed by
  a failure would inflate the solo rate. With no usable calibration at all
  (or `calibrate = false`), the binding is not measured, and the verdict is
  INCOMPLETE rather than VOID.
- **The temporary account** has no device or flow limit of its own, so the
  gateway's `--max-sessions` is the only admission limit. A campaign then
  measures the path rather than account admission.

### The criteria

Each criterion is a paired per-block contrast against the legacy arm. Both
the t and the bootstrap one-sided lower bounds must be above zero.

| Criterion | Contrast |
|---|---|
| sustained goodput (aggregate of loaded leaves) | `cand − 0.90 × legacy` |
| request completion (loaded and control requests) | `cand − (legacy − 0.02)` |
| request latency p50 and p99 | `1.10 × legacy − cand` |
| worst leaf's service | `cand − 0.90 × legacy` |
| completion spread (slowest leaf's median over fastest leaf's) | `1.25 × legacy − cand`, and `3.5 − cand` |
| cross-leaf fairness (Jain's index) | `cand − (legacy − 0.05)` |
| control leaf request p50 | `1.10 × legacy − cand` |

The spread bound exists because of the regime the profile document reports
(`DESIGN-DC-PROFILE.md`, "One relay per node isn't faster than several" and
"Relay layout"). Independent relays moved about 2.5× the aggregate of one
shared relay, but their groups spread out by 3.5×, while the shared relay
finished every flow within 2 ms. A knowledge-only tree buys aggregate the same
way, so a candidate that wins on goodput by starving a leaf fails here.

The campaign is **VOID** when the evidence contradicts the gate's premise:

- **Carrier and leaves.** With the campaign trace on, the gateway's flow
  record names each QUIC carrier's path-tree egress and leaf key
  (`path_egress`, `path_key`, as `resolvePath` keys the tree). Each solo
  calibration trial must show exactly one leaf key. The loaded leaves' keys
  must be distinct: two hosts behind one address are one leaf. Each
  simultaneous trial must show exactly those keys plus one control key, all
  under one egress. Every flow must be QUIC: its opening carrier and every
  lane that carried bytes. No client's fallback counter may move. If the
  records or counters are missing, or a record has an empty egress (the
  gateway's route lookup failed), there is no verdict.

- The shared ancestor did not bind. The legacy arm's simultaneous rate must
  average at most 0.80 of the sum of the leaves' solo rates. Each rate is a
  leaf's bytes over the duration of its longest flow, so objects that finish
  early cannot hide a shared bottleneck. Otherwise the campaign did not test
  the regime, so add load and rerun.
- A sustained flow was byte-capped (as in Gate 1). Raise `object_bytes`.
- More than 10% of the legacy arm's blocks delivered nothing. Rerun.
- The legacy arm completed fewer than 90% of its requests, so the workload
  does not work at all. Latency would otherwise compare two sets of
  substituted timeouts.

A log file that could not be fetched withholds the log-derived measures of
its trials, and the coverage rule then leaves the affected secondaries
unmeasured. An unfinished campaign is INCOMPLETE, as in Gate 1. So is one in which any
criterion lacks evidence in some judged block, for example a control probe
that produced nothing or did not overlap the load. So is one in which any
endpoint does not show `dc-long-haul`. The runner adds that flag to both
ends and rejects configs that set it. The check is made one endpoint at a
time:

- each client: its runtime configuration record;
- the gateway: its running command line, recorded by the runner at every
  start, because the gateway's record carries no profile.

```sh
./scripts/ab_campaign.py --config scripts/validation/dc.local.ini --dry-run
./scripts/ab_campaign.py --config scripts/validation/dc.local.ini --output ~/campaigns/dc-gate
./scripts/dc_gate.py ~/campaigns/dc-gate > comment.md
```

Section 11.3 states what follows from each outcome:

- **PASS:** the profile moves to the new architecture, and the legacy bundle
  may be deleted at stage 11.
- **FAIL:** the legacy bundle stays for this profile, nothing it needs is
  deleted, and joint limiting waits for the tree-arbiter design (Q2).

## Gate 3: the default flip (stage 10)

Stage 10 flips every default in one release, gateways first, and soaks for
one release. Its gate is Gate 1 run on the release candidate:

- **Arms.** The reference arm is the previous release, or `origin/main`
  before the flip, with no flags: the legacy bundle. The candidate is the
  release-candidate build with no flags, so its new defaults are what is
  measured.
- **Paths and workloads.** Every path in the field campaign. Run every
  workload in `FLIP_WORKLOADS`: `bulk`, `single`, `interactive`, `llm`,
  `term` and `mixed` for now; Phase B of the workload model adds the rest.
- **Gateways first.** Add a crossover arm (`gateway_ref = <candidate>`,
  `client_ref = <previous release>`). It shows that an upgraded gateway does
  no harm to clients that have not upgraded yet, which is the order the
  rollout takes.
- **Legacy bundle.** The legacy bundle must still start and pass. Keep an arm
  with every switch explicitly off on the candidate build, so the rollback
  path is measured in the same campaign.
- **DC profile.** Because of the gate's outcome, `dc-long-haul` either moved
  at stage 9 or still runs the legacy bundle. Run its campaign again on the
  candidate either way.

The flip passes only if every path × workload × cell of every candidate is
PASS. INCOMPLETE is not PASS. `ab_analyze.py --flip` judges a set of
campaign directories, prints that matrix, and exits non-zero unless it
passes. It also fails in three other cases: a required workload has no
campaign; a workload is missing on a path that another campaign measured;
or the campaigns did not all test the same reference and candidate builds,
in which case they are not one release:

```sh
./scripts/ab_analyze.py --flip ~/campaigns/rc-bulk ~/campaigns/rc-single ~/campaigns/rc-interactive \
    ~/campaigns/rc-llm ~/campaigns/rc-term ~/campaigns/rc-mixed
```

## Scenario workloads

The workload model groups applications by the network behaviour that
changes what the transport does. Phase A of it, needed before the stage 6
and 7 campaigns, is built:

| Workload | Cells (one per phase) | Measures (rules in `gates.ini`) |
|---|---|---|
| `llm` (W5, LLM streaming) | `<sequence>-<U>k-<background>`, for sequences single (cold connection per call), loop (5 turns on one connection, each turn's prompt 8 KB larger) and swarm (8 looping agents, staggered over 2 s); prompts of 100 and 400 KB; backgrounds none, down4 and up1 | TTFT net of think time, p50 and p95; delta lateness p99; stalls (gap ≥ 0.5 s) and freezes (≥ 2 s) per streamed minute; call failures; delta bytes sent INTERACTIVE |
| `term` (W6, SSH keystrokes with output bursts) | `term-<background>` | echo p99 before the first burst (phase A) and after it (phase C, leaving out keystrokes within 2 s of a burst); fraction of echoes over 1 s; the latch ratio p99(C) / p99(A) |
| `mixed` (M1, M2) | `m1-<U>k` (single calls under four downloads), `m2-<U>k` (under one upload) | the `llm` measures, a phase-A keystroke canary's echo p99, and the background's own bytes |

How these campaigns run:

- **The far side.** `scripts/validation/wlserver.py` runs on the gateway
  host's loopback, beside the HTTP origin, and the preflight reserves its
  ports. It provides:
  - a deterministic SSE emitter: it reads the prompt and measures its
    upload time `R`, thinks 500 ms, then sends 400 deltas, one every 25 ms;
  - a terminal echo with on-demand 300 KB output bursts;
  - an upload sink.

  Both ports use TLS: 1.3 where the host's ssl library allows it, otherwise
  1.2. Each call records which version it negotiated. TLS needs `openssl`
  on the gateway host, unless you set `tls = false`.
- **The client side.** `scripts/validation/wlclient.py` is copied to every
  client host, which must have `python3`. It records raw timings only; every
  measure is computed by `scripts/workloads/phase_a.py`. So definitions can
  change and old campaigns can be re-analysed.
- **Clock-free lateness.** Each delta carries the server's monotonic
  emission time `s_i`. The client records its receive time `r_i`. Lateness
  is `(r_i − s_i) − min_j (r_j − s_j)` per call, so the unknown clock offset
  cancels.
- **Backgrounds.** A background starts 5 s before the foreground and stops
  when it ends. Every one of its flows (four downloads, or one upload) must
  have moved bytes across at least 90% of the foreground's real span, in
  every trial. Otherwise that cell is unmeasured.
- **Evidence per trial.** Each trial must have its own records for a cell
  before the trials are pooled. A call the phase never reached counts as a
  failed call, and a keystroke whose echo never came is counted at the time
  waited, not dropped.
- **Pooling.** An arm's per-block statistic pools its two trials' raw
  samples before taking quantiles.
- **Provisional bounds.** Every bound comes from the workload model and is
  provisional until a pilot confirms it.
- **Two rules are expected to fail today.** "≥ 99% of delta bytes
  INTERACTIVE" needs the per-byte class telemetry the model recommends (R3).
  Until that telemetry exists, the measure is unmeasured and `llm` and
  `mixed` are INCOMPLETE. The terminal's latch ratio bound (≤ 1.15) assumes
  the recommended classifier change (R1).

`scripts/validation/scenario-campaign.example.ini` is an example.

## Adding a workload

A workload is a generator, an endpoint and a metrics extractor that travel
together, registered by name in `scripts/ab_workloads.py`. The built-ins are
`bulk`, `single`, `interactive` and `dc`. A new workload is a module under
`scripts/workloads/`, imported automatically, that defines four things:

- **Generators.** `generator(name, Generator(script, parse, expected))`.
  - `script(campaign, job, arm, duration, start_at)` returns the shell a
    client host runs through the arm's SOCKS5 client. It reaches its own
    `job.client.socks_port_base + arm.index`.
  - `parse(output, job, duration)` returns rows of
    `(flow, http, bytes, seconds, curl_exit, t_start, t_end)`, with
    millisecond times, for `trials.tsv`.
  - `expected(job)` is the size of a complete transfer.
- **A `Workload` subclass under `@register`.**
  - `phases(cfg, path)` returns the phases of one trial. Each `Phase` lists
    `Job`s: a generator on a client. A load job (`load=True`) runs for the
    window. Any other job measures under that load and stops when it ends.
    `barrier=True` starts the jobs on a shared remote clock.
  - `configure(cfg, cp)` reads the workload's own `[workload:<name>]`
    section; `self.params(cp)` returns it.
  - `positive(cfg)` lists the values that must be positive.
  - `objects(cfg)` lists the files the HTTP origin serves.
  - `paths(cfg)` and `clients_for(cfg, path)` choose which clients make a
    path.
  - `primary = False` marks a workload that has no byte outcome for the
    0.90 gate.
- **Endpoints.** `endpoints(cfg)` returns the services it needs on the
  gateway host's loopback, each an `Endpoint` with a command run from the
  work directory and its TCP and UDP ports. The preflight reserves those
  ports like any other, setup starts the service, and teardown stops it.
  Files listed in `files` are copied into the work directory first.
- **Metrics.** `extract(trial, plan)` returns the trial's measures as
  `"<phase>.<metric>"`. The analyzer judges each one against a rule in
  `gates.ini`, section `[workload:<name>]`, written as
  `phase.metric = rule value`. Each rule is a per-block contrast that must
  be positive at the one-sided 95% level by both the t and the bootstrap
  bound:

  | Rule | Contrast | For |
  |---|---|---|
  | `atleast R [A]` | `cand − (R × main − A)` | throughput |
  | `atmost R [A]` | `(R × main + A) − cand` | latency, "≤ main × R + A" |
  | `higher M` | `cand − (main − M)` | a rate near 1, such as completion |
  | `lower M` | `(main + M) − cand` | a rate or latency near 0 |
  | `floor V` | candidate ≥ V in every judged block | a hard assertion |
  | `ceiling V` | candidate ≤ V in every judged block | a hard assertion |

  A key `*.metric` applies to that measure in every phase. Every measure the
  workload reports must have a rule, unless the workload lists it in
  `report_only`. Every rule must find its measure in every phase that ran,
  with evidence in every judged block.
  Otherwise the verdict is INCOMPLETE. `block_extract(trials, plan)` gives
  an arm's per-block measures. By default it is the mean of `extract` over
  the arm's trials; a workload may pool their raw samples instead.
  A workload with no rules is never PASS. Mark new bounds provisional
  until a stage confirms them.

`workload = <name>` in the campaign config selects it. Flags the workload
sets on both ends (`owned_flags`, such as `dc`'s `--path-profile`) may not
be set in the config.

## Tests

```sh
python3 -m unittest discover -s scripts -p 'test_*.py'
```

CI runs these tests. They cover:

- **The statistics:** t quantiles against tables; the bootstrap against
  closed-form two-point and constant cases and against the t-interval; a
  false-pass rate under the null near 5%; the contrast, gate, void rule and
  sample size against known answers.
- **The analyzers:** synthetic campaigns whose transit, queue delay, loss and
  CPU are known.
- **The DC criteria:** a candidate that buys aggregate with unfairness fails,
  and an ancestor that never binds voids the campaign.
- **The runner:**
  - dry runs contact nothing;
  - the schedule's balance;
  - config rejections;
  - the refusal logic;
  - snapshot, spawn, stop and teardown, run against processes the test itself
    starts on the local machine.

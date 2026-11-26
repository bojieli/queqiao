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

**Live time is kept small.** The default is the minimal profile: four trial
types and the DC gate, with sequential stopping. It comes to about 2.5–3
hours per path, and `--dry-run` prints the estimate before anything runs.
The full matrix of workloads remains available as an opt-in. See "The
minimal profile" below.

**Where campaigns run.** Temporary gateways run on the two shared proxy
hosts (ssh aliases `proxy-a` and `proxy-b` in the examples), on their own
ports, beside each host's production `queqiaod.service`. The runner never
stops, restarts or reconfigures that service. Declare its unit, ports
(including the whole hop-port range) and paths in the host's
`[production:...]` section. The runner then refuses to start on any of
them, refuses any flag that names one of those paths, and checks the unit
is untouched before, during and after the campaign.

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
  gateway's whole hop-port range), production paths (its binary, state,
  config and logs), and a process pattern. No configured flag may name a
  production path, and no work directory may be inside one. Before anything
  is copied, the preflight resolves those paths, the work directory and
  every path a flag names on the host itself (symlinks, `..`, `//`, and a
  symlink as the last component). So a path that reaches production through
  a symlink is refused too. A path-valued flag must be absolute: a relative
  one (`a/b`, `../x`, `~/x`) is refused, since from the endpoint's working
  directory it could climb into production unchecked. The section must
  exist even when it lists nothing, so no host is touched without being looked
  at first.
- **Preflight is read-only, and it refuses.** Before anything is copied or
  started, the runner snapshots each host:
  - each unit's state, MainPID, NRestarts, start time and executable SHA-256.
    With `snapshot_sudo = true` in a `[production:...]` section, this
    observation of that section's units (and nothing else the campaign does)
    runs through `sudo -n`; a sudo that fails is an observation error.
    A running unit whose executable cannot be read or hashed is not
    observed: the preflight refuses, and during the run it is a change;
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
  while the job's ssh session was still open. Each job is a file run as
  `env QQ_AB_TOKEN=<token> bash <file>`, so the job's shell and every
  process it starts carry the token from exec on, and ownership is that
  token alone: a fixed-length random token, matched as the exact entry
  `QQ_AB_TOKEN=<token>` (a whole NUL-separated entry of
  `/proc/<pid>/environ` on Linux; a whole word of `ps -E`'s line where it
  shows environments). Teardown signals exactly the owners (TERM, then
  KILL), checks that none is left, and only then removes the job's record:
  only a verified absence retires it, and an inspection that fails leaves it
  in place, reported as unverified. A process whose environment cannot be
  read (not dumpable, or exiting) is skipped, except the job's own recorded
  process, which must be readable or the inspection fails. Where ownership cannot be established (a macOS host whose
  `ps` hides environments), nothing is signalled: the record stays and the
  teardown is reported as unverified, for manual cleanup. A record whose
  fields do not each check out is left alone; a `run/` directory that
  cannot be read and searched, or a failed check, is unverified, never
  clean.
- **Jobs are detached, and survive ssh drops.** A job is started under
  `setsid nohup`, keeps its output and exit status in `run/job-N.out` and
  `run/job-N.rc`, and the runner polls for them and fetches them over fresh
  ssh sessions (`ServerAliveInterval=10`), retrying a dropped session with
  backoff (5 tries). A dropped session costs nothing.
- **Control-plane failures void the block; workload failures are evidence.**
  Only two things are the harness rather than the arm: a job host that stays
  unreachable over ssh through every retry, and a job the runner itself
  killed for a reason it records (tearing down after an abort). Either voids
  the whole block for both arms (not evidence; it still counts toward the
  cap), and a trial type with more than `control_plane_void_max` (2) such
  blocks in a campaign has no verdict (`gates.ini [harness]`). Everything
  else is the arm's failure and stays evidence: a job that outlives its
  window (the runner stops it at the deadline and records a `timeout`), a job
  lost or silent while its host stays reachable, and the failures a job
  records itself -- a transfer cut or reset inside the tunnel (curl exit 18,
  56, ...), a failed session. Such a job's flows are failed rows and a
  measuring job without a record of its own gets a failed session, so the
  failure lowers that arm's completion and session measures. The report
  lists both kinds per block.
- **Overhead.** Every ssh session to a host shares one connection
  (ControlMaster, sockets under the campaign directory), the arms' gateways
  restart together, scrapes and CPU samplers run in parallel, jobs are
  polled every second, and a binary already on a host with the same SHA-256
  is not copied again. `timing.tsv` in the campaign directory records every
  session's wall time, for profiling the overhead between windows.
- **A relay hop is declared, and temporary.** Where production clients
  reach their gateway through a relay, a `[relay]` section (`host`,
  `address`, optional `platform` and `flags`) puts the same hop in the
  measured path: per arm the runner starts `queqiaod relay` (the main arm's
  build, for both arms) on the relay host on that arm's gateway port,
  forwarding TCP and UDP to the gateway's same port, and clients enroll with
  and dial the relay's address. The relay host needs its own
  `[production:...]` section; its ports are reserved and checked like every
  other, its relays are stopped by teardown, and with `host_cpu` its CPU and
  production's are sampled too.
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
| 6 | `origin/main` | `--cc-api=v2 --sched=class --congestion-scope=path` | works with isolation on |
| 7 | `--congestion-scope=path` with isolation | `--congestion-scope=path`, and `--topology=single` on the client only (`client_flags`) | the topology test of 15.3: the reference arm is *not* main |
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
unmeasured.

Sender-side credit delay p95 comes from every endpoint's
`queqiao_credit_delay_seconds` histogram, scraped before and after each
trial. Each endpoint times what it sends over a QUIC stream: from a chunk's
last byte being taken by the carrier to the flow acknowledgement that covers
it, minus the path's minimum round trip as the QUIC transport measures it
(whatever congestion controller runs, and only once the path has a
round-trip sample), one chunk per flow at a time. Only an original
transmission starts a sample, judged by the byte ranges carriers have
already taken rather than by order, since lanes finish out of order; a
re-issue can complete a sample but never starts one, so a repair is timed
from the first take. Writes before OPEN_OK are not timed. A scrape must carry
every exported bucket. An
arm's block p95 is taken over the histograms of its trials, gateway and
client, pooled, never as a mean of per-trial percentiles; a block with fewer
than 20 samples per arm is pooled over blocks like any sparse tail. A build
without the histogram, or a scrape that is truncated or went back (a
restart), leaves it unmeasured.

The shadow false-promotion rate is, from the same scrapes, the
carrier-shadow observer's resolved no-progress episodes that reached its
`k_p` (the promotions the design would have made) and the share of them
that then progressed again, so that the promotion would have been in vain.
Each episode is counted once, when it is resolved, in the numerator and the
denominator together. It is the calibration rate at `n = k_p` that
`carrier_shadow_summary.py` prints from the logs. Promotions are rare, so
the rate is pooled over every block's promotions and tested by resampling
blocks; an arm with no promotion at all has a zero denominator, and the rate
is reported, not gated (design section 15.4). A block resample that leaves
out every block with an arm's promotions has no rate; such draws count
against both bounds rather than being dropped, so a rate seen in one block
is never certain. With `k_p = 1` the summary script cannot count a carrier
still open at capture end (its one-expiry episodes are in no log line) and
reports the rate as unmeasured; the analyzer reads the `/metrics` counters,
which count them. It needs `--carrier-shadow` on
every endpoint of both arms, as each endpoint's runtime configuration
record shows; otherwise it is unmeasured. Both read the existing
`[secondary]` bounds in `gates.ini`, which is unchanged, so campaigns that
froze it keep their verdicts.

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

Section 11.3 keeps `dc-long-haul` on the legacy bundle by default through the
migration. Since stage 9 the profile also starts on the whole new architecture
(`--cc-api=v2 --sched=class --congestion-scope=path --coded-retry=sticky
--recovery=carrier`, `--topology=single` on clients, optionally
`--classifier=phased --mem=owned --gateway-admission=owned`), which is the
candidate in both DC example campaigns; the daemon refuses any bundle in
between. The default moves only if the profile passes its own gate on the
new architecture. The bounds are in `scripts/validation/dc_gate.ini`. They are
**frozen**: section 11.3 and the round-5 review both require them to be fixed
before the campaign runs. A change must land in its own commit before any
stage 9 data exists.

### The workload

A leaf is an endpoint pair, and the gateway is the sender for downloads, so
each client host is one leaf under the gateway's shared egress. Under the
candidate the gateway's authorities sit in its tree as knowledge only. The
legacy gateway has no tree at all: its per-peer controllers are independent,
and the legacy tree's joint limit couples only clients in one process. So
this campaign compares one authority per leaf with independent legacy
controllers. The joint limit itself is exercised by the deterministic test
`TestDCMultiLeafFairnessHoldsWithAKnowledgeOnlyTree` (uploads, several
clients in one process), which computes the same contrasts with the same
bounds. The DC campaign
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
  (*w* − 1) × `request_timeout` on every leaf. A W4 client (`wlclient rpc`)
  is given the barrier's absolute time, starts at once and waits for the
  barrier after initialising, so the leaf hosts' interpreter start-up times
  do not shift their waves. The analysis requires every
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

Each endpoint's bundle comes from the runtime configuration record it logs
at every start. Every candidate endpoint must have run the new-architecture
bundle (clients with `topology=single`), and every legacy endpoint must have
run without `cc-api=v2`. Otherwise the campaign compared something else and
is VOID. If an endpoint recorded no bundle, there is no verdict.

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
- **Paths and workloads.** Every path in the field campaign, with the
  minimal profile: trial types A, B, C and D (`minimal`, and its
  `idle-wake` subdirectory) on every path, and the DC gate. `--profile full`
  instead requires the full matrix (`bulk`, `single`, `interactive`, `llm`,
  `term`, `mixed` and `dc`).
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

The flip passes only if every path × trial type of every candidate is PASS
(design section 15.4: A, B, C, D and DC on every path). INCOMPLETE is not
PASS. `ab_analyze.py --flip` judges a set of campaign directories, prints
that matrix, and exits non-zero unless it passes. Coverage is checked before
any verdict, cell by cell: every candidate must have every required cell on
every path. A DC campaign alone covers nothing else.

Each trial type runs once per path, at its gate (design section 15.3): A
and B at stages 6 and 7, DC at stage 9, C and D at stage 10. A pass carries
forward to the flip; a trial is rerun on the final bundle only when a later
stage changes what it exercises. The flip takes one final build per
candidate arm, across every path: what it ran in the newest campaign (its
build: commits, or each prebuilt binary's SHA-256 by role and platform; and
its arm flags). On each path the result must also have that path's final
endpoint switches (`[gateway]` flags and that path's `[client:...]` flags,
from the newest campaign on the final build there). A
cell measured on the final bundle counts as it is. A cell measured only on
an earlier build or flag bundle counts only when you name it in `--carry`,
your statement that no later stage changed what it exercises; the matrix
marks it "carried forward from" its campaign. Otherwise the flip lists it
under "Not on the final bundle". An arm that is the reference in one
campaign and a candidate in another is inconsistent and fails the flip.

```sh
./scripts/ab_analyze.py --flip --carry bulk,mixed-67,dc-gate \
    ~/campaigns/s67-a ~/campaigns/s67-b ~/campaigns/s9-dc \
    ~/campaigns/rc-a ~/campaigns/rc-a/idle-wake ~/campaigns/rc-b ~/campaigns/rc-b/idle-wake
```

## The minimal profile

The model's minimal live plan (`docs/WORKLOAD-MODEL.md`, design section
15.3) gates on four trial types and the DC gate, about 2.5 to 3 hours of
live time per path. Every setting is fixed by that plan; the harness has no
knob for them. Each trial type is a block of its own: four trials in ABBA or
BAAB order with every endpoint restarted before the block, so each arm's
first trial in it is cold. The types run round-robin at block granularity
(every type's block b before any type's block b + 1), so a path that drifts
drifts under all of them. Within a trial every workload's metrics are judged
separately, so a bulk gain cannot hide a regression in call jitter or page
time.

| Trial | Cell | What runs | Length | Gate |
|---|---|---|---|---|
| A | `minimal` / `bulk` | four downloads | 60 s | stages 6 and 7 |
| B | `minimal` / `mixed-67` | a 5-turn LLM loop on one connection (100 KB prompt plus 8 KB per turn, 2 s apart) and keystrokes with one 300 KB burst at 40 s, under two downloads that start 5 s earlier | 90 s | stages 6 and 7 |
| DC | `dc` / `dc-gate` | the stage 9 gate at minimal size: 10 transactional requests one at a time, 2 s apart, ASR (355 KB up, 200 B back) and TTS (300 B up, 300 KB back) alternating, the first on a cold app connection, then 40 s of the sustained-goodput and multi-leaf fairness workload (`dc-minimal.example.ini`) | 60 s | stage 9 |
| C | `minimal` / `mixed-all` | at once: a video call for the whole trial (audio and about 3 Mbit/s of video each way, open loop, through UDP ASSOCIATE), one h2 page on cold connections at 20 s, one LLM call (100 KB) at 50 s, keystrokes without bursts, one download and one upload | 120 s | stage 10 |
| D | `idle-wake` / `wake` | 50 connections per arm after a 200 KB sync, no heartbeat; each woken once at 1, 5 or 16 minutes (17, 17 and 16), push and send alternating, then 5 warm follow-ups | about 17 min | stage 10 |

Choose the types a campaign runs with `[workload:minimal] trials`:
`bulk, mixed-67` at stages 6 and 7, `mixed-all` at stage 10.

What each cell gates (`gates.ini [workload:minimal]`, provisional bounds
from the model):

- **Primaries.** Only the bulk flows' bytes: A's downloads (over the
  trial's fixed 60 s); B's downloads; C's download and upload, each its own
  primary; DC's sustained phase. B's and C's are the bytes the background
  flows moved inside the foreground's window: each flow adds to a counter
  after every chunk of at most 64 KiB (a download's only for a 200
  response; an upload's as handed to the tunnel), a sampler records the
  counter (with its time, read together) every 250 ms whatever the I/O is
  doing, and the window's bytes are those of the sample intervals lying
  wholly inside it: conservative, never interpolated, never crediting a
  byte from outside. An
  upload's timeline ends at its transfer's end, before any receipt is
  fetched. The same timelines show that every flow was running across the
  window; a flow with no qualifying bytes fails that check. Each
  is `atleast 0.90` of main, and is void
  (rerun) when more than the pre-registered fraction of main's blocks
  delivered nothing.
- **Generic secondaries** (`[secondary]`), as rules of each trial type
  where defined: stalls, reissued bytes, queue p90 and client CPU in every
  type; flow completion and wire bytes per useful byte in A, whose bulk
  flows define them. Credit delay p95 and the shadow false-promotion rate
  are rules of every trial type in a stage-10 campaign, and are reported
  without being gated at stages 6 and 7. `[workload:minimal] stage` sets the
  stage: `auto`, the default, is 10 when the campaign runs C and 7
  otherwise; C cannot run below stage 10. Run every endpoint of both arms
  with `--carrier-shadow` at stage 10, or the false-promotion rate is
  unmeasured and the cell is INCOMPLETE. A cell from a campaign below stage
  10 never stands as the final bundle's own evidence at the flip: it counts
  only when carried forward (`--carry`), so a rerun on the final bundle
  sets `stage = 10`. D is judged on its own rules only (design section
  15.4).
- **B:** the LLM's TTFT p50 and p95, delta lateness p99, stalls and
  freezes per streamed minute, call failures, and class correctness (at
  least 99 % of the response's DATA bytes sent INTERACTIVE, from the
  gateway's per-flow class telemetry; a hard assertion); keystroke echo p99
  and the fraction over 1 s before the burst and after it, and the latch
  ratio (at most 1.15, absolute).
- **C:** the same LLM measures; keystroke echo p99 and the fraction over
  1 s (echo time alone, no bursts); the call per direction: audio late
  loss, frame loss, freezes per minute and lateness p99 (a packet is bad in
  a direction when it is lost there or more than 150 ms late). Each
  direction is its own open-loop stream on its own clock: the far side
  starts its downlink when the call's first packet names the call and its
  return address, and uplink arrivals never pace it; its report gives the
  uplink's arrivals and the downlink's sends. The call ends, for C's
  background window, with its media (the uplink's last send or the
  downlink's last arrival), not with the report's fetches, which cross the
  same SOCKS path afterwards; the page load time p50 and p95 (from the first CONNECT)
  and failed objects.
- **D** (`gates.ini [workload:idle-wake]`). The idle interval starts when
  the client has the whole 200 KB sync and acknowledges it, for pushes and
  sends alike; the sync's own duration is reported. Gated: no silent deaths and every wake
  at 1 and 5 minutes delivered within 5 s (hard); the delivered count at
  16 minutes at least main's (a count); the push and send wake penalty p50
  within the model's bound of main's (an unpaired bootstrap over the
  session's wakes). The p95, max, death modes (EOF, reset, a send's 30 s
  timeout, a push never seen) and idle EOF times are reported.

A tail percentile is gated per block only when the block holds at least 20
samples per arm for a p95 and 100 for a p99. Otherwise (B's and C's TTFT
p95, C's page load p95) it is computed over every block's samples pooled,
at the stopping look, and tested with a bootstrap that resamples blocks.

**D beside the blocks.** D barely contends after its sync, so its arms run
at once as paired sessions, each on its own gateway and client instances
(`<arm>.wake`, a name no arm can take, on the ports after the arms'), which the blocks' restarts do
not touch. The campaign starts D first and waits until every connection has
synced (if they have not within two minutes, the campaign aborts before any
block), then runs C's first two blocks back to back to cover D's
17 minutes, so its wakes happen under the path's load. With
`idle_wake = auto` (the default) D runs when the campaign has C; `true` runs
it beside A and B as well (the `classifier=v2` gate). It is written as a
campaign of its own in `<campaign>/idle-wake`, judged by `ab_analyze.py`
like any other. On a small client host D's two idle client instances need
`idle_wake_on_small = true`. A campaign of D alone is refused: its arms
would run one after the other. A campaign with D has one path: blocks load
one path at a time, so on a second path D's wakes would fall while that path
is idle. Run one campaign per path.

**Sequential stopping** (design section 15.4; `sequential = true`). Each
trial type starts with 6 blocks per path. After block 6 and after each
further block, that type's one-sided tests are re-run at the look's
nominal level, from the Lan–DeMets Pocock-type α-spending function
α ln(1 + (e − 1) t) over the 7 possible looks (t = blocks / 12) with total
one-sided α = 0.05. The levels are computed before the first block and
frozen in `plan.json` (look 6: 0.031, then about 0.016 to 0.019; the dry run
prints them). `spending = bonferroni` uses 0.05/7 at every look instead. At
a look a trial type:

- **passes** when the lower bound of every contrast is above zero (the t
  and the bootstrap bound both) and every hard assertion holds;
- **fails** when a hard assertion fails, or when the upper bound of any
  contrast is below zero (either bound);
- otherwise gets one more block. At the 12-block cap a type that has not
  passed fails.

Each trial type stops on its own; the others continue. Coverage is counted
against the blocks a type ran, and a cell passes only when every one of its
rules passed: undecided or unmeasured leaves it INCOMPLETE. `plan.json`
freezes the SHA-256 of `gates.ini` (and, for a DC campaign, of
`dc_gate.ini`); the analyzers, and the flip, give no verdict under another
file. The flip compares each path's results with that path's own final
client switches. `status.json`
records the units run (block, path, type), why each type stopped, and the
wall time. The analyzer judges each type at its last look, and its tables
show the bounds at that look's level. Sequential mode is only for workloads
judged entirely by their own rules, such as `minimal`; the primary gate
with its `[secondary]` bounds (`bulk`, `single`, `interactive`), D and the
DC gate keep a fixed number of blocks.

**Contention** (`[campaign] host_cpu = true`). Through every trial a
read-only sampler (a job, torn down with the rest) records each loaded host's
CPU from `/proc/stat` (steal included), its load, and production's own CPU
(its units' processes, from `/proc/<pid>/stat`) every 2 s. A block is void
when, in any of its trials on a loaded host, production took more than 20 %
of the host's CPU, the hypervisor stole more than 10 % of it (a neighbour
contending, which hits the arms unpredictably), or the host's or
production's CPU record is missing (`gates.ini [contention]`, frozen with
the gates). A void block counted toward the cap but is not evidence; the
report lists each one with its reason. The test's own load is not
contention of production and voids nothing: on these small hosts a bulk
trial may hold the CPU near 100 %. The report shows each arm's mean host
CPU, the share of its windows at or above 90 %, production's share and the
steal, void blocks included, and flags an arm as CPU-bound when, in more
than half its trials, a host was at or above 90 % for most of the window:
its bulk numbers may then be limited by the CPU rather than the network.

**Cost.** One block is four trials plus about a minute of restarts, so a
block of A takes about 5 minutes, B 7, C 9 and DC 5: about 26 minutes per
block per path for all four, 2.6 hours at 6 blocks and 5.2 hours at the
12-block cap (design section 15.4). D adds no wall time. `--dry-run`
prints the per-type block time, the campaign's estimate, the look levels,
and the run order. DC calibrates each loaded leaf alone in its first block
only (`calibrate = first`), and that block's solo rates stand for every
block's ancestor-binding check and leaf identities; every block's carrier
and leaf evidence is still checked. With 30 requests per arm in a trial,
DC's request p99 is pooled over blocks and tested by resampling blocks.
Every request and control percentile of an arm's block is taken over the
block's pooled samples. Each DC request has at most `request_timeout`
seconds and counts as completed only with the full ASR or TTS answer; the
job's cap allows every request its timeout, so a candidate that fails them
all is measured, not cut off.

**Optional workloads.** The rest of the workload model is available but is
not a gate:

- `llm`, `term` and `mixed` with every variant;
- the swarm (M3) and the soak (M7);
- the entries the model moves to deterministic tests (W8, W9b, W10, W11,
  M5), whose live forms are not built.

`ab_analyze.py --flip --profile full` judges the full matrix instead of
the minimal one.

## Impaired networks

`workload = impaired` (`scripts/workloads/impaired.py`, `scripts/ab_impair.py`,
`validation/impaired-campaign.example.ini`) measures reliability rather than
throughput: the mixed foreground under slow, lossy and broken networks, with
the impairment applied by tc/netem on the path's client host (`[impair]`,
which needs root there). One path per campaign; any number of arms; one trial
per arm per block, the arms in random order.

**Scenarios** (frozen in `ab_impair.SCENARIOS` and recorded in `plan.json`;
round-trip delay, split evenly between the directions; loss and rate per
direction):

| trial type | scenario | trial |
|---|---|---:|
| `slow-lossy` | 5 Mbit/s, +250 ms, 20 ms jitter, Gilbert-Elliott bursty loss (p 1 %, r 30 %: 3.2 % average) | 90 s |
| `blackhole` | 20 Mbit/s, +150 ms; 100 % drop for 8 s at 30 s and for 25 s at 70 s | 120 s |
| `policed` | a 4 Mbit/s token-bucket policer with a 12 KB bucket, +150 ms | 90 s |
| D, `idle-wake-lossy` | 2 % random loss, +200 ms, on D's own ports for its whole session | about 17 min, beside the blocks |

Every trial runs the W5 LLM loop (5 turns), W6 keystrokes and one download
of 4 MiB objects for the whole window; `blackhole` adds a long upload, and
probes every 0.25 s (each its own process) that time the recovery.

**Gates** (`gates.ini [workload:impaired]` and the `[secondary]` bounds, each
one-sided against main): flow completion, stalls per trial, LLM call
failures, TTFT p99, keystroke echo p99, silent deaths (an LLM call or
keystroke session that ends in the client's own timeout: none, in every
block), and for `blackhole` the recovery time (end of each outage to the
first probe completed after it, the worse of the two). D is judged as its own
campaign in `<campaign>/idle-wake` by `[workload:idle-wake]`. Goodput is
reported only.

**How the impairment is applied.**

- A `clsact` qdisc on the interface that carries the route to the address
  clients dial adds classification hooks; the interface's root qdisc is never
  replaced, so production traffic keeps its queueing.
- Flower filters match only IPv4 to (egress) or from (ingress) that address,
  UDP and TCP, with the remote port in the campaign's arm ports (D's own
  ports for D). They redirect to the campaign's own IFB devices
  (`qqimp-<set><u|d>`), each with the direction's netem; a policer is an
  action on the same filters.
- The blocks' set is applied just before each trial and removed right after
  it, on every exit; a `blackhole` trial's outages are a harness job on the
  client host that changes only those IFB devices' netem, with a trap that
  restores the base on any exit, and prints when each outage began and ended
  (the probes' clock). A failure of that job is the control plane's.

**Safety.**

- Preflight refuses when a filter could match production: a filter port
  declared production on any host, a filter port inside either end's
  ephemeral port range, or a remote address that is not an IPv4 literal. It
  also refuses when the interface already has a clsact or ingress qdisc, a
  device with the campaign's IFB names exists, the route to the remote leaves
  by another interface, or the host is not root. Each apply checks the
  production ports again.
- Before the first trial it snapshots every interface's qdiscs, classes and
  filters and the interface list (`impair-before.txt`). After every removal
  that leaves no set applied, and at teardown, the state must equal the
  snapshot: a difference fails the campaign loudly (a teardown leftover;
  `status.json` `impairment: not restored`).
- Each applied set has a watchdog on the host that removes it after its
  deadline (the trial's cap plus `watchdog_slack`, 300 s) even if the runner
  or its connection dies. A trial whose watchdog fired is void (control
  plane). A failed apply removes whatever it had applied.
- Teardown removes every set before stopping any process. The one host-wide
  effect is loading the `ifb` module (with no default devices), which is
  left loaded.

## Scenario workloads

The workload model groups applications by the network behaviour that
changes what the transport does. Phase A of it, needed before the stage 6
and 7 campaigns, is built:

| Workload | Cells (one per phase) | Measures (rules in `gates.ini`) |
|---|---|---|
| `llm` (W5, LLM streaming) | `<sequence>-<U>k-<background>`, for sequences single (cold connection per call), loop (5 turns on one connection, each turn's prompt 8 KB larger) and swarm (8 looping agents, staggered over 2 s); prompts of 100 and 400 KB; backgrounds none, down4 and up1 | TTFT net of think time, p50 and p95; delta lateness p99; stalls (gap ≥ 0.5 s) and freezes (≥ 2 s) per streamed minute; call failures; delta bytes sent INTERACTIVE |
| `term` (W6, SSH keystrokes with output bursts) | `term-<background>` | echo p99 before the first burst (phase A) and after it (phase C, leaving out keystrokes within 2 s of a burst); fraction of echoes over 1 s; the latch ratio p99(C) / p99(A) |
| `mixed` (M1, M2) | `m1-<U>k` (single calls under four downloads), `m2-<U>k` (under one upload) | the `llm` measures, a phase-A keystroke canary's echo p99, and the background's own rate over the foreground's window |

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
- **Background rate.** The background's bytes are counted within the
  foreground's own window, each transfer's bytes prorated over its interval,
  and judged as a rate. So arms whose calls take different times compare
  like with like. An upload that the window cuts counts the bytes the far
  side's sink received for it (`/sink-report`). If that cannot be learned,
  the trial's background rate is unmeasured.
- **TTFT.** It is taken when the first byte of the first event's payload
  arrives. Each delta's receive time is when that delta is complete.
- **Evidence per trial.** Each trial must have its own records for a cell
  before the trials are pooled. A call the phase never reached counts as a
  failed call, and a keystroke whose echo never came is counted at the time
  waited, not dropped. A terminal write that fails ends the session as a
  failure. A canary counts only if it measured echoes.
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

`scripts/test_argv_smoke.py` starts every binary role a campaign uses (each
arm's gateway and client, and the relay hop) with the runner's exact
command line for about a second on scratch ports on the machine running the
tests, after a real provider init and enrollment. It builds `cmd/queqiaod`
(skipped without Go) and covers the example configs built from
`origin/main`; `QQ_SMOKE_CONFIG=<config> python3 -m unittest test_argv_smoke`
checks a live campaign's own config before it starts, so a wrong subcommand
or flag fails there rather than on production hosts.

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

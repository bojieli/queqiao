# Runtime logging

Both `queqiaod client` and `queqiaod server` create a structured runtime log
by default. The log is the durable operational record; stderr is a second copy
for an interactive terminal or service journal.

## Find the active log

```sh
queqiaod logs
queqiaod logs client
queqiaod logs server
```

The command prints the absolute default path, whether it exists, its current
size, and a command that follows it. The default files are separate:

| Platform | Client | Server when run interactively |
| --- | --- | --- |
| macOS | `~/Library/Logs/Queqiao/client.log` | `~/Library/Logs/Queqiao/server.log` |
| Linux | `${XDG_STATE_HOME:-~/.local/state}/queqiao/client.log` | `${XDG_STATE_HOME:-~/.local/state}/queqiao/server.log` |
| Windows | `%LOCALAPPDATA%\Queqiao\Logs\client.log` | `%LOCALAPPDATA%\Queqiao\Logs\server.log` |

The production systemd unit sets `QUEQIAO_LOG_DIR=/var/log/queqiao`, so its
server log is `/var/log/queqiao/server.log`. The macOS LaunchAgent template
sets the client file explicitly to the macOS path above.

An explicit `--log-file` always wins. Relative paths are converted to absolute
paths at startup, `~/` is expanded, and the resolved path is recorded in the
first `runtime logging initialized` entry. The parent directory is created if
needed.

Read the current and rotated files with ordinary tools:

```sh
tail -n 200 -f ~/Library/Logs/Queqiao/client.log
tail -n 200 /var/log/queqiao/server.log
ls -lh /var/log/queqiao/server.log*
```

The visualizer's **Select log files…** action accepts the active file and any
rotated `.1`, `.2`, and later files together.

The system service owns its `0600` server log, so a desktop browser normally
cannot open it directly. Copy only the evidence you need into your current
user's private directory; do not make the service log world-readable:

```sh
mkdir -m 0700 ./queqiao-log-review
sudo install -m 0600 -o "$(id -u)" -g "$(id -g)" \
  /var/log/queqiao/server.log ./queqiao-log-review/server.log
```

Then choose `queqiao-log-review/server.log` in the visualizer and remove the
review copy when the investigation is finished.

## Defaults and controls

The production defaults are:

- JSON Lines format, one complete object per line;
- level `info`;
- a 32 MiB active file;
- five rotated backups (`client.log.1` through `client.log.5`, likewise for
  the server);
- mode `0600` for active and newly rotated files;
- a performance snapshot every five seconds while flows are active or state
  changes; and
- a second copy on stderr for a terminal or the service journal.

Both client and server accept the same flags:

```text
--log-file PATH                 auto, an explicit path, or none
--log-format json|text          json by default
--log-level debug|info|warn|error
--log-stderr=true|false         mirror to stderr/service journal
--log-max-size-mib 32
--log-max-backups 5
--telemetry-log-interval 5s     0 disables periodic snapshots
```

`--json-logs` remains as a deprecated compatibility alias for
`--log-format=json`. `--log-file=none` is an explicit container/console mode
and is rejected unless stderr logging remains enabled.

For finer profiling, use `--telemetry-log-interval 1s`. Values below one
second are rejected to prevent an accidental log flood. A level above `info`
also suppresses performance snapshots, so `info` or `debug` is required for
dashboard time series.

## What a runtime log contains

Every entry includes `time`, `level`, `msg`, `service`, `role`, and `pid`.
Startup records also identify the build, wire protocol, resolved log path,
format, retention, telemetry interval, and a non-secret snapshot of transport,
congestion, framing, timeout, pooling, and admission settings. Shutdown
failures are written to the file before it is closed.

Performance records use `msg="performance snapshot"`, `type="metrics"`, and
`telemetry_schema=1`. Their flat `queqiao_*` fields intentionally match the
Prometheus `/metrics` names. They cover:

- active/started/completed/failed flows and transferred bytes;
- latest, smoothed, and controller-minimum RTT;
- QUIC sent and received bytes and packets, and two loss counters that are now
  the same number derived two ways: `queqiao_quic_loss_observed_packets_total`
  is every loss the sender detected, and `queqiao_quic_controller_packets_lost`
  is what the congestion controller was charged. Nothing is withheld from the
  controller any more, so they agree; they are both kept so that a divergence
  is visible rather than silent. Divide either by `queqiao_quic_packets_sent`
  for a loss rate;
- delivery, ACK, send, pacing, and maximum-bandwidth estimates;
- congestion window, bytes in flight, controller round/mode/recovery;
- lanes, failures, replacements, reinjections, fallbacks, and timeouts;
- transient local UDP send errors absorbed into QUIC loss recovery;
- flow telemetry entries expired because nothing refreshed them, which is how
  a round-trip aggregate frozen at a stale constant announces itself;
- lane joins refused and account flow opens refused, each split by reason;
- the flow-stall watchdog and its parallel rescue:
  `queqiao_flow_stalls_detected_total` counts stall episodes (unacknowledged transport work pending
  with no forward progress for three minimum round trips; application response
  silence alone is not a stall),
  `queqiao_stall_spare_attaches_total` counts stalls where an already-healthy
  lane took over at once, `queqiao_lane_rescue_attempts_total` counts
  individual dial+JOIN attempts and
  `queqiao_lane_rescue_wins_total{attempt="N"}` which attempt of a round
  produced the lane the flow kept -- a win above zero is an independent QUIC
  dial beating the shared QUIC strategy; attempt zero also counts the
  sequential TCP fallback after the QUIC race fails. TCP JOINs never race
  QUIC JOINs because admission immediately retires QUIC on the gateway, and
  `queqiao_lane_grace_extensions_total` counts replacement graces a gateway
  restarted because a rescue JOIN arrived while the flow was still waiting;
  and
- the erasure the path is measured to be applying to the direction this
  endpoint sends into, published as `queqiao_erasure_ratio{direction="send"}`
  and as `queqiao_erasure_ratio_send` in the log. A gateway's send direction is
  its downstream. This is what a code is sized from;
- the shape of the delivery-rate samples the bandwidth estimate is built from:
  `queqiao_quic_sample_mean_bytes_per_second`,
  `queqiao_quic_sample_max_bytes_per_second`, and the
  `..._max_delivered_bytes` and `..._max_interval_seconds` behind that widest
  sample. The estimate is a maximum over these samples, and a maximum alone
  cannot be read: a rate is high either because the path is fast or because the
  window it was measured over was short. A maximum far above the mean is a tail
  rather than the path, and a tail measured over a short interval is a
  measurement artefact rather than either;
- how much of the sending rate the delay bound is removing, as
  `queqiao_delay_brake_ratio`. It is non-zero only while the path is carrying
  more than one bandwidth-delay product of queue, and it separates a rate held
  back by the path's own queue from one that simply measured less;
- the receive direction, measured by this endpoint's decoders rather than
  inferred from acknowledgements: `queqiao_coded_symbols_total` split by
  outcome (`arrived`, `recovered`, `lost`), with
  `queqiao_erasure_ratio{direction="receive"}` and
  `queqiao_erasure_residual_ratio{direction="receive"}` derived from them. Every
  source symbol the peer sent ends in exactly one outcome, so the three are a
  denominator and the ratios are counters over counters rather than a mean of
  per-flow ratios. The residual is what the code could not repair and the
  session re-issues a round trip later; and
- sampler diagnostics and class transitions.

Flow completion records also carry what the flow's lane replacements did:
`lane_replacement_waits`, `lane_replacement_timeouts`, `lane_replacement_wait`,
and `lanes_joined`. They are written on every flow record, not only on
failures, because a replacement that succeeded is the control case for one that
did not. A client flow counts its initial lane in `lanes_joined`; an established flow
with no subsequent admission reports one. Both
endpoints use the same names, so a client record and a gateway record about the
same failure can be read side by side.

`lane_replacement_attempts` and `lane_replacement_failures` say what the
client did after losing its last lane. A QUIC attempt counts a recovery round,
which may race several JOINs. TCP counts each replacement it launches. A round
that returns an error increments failures. A difference between attempts and
failures can therefore be a successful replacement or an outstanding attempt
at the time the flow record was written. These counters exclude bundle
widening and rescue of a suspected lane that is still open.

These two counters, and `lane_replacement_last_failure`, are client-only in
meaning. The gateway never opens a replacement lane, so its
`remote flow ended with error` and `remote flow complete` records always carry
`lane_replacement_attempts=0` and `lane_replacement_failures=0`. The fields are
written there so both endpoints share one schema, and a gateway's zero says
nothing about whether the client tried. Do not bucket gateway records by them:
a gateway-side split puts every failure in "nothing was attempted". The
gateway's own fields (`lane_replacement_waits`, `_timeouts`, `_wait`,
`lanes_joined`) do describe the gateway's side of the outage.

When a client has recorded a failed zero-lane recovery, its flow completion
record includes `lane_replacement_last_failure`. The value classifies the last
returned QUIC round or TCP replacement error: `timeout`, `peer_rejected`,
`peer_capacity`, `canceled`, or `other`. This lets you split counted errors
even when the attempt timed out without its own warning. A later successful or
pending attempt does not clear this historical observation. Older clients and
flows with no recorded failure omit the field. The category describes the
returned error; it does not identify which physical path caused a timeout.

To compare the no-admission failures from issue #53 on a fresh client window,
use the client's JSON-lines log. This groups flows by observable outcome and
preserves records from older clients without counting missing counters as zero:

```sh
jq -nr '
  reduce (inputs | select(.msg == "local flow ended with error" and
    ((.error // "") | contains("lane replacement timeout")) and
    .lanes_joined == 1)) as $f ({};
    (if ($f.lane_replacement_attempts == null or $f.lane_replacement_failures == null)
     then "counters missing"
     elif $f.lane_replacement_attempts == 0 then "no zero-lane attempt"
     elif $f.lane_replacement_attempts == $f.lane_replacement_failures
     then "all attempts returned errors"
     else "inspect attempt outcomes" end) as $group |
    .[$group] += 1)
' client.jsonl
```

Correlate the original client records with the per-attempt lines by
`flow_id` and time window. A QUIC zero-lane recovery round that returns an
error writes `msg="lane recovery unavailable"`; a TCP replacement join that
fails writes `msg="TCP bundle lane unavailable"`. Both carry `flow_id`,
`attempt`, and `failure_kind`, which uses the same categories as
`lane_replacement_last_failure`. `attempt` is the flow's replacement ordinal,
so the highest one logged for a flow is at most its
`lane_replacement_attempts`. The TCP line also carries `replacement`: a join
that widened a healthy bundle has `replacement=false` and no `attempt`, and is
not counted in the flow record. Every counted failure gets a line, including
a peer refusal that ends recovery at once. The level follows `failure_kind`: `timeout`
(a context deadline or a socket deadline on the handshake), `peer_rejected`
and `peer_capacity` are written at `info`, and `other` at `warn`. A replacement
cancelled because the flow ended is not written, and any other cancellation is
written only at `debug`. A failed widening join is written at `warn` with the
bundle's back-off count as `failures`, and only when it counts against that
back-off. Clients before this change skipped deadline errors and refusals on
the QUIC line, wrote no TCP line for refusals, and wrote the TCP line without
`flow_id`. An error can be a timeout, dial failure, or peer refusal;
the counts alone do not identify a keepalive or path failure. Record build IDs,
window duration, and total completed and failed flows alongside the groups
before comparing windows.

`scripts/lane_replacement_split.py` does the same selection with the finer
split issue #53 needs, and also reads gateway logs for context. It needs only
Python 3 and reads rotated files in any order:

```sh
./scripts/lane_replacement_split.py client.jsonl client.jsonl.1 \
    --gateway server.jsonl --gateway server.jsonl.1    # add --json for a machine-readable report
```

It selects `local flow ended with error` records whose error contains
`lane replacement timeout` and whose `lanes_joined` is 1, and buckets them:

- `no zero-lane replacement attempted (attempts == 0)`: the client launched
  no counted replacement after losing its last lane. This is not by itself
  proof that the pool would not rebuild. The TCP bundle manager, for one, does
  not launch a zero-lane replacement while a join it started earlier (for
  example to widen the bundle) is still pending, and that join is not counted.
- `every attempt failed (attempts == failures)`, sub-split by
  `lane_replacement_last_failure` (`every_attempt_failed_by_last_failure` in
  `--json`). Only `timeout` suggests the path did not carry a handshake.
  `peer_rejected` and `peer_capacity` prove the peer answered the handshake and
  refused it, and `canceled` and `other` need the per-attempt lines.
- `attempt outcome unrecorded (attempts > failures)`: at least one counted
  attempt left neither a failure nor an admitted lane. That can be a dial still
  outstanding when the record was written. It can also be a TCP join whose
  result was discarded because the manager stopped first, or a successful dial
  whose lane was rejected because the flow had already ended. The outcome is
  unknown, and it is not a refusal.
- `old client (fields missing)`: a record without the attempt counters or
  without `lanes_joined`. Missing is never read as zero.
- `inconsistent (failures > attempts)`: should be empty. Anything here is a bug
  in the counters. It is checked first, so `attempts=0, failures=1` lands here
  and not in the zero-attempt bucket.

It counts `no replacement lane will be attempted` and
`peer does not hold this session` failures separately, at any `lanes_joined`.
For every bucket it reports how the last lane died, taken from the
`last lane failed (...)` part of the error: `EOF`, `no recent network
activity`, `other`, or `unrecorded`. With `--gateway` it looks up each
bucketed client failure's `remote flow ended with error` record by
`session_id` and `flow_id`. It also tallies `lane join refused` records by
`reason`. A refusal count is each record plus its `suppressed`, so a storm's
tail after its last record is not included. The tool never uses the gateway's
attempt counters. It ignores a client-message record written by
`role=server`, and says so.

`lane_replacement_waits` counts waiters, not graces. The flow's run loop, its
frame and control writers, and its acknowledgement loop all wait for a missing
lane, so a lane that dies with writes in flight leaves several of them waiting
for the same replacement, and a count above one says how much of the flow was
stuck rather than how long it waited. The grace belongs to the outage: reading
`lane_replacement_wait` against it tells you whether the flow waited out a
whole outage or ended early, and `lanes_joined` tells you whether anything ever
arrived to end one.

The dashboard calculates interval packet loss from changes in
`queqiao_quic_packets_sent` and `queqiao_quic_loss_observed_packets_total`.
`queqiao_quic_packets_lost` and `queqiao_quic_bytes_lost` were removed: they
were quic-go's own counters, incremented only inside its cubic sender, and this
transport installs its own controller, so nothing ever moved them off zero
while the dashboard divided by them. A counter that cannot be produced is worse
than a missing one once it is monotonic, because it then reads as a
measurement. Those counters are process-wide monotonic totals: they are
accumulated from the forward movement of each QUIC connection, measured once
per connection against a baseline the connection itself holds.

That scoping is what makes the difference between two scrapes mean something.
A QUIC connection here is pooled, so at any moment several lanes belonging to
several flows are reading the same counters out of the same connection, and
each of those flows publishes telemetry on its own timer. Adding up what the
live flows currently report would count one connection once per flow
referencing it, and would move the total up and down as flows start and end --
so an interval difference would measure flow churn rather than the path, in
both the numerator and the denominator. Neither a flow ending nor its
telemetry expiring moves these counters now; both only retire gauges.

Within one interval a counter can still fail to advance. QUIC may recognize a
packet it previously declared lost, and a pooled connection replaced by a new
generation restarts its counters at zero. Both are read as no forward
movement rather than as a negative or a wrapped-around jump, and the
connection is re-baselined at the new reading.

A gateway that refuses a lane join writes `msg="lane join refused"` with the
reason at `info`, or at `warn` for a flow or principal mismatch, which are a
peer naming a live session that is not the one it holds. A storm is rate
limited per reason rather than per session -- the identifiers in a refused join
are the peer's to choose, so a map keyed by them is memory a peer sizes -- and
each record carries how many refusals of that reason it stands for in
`suppressed`. The matching counters are `queqiao_lane_join_refused_total` by
reason.

A gateway that refuses a flow open because of the opening account's own limits
writes `msg="account flow open refused"` at `warn`, naming the `reason` --
`flow_limit`, `client_limit`, or `unauthorized` -- with the account and device.
It is rate limited and counted the same way, with `suppressed` and `total` in
each record and `queqiao_account_admission_refused_total` by reason. This is
the record to look for when a user reports that most sites load and a few do
not: an account whose flow limit is too low for a browser fails exactly that
way, and the flow limit counts connections rather than devices.

Flow-completion records add an opaque session/flow correlation ID, transport,
duration, directional bytes, class, lane byte
allocation, coded-versus-stream payload, and the FEC sent/repair/recovered/
residual/window/rate summary.

The FEC counters have two directions and they must not be divided into each
other. `fec_sent_total` and `fec_repairs_total` are what this endpoint
transmitted; `fec_arrived_total`, `fec_recovered_total` and
`fec_residual_lost_total` are what it received, so on an asymmetric flow
`lost` above `sent` is ordinary rather than impossible. The receive direction's
rates are `fec_receive_erasure`, the share of the peer's source symbols that
did not arrive, and `fec_receive_residual_loss`, the share the code could not
repair and the session had to re-issue. Both are taken over
`fec_source_symbols_total` and are therefore in [0,1]. The `coded_substrate`
summary string carries the same two as `recv_residual` and `recv_erasure`.

An endpoint therefore reports three different erasure figures, and comparing
them without knowing which direction each measures is how an asymmetric path
reads as a fault:

| Field | Direction | Measured from |
| --- | --- | --- |
| `fec_receive_erasure` | what this endpoint receives | source symbols the decoder accounted for |
| `fec_observed_loss` | what this endpoint receives | gaps in the peer's transmission sequence |
| `queqiao_quic_controller_erasure_floor_ratio` | what this endpoint sends into | the controller's own acknowledgements |

On a path whose downstream erases and whose upstream does not, the first two
are large while the third is near zero, and all three are correct. The
controller's floor is the one that sizes this endpoint's parity, because the
direction a sender codes for is the direction it sends into. Failed flows are warning-level records with the
same performance and FEC fields plus the error, so they remain visible at the
default `info` level. `QUEQIAO_LANE_TRACE=1` remains an opt-in raw
per-lane diagnostic. It is not needed for the standard aggregate dashboard.

No application payload is logged. Operational logs can contain configured
endpoint addresses and error text; debug records may also contain local uplink
addresses and device/account identifiers. Treat the `0600` files as sensitive
operational data.

## Service operation

The production systemd service writes the file and mirrors JSON records to
journald. Either surface can diagnose startup when the other is unavailable:

```sh
sudo tail -f /var/log/queqiao/server.log
sudo journalctl -u queqiaod -f
```

Rotation is internal and does not require `logrotate`, a SIGHUP, or reopening
the process. Do not configure an external rotator to rename the same files.
The process fails startup rather than silently running without its configured
file when the directory cannot be created or the file cannot be opened.

Do not capture stdout or stderr to a file from the service manager either. A
LaunchAgent `StandardOutPath` or `StandardErrorPath`, or a systemd
`StandardOutput=file:`, stores a second copy of every line somewhere nothing
rotates, so it grows without bound while the rotating log beside it stays
within its five backups. Mirroring to a journal is not the same thing:
journald applies its own retention, which is why the generated systemd unit
leaves stderr on, while the generated LaunchAgent has no journal to write to
and passes `--log-stderr=false` instead. A unit hand-written before that
should drop the redirect, pass `--log-stderr=false`, or both.

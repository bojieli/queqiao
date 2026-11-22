# Measurement harnesses

## Local performance dashboard

[`tools/visualizer/`](../tools/visualizer/) is an offline HTML dashboard for
the bounded JSON files created automatically by both `queqiaod client` and
`queqiaod server`, optional lane traces, benchmark JSON, field-soak JSONL, and
the TSV/CSV files produced by these harnesses. Run `queqiaod logs` to locate
the standard runtime files, then open the visualizer's `index.html` directly.

Use `capture_metrics.py` when an independent `/metrics` scrape is needed. The
normal runtime log already contains five-second performance snapshots. This
script adds a UTC and
monotonic timestamp to each scrape, preserves failed scrapes as gaps, writes a
new JSON Lines file, and runs until interrupted when `--duration` is zero:

```sh
./scripts/capture_metrics.py \
  --url http://127.0.0.1:12090/metrics --interval 1 --duration 120 \
  --label client --output /tmp/client-metrics.jsonl
```

See the [visualizer guide](../tools/visualizer/README.md) for the recommended
combined metrics, lane-trace, and structured-log capture.

## Static security gate

`check_gosec.py` applies the reviewed rule-and-file ceilings from the public
release audit to a full gosec JSON report. It rejects a new rule, a finding in
a new file, or an increased bucket; it is intentionally stricter than a global
rule exclusion.

```sh
go run github.com/securego/gosec/v2/cmd/gosec@v2.28.0 \
  -fmt json -out gosec.json ./... || test $? -eq 1
./scripts/check_gosec.py gosec.json .
```

## Changelog assembly

`changelog.py` keeps `CHANGELOG.md` out of feature branches. Each user-visible
change adds one file to `changelog.d/`, so two branches never write the same
lines and a merge has nothing to resolve; the released file is assembled from
those files when a version is cut.

```sh
./scripts/changelog.py new fixed provider-unit-bind-capability
./scripts/changelog.py check                       # what CI runs
./scripts/changelog.py preview                     # the pending section
./scripts/changelog.py release --version v0.1.2    # cut it, at release time
```

`check` rejects an unknown category, a malformed filename, an empty entry, CRLF,
a hand-written list marker, and prose that would exceed 80 columns once the
bullet is rendered. `release` refuses a version already in the file and an empty
pending set. See [`docs/RELEASING.md`](../docs/RELEASING.md).

## Public-history secret scan

`scan_history_secrets.sh` downloads a pinned, checksum-verified Gitleaks binary,
proves its rules detect a generated canary, and scans every reachable Git
commit. The report path is controlled by `QUEQIAO_SECRET_SCAN_REPORT` and must
remain outside a public artifact if it contains a finding.

```sh
QUEQIAO_SECRET_SCAN_REPORT=history-secret-scan.json \
  ./scripts/scan_history_secrets.sh .
```

## Release artifact validation

`validate_release.py` verifies checksum coverage, safe archive paths, required
documents, executable mode, binary/BUILDINFO hashes, internal/external SBOM
identity, CycloneDX component/dependency coverage, wire version, and linked
license coverage without extracting untrusted archive paths.

```sh
./scripts/validate_release.py dist
```

## Emulated-path comparison (the fast inner loop)

`bench_matrix.sh` runs queqiao and a TUIC-shaped reference proxy over one
deterministic emulated path, in a single process, via `cmd/queqiaobench`. Use it
for any transport change before spending time on the live link:

```sh
./scripts/bench_matrix.sh --trials 5 --output /tmp/matrix.tsv
```

The reference (`internal/baseline`) is a measurement control, not a product: it
reproduces TUIC's data-path shape on the same QUIC stack and controllers queqiao
uses, so a gap between the two rows is attributable to the transport design
rather than to the language or QUIC library. The emulator (`internal/pathsim`)
applies a fixed delay, seeded loss, a bottleneck with tail-drop queueing, and
optionally a per-source-address policer; one seed reproduces one loss pattern.

Single cells can be run directly:

```sh
go run ./cmd/queqiaobench --rtt 200 --loss 3 --rate 100 --bytes $((10*1024*1024)) \
    --trials 5 --flows 1,4 --latency
```

Extra lanes cannot raise one flow's goodput when the only limit is an aggregate
bottleneck. Use `--per-flow-rate` to model a path that polices per 4-tuple,
which is the regime where lanes are supposed to help:

```sh
go run ./cmd/queqiaobench --stacks queqiao --rtt 200 --loss 1 --rate 400 \
    --per-flow-rate 25 --bytes $((100*1024*1024))
```

The emulator models independent per-packet loss and one bottleneck queue per
direction. It does not model bursty or correlated loss, reordering, variable
delay, or middleboxes, and both endpoints run on one machine. Live campaigns
remain necessary.

## Matched live comparison

`bench_live_matched.sh` alternates trials between two already-running SOCKS5
endpoints against one fixed remote object. The China-US path moves between
roughly 0% and 50% loss within minutes, so running all of one transport's
trials and then all of the other's compares two path windows rather than two
transports; alternating, and swapping which goes first each round, keeps the
comparison inside one window. Even so, a handful of rounds is not enough on
this link — expect to need well over ten, and report completion counts, not
only medians.

```sh
# US host: the reference server and an isolated queqiao server.
./queqiaoref --mode server --listen :12531 --tls-cert c.pem --tls-key k.pem --token-file t
./queqiaod server --state /var/lib/queqiao/provider --listen :12540

# Local: one client each, both bound to the physical interface so a TUN-mode
# proxy does not capture the outer UDP socket and silently tunnel the test.
./queqiaod client --profile ~/.config/queqiao/PROFILE.json \
  --listen 127.0.0.1:12140 --local-address 192.0.2.10
./queqiaoref --mode client --listen 127.0.0.1:12141 --remote IP:12531 --local-address 192.0.2.10 ...

./scripts/bench_live_matched.sh --url http://127.0.0.1:28095/10mb.bin --rounds 16
```

Use the literal server IP, never a hostname that a local proxy may resolve to a
fake IP. Both clients must use the same timeout, and any temporary listener
must be stopped when the campaign ends.

`bench_http.sh` measures independent application TCP flows through an already
running local SOCKS5 client. It emits one TSV row per flow and keeps partial
HTTP bodies, non-200 responses, curl errors, and timeouts in the output. A
row is complete only when curl exits successfully, the response is HTTP 200,
and the exact expected body length is present.

Example for the fixed CacheFly object used in the reports:

```sh
QUEQIAO_SOCKS5=127.0.0.1:12080 \
QUEQIAO_TRIALS=5 QUEQIAO_FLOWS='1 2 4 8' \
./scripts/bench_http.sh --output /tmp/queqiao-http.tsv
```

The script intentionally measures concurrent independent application flows.
It does not claim that eight application connections are equivalent to one
logical striped flow; single-flow striping was measured and then deleted. Run
the client with a physical source binding when Clash TUN/fake DNS would
otherwise capture the outer endpoint, and keep the previous binary as the
rollback path.

`bench_single_flow.sh` measures exactly one HTTP application connection per
trial. Run it once for each separately configured transport/controller, and
keep the label in the output so configurations cannot be mixed:

```sh
QUEQIAO_SOCKS5=127.0.0.1:12080 QUEQIAO_LABEL=erasure-default \
  QUEQIAO_TRIALS=5 ./scripts/bench_single_flow.sh --output /tmp/one.tsv
```

There is no lane count to set: a flow's data goes over one connection. See
`docs/DESIGN.md` for why striping was deleted. `--quic-pool` is enabled by
default and provides the bounded multiplexed QUIC control connection.

## Dedicated upload sink

Public upload endpoints are unsuitable throughput oracles. For a controlled
US-side upload measurement, run the bounded test-only sink on the fixed-egress
host, expose it only for the duration of the trial, and stop it afterward:

```sh
python3 scripts/upload_sink.py --listen 0.0.0.0:28080 --max-bytes 67108864
dd if=/dev/zero bs=1m count=10 2>/dev/null |
  curl --socks5-hostname 127.0.0.1:12081 --data-binary @- \
       --write-out '%{http_code}\t%{size_upload}\t%{time_total}\n' \
       http://EGRESS-IP:28080/
```

The sink requires an explicit bounded `Content-Length`, accepts one request by
default, and exits after that request. It must not be left as a public
service; use a temporary systemd unit or process supervisor and verify that
the listener is gone after the measurement.

## Fallback soak

`fallback_soak.sh` repeatedly withdraws UDP under live associations, verifies
TCP rescue and remote relay-source preservation, restores UDP on the same
endpoint, and verifies that post-cooldown health probes return new associations
to QUIC. It runs both normal and race-detector repetitions and emits a
checksummed provenance bundle:

```sh
./scripts/fallback_soak.sh --runs 50 --race-runs 20 \
  --output-dir /tmp/queqiao-fallback-soak
```

This is the deterministic release soak and runs weekly at a smaller count. It
does not replace broader live NAT and middlebox campaigns.

`field_soak.py` is the long-duration real-path harness. It keeps one SOCKS5 UDP
association alive while opening independent verified HTTPS flows, snapshots
metrics and process resources, writes redacted JSON Lines events, applies
explicit success/tail gates, and checksums the evidence directory. Use an
opaque path label rather than an ISP account or subscriber address.

```sh
./scripts/field_soak.py --socks 127.0.0.1:12080 \
  --duration 86400 --interval 5 --https-every 12 \
  --metrics-url http://127.0.0.1:12090/metrics --pid CLIENT_PID \
  --label mobile-carrier-a-primary-443 \
  --output-dir /tmp/queqiao-field-mobile-a
```

For a stable test fixture, pass `--expected-https-sha256 HASH` to require the
SHA-256 of the decoded HTTP body. Truncated Content-Length or chunked bodies
fail instead of counting as HTTP successes. Without an expected digest,
`body_integrity_checked` is false: recorded hashes alone do not prove integrity.
Without resource inputs, `resources_checked` is false and `resources_settled`
is null, not a resource-convergence pass.

The harness does not create network diversity. Run it separately on the exact
independent paths recorded in `docs/FIELD-VALIDATION.md`; endpoint-injected
faults must be labeled as such.

`website_latency_soak.py` is a focused overnight TCP/HTTPS stability check. It
uses fresh requests through the running Queqiao SOCKS5 listener to several
small endpoints, records each attempt as JSON Lines, and writes per-target and
hourly success rates plus p50/p95/p99 latency in a checksummed summary. A round
where every unrelated target fails is reported separately from a one-site
failure.

```sh
run_dir=/tmp/queqiao-websites-$(date -u +%Y%m%dT%H%M%SZ)
nohup caffeinate -i ./scripts/website_latency_soak.py \
  --socks 127.0.0.1:12080 --duration 28800 --interval 60 \
  --output-dir "$run_dir" >"$run_dir.log" 2>&1 &
echo "PID $!; results: $run_dir; progress: $run_dir.log"
```

`caffeinate -i` keeps macOS awake while the process runs; omit it on Linux or
on a host whose sleep policy is already disabled.

The defaults probe Cloudflare, Google, Apple, GitHub, and Wikipedia. Repeated
`--target NAME=https://URL` arguments replace that set. The script resolves
destination names through SOCKS (`socks5h`), forces curl not to honor a
conflicting `NO_PROXY`, applies no retries, and exits nonzero if any target's
success rate is below `--min-success-rate` (default 99%). An optional
`--max-p95-ms` adds a per-target TTFB gate. Individual website failures are not
by themselves proof of a Queqiao failure; correlated rounds and Queqiao's own
metrics/logs provide the stronger signal.

`udp_association_check.py` is the live-path companion. It sends DNS queries
through one unchanged SOCKS5 UDP association, records every loss and latency as
TSV, and can require both an overall success count and a successful tail. The
tail condition matters in a blackhole test: successful queries before the
fault must not hide a failure to recover afterward.

```sh
./scripts/udp_association_check.py --socks 127.0.0.1:12080 \
  --count 50 --interval 0.5 --timeout 2 --min-success 10 \
  --require-final-successes 5 --output /tmp/udp-rescue.tsv
```

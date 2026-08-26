# Measuring this transport

> [!NOTE]
> **Status:** Current benchmark methodology for public protocol 1
> **Last reviewed:** 2026-08-20

This is the reproducibility guide for the measurement rig. It exists because the motivating
link moved between roughly 0% and 50% packet loss within minutes, so
running one transport's trials and then another's compares two path windows
rather than two transports. Every performance claim in this repository should
be reproducible with the commands here.

Read the [project status](STATUS.md) before interpreting a result. The harness
can establish correctness, behavior under controlled impairment, and a fair
same-window comparison; it cannot turn one path into a universal performance
claim.

`cmd/queqiaobench --cpuprofile` is how to ask where the CPU goes, and it is
worth knowing that nothing in the data path had a benchmark until a profile
found a bandwidth-estimator loop rescanning the whole send window on every
acknowledgement, at 13% of all samples. A profile of a 300 Mbit/s transfer puts
encryption at under 2% and the coding below the top twenty; what dominates is
the syscall per batch of packets.

This document covers the emulated rig, which is where a result is reproducible.
For measuring a live path instead, `cmd/pathprobe` describes the path and
`cmd/pathmeasure` describes what a stack achieves on it, including
`-mode workload`, which drives a real endpoint rather than a transfer of the
same size. [MEASURING-A-DC-PATH.md](MEASURING-A-DC-PATH.md) is the procedure
and the traps.

## One protocol, three workload views

Short-lived, interactive, and bulk are evaluation families for the same
Queqiao architecture. The benchmark changes the offered workload; it does not
select three transports.

| Family | Primary outputs | Minimum comparison |
| --- | --- | --- |
| Short-lived | fresh/warm connect, first byte, completion, failure tail | matched small requests with `--latency` |
| Interactive | latency/jitter/delivery while bulk occupies the bottleneck | `--interactive` with an idle-path control |
| Bulk | useful goodput, completion count, physical/logical bytes, CPU and memory | matched downloads and uploads long enough to leave startup |

For a transport or controller change, report applicable results from every
family. A bulk improvement is incomplete evidence if it moves interactive tail
latency or small-flow setup in the wrong direction.

## The three pieces

**`internal/pathsim`** — a deterministic UDP path emulator. Clients send to the
relay in place of the server. It applies, per direction:

| Knob | Models |
| --- | --- |
| `OneWayDelay` | propagation delay; the emulated RTT is twice it |
| `DelayJitter` | per-packet delay variation, which also reorders |
| `LossRate` | overall drop probability |
| `UpstreamLossRate` | a different drop probability client-to-server |
| `LossBurstPackets` | correlated loss: a Gilbert chain with this mean burst |
| `RateBytesPerSec` | a bottleneck with tail-drop queueing |
| `PerFlowRateBytesPerSec` | a policer applied per source address, with a bucket scaled from its own rate |
| `DelayWander` | amplitude of a correlated random walk on the one-way delay |
| `QueueBytes` | the bottleneck buffer; zero selects one BDP |
| `Seed` | makes the loss pattern reproducible |

### Jitter and wander are different impairments

`DelayJitter` draws per packet, so it reorders and leaves the smoothed round
trip near the minimum. `DelayWander` walks the one-way delay on a slower clock,
so a whole flight shifts together: the round trip varies while the minimum stays
put, and almost nothing reorders. A long-haul path does the second. The live
China-US link this project targets measures 226 to 440 ms with a 48 ms standard
deviation and a stable minimum, which `--delay-wander 107 --rtt 226`
approximates.

This mattered: a change to the congestion controller measured better on the
emulator without wander and cost more than half the throughput live. Adding
wander makes the emulator produce that regime -- it costs the transport 26% of
its goodput on an otherwise identical path -- but it does **not** by itself
reproduce that particular verdict. The emulator is a filter, not an oracle, and
this is the measured limit of the filter.

One seed reproduces one loss pattern exactly. That property has its own test,
because everything else depends on it.

**`internal/baseline`** (runnable as `cmd/queqiaoref`) — a TUIC-shaped reference
proxy: one authenticated QUIC connection, one bidirectional stream per relayed
TCP connection, a short destination header, then unframed copying. It runs on
the same quic-go fork and the same congestion controllers queqiao uses, with
TUIC's published transport windows.

It is a control, not a claim to be native TUIC. Comparing against a separately
built Rust implementation conflates the transport design with the language and
QUIC library; comparing against this isolates the design.

**`internal/extproxy`** — launches third-party implementations so the
comparison is not limited to an in-tree control:

| Stack | Implementation | Carried by | Needs |
| --- | --- | --- | --- |
| `tuic` | sing-box, native TUIC v5 | UDP relay | `--sing-box` |
| `hysteria2` | sing-box | UDP relay | `--sing-box` |
| `vless-tcp` | sing-box, VLESS over TLS | TCP relay | `--sing-box` |
| `vless-ws` | sing-box, VLESS over WebSocket | TCP relay | `--sing-box` |
| `kcptun` | kcptun, KCP with fixed-rate FEC | UDP relay | `--kcptun-client`, `--kcptun-server` |

Each runs as a server the emulator forwards to and a client exposing SOCKS5,
over exactly the same seeded path as queqiao. Where the transport has TLS the
client trusts exactly the server's certificate, so nothing disables
verification. They are registry entries rather than special cases; see the
contract below for adding another.

`kcptun` is the one that is not a proxy. It forwards a local TCP port, so the
harness runs a SOCKS5 server of its own beyond the emulator for the tunnel's
server to forward to, and the tunnel's local port is what the benchmark speaks
to:

```
benchmark -> [kcptun client] -> emulator -> [kcptun server]
          -> [harness SOCKS5 target] -> destination
```

The measured path is crossed exactly once, as for every other stack; the two
extra hops are loopback. kcptun's upstream repository was withdrawn and
`github.com/xtaci/kcptun` no longer resolves, which is why the stack takes two
binary paths rather than naming a source: build from whatever copy you trust,
and record which one and at what commit alongside the run, as for any other
measured dependency. It is the comparison queqiao's own coding most needs,
because both transports spend parity to avoid a round trip and choose how much
in opposite ways — see the fixed-parity section below before quoting a number.
kcptun ships one program per side, which is why it takes two paths; its
compression is disabled, because the benchmark's payload is a repeating byte
ramp that snappy would reduce to almost nothing, and the stack would then
report the compressor's rate rather than the path's.

**`cmd/queqiaobench`** — runs the selected stacks over one emulated path in a
single process and reports per-trial rows, a summary, optional JSON, and an
optional regression gate.

## Do not compare a QUIC stack against a TCP stack

The two relays model different things, and their numbers are not
interchangeable.

The UDP relay models per-packet serialization, tail drop, and loss. The TCP
relay cannot: a userspace relay receives a byte stream, not segments, so
dropping bytes would deliver a hole rather than trigger a retransmission, and
the kernel TCP stack whose loss recovery is the interesting behaviour sits
below the relay. It therefore refuses a loss rate instead of silently
producing a lossless result, and it applies backpressure where the packet relay
would tail-drop.

So `vless-ws` against `vless-tcp` is a fair comparison — it isolates
WebSocket's framing cost — and `tuic` against `queqiao` is a fair comparison.
`vless-tcp` against `tuic` is not. Emulating loss for a stream transport needs
an IP-layer facility such as dummynet, which needs privilege this harness does
not take.

## Adding an external transport

A stack is a pair of processes the harness starts, waits for, and stops. The
harness owns the emulated path, the addresses, the TLS material, the work
directory and the process lifetime. A stack answers two questions: what
configuration to write, and what to run. `internal/extproxy` keeps those apart
so a transport is added by registering it rather than by editing the launcher:

```go
// internal/extproxy/extproxy.go
var stacks = map[Kind]stack{
    TUIC: {transport: "udp", implementation: "sing-box", launch: singBoxLaunch},
    ...
}

// A launch builder returns what to write and what to run.
func singBoxLaunch(cfg Config) (Launch, error)
```

`Launch` carries `Files` (written before either process starts, keyed by a path
under `Config.WorkDir`), `ServerArgs` and `ClientArgs`, and optionally
`ServerBinary`/`ClientBinary` for an implementation that ships one program per
side. `Plan` resolves it without running anything, which is how a stack's
wiring is tested with nothing installed.

What the running pair must satisfy:

1. **The server binds `ServerListen` exactly.** That is the address the
   emulator forwards to. A server that binds anything else produces a working
   measurement that never crosses the emulated path, which is worse than a
   failure because it looks like a result.
2. **The client dials `ClientRemote`**, which is the emulator, never the server
   directly.
3. **The client exposes SOCKS5 at `SOCKSListen`.** SOCKS5 is the whole contract
   with the benchmark, and the pair is considered ready when that address
   accepts a TCP connection.
4. **TLS material is the harness's.** The server uses `CertificatePath` and
   `KeyPath`; the client trusts exactly `CertificatePath`. No verification
   bypass, and the client's configuration must never reference the private key.
   Both are tested.
5. **Configuration goes in `WorkDir` and nowhere else.** The harness creates it
   and removes it.
6. **The processes must die with their process group.** The harness starts each
   side in its own group and escalates if it does not exit; an implementation
   that daemonizes or leaves a helper behind holds its ports and silently
   contaminates every later trial.
7. **Declare the relay honestly.** `transport: "udp"` puts the stack behind the
   packet emulator, where loss, delay and rate apply. `"tcp"` means it can only
   be measured with `--loss 0`, for the reason in the section above.

The harness has to be told where the binary comes from, which is the one step
that is not in `internal/extproxy`. Add a flag for it in `cmd/queqiaobench` and
a case to `externalBinaries` keyed on the implementation the registry entry
names; a stack that skips this registers fine and then refuses every run with
`stack "x" needs a y binary, and this benchmark has no flag for one`. An
implementation shipping one program per side takes two flags, as kcptun does.

### What the harness records

A stack is measured from the outside. Per trial it records elapsed seconds,
goodput, and whether the transfer completed -- and completion means the exact
expected byte count arrived, not merely that the request returned. `--latency`
and `--interactive` add cold/warm setup and first-byte splits, and `--contend`
records each stack's share of one shared bottleneck. `--json` writes all of it
with the commit, dirty bit, Go toolchain, target, module graph, exact
arguments, and seeded path parameters, which is what makes a cell reproducible
by somebody who was not there.

Nothing reads a stack's own telemetry. Queqiao's counters reach the report
because the harness runs it in-process; an external stack is a pair of
processes whose logs are captured only to explain a failure. So a claim that
depends on what the transport did internally -- parity share of bytes sent,
retransmissions, window behaviour -- has to be recorded alongside the run by
hand, from that implementation's own output. The fixed-parity section below is
one instance of that requirement rather than an exception to it.

A tunnel rather than a proxy has no SOCKS5 of its own: it forwards a local TCP
port to a target. Declare `socksTarget: true` in the registry entry and the
harness runs `extproxy.StartSOCKSTarget` beyond the emulator and passes its
address as `Config.SOCKSTarget`; forward to that, and expose the tunnel's own
local port as `SOCKSListen`. The target is in-process on purpose: an external
SOCKS5 server would be a third implementation in the measurement whose version
and buffering nobody recorded. `Plan` refuses such a stack when the target is
missing, because a tunnel forwarding to nothing fails every trial at SOCKS with
an error that says nothing about why.

`kcptun` is the worked example of all of this.

### Comparing a fixed-parity transport

A transport with FEC in the matrix is worth having, and it needs one thing said
about the comparison before the numbers are taken. Queqiao chooses its parity
from the erasure it measures, and it revises that choice while a flow runs; a
kcptun-style code rate is a constant chosen in advance. A single configuration
is therefore a comparison against one guess, and whichever way it lands the
result is mostly about the guess.

So a comparison should carry:

- **Size the window before anything else.** This is not a refinement, it is
  the difference between a measurement and a rigged one. kcptun's default send
  window of 128 packets caps a 210 ms path at about 6.6 Mbit/s no matter what
  the code does. Measured live on that path, the same build went from 5.09 to
  25.36 Mbit/s -- five times faster -- when `--kcptun-sndwnd` and
  `--kcptun-rcvwnd` were raised to a delay-bandwidth product's worth of
  packets, and the gap to queqiao fell from 9.6x to 2.1x. A comparison run at
  the default windows is measuring the window.
- **A parity sweep, not a point.** `--kcptun-parityshard` against a fixed
  `--kcptun-datashard`, spanning ratios above and below the erasure rate, with
  the best one shown against queqiao rather than an arbitrary one. The
  defaults are kcptun's own (10 data, 3 parity, `fast`, 128/512 windows), which
  is a starting point rather than an answer.
- **The cost alongside the rate.** Parity is bandwidth spent on the same
  bottleneck, so report the parity share of bytes sent on both sides. Queqiao's
  is `fec_repairs_total` over `fec_sent_total` in its runtime log, and kcptun's
  is the ratio the sweep fixed; a transport buying more of the wire can finish
  sooner while delivering fewer useful bytes per byte sent, and a goodput
  column alone hides that.
- **Every parameter that moves the answer.** The implementation version, and
  the settings the harness passes: `--kcptun-mode`, `--kcptun-datashard`,
  `--kcptun-parityshard`, `--kcptun-sndwnd`, `--kcptun-rcvwnd`. The windows are
  the congestion control in practice, so a run that does not state them is not
  reproducible. They appear in the archival JSON with the rest of the command.
- **The same rules as every other cell**: one seeded path, alternating trial
  order, medians over all trials counting failures at their partial rate, and a
  check against the calibration bound below.

```sh
# One cell of a parity sweep, against queqiao on the same seeded path.
for parity in 1 3 6 10; do
  go run ./cmd/queqiaobench --stacks queqiao,kcptun \
      --kcptun-client ./kcptun/client --kcptun-server ./kcptun/server \
      --kcptun-mode fast3 --kcptun-datashard 10 --kcptun-parityshard "$parity" \
      --rtt 200 --loss 20 --rate 50 --bytes $((16*1024*1024)) --trials 5 \
      --json "/tmp/kcptun-parity-$parity.json"
done
```

## Reading the numbers

The summary reports **medians over all trials, counting failures at their
partial rate**, alongside the completion rate.

This matters. A median over completed trials alone rewards a transport for
giving up: in a 35%-burst-loss block the reference completed 7 of 12 trials and
queqiao 10 of 12, and scoring only the successes made the transport that
finished the hard trials look slower than the one that abandoned them.
`median_mbits_completed_only` is retained for continuity with older campaign
notes and must never be compared across stacks with different completion rates.

`--interactive` additionally issues small requests during the bulk transfer and
splits their latency into connect and first-byte. Those are different defects
with different fixes: a slow connect is flow setup, a slow first byte is
queueing behind the bulk transfer.

## Typical invocations

```sh
# Short-lived requests: cold/warm setup, first byte, and completion.
go run ./cmd/queqiaobench --rtt 200 --loss 1 --rate 100 \
    --bytes $((64*1024)) --latency --trials 8

# Interactive requests while the same protocol carries a bulk transfer.
go run ./cmd/queqiaobench --rtt 200 --loss 1 --rate 100 \
    --bytes $((50*1024*1024)) --interactive --trials 5

# Bulk completion and useful goodput.
go run ./cmd/queqiaobench --rtt 200 --loss 1 --rate 100 \
    --bytes $((100*1024*1024)) --trials 5

# Against real implementations rather than only the in-tree control.
go run ./cmd/queqiaobench --stacks baseline,queqiao,tuic,hysteria2 \
    --sing-box /path/to/sing-box --rtt 200 --loss 1 --rate 100 --trials 5

# Against a fixed-rate erasure code, which is the comparison queqiao's own
# coding most needs. Sweep the parity; see the fixed-parity section.
go run ./cmd/queqiaobench --stacks queqiao,kcptun \
    --kcptun-client /path/to/kcptun/client --kcptun-server /path/to/kcptun/server \
    --kcptun-mode fast3 --kcptun-parityshard 3 \
    --rtt 200 --loss 20 --rate 50 --bytes $((16*1024*1024)) --trials 5

# The TCP family, which cannot be measured under loss.
go run ./cmd/queqiaobench --stacks vless-tcp,vless-ws \
    --sing-box /path/to/sing-box --rtt 200 --loss 0 --rate 100 --trials 4

# The standard matrix, five trials per cell.
./scripts/bench_matrix.sh --trials 5 --output /tmp/matrix.tsv

# An archival bundle: TSV, per-cell JSON, source/toolchain manifest, any dirty
# source patch, and checksums. The directory must not already exist.
./scripts/bench_matrix.sh --trials 5 --json-dir /tmp/queqiao-report

# One cell, both stacks, with a machine-readable record and a CI gate.
go run ./cmd/queqiaobench --rtt 200 --loss 3 --rate 100 --trials 5 \
    --json /tmp/result.json --gate --tolerance 0.10

# Correlated loss, controller held constant so the transports are compared
# rather than the controllers.
go run ./cmd/queqiaobench --rtt 178 --loss 35 --loss-burst 10 --rate 50 \
    --bytes $((4*1024*1024)) --congestion brutal --brutal-rate 12 --trials 12

# A reverse-path-heavy regime, which is where a transport that layers its own
# acknowledgements over QUIC gets into trouble.
go run ./cmd/queqiaobench --rtt 200 --loss 0.5 --loss-up 25 --rate 100 \
    --bytes $((32*1024*1024)) --trials 4
```

## Measuring contention

Running one stack and then the other answers which is faster alone. It cannot
answer which takes more of a link when both want it, and for a transport whose
purpose is to win a contended bottleneck that is the question that matters.

`--contend` attaches two stacks to one shared `pathsim.Bottleneck` -- one
serialization clock, one queue, one loss process -- starts both transfers
together, and reports each one's share:

```sh
go run ./cmd/queqiaobench --contend queqiao,baseline --rtt 200 --rate 100 \
    --bytes $((20*1024*1024)) --trials 6
```

A share of 0.5 is an even split. This is what found the last argument against
striping before it was deleted: four lanes measured 60 Mbit/s against one
lane's 58 on a shared path run sequentially, which reads as harmless, and took
0.40 of the link against one lane's 0.51 when actually contended -- the same
sequential number, and the opposite conclusion.

## Calibrating the instrument

`TestForwardingCapacity` measures what the emulator itself can forward, which
bounds every transport number taken through it: 1722 Mbit/s at 200 ms, 1784 at
20 ms. Quote no result that is within a small factor of these without checking
it first.

## Archival reports and provenance

`bench_matrix.sh --json-dir DIR` creates a self-checking report bundle instead
of loose JSON files. `manifest.txt` records the commit, Go toolchain, target,
matrix settings, and shell-escaped invocation. Every per-cell JSON report has
schema version 1 and repeats the exact command arguments, seeded path settings,
VCS revision/dirty bit, Go target, and complete module dependency graph. Cold
and warm latency records and contention records are machine-readable too; they
are no longer terminal-only output. `SHA256SUMS` covers the bundle.

For a report intended to support a published claim, start from a clean tree so
the recorded commit is sufficient to reproduce its source. A development run
is still honest: `source-status.txt` records the dirty paths and `source.patch`
captures tracked changes. Untracked files cannot be reconstructed from that
patch, which is why an archival run must be clean.

The emulator is deterministic for a given seed, but elapsed time is not:
scheduler load, CPU frequency, kernel, and Go version still affect performance.
Reproduce comparisons on a comparable host and judge paired stacks from the
same cell, rather than expecting benchmark JSON to be byte-identical.

## Live campaigns

The emulator is the inner loop, not a replacement for the real link.
`scripts/bench_live_matched.sh` alternates trials between two already-running
SOCKS5 endpoints and swaps which goes first each round, keeping a comparison
inside one path window. Expect to need well over ten rounds, and report
completion counts rather than only medians.

Five mistakes produced confident, wrong results during this project's
campaigns. All five are cheap to avoid and expensive to miss:

- **Use the literal server IP.** A hostname that a local TUN-mode proxy
  resolves to a fake IP means both transports are measured *through the
  existing tunnel*, not over the path under test.
- **Bind the outer socket to the physical interface** (`--local-address`) on
  both clients, for the same reason, and give both the same timeout.
- **Prove the binding worked, don't assume it.** Have the server report the
  source address it sees. On 2026-08-16 an unbound datagram arrived from
  `<TUNNEL-EGRESS-IP>`, the existing tunnel's exit, and a bound one from
  `<CLIENT-PUBLIC-IP>`, the real uplink — same host, same destination, one flag
  apart. Third-party clients need their own equivalent (`inet4_bind_address`
  for sing-box outbounds), and they need checking too.
- **Clear `NO_PROXY` before using curl.** `curl` honours `NO_PROXY` even when a
  proxy is named explicitly with `--socks5-hostname`, so a shell exporting
  `NO_PROXY=*` sends every "proxied" transfer straight to the origin and
  reports it as a success. Use `env -u NO_PROXY -u no_proxy curl --noproxy ''`.
- **Make the remote oracle concurrent.** A single-threaded `http.server` lets a
  lingering connection from one trial delay the next; before this was fixed,
  queqiao measured 1.19 Mbit/s against the reference's 4.52, and with a threaded
  oracle and nothing else changed the two measured 0.478 and 0.522.

**Measure a fixed duration, not a fixed object, when the stacks are far apart.**
A 16 MiB object is four round trips for a QUIC stack on a fast path and three
minutes for VLESS over TCP on the same path; a 20-second window measures the
rate each one actually sustains and keeps completion honest.

Stop every temporary listener when the campaign ends. An earlier session left
an authenticated listener bound to all interfaces for thirteen hours.

## What the rig cannot tell you

It models one bottleneck queue per direction plus an optional per-source
policer. It does not model middlebox behavior, path MTU changes, or NAT
rebinding. Both endpoints run on one machine, so it cannot expose a defect that
only appears with a real NIC or a real scheduler.

It says nothing about correctness under lane failure, UDP blocking, or restart.
Those gates are in [`PRODUCTION-DESIGN.md`](PRODUCTION-DESIGN.md).

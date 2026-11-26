# Queqiao workload model for the live evaluation harness

Status: proposal. Written against `origin/main` `0eb4f36` (design v6 merged; stages 1, 3, 4 merged; 2, 5 in review; 6 on a branch).

What this is based on, and what it is not:

- Read: README, `docs/DESIGN-PATH-AUTHORITY.md` v6, `docs/BENCHMARKING.md`, `internal/classifier`, and the classifier's call sites in `internal/pep` and `internal/profile`.
- The classifier traces in §5 are from reading the code. Nothing was executed; each finding names the unit test that would confirm it.
- The stage-2 (`sched=class`) and stage-6 branches are not on `origin/main` and were not read. The weights 4:2:1 are taken from the brief; the design doc still lists weights as open question Q1.
- No 900 s default exists on `origin/main` (a 15-minute relay idle timeout appears only in a test fixture, `cmd/queqiaod/relay_test.go:70`). The production 900 s idle EOF is therefore not explained by a default, and workload W9 is designed to localise it.
- All numeric regression bounds are provisional. They are frozen as the pre-registered bounds before the first gated block (design §15.4).
- The findings of §5 are merged with the stage-2 classifier audit in `docs/AUDIT-CLASSIFIER-WORKLOADS.md`, which keeps the F and R numbers used here and extends them. That audit read the stage-2 branch and confirms the 4:2:1 weights (`internal/congestion/classarb.go:20` on PR #181).

## Minimal live plan

The owner approved a minimal, representative live evaluation: live runs total about 2.5–3 h per path across all gates, not tens of hours. The gates are four trial types plus the DC gate, run on both live paths (design §15.3); everything else in this document is covered inside them, by a deterministic or emulated test, or by an optional run. The full matrix of §3–§4 stays as an opt-in reference.

| Trial | Gate | Content | Length |
| --- | --- | --- | --- |
| **A `bulk`** | stages 6/7 | W1, 4 downloads | 60 s |
| **B `mixed-6/7`** | stages 6/7 | W5-loop (5 turns, `U` = 100 KB plus 8 KB per turn, 2 s gaps) and W6 keystrokes with one 300 KB burst at 40 s, under 2 bulk downloads | 90 s |
| **DC** | stage 9 | W4 at minimal size (10 requests, ASR and TTS alternating, the first on a cold app connection), then design §11.3's sustained-goodput and fairness workload | 60 s |
| **C `mixed-all`** | stage 10 | concurrently: W7 call (whole trial), one W3 page (h2, cold app connections, at 20 s), one W5-single call (`U` = 100 KB, at 50 s), W6 keystrokes without bursts, 1 W1 download, 1 W2 upload | 120 s |
| **D `idle-wake`** | stage 10 (and `classifier=v2`) | 50 connections per arm, 200 KB sync, no heartbeat; each woken once at 1, 5 or 16 min (17/17/16), push and send alternating, then 5 warm follow-ups | about 17 min |

- **Metrics.** In B and C every workload's metrics come from the same trial, with its §3 metrics and provisional bounds, except: W6's latch ratio comes from B only (keystrokes in the 40 s before the burst against those after it); C's keystrokes are judged on echo time alone; D's gated metrics are below. The primary is bulk flows' bytes only: B's downloads, C's download and upload as two separate primaries, DC's sustained phase.
- **D** is scored on: no silent deaths (hard); every wake at 1 and 5 min delivered within 5 s (hard); delivered count at 16 min ≥ main's; push and send wake penalty p50 within W9a's bound. The p95, max, death modes and idle EOF times are reported. W9a's repeated cycles, six sessions, p99 and idle cost belong to the opt-in matrix.
- **Statistics.** Arms are interleaved in randomised ABBA or BAAB blocks: 6 blocks, then one block at a time with the one-sided test re-run and corrected for repeated looks, until decided or 12 blocks (design §15.4). D is one paired session per path, not a block sequence.

**Moved out of the live gates.** These are deterministic or emulated tests in CI, on `internal/pathsim`, measured against the legacy bundle on the same seeded path. They are in `internal/pep/workload_*_test.go`: each runs the legacy bundle and the flip bundle (every merged switch on) through a real client and gateway. W9b, W8 on its clean path and W10 apply their bounds in `-short`; W8's lossy path, W11's bound (200 queries) and M5's bounds run in the full suite (`deep.yml`, release candidates). M5's measures follow a nested controller's sawtooth, so it runs §15.4's sequential gate on 30 s blocks: 6, then one at a time to 12, passing only when every measure's lower bound clears zero and failing as inconclusive at the cap. Under the race detector the comparative bounds are reported, not judged. W9b's clock reaches the flow idle limit and classification; the binding deadline and keepalive schedule read it when they are built.

| Entry | Test | Asserts |
| --- | --- | --- |
| W9b | `TestIdleFlowSurvivesEachIdleTargetOnAnInjectedClock` | every cell of idle target (5–60 min, including 29/31/45/60) × heartbeat mode × wake direction: delivery below the flow idle timeout, the documented outcome above it, and EOF or RST at both applications within 5 s (no silent death). Needs an injected clock in the flow idle timer, binding deadline and keepalive schedule. It reproduces timer behaviour; attributing the production 900 s EOF to a middlebox or the destination needs live endpoint traces (optional) |
| W8 | `TestTunnelledQUICDownloadOnAnEmulatedPath` | W8's bounds for download and page: goodput, inner loss, wire bytes per useful byte |
| M5 | `TestHeavyUDPDoesNotDelayThinUDPOrCodedChunks` | W7 audio beside a W8 download and W6 keystrokes: audio late loss and lateness p99 (W7 bounds), keystroke echo p99 (W6 bound); FIFO sojourn and PACKET drops reported |
| W10 | `TestSegmentFetchOnAnEmulatedPath` | W10's bounds: p10 throughput and rebuffer ratio |
| W11 | `TestOneShotDatagramOnAnEmulatedPath` | W11's bound: fraction over 1 s, fresh and reused associations |

**Optional manual runs**, reported and never gated: M3 swarm, M7 soak, and every variant not named above (standalone one-flow W1 and `speedtest`, W2 on the iOS profile, W3 h1/h2 × cold/warm sweep, W5 `U` sweep, swarm, h2 and `reasoning`, W7 `game`). Design §15.1 test 14 holds the memory bound for many idle sockets on the iOS profile (M6's memory readout); W2's sustained-upload memory on that profile has no CI assertion yet and is optional live.

## 0. Decisions in one screen

1. **Eleven clusters, eleven workloads, built from five generators** (`bulk`, `rpc`, `stream`, `dgram`, `h3`). Eight workloads exist in the plan already; three are new and necessary: **W5 LLM stream**, **W8 tunnelled QUIC**, **W9 idle wake**. Two are cheap variants: W10 segment fetch, W11 one-shot datagram.
2. **The axis the current plan does not cover is phase change inside one connection**: burst then trickle, idle then wake, interactive then bulk then interactive. Every planned workload has one shape for its whole life. The classifier is weakest exactly there.
3. **The classifier latches BULK permanently, and an LLM call with a request of 128 KiB or more is labelled BULK at both ends by the first response byte.** The response deltas then lose coding today, and lose priority under `sched=class`. The same latch hits SSH after a large output, a messaging connection after its login sync, and an HTTP/2 connection after a page load.
4. **Three classifier changes must land before the stage 6/7 campaigns, in both arms**: a non-sticky scheduling class (sparse-write rule plus a windowed exit from BULK), the stale-window fix, and per-byte class telemetry. Otherwise the stage-7 gate measures the mislabel, not the topology.
5. **UDP is always INTERACTIVE and shares one ordered symbol stream.** A tunnelled QUIC download therefore sits in the same FIFO as voice packets and coded LLM deltas. This is Q5 of the design. It does not block stages 6/7; it must be measured (W8, M5) and decided before the stage-10 flip.
6. **Must-have before stage 10**: trials A, B, C, D and DC live on every path, and the W8, W9b, W10, W11 and M5 tests in CI (Minimal live plan). Optional: M3, M7, the remaining variants, real-application validation runs.

## 1. Characteristic axes

An axis is listed only if moving along it changes what the transport does. Reference path: RTT 200 ms, 15–50 % random erasure, asymmetric. At 20 Mbit/s one BDP is about 500 KB; at 100 Mbit/s about 2.5 MB. An initial window is about 15 KB.

| # | Axis | Values | Queqiao mechanism it flips |
| --- | --- | --- | --- |
| A1 | Substrate | reliable ordered stream (TCP CONNECT); datagram with a deadline (UDP ASSOCIATE) | stream vs coded datagram path; repair owner; whether late delivery is useless |
| A2 | Direction shape | down-heavy, up-heavy, symmetric, alternating | which direction's authority is loaded; whether the app's data shares a direction with its own flow ACKs |
| A3 | Burst size vs IW and BDP | under one IW (≤ 15 KB); sub-BDP (15 KB–1 BDP); multi-BDP | sub-IW: one flight, tail loss decides; sub-BDP: start-up and path knowledge decide; multi-BDP: steady rate decides |
| A4 | Cadence | one-shot; clocked (20–33 ms); app-limited trickle; bursts with think time; continuous | pacing restart after idle; coded window sizing (`RateBytesPerSec` prior, §9); application-limited detection |
| A5 | Lifetime and idle | under 1 s; seconds; session with gaps; idle for minutes to hours | classifier age thresholds; flow idle timeout; binding deadline; keepalive schedule; NAT state |
| A6 | Metric the user feels | completion time; first byte; per-message latency; gap and jitter tail; wake latency; goodput | which quantile is the product |
| A7 | Concurrency and multiplexing | 1 flow; N flows; N application streams inside one flow (HTTP/2) | classifier sees the aggregate; head-of-line inside one ordered stream; memory fairness |
| A8 | Phase change within a connection | none; burst→trickle; idle→wake; interactive→bulk→interactive | classifier hysteresis; class-dependent coding; lane and isolation decisions |
| A9 | Inner control loop | none; app congestion control (WebRTC GCC, inner QUIC); ABR; timers (keepalive, request timeout, DNS retry) | nested loops react to tunnel delay and hidden loss; a timer turns a latency tail into a failure |
| A10 | Elasticity | elastic (takes all capacity); fixed-rate; app-limited | whether a class weight binds; whether the flow is ever send-limited |
| A11 | Failure semantics | idempotent retry; resumable; session lost | whether a carrier replacement or idle EOF is visible to the user |

A1, A3, A4, A5 and A8 separate the clusters. A7, A9 and A10 are parameters inside a cluster.

## 2. Application catalogue and clustering

### 2.1 Catalogue

| Application | A1 | A2 | A3 burst | A4 cadence | A5 lifetime | A6 metric | A9 inner loop | Cluster |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| File download, package tarball, docker pull, VOD progressive | TCP | down | multi-BDP | continuous | s–min | goodput, completion | none | C1 |
| git clone | TCP | down | multi-BDP after a small negotiation | continuous | s–min | completion | none | C1 |
| Speedtest | TCP | down then up | multi-BDP, 8–16 flows, 10–15 s | continuous | 15 s | peak goodput | none | C1 + C2 (variant) |
| Upload, git push, CI artifacts, backup, cloud-sync upload | TCP | up | multi-BDP | continuous | s–h | goodput, completion | none | C2 |
| Web page, HTTP/1.1 | TCP | down | 20–60 sub-BDP objects on 6 conns per origin | burst then think | s | page load, first byte | none | C3 |
| Web page, HTTP/2 | TCP | down | same objects on 1 conn per origin | burst then think | s–min | page load | none | C3 (A7 variant) |
| API/RPC, small | TCP | sym | sub-IW | one-shot or warm | ms–min | latency p99 | request timeout | C3 (covered by the interactive probe) |
| npm/pip metadata, cloud-sync small files | TCP | down/up | hundreds of sub-BDP | continuous fan-out | s–min | completion | none | C3 |
| ASR request | TCP | up | 100 KB–1 MB, sub-BDP | one-shot | < 1 s | completion | none | C4 |
| TTS response, image upload in chat | TCP | down / up | 100 KB–1 MB | one-shot | < 1 s | completion | none | C4 |
| LLM streaming chat | TCP | up burst then down trickle | 30 KB–1.5 MB up, then ~200 B deltas | 20–100 ms for 10–60 s | 10–60 s; conn reused | first token, gap tail, stalls | SDK idle timeout | **C5** |
| Agent tool loop | TCP | same, repeated, growing | same per turn | turns separated by 1–5 s | minutes on one conn | per-turn first token, total | retries | C5 |
| gRPC/WebSocket server streaming, SSE dashboards, streaming ASR results | TCP | down trickle | small | 20 ms–1 s | min–h | gap tail | none | C5 |
| SSH, terminal, DB shell | TCP | sym thin | 1–100 B | human typing | min–h, idle gaps | echo p99 | none | C6 |
| SSH with output (`cat`, `git log`), remote desktop over TCP, VNC | TCP | thin up, bursty down | 50 KB–1 MB bursts | event-driven | min–h | echo p99 after a burst | none | C6 (phase variant) |
| Online game over TCP, collaborative editing | TCP | sym thin | < 1 KB | 10–60 Hz or events | min–h | latency p99 | none | C6 |
| Video call (WebRTC), VoIP | UDP | sym | frames 2–16 KB | 20 ms audio, 33 ms video | min–h | late loss, freezes | GCC | C7 |
| Online game over UDP, cloud gaming input | UDP | sym thin / down heavy | 100–300 B | 30–60 Hz | min–h | latency p99, loss | none / own | C7 (thin variant) |
| HTTP/3 browsing and downloads, YouTube over QUIC, WireGuard in tunnel | UDP | down | multi-BDP inside datagrams | continuous | s–min | goodput | inner QUIC CC and recovery | **C8** |
| Telegram, WeChat, MQTT, WebSocket presence | TCP | sym tiny | < 2 KB, after an initial sync of 50 KB–1 MB | pings 30–60 s, rare messages | hours | wake latency, survival | app ping timeout | **C9** |
| APNs/FCM push | TCP | down tiny | < 4 KB | heartbeat 15–28 min or none | hours–days | wake latency, survival | heartbeat | C9 (no-heartbeat variant) |
| IMAP IDLE, long poll | TCP | down tiny | < 2 KB | re-arm every 29 min / 30–60 s | hours | wake latency | re-arm timer | C9 |
| HLS/DASH live and VOD with ABR | TCP | down | 0.3–4 MB segment | every 2–6 s on a warm conn | min–h | rebuffer ratio, bitrate | ABR | C10 |
| Live publish (RTMP/SRT over TCP) | TCP | up | fixed 2–6 Mbit/s | continuous, inelastic | min–h | stall ratio | encoder buffer | C10 (up variant) |
| DNS over UDP | UDP | sym | 1 packet each way | one-shot | ms | latency, retry rate | 1–5 s retry timer | C11 |
| DNS over TCP / DoH | TCP | sym | sub-IW | one-shot / warm | ms | latency | timeout | C3 |
| Email send/fetch | TCP | up/down | sub-BDP | one-shot | s | completion | none | C3 / C2 |

### 2.2 Clusters

| Cluster | Defining characteristics | Members | Representative | Why it is distinct |
| --- | --- | --- | --- | --- |
| C1 Elastic bulk, down | TCP, multi-BDP, continuous, elastic | downloads, git clone, VOD progressive, docker pull, speedtest down | **W1** | the goodput gate; the client's uplink is only an ACK channel |
| C2 Elastic bulk, up | same, opposite direction | uploads, git push, CI artifacts, backups, sync, speedtest up | **W2** | directions are independent authorities (I10) with different loss; the client is the sender, so its memory budget binds |
| C3 Short-request fan-out | TCP, many sub-BDP objects, many cold flows at once, think time | browsing h1/h2, small RPC, package metadata, DoH, email | **W3** (+ interactive probe for the single small RPC) | connection set-up, start-up and tail loss decide; every flow is NEW |
| C4 Single transactional burst | TCP, one 100 KB–1 MB burst, completion is the only metric | ASR, TTS, large API payloads, chat attachments | **W4** (the DC gate) | one burst against cold or warm path knowledge |
| C5 Burst then trickle | TCP, large request, then long low-rate small messages; reused connection; phase change | LLM chat, agent loops, server streaming, SSE | **W5** | per-message gap tail on a reliable stream after a bulk-looking phase |
| C6 Thin interactive stream | TCP, tiny symmetric messages, human cadence, optional output bursts | SSH, terminals, TCP games, remote desktop over TCP | **W6** | per-message round trip; one loss costs a round trip unless coded |
| C7 Real-time datagrams | UDP, clocked, fixed rate, deadline, loss-tolerant | WebRTC, VoIP, UDP games, cloud gaming | **W7** | late is lost; the only cluster where delivery ratio is a metric |
| C8 Elastic UDP with inner CC | UDP, multi-BDP, own recovery and congestion control | HTTP/3, QUIC video, VPN-in-tunnel | **W8** | bulk behaviour inside the INTERACTIVE datagram class; nested control loops |
| C9 Long-idle persistent | TCP, hours, almost no bytes, wake latency and survival | messaging, push, IMAP IDLE, MQTT, long poll | **W9** | the only cluster governed by timers and state retention rather than by rate |
| C10 Deadline-paced bursts | TCP, periodic multi-hundred-KB bursts on a warm connection, ABR | HLS/DASH, live publish | **W10** (variant of the `rpc` generator) | on-off duty cycle; restart after idle every few seconds |
| C11 One-shot datagram | UDP, one packet each way, nothing follows | DNS, NTP, STUN | **W11** (variant of the `dgram` generator) | a coded tail with no later arrival; a 1 s+ application retry timer |

What collapses and what does not:

- git clone, package tarballs, VOD progressive and downloads are one workload. git's negotiation phase is a C3 request and adds nothing.
- CI uploads, backups, git push and sync uploads are one workload.
- A small RPC is the existing interactive probe. HTTP/1.1 and HTTP/2 browsing are one workload with a multiplexing parameter.
- ASR and TTS are one generator with the direction swapped. They are not the same as LLM: nothing follows the burst.
- LLM streaming is **not** SSH (it starts with a bulk-looking upload and is one-directional afterwards), **not** the DC burst (its product is the gap tail, not completion), and **not** bulk.
- Messaging is **not** SSH: an SSH session that idles 30 minutes behaves like C9, but SSH's product is echo latency while typing and messaging's is the first message after silence.
- Tunnelled QUIC is **not** a video call. Both are UDP, but one is elastic with its own recovery and the other is fixed-rate with a deadline. Queqiao cannot tell them apart today.
- Speedtests are W1 and W2 with 8 flows and a 10 s duration: a parameter, not a workload.

## 3. Representative workloads

### 3.0 Shared harness conventions

**Generators.** Five programs cover everything. A workload is a parameter file.

| Generator | Shape | Workloads |
| --- | --- | --- |
| `bulk` | N TCP flows, fixed duration or size, one direction | W1, W2, backgrounds |
| `rpc` | HTTPS request/response with given up/down sizes, fan-out, connection reuse, think time, optional HTTP/2 | W3, W4, W10, interactive probe |
| `stream` | one held TLS connection carrying scheduled small messages in either direction, with optional leading upload, idle periods and bursts | W5, W6, W9 |
| `dgram` | UDP through SOCKS5 UDP ASSOCIATE, clocked packets with sequence numbers, reflector on the far side | W7, W11 |
| `h3` | a real QUIC client (quic-go) over a SOCKS5-UDP `PacketConn`, HTTP/3 server on the far side | W8 |

**Far side.** One `wlserver` process next to the gateway (same host or same LAN, so the last hop adds under 1 ms). All TCP workloads use real TLS 1.3 with the harness certificate, so handshake round trips and record sizes are real.

**Clock-free lateness.** No workload depends on synchronised clocks. Every scheduled message `i` carries the sender's monotonic emission time `s_i`. The receiver records `r_i`. Then

- `δ_i = r_i − s_i` (contains the unknown clock offset),
- `lateness L_i = δ_i − min_j δ_j` over the same connection and direction (the offset cancels; drift at 20 ppm is 1.2 ms per minute and is ignored below 5 minutes, and re-based per segment above).

`L_i` is "how much later than the best this path did" for that message. It is used for LLM deltas, datagrams and pushes.

**Backgrounds.** `BG-none`; `BG-down4` = four `bulk` downloads for the whole trial (as ab6); `BG-up1` = one `bulk` upload for the whole trial. Backgrounds start 5 s before measurement so they are classified and at rate.

**Cold and warm.** Cold tunnel: both endpoints restarted, no path knowledge, no pooled connection. Warm tunnel: traffic within the previous 5 s. Cold app connection: new SOCKS5 CONNECT plus TLS handshake. Warm app connection: reused keep-alive connection. These are two independent switches.

**Trial, block, gate.** As design §15.3–15.4: a block is four trials (ABBA or BAAB) on one path with endpoints restarted before the block. Block counts and background cells below describe the opt-in full matrix; the gates are the Minimal live plan. The block is the unit of analysis. For a metric where smaller is better, the per-block statistic is computed per arm by pooling that arm's two trials, and the secondary gate is the one-sided test on

`d_b = (m × main_b + a) − candidate_b`, pass when the lower bound of mean `d_b`, at the current look's level (design §15.4), is above zero,

where `m` is the multiplicative margin and `a` the absolute slack given per metric below. For "larger is better" the contrast is `candidate_b − m × main_b` as in §15.4.

**Blocks needed** (opt-in full matrix only; the gates use the sequential rule of design §15.4). For a margin of 10 % and 80 % power the block count is `n ≈ (2.49 × CV / 0.10)²`, where `CV` is the standard deviation of the block contrast over main's mean. CV 0.2 gives 25 blocks, 0.3 gives 56, 0.5 gives 155. Each table below gives a planning number for that matrix.

**Mandatory telemetry for every trial**, exported per endpoint: bytes sent per (flow, class, substrate ∈ {stream, coded}), class transitions with timestamps, coded-path FIFO sojourn p99, UDP PACKET frames dropped at overflow, reissued bytes, carrier generation changes, memory high-water mark, admission refusals by reason.

### W1 Bulk download (existing)

| | |
| --- | --- |
| Generator | `bulk`, TCP, down. (a) 4 concurrent flows, 30 s. (b) 1 flow, 30 s. Variant `speedtest`: 8 flows, 10 s down then 10 s up, cold tunnel. |
| Stresses | path authority rate estimate, BULK class, stream repair, reverse ACK channel (flow ACK is CONTROL, QUIC ACK is EXEMPT), isolation and pool under legacy, memory at the gateway sender |
| Metrics | useful bytes delivered in the fixed duration (primary, §15.4); time to 90 % of final rate; reissued bytes per source byte; wire bytes per useful byte; stalls (≥ 1 s with zero delivered bytes) |
| Regression | primary gate 0.90 as designed. Secondary: stalls per trial ≤ main + 0.1; reissue ratio ≤ main × 1.25 |
| Trials | 30 s trial, block = 2.5 min with restarts. Plan 100 blocks on the high-loss path (an earlier estimate), 30 elsewhere |

### W2 Bulk upload (existing)

Same as W1 with the direction reversed: (a) 2 concurrent flows, 30 s; (b) 1 flow, 30 s. Run on the desktop profile and on the iOS memory profile (28 MiB soft limit, 1 MiB per-flow retention).

Adds to W1: the client is the data sender, so `mem=owned` reservations, retention autotune and the 64 KiB iOS stream window bind. Extra metric: client memory high-water mark, must stay within the profile. Same gates. Plan 30 blocks per profile.

### W3 Browsing fan-out (existing)

| | |
| --- | --- |
| Generator | `rpc`. A page = 1 HTML document of 30 KB, then 40 objects with log-normal sizes (median 15 KB, p95 300 KB, capped at 2 MB, total about 2 MB), spread over 6 origins. Each object: 600 B request up. Two modes per page, alternated: **h1** (up to 6 cold connections per origin, objects queued on them) and **h2** (1 connection per origin, all objects as concurrent streams). 5 s think time. Odd pages cold app connections, even pages reuse the previous page's connections. 8 pages per trial, about 60 s. Run with `BG-none` and `BG-down4`. |
| Stresses | 20–40 simultaneous OPENs (CONTROL class), NEW-flow start-up from path knowledge, coded path for flows under 256 KiB (`codedFlowBytes`), INTERACTIVE-vs-INTERACTIVE sharing, gateway session admission, HTTP/2 connections crossing the BULK threshold mid-page |
| Metrics | page load time `PLT = t(last byte of last object) − t(first CONNECT sent)`; HTML first byte `t(first response byte) − t(first CONNECT sent)`; time to 80 % of objects complete; per-object completion p95; failed objects |
| Regression | PLT p50 ≤ main × 1.10; PLT p95 ≤ main × 1.15 + 100 ms; failed objects ≤ main + 0.2 pp |
| Trials | 16 pages per arm per block (8 per mode). Plan 30 blocks → 480 pages per arm |

### W4 Transactional burst, the DC gate (existing)

`rpc`, one request at a time. ASR: 355 KB up, 200 B down. TTS: 300 B up, 300 KB down. Cold tunnel and warm tunnel, 20 requests per trial with 2 s spacing. Metrics and thresholds are those already fixed in design §11.3 (request latency within 10 % of legacy at median and p99). On the access-link profile the same generator is run with `BG-none` only, as a report.

Stresses: start-up from knowledge, burst allowance, coded versus stream choice at 256 KiB, and on `dc-long-haul` the 32 MiB bulk threshold and idle veto.

### W5 LLM streaming call (new, must-have)

The far side is a deterministic SSE emitter, not a real model, so server time is a known constant.

| Parameter | Value |
| --- | --- |
| Transport | TCP CONNECT, TLS 1.3, HTTP/1.1 keep-alive by default (what Python and Node SDKs use); `h2` variant for W5-h2 |
| Request | POST with body `U` bytes of incompressible data. `U ∈ {30 KB, 100 KB, 400 KB, 1.5 MB}`. 100 KB is the owner's case; 400 KB is 100 K tokens of text; 1.5 MB is a request with images |
| Server think | fixed `T = 500 ms` after the last request byte arrives. Variant `reasoning`: `T = 15 s` with a 12-byte SSE comment every 5 s |
| Response | `N = 400` SSE events, one per `c = 25 ms` (40 tokens/s), each 180 B ± 60 B of JSON, written and flushed individually. Event `i` carries `s_i`. The first event also carries the server-measured request receive duration `R = t(last request byte) − t(first request byte)`. Then `[DONE]` |
| Call sequence, **W5-single** | cold app connection, one call per connection |
| Call sequence, **W5-loop** | one connection, 5 turns. Turn `k` uploads `U + k × 8 KB`. 2 s gap between turns (tool execution) |
| Call sequence, **W5-swarm** | 8 concurrent W5-loop agents, start times staggered uniformly over 2 s |
| Trial | 5 calls (single) or 1 loop of 5 turns, about 65 s. Backgrounds: `BG-none`, `BG-down4`, `BG-up1` |
| Client timeout | 30 s with no bytes is a failure (SDK idle timeout) |

Mechanisms stressed: classifier phase change (§5 F1–F3); class-dependent coding for the deltas; upstream authority for the request burst; start-up after the think gap (pacing restart, coded window prior after idle, §9); flow ACK path for a download whose sender is almost always application-limited; HoL in one ordered stream at 15–50 % loss; per-flow retention for a 1.5 MB request on iOS.

Metrics, all from client timestamps plus the embedded `s_i` and `R`:

| Metric | Definition |
| --- | --- |
| Connect time | `t(TLS handshake done) − t(CONNECT sent)`; cold app connections only |
| Upload time | `R` (server-side duration, one clock) |
| TTFT | `t(first byte of event 1) − t(first request byte written)`. Report `TTFT_net = TTFT − T` |
| Delta lateness | `L_i` as §3.0, per call. Report p50, p95, p99, max |
| Inter-delta gap | `g_i = r_i − r_{i−1}`. Report p99 and max. The emitter's own gap is 25 ms |
| Stall | `g_i ≥ 500 ms` (visible hesitation, over 2 RTT). Freeze: `g_i ≥ 2 s`. Report stalls and freezes per streamed minute |
| Stall time ratio | `Σ max(0, g_i − 250 ms) / (r_N − r_1)` |
| Completion | `t([DONE]) − t(first request byte written)`; also reported as excess over the emitter's own schedule, `T + N × c` |
| Failure | reset, premature EOF, or client timeout. Count per call |
| Class correctness | fraction of response-direction DATA bytes sent as INTERACTIVE, from gateway telemetry |

Provisional bounds (candidate against main, same classifier in both arms):

| Metric | Bound |
| --- | --- |
| `TTFT_net` p50 | ≤ main × 1.10 |
| `TTFT_net` p95 | ≤ main × 1.15 + 50 ms |
| Lateness p99 | ≤ main × 1.10 + 20 ms |
| Stalls per streamed minute | ≤ main + 0.2 |
| Freezes per streamed minute | ≤ main + 0.05 |
| Call failures | ≤ main + 0.5 pp |
| Class correctness | ≥ 99 % of delta bytes INTERACTIVE. Hard assertion, not statistical. Fails today for `U ≥ 128 KiB`; see §5 |

Trials (opt-in matrix): a block gives each arm 10 calls and 4,000 deltas. Plan 30 blocks → 300 TTFT samples and 120,000 deltas per arm per variant. Run the full `U` sweep with `BG-none` once, and `U = 100 KB` and `U = 400 KB` under all three backgrounds. The gate is trial B and C's `U = 100 KB`.

### W6 Terminal keystrokes with output bursts (existing, extended)

| | |
| --- | --- |
| Generator | `stream`, one TLS connection, far side echoes. Keystroke: 36 B record up, 36 B echo down. Inter-key gap: exponential with mean 150 ms, floor 30 ms. **Phase A** 0–40 s: keystrokes only. **Phase B** at 40 s: the far side sends a 300 KB burst (a `cat`). **Phase C** 42–120 s: keystrokes continue; every 20 s another 300 KB burst. Backgrounds: `BG-none`, `BG-down4`, `BG-up1`. |
| Stresses | coded path for a thin stream (one lost frame costs a round trip unless duplicated), INTERACTIVE priority under BULK, control boost on a flow's own stream, the BULK latch after the burst (§5 F2) |
| Metrics | echo time `e_k = t(echo byte received) − t(key written)`; p50, p95, p99, and fraction over 1 s. Reported separately for phase A and phase C. **Latch ratio** = p99(C) / p99(A), keystrokes within 2 s after a burst excluded |
| Regression | p99 ≤ main × 1.10 + 20 ms in each phase and each background; fraction over 1 s ≤ main + 0.2 pp; latch ratio ≤ 1.15 (absolute, after the classifier change of §5) |
| Trials | about 750 keystrokes per trial, 1,500 per arm per block. Plan 30 blocks → 45,000 keystrokes per arm |

The earlier "interactive probe under bulk" is phase A of this workload; in the gates it is B's keystrokes before the burst, under two downloads.

### W7 Video call (existing, made precise)

The brief says "2–4 Mbit/s, one packet per 20 ms". One packet per 20 ms at 2 Mbit/s would be a 5 KB datagram, so the two numbers describe two sub-streams. Defined here as:

| | |
| --- | --- |
| Generator | `dgram`, one UDP association, both directions simultaneously, open loop (no rate adaptation, so the transport is measured and not the adapter). **Audio**: 160 B every 20 ms (64 kbit/s). **Video**: 30 frames/s, each frame split into 1,100 B packets sent back to back; frame size 10 KB (2.4 Mbit/s), with a 40 KB key frame every 2 s (average about 3 Mbit/s). Every packet carries stream id, sequence, frame id, packet-in-frame index and `s_i`. 60 s. Variant `game`: 100 B at 60 Hz up, 300 B at 30 Hz down, no video. Backgrounds: `BG-none`, `BG-down4`. |
| Stresses | coded datagram path and its window sizing, the INTERACTIVE class against BULK, UDP input-queue overflow (drop-oldest whole PACKET), burst allowance for key frames, both directions' authorities at once, the 64-packet replay window, association rescue |
| Metrics per direction | delivery ratio; **late loss** = fraction with `L_i > 150 ms` or missing (150 ms is the playout budget on top of the path's own delay); lateness p99; longest run of consecutive lost-or-late audio packets; **audio gaps** = runs ≥ 3 packets (60 ms) per minute; **frame loss** = frames with any packet lost or late; **freezes** = runs ≥ 6 frames (200 ms) per minute; duplicates and reordering depth |
| Regression | audio late loss ≤ main + 0.5 pp; frame loss ≤ main + 1 pp; freezes per minute ≤ main + 0.2; lateness p99 ≤ main × 1.10 + 10 ms; under `BG-down4` the same bounds |
| Trials | 3,000 audio packets and 1,800 frames per direction per trial. Plan 25 blocks |

A real WebRTC pair (pion, GCC enabled) through the same association is a nice-to-have validation run. It answers a different question: whether the adapter's delay-based estimator misreads the tunnel.

### W8 Tunnelled QUIC (new, must-have)

*Gate: deterministic test on an emulated path (Minimal live plan); the live form below is opt-in.*

| | |
| --- | --- |
| Generator | `h3`: quic-go HTTP/3 client whose `PacketConn` is a SOCKS5 UDP ASSOCIATE; HTTP/3 server in `wlserver`, default CUBIC and default loss recovery. **(a) download**: one 20 MB GET, or 30 s, whichever is first. **(b) page**: W3's object list as 40 concurrent HTTP/3 streams on one connection, 8 pages. **(c) reference**: the same 20 MB object over TCP CONNECT in the same block. |
| Stresses | heavy UDP at INTERACTIVE weight (design §9, Q5); the single ordered symbol stream as a FIFO; FEC on top of an inner protocol that also repairs; UDP overflow drops read by the inner CC as congestion; nested RTT estimation (inner PTO against tunnel repair delay); MTU of an inner 1,200–1,350 B QUIC packet plus SOCKS and frame headers, and fragmented PACKET handling |
| Metrics | goodput of (a); PLT of (b); inner QUIC statistics from the client's connection stats: packets lost, PTO count, smoothed RTT minus the path's minimum RTT (the tunnel's queueing as the inner flow sees it); tunnel telemetry: PACKET frames dropped at overflow, coded FIFO sojourn p99, wire bytes per useful byte; ratio (a)/(c) |
| Regression | goodput (a) ≥ 0.90 × main (same contrast as §15.4); inner loss rate ≤ main + 1 pp; wire bytes per useful byte ≤ main × 1.15. The ratio (a)/(c) is reported and not gated |
| Trials | 30 s trial. Plan 30 blocks |

### W9 Idle wake (new, must-have)

Two parts, because wake latency needs many samples and survival needs long waits.

**W9a Short-idle wake cycle (statistics).** *Gate: trial D, a one-session minimal form (Minimal live plan).* `stream`, 50 concurrent TLS connections. On connect each receives a 200 KB sync burst (login history), then goes silent. Each connection then repeats: idle `I` seconds, one **server push** of 300 B carrying `s`, client replies with a 60 B ack; then 5 follow-up pushes at 1 s spacing (the warm reference); idle `I` again; one **client send** of 300 B, far side acks 60 B. `I` drawn per cycle from {30 s, 60 s, 120 s, 300 s}. Trial 20 minutes. App-level ping variant: 40 B ping and pong every 60 s.

**W9b Long-idle survival (counts).** *Gate: deterministic test with an injected clock (Minimal live plan); the live sessions below are opt-in.* 120 connections per arm, opened together, 200 KB sync, then silence. Idle targets {5, 14, 16, 29, 31, 45, 60 min} × 3 heartbeat modes {none, 60 s ping, 10 min ping} × 2 wake directions {server push, client send}, plus spares; one wake per connection, then 5 warm follow-ups. Session length 65 minutes. The 14/16 and 29/31 pairs bracket the observed 900 s EOF and the 30-minute `FlowIdleTimeout` default.

Idle connections do not contend, so **both arms run concurrently** from the same host in W9a and W9b. This gives perfect pairing and halves wall time. One `BG-down4` variant of W9a checks wake under load (also M6).

Mechanisms stressed: flow idle timeout (payload-only activity, `mpflow.go:3366-3367`, default 30 min at `mpflow.go:95`); binding deadline for an idle flow and rebind after a carrier replacement (§10.3); adaptive keepalive schedule (§10.2, fork change 11); gateway session and account slots held by idle flows; memory for hundreds of idle sockets (test 14); pacing and coded-window restart from idle; the BULK latch set by the sync burst (§5 F2); close propagation.

| Metric | Definition |
| --- | --- |
| Push wake penalty | `δ_wake − min(δ of the 5 follow-ups)`. Clock-free |
| Send wake time | `t(ack received) − t(send written)`; penalty = that minus the minimum over warm echoes on the same connection |
| Delivered | wake message arrived within 5 s. Counted per idle target, heartbeat mode and direction |
| Death mode | for an undelivered wake: `EOF`, `RST`, `timeout` (client's own 30 s), or `silent` (no local signal at all before the wake was attempted) |
| Idle EOF time | for a connection that closes while idle: seconds from last payload to the local close event |
| Close propagation | when one side's flow ends, time until the other side's application sees EOF or RST |
| Idle cost | tunnel wire bytes per idle connection per hour, from client telemetry during an all-idle window |
| Memory | client and gateway high-water mark with all connections idle |

Provisional bounds:

| Metric | Bound |
| --- | --- |
| Push and send wake penalty, p50 (W9a) | ≤ main × 1.10 + 20 ms; absolute target ≤ 0.25 RTT |
| Push and send wake penalty, p99 (W9a) | ≤ main × 1.10 + 50 ms; absolute target ≤ 1.5 RTT |
| Delivered, heartbeat 60 s, every idle target | 100 %. Hard |
| Delivered, no heartbeat, targets below the configured flow idle timeout | ≥ main, and any count drop of 3 or more out of 20 fails |
| Silent deaths | 0. Hard. A flow the tunnel ended must reach the application as EOF or RST within 5 s |
| Idle cost | ≤ main × 1.20 |

Trials: in W9a one push-plus-send cycle takes about 265 s on average, so 50 connections give about 225 wakes per direction per arm per 20-minute session; 6 sessions → about 1,350 per direction per arm, enough for a p99. W9b has 42 cells, so 120 connections give about 2.9 per cell per session; 8 sessions (nights) → about 23 per cell. Survival is reported as counts with Clopper–Pearson intervals; latency for W9b is p50 and max only.

### W10 Segment fetch (deterministic test; live form opt-in)

*Gate: deterministic test on an emulated path (Minimal live plan); the live form and its block count below are opt-in.* `rpc` on one warm connection: a 1.5 MB GET every 4 s for 60 s (15 segments), down. Variant `publish`: `stream` writing 4 Mbit/s upstream at constant rate for 60 s. Metrics: segment download time; **rebuffer ratio** = `Σ max(0, d_k − 4 s) / 60 s`; throughput as an ABR would see it (`size / d_k`, p10). Bound: p10 throughput ≥ 0.90 × main; rebuffer ratio ≤ main + 1 pp. Stresses restart after idle every 4 s and a classifier that flips with the duty cycle once it is no longer sticky. 20 blocks.

### W11 One-shot datagram (deterministic test; live form opt-in)

*Gate: deterministic test on an emulated path (Minimal live plan); the live form is opt-in.* `dgram`: a fresh UDP association per query half the time, a reused one otherwise; one 60 B packet up, one 300 B packet down, nothing else; one query per second for 60 s. Metrics: response time p50/p99; fraction over 1 s (a stub resolver's first retry); loss. Bound: fraction over 1 s ≤ main + 0.5 pp. Stresses association set-up cost and a coded tail with no later symbol (test 15, "a lost tail with no further arrival"). With SOCKS5 hostname CONNECT the gateway resolves names and this workload does not occur; it matters for clients that capture all UDP.

## 4. Mixed scenarios

In the full matrix every mixed scenario also runs W6 phase A (keystrokes) as a canary, because it is cheap and is the continuity metric with ab6, and each foreground metric is compared with the same bound as in its solo workload, computed in the mixed run against main's mixed run. Optional runs (M3, M7) report these comparisons; they do not gate.

| ID | Composition | Cross-class interaction it isolates | Key readout |
| --- | --- | --- | --- |
| M1 | W5-single (U = 100 KB, 400 KB) + `BG-down4` | INTERACTIVE trickle against BULK in the **same** direction; mislabel cost is maximal here | delta lateness p99, stalls, bulk bytes |
| M2 | W5-single + `BG-up1` (the owner's example) | request burst against BULK upload; the delta stream's flow ACKs and QUIC ACKs share the loaded uplink (CONTROL and EXEMPT against BULK) | upload time `R`, TTFT, sender-side credit delay p95 |
| M3 | W5-swarm (8 agents) + W3 tool calls (one 8-object fan-out per agent turn), no bulk | INTERACTIVE against INTERACTIVE: no class boundary protects anything; simultaneous request bursts of 8 × 100 KB+; memory fair grants | TTFT p95 across agents, worst agent's stall rate, memory high-water |
| M4 | W7 call + W3 pages + `BG-down4` | datagram stream against NEW-flow fan-out against BULK; the three-way weight split and key-frame bursts | audio late loss, freezes, PLT p95, bulk bytes |
| M5 | W7 audio-only + W8 download + W6 | heavy UDP and thin UDP in one ordered symbol stream, plus coded TCP chunks in the same FIFO (Q5) | audio late loss and lateness p99, keystroke p99, coded FIFO sojourn, PACKET drops |
| M6 | 200 idle W9 connections + W9a wake cycle on 50 of them + `BG-down4`, iOS memory profile on the client | wake from idle under load; idle state against the memory budget; rebind of idle flows if a carrier is replaced | wake penalty p99, delivered count, memory high-water |
| M7 | "Day in the life", 30 min: W3 page every 20 s, W5-loop continuously, W7 call for 10 min, one W1 download and one W2 upload of 200 MB each at random times, 50 idle W9 connections | none in isolation; a soak that every solo bound must still hold in | all solo metrics, failures, carrier replacements, memory |

Coverage check:

| Interaction | Covered by |
| --- | --- |
| INTERACTIVE stream vs BULK, same direction | M1, W6 + `BG-down4` |
| INTERACTIVE stream vs BULK, opposite direction; ACK channel under load | M2, W6 + `BG-up1` |
| CONTROL (OPEN storm) vs data classes | W3 + `BG-down4`, M4 |
| INTERACTIVE datagrams vs BULK | W7 + `BG-down4`, M4 |
| INTERACTIVE vs INTERACTIVE, streams | M3 |
| Datagram vs datagram, and datagram vs coded stream chunks | M5 |
| Idle state vs load and memory | M6 |
| Everything, long | M7 (existing "mixed") |

In the minimal live plan M1 is covered by B, M2 and M4 by C, M6 by D beside C, and M5 by a deterministic test; M3 and M7 are optional manual runs.

## 5. Classifier implications

### 5.1 What the code does

Default profile (`wan-shared-bottleneck`, `internal/profile/profile.go:151`) uses `classifier.DefaultConfig()` (`internal/classifier/classifier.go:82-100`): `BulkBytes` 128 KiB, `BulkMinimumAge` 1 s, `NewAge` 3 s, idle veto off.

| Rule | Code |
| --- | --- |
| A flow is BULK when age ≥ 1 s **and** up + down ≥ 128 KiB **and not** (bidirectional and small-recent) | `classifier.go:206-213` |
| small-recent = each direction carried ≤ 64 KiB in the current plus the previous 1 s window | `internal/pep/mpflow.go:3412`, `3320`, `3326` |
| The window rolls only when an observation arrives at least 1 s after the window started, and it rolls once, whatever time has passed | `mpflow.go:3345-3349` |
| bidirectional = both lifetime byte counts non-zero, which is true for any TLS connection | `mpflow.go:3385` |
| BULK is permanent; `Observe` returns immediately once set, and `Declare` cannot undo it | `classifier.go:178-183`, `150-160` |
| NEW becomes INTERACTIVE only at age ≥ 3 s; both are the same scheduling class | `classifier.go:198-202`; design §4.3 table |
| The idle veto can only prevent BULK, permanently, and is off on the default profile | `classifier.go:184-193`, `217-222`; `profile.go:185-194` sets it for `dc-long-haul` only |
| Observations happen when bytes are read from the local application, when bytes are delivered to it, and at most every 250 ms when the sender asks whether to code | `internal/pep/stripedsend.go:290-295`; `mpflow.go:3180`; `mpflow.go:1276-1341` |
| Each endpoint classifies on its own; the gateway's label governs the response direction | `internal/pep/server.go:1190`, `2000-2010` |
| The class decides the frame's class byte, the lane queue, the aggregate budget's interactive flag, whether to code, and lane isolation | `stripedsend.go:612-615`; `mpflow.go:2290-2320`; `mpflow.go:1281-1298`; `internal/pep/client.go:2737`, `3022`; `stripedsend.go:584-586` |
| UDP PACKET frames are INTERACTIVE by constant; no classifier runs | `internal/pep/udp.go:562-565`, `965-968` (PACKET); `363`, `464`, `933` set the same constant on CLOSE, OPEN and ACK |
| Flow activity for the idle timeout is payload only; default 30 min | `mpflow.go:3366-3367`, `95`, `1834-1880`; `cmd/queqiaod/main.go:616` |

### 5.2 Label over each workload's lifetime

| Workload | Label over time (default profile) | Verdict |
| --- | --- | --- |
| W1, W2 bulk | NEW for the first second (coded up to 256 KiB, `mpflow.go:1306-1312`), then BULK | correct |
| W3 h1 page | each connection NEW; one that carries ≥ 128 KiB and lives ≥ 1 s becomes BULK | acceptable; small objects finish as NEW |
| W3 h2 page | the one connection per origin crosses 128 KiB within the first second of the page and is BULK from age 1 s, **permanently**. Later API calls and the next page's HTML on that connection are BULK | **mislabel after the load ends** (F2) |
| W4 ASR/TTS, default profile | 355 KB in under 1 s: NEW if the flow ends before age 1 s, otherwise BULK for its tail | acceptable on this profile; correct on `dc-long-haul` (32 MiB threshold) |
| W5, U = 30 KB | NEW → INTERACTIVE at 3 s; deltas coded | correct |
| W5, U = 100 KB, first call on a connection | about 105 KB total, under the threshold; stays INTERACTIVE unless the first second of response pushes the total over 128 KiB while the stale window still holds the upload (F3). A request between roughly 120 and 128 KiB flips | **knife-edge** (F1, F3) |
| W5, U ≥ 128 KiB (400 KB, 1.5 MB) | client: BULK at age 1 s during the upload. Gateway: BULK when the upload arrives or, at the latest, on the first delta it reads, because the stale window still shows the upload. Permanent | **mislabel for the entire response** (F1) |
| W5-loop, any U ≥ 64 KiB | second turn: the total is already over 128 KiB and the new upload exceeds 64 KiB in the window → BULK at both ends for the rest of the connection | **mislabel from turn 2** (F1) |
| W5-h2 swarm | all agents on one flow; the aggregate is BULK almost immediately | **mislabel** (F1, F6) |
| W6 phase A | INTERACTIVE, coded | correct |
| W6 phase C | the 300 KB burst exceeds 64 KiB in the window with total over 128 KiB → BULK at the gateway and the client; keystrokes afterwards are BULK and uncoded | **mislabel** (F2) |
| W7 call | INTERACTIVE constant | correct |
| W8 QUIC download | INTERACTIVE constant, coded, in the same ordered symbol stream as W7 and as coded TCP chunks | **mislabel by construction** (F5; design Q5) |
| W9 | the 200 KB sync makes the connection BULK at age 1 s; every later push and ping is BULK and uncoded. Without a sync it stays INTERACTIVE | **mislabel** (F2) |
| W10 segments | BULK from the first segment | acceptable |
| W11 DNS | INTERACTIVE constant | correct |

### 5.3 Findings

**F1. A large-context LLM call is BULK for its whole response.** Three independent routes lead there: a single request ≥ 128 KiB; a second request on a reused connection; several calls multiplexed on one HTTP/2 connection. Consequences:

- *Today, with the legacy bundle:* the gateway's deltas are not coded (`mpflow.go:1281-1285`). On a path with 15–50 % erasure each lost delta packet waits for a stream retransmission and holds every later delta behind it. The flow is also a candidate for bulk isolation (`client.go:2737`).
- *Under `sched=class`:* the deltas are charged to BULK, the lowest weight, and compete inside that class with elastic downloads. With `topology=single` there is no separate connection to escape to.
- Confirm with: a unit test feeding `observe` a 400 KB upload, a 500 ms pause, then 200 B reads at 25 ms; assert the class on the gateway side. Then W5 class correctness.

**F2. The BULK latch is wrong for every multi-phase connection.** SSH after a large output, an HTTP/2 connection after a page, a messaging connection after login sync, remote desktop after a screen refresh. The comment that justifies stickiness (`classifier.go:113-115`) protects a large transfer from a short gap. The `dc-long-haul` notes (`classifier.go:64-78`) already record the mirror-image failure, a 240 MB pull stuck INTERACTIVE for 228 MB. The defect in both is permanence.

**F3. The recent window goes stale across an idle gap.** `recentBytes` shifts `current` into `prior` once, however long ago the window started (`mpflow.go:3345-3349`). The first observation after any idle period therefore still sees the pre-idle burst as recent, for a further second. This is what makes the gateway label a ≥ 128 KiB request BULK on the first delta even when the upload finished before age 1 s, and what creates the 120–128 KiB knife-edge. Confirm with: observe 100 KB up, advance the clock 10 s, observe 1 B down; assert `SmallBidirectionalBursts`.

**F4. The idle veto does not help here.** It is off on the default profile, it fires only after two idle episodes, and an LLM flow is already BULK during its first upload. It is also permanent in the other direction.

**F5. UDP has no classification at all.** A tunnelled QUIC download is served at INTERACTIVE weight, twice BULK's, and shares one blocking FIFO with voice packets and with the coded chunks of interactive TCP flows (design §9). After the stage-10 flip a QUIC download would out-weigh a TCP download 2:1 and delay LLM deltas and keystrokes that ride the coded path. Rate alone cannot separate it from a 4 Mbit/s call; elasticity can.

**F6. HTTP/2 hides streams.** One flow carries many application streams and the classifier, by design, does not look inside TLS (`classifier.go:1-3`). Per-stream classes are not available without breaking that. A class that follows current behaviour (below) is the achievable substitute: the connection is BULK while it is transferring and INTERACTIVE when it is not.

**F7. Short flows are never BULK.** A flow that ends before age 1 s is NEW throughout. A package install of hundreds of sub-second objects is continuous INTERACTIVE-class traffic. Under `sched=class` nothing inside the class separates it from keystrokes. This is acceptable now; M3 and W3 + W6 measure it.

**F8. Idle handling is not a classifier problem but shows up in the same workloads.**

- Only payload refreshes a flow's activity (`mpflow.go:3366-3367`). Kernel TCP keepalives on the two outer legs (30 s: `client.go:833`, `internal/pep/destination.go:67`) keep those sockets alive but do not count.
- The default `FlowIdleTimeout` is 30 min. FCM's Wi-Fi heartbeat is 28 min; IMAP IDLE re-arms at 29 min. The margin is two minutes and one minute.
- The production 900 s EOF has no matching default on `origin/main`. Candidates: an operator-set `--flow-idle-timeout`, a middlebox or `relay` hop (the relay's own default is 2 min, `cmd/queqiaod/relay.go:103`), or the destination. W9b's 14/16-minute cells and the idle-EOF-time and death-mode metrics identify which side closes first.

### 5.4 Recommended changes

| ID | Change | Detail | Blocks |
| --- | --- | --- | --- |
| R1 | **Split the class into a sticky fact and a current state** | `everBulk` (sticky, as today) keeps driving isolation, pre-warm and lane choice (`client.go:2737`, `3022`; `stripedsend.go:584`). `nowBulk` drives the frame's class byte, the lane queue, the budget flag and the coding choice (`stripedsend.go:614`, and the `carriesBulk` sample passed down at `456`, `473`, `637`; `mpflow.go:2291`, `2311-2319`, `1281`). Lane machinery does not flap; scheduling follows behaviour. | **stage 6/7 campaigns** |
| R1a | Sparse-write rule (demotion on cadence) | A write that finds the flow's send queue empty and is ≤ 2 packets is sent INTERACTIVE and coded, whatever `nowBulk` says. This is the sparse-flow rule of fq_codel applied per flow. It covers LLM deltas, keystrokes after `cat` and pushes after sync immediately, with no timer. A transferring flow almost never has an empty queue, so it cannot use this to jump the class. | with R1 |
| R1b | Windowed exit from BULK (hysteresis) | `nowBulk` clears after 2 consecutive full windows (2 s) with ≤ 64 KiB in each direction and an empty send backlog. It sets again when the flow has had a continuous backlog for `max(1 s, 4 × SRTT)`; the cumulative byte test is already met. Asymmetric on purpose: slow to leave, slower to bounce. | with R1 |
| R2 | Fix the stale window | In `recentBytes`, when `now − recentStart ≥ 2 × exchangeWindow`, zero `prior` as well. One line plus the test in F3. | **stage 6/7 campaigns** |
| R3 | Per-byte class telemetry | Bytes sent per (flow, class, substrate) and class transitions with timestamps, exported per trial. Without it W5's class-correctness assertion and every mislabel above are invisible in a goodput gate. | **stage 6/7 campaigns** |
| R4 | UDP: fair input to the coded path | Deficit round robin across associations and coded TCP chunks at the coded path's input, before frames enter ESI order, with the same sparse rule as R1a. The design already places overflow at that input (§9), so this needs no wire change and does not reorder symbols. | stage 10 |
| R5 | UDP: decide Q5 with data | Per-association rate and elasticity meter (does the rate follow what it is given?). Then choose from W8 and M5 results between charging heavy associations as BULK through a second datagram queue, or leaving them INTERACTIVE with R4. | stage 10 (decision), not the mechanism |
| R6 | Idle semantics | Make the tunnel never the shortest timer: raise the default flow idle timeout to at least 2 h while both outer TCP legs still answer kernel keepalives, and guarantee that a flow the tunnel ends reaches both applications as EOF or RST within 5 s. Localise the 900 s EOF with W9b first. | stage 8 (it activates the keepalive schedule) and stage 10 |
| R7 | Do not use `ClassHints` for this | `Declare(Interactive)` buys only the first second and cannot prevent or undo BULK (`classifier.go:138-160`). Hints do not fix F1 or F2. | — |

**Why R1–R3 block the stage 6/7 validation and the rest do not.** Stage 6 requires `sched=class`, and stage 7 removes isolation. A flow wrongly labelled BULK loses more in the candidate arm (lowest weight on one shared connection) than in main (its own lane, per-lane interactive reserve). A stage-7 A/B run with today's classifier would attribute that loss to `topology=single`. The fix is to land R1–R3 first as their own switch (`classifier=v2`), gate that switch on trials A (0.90 goodput, to prove R1b does not cost bulk), B and D, and then run stages 6 and 7 with it enabled in both arms. R4–R6 concern behaviour that is unchanged from main in stages 6 and 7 (the design keeps UDP at INTERACTIVE deliberately), so they wait for the flip.

## 6. Prioritisation

Priorities follow the Minimal live plan: the live gates are trials A–D and DC, and the deterministic tests replace the long and the UDP-heavy entries.

### 6.1 Before the stage 6/7 campaigns

| Item | Reason |
| --- | --- |
| Trial A (W1, 4 downloads) | the designed primary gate |
| Trial D for the `classifier=v2` switch, beside A and B blocks | R1–R3 change labels after a sync burst |
| Trial B (W5-loop at 100 KB, W6 with one burst, under bulk; covers M1 and the interactive probe) | the owner's case; classifier and class scheduler interact here; continuity with ab6, plus the latch ratio |
| R1, R2, R3 | §5.4 |

### 6.2 Must-have before the stage-10 default flip

| Item | Reason |
| --- | --- |
| Trials A and B | as above; rerun on the final bundle only if a later stage changes what they exercise |
| DC | stage 9's gate |
| Trial C (W1, W2, W3, W5, W6, W7 together; covers M2, M4) | goodput in both directions, OPEN storms, the datagram class against BULK, every class at once |
| Trial D (W9a at 1, 5, 16 min beside C; covers M6) | stage 8 changes keepalive and binding ownership; idle regressions are invisible to every other workload |
| W9b, W8, M5, W10, W11 deterministic tests | long idle and heavy UDP, without overnight or UDP-heavy live runs; Q5 is decided on W8 and M5 data |

### 6.3 Optional

| Item | Why it can wait |
| --- | --- |
| M3 swarm, M7 soak | manual runs; the release soak still follows the flip |
| W1 one-flow and `speedtest`, W2 iOS profile, W3 sweep, W5 `U` sweep, h2 and `reasoning`, W7 `game` | variants of entries the gates already cover |
| Real-application runs: a browser page set, a pion WebRTC call with GCC, a real LLM API, a real Telegram client | validation that the synthetic generators predict the real thing; not usable as gates because the far side is not controlled |

### 6.4 Campaign cost

Per path, a block is 4 trials with restarts. Each trial type runs 6 blocks, then one at a time until decided, at most 12 (design §15.4).

| Trial | Per block | 6 blocks | 12 blocks (cap) |
| --- | --- | --- | --- |
| A | ~5 min | 30 min | 1 h |
| B | ~7 min | 42 min | 1.4 h |
| C | ~9 min | 54 min | 1.8 h |
| DC | ~5 min | 30 min | 1 h |
| D | one 17-minute session, during C's first two blocks | 0 | 0 |
| **Total** | ~26 min | **2.6 h** | 5.2 h |

Most trial types are expected to decide at 6–7 blocks: about 2.5–3 h per path when each trial type runs once, at its gate. A pass carries forward to the flip; a trial is rerun on the final bundle only when a later stage changes what it exercises, and each rerun adds its row's cost. The deterministic tests run in CI and cost no live time. The opt-in full matrix of §3–§4 is tens of hours per path and is not planned.

Interleave trial types round-robin at block granularity rather than running one type's blocks back to back, so that a path that drifts between 0 % and 50 % loss within minutes drifts under all of them equally.

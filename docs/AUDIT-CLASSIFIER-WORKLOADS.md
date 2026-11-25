# Classifier audit against the §15.3 workloads

Status: **read-only audit**, for `DESIGN-PATH-AUTHORITY.md` §15.3 and stage 2. Line numbers refer to `feat/stage2-sched-class` at `d349fa0` (PR #181). Nothing here changes code.

## What decides a frame's class under `sched=class`

- **Frame to class.** `frameSendClass` (`internal/pep/sched.go:33-48`): DATA whose header class is BULK is BULK; any other DATA is INTERACTIVE; PACKET is INTERACTIVE; everything else is CONTROL. DATA sent as coded datagrams is INTERACTIVE whatever its header says, because the datagram queue is (design §9).
- **Header class.** Stamped from the flow's current class when the frame is built (`internal/pep/stripedsend.go:612-615`). Each endpoint stamps its own sending direction from its own classifier. The gateway creates one per flow (`internal/pep/server.go:1196`) and never reads the peer's class byte for DATA.
- **Classifier inputs.** `multipathFlow.observe` (`internal/pep/mpflow.go:3363-3421`) runs on every local read (`stripedsend.go:291-294`, counted as up), every delivery to the application (`mpflow.go:3180`, down), and on a 250 ms refresh (`mpflow.go:1317`, `1329-1341`; driven by the client's flow monitor at `client.go:2973` and by the coding decision at `mpflow.go:1280`). `SmallBidirectionalBursts` means that each direction carried at most 64 KiB in the last 1–2 s (`mpflow.go:3320`, `3326`, `3412`).
- **Transitions** (`internal/classifier/classifier.go:178-213`, defaults `82-100`).
  - NEW → BULK at age ≥ 1 s and ≥ 128 KiB total, unless the flow is bidirectional with small recent bursts.
  - NEW → INTERACTIVE at age ≥ 3 s.
  - **BULK is never left** (`181-183`, pinned by `TestBulkClassDoesNotFlap`). The idle-gap veto that can keep a flow out of BULK is off in the default `wan-shared-bottleneck` profile (`internal/profile/profile.go:151`) and on only in `dc-long-haul` (`profile.go:185-194`).
  - A process hint (`Declare`, `classifier.go:150-160`) is applied only on the client (`client.go:3650`). A declared INTERACTIVE is not sticky.
- **Weights.** `DefaultClassWeights` are 4:2:1 (`internal/congestion/classarb.go:20`). With CONTROL idle, INTERACTIVE gets two thirds of a saturated path against BULK.

## Workload traces

| Workload | Lifetime class (each sending endpoint) |
| --- | --- |
| `browsing` | NEW, hence INTERACTIVE, for each connection's first second. A connection that moves more than 64 KiB in a window after 1 s and has 128 KiB total becomes BULK for life. |
| `ssh` | NEW, then INTERACTIVE at 3 s. Stays INTERACTIVE while both directions stay under 64 KiB per window. One output burst over that, once the session has 128 KiB total, makes it BULK for life. |
| `video-call` (UDP) | PACKET frames on the datagram substrate (`internal/pep/udp.go:562-565`): INTERACTIVE always. There is no classifier. |
| `upload` | BULK after 1 s and 128 KiB, decided by the client, which is the sender. The gateway reaches the same class from the delivered bytes and uses it only for its CONTROL frames. |
| bulk download | INTERACTIVE for the first second, then BULK. On a path where the flow's rate is under about 64 KiB/s per window it never becomes BULK. |

## Risks

### R1. A burst latches an interactive session into BULK for life

- **Current behaviour.** `cat` of a large log, `scp` or port-forwarded traffic over the same SSH connection, or any burst over 64 KiB in a window once the session has 128 KiB total, sets BULK permanently (`classifier.go:181-183`, `206-213`). It happens at both ends, because both observe the burst. A client process hint cannot prevent it: `Declare(INTERACTIVE)` is not sticky, and the gateway never sees the hint.
- **Under `sched=class`.** From then on every keystroke and echo is BULK DATA at weight 1 of 7 behind any other bulk flow. It also loses coding (`prefersCodingOverRetransmission`, `mpflow.go:1281-1285`) and is eligible for bulk-lane isolation (`client.go:2743`). Echo RTT under bulk becomes the bulk queueing delay until the session ends.
- **Fix.**
  - Classify DATA by backlog rather than by history, or give each flow an INTERACTIVE token bucket. A flow may send up to `R_I` bytes/s, with burst `B_I`, as INTERACTIVE; only the excess is BULK.
  - With `R_I` = 64 KiB/s, which is `smallExchangeBytes`, a log dump's excess is BULK while it lasts and the next keystroke is INTERACTIVE again. It also replaces the first-second exemption below.
  - The minimal alternative is demotion with hysteresis. BULK → INTERACTIVE after `k` consecutive windows (for example 3 s) in which the flow carried payload and both directions stayed under 64 KiB. This keeps `TestBulkClassDoesNotFlap` (an idle gap carries no payload) and does not flap within a transfer.
- **Blocks.** Stage 7's gate and the stage-2 product screen. With `topology=single`, the class scheduler is the only protection a latched session has. It does not block stage 6: both arms use the same classifier and stage 6 keeps isolation. It must be fixed before stage 10, because the `ssh` "after a large output burst" scenario will fail.

### R2. A browser connection latches BULK after its first large object

- **Current behaviour.** HTTP/2, or a reused HTTP/1.1 connection, is one opaque TLS byte stream. The classifier does not decrypt (`classifier.go:1-3`), so it cannot see streams. The first object over 64 KiB in a window after age 1 s makes the connection BULK for life. Browsers keep these connections for minutes.
- **Under `sched=class`.** Later page loads on the warm connection are BULK, so warm `browsing` under bulk can be *slower than cold*. Within one burst the byte order is TCP's, so a small object behind a large one waits for it whatever the class. The class adds the wait only after the large object has drained.
- **Fix.** Per-stream classification is not available without MITM and is not proposed. The backlog or token-bucket rule of R1 fixes exactly the harmful part: once the large object drains, the next request goes out INTERACTIVE. Demotion with hysteresis also fixes it, but `k` windows later.
- **Blocks.** Stage 7 and stage 10, as R1. The warm/cold split in §15.3 is what makes it visible.

### R3. Heavy UDP occupies INTERACTIVE

- **Current behaviour.** Every PACKET is INTERACTIVE, at any rate (`sched.go:40-43`; design §9, Q5). The mobile packet stack forwards all device UDP (`mobile/core/packetstack.go:216-218`, `467`). QUIC/HTTP3 video and large UDP transfers therefore arrive as INTERACTIVE.
- **Under `sched=class`.** A saturating UDP flow takes up to two thirds of the path from BULK, which is a fairness cost. Worse, the call shares one ordered datagram FIFO and symbol stream with it (design §9). The call's packets queue behind the heavy flow's: one-way delay and freezes are set by the heavy flow, not by bulk.
- **Fix.** Give each association an INTERACTIVE rate allowance, for example 8 Mbit/s sustained over 2 s, enough for any call.
  - Short term: police the excess at the association's enqueue, either deferred or dropped. QUIC above it adapts.
  - Long term, which also resolves Q5: a second datagram queue and coded stream charged as BULK in the fork, with an association moved whole between them. Attribution is then per queue, never per symbol, so priority is not inverted inside an ordered stream.
- **Blocks.** Not stage 6/7 on the core workloads: TCP bulk does not touch the datagram FIFO. It does block stage 10 through the `video-call` "under a saturating UDP flow" scenario. Q5 should be decided before that run.

### R4. Is upstream classified like downstream?

- **Current behaviour.** The rule is symmetric. It uses the sum of both directions and a per-direction recent window, and the sender of each direction decides that direction's DATA class. An upload is BULK at the client after 1 s, as a download is at the gateway. There are three asymmetries:
  - Process hints exist only at the client, so they affect only upstream DATA.
  - The gateway re-examines a flow only on payload and coding decisions, while the client also has a 250 ms tick. This has no effect, because NEW and INTERACTIVE map to the same send class.
  - Each end's classifier is fed by what it reads and what it delivers, so the receiver's view lags by the delivery delay.
- **Under `sched=class`.** Upstream scheduling is correct. Downstream ignores a declared class: an `ssh` hint protects keystrokes but not echoes, and a declared bulk backup is INTERACTIVE downstream for its first second.
- **Fix.** Carry a declared class to the gateway in OPEN, with a flag that distinguishes a declaration from the default class byte (PROTOCOL.md §7), and `Declare` it there. Fixing R1 matters more than this, because a hint is only a starting point.
- **Blocks.** Nothing.

### Also seen

- **Every new bulk flow is INTERACTIVE for its first second.** At 100 Mbit/s that is about 12 MB per flow at INTERACTIVE weight, which delays a call or an echo at every bulk start. Only `mixed` and `browsing` under bulk see it, because §15.3 starts bulk before measuring. The token bucket of R1 bounds it to `B_I`.
- **Slow bulk never becomes BULK.** A transfer that stays under about 64 KiB/s per window stays INTERACTIVE. It is cheap at that rate, but several of them on a poor mobile path add up.
- **A call over TCP becomes BULK.** A call relayed over TCP (TURN/TCP, or WebRTC behind a proxy that carries no UDP) runs at 2–4 Mbit/s and is BULK after one second. The token bucket would serve its first `R_I` as INTERACTIVE and the rest as BULK. This needs a per-flow allowance large enough for a call, or a hint. Worth one `video-call` scenario over TCP.

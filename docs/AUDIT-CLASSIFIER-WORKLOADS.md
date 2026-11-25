# Classifier audit against the workload model

Status: **read-only audit.** It merges two independent readings: the stage-2 audit of `sched=class` (PR #181), and §5 of `docs/WORKLOAD-MODEL.md`. Findings keep the model's numbers F1–F8 and recommendations R1–R7. Findings only the stage-2 audit made are F9–F13 and R8–R9. The table at the end maps the first audit's own numbers to these.

Line numbers refer to `origin/main` `0eb4f36`. `internal/pep/sched.go` and `internal/congestion/classarb.go` exist only on `feat/stage2-sched-class` `d349fa0`, and are marked "#181". The classifier, `mpflow.go`, `stripedsend.go`, `udp.go` and `profile.go` are identical on both. Nothing was executed; each finding names the test that would confirm it. Workload IDs (W1–W11, M1–M7) are the model's.

## 1. What decides a frame's class

- **Frame to class (#181).** `frameSendClass` (`internal/pep/sched.go:33-48`, #181):
  - DATA whose header class is BULK is BULK, and any other DATA is INTERACTIVE.
  - PACKET is INTERACTIVE.
  - Every other frame is CONTROL.
  - DATA sent as coded datagrams is INTERACTIVE whatever its header says, because the datagram queue is (design §9).
  - The weights are 4:2:1 (`classarb.go:20`, #181). With CONTROL idle, INTERACTIVE gets two thirds of a saturated path against BULK.
- **Header class.** It is stamped from the flow's current class when the frame is built (`internal/pep/stripedsend.go:612-615`). Each endpoint labels its own sending direction from its own classifier. The gateway creates that classifier (`internal/pep/server.go:1190`) and never adopts the peer's class byte.
- **Rules.** Model §5.1 has the full table. In short:
  - BULK requires age ≥ 1 s, 128 KiB total, and not "bidirectional with small recent bursts" (`internal/classifier/classifier.go:206-213`). "Small recent" means ≤ 64 KiB per direction in the current and previous 1 s windows (`mpflow.go:3320`, `3326`, `3412`).
  - BULK is permanent (`classifier.go:181-183`).
  - The idle veto is off on the default profile (`internal/profile/profile.go:151`, `185-194`).
  - Hints (`Declare`) apply only on the client (`internal/pep/client.go:3644`).
- **Observations.** One on each local read (`stripedsend.go:291-294`), one on each delivery (`mpflow.go:3180`), and a refresh every 250 ms at most (`mpflow.go:1317`, `1329-1341`). The refresh is driven by the client's per-flow monitor (`client.go:2795`, `2967`) and by the coding decision (`mpflow.go:1280`).

Model §5.2 traces every workload's label over its lifetime.

## 2. Findings

| ID | Finding | Workloads | Consequence under `sched=class` | Fix |
| --- | --- | --- | --- | --- |
| **F1** | An LLM call is BULK for its whole response: a request of 128 KiB or more, the second request on a reused connection, or several calls on one HTTP/2 connection. On a fresh connection a 100 KB prompt usually stays INTERACTIVE. Because of F3, a prompt between roughly 120 and 128 KiB can flip once the first second of response pushes the total over 128 KiB. | W5, M1–M3 | Deltas are BULK at weight 1 of 7 behind downloads, and uncoded (`mpflow.go:1281-1285`). Each lost delta costs a retransmission round trip and holds the deltas after it. TTFT and gap tail degrade from turn 2. | R1, R1a, R1b, R2 |
| **F2** | The BULK latch is permanent, so every multi-phase connection is mislabelled after its first burst: SSH after `cat`, `scp` or a port forward in the same connection; HTTP/2 after a page; messaging after login sync or a media send; remote desktop. Both ends latch. The `dc-long-haul` notes (`classifier.go:64-78`) record the mirror-image failure, a 240 MB pull stuck INTERACTIVE for 228 MB; the defect in both is permanence (`classifier.go:113-115`). | W3-h2, W6 phase C, W9 | Every later keystroke, request or push is BULK and uncoded. With `topology=single` the class is the only protection, so a session pays bulk queueing until it closes, which for messaging can be hours. Warm browsing under bulk can be slower than cold. The flow is also eligible for bulk isolation (`client.go:2737`). | R1, R1a, R1b |
| **F3** | **The recent window goes stale across idle. This looks like a plain bug on main.** `recentBytes` rolls `current` into `prior` once, however long ago the window started (`mpflow.go:3345-3349`). The first observation after any idle period still sees the pre-idle burst as "recent". | W5, W9, W6 | It can make the gateway label a prompt of 128 KiB or more BULK on the first delta after age 1 s, even when the upload finished earlier, and it creates the 120–128 KiB knife-edge. A zero-byte refresh in the gap ages the burst out, so the effect depends on whether one ran. It also contradicts the function's own contract: "what each direction has carried in the last exchangeWindow" (`mpflow.go:3328-3330`). | R2 |
| **F4** | The idle veto does not help. It is off by default, needs two idle episodes, and is permanent in the other direction. | W5, W9 | — | none (R1 replaces it) |
| **F5** | UDP has no classification. Every PACKET is INTERACTIVE at any rate (`sched.go:40-43`, #181; PACKET frames are built at `udp.go:562-565` and `965-968`). The mobile packet stack captures all device UDP (`mobile/core/packetstack.go:216-218`, `467`). Whatever routing policy does not reject (`479-482`) and the local resolver does not answer (`490-500`) is forwarded as PACKETs. | W8, M5 | Heavy UDP takes up to two thirds of the path from BULK. It also shares one ordered datagram FIFO with voice and with coded deltas and keystrokes, so their delay is set by the heavy flow (design Q5). Rate alone cannot separate it from a call; elasticity can. | R4, R5 |
| **F6** | HTTP/2 hides streams. The classifier does not look inside TLS (`classifier.go:1-3`), and within one byte stream a small object waits behind a large one whatever the class. | W3-h2, W5-h2 | Only the wait after the large object has drained is the classifier's doing, and that is F2. | R1 (behaviour-following class) |
| **F7** | Short flows are never BULK. A flow that ends before age 1 s is NEW throughout, so hundreds of sub-second objects are INTERACTIVE traffic. | W3, M3 | Nothing inside INTERACTIVE separates them from keystrokes. Acceptable now; measured by M3 and by W3 + W6. | — |
| **F8** | Idle handling shows up in the same workloads:<br>• only payload refreshes activity (`mpflow.go:3366-3367`), and kernel TCP keepalives on the outer legs (30 s: `client.go:833`, `internal/pep/destination.go:67`) do not count;<br>• the default flow idle timeout is 30 min (`mpflow.go:95`; `cmd/queqiaod/main.go:616`), one or two minutes above FCM's and IMAP IDLE's own timers;<br>• the production 900 s EOF matches no default on main. Candidates are an operator-set `--flow-idle-timeout`, a relay hop (default 2 min, `cmd/queqiaod/relay.go:103`; a 15-minute relay idle timeout appears in a test fixture, `cmd/queqiaod/relay_test.go:70`, which does not establish the deployed setting) or the destination. | W9, M6 | Not a scheduling issue. | R6 |
| **F9** | Process hints are client-only and never reach the gateway (`client.go:3644`; `server.go:1190`). The rule itself is symmetric: an upload is BULK at the client exactly as a download is at the gateway. | W2, W6 | A hint can protect only upstream DATA. An `ssh` hint covers keystrokes but not echoes, and `Declare(INTERACTIVE)` cannot prevent F2 anyway (R7). | R8 (low priority) |
| **F10** | Every client flow, idle or not, has its own manager ticker: 250 ms on a TCP bundle (`client.go:2795`, which also refreshes the class at `2967`) and 500 ms on QUIC lanes (`client.go:2499`). | W9, M6 | Not a classification error. Hundreds of idle connections on mobile cost two to four wakeups per second each, which is CPU and battery. | R9 |
| **F11** | Every new bulk flow is INTERACTIVE for its first second (NEW until `BulkMinimumAge`). | W1, M4, M7 | About 12 MB per flow at 100 Mbit/s is served at INTERACTIVE weight, which delays a call or an echo at each bulk start. Backgrounds that start 5 s early hide it, so only the mixed runs see it. | R1b (backlog onset sets BULK) |
| **F12** | Slow bulk never becomes BULK. The small-burst test compares the sum of the current and previous buckets, 1–2 s of traffic, with 64 KiB per direction (`mpflow.go:3345-3360`, `3412`). A bidirectional transfer that stays under roughly 32–64 KiB/s therefore remains "small bidirectional bursts". A unidirectional one is not exempt (`classifier.go:210-212`). | W1, W2 on a poor path | Several such transfers on a poor mobile path add up inside INTERACTIVE. | R1b (continuous backlog sets BULK whatever the rate) |
| **F13** | A call relayed over TCP (TURN/TCP, or WebRTC behind a proxy that carries no UDP) runs at 2–4 Mbit/s and is BULK after 1 s. | W7 over TCP | Media at BULK weight. R1a does not rescue it: video frames exceed two packets. | measure first: a W7-over-TCP variant; hint or per-flow allowance if needed |

**Confirming tests** (model §5.3): F1 and F3 as stated there. F2: feed `observe` keystrokes, a 300 KB burst, then keystrokes, and assert the class after 3 s. F9: an end-to-end check that a declared flow's downstream DATA carries the declared class. F11: assert the class byte of DATA sent between 0 and 1 s of a bulk flow.

## 3. Recommendations

| ID | Change | Blocks |
| --- | --- | --- |
| **R1** | Split the class into a sticky fact and a current state. `everBulk` (sticky) keeps driving isolation, pre-warm and lane choice (`client.go:2737`, `3022`; `stripedsend.go:584`). `nowBulk` drives the class byte, the lane queue, the budget flag and coding (`stripedsend.go:614`; `mpflow.go:2291`, `2311-2319`, `1281`). Striped DATA samples `carriesBulk` separately and passes it down (`stripedsend.go:456`, `473`, `637`), so that sample must read `nowBulk` too. | stage 6/7 campaigns |
| **R1a** | Sparse-write rule: a write that finds the flow's send queue empty and is at most 2 packets is sent INTERACTIVE and coded. This fixes F1 and F2 immediately, with no timer. | with R1 |
| **R1b** | Windowed exit from BULK. `nowBulk` clears after 2 consecutive windows in which the flow carried payload, stayed at or under 64 KiB each way, and had no backlog. A window with no payload neither clears nor sets it, so `TestBulkClassDoesNotFlap` holds. It sets again after a continuous backlog of `max(1 s, 4 × SRTT)` (model R1b). *Extension beyond the model:* using the same backlog test for the first entry into BULK would bound F11 and close F12. | with R1 |
| **R2** | Fix the stale window: when `now − recentStart ≥ 2 × exchangeWindow`, zero `prior` as well, and add the F3 test. It restores the function's stated contract rather than changing policy, so it qualifies under design §14 as a present-bug fix that ships on by default. It is then in both arms without a switch. The model groups it under `classifier=v2`; either placement meets the blocking condition. | stage 6/7 campaigns |
| **R3** | Per-byte class telemetry: bytes per (flow, class, substrate) and class transitions with timestamps. Without it every finding above is invisible to a goodput gate. | stage 6/7 campaigns |
| **R4** | Fair input to the coded path: deficit round robin across UDP associations and coded TCP chunks before frames enter ESI order, with the R1a sparse rule. No wire change and no symbol reordering. | stage 10 |
| **R5** | Decide Q5 with data. Add a per-association rate and elasticity meter, then choose from W8 and M5 between charging heavy associations as BULK through a second datagram queue and coded stream (a fork change) or keeping them INTERACTIVE with R4. Until then, a per-association allowance of about 8 Mbit/s, with the excess policed at enqueue, is the stopgap. | stage 10 (decision) |
| **R6** | Idle semantics: the tunnel must never be the shortest timer. Raise the default flow idle timeout to at least 2 h while both outer legs answer keepalives. A flow the tunnel ends must reach both applications as EOF or RST within 5 s. Localise the 900 s EOF with W9b first. | stage 8 and stage 10 |
| **R7** | Do not use `ClassHints` for F1 or F2. `Declare(INTERACTIVE)` buys only the first second and cannot prevent or undo BULK (`classifier.go:138-160`). | — |
| **R8** | Carry a declared class to the gateway in OPEN, with a flag that distinguishes a declaration from the default class byte (PROTOCOL.md §7), so a hint covers both directions. | nothing |
| **R9** | Stop an idle flow's monitor ticker and re-arm it on the next payload or lane event. It belongs with stage 8, which owns idle liveness. Bound it through the existing client-CPU secondary in M6. | nothing |

**Why R1–R3 block stage 6/7.** Stage 6 requires `sched=class`, and stage 7 removes isolation. A flow wrongly labelled BULK therefore loses more in the candidate arm, where it has the lowest weight on one shared connection, than in main, where it has its own lane and an interactive reserve. An A/B run with today's classifier would attribute the mislabel to the controller or the topology.

Land R1 and R3 as their own switch (`classifier=v2`), and R2 on by default or inside that switch, gated on W1 (the 0.90 goodput gate, to show R1b costs bulk nothing), W5, W6 and W9a. Then run stages 6 and 7 with it on in both arms. R4–R6 concern behaviour that stages 6 and 7 leave unchanged from main, so they wait for the flip.

This corrects the first stage-2 audit's statement that the latch does not block stage 6. That statement held only that both arms use the same classifier, and missed that the arms pay for a mislabel differently.

## 4. Cross-reference

| First stage-2 audit (PR #183 commits `51a50ba`, `71acbad`) | Model §5 | Here |
| --- | --- | --- |
| R1 SSH or interactive flow bursts, then latches | F2 | F2 → R1, R1a, R1b |
| R2 HTTP/2 connection latches after first large object | F2, F6 | F2, F6 → R1, R1a, R1b |
| R3 heavy UDP occupies INTERACTIVE | F5 | F5 → R4, R5 |
| R4 upstream symmetry; hints client-only | — | F9 → R8 |
| R5 LLM 100 KB prompt then deltas, latches on reuse | F1, F3 | F1, F3 → R1, R1a, R1b, R2 |
| R6 long-idle messaging: latch on media; per-flow ticker | F2, F8 | F2, F8, F10 → R1, R6, R9 |
| "Also seen": first second INTERACTIVE | (F7, related) | F11 → R1b |
| "Also seen": slow bulk never BULK | — | F12 → R1b |
| "Also seen": call over TCP becomes BULK | — | F13 |
| — | F4 idle veto | F4 |
| — | F7 short flows | F7 |
| — | R3 telemetry, R6 idle, R7 no hints | R3, R6, R7 |

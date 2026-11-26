# Queqiao transport/control architecture — design v6

Status: **design, partly implemented.** It was written from first principles and revised over five adversarial review rounds. Round 5 reviewed v5 and returned "build with listed changes": two round-4 items were only partly resolved and four new P1 were raised; this revision absorbs all six. Stages 1, 3 and 4 of the migration are merged, stage 5 is in review, and the fork side of stage 2 is in review (§14). Where an implementation corrected or sharpened this document, the relevant section states the rule as built and Appendix B records the divergence and links the pull request. Appendix A maps every review finding to its resolution. Line numbers refer to `origin/main` `e3c2654` and the fork `bojieli/quic-go` `53637197` as pinned before stage 1, unless a section names the stage-1 pin (`1595c13a`). Live measurements come from the A/B campaigns posted on PR #146 and PR #161.

## 0. Decisions in one screen

1. **One congestion authority per path and sending direction** (`PathController`). Connections *attach* to it and ask it for admission. There are no members, shares, seeds, ramps or re-arms, because nothing is divided.
2. **The authority also owns class arbitration.** It decides which class the next packet belongs to; the carrier's packet builder executes that decision and reports what it packed.
3. **One QUIC connection per path is the steady-state topology.** Flows are streams plus the connection's datagram substrate. Several attachments exist only during a handover, for an old client that isolates, and for an explicit CPU fan-out. The same authority admits every packet in all three.
4. **Delivery is end-to-end at the flow layer and unchanged on the wire.** The flow ACK stays the only delivery credit, generated as today.
5. **Three kinds of evidence, three scopes of remedy.**
   - Carrier transport silence replaces the carrier, once, for all its flows; the client initiates.
   - A flow's stream definitively failing on a live carrier replaces that stream on the same carrier.
   - Missing flow credit on a live stream moves coded bytes to the stream and otherwise waits, up to the existing no-progress budget. It never dials and never replaces a stream.
6. **Each byte has one repair owner at a time.** Coded datagram: the code, until it is discarded or until a deadline that starts when its last source symbol is registered in a packet. Stream: QUIC. Lost binding: the flow's retained replay.
7. **Every reservation of shared memory has a named owner with a bounded obligation**, from grant to release. A producer waiting for the application holds none.
8. **No wire change.** Everything is sender-local policy under PROTOCOL.md §18. Wire 3 is recorded as a contract and stays out of the migration.
9. **`dc-long-haul` stays on today's code by default throughout the migration.** Since stage 9 it can also run the whole new architecture (§11.3), opt-in. Deletion of the old machinery is gated on that profile passing its own gate on the new architecture.

## 1. What the history says, read as architecture

The README promises "one coordinated long-haul path" and "flows sharing one bottleneck should share one model". The code implements that promise at the wrong layer.

- With `--congestion erasure`, the default (`cmd/queqiaod/main.go:614`), every QUIC connection gets its own `ErasureSender` with its own BBR machine, pacer and window (`internal/pep/transport.go:1085-1098`). A bulk flow that contends with another moves its data to a secondary connection from a bounded pool (`internal/pep/client.go:2169-2265`, `2459`, `2706-2712`; up to eight on desktop, one on iOS), so a client runs up to nine controllers on one path.
- "Sharing" is reconstructed afterwards by `pathmodel.PathModel`: each controller reports a delivered rate, the model takes a 10 s maximum of the sum, and hands each member `Share = 1.25 × bottleneck / live` and a joiner `Seed = bottleneck / (live+1)` (`internal/pathmodel/pathmodel.go:193-213, 300-322, 388-398`). The share caps the member's rate (`internal/congestion/erasure.go:237-250`).

This structure has one defect with many faces: **the quantity being divided is measured from the traffic the division limits.**

| Symptom | Mechanism | Patch | Why the patch cannot close it |
| --- | --- | --- | --- |
| #73 seed inflation, up to 87× | a seeded member reports its seed as delivery; the pool sums it | #146 own-delivery filter, unscaled seed | makes the share honest, so it now binds |
| #146 Beijing 0.52× main | an honest share is `1.25 × 4/5 = 1.0×` of past delivery beside an idle control member; a capped lane looks application-limited and never leaves STARTUP | #161 ramp, startupHeld, own floor, earned compensation, re-arm, busy split | each rule re-derives, per lane, a fact the path already knows |
| #160 tail regrowth, idle-member share, summed stale peaks | per-lane BBR state must regrow after a sibling leaves; the split has to guess who is busy | open | there is no correct split of a self-referential estimate |
| ACK-channel throttling (flow-ACK excess delay 0.05 s → 4.4 s → 2.0 s) | a mostly-receiving client's bucket is charged for QUIC ACK-only packets (`erasure.go:516-519`, `pacer.go:68-76`) that the sampler never credits (`bbr_tuic_bw.go:128-135`); the cap then sits at the trickle | B1/B2 one-change builds | each exempts one case of "do not rate-limit traffic by an estimate it was not measured on" |

The ACK-channel row is no longer an inference. The gateway × client 2×2 crossover posted on #161 isolated it on the high-loss Beijing path. There, the #161 gateway with main's client was at parity with main, and every arm with the #161 client had upstream pacing on its ACK-carrying lanes at about a tenth of main's, with median flow-ACK transit 1.8–2.8× main's. The #161 client damaged the ACK channel; with main's gateway goodput still held (0.98×), and the #161 gateway turned the damage into a deficit (0.76× for both together). On that client's upstream 94 % of wire bytes were QUIC ACK-only, and the share cap bound 84 % of flow-ACK writes. Either one-change build alone, B1 (do not debit non-eliciting packets, which is I2) or B2 (do not cap a member that is not send-limited), restored flow-ACK transit to main's and removed most of the excess timer reissues (28 % of source bytes down to 7.9–8.3 %, against main's 6.4 %). On the Mac path the #161 client damaged the ACK channel too (median flow-ACK transit 2 s with both #161 builds), but the gateway-only arm delivered 0.78× main's goodput, so gateway parity is specific to the Beijing path. The live arms tested the mechanism on #161's builds; stage 1's real-QUIC test checks the v1/v2 difference directly (Appendix B).
| False stalls, 3× main; 23–29 % reissued bytes | one timer (`mpflow.go:1945-1964`: 3×minRTT of flow-ACK silence) drives both a byte-scoped remedy and a connection-scoped one (`mpflow.go:2040-2160`, `2193-2214`) | none | a flow signal is used as carrier evidence |
| Coded tail loss | a datagram tail is lost on a healthy connection; nothing later exposes it | watchdog escape (`mpflow.go:2169-2186`) | right remedy, attached to the wrong trigger |
| Lane replacement failures #53/#159 | per-flow lane managers each rebuild carriers; a carrier is retired while flows depend on it | #158, #163, #164 | carrier lifecycle has no single owner |

Main "works" on the Beijing path because echoed seeds inflate the share until it never binds: it is N independent erasure-compensated BBRs. That is also why main has the worst Mac result (loss 0.50, queue p90 1,030 ms, 43/48 flows; the round-6 A/B posted on #161). Neither arm is the README's design.

Two things already in the tree are right and are kept: the erasure-aware control law (`erasure.go:247-481`) and the logical flow (byte offsets, range ACKs, retention, JOIN replay). The redesign moves ownership. It does not invent a controller or a protocol.

Two earlier rejections do not apply. LIA was removed because its fairness obligation went away and because it nested a flow-level window above per-lane controllers (`laneadmission.go:8-18`); one authority has no nesting. Data aggregation was removed because extra 4-tuples add no capacity (`docs/DESIGN.md`); attachments add none and claim none.

## 2. Invariants

- **I1. One congestion authority per path and direction.** It alone owns the delivery-rate estimate, minimum RTT, erasure estimate, compensation, delay brake, pacing balance, window, and the choice of which class is served next.
- **I2. A rate limit applies only to traffic its estimate was measured on.** The balance is debited by exactly the ack-eliciting packets, as actually built. Application-limited is derived from demand and unused budget, both of which the authority observes.
- **I3. The filter contains only samples.** No seed, prior or share is written into an estimator.
- **I4. Priority is decided by the authority and executed where packets are built.** No layer above the carrier queues one flow behind another. On wires 1 and 2 a flow's control frames share its own ordered stream, so the guarantee there is a bounded *scheduling* delay (§6); independent *delivery* of control needs wire 3.
- **I5. Delivery state is end-to-end at the flow layer.** Only a validated flow ACK completes a chunk, releases retained bytes, or lets FIN proceed.
- **I6. One repair owner per byte at a time.** Ownership moves only on evidence.
- **I7. A remedy's scope equals its evidence's scope** (decision 5).
- **I8. Erasure is measured once and handled once.** One estimator over one send order; compensation scales the path rate once; parity, retransmissions and control draw on one balance.
- **I9. Bounded memory with guaranteed progress.** Every reservation of shared memory has, at every moment, one owner with an obligation that either releases it or ends the flow within a stated no-progress bound. The bound is on waiting without progress, not on completion time.
- **I10. Directions are independent.** Each endpoint controls only what it sends.

| Layer | Owns | May read | Must not know |
| --- | --- | --- | --- |
| Path authority | rate, window, erasure, minRTT, balance, flight sum, class arbitration | per-attachment packet events, flight gauges, demand | flows, streams, FEC, lanes, the peer's rates |
| Carrier (QUIC connection, TLS socket) | packet reliability, PTO, its own RTT, crypto, transport progress | permits | flows; other carriers |
| Packet builder (in the carrier) | filling one packet under a permit | class of queued byte ranges | why a class was assigned; rates |
| Flow | byte offsets, chunks, retention, flow ACKs, substrate and FEC choice | read-only path snapshot; its binding | how the rate is computed |
| Carrier manager (client) | carrier set per path, bindings, replacement, JOIN orchestration | transport progress; binding-lost events | flow byte state |

## 3. Layers and contracts

```text
application ── SOCKS5 ──► Flow layer        chunks, retention, flow ACK/credit, substrate + FEC choice
                              │  WriteClass(stream, bytes, class) / coded frame            ▲ read-only Snapshot
                              ▼                                                             │
                         Carrier (QUIC conn)   demand ─► Admit ─► permit ─► builder ─► OnSent ─► PathController
                              │  packets                                                    ▲ OnAckEvent / gauges
                              ▼                                                             │
                         UDP 4-tuple  ◄──────────── Carrier manager (client, per path): ACTIVE / STANDBY / DRAINING
```

- **Flow → carrier.** A stream write blocks until the stream has accepted the bytes; the fork returns once all but a small tail has been packed and buffers that tail in `nextFrame` (`send_stream.go:247-299`). A flow has at most one chunk (configured size: 32 KiB default, `flow.go:12`; 16 KiB on mobile) plus that tail ahead of the builder.
- **Carrier → authority.** The carrier keeps the authority told of its eligible demand, asks `Admit` before building each ordinary packet, and reports every packet built, every acknowledgement, loss and discard, and every change to its bytes in flight. It never computes a rate.
- **Authority → everyone.** A read-only `Snapshot{DeliveryRate, WireRate, Window, InFlight, Erasure, BurstFactor, DatagramBurstFactor, MinRTT, SRTT}`. No reader may feed it back.
- **Flow ↔ peer flow.** The ACK contract of §8.2.
- **Carrier manager ↔ flow.** `Bind`/`Rebind` downward; `BindingLost(flow, cause)` upward. A flow reports that its stream failed; it never decides what replaces it.

## 4. The path authority

### 4.1 Key, law and lifecycle

- **Key.** `(authorised principal, local uplink identity, normalised remote host)`. The remote port is excluded, as today (`transport.go:1158-1175`), so port hopping stays on one path.
- **Provisional control.** A gateway installs a controller before it knows the ALPN or the principal (`server.go:703-757`). Every accepted connection starts on a standalone, bounded controller. Only a connection that negotiated a data ALPN and whose principal is authorised attaches to an authority. Enrollment and renewal connections never attach. Per-stream re-authorisation on long-lived connections (`server.go:809-815`) is unchanged.
- **The law is pluggable; ownership is not.** The authority hosts one control-law instance per path. `erasure` is the default; `bbr`, `bbr-tuic`, `adaptive` and `brutal` (`transport.go:1071-1101`) plug in the same way. `reno` is quic-go's own controller, cannot be hosted, and remains a per-connection diagnostic that is available only in the legacy bundle (§14).
- **Knowledge.** When the last attachment detaches, `{bw, minRTT, erasure, burst, earned arrival}` are kept for `knowledgeLifetime` (10 min, as today). A later first attachment uses them as an initial pacing hint and as FEC priors. They are not samples (I3): the max filter stays empty until a real delivery sample exists, and the hint is dropped then.

### 4.2 State

- **Control law (unchanged for `erasure`).** `TUICBBRSender` with `lossIsCongestion=false`, the compensation gate (`erasure.go:270-320`), the delay bound (`erasure.go:381-390`) and the unmetered-burst rule (`erasure.go:417-481`). `WireRate = delayBounded(bw × gain / arrival)`, `Window = cwnd / arrival`. Applied once.
- **Which packets are samples.** The fork labels every packet with a kind (fork change 9). A packet enters the delivery sampler and the erasure estimator only if it is of kind ORDINARY or PTO_PROBE (an ACK packet that gained a PING is kind ACK_PING and is not a sample), is application-space and ack-eliciting, and was sent by a COUNTED attachment (§4.4). MTU probes are in flight but are not samples, because their loss says nothing about the path's erasure; path probes, keepalives and the packets of a PROBING attachment are not samples either.
- **One send order.** Each sample packet gets a path sequence `pseq` at `OnSent`, under the controller's lock. A bounded ring of fate records is indexed by `pseq` and keyed back to `(attachment epoch, space, packet number)`. A record ends in one of: *delivered*, *lost*, *discarded* (fate unknowable: key drop, Retry, migration, a packet declared lost only to build a PTO probe, detach or demotion), or *expired* (unresolved for longer than `4×SRTT`; independently, the ring holds at most 8192 records and the oldest is evicted as expired when it is full, whichever comes first). A later acknowledgement of a packet reported lost is a *spurious* correction that flips its fate if the record is still in the ring. Corrections are delivered only when the fork detects them. As built, the fork keeps a bounded late-acknowledgement history of application-space ack-eliciting packets that left the flight as lost, were retired for a PTO probe, or were retired by migration (at most 256 records, pruned after three PTOs, matched by binary search). A late acknowledgement of any of them is transport progress; it produces a `Spurious` correction only for a loss that was reported to the controller still installed, naming exactly that reference. Beyond that horizon a lost fate stands, which biases the erasure estimate slightly upward under heavy reordering and is accepted. Initial and Handshake packets get no correction.
- **Sampler state is retired by fate, not by watermark.** The present TUIC sampler derives an obsolete-packet watermark from the largest acknowledgement in each event and deletes every older per-packet state below it (`bbr_tuic.go:550`, `649`; `bbr_tuic_bw.go:201`, `263`). On one connection that is sound. On a shared send order it is not: if attachment A registers `pseq=10`, B registers up to `pseq=20` and B's acknowledgement arrives first, the watermark deletes A's state, and A's later acknowledgement takes the state-miss branch before `totalAcked` is incremented, so real delivered bytes vanish from the rate input while the fate ring still looks correct. In the path controller, per-packet sampler state is retired only when that packet's own fate is known (delivered, lost, discarded) or when its ring record expires. No cross-attachment watermark exists.
- **Burst statistics come from send-order neighbours.** When a record's fate becomes known, the estimator counts the transitions to `pseq−1` and `pseq+1` if their fates are known, and adjusts those counts on a spurious correction. A transition is counted only between two known fates that are adjacent in the sequence of sample-eligible registrations (`pseq`); an unknown fate breaks the run. This is registration order, not wire order: a non-sample packet between two samples does not break adjacency, and two connections' socket queues can reorder output slightly. It is the order the estimator is defined on, and test 4 checks the statistic under both readings before it is used to size parity. The datagram statistic counts a transition only when both neighbours carried a datagram. The loss-run statistic is therefore independent of the order in which ack batches from different attachments arrive, which the present in-order Markov update is not (`lossmodel.go:103-115`, fed per batch at `erasure.go:572-585`). Delivery-rate sampling proceeds per acknowledged packet and never waits on another packet's fate.
- **Flight is a sum of gauges.** Each attachment reports its connection's own `bytesInFlight`. `InFlight = Σ gauges of COUNTED attachments`. The sampler's pre-send flight is the sum *before* the reporting attachment's gauge is updated; nothing is subtracted, so it cannot go negative. Events carry only the flight *after* the event; the flight before it is the attachment's previous gauge value, which is exact because every change to the connection's flight is reported (sent, acknowledged, lost, discarded). Stage 1's per-connection adapter derives the legacy callbacks' prior flight the same way, and its tests assert gauge = `bytesInFlight` after every event.
- **RTT.** Per-connection RTT and PTO stay in quic-go. The controller keeps a windowed minimum and an EWMA over the RTT samples that accompany ack events (an event may carry none). One ProbeRTT per path.
- **Demand.** Each attachment exports a level-triggered `Demand`: per class, whether it has work it could put in a packet now if admitted. The carrier computes it with a non-consuming query that covers stream data with flow-control credit, stream and control-frame retransmissions, due window updates (MAX_DATA is otherwise created only inside `sendPackets`, `connection.go:2564-2569`, and MAX_STREAM_DATA when the stream is asked for it, `receive_stream.go:545`), queued control frames, post-handshake crypto data, and datagrams. Work blocked by peer flow control, a full send queue or amplification limits is not eligible. Demand is refreshed before each admission of a batch and after every build, *including a build that produced no packet*: a build can consume the last eligible work without emitting anything (the packer drops a datagram that fits no packet, a stream's write limiter returns zero), and a stale assertion would otherwise persist until an unrelated wake (#176, fixed in the stage-2 fork change).
- **Application-limited is an unused opportunity, measured by the authority.** The path is marked application-limited when there was an interval, at least one packet time long, during which an ordinary `Admit` would have succeeded (balance at least one packet, counting time spent at the burst cap and after any debt was repaid, and the window open) and no attachment had eligible demand. Refill that only repays debt is not an opportunity. The authority evaluates this lazily at the next event and then sets `appLimitedUntil` as the delivery-rate algorithm defines. A queue that is empty for an instant between two writes forgoes no opportunity and marks nothing. A sender held back by its own socket or by the peer's flow control has no eligible demand, so its samples are, correctly, not measurements of the path. This replaces `appLimited()` (`bbr_tuic.go:639-647`) and #161's `backlogged()`.

### 4.3 Admission and class arbitration

**Classes.**

| Class | Contents |
| --- | --- |
| EXEMPT | ACK-only packets, PTO probes, handshake packets, path probes, liveness PING, a recovery transaction's verification exchange |
| CONTROL | OPEN/OPEN_OK/JOIN/ACK/CLOSE/RESET/PROBE frames; QUIC window updates and similar; DATA bytes boosted ahead of a waiting control frame (§6) |
| INTERACTIVE | DATA of NEW/INTERACTIVE flows; the coded datagram stream (symbols, repairs, UDP PACKET); MTU probes |
| BULK | DATA of BULK flows |

**Budget state.** A signed byte balance `B`, refilled at `WireRate` and capped at the burst allowance; the window against `InFlight`. Every ack-eliciting packet, exempt or not, is debited by its size at `OnSent`; the debit is never forgiven. Non-eliciting packets are counted and not debited. There are no per-class reserves.

**Arbitration state.** Three byte deficits `Δ_C`, `Δ_I`, `Δ_B` with weights `w_C : w_I : w_B`. All three classes are in one deficit round robin, so every class's service is bounded relative to bytes actually served, at any rate. There is no separate per-round-trip control allowance: at a low rate such an allowance could exceed the whole path's service and starve the data classes.

**The turn.** `Select()` is a pure function of deficits and path-wide demand: the highest-priority class that has eligible demand and a positive deficit. It is always preceded, under the controller lock, by *normalisation* (below), so whenever any class has eligible demand some class with demand has a positive deficit at the moment of selection. Control traffic is normally a small fraction of `w_C`'s share, so its deficit stays positive and it is served first; when control demand, including boosted DATA, exceeds its share, the data classes still get theirs.

**`Admit(attachment, now, maxBytes) → Permit`.**

1. `turn = Select()`.
2. If this attachment has no eligible demand in `turn` while another does, it is refused with `RetryAt = now + maxBytes/WireRate`. If the other attachment has not sent by then, its claim on this turn lapses and `Select()` is evaluated without it; the class keeps its deficit.
3. Window: refused if `InFlight + maxBytes > Window`. A CONTROL turn may exceed the window by the control debt `D`.
4. Balance: refused with `RetryAt` if `B < maxBytes` (less `D` on a CONTROL turn).
5. Otherwise `Permit{Turn, Fill, MaxBytes}`.

**What the builder must do with a permit.** Bytes of class `Turn` first. Remaining space may be filled only by classes the permit lists in `Fill`, each up to its listed byte allowance; otherwise the packet goes out short. `Fill` contains the classes whose deficit is positive at admission, with that deficit as the allowance, and is empty when the permit relied on the control debt, so nothing rides on borrowed budget and no class is served beyond its entitlement because space happened to be left over. Packet overhead is charged to `Turn`. A queued datagram that merely exceeds its `Fill` allowance, or a short permit, stays queued for a later packet; only a datagram that fits no packet of the connection's size is discarded (and reported, §5 change 12). The fork's present "doesn't fit" path drops the head of the queue and must not be reached for a datagram that is only out of allowance.

**Settlement.** `OnSent` reports bytes per class. The authority debits `B` and reduces each class's deficit by its bytes; deficits may go negative and the shortfall is carried.

**Normalisation is tied to selection, not to sending.** When no class with eligible demand has a positive deficit, quanta of `w_c × quantum` are added to every class with demand, repeatedly, until at least one is positive, so an indebted class regains eligibility in bounded rounds. This runs under the controller lock immediately before every `Select()`, and also on every demand transition. It cannot be left to `OnSent` alone: demand changes outside packet registration (a producer enqueues a datagram, a stream writer publishes bytes, demand clears between permit and build), so a class with a positive deficit can lose its demand before sending and leave a sole indebted class that no settlement would ever replenish. Normalisation only adds quanta to classes that have demand at that moment, so it cannot bank credit for an idle class. The stage-2 arbiter (on its branch, not yet in review) goes further: an idle class's positive credit is dropped while its debt is kept, and a newly eligible class that is not indebted gets one quantum per round.

With indivisible units such as a 900-byte datagram, a class's service can deviate from its weight by at most one maximum packet per round; that is the stated service error. Because a turn's packet *starts* with that turn's class and every byte is charged to the class it belongs to, no class can take another's turns, on one attachment or across several.

**Why there is no reservation.** A permit is a value owned by one build operation. Nothing is reserved when it is issued and served bytes are charged at `OnSent`, so a permit that builds no packet needs no cancel. The cost is bounded staleness: with `k` attachments, up to `k−1` other permits can have been issued against the same state. For arbitration that means a class can be over-served by at most `k−1` packets at a time, and since deficits carry the overdraft into later rounds, its long-run share is unaffected.

**Verification grant.** A recovery transaction has to exchange a PROBE in both directions on a new connection while the dead carrier's flight may still fill the window and exempt sends may have driven the balance negative. Availability of that exchange is a liveness premise, so unlike ordinary admission it has an exclusive owner before anything is built:

- Each endpoint's authority holds a small verification budget `V` (bytes), refilled no faster than the replacement cooldown.
- The authority issues one *verification grant* at a time, to one named attachment, atomically, with an expiry. On the client the carrier manager requests it for the standby it is verifying; on the gateway it is issued to a newly attached data connection when that connection's first stream begins with PROBE.
- The grant covers a byte range of one stream: the PROBE request or echo and the QUIC frames needed to carry it. Only those bytes are EXEMPT under it (gated by neither window nor balance, debited, reported). Unrelated frames on the same connection are ordinary.
- **Binding the grant to the stream is a local fork operation** (§5 change 13), because neither endpoint's write path can name the bytes otherwise: the client opens a stream and performs an ordinary framed write, and the gateway learns it owes an echo only after reading and authorising the PROBE. Each side therefore reads the stream's accepted offset `Start`, binds `{ID, Start, End = Start + encoded length, Budget, Expiry}` on that stream, and only then writes the frame. Binding comes first because the write can block on a refused admission that only the grant can clear; the fork accepts a range not yet written. The fork serves the range as new data and as retransmissions inside it (a frame straddling `Start` is split), never a FIN-only frame, settles each packet at `OnSent` as kind VERIFICATION with the grant's ID, and charges the packet's other bytes (ACK, padding, exempt obligations) against `Budget` too. The grant is consulted only while `Admit` refuses; when ordinary admission succeeds the same bytes go out under the permit.
- **One holder, effective at bind.** Binding another grant to the connection reissues: from the moment the bind returns, the earlier grant covers nothing, and no packet built afterwards is charged to it. Replacing the connection's controller clears every grant bound before the replacement was requested, so a successor never receives a predecessor's grant ID. (Stage 1 merged range binding and settlement. As merged, a bind took effect only at the next run-loop pass, which let one packet be charged to the superseded grant, and a controller replacement kept the grant; both are #176, and the fixes are in the stage-2 fork change, in review.)
- If the grant expires unused, its budget is spent, or `V` is depleted, it covers nothing: the attempt fails as an unverified standby and the next may start after the cooldown. A grant guarantees an admission opportunity, not delivery through loss; a lost verification packet is retransmitted under the same grant until it expires or its budget is spent.

This is the one place a reservation is used. It is rare, bounded, single-holder and self-expiring, which is what makes a reservation safe.

**Bounds, stated separately.**

- *Ordinary admission.* Each ordinary packet is built after its own `Admit` (the fork checks the send mode before each packet of a batch: `connection.go:2596-2630`, `2632-2699`), on one goroutine per connection. Under fixed limits, one admission decision can add to any existing excess over window or balance at most `D + Σ maxBytes` of the other attachments. This bounds the increment at admission. It does not bound the overhang itself, which also grows when the window shrinks or when a PROBING attachment's gauge returns to the sum; in both cases ordinary sends simply stop until the overhang drains.
- *Balance over an interval.* Eliciting bytes registered in `[t0, t1]` are at most `∫WireRate dt + burst allowance + D + Σ maxBytes(others) + E(t0, t1)`, where `E` is exempt eliciting bytes. `E` is not gated by the authority; it is limited by transport rules (two probes per PTO expiry with backoff, `sent_packet_handler.go:982`; PINGs periodically added to ACK packets, `packet_packer.go:695-703`; path probes; liveness PINGs on their schedule) and by `V`. The balance always evolves with elapsed time, so debt is repaid by refill whether or not there is traffic, and refill accrued while idle is applied lazily at the next event up to the burst cap. What is not guaranteed is that exempt traffic stays below the refill; no unconditional long-run equality is claimed. Exempt bytes and unpaid debt are exported in telemetry.
- *Socket output.* "Sent" means registered with the sent-packet handler (`connection.go:2784-2814`). Up to eight queue entries plus the one being written sit between registration and the socket, and with GSO each can be a batch (`send_queue.go:35-43, 90-101`). That lag is per connection and is reported.
- Today's pacer forgives debt (`pacer.go:68-76`); the shared balance must not.

**Wake-ups.** The authority calls an attachment's waker when a recorded refusal reason may have cleared: flight fell or the window grew, the rate rose enough to move `RetryAt` earlier, another attachment's claim lapsed or its demand cleared.

### 4.4 Attachments

An attachment has an epoch and a state. Only the authority changes them, under its lock.

- **COUNTED.** Normal.
- **PROBING.** Set by an endpoint for its own attachment when that connection has `k_s` PTO expiries since transport progress (§10.2). Its gauge is tracked but not summed, because bytes outstanding across probe timeouts must not block a standby. It may send exempt packets, and ordinary packets only while its own gauge is below a small probe window. Its sends are debited. Its packets are not samples. A late callback cannot resurrect its flight, because inclusion is decided by state.
- **Return to COUNTED** follows the endpoint's own transport progress on that connection. On the *client* this is possible only before a promotion commits: promotion increments the client's path generation, a DRAINING carrier's attachment never returns, and state changes carrying an older generation are ignored. The *gateway* cannot observe the client's commit on wires 1 and 2, so its rule is purely local and reversible: an attachment that makes progress is COUNTED again with its current gauge. That is safe. If the old connection is acknowledging packets, its flight is real and is draining; while it re-enters the sum it can only create an overhang, which stops ordinary sends until it drains. Flows leave the old connection one by one as the gateway accepts their JOINs, and the client closes it when DRAINING ends.
- **Detach** discards its unresolved records.
- Attach, detach and migration are delivered on the connection's own run loop, so the first event is `OnAttach(initial flight)`.

Concurrency: one mutex per controller, every callback O(batch), event times clamped non-decreasing.

## 5. Fork changes (exact)

All in `bojieli/quic-go`. `CongestionControl`/`CongestionControlEx` stay for other controllers. A controller implementing `PathSender` gets these calls instead. Changes 1–3, 6–12 and 13 are merged (bojieli/quic-go#2, pinned by stage 1); changes 4–5 and the #176 fixes are in review (bojieli/quic-go#3, stage 2). The listing below is the API as built.

```go
package congestion

type PacketSpace uint8 // SpaceInitial, SpaceHandshake, SpaceApplication
type SendClass uint8   // ClassExempt, ClassControl, ClassInteractive, ClassBulk

type Demand struct{ Eligible [4]bool }

type Permit struct {
    OK           bool
    Turn         SendClass
    Fill         [4]ByteCount   // bytes of other classes that may fill leftover space; all zero = Turn only
    MaxBytes     ByteCount
    RetryAt      monotime.Time  // pacing or precedence; zero when window-limited
}

type VerificationGrant struct {
    ID         uint64         // nonzero
    Start, End ByteCount      // stream byte range covered, retransmissions included
    Budget     ByteCount      // packet bytes the grant may carry
    Expiry     monotime.Time
}

type SentPacket struct {
    Space           PacketSpace
    Number          PacketNumber
    Bytes           ByteCount
    AckEliciting    bool           // as actually built
    InFlightCounted bool           // included in this connection's bytes in flight
    Exempt          bool           // built without a permit
    Kind            PacketKind     // Ordinary, PTOProbe, MTUProbe, PathProbe, AckOnly, AckPing, Handshake, Liveness, Verification
    ClassBytes      [4]ByteCount
    HasDatagram     bool
    InFlight        ByteCount      // this connection's bytes in flight after this packet
    Time            monotime.Time
    Grant           uint64         // verification grant the packet was built under, or zero
    ClassBuilt      bool           // the permit's class decision was executed (stage 2)
}
type PacketRef struct{ Space PacketSpace; Number PacketNumber; Bytes ByteCount }

type AckEvent struct {
    Time          monotime.Time
    InFlight      ByteCount      // after this event
    RTTSample     time.Duration  // zero when none
    ECNCongestion bool
    Acked, Lost   []PacketRef
    Spurious      []PacketRef    // previously reported lost, now acknowledged
}

type PathSender interface {
    CongestionControl
    OnAttach(now monotime.Time, inFlight, maxPacket ByteCount, wake func())
    OnDemand(d Demand)
    Admit(now monotime.Time, maxBytes ByteCount) Permit
    OnSent(p SentPacket)
    OnAckEvent(e AckEvent)
    OnDiscarded(now monotime.Time, inFlight ByteCount, refs []PacketRef, all bool, reason DiscardReason)
    OnPathMigrated(now monotime.Time, oldLocal, newLocal, oldRemote, newRemote net.Addr)
    OnDetach(now monotime.Time)
}

// Optional, stage 2: a PathSender that also implements this has its permits executed by the builder.
type ClassScheduler interface {
    SchedulesClasses() bool        // read at installation
}

// On the connection and stream (local API, no wire effect):
//   (*Conn).BindVerificationGrant(str GrantableStream, g VerificationGrant) error
//   (*SendStream).AcceptedOffset() ByteCount
//   (*Conn).SendDatagramTagged(p []byte, tag uint64) error
//   (*Conn).SetDatagramObserver(func(tag uint64, outcome DatagramOutcome))
//   (*Conn).SetKeepAliveSchedule(floor time.Duration)
```

1. **Install on the run loop.** `Conn.SetCongestionControl` (`connection.go:3189`) posts the installation to the run loop; `OnAttach` is the first call and carries the current flight and a closure over `scheduleSending` (`connection.go:2989-2995`).
2. **Permit in the send loop.** `SendMode` (`sent_packet_handler.go:1060-1098`) calls `Admit` for a `PathSender` in place of `CanSend`/`HasPacingBudget`. Refusal maps to `SendAck` (window) or `SendPacingLimited` (pacing or precedence). Amplification, tracked-packet and PTO modes keep their precedence. The connection keeps the permit for the packet it is about to build and uses `Permit.RetryAt` wherever it now calls `TimeUntilSend` (`connection.go:2495-2504`, `2696-2702`). A permit covers one build and, in a coalesced datagram, only its application-space packet. Until change 4 lands the builder does not honour `Turn`, `Fill` or `MaxBytes`, so a controller installed at stage 1 must admit whole packets of any class; this is documented on `Permit`.
3. **Demand.** A non-consuming eligibility query over the framer, each stream's retransmission queue and reserved tail, the packer's retransmission queue, due flow-control updates, queued control frames and the datagram queue; `OnDemand` whenever its answer changes. Stream demand counts only work that could be sent now: retransmissions, a reserved tail, FIN, a reliable-reset prefix, or new bytes both flow-control windows allow. The query is refreshed before each admission of a batch and after every build, including an empty one (§4.2).
4. **Builder honours the permit.** `framer.Append` (`framer.go:88-155`) and the packer's datagram selection (`packet_packer.go:736-785`) take the permit and fill in the order of §4.3. The framer's single ring (`framer.go:247-270`) becomes one ring per class, round-robin within a class. The datagram queue stays one blocking FIFO (`datagram_queue.go:12-14, 45-70`) and is INTERACTIVE as a whole (§9). A datagram that exceeds only its `Fill` allowance stays queued (§4.3). Exempt obligations (ACK, PATH_RESPONSE, liveness PING) may ride any turn. As built in the stage-2 fork change, permits are executed from handshake confirmation on (before it they only admit), a permit too small to carry a classed frame is treated as a window refusal so that due ACKs still take the exempt path, and `SentPacket.ClassBuilt` reports whether the permit's class decision was executed. A controller opts in by implementing `ClassScheduler` and returning true from `SchedulesClasses()` at installation; otherwise the builder is unchanged.
5. **Class on byte ranges, and boost.** `SendStream.WriteClass(p, class)` records the class of the bytes it accepts. `SendStream.SetBoost(on)` sets a persistent property of the stream, under the stream's own mutex. While it is on, every byte the stream holds (a write in progress, a buffered tail, queued retransmissions: `send_stream.go:389-405`) *and every byte it accepts afterwards* is served at CONTROL. Turning it off affects only bytes accepted later. A stream's *effective class* is CONTROL while it holds any boosted or CONTROL byte that has not been transmitted for the first time or is queued for retransmission; otherwise it is the class of its oldest unsent byte. The stream re-registers with the framer when its effective class changes. This is a mechanism; the intent it serves is owned by the flow's writer (§6).
6. **Exact outcomes.** For a `PathSender` the handler stops the per-packet callbacks and sends one `AckEvent` per received ACK or loss-timer firing (`sent_packet_handler.go:423-487`, `921-940`). Spurious-loss detection (`:490-497`, `525-561`) runs before the empty-ACK return at `:423-427`, so an ACK that covers only a previously lost packet still produces an event. As built, late acknowledgements are matched against the bounded late-acknowledgement history of §4.2 (application space only), and a correction names exactly the reference that was reported lost, to the controller it was reported to.
7. **Discards.** `DropPackets` (`:195-236`), `ResetForRetry` (`:1153`) and `QueueProbePacket` (`:1120-1132`) call `OnDiscarded` with the packets concerned and the resulting flight. All silent flight changes pass through `removeFromBytesInFlight` (`:185-193`) or the Retry reset, so the list is complete.
8. **Migration keeps the controller.** `MigratedPath` (`:1199-1222`) replaces the controller with Reno. It is reached when a peer's address changes (`connection.go:1325-1336`), which is a client's NAT rebinding seen from the gateway, and when the client switches its own local transport (`connection.go:720-726`, `939-949`). For a `PathSender` it calls `OnPathMigrated` with the old and new local and remote addresses, after the connection has recorded them, and leaves the controller installed; for a `PathSender` the packet size returns to the initial size. Queqiao re-attaches the connection to another authority only if the path key changed: a new remote host, or a new local uplink, which is §10.2's path change. The comparison is against the key the controller was chosen for, not a recomputed one. Every reader of the path's state must follow that re-attachment, not only the controller: the coded path's sizing and the control generation's erasure reads still hold the model from installation (open after stage 1, assigned to stage 6).
9. **Packets classified as built.** `OnSent` is called for every packet, including ACK-only packets (`connection.go:2704-2725`) and path probes, with `Kind` and the flags set from the packet actually assembled. An MTU probe is built under an ordinary INTERACTIVE permit. A PTO probe may carry the retransmittable data quic-go selects; those bytes are reported by class and settle the deficits like any others. Until change 5 tags byte ranges, `ClassBytes` is assigned by frame type: STREAM data is BULK, DATAGRAM is INTERACTIVE, ACK, PING and path frames are EXEMPT, and everything else is CONTROL.
9a. **An exempt build path.** Today a window-refused connection can send an ACK-only packet but not a queued PING (`connection.go:2488-2512`), the keepalive is suppressed while congestion-blocked (`:895-910`), and path probes sit behind `SendAny` (`:2525-2558`). For a `PathSender`, when `Admit` refuses, the connection may still build one packet that carries only exempt obligations: ACK, a liveness PING, PATH_CHALLENGE or PATH_RESPONSE, and bytes covered by a verification grant. It is subject to amplification limits and socket back-pressure, and its bytes are settled at `OnSent`. The keepalive is no longer suppressed while congestion-limited. A quiet carrier beside a busy sibling therefore still gets its liveness test.
10. **Transport progress export.** `ConnectionStats` (`connection.go:817-866`) gains a coherent snapshot taken on the run loop: `ProgressEpoch` (incremented when an ack-eliciting application-space packet is newly acknowledged, including a late acknowledgement of one declared lost or retired for a PTO probe or by migration, found in the late-acknowledgement history), `LastProgress`, `PTOExpiriesSinceProgress` (not reset by a key drop, unlike `ptoCount` at `:238-241`) and `ElicitingOutstanding`. It is maintained for every connection, whatever its controller, so stage 0's shadow carrier states can use it before any controller changes.
11. **Adaptive keepalive in the fork's own timer.** The fork already owns one keepalive timer and its outstanding-PING state (`connection.go:701-705`, `879-884`). The interval becomes a schedule set by the application: it starts at a floor, doubles after each keepalive PING up to `KeepAlivePeriod`, and returns to the floor only when application payload is sent or received. A PING's own acknowledgement does not reset it. As built, a FIN-only STREAM frame counts as payload, and an outgoing long-header 0-RTT stream packet does not reset the schedule. The PING is an exempt obligation (change 9a).
12. **Datagram outcomes, exactly once.** `SendDatagramTagged(p, tag)` and an observer called on the run loop with `(tag, Registered | Discarded)`. The contract is that every tag the call *accepted* receives exactly one terminal outcome, and a tag the call refused receives none:
    - *Registered* when the packet carrying the datagram's frame is registered (`connection.go:2784-2814`).
    - *Discarded* when the packer drops it (`packet_packer.go:736-759`), when a close finds it queued, or when it was popped for a packet whose build failed before registration.
    - Acceptance and close are atomic under the send queue's lock: a tagged `Add` after close is refused. The pinned queue checked capacity before closure, so with room it could accept after `CloseWithError` and leave a tag that no drain would ever report.
13. **Verification grant binding.** `BindVerificationGrant(stream, grant)` and `SendStream.AcceptedOffset()` (§4.3). The bind is checked (nonzero ID, a non-empty range within the stream, a positive budget, a send stream of this connection; a nil or foreign stream is an error), is meant to take effect when it returns and to be cleared by a controller replacement; both, and reading, spending and charging the grant under one lock in the exempt builder, are the #176 fixes in bojieli/quic-go#3.

Nothing here changes a QUIC wire image.

## 6. Scheduling

- **Who assigns a class.** The sending endpoint's flow layer, from its own classifier and the frame type. The class byte in a peer's frame header remains a hint (PROTOCOL.md §7).
- **Who decides service order.** The authority (§4.3). **Who executes.** The builder.
- **Above the carrier there is no cross-flow queue.** A flow's lane writer and `frameConn.writeMu` serialise only that flow's own frames.
- **Control intent is owned by the flow's writer and lasts through a frame boundary.** A flow's DATA writer holds `writeMu` for the whole blocking stream write (`frameconn.go:426-456`), so an ACK for the reverse direction can wait *outside* the stream. `frameConn` keeps a count of control frames waiting for `writeMu`, under its own small lock.
  - The stream's boost property is on exactly while the count is non-zero: the waiter that raises it from zero calls `SetBoost(true)`, and it is turned off only after the last waiting control frame has itself been accepted by the stream. The zero-to-one and one-to-zero transitions are made under the same lock as the count update, including when a waiter cancels, so two waiters cannot interleave a stale `SetBoost(false)` after a fresh `SetBoost(true)`.
  - Because the property is persistent and applies to bytes accepted later, it does not matter whether the DATA writer's bytes reach the stream before or after the waiter arrives; there is no interval in which a check can be stale.
  - The control frame itself is written with `WriteClass(CONTROL)`.
- **Boosted bytes are charged to CONTROL's deficit** like any CONTROL byte, so a backlog of boosted DATA is limited to CONTROL's share of served bytes and cannot take over the path.
- **Scheduling-delay goal on wires 1 and 2.** From the moment a control frame is queued, it is selected for transmission after at most one chunk of its own flow plus that flow's buffered tail and pending retransmissions, all served at CONTROL priority within CONTROL's share. This holds while the stream and the connection have flow-control credit; the peer's lane reader drains the stream independently of application delivery (`inbound.go:10-26`). The time spent waiting for `writeMu` is measured separately from the time in the stream.
- **What that goal does not cover.** Delivery is ordered: if a packet carrying earlier bytes of the stream is lost, the control frame waits for QUIC's retransmission. That is the stated reason for wire 3 (§12).
- **Operator cap.** `limiter.Budget` is an endpoint-wide operator policy spanning paths and both substrates (`limiter/budget.go:14-20`). It is unchanged and stays at the flow's enqueue point. It is not copied into any `WireRate`.

## 7. Topology

**Decision: one connection per path in steady state; the authority supports several attachments exactly.**

Evidence for one connection:

- **Extra 4-tuples add no capacity on the measured access path** (`docs/DESIGN.md`, "Why QUIC data aggregation was removed"; `pathmodel.go:35-37`).
- **Isolation exists to buy priority.** Its stated purpose is to "keep a growing data stream from trapping new work and feedback in its connection congestion window" (`docs/DESIGN.md` §5). The fork's framer is plain round-robin (`framer.go:247-270`), so priority was bought with a second connection and controller. Isolating a lone flow was measured to cost about 8 % (`client.go:2706-2712`).
- **Most multi-connection problems vanish at k = 1.** No stale permits, no precedence between send loops, no per-lane startup, no megabytes committed behind a slow sibling lane.
- **CPU.** The 1-vCPU Beijing client ran at 25/44/71 % (median/p90/max; the round-6 A/B posted on #161) with four connections.
- **Mobile.** iOS allows one secondary connection (`docs/MOBILE-MEMORY.md`): exactly one carrier plus one standby.
- **The DC profile's own finding.** One, four and sixteen request-shaped flows estimated 6.8, 24.6 and 87.6 Mbit/s because each was application-limited alone. On one connection the aggregate is the sample.

Why several attachments must remain expressible: a handover needs two for a few seconds; old clients isolate and rescue on their own; and one QUIC connection plateaued at 292 Mbit/s against 536 for four on an unconstrained 200 ms path, with the remaining bottleneck unidentified (`docs/archive/2026-08-development/DESIGN-MULTIPATH.md:405-430`). Fan-out is a carrier-manager policy that stays off until that bottleneck is identified; it is part of the `dc-long-haul` gate (§11.3).

**Every byte flow's OPEN reserves the control role.** A new client sets `RESERVE_CONTROL` on every TCP-flow OPEN (as pooled flows do today, `client.go:1666-1682`), which is what lets a later JOIN replace that role (§10.3).

**One wire version per path at a time.** A flow is pinned to the wire of its OPEN and can only JOIN on a connection of that wire (PROTOCOL.md §10.1). So the wire of a path's carrier is chosen when the path's carrier set becomes non-empty and is kept, across replacements, until it is empty: every replacement is dialled offering exactly that wire (`client.go:1624-1638`), and new flows open on the existing carrier. A newer wire is adopted the next time the path has no flows. A client therefore never needs two carriers for two wire groups, which matters on a device with two connection slots. If a gateway stops accepting the pinned wire, those flows cannot be rescued, as today.

Connections are thus a parallelism and fate-sharing resource, never a congestion resource. A path whose 4-tuples are separately policed is several paths; nothing detects that today (`correlate.go:100-148`) and it remains a known limitation.

## 8. Flow layer

### 8.1 Chunk state machine (one repair owner)

```text
READY ─coded policy─► ENCODING ─last source symbol registered─► CODED(deadline = registration + clamp(2×RTT, 200 ms, 1.5 s))
  │                      │ abandoned: symbol refused or discarded,    │ covered by flow ACK ─────────► DONE
  │                      │ or the coded path closed                   │
  │                      └──────────────► READY[reliable]             │ deadline, or ranges show later coverage
  │                                                                   └───────────────────────────► READY[reliable]
  └─otherwise─► STREAM ── covered by flow ACK ─► DONE
                  └─────── binding replaced ─► READY[reliable]
```

- **A chunk's first attempt may be coded; every later attempt is on the stream.** The scheduler keeps a sticky per-chunk *reliable* bit, set when any attempt of that chunk is retired. It replaces the watchdog's byte frontier (`mpflow.go:2169-2186`) once the frontier is deleted; from stage 4 until then the two coexist. It is snapshotted at dispatch like `ForceReliable` today (`stripe.go:505-523`).
- **Attempt identity down to the symbol.** The coded path maps each source symbol it emits to the `(flow, attempt)` pairs whose bytes it carries (a symbol can carry several frames: `coded.go:402-432`), and tags each datagram (fork change 12). As built (stage 4), `coded.Path.SendObserved` accepts a frame with an observer and reports it exactly once: `Registered`, with the instant at which every source symbol carrying it had been registered in a packet, or `Abandoned`. Only source symbols of observed frames are tagged; repair symbols and unobserved frames (UDP PACKET, control) stay untagged. On a tagged carrier, outcomes are delivered by one extra goroutine per coded path, outside the switch, never on the submitter's.
- **The deadline starts at registration.** `coded.Path.SendContext` returns when a frame is copied into a queue (`coded.go:239-263`), and `SendDatagram` only places it in a 32-entry FIFO (`connection.go:3058-3080`). The CODED deadline starts when the datagram carrying the attempt's *last source symbol* is registered in a packet, measured from the packet's registration time, not from when the report reached the scheduler or when the scheduler's lock was taken. An attempt has no deadline before then. The same event is to feed the FEC source rate (§9), which lands with stage 6.
- **A frame stays the code's until it is registered or discarded.** The coded path never cancels a frame it has started emitting; there is no separate abandonment timer. A frame whose symbol is refused at submission, or whose carrying datagram the connection discards (packer drop, close, or a build that failed before registration), is `Abandoned` and the attempt is retired at once onto the stream. An attempt that is never registered therefore stays with the code until its connection discards it or closes, which fork change 12 guarantees is reported. There is no per-frame cancellation timer; what bounds the *flow's* wait is its no-progress budget. `SendObserved` promises exactly one outcome for a frame it accepted and none for one it refused synchronously; on a tagged carrier the registration time is captured in the fork's observer, before the outcome is delivered asynchronously to the scheduler; a carrier without tags reports registration at send acceptance, on the send loop. Callbacks carry the attempt ID, so one arriving after the attempt was retired is ignored.
- **A frame too large to fragment is that frame's failure.** A frame needing more than 65,535 fragments is dropped and reported as abandoned. Before stage 4 it stopped the coded send loop without closing the path, leaving a path that accepted frames and never sent them; the fix is outside the switch.
- **Gap evidence** stays what it is: ranges covering at least `lostChunkEvidence` beyond a chunk (`stripedsend.go:58-66`) make it probably missed, which suffices for a stream retry because a duplicate is harmless. It retires only a *registered* attempt, and only evidence observed after that attempt's registration counts. The observation time should be when the ACK arrived; as merged it is when the scheduler's scan ended, which can make one retry early (a harmless duplicate with one owner at a time; #178).
- **Retries are ordinary work** in the flow's own class under normal admission.
- **The OPEN exception is preserved**: before OPEN_OK the first DATA frame may go coded with one stream safety copy (PROTOCOL.md §8.1, `frameconn.go:404-424`).
- **A timer still exists.** The per-flow chunk supervisor (`stripedsend.go:282-304`) keeps running on both endpoints. What is removed is its coupling to the stall watchdog.
- **A STREAM attempt on a live binding is never copied.** Speculative copies of reliable attempts (`stripe.go:883-916`) are kept only for TCP bundles (§11.1).

### 8.2 The flow ACK contract

Three obligations. None is changed; they are stated because the design depends on them.

*Generation (receiver).*

- **G1.** When the contiguous point advances, an ACK is scheduled: sent at once if 64 KiB have accumulated since the last one, otherwise after a 10 ms timer; a threshold crossed while the timer runs does not interrupt it (`mpflow.go:2823-2824`, `2910-2964`).
- **G2.** The timer fires without a further arrival, so a transfer's tail is acknowledged.
- **G3.** If range ACKs were negotiated, an arrival that does not advance the point emits up to 16 ranges above it (`mpflow.go:2845-2877`).
- **G4.** DATA at or below the point repeats the cumulative ACK.
- **G5.** A newly admitted lane is sent the current ACK state. On main this happens only when the cumulative point is above zero (`mpflow.go:665-675`); a new build also emits it at zero when it holds ranges. Against an old peer G5 is therefore best effort, and §10.3 does not depend on it.
- **G6.** A normal final ACK is sent at the exact FIN offset after contiguous delivery and half-close. An abort ACK grants no credit.
- **G7.** The cumulative point advances as bytes are delivered to the application (`mpflow.go:3180-3184`). A blocked application therefore, correctly, withholds credit: this is the flow's end-to-end backpressure, bounded by the receiver's blocked-write budget.

*Carriage.* An ACK frame is CONTROL (§6). A blocked ACK write has its own deadline (`frameWriteTimeout`); when it expires the lane fails (`mpflow.go:2777-2787`), which is a binding-lost event with an owner (§10.4).

*Validated credit (sender).* `acknowledgeCoverage` validates and publishes coverage (`mpflow.go:2638-2667`); chunk completion follows (`stripedsend.go:448-490`) and the chunk's memory is released (`stripe.go:596-604`). Only this path completes chunks or releases their memory (I5). The ACK is not a send clock.

Cadence is deliberately not changed.

### 8.3 Memory

**Reservation lifecycle and owners.**

| State | Shared memory held | Owner | Obligation that ends it |
| --- | --- | --- | --- |
| STAGING: producer blocked reading the application | none (a small per-session buffer in the static footprint) | — | — |
| GRANTED/READY: bytes in hand, not yet dispatched | the bytes' reservation | the flow | dispatch, which waits only on carrier admission (every class is served: §4.3), on the operator limiter (a non-zero rate), or on a binding (bounded by §10); READY bytes count as pending work for the 45 s no-progress budget, which today looks only at sent offsets and tracked chunks (`mpflow.go:1987-2000`) |
| DISPATCHED: sent, unacknowledged | the chunk's reservation | the peer, through the flow-credit contract | G1–G7 and the cases below |

- **Staging.** Today the producer acquires a whole chunk from the endpoint budget *before* it blocks reading the application (`stripe.go:315-357`). On iOS 192 idle readers hold all 3 MiB (`mobile/core/resources.go:50`). Instead the producer blocks reading into its staging buffer and acquires shared memory only for bytes it holds, sized to their backing allocation. Only if that read filled the staging buffer does it try to fill the rest of a chunk, and only from bytes the application has already written: the full-chunk allocation is taken only if it is free at once (a non-waiting acquire), and the fill read uses a deadline 1 ms ahead, because Go's poller fails a deadline already in the past before it tries the socket, so "a deadline of now" never returns data. Otherwise, or where the source cannot do such a read, it publishes the bytes it holds as a short chunk. A second ordinary `Read` is never issued while holding a reservation. Because main violates the bound it claims (#169) and the change is local to the producer, this part is on by default wherever a shared send budget exists; the rest of the section is the `mem=owned` bundle.
- **Fair grants, on the send budget.** `memlimit.Budget` wakes all waiters and lets them race (`memlimit/budget.go:23-31`). The send budget becomes an arrival-order grant: released bytes go to the head waiter, a non-waiting acquire cannot overtake a queued waiter, and a grant that races cancellation is refunded. Each producer asks for one chunk at a time, so this is round-robin of one chunk per waiting flow. The receive budget needs no queue: receive admission never waits (a lane reader that cannot reserve must keep servicing ACKs), so there is nothing to order.
- **Reserved size.** A reservation is accounted at the size of its backing allocation, including a short staged chunk. A mostly empty chunk is compacted only if its smaller replacement can be reserved first.
- **Bounds (kept).** Per flow `maxSendBytes`; per endpoint `sendMemory` where configured; chunk-count backstops (`stripedsend.go:253-272`).
- **Dynamic bound.** `retentionBytes` sums the congestion windows of a flow's lanes (`stripedsend.go:129-146`); with one path window that would let every flow claim the path twice. It becomes a flow-layer autotune over two gauges the flow owns: `U`, dispatched-and-unacknowledged unique bytes, and `Q`, READY bytes. Once per credit round trip (`SRTT` plus the ACK coalescing delay; until stage 6 provides a path `SRTT`, the slowest lane's RTT): if newly validated unique coverage arrived in that round, *and* the producer was blocked on the bound at some point in it, *and* a lane had byte room to take more at that point, the bound doubles (up to `maxSendBytes`); otherwise it moves halfway toward `2 × max U` over the last four rounds, not below a startup allowance of four chunks, and never upward in a round without newly validated coverage. Since doubling needs a round that already had coverage, growth starts one round after the first credit, like slow start. Rounds are evaluated lazily and elapsed rounds are replayed, so a long idle gap still decays the bound. `Q` is limited separately by the write-ahead bound, so the bound never grows without credit arriving. Newly covered unique bytes are counted once across cumulative and range coverage, a retry is charged once, and a read is clamped to what remains of the allowance.
- **Write-ahead.** `windowBytes` (`stripedsend.go:108-120`) reads `Snapshot.Window`; the 64–512 KiB clamp is unchanged. Until stage 6 the READY limit is the sum of `windowBytes` over the lanes that dispatch would actually use for this flow's data, with a floor of one minimum lane queue when there is none.

**Progress.** Let flow F be blocked on memory.

*F is blocked on its own bound.* F holds READY or DISPATCHED bytes. READY bytes are covered by the owner table above. For each DISPATCHED chunk exactly one holds:

1. *Delivered to the peer application.* The receiver owes G1/G2 at CONTROL class on a live binding.
2. *Received, not yet delivered because the peer application is slow.* Credit is legitimately withheld (G7). F waits; both ends' no-progress budgets bound a wait *without progress*.
3. *In flight or lost on a live binding.* CODED: the deadline, abandonment or gap evidence moves it to the stream. STREAM: QUIC retransmits.
4. *Its binding is lost* (stream failed, or carrier replaced). The carrier manager rebinds F (§10) or F fails at the replacement deadline.

No case waits on F sending more data, so there is no credit deadlock.

*F is blocked on the endpoint budget.* Every holder is in the owner table, so every reservation is released by dispatch-then-credit or by its flow ending at a no-progress bound. Grants are round-robin. This bounds waiting without progress; it is not a completion-time bound.

### 8.4 Replay safety

Unchanged because the mechanism is unchanged: bytes are retained until flow-acknowledged, and a rebound flow replays after OPEN_OK (PROTOCOL.md §10), across lanes and across QUIC→TCP. Nothing is released on a transport acknowledgement or a local write. Completion tombstones still answer late JOINs (PROTOCOL.md §9.3); under `gateway-admission=owned` they hold no session slot (§10.5).

## 9. FEC, coding and the datagram stream

- **Placement.** Coding is a flow-layer substrate choice per chunk (§8.1), executed by the carrier's one coded path, whose sliding window spans every coded frame on the connection.
- **One ordered symbol stream, one class.** Frames from different flows, and UDP PACKETs, are packed and fragmented into one ESI-ordered stream (`coded.go:384-462`). Symbols cannot be reordered or dropped individually, so the stream is INTERACTIVE as a whole and the QUIC datagram queue stays a blocking FIFO.
- **A high-volume UDP association is therefore served at INTERACTIVE weight.** That is today's behaviour (UDP always takes the datagram substrate) and is kept deliberately; a PACKET carried on the reliable stream is INTERACTIVE as well. Whether heavy UDP should instead be charged as BULK is Q5.
- **Overflow acts on whole frames at the coded path's input**, which is bounded in bytes as well as frames. A producer obtains input capacity for the encoded length *before* the frame is serialised (today the caller serialises with `protocol.AppendFrame` and `SendContext` then copies and waits: `frameconn.go:346-353`, `coded.go:239-263`), so neither a serialised buffer nor a copy exists for a frame that is still waiting. DATA frames back-pressure their flow and are never dropped. UDP PACKET frames wait in a bounded input queue; on overflow the coded path itself removes the oldest whole PACKET frame that has not started encoding.
- **Erasure inputs.** `Snapshot.Erasure`, and `Snapshot.DatagramBurstFactor`: the neighbour-transition statistic of §4.2 restricted to packets that carried a datagram. Until it has enough samples the path-wide `BurstFactor` is used.
- **Sizing rate.** `coded.Path.rate()` reads `Share` and otherwise assumes 64 symbols per round trip (`coded.go:880-896`). `RateBytesPerSec` becomes the rate at which *source* symbols were registered in packets over the last round trip. Repairs are excluded, so parity cannot raise its own sizing input, and registration is used, not queue acceptance, so a backlog does not read as demand. While the encoder has been active for less than a round trip, or after an idle gap, the existing 64-symbol prior (`coded.go:889-896`) is used. The encoder window keeps its explicit bounds (PROTOCOL.md §12.4).
- **Budget.** Symbols and repairs are debited from the one balance.
- **Policy.** Unchanged (`mpflow.go:1263-1299`).

## 10. Liveness and rescue

### 10.1 Roles

**The client initiates every replacement.** On wires 1 and 2 only the client sends JOIN, and a gateway cannot dial a client behind NAT. Each endpoint demotes its own silent attachment to PROBING on its own transport evidence, and only the client's promotion is irreversible (§4.4). The gateway keeps its flows in the existing replacement grace and waits for the client's JOINs or the common deadline.

### 10.2 Carrier states (client, QUIC)

Evidence is fork change 10. It is never packet counts (`poolliveness.go:35-43`) and never flow ACKs.

```text
CONNECTING ─► VERIFYING ─► ACTIVE ◄──── progress epoch advances (before promotion) ────┐
                              │ k_s PTO expiries since progress                         │
                              ▼                                                         │
                           SUSPECT ── standby verified, epoch unchanged, k_p expiries ──┼─► DRAINING ─► CLOSED
                              │ idle timeout or connection error                        │   (irreversible)
                              └────────────────────────────────────────────────────────► DEAD
```

- **SUSPECT** costs at most one dial round. The carrier takes no new flows, its attachment becomes PROBING, and one STANDBY is dialled. Nothing moves.
- **Verification** is today's bounded PROBE exchange on a new stream (`poolprobe.go:95-127`). It needs a reply from the gateway. Both directions are admitted under each endpoint's verification grant (§4.3), so neither end's verification waits on the dead carrier's flight or on a negative balance.
- **Promotion** requires a verified standby, an unchanged progress epoch on the old carrier since SUSPECT, and `k_p` expiries. It is single-flight per path and increments the path's generation.
- **Every dial round is charged to one path-level attempt history**, whether it ended in a promotion, a failed verification, or the old carrier recovering first. The cooldown doubles with each charged round and the history resets only after a healthy dwell, so a carrier that flaps between SUSPECT and ACTIVE cannot produce a dial storm. Replacement attempts keep today's cap: eight, reset after a sustained healthy dwell (`client.go:43-44`, `2683-2691`). A dial round's spray (at most three concurrent attempts, as today) is one attempt, and its connections count toward the device's connection cap.
- **If the old carrier makes progress before promotion** it returns to ACTIVE and COUNTED; the standby is kept warm for 30 s, then closed. After promotion nothing returns.
- **DEAD with no standby.** Flows are unbound and share one replacement grace for the path (the present 45 s; `TestOneOutageSpendsOneGraceHoweverManyWaitersItHas`). AUTO mode's TCP handoff remains available per flow.
- **When make-before-break is unavailable.** A gateway of a released version charges its session limit to every stream before reading the first frame, and can refuse a standby connection outright when full (§10.5). A standby that cannot be dialled or verified is simply absent: the carrier stays SUSPECT until progress or DEAD, and recovery is the break-before-make path above, which is what happens today.
- **A saturated released gateway cannot be rescued by the client.** Its OPEN handlers keep their session slots while they wait for a JOIN (`server.go:945-952`), and a JOIN, over QUIC or TLS/TCP, needs a slot before it is read (`server.go:526-536`, `760-773`). With every slot held by the flows being rescued, each JOIN is refused until those flows expire. This case exists on main. It is stated as unrecoverable against released gateways; the mixed-version recovery guarantee requires session headroom on the gateway, and gateways are upgraded first.
- **`k_s` and `k_p` are calibrated, not asserted.** For independent loss with round-trip survival `q`, `P(reach expiry n | first expiry) ≈ (1−q)^{2(n−1)}`: with 50 % loss one way and 5 % the other, 28 % for `n = 2`, 7.6 % for `n = 3`, 2.1 % for `n = 4`; burst loss makes it worse. That is why SUSPECT moves nothing. Thresholds are set from the measured conditional false-promotion rate (§15), and the state machine first runs in shadow mode beside the existing rescue.
- **Keepalive.** Fork change 11, with a floor of `max(4×SRTT, 500 ms)` while the carrier has open flows. On a healthy quiet connection it sends at 0.5, 1, 2, 4, 5, 5 … seconds and settles at today's 5 s.
- **Path change.** A new uplink identity is a new path key: new authority, old carriers DEAD.

### 10.3 Bindings and rebinding

**Binding state is persistent and reconciled.** The carrier manager keeps, for every flow, either a binding (identified by generation and lane) or the fact that it is *unbound and needs a JOIN*. That fact is not an event that can be missed. Whenever a carrier becomes eligible (it is ACTIVE, including on return from SUSPECT, or it has just been promoted), the manager reconciles: every unbound flow is queued for a JOIN on it. A `BindingLost` report and a JOIN completion both name the binding they refer to, so a late report about an old binding cannot undo a newer one. A flow whose stream fails while its carrier is SUSPECT therefore waits, unbound, for whichever comes first, the carrier's recovery or a promotion, and is rebound either way.

After a promotion, and for any other reconciliation:

1. **One JOIN per flow, on a stream of the eligible connection, with `RESERVE_CONTROL`, and the old stream is closed on commit.** In this topology a flow has one lane, which holds the control role its OPEN reserved (§7), and a `RESERVE_CONTROL` JOIN replaces that role on both wire versions (PROTOCOL.md §10; `client.go:2023-2029`). With one lane the lane table is never full, so wire 1's age rule for a full table (`mpflow.go:971-986`) does not arise. This is the path pooled flows already take (`TestPooledFlowsRecoverThroughOneReplacementGeneration`). A released gateway that has room in its lane table *adds* the new lane and keeps the old one schedulable (`server.go:229-257`, `1197-1202`), so when the client receives OPEN_OK it closes that flow's old stream; the gateway then sees that lane fail and stops using it. A new gateway transfers the control role, but **commits the transfer only after the replacement's OPEN_OK**:
   - *Admission stages.* Admitting the JOIN inserts the candidate lane as staged and records the lanes it explicitly displaces: a lane the JOIN names with `REPLACE_LANE`, and, at the lane ceiling, the lane its admission evicts. Nothing is removed yet.
   - *Commit.* Once OPEN_OK has been written and the lane activated, the recorded lanes are retired, and then the *current* control holders are examined: each that is suspected, or older than the eviction protection, is retired. A holder younger than that is kept, because it is a rescue that has just won; youth does not protect an explicitly displaced lane or a suspected holder. PROTOCOL.md already requires OPEN_OK before a new lane becomes schedulable, so this is the first point at which the candidate can carry the flow.
   - *Rollback.* If OPEN_OK cannot be delivered, removing the staged lane undoes the JOIN, and the lane table, control holder included, is as it was. A failed make-before-break attempt therefore never costs the lane that was still working.
   - This is the `gateway-admission=owned` behaviour. In the default mode a new gateway admits as a released one does: with spare capacity and no explicit replacement it adds the lane beside the old one, and a named or ceiling victim is retired at admission.
2. **Stream slots are reserved for existing flows.** A JOINed flow keeps its stream for life, so slots are a capacity, not a rate. On a promoted carrier the manager admits a new OPEN only against stream credit in excess of the number of flows still waiting to rebind. If the peer grants fewer streams than there are flows to move, the remainder are handled by the binding deadline of step 4.
3. **Concurrency scales with the queue.** JOINs are small CONTROL exchanges. The number in flight is `max(16, N × r / T)` for `N` flows to move, JOIN round trip `r` and a target `T` of a few seconds. Order: flows with a pending FIN or ACK, then INTERACTIVE, then BULK.
4. **Every binding operation has a deadline, and it is the gateway's.** The only cross-endpoint event that keeps a flow alive at the gateway is that flow's own JOIN arriving and being parsed; verifying a carrier says nothing to the gateway about any particular flow, and the gateway runs its last-lane grace per flow whether or not the flow has bytes pending (`mpflow.go:1769-1772`). So each flow that is unbound, or bound to a superseded carrier, carries a *binding deadline*, which applies to idle flows too; the delivery no-progress budget remains a separate timer for pending bytes.
   - *The two endpoints observe the loss at different times.* The gateway's grace `G` (45 s) starts when *it* loses the flow's last lane; the client starts counting when *it* observes its binding fail or be superseded. Main budgets `G` as 15 s for detection skew plus 30 s for rescue (`mpflow.go:57`), which is the assumption here: the client's observation is at most `S` = 15 s later than the gateway's.
   - *The deadline is on JOIN arrival, so the latest handoff start subtracts the handoff's own transit.* With `t_c` the client's observation, the JOIN must reach the gateway by `t_c + G − S`. A JOIN on either substrate must *start* by `t_c + G − S − J`, where `J` bounds the whole interval from starting the handoff to the gateway parsing a valid JOIN: for TCP the dial, the TCP and TLS handshakes, the frame's transit and any first-frame admission wait; for QUIC the stream open, any admission wait and the frame's transit. No single existing timeout bounds that interval (the dial, the TLS handshake and the first-frame read have separate deadlines, and a QUIC stream can wait for admission before its read deadline starts), so `J` is a configured budget, set from measured handoff times, and a round trip is only an estimate of its QUIC part.
   - *When the skew or the transit cannot be bounded*, for example when the gateway's lane failed on its own write deadline long before the client saw anything, or a handoff stalls past `J`, recovery is best effort. Nothing on wires 1 and 2 tells the client when the gateway's grace began.
   - The JOIN queue is served earliest-deadline-first within the order of step 3.
   - A flow that will not get a stream slot by its latest QUIC JOIN time starts AUTO's TCP handoff, no later than its latest handoff start.
   - In QUIC-only mode such a flow fails at its binding deadline. That is the actual boundary on wires 1 and 2: stream credit that arrives after the gateway's grace cannot save the flow.
   - DRAINING ends when no flow is bound to it, which the binding deadlines bound at one grace. On iOS that also bounds how long a DRAINING carrier can hold the second connection slot; a second failure during the drain falls back to break-before-make.
   - A carrier is never closed under a flow that still depends on it (#159).
5. **Replay is gated on the new binding.** The scheduler holds a rebound flow's READY data until the first ACK arrives on the new lane, or for one SRTT. ACK and FIN frames are not held. A new peer re-emits its ACK state whenever it has any (G5); a peer with nothing received, or an old peer at a cumulative point of zero, sends nothing, and the timeout applies. The cost of the timeout is at most replaying chunks the peer already holds.
6. **Replay is ordinary READY work** under normal admission. Its rate is the path's rate; its volume is bounded by retained bytes.
7. **Unconfirmed OPENs.** While the carrier is only SUSPECT or DRAINING, a flow whose OPEN is unconfirmed is not rebound, because a JOIN can overtake an OPEN the gateway has not admitted (`client.go:2509-2515`); the carrier's probes keep retransmitting its OPEN. When its lane has actually failed, main's rule for a failed lane applies: the JOIN is attempted and the gateway holds or refuses it (`openjoin_test.go`).
8. **Failures.** A refused JOIN keeps today's meanings: capacity is transient within the per-flow budget; "does not hold this session" is terminal for that flow.

### 10.4 Flow-scoped evidence

| Evidence | Meaning | Remedy | Owner |
| --- | --- | --- | --- |
| a coded attempt passed its deadline or was abandoned; ranges show later coverage | the code did not cover it | retry on the stream (§8.1) | flow scheduler |
| the flow's stream failed definitively (reset, read or write error, write deadline) | the binding is lost; the path may not be | the flow becomes *unbound*; reconciliation JOINs it on the next eligible carrier, which is the same carrier if it is or becomes ACTIVE (§10.3); per-flow replacement budget; no dial | carrier manager, on `BindingLost` |
| unacknowledged bytes on a live stream | QUIC is delivering, or the peer application is slow, or the ACK is in transit | none | — |
| no acknowledged progress for the no-progress budget (45 s) | terminal | fail the flow, release its memory | flow |

- A stream failure is definitive evidence about one flow, so its remedy is one stream. Main reaches the same event through `failLane` (`mpflow.go:1346-1375`, consumed at `:1751-1796`) and answers it with a per-flow dial; here the same connection carries the JOIN.
- Missing credit on a live stream has no remedy between "wait" and "fail". The transport cannot tell a faulty peer flow from a slow application: QUIC can acknowledge every byte while the receiver, correctly, withholds credit (G7). Replacing the stream would replay into the same blocked application.
- On the gateway a failed stream puts the flow into the existing replacement grace, as today.
- The stall watchdog's other duties go away: it no longer demotes a lane (`suspectDataLanes`), requests a dial+JOIN round (`client.go:2498-2551`), or pins a byte frontier.

### 10.5 Gateway admission of recovery work

A gateway holds one slot of its session semaphore for every stream, taken before the first frame is read, and closes the stream when none is free (`server.go:760-773`); a JOIN's handler and a PROBE take slots like an OPEN, and the connection semaphore has the same size (`server.go:376-378`, `655-669`). A gateway whose sessions are all in use therefore refuses exactly the PROBE and JOINs that would let those sessions recover. This is true on main today. In a new gateway:

- **Sessions are charged at OPEN, at both the QUIC and the TLS/TCP entry points.** The session limit counts logical flows. A stream's first frame is parsed under a separate, small per-connection bound with the existing handshake deadline, and under a gateway-wide bound on streams being read before admission, so frame readers cannot grow with the number of connections. A connection whose per-connection bound is full waits rather than refusing. An OPEN refused at the limit gets `RESET FLOW_LIMIT`, a code both wires already share.
- **A JOIN is charged to the flow it joins**, within that flow's lane ceiling, not to the session limit. A PROBE is charged to a small per-connection probe allowance (its work is already bounded: PROTOCOL.md §14). A JOIN still extends the flow's grace when it is parsed, as on main.
- **Completed-session tombstones are not sessions** (owned mode). The records kept to answer late JOINs (§8.4) hold no session slot, are left out of the live count, and have their own bound; past it a record is dropped before its flow's slot is released. A completed flow's slot is therefore usable at once.
- **Connections: the general bound stays; one standby per device rides past it.** There is no per-principal connection limit. When the general connection bound is full, a device (the authorised principal including its device identity) that already holds a data connection may have exactly one more, its standby. The overflow is therefore bounded by the general bound itself. The standby takes over the general slot of the first of that device's connections to close, so the device can have a standby again for its next recovery. A connection's slot is released when the connection ends, not when the flows it once carried end; this holds in every mode, below saturation too. The pre-authentication bound limits only handshakes in progress; on the TLS/TCP entry point a gateway-wide bound covers the TLS handshake and first-frame admission, in owned mode and on the saturated default path.
- **The part that fixes a bug is on by default.** In the default mode, recovery admission changes only in the saturated case: a stream that finds no session slot has its first frame read under the bounds above, and only a JOIN for an existing flow or a valid authenticated PROBE is admitted; anything else is closed unanswered as before. The one-standby rule applies too, and a connection's slot is released at connection end at any load. Charging sessions at OPEN and answering `FLOW_LIMIT` change what the limit counts and what a client is told, so they are the `gateway-admission=owned` bundle.

Against a released gateway the degraded path of §10.2 applies.

### 10.6 Cases

- **#92 one-way failure.** Either dead direction stops the client's progress epoch, so its carrier reaches SUSPECT on transport evidence without waiting for the idle timeout.
- **#53/#158 single TCP lane.** §11.1; unchanged.
- **#114 JOIN atomicity, late losers, eviction.** Gateway admission rules for a JOIN are unchanged (`TestRejectedJoinPreservesExistingLanes`, `laneisolation_test.go`).
- **Coded tail.** §8.1; independent of every state in this section, on either endpoint.

## 11. Other substrates and profiles

### 11.1 TLS/TCP fallback

- A TCP carrier is its own congestion authority (the kernel). Queqiao adds no window above it (`laneadmission.go:8-18`).
- A flow never mixes QUIC and TCP data (PROTOCOL.md §10).
- TCP exposes no portable acknowledgement signal, and a TCP carrier serves exactly one flow. So for TCP the flow-progress timer is the carrier-liveness proxy, and the per-flow manager is kept as it is (`manageTCPBundle`, `client.go:2758`): EOF, reset or write error is DEAD at once even when idle, the zero-lane and stall-rescue budgets are separate (#158), bundles keep their 2 s reissue delay and one speculative copy of the oldest missing chunk. The signals coincide here because the scopes coincide.

### 11.2 UDP ASSOCIATE

- All three carriage modes are preserved: QUIC datagrams through the coded path; the reliable stream when DATAGRAM was not negotiated or `UDPOnStream` is set (`frameconn.go:191-220`); the stream on TCP. The 64-packet replay window, fragmented PACKET handling, resume-token rotation and non-resumable associations are unchanged. AUTO's UDP-to-TCP rescue is unchanged.
- **Promotion and the relay: one client ordering for every gateway.** A gateway hands a relay back only if it is *parked*, which happens after the old lane has ended (`udp.go:709-765`). The client cannot tell a new gateway from a released one before it acts, so it always does the same thing: close the old association's lane, then send the resume OPEN.
  - *Released gateway.* If the close arrived *and the gateway had finished parking the relay before the resume OPEN's claim* (parking happens in the old handler's deferred cleanup, after its workers are joined: `udp.go:818-835`, `921-939`), it is handed back. If the old carrier was dead and the close did not arrive, the gateway opens a fresh relay; resume is best effort, as documented (PROTOCOL.md §8.2).
  - *New gateway.* The relay store additionally accepts a resume token that names a relay still held by a live association of the same principal. It fences the old handler's deferred park and close, joins its forwarding workers, then publishes the new owner and rotates the token. The transfer moves the association's logical session slot and account slot (its *lease*); it does not charge new ones. Ending a live association and parking its relay is one step under the relay store's lock, so the token is claimable throughout. If the old handler does not stop within half the handshake deadline, the replacement attempts a fresh open, as on a released gateway, which is subject to capacity like any OPEN. All of this is `gateway-admission=owned`.
  - *The rotated token is not trusted until it is known to have arrived.* When a replacement actually reclaimed a relay with the old token, then until the client has sent on the replacement's lane the relay answers to both tokens: the old one, in case the OPEN_OK was lost, and its own new grant. Taking it under either consumes both. A fresh relay opened as a fallback does not acquire the old alias. In owned mode a reclaimed relay whose OPEN_OK write fails is kept for a retry; a fresh one may be dropped.
  - *Leases follow confirmation.* A parked relay keeps its lease for its replacement and releases it on expiry; failed or expired leases are released before a waiting replacement is woken or an OPEN is refused for capacity. A fresh association that never heard from its client keeps its relay reclaimable but gives its slots back before the relay becomes visible. The OPEN_OK write keeps the deadline already running, not a fresh one.
  - The gate reports the two outcomes separately.

### 11.3 `dc-long-haul` (hierarchical)

- **During the migration the profile runs the legacy bundle unchanged**: per-connection `ErasureSender`s reporting to a `pathmodel.Chain` (`transport.go:1136-1145`), isolation, the bulk pool and per-flow recovery, selected by the profile exactly as today (`profile.go:194-204`, both client dial sites, and the gateway's own selection at `server.go:703-706`).
- **Why its limiters are not migrated.** A chain of limiters needs an owner for joint decisions that this document does not design: two children can both pass a parent's check before either debits it; a leaf can sleep on a parent while another leaf frees it; an ancestor's first binding rate is undefined; a persistently binding ancestor censors its descendants' estimates; correlated reverse-path delay could engage an unrelated forward limiter.
- **The DC gate (stage 9).** The profile is run on the new architecture with one authority per leaf (endpoint pair), the tree used only as hierarchical *knowledge* (priors for erasure and round trip, which I3 permits), and fan-out if the single-connection ceiling binds. The acceptance thresholds are fixed before it is run: with the profile's published results as the baseline (`docs/DESIGN-DC-PROFILE.md`; README: 302 ms cold and 237 ms warm for the 355 KB request, 310 Mbit/s sustained), sustained goodput must pass the 0.90 gate of §15.4 against the legacy bundle measured in the same campaign, request latency must not exceed the legacy bundle's by more than 10 % at the median and p99, and the hierarchy's own measured benefit must be preserved: under a simultaneous multi-leaf workload with a binding common ancestor and an uncongested sibling as control, the worst leaf's service and the spread of completion times must be within a pre-registered bound of the legacy bundle's (the profile document reports the regime this guards: independent relays gained about 2.5× aggregate throughput while their groups spread by 3.5×, `docs/DESIGN-DC-PROFILE.md:149-151`, `468-475`). A knowledge-only tree that fails the fairness criterion fails the gate.
  - *Pass:* the profile moves to the new architecture and the legacy bundle is deleted.
  - *Fail:* the legacy bundle stays for this profile, nothing it needs is deleted, and joint limiting waits for a tree-arbiter design (Q2): one arbiter per tree, under one lock, that evaluates and debits a leaf's whole chain jointly and owns the waiters.
- **As built (stage 9).** The profile accepts exactly two bundles, and start-up refuses everything in between: the legacy bundle (every switch off, `classifier=phased` refused) and the whole new architecture, `cc-api=v2 sched=class congestion=path coded-retry=sticky recovery=carrier`, plus `topology=single` on a client; `classifier=phased`, `mem=owned` and `gateway-admission=owned` may ride along. The default is unchanged.
  - *One authority per leaf.* The authority is the stage-6 `PathController` with its stage-6 key, (identity, uplink, remote host). For a multi-provider client process, where every `Client` reaches its own gateway, that key is the endpoint pair. Two identities on one endpoint pair are still two authorities (§11.4); the tree's endpoint-pair node is what they share, as knowledge.
  - *The tree as knowledge.* The authority's own model is the leaf. Its ancestors are the tree's existing nodes for the path (the uplink, then the endpoint pair), the same nodes a legacy chain to that pair crosses. Every report reaches every node. What the law and the FEC readers read back is the leaf's own state once the leaf has `confidentSamples` (100) decided samples. Before that, the leaf's erasure, burst factor and round trip are raised to the largest any *confident* ancestor knows. A share or seed is never read (`pathmodel.Knowledge`). The law reports no delivered rate (stage 6), so no node computes a share. A gateway under the profile places its authorities in its own tree the same way; the legacy gateway had no tree.
  - *No bandwidth hint.* §4.1's "initial pacing hint" is not taken from the tree. A sibling leaf's rate says nothing about this leaf's bottleneck, and a seed is what I3 forbids.
  - *Fan-out.* Not built. `recovery=carrier` refuses `--topology-fanout` (stage 8), and the profile gets no exception. On the emulator the single-connection ceiling did not separate the bundles. Both sustained 45–60 Mbit/s on the 333 Mbit/s emulated path, a ceiling of the in-process emulator, not of either bundle. So the emulator cannot answer whether the 292 Mbit/s plateau binds; only the live gate can. If it binds, what is needed is fan-out owned by the carrier manager. That means several ACTIVE carriers in one path's carrier set, each an attachment of the one authority, with bindings spread across them and each carrier verified and replaced on its own evidence. §7 and Q3 leave this undesigned.
  - *Deterministic fairness (`TestDCMultiLeafFairnessHoldsWithAKnowledgeOnlyTree`).* The setup is one client process, three loaded leaves behind one emulated 12 Mbit/s ancestor, an uncongested sibling on its own 200 ms path, and the gate's two phases. Measured on uploads, where the legacy chain's shared node caps every member, with three trials per arm, ABBAAB, against the gate's frozen bounds. The test asserts what knowledge-only must guarantee: requests complete, the ancestor binds (legacy aggregate / solo sum ≤ 0.80), and the sibling is not throttled. It reports the six §11.3 contrasts as a verdict instead of asserting them. A FAIL is the design's legitimate outcome, not a defect. Measured twice. With two trials per arm the knowledge-only tree **failed** three contrasts: worst-leaf service (0.85× legacy against 0.90), Jain (0.856 against 0.927 − 0.05) and loaded request p50 (+17 %). With three trials per arm it **passed** all six: worst leaf 1.00×, Jain 0.863 against 0.858, spread 1.27 against 1.25, aggregate 1.13×. The legacy arm's own Jain index ranged from 0.80 to 0.97 across trials. At this size the emulator cannot separate the bundles, so it detects a gross regression, not a 10 % one. The live gate decides.
  - *What the live gate cannot see.* In the legacy code the tree exists only in a client process (`congestionConfig.hierarchicalPath` is never set on a gateway), and its shared nodes couple only `Client`s in the same process. The gate's campaign runs one client process per leaf host and measures downloads, which the gateway sends. Neither arm therefore has a joint limit in that campaign. The live gate measures one authority per leaf against independent per-peer legacy controllers, not against the hierarchy's joint limit. The joint limit is exercised only by the deterministic test above. The profile document's 2.5×/3.5× regime is one shared *relay* (one client, one connection) against several, which neither bundle changes.

### 11.4 Several clients behind one NAT

The gateway's key includes the authorised principal, so two devices behind one address get two authorities and two minimum RTTs. They share a bottleneck as two BBR flows do, with the delay bound as their common signal. Two connections from one device and host attach to one authority.

### 11.5 Mobile

- Limits (`docs/MOBILE-MEMORY.md`): 28 MiB Go soft limit inside a 50 MiB extension ceiling on iOS; 3 MiB shared and 1 MiB per-flow retention; 2 MiB connection and 64 KiB stream receive windows; one secondary connection.
- **Connection cap.** ACTIVE, STANDBY candidates and DRAINING together are capped per profile. On iOS the cap is two: one ACTIVE and one STANDBY, so a dial round is sequential, and the DRAINING carrier is the previous ACTIVE one. With one wire per path (§7) no second group competes for the slot.
- **Controller memory** is one fate ring (8192 records) per path instead of one sampler per connection.
- **Windows.** All flows share the connection window: about 84 Mbit/s in aggregate at 200 ms on iOS, and about 2.6 Mbit/s for one stream from its 64 KiB window. Both limits exist today.
- **Staging.** With 1,024 sessions the staging buffer must be small (1 KiB is 1 MiB worst case) and is part of the static footprint.

## 12. Compatibility and wire

**No wire change.** PROTOCOL.md §18 exempts sender-only policy that preserves every wire invariant.

| Pair | Behaviour |
| --- | --- |
| new client, old gateway | The client sends under its authority and never isolates. The old gateway sees one connection, so its pool has one member and applies no share (`pathmodel.go:311-322`). While a handover leaves exactly two members reporting, it caps each at 0.625× the pooled estimate, until five seconds after the old one's last report (`pathmodel.go:190-197`). Make-before-break and UDP relay transfer degrade as in §10.2 and §11.2. G5 is best effort (§10.3). |
| old client, new gateway | The old client isolates and rescues as today. The gateway attaches all of that device's data connections to one authority and classifies its own sending flows. |
| old, old | unchanged |

**Wire 3 (not part of this migration).** Recorded so that it is not improvised later:

- *A redundant, unordered copy of the ACK.* The stream stays the carrier of record for every ACK. Wire 3 may additionally send the same cumulative point and bounded ranges in a CONTROL datagram, validated exactly as a stream ACK. Losing any number of copies loses nothing.
- *Gateway instance identifier* in OPEN_OK (#165).
- *Batch JOIN*: one frame rebinding many flows after a promotion.

It is considered only if ordered stream delivery is shown to dominate flow-ACK delay after the migration, and must first pass a lost-tail test in both directions.

## 13. Historical failures, by construction

| Failure | Why it cannot occur |
| --- | --- |
| #73 seed inflation | No member reports to a pool. The filter holds only delivery samples (I3). The compensation division happens once. |
| #146/#161 under-use behind an honest share | No share exists. A new connection or flow draws on the same balance. |
| #160 tail regrowth; idle-member share; summed stale peaks | No per-lane BBR state, no membership, no split, one delivered counter. |
| ACK-channel throttling | Non-eliciting packets are not debited (I2); flow ACKs are CONTROL with a bounded debt and writer-owned boost; application-limited comes from unused budget; no cap exists. |
| False-stall rescue amplification | A dial is triggered only by transport evidence, is single-flight per path, and moves nothing until a standby is verified. Stream attempts on a live binding are never copied. |
| Coded tail loss | A coded miss moves to the stream on a deadline that starts at packet registration, in any carrier state, on either endpoint. |
| #53/#159 lane replacement | One carrier manager per path owns bindings and replacement; DRAINING waits for its flows; TCP flows keep their own manager. |
| Present on main: idle readers exhausting shared memory | Shared memory is acquired only for bytes in hand, and every reservation has an owner. |
| Present on main: controller reset to Reno on NAT rebinding | Migration keeps the `PathSender`. |
| Present on main: a full gateway refuses the JOINs and PROBEs that would free it | Sessions are charged at OPEN; recovery work has its own bounded admission. |

## 14. Migration

Configuration accepts exactly the bundles below and rejects any other combination at start-up. Exactly one component owns QUIC recovery in each.

| Stage | Bundle adds (requires) | Content | QUIC recovery owner | Status |
| --- | --- | --- | --- | --- |
| 0 | — | Finish the 2×2 and B1/B2. Instrument: ACK carrier, sender-side credit delay, `writeMu` wait, admission refusals by reason, reissues by cause, shadow carrier states (from fork change 10, which stage 1 maintains for every connection). Only the baseline experiments run here; the class-scheduler arm of §15.3 needs stage 2. | per-flow (today) | partly done: 2×2 and B1/B2 done, and the #161 trace measured ACK carrier, `writeMu` wait and reissues by cause; the progress export is merged with stage 1; shadow carrier states in progress on a branch, no PR |
| 1 | `cc-api=v2` | Fork changes 1–3 and 6–13 are *present*; today's `ErasureSender` is driven through them, one per connection. *Activated* behaviour changes are exactly three: non-eliciting packets are not debited; the controller is kept across migration; exempt obligations have a build path. The keepalive schedule, progress export, datagram tags and verification grants are present and unused. Accepted only with `--congestion erasure` and outside `dc-long-haul`. The #176 fixes are required before v2 is enabled anywhere. | per-flow | **merged**: #175, bojieli/quic-go#2 |
| 2 | `sched=class` (1) | Fork changes 4–5; class tags; boost; the §4.3 arbiter, with normalisation before selection, instantiated per controller; the #176 fixes. | per-flow | **in review**: fork side bojieli/quic-go#3; queqiao side implemented on a branch, no PR yet |
| 3 | `mem=owned` | Staging; reservation owners; fair grants; retention autotune. Independent. The #169 fix is on by default where a shared send budget exists. | per-flow | **merged**: #172 |
| 4 | `coded-retry=sticky` (1) | Registration-based coded deadline; sticky reliable bit. The watchdog's frontier remains beside it. | per-flow | **merged**: #177; follow-ups #178 |
| 5 | `gateway-admission=owned` | §10.5, the staged control transfer of §10.3 step 1, and the UDP relay transfer of §11.2. Gateway only. Independent. The #170 fix is on by default for the saturated case. | per-flow (unchanged) | **in review**: #173 |
| 6 | `congestion=path` (1, 2) | `PathController` with attachments; sampler state retired by fate (§4.2); every path-state reader re-keyed with its attachment (§5 change 8); FEC sizing from registered source symbols (§9). Works with isolation on, which is also the old-client case. | per-flow | in progress on a branch, no PR |
| 7 | `topology=single` (6) | Client stops isolating. Per-flow rescue still dials; those connections are attachments. | per-flow | **in review** (stacked on stage 6) |
| 8 | `recovery=carrier` (7, 4) | Carrier manager becomes the single owner of bindings and QUIC replacement; per-flow QUIC rescue dials and lane demotion are disabled by the same switch; the adaptive keepalive schedule is activated; binding deadlines with the skew rule of §10.3 step 4; §10.4. | carrier manager | not started |
| 9 | DC gate | `dc-long-haul` on the new architecture (§11.3): the profile accepts the legacy bundle or the whole stage 1–8 bundle (with `classifier=phased`) and nothing in between; one authority per leaf; the tree as knowledge only. | carrier manager | implemented, opt-in; live gate not run |
| 10 | defaults | Flip defaults in one release, gateways first; soak one release. Requires the final bundle to pass trials A, B, C, D and the DC gate of §15.3 under §15.4 on every path, and the deterministic replacements of §15.3 in CI; one failure blocks the flip. | carrier manager | implemented in a draft PR, merged only after the live gates pass; rollout plan in `docs/ROLLOUT-v0.15.md` |
| 11 | delete | Below. | — | not started |

- **Fixes of present bugs ship on by default, small, beside their bundle.** Two stages found that main violates a bound it claims and fixed it in the default configuration with a minimal, local change: the producer's reservation before reading (#169, stage 3) and the full gateway that refuses its own recovery work (#170, stage 5). Everything that changes policy, rather than restoring a claimed bound, stays behind the switch.
- **The legacy bundle** is everything off: today's behaviour. It is what `dc-long-haul` and `--congestion reno` run until stage 11, and it is start-up and end-to-end tested in every release until it is deleted.
- **Rollback.** A switch can be turned off when its dependants are off; the configuration validator enforces that.
- **Test-only knob.** `isolation=off` on the legacy bundle (flows stay on the pooled connection) exists for the product-configuration screen of §15.3 and is not a supported deployment.
- **Every test arm publishes its effective configuration per endpoint**: bundle, controller law, topology, scheduler, recovery owner, profile.

**Deleted at stage 11.**

- `congestion`: `ErasureSender.share`, the cap in `deliveredRate`, `seedBandwidth`, the seeding in `NewErasureSenderOn`, per-connection ownership of the pacer.
- `pep`: reactive isolation in `manageQUICLanes`; the bulk pool (`bulkConn`, `reserveBulkConn`, `dialBulkConn`, `maxBulkConnections`, `quicPoolActive`); `suspectDataLanes`, `preferUnsuspectedLanes`, per-flow QUIC rescue dials; `escapePendingCoding`'s frontier; `poolLiveness`'s counter heuristic.
- `pathmodel`: members, reports, `Share`, `Seed`, the aggregate window, and the share/seed combination in `tree.go`. Knowledge and its hierarchical combination stay.
- Branches #146 and #161 are not merged.
- **Condition.** If the DC gate fails, the legacy bundle and everything it uses stay, and only code no bundle reaches is deleted. `--congestion reno` then stays in the legacy bundle; if the gate passes it is removed with it.
- **Kept for old peers:** gateway admission of isolation and rescue JOINs; wires 1 and 2.

## 15. Validation

### 15.1 Correctness gates (every stage, wires 1 and 2)

Existing tests that pin behaviour this design depends on:

- Coded tail: `TestCodedSendStallRecoversOnExistingStream`, `TestCodedRecoveryUsesValidatedSelectiveCoverage`, `TestCodedRecoveryCannotRaceAnAcknowledgement`, `TestSmallExchangesAreRepairedOnceThePathIsKnown`. `TestCodedRecoveryEndsAtTheStalledByteFrontier` inspects the frontier; stage 4 kept the frontier beside the sticky bit and left the test unchanged, and it is rewritten against the sticky bit when the frontier is deleted (stage 11).
- Credit and progress: `TestLaneReaderReachesACKBehindFullDataQueue`, `TestRepeatedSACKDoesNotPretendProgress`, `TestSACKCannotAcknowledgeUnsentBytes`, `TestPendingWorkHasOneRecoveryBudget`, `TestIdleConnectionHasNoProgressDeadline`, `TestApplicationBackpressureIsBoundedAndResumes`, `TestDuplicateDataReacknowledgesLostACK`, `TestRecoveryLaneReceivesCurrentACK`, `TestACKProgressBypassesBlockedApplicationDelivery`, `TestReceiveBudgetIncludesBlockedApplicationWrite`, `TestMemoryLimitsRejectDeadlockingOrMultiplicativeSettings`, `TestQUICWindowsCanBeHardCappedForMobile`.
- Carrier and JOIN: `TestPooledFlowsRecoverThroughOneReplacementGeneration`, `TestResumeReplacesPoolsBeforeConcurrentBorrowersReuseThem`, `TestARequestAfterThePooledConnectionDiesDoesNotWaitForTheIdleTimeout`, `TestAHealthyIdlePoolIsNotSuperseded`, `TestRejectedJoinPreservesExistingLanes`, `TestJoinDuringDestinationDialWaitsForTheSession`, `TestOneOutageSpendsOneGraceHoweverManyWaitersItHas`, `TestAcknowledgedRequestWaitingForResponseDoesNotRescue`, `TestAcknowledgedApplicationMessageDoesNotTriggerRescue`.
- TCP: `TestSingleTCPLaneEOFWhileIdleRecoversEndToEnd` and its three siblings, `TestTCPStallRescuesDoNotSpendZeroLaneRecovery`, `TestTCPZeroLaneRecoveryExhaustionAbandonsTheFlow`, `TestAbandonedReplacementDoesNotKillAFlowRescuedOntoTCP`, `TestAutoRescueDoesNotRaceTCPHandoffWithQUIC`, `TestPooledAutoFlowCommitsToTCPWhenQUICGenerationCannotRecover`.
- Wire: `TestWire1ClientAgainstTheDualGateway`, `TestStallRescueAcrossWireVersions`, `TestAJoinOfTheOtherVersionIsRefusedAndTheFlowSurvives`, `TestPinnedFlowDialsOnlyItsVersion`, the conformance vectors, and the released-binary interop gate against real supported release pairs.
- UDP: `TestALostUDPPacketDoesNotHoldUpTheOneBehindIt`, `TestAPacketTakesTheDatagramSubstrateWhateverTheDataDoes`, `TestARescuedUDPAssociationKeepsItsRemoteSourceAddress`, `TestUDPAssociationRescuesToTCP`.
- Control law: `internal/congestion` `erasure_test.go`, `delaybound_test.go`, `policersampler_test.go`, `bandwidthdecay_test.go`; `TestCase4APolicedPathIsStillUnbraked` (must not get worse).

Tests of mechanisms that stage 8 disables get behaviour-equivalent replacements (`TestStallWatchdogCountsTheWarmSpare`, `TestSuspectedLaneIsDemotedNotKilled`, `TestACKAvoidsSuspectedLaneWhenFreshLaneExists`, `TestParallelRescueFirstWinsAndLosersAreCancelled`); the originals keep running against the legacy bundle while it exists.

New deterministic tests, on real fork send loops over an impaired in-process `PacketConn` (`internal/pathsim`):

1. *Shared admission.* k = 1, 2, 4 attachments: the three bounds of §4.3, each measured in its own domain.
2. *Served bytes.* A saturated datagram FIFO against a saturated bulk stream, on one connection and across two: each class's served bytes match its weight; at a rate so low that control demand exceeds its share, BULK is still served; two attachments granted the last control quantum together overdraw it by at most one packet and the overdraft is carried; nothing but the turn's class in a packet whose permit has an empty `Fill`; with 9:1 weights, 900-byte datagrams and a saturated bulk stream, served bytes stay within one packet per round of the weights; a datagram over its `Fill` allowance stays queued. *Normalisation:* demand disappears between permit and build in the only class with a positive deficit, leaving a sole indebted class with demand; it is selected at the next admission with no further packet or timer needed, and likewise for an idle class that becomes eligible after the last settlement.
3. *Exempt traffic.* PTO storms, path probes, an ACK packet that gains a PING: debited when eliciting, never gated, reported. A quiet attachment beside a sibling that fills the window still sends its liveness PING. The verification exchange completes with a full window and a negative balance at both ends, repeated attachment does not renew the budget, unrelated first frames cannot spend a grant, two attachments cannot hold one grant, and a lost first verification packet is retried under it. A builder delayed across a grant's expiry and across a reissue charges nothing to the old grant after the new bind returns; a controller replacement clears the grant; a FIN-only frame and bytes outside the range are never carried under it.
4. *Flight, fates and order.* Key drop, PTO-probe declaration, Retry, migration with both addresses, abrupt close, demotion and return: each gauge equals its handler's `bytesInFlight`; every record ends in exactly one fate; pre-send flight is never negative; alternating losses delivered as an all-delivered batch and an all-lost batch from two attachments give the same burst statistic as in-order delivery; an ACK covering only a previously lost packet produces a spurious correction; an MTU probe's loss does not enter the estimator; a local-address migration is a path change. *Delivered bytes under cross-attachment reordering:* attachment A registers early, B registers many later packets and B's acknowledgement arrives first; the delivery sampler's total delivered bytes equals the sum of acknowledged sample bytes, so no watermark has discarded A's state.
5. *Reverse ACK channel.* A download with heavy reverse QUIC ACK traffic: sender-side credit delay p99 below one RTT plus 10 ms; no non-eliciting byte debited.
6. *Control latency.* A control frame queued while its flow's DATA write holds `writeMu`; one whose tail was buffered in `nextFrame`; a control frame that starts waiting at each point of the DATA writer's path, including after its last check and before its bytes reach the stream; two waiters of which one cancels; many flows boosted at once stay within CONTROL's share.
7. *Demand and application-limited.* A continuously backlogged writer that is never refused is not application-limited; a writer whose queue is empty only between writes is not; one blocked on peer flow control or a full send queue is. A due MAX_DATA with nothing else queued is CONTROL demand and is served against a busy sibling.
8. *No remedy on a live stream.* Delay every flow ACK by 5 s; separately, block the receiving application: zero dials, zero stream replacements, zero reissued stream bytes.
9. *Binding lost.* Reset one flow's stream, and separately expire its ACK write, while siblings continue: one JOIN on the same carrier, no dial, siblings undisturbed; with an unconfirmed OPEN the failed-lane rule applies. Reset a stream while the carrier is SUSPECT, then let the carrier recover, and separately let it be promoted: the flow is rebound in both orders, on both endpoints' view. A late `BindingLost` for an old binding changes nothing.
10. *Carrier states.* Blackhole each direction and both: SUSPECT, standby, promotion; recovery before promotion returns to ACTIVE; a late ACK on a DRAINING carrier changes nothing; the standby verifies in both directions while the dead carrier holds a full window at both ends; promotion is single-flight.
11. *Promotion at scale.* 1,024 flows, delayed credit, a short outage: every flow rebinds or ends on its own no-progress budget; none fails for having waited in the JOIN queue; a new OPEN arriving mid-rebind does not take a slot a waiting flow needs; with fewer granted streams than flows the remainder take TCP before the gateway's grace, or in QUIC-only mode end at their binding deadline; an idle flow has a binding deadline; stream credit arriving just before and just after the gateway's expiry gives the documented outcome on a released and on a new gateway; the client's and the gateway's failure-observation times are varied independently up to `S` and the outcome is judged by when the JOIN, QUIC or TCP, *arrives* at the gateway, not when stream credit arrives; an OPEN_OK write failure injected on a new gateway under `gateway-admission=owned` while the old control lane is healthy, including at the lane ceiling and with `REPLACE_LANE`, leaves the old lane carrying the flow; after OPEN_OK the old stream is closed and a released gateway stops scheduling on it while a second flow keeps the old connection open; a carrier that flaps between SUSPECT and ACTIVE is rate-limited by the attempt history; replay is held for the first ACK or one SRTT, is paced by the path, and is bounded by retained bytes. The same against a peer that does not emit G5 at zero.
12. *Gateway capacity.* All sessions occupied, idle and active: a standby's PROBE and the flows' JOINs are admitted; a new OPEN is refused. Against a released gateway the client falls back to break-before-make, and with `MaxSessions=1` and one confirmed flow the documented unrecoverable case is observed, not hidden. The same accounting is exercised through the TLS/TCP entry point.
13. *Wire stickiness.* A gateway upgraded mid-flow: the replacement is wire 1, new flows use it, and the next carrier after the path empties is wire 2. On iOS's two slots a standby is always available.
14. *Memory.* Hundreds of idle sockets plus one active request under the iOS profile: the request is read and sent, including EOF discovery. A source without read deadlines publishes short chunks and never blocks holding a reservation. READY bytes with no binding end with the flow at the replacement deadline. The retention bound does not grow while flow ACKs are withheld. Producers blocked outside the encoder queue are inside its byte bound. Peak memory during a replacement stays within the profile.
15. *Coded attempts.* A frame queued but never emitted is retired and retried on the stream; a started frame is never cancelled and ends in exactly one outcome, registered or (on discard or close) abandoned, while the flow's no-progress deadline is tested separately; a datagram waiting a full pacing interval in the FIFO does not expire early; a late callback for a retired attempt is ignored; a lost tail with no further arrival is recovered on a gateway; the pre-OPEN_OK safety copy is still sent.
16. *UDP.* Overflow drops whole PACKET frames only; a replacement that reclaimed a relay and whose OPEN_OK was lost can be retried with the old token until the client sends on it, and a claim under either token consumes both; the source address is kept only when a relay was actually reclaimed; the stream override and no-DATAGRAM mode still work; with the single close-then-resume ordering, a rebind keeps the relay's source address whenever the relay is reclaimed: in owned mode whether or not the close arrived, and in the default mode or against a released gateway only when the park was published before the claim (tested with a barrier between EOF and publication); a fence timeout leads to a capacity-checked fresh OPEN with no address guarantee.
16a. *Datagram tag lifecycle.* Every accepted tag gets exactly one outcome when it is registered, dropped by the packer, found queued at close, or popped for a build that fails before registration; a tagged send racing close is either refused or reported, never both and never neither.
17. *Mixed peers, migration, enrollment.* Old-client behaviour against the new gateway and the reverse, for FIN, abort, JOIN and TCP handoff; a NAT rebinding keeps the authority; an enrollment connection never attaches.
18. *Bundles.* Each supported bundle starts and passes the end-to-end suite; each unsupported combination is rejected; the healthy-idle keepalive overhead matches the schedule.
19. *Adapter equivalence (stage 1).* One packet-event trace drives the legacy callbacks and the `PathSender` adapter around the same controller; exported telemetry, flight, window admission and pacing decisions are compared at each step and differ only where non-eliciting packets are no longer debited (that counter is excluded). Migration and exempt sends, the other two activated changes, have their own tests. Main converts post-send flight to pre-send inside TUIC and debits its inner pacer there (`bbr_tuic.go:381`, `erasure.go:516`), so an adapter can subtract or debit twice while producing plausible telemetry; this test is what catches it (it caught an attach-time packet-size reset in stage 1).

### 15.2 Simulator

`joinburst_sim_test.go` (on #161) drives real estimators over a simulated FIFO and does not run send loops, streams or JOINs. Its scenarios stay as regression inputs for the control law. Deterministic tests gate invariants; the live gate decides comparative performance.

### 15.3 Live A/B

- **Unit.** One *block* is four trials on one path, in ABBA or BAAB order chosen at random, with both endpoints restarted before the block so no knowledge (`knowledgeLifetime`) carries between blocks, and with cold and warm starts balanced across arms. Blocks are the unit of analysis.
- **Outcome per arm per block.** Useful bytes delivered within a fixed duration, summed over that arm's two trials and all their flows. A failed flow contributes what it delivered; a total outage contributes zero. A scenario with no bulk component has no primary outcome and is judged on its secondaries alone (§15.4).
- **Minimal live plan (owner's decision).** Live runs total about 2.5–3 h per path across all gates. Four trial types and the DC gate are the gates; every other entry of `docs/WORKLOAD-MODEL.md` is covered inside them, by a deterministic or emulated test in CI, or by an optional run. The full workload matrix stays as an opt-in reference. Generators, metrics and provisional bounds are the model's.

| Trial | Gate | Content | Length |
| --- | --- | --- | --- |
| **A `bulk`** | stages 6/7 | 4 bulk downloads | 60 s |
| **B `mixed-6/7`** | stages 6/7 | a 5-turn LLM loop (100 KB prompt) and terminal keystrokes with one 300 KB burst, under 2 bulk downloads | 90 s |
| **DC** | stage 9 | the §11.3 gate at minimal size: 10 transactional requests (ASR and TTS alternating, the first on a cold app connection), then the sustained-goodput and multi-leaf fairness workload | 60 s |
| **C `mixed-all`** | stage 10 | concurrently: a video-call UDP stream, one browsing burst, one LLM call, terminal keystrokes, 1 bulk download and 1 bulk upload; metrics per class and per direction | 120 s |
| **D `idle-wake`** | stage 10 (and `classifier=v2`) | 50 idle connections per arm after a 200 KB sync, each woken once at 1, 5 or 16 min (crossing the 15-minute boundary), push and send alternating; run once per path, beside C (beside A and B for `classifier=v2`) | about 17 min |

- **One trial, every metric.** In B and C every workload's metrics come from the same trial as the bulk primary, so one block yields all of them. The model's Minimal live plan fixes each trial's generator parameters and which metrics each trial gates.
- **Primary per trial.** Only bulk flows count: B's downloads; C's download and upload, each its own primary; DC's sustained phase. A has nothing else.
- **Each trial type runs once per path, at its gate.** A pass carries forward to the flip; a trial is rerun on the final bundle only when a later stage changes what it exercises.
- **D** barely contends after its sync, so its arms run concurrently as paired sessions on their own client and gateway instances, which C's block restarts do not touch. Its syncs finish before C's first trial, and C's first two blocks run back to back to cover its 17 minutes, so its wakes happen under shared-link load (M6) at no extra wall time. Same-tunnel idle memory under load is test 14 of §15.1, not D.
- **Directions.** Upload traffic appears wherever a workload's reverse direction carries its acknowledgements or its request (B's LLM requests, C's bulk upload), because the two directions differ in capacity and loss and are controlled independently.
- **Coverage of the model's entries.**

| ID | Workload | Covered by |
| --- | --- | --- |
| W1 | Bulk download | A (four flows); C (one flow). Standalone one-flow and `speedtest` variants: optional |
| W2 | Bulk upload | C (desktop profile). iOS memory profile: optional |
| W3 | Browsing fan-out | C (one burst). The h1/h2 × cold/warm sweep: optional |
| W4 | Transactional burst | DC |
| W5 | LLM streaming call | B (loop; turn 1 opens a cold app connection); C (single call). Swarm: optional |
| W6 | Terminal keystrokes with output bursts | B (before and after the burst, latch ratio); C |
| W7 | Video call over UDP | C |
| W8 | Tunnelled QUIC | deterministic test, emulated path |
| W9a | Short-idle wake | D |
| W9b | Long-idle survival (29/31/45/60 min) | deterministic test, injected clock |
| W10 | Segment fetch | deterministic test, emulated path |
| W11 | One-shot datagram | deterministic test, emulated path |
| M1 | LLM call against bulk download | B |
| M2 | LLM call against bulk upload | C |
| M3 | Agent swarm | optional |
| M4 | Call + browsing + bulk | C |
| M5 | Heavy and thin UDP together | deterministic test, emulated path (with W8) |
| M6 | Idle wake under load | D beside C; idle-socket memory on the iOS profile: test 14 of §15.1 |
| M7 | Day-in-the-life soak | optional |

- **Deterministic replacements** run in CI on `internal/pathsim`; the model's minimal live plan names them (still to be written). They gate stage 10 together with the live trials. Optional runs are reported, never gated.
- **Replaces the echo probe.** The interactive echo probe of earlier campaigns is B's keystrokes before the burst, under bulk downloads.
- **Classifier first.** The workload model and `docs/AUDIT-CLASSIFIER-WORKLOADS.md` find that today's classifier mislabels W3-h2, W5, W6 and W9 (F1–F3). Its fixes R1–R3 land first and are on in both arms of the stage 6/7 campaigns, or those gates measure the label instead of the change. The switch that carries them is itself gated on A, B and D.
- **Product-configuration screen (after stage 2).** {isolated, single connection} × {round-robin, class scheduler} under today's controllers, plus one stream against several on one connection at a fixed connection window. This chooses a configuration; because isolation also changes the number of independent controllers, it does not attribute a result to topology.
- **Topology under one authority (stage 7's gate).** `topology=single` against isolation with `congestion=path` in both arms. This is the comparison that tests §7. A result against the single connection reopens it.
- **Credit delay** is measured on the sender's clock alone: time from a chunk's last byte being taken by the carrier to its covering ACK, minus the path's minimum RTT.

### 15.4 Statistical gate

- **Primary test.** For each block `b`, the contrast `d_b = candidate_b − 0.90 × main_b` in delivered bytes. The gate passes on a path when the lower bound of the one-sided confidence interval of the mean contrast, at the level of the current look (below; 95 % overall), is above zero. The contrast is defined for zero outcomes in either arm and needs no ratio of a resampled denominator. A campaign in which more than a pre-registered fraction of main's blocks are zero is void and is rerun. "Point estimate ≥ 0.95 and the interval includes 1" is not accepted.
- **Sequential stopping.** Arms are interleaved in ABBA or BAAB blocks, and the trial types round-robin at block granularity so path drift hits all of them equally. Each trial type starts with 6 blocks per path. After block 6 and after each further block the one-sided tests are re-run, at a nominal level per look from an α-spending function (Lan–DeMets, Pocock type) over the 7 possible looks (6 to 12 blocks) with total one-sided α = 0.05, so the repeated looks do not inflate the false-pass rate; the levels are computed once and frozen before the first block, and Bonferroni (0.05/7 per look) is the conservative fallback. At a look the trial type
  - **passes** when the lower bound of the primary contrast and of every secondary contrast is above zero and every hard assertion holds;
  - **fails** when a hard assertion fails, or when the upper bound of the primary or of any secondary contrast is below zero;
  - otherwise gets one more block. At the 12-block cap a trial type that has not passed fails.

  Each trial type stops on its own; the others continue. The interval is a one-sided t-interval on the block contrasts (bootstrap as a check). No separate pilot is run: the model's provisional bounds (margins `m`, slacks `a`) and the candidate's configuration, including the Q1 weights, are frozen before the first gated block; changing either after any block has been seen voids the campaign.
- **Control.** "main" in these contrasts is the comparison's registered control: main for `classifier=v2`; main with `classifier=v2` for stage 6; isolation with `congestion=path` for stage 7 (§15.3); the legacy bundle for DC (§11.3); main for the flip.
- **Initial state per arm.** Each arm runs as its own processes with its own path knowledge. An arm's first trial in a block starts cold (processes restarted) and its second starts from that arm's own first trial, in both ABBA and BAAB, so neither arm inherits the other's state by position.
- **Secondary, all required, each with a pre-registered one-sided bound:** flow completion; stalls per trial; reissued bytes per source byte; wire bytes per useful byte against main's measured baseline by substrate and direction; sender-side credit delay p95; client CPU p90; shadow false-promotion rate.
- **Workload secondaries.** Each metric the model gates for a workload inside a trial (§15.3) is a required secondary of that trial, per path, with its own pre-registered one-sided bound against main in the same campaign; a metric the model marks as reported is not gated. For a metric where smaller is better the contrast is `d_b = (m × main_b + a) − candidate_b`, with a multiplicative margin `m` and an absolute slack `a` fixed per metric before the first gated block, so that a metric near zero on main is not failed by noise. Percentiles are computed over each arm's pooled samples in a block. A tail percentile is gated per block only when the block holds at least 20 samples per arm for a p95 and 100 for a p99; otherwise it is computed over all blocks' pooled samples at the stopping look and tested with a bootstrap that resamples blocks (this is how DC's p99 latency is tested). Generic secondaries apply where defined; a rate with a zero denominator is reported, not gated. Hard assertions in the model (class correctness, delivery, no silent deaths) and absolute ceilings (W6's latch ratio ≤ 1.15) are pass/fail, not statistical; absolute targets (W9a's RTT targets) are reported. Survival counts are reported with exact binomial intervals.
- **D (`idle-wake`)** is one paired session per path, not a block sequence. It passes when its hard assertions hold (no silent deaths; every wake at 1 and 5 min delivered within 5 s), its delivered count at 16 min is at least main's, and its wake-penalty p50 is within the model's W9a bound of main's from the same session; the p95, max and death modes are reported. Generic secondaries do not apply to D.
- **Pass.** A path × trial type passes under the rule above. Stage 10 requires A, B, C, D and DC on every path, and the deterministic replacements in CI (§14).
- **Budget.** One block each of A, B, C and DC takes about 26 min with restarts, so 6–7 blocks of each is 2.5–3 h per path when each trial type runs once; the 12-block cap bounds that at about 5 h. Reruns on the final bundle add their own cost.

## 16. Risks and open questions

**Risks.**

1. *One estimate, one mistake.* Mitigation: the control law is the one already validated per connection; gates are live and per stage.
2. *One connection may not match four uncapped probers on the Beijing path.* Stage 7's gate answers this under one authority.
3. *Fate sharing.* One carrier's death stalls every flow until promotion, as the pooled control connection's death already stalls every flow's control plane.
4. *Fork surface.* Fourteen changes in a QUIC stack's send path (1–12, 9a and 13), staged under the unchanged controller first. Stage 1 confirmed the cost: an independent review of the default path found no P0/P1 for a connection that never installs a `PathSender`, but did find always-on bookkeeping and one observable difference during a live controller swap: before stage 1 the handler re-read the current controller for the aggregate congestion callback, while stage 1 kept the recipient it started the ACK with, so a replaced controller could receive that event. The stage-2 fork change (in review) suppresses that event when its recipient has been replaced, and also takes a spurious loss back out of `PacketsLost`/`BytesLost` (#176).
5. *Slower detection of a true blackhole than today's 3×minRTT flow timer*, accepted in exchange for removing false rescues and calibrated in shadow mode.
6. *The policed path is still unbraked.* Unchanged; one compensation loop instead of N.
7. *Arbitration cost.* One mutex acquisition per packet per attachment; measured on the 1-vCPU client at stage 2.
8. *Released gateways* give break-before-make recovery, best-effort UDP resume, and no recovery at all when their session limit is saturated, until they are upgraded; gateways are upgraded first.
9. *The class is only as good as the classifier.* Under `sched=class` a flow's stream DATA is BULK exactly when its flow is classified BULK (coded DATA and UDP are INTERACTIVE whatever the flow's class), and today's classifier latches BULK for life and reads a stale window after idle, so LLM responses, SSH after output, HTTP/2 after a page and messaging after sync are served as bulk; heavy UDP is INTERACTIVE (Q5). `docs/AUDIT-CLASSIFIER-WORKLOADS.md` gives the findings (F1–F13), the fixes (R1–R9) and which gate each blocks.

**Open questions.**

- **Q1. Weights and allowances** (`w_C : w_I : w_B`, `D`, `V`, the probe window). Latency policy, set by tests 2 and 6, trials B and C, and the emulated W8 and M5 tests (§15.3).
- **Q2. Joint limiting for the hierarchy** (§11.3), needed only if the DC gate fails.
- **Q3. Fan-out trigger** (§7): which measurement identifies the single-connection ceiling.
- **Q4. `k_s`, `k_p` and the keepalive floor** (§10.2), from shadow-mode data.
- **Q5. Whether heavy UDP should be charged as BULK** (§9). Doing so needs byte attribution through an ordered symbol stream without inverting priority inside it.
- **Q6. Whether an address-level erasure prior should seed FEC for a new principal behind a known address.** Not included.

## Appendix A. Review log

**Round 1 → v2.**

| Finding | Resolution |
| --- | --- |
| P0-1 idle readers hold the endpoint budget | Staging outside the shared budget; fair grants (§8.3) |
| P0-2 transport delivery cannot diagnose a peer-flow fault | No remedy for missing credit on a live stream; backpressure explicit (G7, §10.4) |
| P0-3 quiesce strands the standby or resurrects flight | COUNTED/PROBING states with epochs (§4.4) |
| P0-4 replacement and JOIN availability | Client-initiated; `RESERVE_CONTROL` rebind; TCP handoff kept (§10) |
| P0-5 wire-3 ACK datagrams deadlock | Datagram is a redundant copy; wire 3 out of the migration (§12) |
| P1-1…10 | Authority-owned arbitration; byte bounds; class on stream byte ranges; packet fates and discards; demand; promotion single-flight; calibrated PTO thresholds; coded attempt boundary; provisional controller and key; hierarchy deferred |
| P2-1…6, P3 | Retention autotune; FEC source rate; UDP modes; experiment design; dependent switches; block unit; citations |

**Round 2 → v3.**

| Finding | Resolution |
| --- | --- |
| P0-1 a failed stream on a healthy carrier has no owner | `BindingLost`; same-carrier JOIN owned by the carrier manager (§10.4); test 9 |
| P0-2 a full gateway rejects recovery work | Sessions charged at OPEN; JOIN, PROBE and standby admitted separately (§10.5); degraded path for released gateways (§10.2); test 12 |
| P0-3 two pinned mobile carriers leave no standby slot | One wire per path at a time (§7); iOS cap is ACTIVE + STANDBY (§11.5); test 13 |
| P1-1 the permit does not encode the service decision | `Permit{Turn, …}`; a turn's packet starts with its class; byte-accurate settlement (§4.3); test 2 |
| P1-2 recovery before and after promotion, both endpoints | DRAINING irreversible with generations (§4.4, §10.2); verification budget for bidirectional verification (§4.3); each endpoint demotes on its own evidence (§10.1); JOIN concurrency and separate deadlines (§10.3) |
| P1-3 byte bound domain | Three bounds stated separately; no asymptotic claim; telemetry (§4.3) |
| P1-4 demand and send opportunity | Non-consuming eligibility query including window updates and retransmissions; application-limited as unused budget (§4.2, fork change 3) |
| P1-5 priority inheritance starts too late | Control intent at queue time; `Boost`; boosted bytes charged to CONTROL (§5, §6) |
| P1-6 excluded flight, migration, reordered outcomes | Sample eligibility rule; pre-update flight sum; migration addresses; spurious-only ACKs; neighbour-transition burst statistic (§4.2, §5) |
| P1-7 coded deadline one queue early | Datagram tags and outcomes; deadline at last-source-symbol registration; cancellation rules (§5, §8.1) |
| P1-8 READY bytes have no owner | Reservation owner table; READY counts as pending work; no blocking second read (§8.3) |
| P1-9 UDP promotion races relay retention | Active-to-replacement transfer on new gateways; best effort on released ones (§11.2) |
| P2-1 reconciliation best effort | Replay gated on the new binding with timeout; G5 at zero on new builds (§8.2, §10.3) |
| P2-2 retention clock and quantity | Two gauges, one update per credit round, growth only on evidence (§8.3) |
| P2-3 bulk UDP weight | Stated as intentional; Q5 (§9) |
| P2-4 keepalive owner and reset | The fork's timer; reset only on payload (fork change 11, §10.2) |
| P2-5 configuration matrix | Bundles, legacy bundle, DC gate, conditional deletion, hosted laws (§4.1, §11.3, §14) |
| P2-6 zero outcomes, confounding | Ratio of totals; randomised order; topology tested under one authority (§15.3–15.4) |
| P3 | Citations corrected; artifact location stated |

**Round 3 → v4.**

| Finding | Resolution |
| --- | --- |
| P0-1 a stream lost during SUSPECT stays unbound after recovery | Persistent *unbound* state, reconciliation on every eligibility change, binding identity on events (§10.3, §10.4); test 9 |
| P0-2 new OPENs take the stream slots established flows need | Stream slots reserved for flows waiting to rebind; shortfall path; one deadline rule (§10.3); test 11 |
| P1-1 verification allowance; ordinary bound | Path-owned verification budget `V` exempt from window and balance; bound restated as an increment at admission (§4.3) |
| P1-2 gateway cannot observe the client's commit | Irreversible DRAINING is client-local; gateway rule local and reversible, with the overhang argument (§4.4) |
| P1-3 settlement-only grants overspend strict priority; control can starve bulk at low rate | CONTROL is a class in one deficit round robin; overdraft carried; no per-RTT allowance (§4.3); test 2 |
| P1-4 exempt packets lack an exempt send path | Fork change 9a; MTU probes become ordinary; PTO payload settled by class (§5) |
| P1-5 boost must be an intent through the frame boundary | Writer-owned waiter count read before each stream write; per-waiter release (§6); test 6 |
| P1-6 packet kinds, spurious horizon, datagram adjacency | `Kind` on `SentPacket`; corrections limited to the fork's horizon, stated; adjacency rule (§4.2, §5) |
| P1-7 UDP ordering needs an unadvertised capability | One close-then-resume ordering for every gateway; new gateway also accepts a live token, with fencing and slot transfer (§11.2) |
| P1-8 released gateway at saturation | Stated as unrecoverable against released gateways; headroom required; TLS/TCP entry point included (§10.2, §10.5) |
| P2-1…6 | Counterfactual-admission definition of application-limited (§4.2); retention growth requires coverage (§8.3); encoder capacity before copy (§9); local migration (§5); present versus activated, test-only knob, DC thresholds (§11.3, §14); block-contrast test from a pilot (§15.4) |
| P3 | Citations corrected; G5 and `RESERVE_CONTROL` claims qualified (§7, §10.3) |

**Round 4 → v5.** Reviewed in round 5; dispositions in the next table.

| Finding | Resolution in v5 |
| --- | --- |
| P0-1 a verified carrier does not end the gateway's outage for a flow still waiting for stream credit | Per-flow binding deadline tied to the gateway's grace, for idle flows too; earliest-deadline-first JOIN queue; early TCP handoff; QUIC-only failure boundary stated; DRAINING bounded (§10.3 step 4) |
| P1-1 packet fill makes class weights unattainable | `Permit.Fill`: leftover space only for classes with positive deficit, up to that deficit, else a short packet; quanta repeated until a class is positive; service error stated (§4.3) |
| P1-2 `V` is not reserved for verification | Single-holder, expiring verification grant covering a byte range (§4.3) |
| P1-3 `Boost()` can miss the DATA write | Boost is a persistent stream property applying to bytes accepted later, driven by the writer's waiter count (§5, §6) |
| P1-4 `RESERVE_CONTROL` does not retire the old gateway lane | Client closes the flow's old stream on OPEN_OK; a new gateway retires the previous control lane on admission (§10.3 step 1) |
| P1-5 the DC gate omits the hierarchy's fairness benefit | Worst-leaf and spread criterion added to the gate (§11.3) |
| P2-1…6 | Park-before-claim condition for released UDP resume (§11.2); no upward retention move without coverage (§8.3); encoder admission before serialisation (§9); adjacency defined on the eligible registration sequence (§4.2); every dial round charged to one attempt history (§10.2); power for the stated alternative, t-interval, per-arm state (§15.4) |
| P3 | ACK+PING kind; ring eviction units; balance evolution during idle |

**Round 5 → v6.** Verdict: build with listed changes; no new P0.

| Finding | Round-5 disposition | Resolution in v6 |
| --- | --- | --- |
| R4 P0-1 verified carrier, flow still waiting for stream credit | Partial: the indefinite wait is fixed; the gateway deadline ignores detection skew | Latest handoff start derived from the gateway's grace less the skew allowance `S` and a configured budget `J` for the whole interval from handoff start to the gateway parsing the JOIN; deadline judged on JOIN arrival; best effort stated when skew or transit is unbounded (§10.3 step 4); test 11 varies both observation times |
| R4 P1-1 packet fill defeats class weights | Resolved | — |
| R4 P1-2 `V` is not reserved for verification | Ownership resolved; fork contract partial | Grant bound to a stream byte range by a local fork call, with expiry, budget, retransmission and settlement by ID; bound before the blocking write; reissue effective at bind; replacement clears (§4.3, §5 change 13); test 3. Range binding and settlement built in stage 1; the reissue and replacement rules are #176, fixed in bojieli/quic-go#3, in review (Appendix B) |
| R4 P1-3 boost misses a DATA write | Resolved; serialise transitions with the waiter count | Transitions under the count's lock, cancellation included (§6) |
| R4 P1-4 `RESERVE_CONTROL` leaves the old gateway lane | Resolved for a successful rebind | See new P1-1 |
| R4 P1-5 DC gate ignores hierarchy fairness | Resolved at design level | Bounds frozen before the run (§11.3, unchanged) |
| New P1-1 gateway role transfer can destroy the last usable lane | — | Admission stages the candidate and its retirements; commit only after OPEN_OK is written and the lane activated; rollback removes the staged lane, also at the lane ceiling and with `REPLACE_LANE` (§10.3 step 1); test 11. Built in stage 5 |
| New P1-2 eligible work and no selectable class | — | Normalisation under the controller lock before every selection and on demand transitions (§4.3); test 2 |
| New P1-3 tagged datagram acceptance races the final discard | — | Exactly-once outcome for every accepted tag, acceptance and close atomic, popped-but-unregistered reported; a datagram over its `Fill` stays queued (§4.3, §5 change 12); test 16a. Built in stage 1, the `Fill` rule in stage 2 |
| New P1-4 TUIC sampler watermark erases another attachment's samples | — | Sampler state retired by individual fate or ring expiry, no cross-attachment watermark (§4.2); test 4 asserts delivered bytes; stage 6 |
| Stage 0–1 guidance | — | Progress export maintained for every connection from stage 1 so stage 0 can use it; stage 0 baselines separated from the post-stage-2 scheduler arm (§14); adapter differential test added (test 19), built in stage 1 |

**Positions the reviewer asked to have recorded, and where this design stands.**

- *Verification and scheduling need different grant semantics.* Accepted. Ordinary admission stays settlement-based with bounded error; the verification exchange gets an exclusive, expiring grant; packet fill is limited to entitled bytes.
- *Carrier verification cannot settle an individual flow's recovery lifetime on wires 1 and 2.* Accepted. The per-flow binding deadline is the gateway's grace, and the QUIC-only failure boundary is stated instead of hidden. Removing it needs a wire change (a batch JOIN or a path-level resume on wire 3).
- *Aggregate throughput and request latency are insufficient grounds to delete hierarchical limiting.* Accepted. The DC gate now carries a fairness criterion, and failing it keeps the legacy profile.
- No disagreement remains on the central decisions: one authority per path and direction, one carrier by default, flow-level delivery credit, client-owned replacement, wire 3 outside this migration.

## Appendix B. Implementation notes

Each entry records where an implementation diverged from, corrected or made concrete what an earlier revision said. The rule as built is now stated in the section named. Open follow-ups are listed with their issue; none of them is a P0 or P1 on a default path.

**Stage 1, `cc-api=v2`: #175, with the fork API in bojieli/quic-go#2.**

| Topic | Earlier text | As built, and why | Section |
| --- | --- | --- | --- |
| Verification grant | A grant "covers a byte range"; no operation named it | `BindVerificationGrant(stream, {ID, Start, End, Budget, Expiry})` with `AcceptedOffset` to name the range; new data and in-range retransmissions, straddling frames split, FIN-only never; packets reported as kind VERIFICATION with the grant ID; consulted only while `Admit` refuses; bound before the blocking write, since the write may need the grant | §4.3, §5 change 13 |
| Grant reissue | "One grant at a time" | As merged, a reissue took effect at the next run-loop pass and a controller swap kept the old grant. The rule is now "effective at bind; replacement clears"; fixed in bojieli/quic-go#3 (#176) | §4.3, §5 change 13 |
| Prior flight | Implicit | Events carry post-event flight only; the adapter takes prior flight from its own gauge, exact because every change is reported; gauge = `bytesInFlight` is asserted after every event | §4.2 |
| Late acknowledgements | "Within the fork's lost-packet tracker" | A bounded late-acknowledgement history (256 records, three-PTO horizon, binary search) of packets declared lost or retired for a PTO probe or by migration; a late ACK is transport progress, and a `Spurious` correction is produced only for a loss reported to the still-installed controller; application space only. The default path's cost was reviewed separately and found to be bookkeeping only | §4.2, §5 changes 6 and 10 |
| Progress export | A stage-8 input | Maintained for every connection whatever its controller, so stage 0 can use it | §5 change 10, §14 |
| Permit execution | Builder honours `Turn`/`Fill` | Not until stage 2: a stage-1 controller must admit whole packets of any class; a permit covers only the application packet of a coalesced datagram | §5 change 2 |
| Class bytes before tags | Unspecified | By frame type: STREAM BULK, DATAGRAM INTERACTIVE, ACK/PING/path EXEMPT, the rest CONTROL | §5 change 9 |
| Path probes and I2 | Every eliciting packet is debited | Stage 1 leaves out-of-flight path probes undebited, as before, so that exactly three behaviours change; in-flight path validation is debited like any packet. The stage-6 controller debits both | §4.3, §14 |
| Migration | Keep the controller | Kept; for a `PathSender` the packet size is reset to the initial size; queqiao re-creates the controller only when the path key changes, compared against the key it was chosen for. Open: other readers of the path model (coded sizing, control generation) still hold the old model after a key change (stage 6); a multihomed gateway's callback reports the old local address | §5 change 8 |
| Demand | Non-consuming query | Stream demand counts only work sendable now; refreshed before each admission of a batch. An empty build could leave demand asserted (#176); refresh after empty builds is in bojieli/quic-go#3 | §4.2, §5 change 3 |
| Datagram tags | Registered or discarded | Exactly one outcome per accepted tag, including close while queued and a pop whose build failed; acceptance atomic with close | §5 change 12 |
| Validator | Bundles | `v2` only with `--congestion erasure`, refused under `dc-long-haul` | §14 |
| ACK channel | Hypothesis from the B1/B2 builds | Checked directly on real fork send loops: under v1 every client ACK-only byte is debited, under v2 none. The live evidence is the 2×2 and one-change builds on #161 | §1 |

**Stage 3, `mem=owned`: #172.**

| Topic | Earlier text | As built, and why | Section |
| --- | --- | --- | --- |
| Non-blocking fill | "A read deadline of now" | Go's poller fails a past deadline before reading, so the fill uses now + 1 ms, only after a full 1 KiB staging read, and only with a full-chunk allocation that is free at once | §8.3 |
| Default | All behind `mem=owned` | The producer fix for #169 is on by default wherever a shared send budget exists; it restores a claimed bound and is local | §8.3, §14 |
| Fair grants | Round-robin grants | Arrival order on the send budget only; receive admission never waits, so it has no queue | §8.3 |
| Retention autotune | "A few chunks"; "moves toward" | Startup allowance of four chunks; halfway steps toward `2 × max U`; round = slowest lane RTT + ACK coalescing until a path SRTT exists; lazy replay of elapsed rounds; retries charged once | §8.3 |
| Write-ahead | `Snapshot.Window` | Until stage 6, the sum of `windowBytes` over the lanes dispatch would use | §8.3 |
| Not yet covered | Test 14 | The encoder-queue byte bound belongs to the coded path; peak memory during replacement is bounded by the hard budget but not asserted | §15.1 |

**Stage 4, `coded-retry=sticky`: #177.**

| Topic | Earlier text | As built, and why | Section |
| --- | --- | --- | --- |
| Observation API | Tags per datagram | `coded.Path.SendObserved` reports each frame once, `Registered` with the packet time of its last source symbol, or `Abandoned`; only source symbols of observed frames are tagged | §8.1 |
| Cancellation | "Completes or abandons within a bound" | The path never cancels a started frame and has no per-frame timer. A frame stays the code's until registered or discarded (the connection's discard or close, which the fork reports); the flow's no-progress budget bounds the flow's wait. One outcome per accepted frame, none for a synchronous refusal | §8.1, decision 6 |
| Deadline clock | At registration | From the packet's registration time, captured in the fork's observer, not the callback's or the lock's | §8.1 |
| Watchdog frontier | Replaced by the sticky bit | Kept beside it; its test unchanged until the frontier is deleted | §8.1, §15.1 |
| Gap evidence | Unchanged | Retires only a registered attempt and only with evidence newer than registration. Open (#178): evidence is timestamped at scan end, not ACK arrival (one harmless early duplicate); gap retries are uncounted in one callback order; a wasted deadline lookup at dispatch | §8.1 |
| Outside the switch | — | A frame needing more than 65,535 fragments used to stop the send loop silently; it is now that frame's failure. Each coded path runs one extra goroutine to deliver outcomes | §8.1 |
| FEC source rate | "The same event feeds" it | Not in stage 4's row and not built; assigned to stage 6 with the removal of `Share` | §9, §14 |

**Stage 5, `gateway-admission=owned`: #173 (in review).**

| Topic | Earlier text | As built, and why | Section |
| --- | --- | --- | --- |
| Connection accounting | Per authorised principal, room for one standby | No per-principal limit: the general bound stays and one standby per device (principal plus device identity) may exceed it; the standby inherits the slot of the device's first connection to close; slots are released at connection end, not flow end | §10.5 |
| Default | All behind the switch | The #170 fix is on by default for the saturated case only (JOIN and PROBE read and admitted past a full limit, one standby); charging at OPEN and `FLOW_LIMIT` stay behind the switch | §10.5, §14 |
| Control transfer | Retire the holder on admission | Owned mode only. Explicit victims (lane-ceiling eviction, `REPLACE_LANE` target) are staged; after OPEN_OK is written and the lane activated they are retired, then current control holders that are suspected or past the eviction protection; a failed OPEN_OK rolls back (round-5 P1-1) | §10.3 step 1 |
| Tombstones | Unchanged | Owned mode: kept outside the session count with their own bound | §10.5, §8.4 |
| UDP relay transfer | Fence, join, publish, rotate | Owned mode. A relay reclaimed with the old token answers to both tokens until the client sends on the replacement; either claim consumes both; a fresh fallback relay gets no alias; unconfirmed fresh associations give their slots back; failed or expired leases are freed before a waiter is woken or an OPEN refused; a fence that does not finish in half the handshake deadline falls back to a fresh-open attempt, subject to capacity | §11.2 |
| Bounds | Small per-connection first-frame bound | Plus a gateway-wide bound on streams being read before admission, and a gateway-wide TLS/TCP bound over handshake and first-frame admission; at saturation a valid authenticated PROBE needs no existing binding | §10.5 |
| Grace timing | — | Unchanged: a JOIN extends the grace when parsed. Binding deadlines are stage 8 | §10.3 step 4 |

**Stage 2, `sched=class`: fork side in bojieli/quic-go#3 (in review).** The builder executes `Turn`, `Fill` and `MaxBytes` from handshake confirmation on; a permit below a minimum classed size is a window refusal so due ACKs keep the exempt path; a datagram over its `Fill` stays queued; a frame never mixes classes and a retransmission keeps its class unless the stream is boosted; the #176 grant and demand fixes; a spurious loss is taken back from the loss counters; the aggregate congestion event is suppressed when the controller that received the per-packet callbacks has since been replaced. The queqiao side (the arbiter, with idle credit dropped and debt kept) is implemented on a branch without a PR yet. Without a class-scheduling controller the builder's output is pinned byte-for-byte to the previous fork.

**Stage 7, `topology=single` (in review, stacked on stage 6).**

| Topic | Earlier text | As built, and why | Section |
| --- | --- | --- | --- |
| Switch | `topology=single` (6) | A client switch only (`--topology`); a gateway never isolates and already attaches an isolating client's connections to one controller (stage 6). It requires `congestion=path` and the pooled connection, without which every flow dials its own | §7, §14 |
| Fan-out | "Fan-out is a carrier-manager policy that stays off"; trigger is Q3 | Built as an explicit option only (`--topology-fanout=N`, off by default): the extra connections come from the bulk pool, capped at N−1 and by the profile's pool (one on iOS), and are triggered by isolation's own condition (a bulk flow sharing the preferred connection). No automatic trigger is built; the hook for one is the client's placement decision. Under `topology=single` a pool failure never falls back to a dedicated dial, which would escape the budget | §7, §16 Q3 |
| Connection receive window | "All flows share the connection window … Both limits exist today" (§11.5) | Not quite: under isolation each isolated bulk flow had its own connection window. With the desktop windows (64 MiB streams, 128 MiB auto-tuned connection) nothing binds. With fixed mobile windows the shared window could bind first (Android: eight bulk flows hold 4 MiB in flight on one connection against 6 MiB isolated), so under `topology=single` the preferred connection's ceiling grows by one stream window per secondary connection not opened (iOS 2 MiB → 2 MiB + 64 KiB, Android 4 → 6 MiB), never past the desktop ceiling. The growth is credit from one client-wide ledger holding at most the unopened connections' own windows (k × connection) across every live preferred connection, draining generations included, and returned when a connection closes; fan-out, bulk and rescue connections keep the configured windows. So the sum over every live connection on the path never exceeds what isolation could hold on the same connections plus its bulk pool. A replacement dialled while its predecessor still drains may start with less than the full growth | §11.5 |
| Stream fairness | Implied by §4.3 | Measured on real fork send loops: 4 and 8 bulk streams on one connection match 4 and 8 isolated connections (1.45 MB/s on a 1.5 MB/s path, each stream within 2 % of its share); 32 bulk streams all progress; 32 page objects and eight 25 ms delta streams beside four bulk streams complete, with delta lateness p99 under 100 ms whether the deltas are INTERACTIVE (`classifier=phased`) or mislabelled BULK, because a BULK stream is one entry in BULK's round robin, not a stream behind the others' backlog | §4.3, §6 |

**Stage 9, `dc-long-haul` on the new architecture.**

| Topic | Earlier text | As built, and why | Section |
| --- | --- | --- | --- |
| Validator | "Accepted only … outside `dc-long-haul`" (stage 1); `classifier=phased` refused with the profile | The profile runs the legacy bundle or the whole new architecture, checked once every switch is known (`ValidateProfileBundle`); partial v2 bundles are refused because none was designed for the hierarchy and none is what the gate measures | §11.3, §14 |
| Leaf | "One authority per leaf (endpoint pair)" | The stage-6 key, (identity, uplink, remote host); for a multi-provider client it is the endpoint pair; two identities on one pair share the pair's tree node as knowledge, not an authority (§11.4) | §11.3 |
| Knowledge | "Priors for erasure and round trip" | Plus burst factor; a prior holds only while the leaf has fewer than 100 decided samples and comes only from confident ancestors; the compensation reads the same view, so a sibling's erasure never sizes this leaf's after it has measured its own (a legacy chain keeps the maximum for life); no bandwidth hint | §4.1, §11.3 |
| Gateway | Unstated | The gateway's authorities also sit in its tree as knowledge under the profile; the legacy gateway had none, which the harness text assumed it had | §11.3 |
| Fan-out | "Fan-out if the single-connection ceiling binds" | Not built: refused under `recovery=carrier`, and the emulator's own ceiling hides the question; carrier-manager fan-out is what would be needed | §7, §11.3, Q3 |
| Gate as a test | Live only | `TestDCMultiLeafFairnessHoldsWithAKnowledgeOnlyTree` and `TestDCRequestLatencyAndSustainedGoodputShape` compute the gate's contrasts with its frozen bounds on the emulator and report a verdict; two runs gave opposite fairness verdicts (FAIL at two trials per arm, PASS at three), and the shape test's goodput contrast failed once on a single slow trial, so neither run decides | §11.3, §15.3 |

**Stage 10, defaults.**

| Topic | Earlier text | As built, and why | Section |
| --- | --- | --- | --- |
| Rollback | "A switch can be turned off when its dependants are off" | Kept, and one switch added: `--bundle=legacy` gives every switch flag not set explicitly its legacy value, on either role; `--bundle=path-authority` is the default. Rolling back nine switches in dependency order by hand, under an incident, is the error this avoids; an explicit switch still overrides and the validator still judges the result. It exists only for the soak and goes with the legacy bundle at stage 11 | §14 |
| Where the default lives | "Flip defaults" | In the product entry points: the daemon's flags (both roles) and the mobile tunnel. The library's zero-valued `ClientConfig`/`ServerConfig` still mean the legacy bundle until stage 11, so the tests and tools that rely on it (and the benchmark's controller comparison) are unchanged; `pep.SwitchesOf` is the one definition of each bundle that the entry points apply | §14 |
| Legacy-only settings | Implicit | `--congestion` other than `erasure` and a client's `--quic-pool=false` have no path-authority form; under the default bundle start-up refuses them and names `--bundle=legacy`, rather than silently selecting the legacy bundle | §14 |
| Soak and release | "Soak one release" | The owner chose one release after stage 11 (v0.15.0). The soak is a deployment of the stage-10 build, gateways first, before stage 11 merges; `docs/ROLLOUT-v0.15.md` | §14 |

**Measurement: the 2×2 on #161.** Crossing gateway and client builds on the high-loss Beijing path placed the deficit in the client's upstream ACK channel, not the gateway's sending; one-change builds then showed that removing either the debit of non-eliciting packets or the cap on a non-send-limited member restored flow-ACK transit and removed most excess reissues. This is the evidence behind I2 and §1's ACK-channel row.

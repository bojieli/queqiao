# Project status

> [!IMPORTANT]
> **Release level:** Public-preview source tree; no production-ready claim
>
> **Wire protocol:** Version 1 only; incompatible versions fail closed
> **Last reviewed:** 2026-08-26

Queqiao is ready to use for its supported paired-gateway topology, from the
binaries published with every release or from source. It is not a universal acceleration layer and it does not promise the
same result on every route. The current work is broadening independent field
evidence and review while keeping the usable protocol moving forward.

## Is Queqiao a fit for my network?

The current deployment has four requirements:

- a client with a known difficult WAN segment;
- one known and trusted gateway or relay beyond that segment;
- multiple application flows whose dominant bottleneck is the client-to-gateway
  path; and
- an operator authorized to coordinate an aggregate policy on that paired
  segment.

This covers intercontinental tunnels, remote corporate access, poor
hotel/mobile/residential links to a stable relay, and individual WAN legs inside
an overlay. The repository provides the paired data plane; peer discovery,
global routing, and multi-egress mesh control are separate integration layers.

Queqiao is not an anonymity network, CDN, universal replacement for Internet
congestion control, or a guarantee that high loss is erasure. Read the [known
limitations](KNOWN-LIMITATIONS.md) before deploying it.

## What is implemented

| Area | Current capability |
| --- | --- |
| Data plane | SOCKS5 TCP CONNECT and UDP ASSOCIATE; pooled QUIC streams and datagrams; authenticated TLS/TCP fallback |
| WAN policy | shared endpoint-pair path model, erasure-aware control, aggregate pacing, and interactive reserve |
| Recovery | byte-offset reassembly, range acknowledgements, bounded replay, sliding-window coding, lane replacement, and UDP relay reclamation |
| Contention | behavioral flow state, priority queues, reactive bulk isolation, and bounded opt-in TCP fallback striping |
| Identity | one-time invitations, provider-pinned gateway identity, per-device mutual TLS, renewal, revocation, and per-user limits |
| Operations | bounded JSON logs, metrics, local visualizer, service examples, release packaging, SBOMs, and rollback procedure |
| Clients | one process per provider or one process serving several providers on separate loopback SOCKS5 listeners; macOS/Linux desktop and gateway binaries published with every release; Windows targets built but under testing; Android SOCKS export and iOS packet-tunnel clients under testing, all using the protocol-1 core |
| Conformance | committed protocol-1 vectors for framing, acknowledgement, destinations, UDP, coding, and enrollment, replayed by the test suite |
| Profiles | `wan-shared-bottleneck` is the default and is what the qualification above refers to; `dc-long-haul` is experimental, covering a long hop between two regions one operator runs, and is qualified on one path |

## Evidence and open work

The motivating path was characterized independently of the transport and showed
a rate-independent downstream erasure floor below a separate congestion knee.
Deterministic tests cover correctness, high loss, reordering, contention,
fallback, and resource bounds. Historical live campaigns also contain useful
causal evidence and rejected designs.

The current evidence boundary is:

- no complete, public protocol-1 multi-network candidate report is recorded;
- the required residential, mobile, managed-network, second-egress, and
  24–72-hour soak matrix is incomplete;
- independent transport, security, and mobile reviews remain open;
- historical throughput figures are design evidence, not a current performance
  promise; and
- the `dc-long-haul` profile has one path behind it. Its measurements are
  recorded in [PATH-CHARACTER-DC-20260826.md](PATH-CHARACTER-DC-20260826.md),
  including the cases where a client-side fix beats it, and it stays
  experimental until a second path is measured.

The [field-validation matrix](FIELD-VALIDATION.md), [release checklist](RELEASE-CHECKLIST.md),
and [production design criteria](PRODUCTION-DESIGN.md) define the gates rather
than a single benchmark number.

## Claim levels

| Claim | Status |
| --- | --- |
| Prebuilt binaries for macOS, Linux, and Windows | Published with every release: six reproducible archives with SBOMs and license texts |
| Builds and runs from source | Supported by the current repository and CI design |
| Usable public preview | Supported for the paired-gateway topology; final publication gates remain explicit |
| Works on every high-loss path | Not claimed |
| Faster than BBR- or QUIC-based proxies in general | Not claimed |
| Production-ready | Not yet claimed; blocked on the qualification and review gates |

## Protocol compatibility

Protocol 1 is the only supported wire protocol. A wire-incompatible change must
increment the version, update [the protocol specification](PROTOCOL.md), add
fail-closed compatibility tests, and document an upgrade path. Protocol 1 has no
silent fallback to an earlier wire or shared-secret mode.

## Help qualify the design

Run Queqiao on a path the maintainers cannot access and report what happened.
Short-lived, interactive, and bulk traffic are views of the same transport;
include a same-window baseline, exact commit and command, and failed trials.
Use [contributing network evidence](CONTRIBUTING-NETWORK-EVIDENCE.md) so the
result is reproducible and safe to publish.

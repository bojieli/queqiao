# Roadmap

Queqiao is usable today for a supported paired client-to-gateway deployment.
The roadmap is therefore about widening confidence and usefulness, not waiting
for a future prototype before anyone can try it.

## Shipped in the current protocol

- Local SOCKS5 TCP and UDP ingress with an authenticated provider gateway.
- QUIC streams and datagrams with authenticated TLS/TCP fallback.
- Shared endpoint-pair path measurement, aggregate pacing, erasure-aware
  recovery, sliding-window coding, and direction-specific control.
- Unified flow framing with byte offsets, range acknowledgements, bounded replay,
  lane replacement, and UDP relay reclamation.
- Behavioral flow hints, priority scheduling, reactive bulk isolation, and
  bounded TCP fallback striping.
- One-time invitations, provider-pinned identity, per-device mutual TLS,
  renewal, revocation, per-user limits, metrics, logs, packaging, and rollback.
- Android export mode and an iOS packet tunnel using the same protocol-1 core.
- An experimental second profile, `dc-long-haul`, for a long operator-owned hop
  carrying inference requests, with the path model, flow attribution, and
  live-path instruments it needs.

An earlier plan to aggregate one flow by opening more paths was measured and
retired: the motivating bottleneck is shared by endpoint pair rather than
independent 4-tuples. Separate connections remain for pooling, reactive
isolation, and failure recovery; they are not a promise of extra capacity. The
full deletion record is in the
[historical multipath design](archive/2026-08-development/DESIGN-MULTIPATH.md).

## Current priority: qualify more paths

The motivating path is characterized, and deterministic tests exercise the
important mechanisms. The next evidence must come from networks the maintainer
does not control:

- two independent residential fixed networks;
- two independent mobile or hotspot carriers;
- one managed or restrictive Wi-Fi network;
- one additional access technology or provider;
- a second egress provider; and
- representative 24–72-hour mixed-workload soaks.

Use [field validation](FIELD-VALIDATION.md) for the matrix and
[contributing network evidence](CONTRIBUTING-NETWORK-EVIDENCE.md) for safe,
reproducible reports.

The experimental `dc-long-haul` profile needs the same thing for a different
shape of link: a long hop between two regions one operator runs. One such path
is characterized, which is not enough to know whether its constants are the
profile's or that link's.
[Qualifying a second path](MEASURING-A-DC-PATH.md#qualifying-a-second-path) is
the ordered set of questions that would settle it, and the most useful second
path is one that is long and does **not** erase, since every figure we have
conflates the round trip with the loss and only a clean long path separates
them.

## Current priority: independent review

The public preview still needs independent transport, security, and mobile
review. The [release checklist](RELEASE-CHECKLIST.md) is the authority for what
must be complete before a release artifact or production-ready claim.

## Deliberately not on the roadmap

- A universal congestion controller for unrelated public bottlenecks.
- Opening more connections to aggregate capacity on one shared endpoint pair.
- A full discovery, global routing, or multi-egress mesh control plane in this
  repository.
- Anonymity, CDN behavior, or application-content inspection.

Those may belong in products built around the paired data plane, but they are
not hidden promises of the current protocol.

## How roadmap items change

Mechanisms can change when measurements contradict them. A wire-incompatible
change increments the protocol version, updates [the protocol specification](PROTOCOL.md),
adds fail-closed tests, and documents migration. Rejected designs and negative
results remain available in the [historical archive](archive/README.md).

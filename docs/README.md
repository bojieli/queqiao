# Queqiao documentation

This is the documentation home for Queqiao, an open-source transport for
making difficult, known long-haul links faster and more reliable. The
[repository README](../README.md) is the best introduction; this index helps
you choose the next document based on what you want to do.

## Start here

| If you want to… | Read |
| --- | --- |
| Understand the problem and the design principles | [Vision and principles](VISION.md) |
| Decide whether it fits, and where the gateway has to go | [Choosing and placing a gateway](CHOOSING-A-GATEWAY.md) |
| Check what has been qualified and what has not | [Project status](STATUS.md) and [known limitations](KNOWN-LIMITATIONS.md) |
| Use a provider and client | [Deployment guide](DEPLOYING.md) |
| Understand the transport in depth | [Current design](DESIGN.md), then [architecture](ARCHITECTURE.md) |
| Measure a path or compare a baseline | [Benchmarking](BENCHMARKING.md) |
| Work out which segment a deployment's loss is on | [Which segment is at fault](PROFILING-A-TUNNEL.md) |
| Run inference traffic between two regions you own | [Datacenter profile](DEPLOYING-DC-PROFILE.md) |
| Contribute code, documentation, or network results | [Contributing](../CONTRIBUTING.md) |

## Use and operate Queqiao

- [Choosing and placing a gateway](CHOOSING-A-GATEWAY.md) — the decision that
  precedes deployment: why both ends must run this software, where a gateway
  has to sit to help, why a destination served from an anycast edge can be made
  slower by one, the client-side fixes to try first, and how to measure all of
  it before spending anything.
- [Deployment guide](DEPLOYING.md) — provider setup, invitations, desktop
  enrollment, multi-provider clients, Clash/mihomo, service installation,
  monitoring, upgrades, and rollback.
- [Runtime logging](LOGGING.md) — log locations, rotation, telemetry, and safe
  evidence collection.
- [Mobile clients](MOBILE.md) — Android and iOS builds, product boundaries,
  and release requirements.
- [Android export mode](ANDROID-EXPORT.md) — how the released Android app
  provides a local SOCKS5 endpoint to an existing routing client.
- [Deploying the datacenter profile](DEPLOYING-DC-PROFILE.md) — running
  `dc-long-haul` for inference traffic on a long hop between two regions you
  operate, including which free client-side fixes to apply first and when the
  transport is still worth deploying.
- [Known limitations](KNOWN-LIMITATIONS.md) — scope, privacy, topology,
  platform, and operational limits to check before deployment.

## Understand the design

- [Vision and principles](VISION.md) — the durable problem statement and the
  assumptions that should survive mechanism changes.
- [Current design](DESIGN.md) — the measured loss model, recovery, pacing,
  pooling, fallback, and rejected alternatives behind protocol 1.
- [Comparing transports](COMPARISON.md) — architectural differences and a
  clearly labeled historical TUIC/Hysteria2 comparison.
- [Architecture](ARCHITECTURE.md) — components, flow lifecycle, trust
  boundaries, resource limits, and carrier behavior.
- [Protocol version 1](PROTOCOL.md) — the normative wire contract, framing,
  authentication, recovery, and conformance rules.
- [Path characterization](PATH-CHARACTER-20260813.md) — the open-loop
  measurement that exposed the motivating path's erasure floor and congestion
  knee.
- [Datacenter profile design](DESIGN-DC-PROFILE.md) — why a second profile
  exists, what separates it from the access-link one, and the four assumptions
  that measurement overturned.
- [Control redesign](CONTROL-REDESIGN.md) — proposed delay-bounded goodput
  objective, the two latched estimators it removes, what it does not solve, and
  the cases that would falsify it.

## Measure and qualify

- [Finding out which segment is at fault](PROFILING-A-TUNNEL.md) — profiling a
  live tunnel to localise loss to the client's access link, the long haul, or
  the gateway's own transit, and why each end needs an anchor that is unfiltered
  where it is probing from.
- [Benchmarking](BENCHMARKING.md) — reproducible short-lived, interactive, and
  bulk workload measurements.
- [Reproducing the datacenter measurements](MEASURING-A-DC-PATH.md) — how to
  characterize a long hop, compare arms without fooling yourself, and measure a
  real inference endpoint rather than a stand-in.
- [What a China-US datacenter path actually is](PATH-CHARACTER-DC-20260826.md) —
  the full characterization, including the real ASR and TTS results and the
  case where a tuned client beats this transport.
- [Field validation](FIELD-VALIDATION.md) — the real-network matrix for NAT,
  middleboxes, access diversity, and release qualification.
- [Field-result index](field-results/README.md) — current protocol-1 records;
  historical records are kept separate until they qualify the current wire.
- [Production design criteria](PRODUCTION-DESIGN.md) — the stronger bar for a
  production-ready claim.
- [Public release checklist](RELEASE-CHECKLIST.md) — the authority for preview
  publication and release evidence.

## Contribute and maintain

- [Contributing guide](../CONTRIBUTING.md) — development checks, pull requests,
  changelog entries, protocol changes, and safe reporting.
- [Contributing network evidence](CONTRIBUTING-NETWORK-EVIDENCE.md) — how to
  share measurements and counterexamples without exposing private data.
- [Roadmap](ROADMAP.md) — what is implemented, what was retired, and what
  qualification work remains.
- [Releasing](RELEASING.md) — changelog assembly, reproducible archives,
  installation, and rollback.
- [Mobile memory](MOBILE-MEMORY.md) — resource budgets for the packet-tunnel
  extension.

## Historical records

The [archive](archive/README.md) preserves dated measurements, rejected
designs, audits, and release experiments. Archive documents may describe old
wire versions, invalid measurements, or removed commands. They are useful
provenance, not current operational guidance; when an archived record differs
from a current document, the current document wins.

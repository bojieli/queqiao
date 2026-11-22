# Production design criteria

> [!NOTE]
> **Status:** Current release criteria
>
> **Applies to:** Public protocol 2
> **Last reviewed:** 2026-11-22

Queqiao is usable as a public preview for the supported paired topology. A
production-ready claim requires evidence for both the transport and the
multi-user security system; passing unit tests alone is not sufficient.

## Security criteria

- Independent review of provider-root pinning, role-constrained certificate
  chains, custom URI verification, and ALPN isolation.
- Adversarial tests for missing/wrong/expired/revoked credentials, invitation
  guessing/replay/concurrency, cross-user joins and UDP resume, malformed
  persisted state, and renewal identity preservation.
- Provider-state backup and trust-domain replacement drills.
- Native permission and atomic-write validation on supported operating systems.
- Load tests showing global/per-user admission and enrollment bounds resist
  descriptor, memory, and CPU exhaustion.

## Operational criteria

- A provider can initialize, add a user, produce one share URI, observe a
  device, revoke it, and restore state without editing certificate fields.
- A user can import that URI into one profile and start the client without DNS,
  CA, UUID, or secret configuration.
- Device and gateway renewal survive normal restart and network interruption;
  failure messages explain when re-enrollment is required.
- Authorization changes propagate to a running gateway and active flows within
  the documented bound.

## Transport criteria

- Deterministic direct-vs-Queqiao tests cover zero loss, high loss, reordering,
  abrupt lane death, half-close, final-ACK loss, UDP rescue, fallback, and
  concurrent interactive/bulk workloads.
- Long live-path trials report completion rate and tails, not only median
  throughput.
- Resource use remains bounded through 24–72 hour soak tests and cancellation
  storms.
- QUIC/TCP fallback decisions distinguish endpoint failure from UDP blocking.

## Release rule

Any wire change increments the version and fails closed. Protocol 2 requires matching
endpoints and rejects protocol 1 during TLS negotiation. A future compatibility policy
must be designed explicitly rather than inferred from permissive parsing.

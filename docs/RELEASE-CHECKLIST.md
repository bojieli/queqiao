# Public release checklist

This checklist is the release authority for v0.1. A Git tag or GitHub Release
must not be created until every preview blocker is checked, the evidence links
name the candidate commit, and the maintainer has reviewed and approved that
exact commit. Production-ready language has additional gates below.

## Public-preview blockers

- [x] Supported topology and known limitations are documented.
- [x] Wire protocol 2 is documented, emitted in build metadata, and rejects
  every other version with a diagnosable error.
- [x] Every protocol-2 limit is fixed by the specification rather than by
  configuration, and `testdata/protocol2/vectors.json` records the framing,
  acknowledgement, destination, UDP, coding, and enrollment encodings as
  committed vectors that the test suite replays on every run.
- [x] Security reporting instructions and residual risks are documented.
- [ ] A pinned full-history secret scan reports zero unresolved findings for
  the exact protocol-2 candidate commit.
- [ ] The protocol-2 candidate history scan contains no deployed credential;
  field-validation
  credentials and TLS material have been rotated independently, and rollback
  material contains no superseded secret.
- [ ] Exact-candidate operational evidence contains no active secret, private key,
  private hostname, personal path, or unnecessary live host address.
- [ ] Linked dependency licenses and the CycloneDX SBOM match the packaged
  binary for every target.
- [ ] GitHub provenance attestations cover every archive, SBOM, and checksum
  manifest in the candidate artifact.
- [ ] Downloaded protocol-2 archives pass native runtime smoke tests on Linux, macOS, and
  Windows; unsupported native architectures are called out rather than implied.
- [ ] Normal CI, full tests, race tests, vet, vulnerability scan, fuzz smoke,
  fallback soak, package reproducibility, and actionlint are green on the exact
  candidate commit.
- [ ] Staticcheck and a freshly reviewed gosec baseline pass on the exact candidate;
  use the historical baseline in
  [`STATIC-SECURITY-AUDIT-20260817.md`](archive/2026-08-development/STATIC-SECURITY-AUDIT-20260817.md)
  only as a format; generate fresh evidence for protocol 2.
- [ ] Install, one-endpoint patch upgrade, coordinated incompatible-version
  refusal, and rollback evidence is attached to the candidate report.
- [ ] `./scripts/changelog.py release --version vX.Y.Z` has assembled the
  pending [`changelog.d/`](../changelog.d/) entries into a dated section, the
  section has been read as prose, and `changelog.d/` is empty on the candidate
  commit. A shipped release with no section of its own is a release whose
  changes are recorded nowhere.
- [ ] The download links in `README.md` name the version being released.
  `python3 -m unittest scripts.test_download_links` fails until they do: it
  compares them against the newest section in `CHANGELOG.md`, which the step
  above has just added.
- [ ] `CHANGELOG.md` and the exact candidate release notes describe v0.1 as a
  paired fixed-egress public preview.
- [ ] The maintainer has reviewed the candidate artifacts and explicitly
  approved publication.
- [x] The GitHub `public-release` environment holds the signing secrets and
  scopes them to the jobs that name it, and the release is triggered either by
  pushing the reviewed `v*` tag or by manual workflow inputs naming the
  reviewed commit and candidate run. The tag push is the approval; the
  environment carries no deployment prompt.

### Repository settings

Four of the items above are settings rather than anything in this tree, and
GitHub offers none of them while the repository is private: rulesets and branch
protection answer 403, the environment reviewer answers 422 on plan grounds,
and private vulnerability reporting is not exposed at all. They are applied
after the visibility flip and before the tag by
[`apply_release_settings.sh`](../scripts/apply_release_settings.sh), which
refuses to run against a private repository and reads every setting back after
writing it.

Applied and read back on 2026-08-19:

- private vulnerability reporting is enabled, so the advisory link in
  `SECURITY.md` reaches a form rather than a 404;
- the `protect main` ruleset blocks deletion and non-fast-forward pushes to
  `main`, with no bypass actors;
- the `protect release tags` ruleset blocks deletion, non-fast-forward, and
  update across `refs/tags/v*`, so a published tag cannot be moved out from
  under the attestation that names it; and
- the `public-release` environment scopes the six Apple signing secrets to the
  jobs that name it, so no other workflow can read them. It carries no
  deployment approval: what makes publication mean anything is the authorize
  job in `release.yml`, which refuses any tag whose commit has no successful
  candidate run.

Confirm them in repository settings as well. From inside a workflow file an
unconfigured gate looks exactly like a satisfied one.

## Production-ready claim blockers

- [ ] The real-network matrix in `FIELD-VALIDATION.md` is complete across the
  minimum independent access networks and two egress providers.
- [ ] At least two representative paths complete the required 24-72-hour soak
  without correctness failures or unbounded resource growth.
- [ ] An independent reviewer assesses the design in `SECURITY.md`; all critical
  and high findings are fixed, and accepted lower-severity findings are public.
- [ ] Operational monitoring, incident response, supported-version lifetime,
  and credential-rotation ownership have named maintainers.
- [ ] An implementation outside this tree replays `testdata/protocol2/vectors.json`
  and interoperates, so protocol 2 is demonstrated rather than only documented.

## Mobile release blockers

- [x] Android and iOS use the protocol-2 core with full IPv4/IPv6 TCP and UDP,
  crash-safe enrollment, platform secure storage, automatic renewal, bounded
  packet/session queues, and pinned runtime dependencies. Android exposes that
  traffic as an authenticated SOCKS5 endpoint for a consumer routing client;
  iOS carries it as a packet tunnel.
- [x] The released Android APK declares no `BIND_VPN_SERVICE` and no
  `android.net.VpnService` intent filter, asserted in CI against the assembled
  artifact.
- [x] The exact linked runtime and isolated build-tool graphs are pinned,
  license-allowlisted, reproducibly audited, and their full notices are
  embedded in both applications.
- [x] Android release lint, R8, APK/AAB assembly, test-APK assembly, iOS strict
  lint, simulator build, and app/core boundary tests pass locally.
- [x] The dependency-free Android instrumentation suite passes all eight
  storage, catalog, routing, connectivity, and protocol-boundary checks on
  isolated API 33 and API 35 emulators.
- [ ] Android API 30, 33, and current physical devices pass the Keystore suite
  and the complete profile-probe, TCP/UDP, IPv4/IPv6, DNS, permission, revoke,
  and lifecycle matrix on Wi-Fi and cellular, driven through a real consumer
  client — v2rayNG, mihomo, or sing-box — with Queqiao excluded from its
  tunnel.
- [ ] Removing that exclusion is confirmed to fail loudly: the notification
  gains the `VPN not excluded` warning while the session is still up, and the
  in-app connection test reports an unreachable provider naming the exclusion,
  rather than the loop degrading silently. Restoring the exclusion clears the
  warning without a reconnect.
- [ ] Current physical iPhones pass signing, install, packet-tunnel TCP/UDP,
  IPv4/IPv6, DNS, per-profile probe, permission, revoke, sleep/wake,
  Wi-Fi/cellular transition, and reconnect tests.
- [ ] Extension memory and `setTunnelNetworkSettings` latency are measured on a
  physical iPhone with the bundled route set enabled, against the profile in
  [`MOBILE-MEMORY.md`](MOBILE-MEMORY.md). The extension times both the plan
  build and the settings install and records them in its diagnostics, so the
  latency half is read from an exported log rather than from Instruments;
  memory still needs the debugger. A regression drops the route bound before
  the feature ships.
- [ ] iOS automatic connection rules are exercised on hardware: a trusted Wi-Fi
  network keeps the tunnel down, a manual disconnect is not undone by the
  connect rule, and disabling the feature while disconnected takes effect
  without a further connect.
- [ ] iOS app-update, configuration removal, provider failure, and network-loss
  stops produce a named provider reason plus `fetchLastDisconnectError` output
  in the sanitized, shareable connection log.
- [ ] Both platforms pass 24-hour mixed interactive/bulk soak with bounded
  memory, goroutines/threads, descriptors, packet queues, and energy use.
- [ ] Near-expiry certificate renewal is demonstrated during an active tunnel,
  and revocation closes active traffic within the documented bound.
- [ ] Clean install, signed upgrade, failed-upgrade recovery, profile deletion,
  and signing-key recovery procedures are exercised on release artifacts.
- [ ] An independent mobile security/privacy review closes all critical and
  high findings and records accepted lower-severity findings publicly.
- [ ] Android Developer Console/package registration is complete, and any Play
  submission has a reviewed `specialUse` foreground-service justification
  matching what the service actually does. iOS remains source-build only
  unless an Organization team completes App Store review and
  jurisdiction-specific VPN obligations.

Completing the preview section permits a v0.1 public-preview release; it does
not complete or waive the production-ready section.

The former wire-3 candidate is preserved only as a historical example in
[`RELEASE-CANDIDATE-20260817.md`](archive/2026-08-development/RELEASE-CANDIDATE-20260817.md).
A new complete candidate report is required for public protocol 2.

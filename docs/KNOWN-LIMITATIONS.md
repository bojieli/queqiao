# Known limitations

> [!NOTE]
> **Status:** Current limitations for public protocol 1
> **Last reviewed:** 2026-08-26

These are the boundaries of the ready-to-use public-preview deployment. Read
them before treating a successful local test as evidence that a different
network will behave the same way. The [project status](STATUS.md) tracks which
qualification items are still open.

- The congestion control described in [the control
  redesign](CONTROL-REDESIGN.md) has no brake on a policed path. A policer
  drops what it cannot pass and holds nothing, so it produces loss and no
  delay -- and that design uses delay as its congestion signal and no longer
  treats loss as one. Measured against an emulated policer shaped to 250 KB/s,
  a sender reaches 2.4 times the path's capacity at 36% loss with the brake
  reading zero throughout. The live path this project targets is a policer, so
  this is the ordinary case rather than an edge one.

  This is shipping. It is the controller in the release, not a proposal, and
  the honest summary is that a policed path is less overdriven than it was
  during this work and is still overdriven: 42 times the path became 7.3 and
  then 2.4, each step a separate fault found by measuring rather than by
  reasoning. The bandwidth estimate turned out not to be one of them -- driven
  in isolation it reads within one per cent of a policer's rate -- and the two
  that were are an erasure compensation that fed itself and an all-time peak
  bandwidth that was re-seeded faster than the filter could retire it. What a
  policed path does not yet have is a signal that stops the remaining
  overdrive, and the transport will keep pushing into one until it does.

  `internal/pep/case4_test.go` asserts this behaviour rather than the fix, so
  it cannot change silently in either direction, and the redesign document
  records what was tried, in what order, and which of it was wrong. It has since
  earned that design twice: a change letting a sender release a burst when
  neither brake reported anything took the same emulated policer from 3.0x to
  4.0x and its loss from 32.9% to 54.9%, because on a policer neither brake
  reports anything by construction. The burst is now also gated on the sending
  direction being close to lossless, which is a guard against the blind spot
  rather than a fix for it.
- The `dc-long-haul` profile is qualified on exactly one path. Everything known
  about it comes from a Guiyang-to-Irvine link, so its constants are that
  link's constants until a second path either confirms them or doesn't. It is
  marked experimental for that reason and the marking is not a formality.
- On a path direction that does not erase, a fully tuned client reaches the same
  median as this transport. Reusing a connection and setting
  `net.ipv4.tcp_slow_start_after_idle=0` took a real 355KB inference upload to
  240.9ms, against 236.5ms through the tunnel in the same minutes. Do the free
  fixes: they cost a config line and they get you there. What they do not reach
  is a connection that is genuinely cold, a direction that erases, or a caller
  you cannot reconfigure. See
  [DEPLOYING-DC-PROFILE.md](DEPLOYING-DC-PROFILE.md).
- Queqiao is a WAN optimization data plane, not an anonymity network. The
  desktop ingress is SOCKS5, the released Android app exports an authenticated
  SOCKS5 endpoint to a separate routing client, and the iOS app is a
  full-device packet tunnel; in every case the provider observes destinations
  and traffic shape.
- High loss by itself does not prove Queqiao's erasure model applies. Queue
  overflow, bursty wireless contention, shaping, route capture, and independent
  erasure require different responses and must be distinguished.
- The non-TCP-friendly policy assumes an operator-controlled endpoint-pair
  segment. It may be inappropriate when the dominant bottleneck is a shared
  public resource outside the operator's authority.
- No complete multi-network protocol-1 field campaign has been published yet;
  performance on unmeasured paths is not guaranteed.
- The frame payload limit is a constant of protocol 1 rather than a deployment
  setting, so a gateway cannot be configured to accept less than the wire
  requires. Version 1 negotiates no capabilities, and a private receive limit
  would fail one direction of traffic without naming the setting or the peer
  holding it. The former `--max-payload` flag is removed; `--chunk-size`
  remains, because what a sender chooses to emit is a local choice.
- The committed conformance vectors are replayed only against this
  implementation. They pin the wire and make a silent divergence loud, but no
  independent implementation has been checked against them yet, so protocol 1
  is documented rather than demonstrated interoperable.
- Metrics have no authentication; bind them to loopback or protect them.
- Provider state is an online high-value secret. Queqiao does not yet integrate
  a hardware security module or operating-system keychain for issuer keys.
- The portable client profile contains the device private key. A GUI may store
  it in a platform keychain later, but the current file must remain mode 0600.
- The packaged provider topology uses one gateway endpoint. The paired data
  plane can serve a leg in a wider corporate or mesh overlay, but multi-gateway
  discovery, route exchange, load balancing, and seamless trust-domain
  migration are not implemented here.
- Automatic physical-source selection currently considers IPv4 addresses.
  Hosts with several active physical IPv4 interfaces must choose one with
  `--local-address if:NAME`; an IPv6-only uplink needs an explicit local IPv6
  address.
- Revocation is enforced at new TLS/stream authorization and by a one-second
  active-flow poll; it is deliberately not instantaneous packet revocation.
- Device renewal requires a still-valid, non-revoked identity. After expiry or
  profile loss, the user needs a new one-time invitation.
- UDP rescue preserves the gateway relay socket when reclamation succeeds but
  cannot recover datagrams in flight during path failure.
- Automatic TCP fallback cannot bypass a network that blocks both transports
  or the selected gateway port.
- `--allow-private-destinations` removes the default SSRF boundary and should
  be used only for an intentional private-access service.
- The desktop SOCKS listener is intentionally loopback-only and has no remote
  authentication. Use a separately authenticated access layer rather than
  exposing it directly to a LAN or public network.
- A trust-root/issuer compromise requires creating a new provider state and
  re-enrolling users; device revocation is insufficient.
- The released Android app is not a VPN and has no routing engine. It exports
  an authenticated SOCKS5 endpoint on loopback, and the consumer client that
  owns the device tunnel supplies rules, per-app policy, and DNS. That client
  must exclude Queqiao's package from its tunnel; if it does not, Queqiao's own
  uplink is captured and the connection loops until it times out instead of
  failing outright. The app detects the condition — Android's per-UID default
  network answers it directly — but the answer is advisory: it names a likely
  cause in the notification and in the connection test, and never blocks a
  connection, because a VPN carrying Queqiao's uplink is not by itself proof of
  a loop. The debug build, which does carry a `VpnService`, reads the same rule
  list as iOS from `routing-rules.conf` in its files directory; that is a
  development affordance and cannot ship, because the released artifact
  declares no `BIND_VPN_SERVICE` and CI asserts it against the assembled APK.
- The iOS client is a full-device tunnel with a routing rule list: `DOMAIN`,
  `DOMAIN-SUFFIX`, `DOMAIN-KEYWORD`, `IP-CIDR`, `GEOIP` and `DST-PORT` rules
  choosing between `PROXY`, `DIRECT` and `REJECT`, first match wins, in the
  syntax Clash, mihomo, sing-box and Shadowrocket read. A flow no rule matches
  takes the tunnel. It does not offer per-app routing, process rules,
  `URL-REGEX` or `USER-AGENT` matching, remote rule-set subscriptions, or a
  choice of outbound: there is one tunnel, so a rule naming a proxy group is
  refused rather than read as `PROXY`.
- Name rules work by the core answering lookups itself, from `198.18.0.0/15`,
  and reversing the handle when the connection arrives. Two consequences are
  worth knowing. An application that ignores the tunnel's resolver and speaks
  DNS-over-HTTPS to a server of its own never asks, so its flows arrive as
  literal addresses and only the address rules can see them. And the handles
  live for as long as the tunnel does: a name evicted from a full map ends the
  flows still using it rather than misrouting them, which is a reset the
  application sees rather than traffic going somewhere it should not.
- `DIRECT` flows are resolved on the device, which is the point of matching
  `DIRECT`; proxied flows are resolved by the gateway, which is the vantage
  they are being sent to use. Queries that are not answered locally still go
  to Cloudflare through the encrypted tunnel.
- The iOS bundled China route set matches addresses, not names, and the
  registry delegates address space that need not be in use. The "keep Chinese
  addresses direct" toggle is therefore still experimental and still off by
  default. What it could never do on its own was follow a Chinese domain that
  answers with a CDN address outside the registry set; a `DOMAIN-SUFFIX` rule
  can, which is why the bundled China preset pairs name rules with `GEOIP,CN`
  rather than relying on either alone.
- iOS automatic connection rules match Wi-Fi networks by typed name. Queqiao
  never scans, so a network the user has not named is treated as untrusted, and
  a renamed network stops matching until the user updates the list.
- Android always-on VPN is not offered at all by the released app, which
  declares no `VpnService`. The debug tunnel keeps it disabled pending
  physical-device locked boot and restart qualification.
- Apple App Store VPN publication requires an organization developer account
  under current store rules, so iOS is source-build/self-sign only. Google
  Play's Organization requirement is scoped to apps approved to use
  `VpnService`, which the released Android app is not; that removes one named
  blocker but is not a guarantee of publication, and direct distribution
  remains the supported Android path.

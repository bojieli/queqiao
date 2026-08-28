# Mobile clients

> This page is for people building or qualifying the mobile clients. For the
> user-facing product boundary, start with [the repository README](../README.md)
> and [known limitations](KNOWN-LIMITATIONS.md).

The Android and iOS clients are under testing. They share the protocol-1 core,
but their platform-specific routing, lifecycle, store, and physical-device
qualification remains open; do not read their source availability as a
production-ready mobile release.

Queqiao has native Android and iOS applications backed by one shared Go core.
Neither is a wrapper around the desktop user interface, and the two are
deliberately different products:

- **Android** is an export client. It enrolls the device, holds the identity,
  keeps the certificate renewed, and serves the gateway as one authenticated
  SOCKS5 endpoint on loopback for whichever routing client the user already
  runs. The released build declares no `VpnService`.
  [Android export mode](ANDROID-EXPORT.md) is the full account.
- **iOS** is a full-device packet tunnel, because it cannot compose: the
  platform runs one tunnel provider at a time, a plain app cannot hold a
  background listener, and no App Store routing client offers a plugin
  interface. It therefore carries its own routing rules.

This follows the scope rule in [Vision](VISION.md) — Queqiao supplies the
paired data plane, and a larger overlay supplies discovery, routing, and
policy. Android can honour it because loopback is shared between apps, so the
released app hands routing to whichever client owns the tunnel. iOS cannot
hand it anywhere: there is no client to hand it to, so the rules live here.

What "here" means is one rule list per profile, in the `TYPE,VALUE,ACTION`
syntax that Clash, mihomo, sing-box and Shadowrocket all read, evaluated in
the shared core rather than in either client. `DOMAIN`, `DOMAIN-SUFFIX`,
`DOMAIN-KEYWORD`, `IP-CIDR`, `GEOIP` and `DST-PORT` decide between `PROXY`,
`DIRECT` and `REJECT`, first match wins, and a flow no rule matches takes the
tunnel. Name rules work because the core answers lookups itself from a
reserved range and reverses the handle when the connection arrives; a
`DIRECT` flow is then resolved on the device, which is the vantage that makes
the answer right. The Android debug build reads the same list from a file, so
the two tunnels behave the same where Android has a tunnel at all.

What it still is not: per-app routing, process rules, `URL-REGEX`,
`USER-AGENT`, remote rule-set subscriptions, or several outbounds to choose
between. There is one tunnel, so an action naming a proxy group is refused
rather than guessed at.

The iOS app and its packet-tunnel extension use public Network Extension APIs,
including `NEPacketTunnelFlow`. The Android debug build, and only the debug
build, additionally offers a `VpnService` tunnel as the test vehicle for the
shared packet stack. No private iOS API is used.

## Distribution constraints

These constraints are store policy, not a technical restriction in the source:

- [Apple App Review Guideline 5.4](https://developer.apple.com/app-store/review/guidelines/#vpn-apps)
  requires a VPN app to use the appropriate Network Extension API and says it
  may only be offered by a developer enrolled as an organization. An
  Individual Apple Developer Program membership can sign a development build
  onto that developer's registered devices, but it does not make the app
  eligible for public App Store publication.
- [Google Play Console requirements](https://support.google.com/googleplay/android-developer/answer/10788890)
  require developers of *apps approved to use the `VpnService` class* to
  register as an Organization. The released Android app declares no
  `VpnService`, so it falls outside that clause. That removes the one blocker
  known by name; it is not a guarantee of publication, because the app's
  `specialUse` foreground-service justification is a review surface of its own
  and Play has separate policy pages. CI asserts the property on the assembled
  release APK so it cannot regress unnoticed.
- Android distribution outside Google Play is a separate system. The
  [Android Developer Console](https://developer.android.com/developer-verification)
  supports verified direct distribution, including personal accounts. Its
  Limited Distribution plan is capped at 20 authorized devices; Full
  Distribution supports wider installation. Regional verification begins on
  September 30, 2026 and expands globally in 2027, so release owners must
  re-check the current rules before publishing.

The project consequently produces Android APK/AAB artifacts suitable for
testing and permitted direct distribution, and treats direct distribution as
the supported Android path whether or not a Play submission is attempted. iOS
remains source-build and self-sign only until an organization deliberately
assumes store, privacy, and support responsibility. Store policy can change;
the linked primary sources are authoritative.

## Functional parity

| Capability | Desktop | Android | iOS |
| --- | --- | --- | --- |
| Product model | SOCKS5 helper | SOCKS5 helper | Full-device tunnel |
| Owner of routing policy | Clash/mihomo | The consumer VPN client (debug build: Queqiao) | Queqiao |
| One-time `queqiao://` enrollment | Yes | Yes | Yes |
| Crash-safe enrollment draft | Mode-0600 file | Keystore-encrypted | This-device-only Keychain |
| TLS 1.3 mutual authentication and root pin | Yes | Same core | Same core |
| Hourly certificate maintenance | Yes | Yes | Yes |
| QUIC with TLS/TCP fallback | Yes | Same core | Same core |
| SOCKS TCP and UDP | Ingress API | Exported listener | Internal adapter |
| SOCKS listener authentication | None; loopback-only | Required, per install | N/A |
| Full IPv4 and IPv6 tunnel | Via external TUN client | Debug build only | Native |
| Bounded sessions and packet queues | Yes | Yes | Yes |
| Aggregate in-memory metrics | Yes | Yes | Yes |
| Multiple device-bound provider profiles | N/A (one profile per process) | Yes | Yes |
| Explicit selected-profile choice | N/A (CLI profile argument) | Yes | Yes |
| Authenticated per-profile reachability and latency test | No | Yes | Yes |
| Full-tunnel and local-network bypass policies | External TUN policy | Consumer client | Native |
| User-supplied CIDR bypass | External TUN policy | Consumer client | Native, bounded |
| Bundled China route set | External TUN policy | Consumer client | Experimental, per profile |
| Automatic connection rules | N/A | N/A | Per profile, typed Wi-Fi names |

## Mobile product model

The mobile applications are organized as connection clients rather than
enrollment forms. Home, Profiles, and Settings work the same way on both
platforms; what differs is what a connection *is*.

- **Home** owns the connection state and the single Connect/Disconnect action.
  It shows the selected provider profile, the routing or endpoint detail for
  the active mode, enrolled device name, and aggregate per-connection transfer
  and flow counters. “Selected” means the profile that the next Connect action
  will use; it never means the connection is up. “Active device” is status
  information; it is not an action.
- **Profiles** is a multi-profile library. Importing another invitation adds a
  profile instead of overwriting the current one. Users can select, rename,
  inspect, test, change the routing options for, and delete profiles. “Test
  all connections” runs at most four iOS probes concurrently and runs Android
  probes serially on its bounded application worker. Each probe measures DNS,
  transport setup, mutual TLS, current device authorization, Queqiao protocol
  negotiation, and one authenticated control round trip. It opens no remote
  destination. Selection, testing, and routing changes require the connection
  to be down so displayed state cannot diverge from the running extension or
  service or be measured through another active VPN.
- **Settings** contains stable privacy, key-storage, version, system VPN, and
  license information rather than connection controls. On Android it also
  holds the exported endpoint, its credentials, and the consumer client setup.
  Its encrypted connection-log ring records named iOS stop reasons and the
  system's last disconnect error, and users can share a sanitized text copy
  from production builds. The app reloads its saved VPN manager after
  configuration changes; an unloaded manager is shown as loading rather than
  as a false disconnect.

Both apps import a `queqiao://` invitation through an explicit in-app paste
action; Android can also appear as a user-selected target for shared plain text.
They intentionally do not register the `queqiao` custom URL scheme because
mobile platforms cannot authenticate which installed application owns a custom
scheme, while an unused invitation is a bearer credential. Enrollment remains
crash-safe: a draft containing the newly generated device key is encrypted
before the one-time token is sent, and an interrupted import resumes that exact
draft.

Queqiao does not import or export the resulting private client-profile JSON as
a portable configuration file. That JSON contains the device identity and is
intentionally stored only in this-device-only Keychain storage on iOS or an
Android-Keystore-encrypted envelope excluded from backup. The portable input is
the provider-issued one-time invitation. Deleting a profile therefore requires
a new invitation, consistent with the desktop identity model.

### Android: one endpoint, someone else's rules

The released Android app connects by starting a foreground service that serves
an authenticated SOCKS5 endpoint on loopback, and by doing nothing else. It
declares no `VpnService`, holds no routing rules, and answers no DNS. The
client that owns the device's tunnel decides what reaches Queqiao, and has to
exclude Queqiao's own package from that tunnel or Queqiao's uplink is captured
by it and the connection loops rather than failing. The app watches its own
default network and says so when that happens, which is advice rather than
enforcement.
[Android export mode](ANDROID-EXPORT.md) covers the endpoint, the per-install
credentials, the bypass step for each consumer client, and the setup snippets
the app renders with the live values filled in.

The full-device tunnel remains in the debug build as the vehicle that drives
the shared packet stack end to end on real hardware, and is never published.

### iOS: a bounded routing subset

iOS cannot compose, so the tunnel carries routing policy itself. What it
carries is deliberately a subset rather than a rule engine.

Each profile has one of two base policies:

- **All traffic** routes IPv4, IPv6, and DNS through Queqiao.
- **Exclude local networks** keeps IPv4 private, shared-address, loopback, and
  link-local destinations plus IPv6 unique-local, loopback, and link-local
  destinations outside the tunnel. Internet and DNS traffic still use
  Queqiao. iOS expresses these as excluded Network Extension routes. The
  Android debug tunnel constructs the exact complement as included CIDR routes
  so behavior is the same on every supported API level, including releases
  before Android added `VpnService.Builder.excludeRoute`.

On top of the base policy, a profile may carry:

- **Typed bypass routes** — up to 256 hand-entered CIDR blocks kept off the
  tunnel. An entry that is not a CIDR block is refused as it is saved rather
  than dropped quietly, because a discarded route would leave the user
  believing a destination is off the tunnel when it is not.
- **The bundled China route set**, experimental and off by default. It carries
  the APNIC-delegated blocks exactly as the registry publishes them rather than
  aggregating neighbours together, because aggregation would take addresses
  nobody asked for off the tunnel — the failure the route plan exists to
  prevent. Provenance, regeneration commands, and the registry's own caveat
  that delegated space need not be in use are in
  `mobile/legal/COUNTRY_ROUTES.md`. The set is address-based only: DNS resolves
  through the tunnel via Cloudflare, so a Chinese domain resolved from the
  gateway's vantage point returns addresses that need not be in the set and
  will still route through Queqiao. The UI states this rather than implying
  domain-level routing.
- **Automatic connection rules**, off by default. A profile may bring the
  tunnel up on Wi-Fi, on cellular, or both, and keep it down on Wi-Fi networks
  the user names. Names are typed, never scanned — scanning would require
  location permission, while the system evaluates `ssidMatch` without it. A
  manual disconnect pauses the rules until the next manual connect, since
  otherwise the system would bring the tunnel straight back up and the button
  would appear broken.

A route plan is bounded, and truncation is reported rather than silent. The
bound exists because every excluded route is one iOS installs and consults per
packet, and `setTunnelNetworkSettings` has to complete inside the extension's
startup budget. The parsed route set is built, applied, and released before the
packet engine starts, so its peak does not collide with the Go memory budget in
[Mobile memory](MOBILE-MEMORY.md).

The encrypted catalog stores only profile metadata, selection, and routing
options; each private profile remains a separate encrypted record. Existing single-
profile installations migrate in place on first launch. The packet-tunnel
extension and VPN service receive an explicit profile identifier when starting,
and automatic certificate renewal writes back only to that identity. A catalog
written before a routing field existed decodes with that field's default rather
than failing and taking every enrolled profile on the device with it.

The apps deliberately do not expose experimental transport tuning in their
primary UI. They use the reviewed desktop defaults. The iOS tunnel installs an
MTU of 1280 and sends DNS through Queqiao to Cloudflare's `1.1.1.1` and
`2606:4700:4700::1111` resolvers; on Android the consumer client decides both.
Always-on VPN is not offered by the released Android app at all, since it
declares no `VpnService`; the debug tunnel keeps it disabled until restart and
locked-device behavior has completed the physical-device qualification
matrix.

## Dependency policy

The packet adapter, SOCKS5 CONNECT/UDP ASSOCIATE implementation, lifecycle,
storage integration, and platform UI are maintained in this repository. The
only non-Queqiao runtime networking foundation added for mobile is the actively
maintained Apache-2.0 gVisor netstack; it supplies TCP/IP state machines, not a
proxy protocol or application. Android UI uses only the platform SDK, and iOS
uses only Apple system frameworks.

Every linked Go module is pinned in `mobile/runtime-dependencies.lock`, limited
to MIT, BSD-3-Clause, or Apache-2.0, and checked from the compiled package graph
by `mobile/scripts/audit-dependencies.sh` and from the built AAR/XCFramework by
`mobile/scripts/audit-mobile-binary.sh`. The x/mobile binding support that
gomobile links is included in the runtime lock and notices. The gomobile/gobind
command graph, Android build tools, and downloaded SwiftLint binary are pinned
separately in
`mobile/build-tools.lock`; compiler dependencies such as x/mod, x/sync, and
x/tools are build-only and are not linked into the apps.
Gradle's complete downloaded plugin graph is SHA-256 pinned in
`mobile/android/gradle/verification-metadata.xml` for macOS, Linux, and Windows
host tools.
`mobile/legal/THIRD_PARTY_NOTICES.txt` is deterministically regenerated
from the exact module license files and embedded in both apps. A dependency
change must update the reviewed lock and notices in the same commit.

The mobile-specific maintenance review was refreshed on August 18, 2026.
The pinned [gVisor](https://github.com/google/gvisor) snapshot and
[Go mobile](https://go.googlesource.com/mobile) binding tools both have active
upstream development. Go mobile nevertheless describes itself as experimental
and provides no end-user support guarantee. Queqiao therefore treats the
generated binding as a replaceable boundary, audits the result rather than
trusting the generator, and makes upstream abandonment or an unpatched security
issue a release blocker. No third-party UI, VPN product, analytics SDK, or
general-purpose proxy application is embedded.

## Build the shared core

Both platforms require Go 1.26.6. The scripts select that patched toolchain,
install the exact pinned gomobile
tools into a temporary directory, verify both module graphs, run the core race
suite, regenerate license notices, remove Go debug tables, audit the linked
module graph, reject local checkout-path leakage, and replace the platform
framework only after a successful build.

Android prerequisites are JDK 17, Android SDK Platform 37, Android Build Tools
36, and NDK `28.0.12433566`:

```sh
export ANDROID_HOME=/absolute/path/to/Android/sdk
mobile/scripts/build-android-core.sh
cd mobile/android
./gradlew --dependency-verification strict \
  lintDebug lintRelease assembleDebug assembleDebugAndroidTest bundleRelease
```

Run the Keystore instrumentation suite on an API 30 or later emulator/device:

```sh
./gradlew --dependency-verification strict connectedDebugAndroidTest
```

The default release output is unsigned. For a directly distributable build,
use a dedicated long-lived release key and set all four variables:

```sh
export QUEQIAO_ANDROID_STORE_FILE=/absolute/path/to/release.jks
export QUEQIAO_ANDROID_STORE_PASSWORD='...'
export QUEQIAO_ANDROID_KEY_ALIAS='...'
export QUEQIAO_ANDROID_KEY_PASSWORD='...'
./gradlew --dependency-verification strict \
  -PqueqiaoVersionCode=1 -PqueqiaoVersionName=0.1.0 \
  bundleRelease assembleRelease
```

Never commit a keystore or password. Keep an offline backup: losing the key
prevents trustworthy updates. Register the final package name and signing
certificate through the applicable Android distribution console before a wide
release. Google Play additionally requires a privacy policy and Data safety
answers; its Organization requirement is scoped to apps approved to use
`VpnService`, which the release build is not.

The release APK must declare no `BIND_VPN_SERVICE` and no
`android.net.VpnService` intent filter. CI checks this with `aapt2 dump
xmltree` over the assembled artifact, and it holds structurally as well: the
tunnel sources live in `app/src/debug/`, so the release build never compiles
them. Verify it locally the same way when changing anything about the manifest
or the source-set split:

```sh
"$ANDROID_HOME"/build-tools/36.0.0/aapt2 dump xmltree \
  --file AndroidManifest.xml app/build/outputs/apk/release/*.apk \
  | grep -iE 'BIND_VPN_SERVICE|android\.net\.VpnService'
```

## Build iOS for a physical device

Prerequisites are Xcode 26 or later, Go 1.26.6, and a paid Apple Developer
Program membership with the Network Extension capability available to the
selected team:

```sh
mobile/scripts/build-ios-core.sh
open mobile/ios/Queqiao.xcodeproj
```

In Xcode Build Settings, set these user-defined values to identifiers owned by
your team for all configurations:

- `QUEQIAO_APP_BUNDLE_ID`
- `QUEQIAO_EXTENSION_BUNDLE_ID` (normally the app ID plus `.PacketTunnel`)
- `QUEQIAO_KEYCHAIN_SUFFIX` (normally the app ID plus `.shared`)

Select the same development team for `Queqiao` and `PacketTunnel`, retain the
Network Extensions and Keychain Sharing capabilities, connect the registered
iPhone, and run the `Queqiao` scheme. Accept the system VPN configuration
prompt on the phone. A different developer repeats these steps with their own
paid membership, team, and bundle identifiers. There is no unsigned IPA that
can be installed randomly on arbitrary iPhones.

`project.yml` is the declarative project source. XcodeGen is optional and is
needed only when changing project structure; ordinary self-builds use the
committed `.xcodeproj`. If the project is regenerated, review the resulting
project diff and re-run both simulator and device qualification.

The portable boundary suite can run without signing:

```sh
cd mobile/ios
xcodebuild -project Queqiao.xcodeproj -scheme Queqiao \
  -destination 'platform=iOS Simulator,name=iPhone 17' \
  -derivedDataPath DerivedData CODE_SIGNING_ALLOWED=NO test
../scripts/run-swiftlint.sh lint --strict --config .swiftlint.yml .
```

Simulator success proves compilation and app/core boundary behavior only. A
simulator cannot qualify the real packet-tunnel lifecycle, signing,
Wi-Fi/cellular switching, sleep/wake, revocation, or sustained resource use.

## Release qualification

Do not label a mobile artifact production-ready until all unchecked mobile
items in `docs/RELEASE-CHECKLIST.md` are complete on the exact release commit.
At minimum this includes physical-device TCP/UDP and IPv4/IPv6 traffic, DNS,
QUIC-to-TCP fallback, certificate renewal, revocation, suspend/resume,
Wi-Fi/cellular transitions, bounded 24-hour load, clean install/update/rollback,
store/direct-distribution declarations, and independent security review.

Two qualifications are specific to the current product split:

- Android export mode is qualified against a real consumer client with Queqiao
  excluded from its tunnel, and then again with the exclusion removed to
  confirm the loop fails loudly. The steps are in
  [Android export mode](ANDROID-EXPORT.md).
- The iOS bundled route set is qualified by measuring extension memory and
  `setTunnelNetworkSettings` latency with it enabled, against the profile in
  [Mobile memory](MOBILE-MEMORY.md). If either regresses, the route bound drops
  before the feature ships.

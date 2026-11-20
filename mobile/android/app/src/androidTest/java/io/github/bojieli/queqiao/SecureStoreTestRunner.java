package io.github.bojieli.queqiao;

import android.app.Activity;
import android.app.Instrumentation;
import android.os.Bundle;
import android.util.Log;

/** Dependency-free on-device mobile boundary checks. */
public final class SecureStoreTestRunner extends Instrumentation {
    private static final int STATUS_START = 1;
    private static final int STATUS_OK = 0;
    private static final int STATUS_FAILURE = -2;

    @Override
    public void onCreate(Bundle arguments) {
        super.onCreate(arguments);
        start();
    }

    @Override
    public void onStart() {
        NamedCheck[] checks = {
                new NamedCheck("encryptedRoundTripAndDeletion", this::testEncryptedRoundTripAndDeletion),
                new NamedCheck("emptySecretIsRejected", this::testEmptySecretIsRejected),
                new NamedCheck("ciphertextCannotMoveBetweenAccounts", this::testCiphertextCannotBeMovedBetweenAccounts),
                new NamedCheck("profileCatalogRoundTripAndNormalization", this::testProfileCatalogRoundTripAndNormalization),
                new NamedCheck("localNetworkRoutePolicy", this::testLocalNetworkRoutePolicy),
                new NamedCheck("connectionProbeWireFormat", this::testConnectionProbeWireFormat),
                new NamedCheck("vpnExclusionReadsTheDefaultNetwork", this::testVpnExclusionReadsTheDefaultNetwork),
                new NamedCheck("listenPortCollisionIsNamed", this::testListenPortCollisionIsNamed),
                new NamedCheck("invitationBatchExtractsEveryInvitation", this::testInvitationBatchExtractsEveryInvitation),
                new NamedCheck("invitationBatchValidatesWithTheCore", this::testInvitationBatchValidatesWithTheCore),
                new NamedCheck("invitationBatchLabelsFromThePayload", this::testInvitationBatchLabelsFromThePayload),
                new NamedCheck("invitationBatchSummariesAndTitles", this::testInvitationBatchSummariesAndTitles),
                new NamedCheck("invitationQueueSkipsDuplicates", this::testInvitationQueueSkipsDuplicates)
        };
        for (int index = 0; index < checks.length; index++) {
            NamedCheck check = checks[index];
            Bundle status = status(check.name, index + 1, checks.length);
            sendStatus(STATUS_START, status);
            try {
                check.check.run();
                status.putString("stream", ".");
                sendStatus(STATUS_OK, status);
            } catch (Throwable failure) {
                status.putString("stack", Log.getStackTraceString(failure));
                status.putString("stream", "\nFAILED: " + check.name + ": " + failure + "\n");
                sendStatus(STATUS_FAILURE, status);
                finish(Activity.RESULT_CANCELED, status);
                return;
            }
        }
        Bundle result = new Bundle();
        result.putString("stream",
                "\nOK (" + checks.length
                        + " mobile storage, routing, connectivity, invitation, and protocol-boundary checks)\n");
        finish(Activity.RESULT_OK, result);
    }

    private void testEncryptedRoundTripAndDeletion() throws Exception {
        SecureStore store = new SecureStore(getTargetContext());
        String name = "instrumentation_test_secret";
        store.delete(name);
        store.put(name, "private-profile-value");
        require("private-profile-value".equals(store.get(name)), "encrypted round-trip changed the value");
        require(store.contains(name), "stored value was not reported present");
        store.delete(name);
        require(store.get(name) == null, "deleted value remains readable");
    }

    private void testEmptySecretIsRejected() throws Exception {
        SecureStore store = new SecureStore(getTargetContext());
        try {
            store.put("instrumentation_test_empty", "");
            throw new AssertionError("empty secret was accepted");
        } catch (java.security.GeneralSecurityException expected) {
            // Expected.
        }
    }

    private void testCiphertextCannotBeMovedBetweenAccounts() throws Exception {
        SecureStore store = new SecureStore(getTargetContext());
        String first = "instrumentation_test_first";
        String second = "instrumentation_test_second";
        store.delete(first);
        store.delete(second);
        store.put(first, "domain-separated-value");
        android.content.SharedPreferences preferences = getTargetContext().getSharedPreferences(
                "queqiao_secure_store", android.content.Context.MODE_PRIVATE);
        String envelope = preferences.getString(first, null);
        require(envelope != null, "encrypted envelope was not persisted");
        require(preferences.edit().putString(second, envelope).commit(), "could not move test envelope");
        try {
            store.get(second);
            throw new AssertionError("ciphertext was accepted under a different account name");
        } catch (java.security.GeneralSecurityException expected) {
            // Expected: the account name is authenticated as AES-GCM associated data.
        } finally {
            store.delete(first);
            store.delete(second);
        }
    }

    private void testProfileCatalogRoundTripAndNormalization() throws Exception {
        ProfileRepository.ProfileSummary summary = new ProfileRepository.ProfileSummary(
                1,
                "Example",
                "gateway.example:443",
                "provider",
                "gateway",
                "account",
                "device",
                "Phone",
                "2030-01-01T00:00:00Z");
        ProfileRepository.ProfileRecord profile = new ProfileRepository.ProfileRecord(
                "first",
                "secret.first",
                "Example",
                summary,
                RoutingConfiguration.DEFAULT
                        .withMode(RoutingConfiguration.Mode.BYPASS_RULES)
                        .withBypassChinaDirect(true)
                        .withCustomRoutes(java.util.Collections.singletonList("203.0.113.0/24"))
                        .withRules("GEOIP,CN,DIRECT\nFINAL,PROXY"),
                "2026-08-18T00:00:00Z");
        ProfileRepository.Catalog catalog = new ProfileRepository.Catalog();
        catalog.selectedProfileId = "missing";
        catalog.profiles.add(profile);
        catalog.profiles.add(profile);

        require(catalog.normalize(), "invalid catalog was not normalized");
        require(catalog.profiles.size() == 1, "duplicate profile was retained");
        require("first".equals(catalog.selectedProfileId), "missing selection was not repaired");

        ProfileRepository.Catalog decoded = ProfileRepository.Catalog.fromJson(catalog.toJson());
        require(decoded.profiles.size() == 1, "catalog round-trip lost its profile");
        RoutingConfiguration routing = decoded.profiles.get(0).routing;
        require(routing.mode == RoutingConfiguration.Mode.BYPASS_RULES
                        && routing.bypassChinaDirect
                        && !routing.bypassLocalNetworks
                        && routing.customRoutes.equals(java.util.Collections.singletonList("203.0.113.0/24"))
                        && routing.rules.equals("GEOIP,CN,DIRECT\nFINAL,PROXY"),
                "catalog round-trip changed the routing settings");

        // A catalog written before routing rules existed excluded local networks
        // through traffic_policy alone; it has to keep doing so after the upgrade.
        org.json.JSONObject legacy = profile.toJson();
        for (String key : new String[] {
                "routing_mode", "bypass_local_networks", "bypass_china_direct", "bypass_routes", "routing_rules"}) {
            legacy.remove(key);
        }
        legacy.put("traffic_policy", "exclude-local-networks");
        RoutingConfiguration migrated = ProfileRepository.ProfileRecord.fromJson(legacy).routing;
        require(migrated.mode == RoutingConfiguration.Mode.BYPASS_RULES && migrated.bypassLocalNetworks,
                "a legacy local-network exclusion did not survive the migration");
    }

    private void testLocalNetworkRoutePolicy() throws Exception {
        RoutingConfiguration routing = RoutingConfiguration.DEFAULT
                .withMode(RoutingConfiguration.Mode.BYPASS_RULES)
                .withBypassLocalNetworks(true)
                .withCustomRoutes(java.util.Arrays.asList("203.0.113.0/24", "198.51.100.7", "not-a-route"));
        RoutePolicy.Plan plan = RoutePolicy.plan(routing, java.util.Collections.emptyList());
        require(plan.rejected.equals(java.util.Collections.singletonList("not-a-route")),
                "an invalid custom route was not reported");
        java.util.List<RoutePolicy.RouteSpec> routes = RoutePolicy.remainderAfter(plan.excluded);
        require(!isRouted(routes, "203.0.113.9"), "a custom bypass block is routed");
        require(!isRouted(routes, "198.51.100.7"), "a custom bypass address is routed");
        require(isRouted(routes, "198.51.100.8"), "the neighbour of a bypassed address is not routed");
        require(RoutePolicy.plan(routing.withMode(RoutingConfiguration.Mode.ALL_TRAFFIC),
                        java.util.Collections.emptyList()).excluded.isEmpty(),
                "bypass rules were applied while routing all traffic");

        byte[] packed = CountryRoutes.packedChinaSet(getTargetContext());
        java.util.List<RoutePolicy.RouteSpec> chinaDirect = CountryRoutes.decode(packed);
        require(chinaDirect.size() == CountryRoutes.blockCount(packed) && chinaDirect.size() > 1000,
                "the bundled country set did not decode to its declared block count");
        RoutePolicy.Plan withChina = RoutePolicy.plan(routing.withBypassChinaDirect(true), chinaDirect);
        require(withChina.excluded.size() > plan.excluded.size()
                        && withChina.excluded.size() <= RoutePolicy.ROUTE_LIMIT,
                "the country set was not added within the route limit");

        // The builder validates every prefix it is handed (it refuses loopback, for
        // one), so the whole plan has to survive the real thing, not just the model.
        RoutePolicy.apply(new android.net.VpnService().new Builder(),
                routing.withBypassChinaDirect(true)
                        .withCustomRoutes(java.util.Arrays.asList("203.0.113.0/24", "127.0.0.1", "::1")),
                chinaDirect);
        require(isRouted(routes, "8.8.8.8"), "public IPv4 address is not routed");
        require(isRouted(routes, "2001:4860:4860::8888"), "public IPv6 address is not routed");
        require(!isRouted(routes, "10.0.0.1"), "private IPv4 address is routed");
        require(!isRouted(routes, "192.168.1.1"), "local IPv4 address is routed");
        require(!isRouted(routes, "127.0.0.1"), "loopback IPv4 address is routed");
        require(!isRouted(routes, "fd00::1"), "unique-local IPv6 address is routed");
        require(!isRouted(routes, "fe80::1"), "link-local IPv6 address is routed");
        require(!isRouted(routes, "::1"), "loopback IPv6 address is routed");
    }

    private void testConnectionProbeWireFormat() throws Exception {
        ConnectionProbe result = ConnectionProbe.available(
                "{\"version\":1,\"transport\":\"quic\",\"latency_ms\":87}");
        require(result.status == ConnectionProbe.Status.AVAILABLE, "valid probe is unavailable");
        require("quic".equals(result.transport), "probe transport changed");
        require(result.latencyMilliseconds == 87, "probe latency changed");
        require("87 ms · QUIC".equals(result.summary()), "probe summary changed");

        requireInvalidProbe("{\"version\":2,\"transport\":\"quic\",\"latency_ms\":87}");
        requireInvalidProbe("{\"version\":1,\"transport\":\"unknown\",\"latency_ms\":87}");
        requireInvalidProbe("{\"version\":1,\"transport\":\"tcp\",\"latency_ms\":0}");
    }

    /**
     * Export mode's loop warning rests on one platform claim: the default
     * network ConnectivityManager reports is per-UID, so a VPN visible here is
     * one that did not exclude this app. The claim is only checkable on a
     * device, which is why it is checked here rather than nowhere.
     */
    private void testVpnExclusionReadsTheDefaultNetwork() throws Exception {
        require(VpnExclusion.of(null) == VpnExclusion.State.UNKNOWN,
                "an unknown network was reported as an answer");

        // No VPN is configured on a test device or emulator, so the app's own
        // default network must not look like one. A failure here on a phone
        // with a device-wide VPN running is the honest answer, not a bug.
        require(VpnExclusion.current(getTargetContext()) != VpnExclusion.State.CAPTURED,
                "no VPN is running, yet the default network reports one");

        // The TRANSPORT_VPN half cannot be checked from here: NetworkCapabilities
        // has no public constructor or builder, so the only capabilities an
        // instrumented test can obtain are the ones the device actually has.
        // Standing up a real VpnService to produce one would be testing the
        // full tunnel, not this. What remains checkable is checked.
    }

    /**
     * The port is the user's to choose and loopback is shared with every app on
     * the device, so a taken port is an ordinary outcome that has to name
     * itself. Anything else must reach the user unedited.
     */
    private void testListenPortCollisionIsNamed() throws Exception {
        Exception collision = QueqiaoProxyService.listenFailure(
                1080, new java.io.IOException("listen tcp 127.0.0.1:1080: bind: address already in use"));
        String message = collision.getMessage();
        require(message != null && message.contains("1080") && message.contains("Settings"),
                "a taken port did not name the port or where to change it: " + message);

        Exception other = new java.io.IOException("certificate has expired");
        require(QueqiaoProxyService.listenFailure(1080, other) == other,
                "an unrelated listener failure was rewritten");
    }

    /**
     * A structurally valid invitation the core accepts. Nothing is enrolled by
     * these checks, so the identifiers and token need only be well formed.
     */
    private static String testInvitation(String name, String endpoint, int seed) throws Exception {
        org.json.JSONObject payload = new org.json.JSONObject();
        payload.put("v", 1);
        payload.put("name", name);
        payload.put("provider", "a".repeat(32));
        payload.put("endpoint", endpoint);
        payload.put("gateway", "b".repeat(32));
        payload.put("root", base64Url(filled(1)));
        payload.put("token", base64Url(filled(seed)));
        payload.put("expires", java.time.Instant.now()
                .plusSeconds(3600)
                .truncatedTo(java.time.temporal.ChronoUnit.SECONDS)
                .toString());
        return "queqiao://enroll/"
                + base64Url(payload.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static byte[] filled(int value) {
        byte[] bytes = new byte[32];
        java.util.Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private static String base64Url(byte[] bytes) {
        return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    /** Mirrors InvitationBatchTests in the iOS app's InvitationScanTests.swift. */
    private void testInvitationBatchExtractsEveryInvitation() throws Exception {
        String first = testInvitation("provider-a", "198.51.100.1:12540", 2);
        String second = testInvitation("provider-b", "203.0.113.9:12548", 3);
        String text = "provider-a: " + first + "\n\n\"" + second + "\",\n" + first + "\nnot an invitation";
        require(InvitationBatch.extract(text).equals(java.util.Arrays.asList(first, second)),
                "one paste did not yield every invitation, in order, once");
        require(InvitationBatch.extract("(" + first + ");[" + second + "]|").equals(
                        java.util.Arrays.asList(first, second)),
                "brackets, semicolons, or pipes were kept as part of an invitation");
        require(InvitationBatch.extract("vless://example\nhttps://example.com").isEmpty(),
                "unrelated links were taken as invitations");
        require(InvitationBatch.extract(null).isEmpty(), "a missing text yielded an invitation");
    }

    private void testInvitationBatchValidatesWithTheCore() throws Exception {
        String valid = testInvitation("provider-c", "198.51.100.2:12540", 4);
        InvitationBatch.Evaluation result = InvitationBatch.evaluate(
                valid + "\nqueqiao://enroll/not-base64!", InvitationBatch.CORE);
        require(result.accepted.equals(java.util.Collections.singletonList(valid)),
                "the core-valid invitation was not accepted");
        require(result.rejected == 1, "the invalid candidate was not counted: " + result.rejected);
    }

    private void testInvitationBatchLabelsFromThePayload() throws Exception {
        String uri = testInvitation("provider-d", "203.0.113.7:12542", 5);
        InvitationBatch.QueuedInvitation item = InvitationBatch.describe(uri);
        require("provider-d".equals(item.name), "the provider name was not read: " + item.name);
        require("203.0.113.7:12542".equals(item.endpoint), "the endpoint was not read: " + item.endpoint);
        require(uri.equals(item.uri), "the label changed the invitation");

        InvitationBatch.QueuedInvitation fallback = InvitationBatch.describe("queqiao://enroll/%%%");
        require(InvitationBatch.FALLBACK_NAME.equals(fallback.name) && fallback.endpoint.isEmpty(),
                "an undecodable payload did not fall back to the neutral label");
        String notObject = "queqiao://enroll/"
                + base64Url("[1]".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        require(InvitationBatch.FALLBACK_NAME.equals(InvitationBatch.describe(notObject).name),
                "a payload that is not an object did not fall back to the neutral label");
    }

    private void testInvitationBatchSummariesAndTitles() {
        require("The clipboard holds no Queqiao invitation.".equals(InvitationBatch.pasteSummary(0, 0, 0)),
                "an empty paste was not named");
        require("Added 1 invitation.".equals(InvitationBatch.pasteSummary(1, 0, 0)),
                "a single invitation was not named");
        require("Added 9 invitations; 2 already queued; 1 could not be used."
                        .equals(InvitationBatch.pasteSummary(9, 2, 1)),
                "a mixed paste was not summarized in one sentence");
        require("Import profile".equals(InvitationBatch.importTitle(1, false)), "single import title changed");
        require("Import 3 profiles".equals(InvitationBatch.importTitle(3, false)), "batch import title changed");
        require("Resume import".equals(InvitationBatch.importTitle(1, true)), "resume title changed");
        require("Import 2 profiles".equals(InvitationBatch.importTitle(2, true)),
                "a draft plus a queued invitation was not counted as two");
    }

    private void testInvitationQueueSkipsDuplicates() throws Exception {
        InvitationBatch.Queue queue = new InvitationBatch.Queue();
        String first = testInvitation("provider-a", "198.51.100.1:12540", 6);
        String second = testInvitation("provider-c", "198.51.100.2:12540", 7);
        require(queue.add(java.util.Arrays.asList(first, second)) == 2, "new invitations were not queued");
        require(queue.add(java.util.Arrays.asList(second, first)) == 0, "a queued invitation was queued again");
        java.util.List<InvitationBatch.QueuedInvitation> items = queue.snapshot();
        require(items.size() == 2
                        && "provider-a".equals(items.get(0).name)
                        && "provider-c".equals(items.get(1).name),
                "the queue lost its order or its labels");
        require(queue.remove(first) && !queue.contains(first) && second.equals(queue.first().uri),
                "removing an entry did not leave the next one first");
    }

    private void requireInvalidProbe(String encoded) throws Exception {
        try {
            ConnectionProbe.available(encoded);
            throw new AssertionError("invalid connection-probe result was accepted: " + encoded);
        } catch (org.json.JSONException expected) {
            // Expected.
        }
    }

    private boolean isRouted(java.util.List<RoutePolicy.RouteSpec> routes, String address) throws Exception {
        for (RoutePolicy.RouteSpec route : routes) {
            if (route.contains(address)) {
                return true;
            }
        }
        return false;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static Bundle status(String name, int current, int total) {
        Bundle status = new Bundle();
        status.putString("class", SecureStoreTestRunner.class.getName());
        status.putString("test", name);
        status.putInt("current", current);
        status.putInt("numtests", total);
        return status;
    }

    @FunctionalInterface
    private interface Check {
        void run() throws Exception;
    }

    private static final class NamedCheck {
        private final String name;
        private final Check check;

        private NamedCheck(String name, Check check) {
            this.name = name;
            this.check = check;
        }
    }
}

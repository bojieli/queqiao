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
                new NamedCheck("invitationQueueSkipsDuplicates", this::testInvitationQueueSkipsDuplicates),
                new NamedCheck("importerResumesFirstAndDequeuesAfterSave", this::testImporterResumesFirstAndDequeuesAfterSave),
                new NamedCheck("importerNeverOverwritesADraft", this::testImporterNeverOverwritesADraft),
                new NamedCheck("importerPrepareFailureKeepsTheInvitation", this::testImporterPrepareFailureKeepsTheInvitation),
                new NamedCheck("importerCompleteFailureKeepsTheDraft", this::testImporterCompleteFailureKeepsTheDraft),
                new NamedCheck("importerRequiresADeviceName", this::testImporterRequiresADeviceName),
                new NamedCheck("importerProcessesOnlyItsSnapshot", this::testImporterProcessesOnlyItsSnapshot)
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
        require(InvitationBatch.extract("Your invitation is " + first + ".").equals(
                        java.util.Collections.singletonList(first)),
                "the full stop ending a sentence was kept as part of the invitation");
        require(InvitationBatch.extract("`" + first + "` and **" + second + "**").equals(
                        java.util.Arrays.asList(first, second)),
                "Markdown backticks or asterisks were kept as part of an invitation");
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
        require("The text holds no Queqiao invitation.".equals(InvitationBatch.typedTextError(0, 0, 0)),
                "typed text without an invitation was not named");
        require("The text holds no usable Queqiao invitation; 2 could not be used."
                        .equals(InvitationBatch.typedTextError(0, 0, 2)),
                "typed text with only unusable invitations was not named");
        require("Added 1 invitation; 1 could not be used.".equals(InvitationBatch.typedTextError(1, 0, 1)),
                "typed text with a mix did not say what was queued");
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

    /** An in-memory draft store that records what the import loop did, in order. */
    private static final class FakeStore implements InvitationImporter.Store {
        final java.util.List<String> events = new java.util.ArrayList<>();
        final InvitationBatch.Queue queue;
        String draft;

        FakeStore(InvitationBatch.Queue queue, String draft) {
            this.queue = queue;
            this.draft = draft;
        }

        @Override
        public boolean hasDraft() {
            return draft != null;
        }

        @Override
        public String draft() {
            return draft;
        }

        @Override
        public void saveDraft(String value) {
            // The invitation must still be queued when its draft is saved.
            String uri = value.substring("draft:".length(), value.lastIndexOf(':'));
            events.add("save " + uri + (queue.contains(uri) ? " queued" : " dequeued"));
            draft = value;
        }

        @Override
        public void discardDraft() {
            events.add("discard " + draft);
            draft = null;
        }

        @Override
        public void importProfile(String profile) {
            events.add("import " + profile);
        }
    }

    /** Stands in for the Go core; fails where told to. */
    private static final class FakeCore implements InvitationImporter.Core {
        final java.util.List<String> events;
        final InvitationBatch.Queue queue;
        String failPrepare;
        boolean failComplete;
        Runnable duringPrepare;

        FakeCore(FakeStore store) {
            this.events = store.events;
            this.queue = store.queue;
        }

        @Override
        public void validate(String invitation) {
        }

        @Override
        public String prepare(String invitation, String deviceName) throws Exception {
            events.add("prepare " + invitation);
            if (duringPrepare != null) {
                duringPrepare.run();
            }
            if (invitation.equals(failPrepare)) {
                throw new java.io.IOException("prepare failed");
            }
            return "draft:" + invitation + ":" + deviceName;
        }

        @Override
        public String complete(String draft) throws Exception {
            events.add("complete " + draft);
            if (failComplete) {
                throw new java.io.IOException("complete failed");
            }
            return "profile(" + draft + ")";
        }
    }

    private static final String INVITE_A = "queqiao://enroll/a";
    private static final String INVITE_B = "queqiao://enroll/b";
    private static final String INVITE_C = "queqiao://enroll/c";

    private static InvitationImporter.Result runImport(
            FakeStore store, FakeCore core, java.util.List<String> invitations, String name) {
        return InvitationImporter.enroll(store, core, store.queue, invitations, name, message -> { });
    }

    private void testImporterResumesFirstAndDequeuesAfterSave() {
        InvitationBatch.Queue queue = new InvitationBatch.Queue();
        queue.add(java.util.Arrays.asList(INVITE_A, INVITE_B));
        FakeStore store = new FakeStore(queue, "draft:old:Phone");
        FakeCore core = new FakeCore(store);
        InvitationImporter.Result result = runImport(store, core, java.util.Arrays.asList(INVITE_A, INVITE_B), "Phone");
        require(result.failure == null && result.imported == 3 && result.total == 3,
                "the batch did not import the draft and both invitations: " + result.failure);
        require(store.events.equals(java.util.Arrays.asList(
                        "complete draft:old:Phone", "import profile(draft:old:Phone)", "discard draft:old:Phone",
                        "prepare " + INVITE_A, "save " + INVITE_A + " queued",
                        "complete draft:" + INVITE_A + ":Phone",
                        "import profile(draft:" + INVITE_A + ":Phone)", "discard draft:" + INVITE_A + ":Phone",
                        "prepare " + INVITE_B, "save " + INVITE_B + " queued",
                        "complete draft:" + INVITE_B + ":Phone",
                        "import profile(draft:" + INVITE_B + ":Phone)", "discard draft:" + INVITE_B + ":Phone")),
                "the batch did not resume first and dequeue only after each save: " + store.events);
        require(queue.size() == 0, "an imported invitation stayed queued");
    }

    private void testImporterNeverOverwritesADraft() {
        // A draft that cannot be completed stops the batch before any
        // queued invitation is prepared over it.
        InvitationBatch.Queue queue = new InvitationBatch.Queue();
        queue.add(java.util.Collections.singletonList(INVITE_A));
        FakeStore store = new FakeStore(queue, "draft:old:Phone");
        FakeCore core = new FakeCore(store);
        core.failComplete = true;
        InvitationImporter.Result result = runImport(store, core, java.util.Collections.singletonList(INVITE_A), "Phone");
        require(result.failure != null && "draft:old:Phone".equals(store.draft),
                "a pending draft was replaced");
        require(!store.events.contains("prepare " + INVITE_A) && queue.contains(INVITE_A),
                "an invitation was prepared over a pending draft");

        // The per-invitation check reads the store, not the state at start:
        // a draft that appears mid-batch is refused, not overwritten.
        InvitationBatch.Queue fresh = new InvitationBatch.Queue();
        fresh.add(java.util.Collections.singletonList(INVITE_B));
        FakeStore racing = new FakeStore(fresh, "draft:other:Phone");
        FakeCore racingCore = new FakeCore(racing);
        InvitationImporter.Store appearing = new InvitationImporter.Store() {
            private boolean asked;

            @Override
            public boolean hasDraft() {
                boolean answer = asked && racing.hasDraft();
                asked = true;
                return answer;
            }

            @Override
            public String draft() {
                return racing.draft();
            }

            @Override
            public void saveDraft(String draft) {
                racing.saveDraft(draft);
            }

            @Override
            public void discardDraft() {
                racing.discardDraft();
            }

            @Override
            public void importProfile(String profile) {
                racing.importProfile(profile);
            }
        };
        InvitationImporter.Result guarded = InvitationImporter.enroll(
                appearing, racingCore, fresh, java.util.Collections.singletonList(INVITE_B), "Phone", message -> { });
        require(guarded.failure instanceof IllegalStateException
                        && InvitationImporter.PENDING_DRAFT.equals(guarded.failure.getMessage()),
                "a draft that appeared mid-batch was not refused: " + guarded.failure);
        require("draft:other:Phone".equals(racing.draft) && racing.events.isEmpty() && fresh.contains(INVITE_B),
                "a draft that appeared mid-batch was overwritten: " + racing.events);
    }

    private void testImporterPrepareFailureKeepsTheInvitation() {
        InvitationBatch.Queue queue = new InvitationBatch.Queue();
        queue.add(java.util.Arrays.asList(INVITE_A, INVITE_B));
        FakeStore store = new FakeStore(queue, null);
        FakeCore core = new FakeCore(store);
        core.failPrepare = INVITE_A;
        InvitationImporter.Result result = runImport(store, core, java.util.Arrays.asList(INVITE_A, INVITE_B), "Phone");
        require(result.failure != null && result.imported == 0, "a failed prepare was not reported");
        require(store.draft == null, "a failed prepare left a draft");
        require(queue.contains(INVITE_A) && queue.contains(INVITE_B),
                "a failed prepare dropped an invitation from the queue");
        require(!store.events.contains("prepare " + INVITE_B), "the batch went on after a failure");
    }

    private void testImporterCompleteFailureKeepsTheDraft() {
        InvitationBatch.Queue queue = new InvitationBatch.Queue();
        queue.add(java.util.Arrays.asList(INVITE_A, INVITE_B));
        FakeStore store = new FakeStore(queue, null);
        FakeCore core = new FakeCore(store);
        core.failComplete = true;
        InvitationImporter.Result result = runImport(store, core, java.util.Arrays.asList(INVITE_A, INVITE_B), "Phone");
        require(result.failure != null && result.imported == 0, "a failed exchange was not reported");
        require(("draft:" + INVITE_A + ":Phone").equals(store.draft), "the failed exchange's draft was lost");
        require(!queue.contains(INVITE_A) && queue.contains(INVITE_B),
                "the drafted invitation stayed queued, or the next one left the queue");

        core.failComplete = false;
        InvitationImporter.Result resumed = runImport(store, core, java.util.Collections.singletonList(INVITE_B), "Phone");
        require(resumed.failure == null && resumed.imported == 2 && store.draft == null && queue.size() == 0,
                "the next batch did not resume the draft and then import the rest");
    }

    private void testImporterRequiresADeviceName() {
        InvitationBatch.Queue queue = new InvitationBatch.Queue();
        queue.add(java.util.Collections.singletonList(INVITE_A));
        FakeStore store = new FakeStore(queue, "draft:old:Phone");
        FakeCore core = new FakeCore(store);
        InvitationImporter.Result result = runImport(store, core, java.util.Collections.singletonList(INVITE_A), " ");
        require(result.failure instanceof IllegalArgumentException
                        && InvitationImporter.NAME_REQUIRED.equals(result.failure.getMessage()),
                "a queued invitation was imported without a device name");
        require(store.events.isEmpty() && "draft:old:Phone".equals(store.draft) && queue.contains(INVITE_A),
                "the batch acted before refusing the missing name");

        InvitationImporter.Result resumeOnly = runImport(store, core, java.util.Collections.emptyList(), "");
        require(resumeOnly.failure == null && resumeOnly.imported == 1,
                "resuming a draft alone needlessly required a device name");
    }

    private void testImporterProcessesOnlyItsSnapshot() {
        InvitationBatch.Queue queue = new InvitationBatch.Queue();
        queue.add(java.util.Arrays.asList(INVITE_A, INVITE_B));
        FakeStore store = new FakeStore(queue, null);
        FakeCore core = new FakeCore(store);
        // A share arrives mid-batch, and the user removes B meanwhile.
        core.duringPrepare = () -> {
            queue.add(java.util.Collections.singletonList(INVITE_C));
            queue.remove(INVITE_B);
        };
        InvitationImporter.Result result = runImport(store, core, java.util.Arrays.asList(INVITE_A, INVITE_B), "Phone");
        require(result.failure == null && result.imported == 1 && result.total == 1,
                "the batch did not import exactly its remaining snapshot: " + result.imported + "/" + result.total);
        require(queue.snapshot().size() == 1 && INVITE_C.equals(queue.first().uri),
                "an invitation queued mid-batch was imported without a new tap");
        require(!store.events.contains("prepare " + INVITE_B), "a removed invitation was imported");
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

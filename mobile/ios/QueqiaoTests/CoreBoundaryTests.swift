import XCTest
import NetworkExtension
import Mobilecore
@testable import Queqiao

final class CoreBoundaryTests: XCTestCase {
    func testRejectsUnknownProfileFields() {
        let profile = "{\"version\":1,\"unknown\":true}"
        XCTAssertThrowsError(try MobileCore.validateProfile(profile))
    }

    func testRejectsMalformedInvitationWithoutNetworkAccess() {
        XCTAssertThrowsError(try MobileCore.validateInvitation("https://example.com/invite"))
    }

    func testCatalogNormalizesMissingSelectionAndDuplicateRecords() {
        let summary = ProfileSummary(
            version: 1,
            name: "Example",
            endpoint: "gateway.example:443",
            providerID: "provider",
            gatewayID: "gateway",
            accountID: "account",
            deviceID: "device",
            deviceName: "Phone",
            certificateExpiry: "2030-01-01T00:00:00Z"
        )
        let profile = StoredProfile(
            id: "first",
            secretAccount: "secret.first",
            displayName: "Example",
            summary: summary,
            routingMode: .bypassRules,
            bypassLocalNetworks: true,
            importedAt: "2026-08-18T00:00:00Z"
        )
        var catalog = ProfileCatalog(
            selectedProfileID: "missing",
            profiles: [profile, profile]
        )

        catalog.normalize()

        XCTAssertEqual(catalog.profiles, [profile])
        XCTAssertEqual(catalog.selectedProfileID, profile.id)
    }

    func testTunnelMetricsDecodeCoreWireFormat() throws {
        let data = Data(
            """
            {"version":1,"state":"running","packets":{},
             "transport":{"BytesUp":2048,"BytesDown":4096,"ActiveFlows":3}}
            """.utf8
        )

        XCTAssertEqual(
            try TunnelMetrics.decode(data),
            TunnelMetrics(bytesUp: 2_048, bytesDown: 4_096, activeFlows: 3)
        )
    }

    func testDiagnosticSanitizerRedactsInvitationsAndSecrets() {
        let message = "open queqiao://enroll/private-payload token=secret-value"
        let sanitized = DiagnosticStore.sanitize(message)

        XCTAssertFalse(sanitized.contains("private-payload"))
        XCTAssertFalse(sanitized.contains("secret-value"))
        XCTAssertTrue(sanitized.contains("queqiao://<redacted>"))
        XCTAssertTrue(sanitized.contains("token=<redacted>"))
    }

    func testProviderEndpointParsesSupportedAddressForms() throws {
        XCTAssertEqual(try ProviderEndpoint.host(from: "203.0.113.8:443"), "203.0.113.8")
        XCTAssertEqual(try ProviderEndpoint.host(from: "gateway.example:8443"), "gateway.example")
        XCTAssertEqual(try ProviderEndpoint.host(from: "[2001:db8::1]:443"), "2001:db8::1")
        XCTAssertEqual(try ProviderEndpoint.resolvedAddress(from: "203.0.113.8:443"), "203.0.113.8")
    }

    func testProviderEndpointRejectsMalformedAddresses() {
        XCTAssertThrowsError(try ProviderEndpoint.host(from: "missing-port"))
        XCTAssertThrowsError(try ProviderEndpoint.host(from: "2001:db8::1:443"))
        XCTAssertThrowsError(try ProviderEndpoint.host(from: "example.com:0"))
        XCTAssertThrowsError(try ProviderEndpoint.host(from: "example.com:70000"))
    }

    func testTunnelStartCompletionIsDeliveredOnlyOnce() {
        var deliveredErrors: [Error?] = []
        let completion = OneShotErrorCompletion { deliveredErrors.append($0) }

        XCTAssertTrue(completion.call(nil))
        XCTAssertFalse(completion.call(TestError.lateFailure))
        XCTAssertEqual(deliveredErrors.count, 1)
        XCTAssertNil(deliveredErrors[0])
    }

    func testTunnelStopCompletionIsDeliveredOnlyOnce() {
        var deliveryCount = 0
        let completion = OneShotVoidCompletion { deliveryCount += 1 }

        XCTAssertTrue(completion.call())
        XCTAssertFalse(completion.call())
        XCTAssertEqual(deliveryCount, 1)
    }

    func testProfileProbeResultDecodesValidatedWireFormat() throws {
        let result = try ProfileProbeResult.decode(
            "{\"version\":1,\"transport\":\"quic\",\"latency_ms\":87}"
        )

        XCTAssertEqual(
            result,
            ProfileProbeResult(version: 1, transport: "quic", latencyMilliseconds: 87)
        )
        XCTAssertThrowsError(
            try ProfileProbeResult.decode(
                "{\"version\":2,\"transport\":\"quic\",\"latency_ms\":87}"
            )
        )
        XCTAssertThrowsError(
            try ProfileProbeResult.decode(
                "{\"version\":1,\"transport\":\"unknown\",\"latency_ms\":87}"
            )
        )
        XCTAssertThrowsError(
            try ProfileProbeResult.decode(
                "{\"version\":1,\"transport\":\"tcp\",\"latency_ms\":0}"
            )
        )
    }

    func testInitialVPNStatusDoesNotCreateSyntheticInvalidTransition() {
        var tracker = VPNStatusTracker()

        let observation = tracker.observe(.disconnected)

        XCTAssertNil(observation.previousStatus)
        XCTAssertNil(observation.transitionDescription)
        XCTAssertFalse(observation.endedActiveEpisode)
    }

    func testVPNStatusTrackerRecognizesDisconnectAfterIntermediateState() {
        var tracker = VPNStatusTracker()
        _ = tracker.observe(.connecting)
        _ = tracker.observe(.connected)
        let intermediate = tracker.observe(.disconnecting)
        let terminal = tracker.observe(.disconnected)

        XCTAssertFalse(intermediate.endedActiveEpisode)
        XCTAssertTrue(terminal.endedActiveEpisode)
        XCTAssertEqual(
            terminal.transitionDescription,
            "VPN status changed from disconnecting to disconnected"
        )
    }

    func testVPNStatusTrackerTreatsInvalidConfigurationAsTerminal() {
        var tracker = VPNStatusTracker()
        _ = tracker.observe(.connected)

        XCTAssertTrue(tracker.observe(.invalid).endedActiveEpisode)
    }

    func testDisconnectRecoveryMarkerSurvivesModelRecreation() throws {
        let suiteName = "io.github.bojieli.queqiao.tests.\(UUID().uuidString)"
        let defaults = try XCTUnwrap(UserDefaults(suiteName: suiteName))
        defer { defaults.removePersistentDomain(forName: suiteName) }

        VPNDisconnectRecoveryMarker(defaults: defaults).markConnected()
        XCTAssertTrue(VPNDisconnectRecoveryMarker(defaults: defaults).needsDisconnectRecovery)

        VPNDisconnectRecoveryMarker(defaults: defaults).resolveDisconnect()
        XCTAssertFalse(VPNDisconnectRecoveryMarker(defaults: defaults).needsDisconnectRecovery)
    }

    func testVPNDiagnosticNamesCoverAppUpdateAndPluginFailure() {
        XCTAssertEqual(VPNDiagnostics.providerStopReasonName(rawValue: 16), "app update")
        XCTAssertEqual(
            VPNDiagnostics.disconnectErrorName(domain: NEVPNConnectionErrorDomain, code: 12),
            "VPN extension failed"
        )
        XCTAssertNil(VPNDiagnostics.disconnectErrorName(domain: "example.error", code: 12))
    }

    func testDiagnosticExporterProducesShareableText() {
        let entry = Queqiao.DiagnosticEntry(
            id: UUID(),
            timestamp: Date(timeIntervalSince1970: 0),
            level: .warning,
            component: "Packet tunnel",
            message: "Tunnel stopped: app update (iOS reason 16) token=private-value"
        )

        let exported = DiagnosticExporter.render([entry])

        XCTAssertTrue(exported.contains("warning Packet tunnel"))
        XCTAssertTrue(exported.contains("app update (iOS reason 16)"))
        XCTAssertFalse(exported.contains("private-value"))
    }
}

private enum TestError: Error {
    case lateFailure
}

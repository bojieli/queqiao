import XCTest
@testable import Queqiao

/// The bundled country set is a build artifact committed to the repository, so
/// these tests have two jobs: prove the parser is strict about a blob it does
/// not recognise, and prove the blob actually in the tree is one it does.
///
/// The second half matters more than it looks. A silently mis-parsed route set
/// does not crash — it takes the wrong addresses off the tunnel, which is the
/// one failure the whole exclusion path exists to avoid.
final class CountryRoutesTests: XCTestCase {
    // MARK: the packed form

    /// One packed IPv6 row. A struct rather than a tuple because the address is
    /// two words and a prefix length, which is one member past what reads well.
    private struct Row6 {
        let high: UInt64
        let low: UInt64
        let length: UInt8
    }

    private func makeBlob(
        magic: [UInt8] = [0x51, 0x51, 0x47, 0x4F],
        version: UInt8 = 1,
        ipv4: [(UInt32, UInt8)] = [],
        ipv6: [Row6] = [],
        truncatingBy: Int = 0
    ) -> Data {
        var bytes = magic
        bytes.append(version)
        bytes.append(contentsOf: [0, 0, 0]) // reserved byte and the count padding
        bytes.append(contentsOf: bigEndian32(UInt32(ipv4.count)))
        bytes.append(contentsOf: bigEndian32(UInt32(ipv6.count)))
        for (address, length) in ipv4 {
            bytes.append(contentsOf: bigEndian32(address))
            bytes.append(length)
        }
        for row in ipv6 {
            bytes.append(contentsOf: bigEndian64(row.high))
            bytes.append(contentsOf: bigEndian64(row.low))
            bytes.append(row.length)
        }
        return Data(bytes.prefix(bytes.count - truncatingBy))
    }

    private func bigEndian32(_ value: UInt32) -> [UInt8] {
        (0..<4).map { UInt8(truncatingIfNeeded: value >> (24 - 8 * UInt32($0))) }
    }

    private func bigEndian64(_ value: UInt64) -> [UInt8] {
        (0..<8).map { UInt8(truncatingIfNeeded: value >> (56 - 8 * UInt64($0))) }
    }

    func testDecodesBothFamiliesInTheOrderTheyArePacked() throws {
        let blob = makeBlob(
            ipv4: [(0x0A00_0000, 8), (0xCB00_7100, 24)],
            ipv6: [Row6(high: 0x2001_0250_0000_0000, low: 0, length: 30)]
        )
        XCTAssertEqual(
            try CountryRoutes.decode(blob).map(\.cidrText),
            ["10.0.0.0/8", "203.0.113.0/24", "2001:250::/30"]
        )
    }

    func testDecodesAnEmptySet() throws {
        XCTAssertTrue(try CountryRoutes.decode(makeBlob()).isEmpty)
    }

    func testHostBitsInTheResourceAreMaskedOffRatherThanTrusted() throws {
        // The generator never writes these, but a hand-edited or corrupted
        // resource must still produce a route that means what it says.
        let blob = makeBlob(ipv4: [(0x0A01_0203, 8)])
        XCTAssertEqual(try CountryRoutes.decode(blob).map(\.cidrText), ["10.0.0.0/8"])
    }

    func testRejectsABlobThatIsNotARouteSet() {
        for blob in [Data(), Data([0x51, 0x51]), makeBlob(magic: [0x51, 0x51, 0x47, 0x00])] {
            XCTAssertThrowsError(try CountryRoutes.decode(blob)) { error in
                guard case CountryRoutes.Failure.notARouteSet = error else {
                    return XCTFail("wrong failure: \(error)")
                }
            }
        }
    }

    func testRejectsAFormatItCannotRead() {
        XCTAssertThrowsError(try CountryRoutes.decode(makeBlob(version: 2))) { error in
            guard case CountryRoutes.Failure.unsupportedVersion(let version) = error else {
                return XCTFail("wrong failure: \(error)")
            }
            XCTAssertEqual(version, 2)
        }
    }

    func testRejectsATruncatedSetRatherThanReadingPartOfIt() {
        let blob = makeBlob(ipv4: [(0x0A00_0000, 8), (0xCB00_7100, 24)], truncatingBy: 3)
        XCTAssertThrowsError(try CountryRoutes.decode(blob)) { error in
            guard case CountryRoutes.Failure.truncated(let expected, let found) = error else {
                return XCTFail("wrong failure: \(error)")
            }
            XCTAssertEqual(expected, 26)
            XCTAssertEqual(found, 23)
        }
    }

    // MARK: the artifact in the repository

    /// The committed resource, read from the test bundle. project.yml copies it
    /// into QueqiaoTests as a resource for exactly this.
    private func shippedSet() throws -> [IPPrefix] {
        let bundle = Bundle(for: Self.self)
        let url = try XCTUnwrap(
            bundle.url(forResource: CountryRoutes.chinaResource, withExtension: "bin"),
            "cn-direct.bin is not in the test bundle; check the project.yml resource path"
        )
        return try CountryRoutes.decode(Data(contentsOf: url))
    }

    func testTheBundleLookupFindsTheResourceRatherThanFailing() throws {
        XCTAssertFalse(try CountryRoutes.chinaDirect(in: Bundle(for: Self.self)).isEmpty)
    }

    func testAMissingResourceIsReportedRatherThanCrashing() {
        // Bundle.main inside a unit-test host is the runner, which carries no
        // country set — the same shape of failure as a botched app build.
        XCTAssertThrowsError(try CountryRoutes.chinaDirect(in: Bundle(for: XCTestCase.self))) { error in
            guard case CountryRoutes.Failure.resourceMissing(let name) = error else {
                return XCTFail("wrong failure: \(error)")
            }
            XCTAssertEqual(name, CountryRoutes.chinaResource)
        }
    }

    /// The header count is what the routing screen shows. If it could drift
    /// from what the tunnel installs, the screen would be quoting a number the
    /// device does not honour.
    func testTheHeaderCountAgreesWithTheSetItDescribes() throws {
        let bundle = Bundle(for: Self.self)
        XCTAssertEqual(
            try CountryRoutes.blockCount(in: bundle),
            try CountryRoutes.chinaDirect(in: bundle).count
        )
    }

    func testTheHeaderCountRejectsWhatTheParserRejects() {
        XCTAssertThrowsError(try CountryRoutes.blockCount(in: Bundle(for: XCTestCase.self))) { error in
            guard case CountryRoutes.Failure.resourceMissing = error else {
                return XCTFail("wrong failure: \(error)")
            }
        }
    }

    func testTheShippedSetParsesToTheBlockCountsItWasGeneratedWith() throws {
        let prefixes = try shippedSet()
        XCTAssertEqual(prefixes.filter { $0.family == .ipv4 }.count, 5_493)
        XCTAssertEqual(prefixes.filter { $0.family == .ipv6 }.count, 2_014)
    }

    func testTheShippedSetIsAlreadySortedAndCollapsed() throws {
        let prefixes = try shippedSet()
        XCTAssertEqual(prefixes, prefixes.sorted())
        // The generator collapses so the extension does not have to. If this
        // ever fails the parse is doing work that belongs in the build.
        XCTAssertEqual(IPPrefix.coalesce(prefixes), prefixes)
    }

    func testTheShippedSetCoversDelegatedAddressesAndNothingElse() throws {
        let prefixes = try shippedSet()
        for address in ["114.114.114.114", "223.5.5.5", "202.106.0.20", "2400:3200::1"] {
            let host = try XCTUnwrap(IPPrefix(cidr: address))
            XCTAssertTrue(prefixes.contains { $0.contains(host) }, "\(address) is not in the set")
        }
        for address in ["8.8.8.8", "1.1.1.1", "2606:4700:4700::1111"] {
            let host = try XCTUnwrap(IPPrefix(cidr: address))
            XCTAssertFalse(prefixes.contains { $0.contains(host) }, "\(address) is in the set")
        }
    }

    func testTheShippedSetFitsTheRouteLimitWithRoomForAUserList() throws {
        let prefixes = try shippedSet()
        let userRoutes = (0..<StoredProfile.maximumBypassRoutes).map { "198.51.100.\($0)/32" }
        let plan = RoutePlan.make(userRoutes: userRoutes, builtIn: prefixes)

        XCTAssertEqual(plan.truncated, 0, plan.diagnosticSummary)
        XCTAssertTrue(plan.rejected.isEmpty)
        XCTAssertLessThanOrEqual(plan.excluded.count, RoutePlan.defaultLimit)
        XCTAssertEqual(plan.ipv4Routes.count + plan.ipv6Routes.count, plan.excluded.count)
    }

    func testTheLocalNetworkPolicyAndTheCountrySetCombineWithoutLosingEither() throws {
        let prefixes = try shippedSet()
        let plan = RoutePlan.make(userRoutes: RoutePlan.localNetworks, builtIn: prefixes)

        for address in ["192.168.1.1", "10.1.2.3", "fe80::1", "114.114.114.114", "2400:3200::1"] {
            let host = try XCTUnwrap(IPPrefix(cidr: address))
            XCTAssertTrue(plan.excluded.contains { $0.contains(host) }, "\(address) lost its bypass")
        }
        let routed = try XCTUnwrap(IPPrefix(cidr: "8.8.8.8"))
        XCTAssertFalse(plan.excluded.contains { $0.contains(routed) })
    }
}

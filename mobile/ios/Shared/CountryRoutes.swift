import Foundation

/// The bundled country route set, read from the packed resource that
/// scripts/generate_cn_geoip.py produces from registry delegation data.
///
/// Deliberately a function and not a cached property. The parsed set is a few
/// hundred kilobytes of prefixes, the packet-tunnel extension runs against a
/// fixed memory profile shared with the Go runtime, and it is needed exactly
/// once per connect — so it is built, handed to a RoutePlan, and dropped before
/// the core starts.
enum CountryRoutes {
    /// The resource shipped inside the packet-tunnel extension.
    static let chinaResource = "cn-direct"

    enum Failure: LocalizedError {
        case resourceMissing(String)
        case notARouteSet
        case unsupportedVersion(UInt8)
        case truncated(expected: Int, found: Int)

        var errorDescription: String? {
            switch self {
            case .resourceMissing(let name):
                return "The bundled route set \(name) is missing from this build."
            case .notARouteSet:
                return "The bundled route set does not carry the expected header."
            case .unsupportedVersion(let version):
                return "The bundled route set is format version \(version), which this build cannot read."
            case .truncated(let expected, let found):
                return "The bundled route set should be \(expected) bytes but is \(found)."
            }
        }
    }

    private static let magic: [UInt8] = [0x51, 0x51, 0x47, 0x4F] // "QQGO"
    private static let formatVersion: UInt8 = 1
    private static let headerSize = 16
    private static let ipv4EntrySize = 5
    private static let ipv6EntrySize = 17

    /// Prefixes the registry delegates to China, sorted and already collapsed
    /// by the generator so no coalescing is needed here.
    ///
    /// This is where the set is *allocated*, which the registry is explicit is
    /// not where it is used. The UI says so; nothing downstream should imply
    /// otherwise.
    static func chinaDirect(in bundle: Bundle = .main) throws -> [IPPrefix] {
        guard let url = bundle.url(forResource: chinaResource, withExtension: "bin") else {
            throw Failure.resourceMissing(chinaResource)
        }
        return try decode(Data(contentsOf: url, options: .mappedIfSafe))
    }

    /// The packed set as bytes, for handing to the core.
    ///
    /// The Go side reads the same file by the same rules and searches it in
    /// place; it needs the bytes rather than the prefixes because a GEOIP rule
    /// is consulted per flow and the parsed form is a quarter of a megabyte to
    /// keep resident. Returns nil rather than throwing: a build without the
    /// resource still runs every rule that is not GEOIP, and the caller records
    /// that it is missing.
    static func packedChinaSet(in bundle: Bundle = .main) -> Data? {
        guard let url = bundle.url(forResource: chinaResource, withExtension: "bin") else {
            return nil
        }
        return try? Data(contentsOf: url, options: .mappedIfSafe)
    }

    /// How many blocks the bundled set holds, without parsing it.
    ///
    /// The counts sit in the header, so a screen can say how heavy the toggle
    /// is — several thousand routes, not a handful — without building or
    /// holding the prefixes. A claim the UI cannot substantiate is worse than
    /// no claim, and this one costs sixteen bytes.
    static func blockCount(in bundle: Bundle = .main) throws -> Int {
        guard let url = bundle.url(forResource: chinaResource, withExtension: "bin") else {
            throw Failure.resourceMissing(chinaResource)
        }
        let mapped = try Data(contentsOf: url, options: .mappedIfSafe)
        let counted = try counts(in: [UInt8](mapped.prefix(headerSize)))
        return counted.ipv4 + counted.ipv6
    }

    /// Reads the packed form. The layout is fixed-width and little else: a
    /// 16-byte header, then 5 bytes per IPv4 block and 17 per IPv6 block, all
    /// big-endian. scripts/test_cn_geoip.py holds the other half of this
    /// contract.
    static func decode(_ data: Data) throws -> [IPPrefix] {
        let bytes = [UInt8](data)
        let (ipv4Count, ipv6Count) = try counts(in: bytes)
        let expected = headerSize + ipv4EntrySize * ipv4Count + ipv6EntrySize * ipv6Count
        guard bytes.count == expected else {
            throw Failure.truncated(expected: expected, found: bytes.count)
        }

        var prefixes: [IPPrefix] = []
        prefixes.reserveCapacity(ipv4Count + ipv6Count)
        var offset = headerSize
        for _ in 0..<ipv4Count {
            let address = readBigEndian32(bytes, at: offset)
            if let prefix = IPPrefix(ipv4: address, length: Int(bytes[offset + 4])) {
                prefixes.append(prefix)
            }
            offset += ipv4EntrySize
        }
        for _ in 0..<ipv6Count {
            let high = readBigEndian64(bytes, at: offset)
            let low = readBigEndian64(bytes, at: offset + 8)
            if let prefix = IPPrefix(ipv6: high, low: low, length: Int(bytes[offset + 16])) {
                prefixes.append(prefix)
            }
            offset += ipv6EntrySize
        }
        return prefixes
    }

    /// Validates the header and returns the two block counts it carries.
    private static func counts(in bytes: [UInt8]) throws -> (ipv4: Int, ipv6: Int) {
        guard bytes.count >= headerSize, Array(bytes[0..<4]) == magic else {
            throw Failure.notARouteSet
        }
        guard bytes[4] == formatVersion else { throw Failure.unsupportedVersion(bytes[4]) }
        return (Int(readBigEndian32(bytes, at: 8)), Int(readBigEndian32(bytes, at: 12)))
    }

    private static func readBigEndian32(_ bytes: [UInt8], at offset: Int) -> UInt32 {
        (UInt32(bytes[offset]) << 24)
            | (UInt32(bytes[offset + 1]) << 16)
            | (UInt32(bytes[offset + 2]) << 8)
            | UInt32(bytes[offset + 3])
    }

    private static func readBigEndian64(_ bytes: [UInt8], at offset: Int) -> UInt64 {
        var value: UInt64 = 0
        for index in 0..<8 { value = (value << 8) | UInt64(bytes[offset + index]) }
        return value
    }
}

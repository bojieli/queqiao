import Foundation

/// The shape of everything Queqiao persists about an enrolled profile.
///
/// Split out of ProfileStore so the persisted model and the Keychain-backed
/// accessor that reads it are two files: the model is what the packet-tunnel
/// extension decodes, and it is the half that grows every time a routing or
/// connection setting is added.

enum TrafficPolicy: String, Codable, CaseIterable, Identifiable, Sendable {
    case allTraffic = "all-traffic"
    case excludeLocalNetworks = "exclude-local-networks"

    var id: String { rawValue }

    var title: String {
        switch self {
        case .allTraffic:
            return "All traffic"
        case .excludeLocalNetworks:
            return "Exclude local networks"
        }
    }

    var detail: String {
        switch self {
        case .allTraffic:
            return "Route IPv4, IPv6, and DNS traffic through the selected Queqiao provider."
        case .excludeLocalNetworks:
            return "Keep private and link-local destinations outside the tunnel; " +
                "route internet and DNS traffic through Queqiao."
        }
    }
}

struct ProfileSummary: Codable, Equatable, Sendable {
    let version: Int
    let name: String
    let endpoint: String
    let providerID: String
    let gatewayID: String
    let accountID: String
    let deviceID: String
    let deviceName: String
    let certificateExpiry: String

    enum CodingKeys: String, CodingKey {
        case version
        case name
        case endpoint
        case providerID = "provider_id"
        case gatewayID = "gateway_id"
        case accountID = "account_id"
        case deviceID = "device_id"
        case deviceName = "device_name"
        case certificateExpiry = "certificate_expiry"
    }
}

struct StoredProfile: Codable, Identifiable, Equatable, Sendable {
    /// How many hand-entered bypass routes one profile may hold.
    ///
    /// The catalog is a single Keychain blob rewritten on every save, so the
    /// list has to be bounded somewhere. Anyone who wants more destinations
    /// than this off the tunnel wants a generated set, not a typed one.
    static let maximumBypassRoutes = 256

    let id: String
    let secretAccount: String
    var displayName: String
    var summary: ProfileSummary
    var trafficPolicy: TrafficPolicy
    /// Destinations kept off the tunnel, in canonical CIDR text. Sanitized by
    /// ProfileCatalog.normalize on every load and save, so a caller may hand
    /// this whatever the user typed.
    var bypassRoutes: [String] = []
    /// Whether the bundled registry set for China is added to the bypass list.
    /// Experimental, and address-based only: see CountryRoutes.
    var bypassChinaDirect = false
    /// The routing rule list, as the user typed or imported it, one rule per
    /// line in the `TYPE,VALUE,ACTION` syntax the lists in circulation use.
    ///
    /// Stored as text rather than as parsed rules on purpose. The text is what
    /// the user wrote and what they will edit next; parsing it here would mean
    /// this catalog owning a second copy of the grammar, and the core -- which
    /// is the only thing that acts on it -- already owns the first.
    var routingRules = ""
    /// Whether the tunnel may bring itself up without being asked.
    var onDemandEnabled = false
    /// Wi-Fi networks on which on-demand keeps the tunnel down. Typed by the
    /// user, never scanned. Sanitized by ProfileCatalog.normalize.
    var trustedNetworks: [String] = []
    var connectOnCellular = true
    let importedAt: String

    var onDemandPolicy: OnDemandPolicy {
        OnDemandPolicy(
            trustedNetworks: trustedNetworks,
            connectOnCellular: connectOnCellular,
            isEnabled: onDemandEnabled
        )
    }

    /// Splits a text field into candidate entries.
    ///
    /// Newlines, commas, semicolons and spaces all separate, because a list
    /// pasted out of a config file, a shell command, or another client arrives
    /// in all four forms and rejecting three of them teaches nothing.
    static func routeEntries(from text: String) -> [String] {
        text
            .split(whereSeparator: { $0.isNewline || $0 == "," || $0 == ";" || $0 == " " || $0 == "\t" })
            .map(String.init)
    }
}

extension StoredProfile {
    /// Decoded by hand because Swift's synthesized Decodable ignores property
    /// defaults: a catalog written before a field existed would fail to decode
    /// and take every enrolled profile on the device with it.
    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decode(String.self, forKey: .id)
        secretAccount = try container.decode(String.self, forKey: .secretAccount)
        displayName = try container.decode(String.self, forKey: .displayName)
        summary = try container.decode(ProfileSummary.self, forKey: .summary)
        trafficPolicy = try container.decode(TrafficPolicy.self, forKey: .trafficPolicy)
        bypassRoutes = try container.decodeIfPresent([String].self, forKey: .bypassRoutes) ?? []
        bypassChinaDirect = try container.decodeIfPresent(Bool.self, forKey: .bypassChinaDirect) ?? false
        routingRules = try container.decodeIfPresent(String.self, forKey: .routingRules) ?? ""
        onDemandEnabled = try container.decodeIfPresent(Bool.self, forKey: .onDemandEnabled) ?? false
        trustedNetworks = try container.decodeIfPresent([String].self, forKey: .trustedNetworks) ?? []
        connectOnCellular = try container.decodeIfPresent(Bool.self, forKey: .connectOnCellular) ?? true
        importedAt = try container.decode(String.self, forKey: .importedAt)
    }
}

struct ProfileCatalog: Codable, Equatable, Sendable {
    static let currentVersion = 1

    var version = currentVersion
    var selectedProfileID: String?
    var profiles: [StoredProfile] = []

    mutating func normalize() {
        var seen = Set<String>()
        profiles = profiles.filter { !$0.id.isEmpty && seen.insert($0.id).inserted }
        for index in profiles.indices {
            profiles[index].bypassRoutes = Self.sanitizedRoutes(profiles[index].bypassRoutes)
            profiles[index].trustedNetworks =
                OnDemandRules.sanitizedNetworks(profiles[index].trustedNetworks)
        }
        if let selectedProfileID, !profiles.contains(where: { $0.id == selectedProfileID }) {
            self.selectedProfileID = profiles.first?.id
        } else if selectedProfileID == nil {
            selectedProfileID = profiles.first?.id
        }
    }

    /// Canonical, deduplicated, and bounded. Entries that are not CIDR blocks
    /// are dropped here rather than at connect time, where a bad one would
    /// surface as a tunnel that failed to configure.
    static func sanitizedRoutes(_ entries: [String]) -> [String] {
        var seen = Set<String>()
        let canonical = IPPrefix.parseList(entries).parsed
            .map(\.cidrText)
            .filter { seen.insert($0).inserted }
        return Array(canonical.prefix(StoredProfile.maximumBypassRoutes))
    }
}

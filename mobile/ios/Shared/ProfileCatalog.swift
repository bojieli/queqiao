import Foundation

/// The shape of everything Queqiao persists about an enrolled profile.
///
/// Split out of ProfileStore so the persisted model and the Keychain-backed
/// accessor that reads it are two files: the model is what the packet-tunnel
/// extension decodes, and it is the half that grows every time a routing or
/// connection setting is added.

/// The routing setting Queqiao stored before routing rules existed.
///
/// Kept only so that an older catalog migrates on load and an older build can
/// still decode what this one writes — see `StoredProfile.migratedMode` and
/// `StoredProfile.legacyTrafficPolicy`. Nothing reads it to make a decision.
enum TrafficPolicy: String, Codable, Sendable {
    case allTraffic = "all-traffic"
    case excludeLocalNetworks = "exclude-local-networks"
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
    /// How much traffic the tunnel carries. Which destinations are carved out
    /// of it is decided by the three rule properties below, not by this.
    var routingMode: RoutingMode
    /// Private and link-local destinations kept off the tunnel. This was a
    /// traffic-policy case rather than a rule until it became clear it behaved
    /// like every other rule and only looked different.
    var bypassLocalNetworks = false
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

    /// The routing settings as one value. The plan builder and the settings
    /// screen both work from this rather than from the individual fields, so
    /// neither can apply a rule the other does not know about.
    var routing: RoutingConfiguration {
        get {
            RoutingConfiguration(
                mode: routingMode,
                bypassLocalNetworks: bypassLocalNetworks,
                bypassChinaDirect: bypassChinaDirect,
                customRoutes: bypassRoutes,
                rules: routingRules
            )
        }
        set {
            routingMode = newValue.mode
            bypassLocalNetworks = newValue.bypassLocalNetworks
            bypassChinaDirect = newValue.bypassChinaDirect
            bypassRoutes = newValue.customRoutes
            routingRules = newValue.rules
        }
    }

    /// What a build from before the routing rules would have stored. Written on
    /// every save so that downgrading still finds the field it decodes without
    /// a default, and never read back except by such a build. Derived rather
    /// than stored because two fields describing one setting is the bug this
    /// refactor exists to remove.
    var legacyTrafficPolicy: TrafficPolicy {
        routingMode == .bypassRules && bypassLocalNetworks ? .excludeLocalNetworks : .allTraffic
    }

    /// The mode a catalog written before the rules existed should load as.
    ///
    /// The old policy only ever decided whether local networks were excluded;
    /// a bypass list and the country set applied whatever it said. Anything
    /// that was carving traffic out of the tunnel therefore has to load as
    /// `bypassRules`, or upgrading would silently push a user's excluded
    /// destinations back through the tunnel.
    static func migratedMode(
        policy: TrafficPolicy,
        chinaDirect: Bool,
        customRoutes: [String]
    ) -> RoutingMode {
        policy == .excludeLocalNetworks || chinaDirect || !customRoutes.isEmpty
            ? .bypassRules
            : .allTraffic
    }

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
    /// Spelled out because `trafficPolicy` is no longer a stored property and
    /// the synthesized keys would not include it, while the catalog format
    /// still carries it for older builds.
    enum CodingKeys: String, CodingKey {
        case id
        case secretAccount
        case displayName
        case summary
        case trafficPolicy
        case routingMode
        case bypassLocalNetworks
        case bypassRoutes
        case bypassChinaDirect
        case routingRules
        case onDemandEnabled
        case trustedNetworks
        case connectOnCellular
        case importedAt
    }

    /// Encoded by hand for the same reason it is decoded by hand: the legacy
    /// policy field has to be written from the routing rules on every save.
    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(id, forKey: .id)
        try container.encode(secretAccount, forKey: .secretAccount)
        try container.encode(displayName, forKey: .displayName)
        try container.encode(summary, forKey: .summary)
        try container.encode(legacyTrafficPolicy, forKey: .trafficPolicy)
        try container.encode(routingMode, forKey: .routingMode)
        try container.encode(bypassLocalNetworks, forKey: .bypassLocalNetworks)
        try container.encode(bypassRoutes, forKey: .bypassRoutes)
        try container.encode(bypassChinaDirect, forKey: .bypassChinaDirect)
        try container.encode(routingRules, forKey: .routingRules)
        try container.encode(onDemandEnabled, forKey: .onDemandEnabled)
        try container.encode(trustedNetworks, forKey: .trustedNetworks)
        try container.encode(connectOnCellular, forKey: .connectOnCellular)
        try container.encode(importedAt, forKey: .importedAt)
    }

    /// Decoded by hand because Swift's synthesized Decodable ignores property
    /// defaults: a catalog written before a field existed would fail to decode
    /// and take every enrolled profile on the device with it.
    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decode(String.self, forKey: .id)
        secretAccount = try container.decode(String.self, forKey: .secretAccount)
        displayName = try container.decode(String.self, forKey: .displayName)
        summary = try container.decode(ProfileSummary.self, forKey: .summary)
        let legacyPolicy = try container.decode(TrafficPolicy.self, forKey: .trafficPolicy)
        bypassRoutes = try container.decodeIfPresent([String].self, forKey: .bypassRoutes) ?? []
        bypassChinaDirect = try container.decodeIfPresent(Bool.self, forKey: .bypassChinaDirect) ?? false
        routingRules = try container.decodeIfPresent(String.self, forKey: .routingRules) ?? ""
        bypassLocalNetworks = try container.decodeIfPresent(Bool.self, forKey: .bypassLocalNetworks)
            ?? (legacyPolicy == .excludeLocalNetworks)
        routingMode = try container.decodeIfPresent(RoutingMode.self, forKey: .routingMode)
            ?? Self.migratedMode(
                policy: legacyPolicy,
                chinaDirect: bypassChinaDirect,
                customRoutes: bypassRoutes
            )
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

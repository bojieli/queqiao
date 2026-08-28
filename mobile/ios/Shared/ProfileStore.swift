import Foundation

/// Reads and writes the enrolled profile catalog, which lives in the Keychain
/// access group the app and the packet-tunnel extension share. The shape it
/// stores is in ProfileCatalog.swift.
struct ProfileStore: Sendable {
    static let catalogAccount = "profile-catalog-v1"
    static let profileAccountPrefix = "client-profile-v1."

    private let keychain: KeychainStore

    init(keychain: KeychainStore) {
        self.keychain = keychain
    }

    init() throws {
        keychain = try KeychainStore()
    }

    func catalog() throws -> ProfileCatalog {
        if let encoded = try keychain.get(account: Self.catalogAccount) {
            var catalog = try decodeCatalog(encoded)
            let original = catalog
            catalog.normalize()
            if catalog != original {
                try save(catalog)
            }
            return catalog
        }
        return try migrateLegacyProfile()
    }

    @discardableResult
    func importProfile(_ profileJSON: String) throws -> StoredProfile {
        try MobileCore.validateProfile(profileJSON)
        let summary = try decodeSummary(profileJSON)
        var catalog = try catalog()
        if let index = catalog.profiles.firstIndex(where: { $0.summary.deviceID == summary.deviceID }) {
            let account = catalog.profiles[index].secretAccount
            try keychain.set(profileJSON, account: account)
            catalog.profiles[index].summary = summary
            catalog.selectedProfileID = catalog.profiles[index].id
            try save(catalog)
            return catalog.profiles[index]
        }

        let identifier = UUID().uuidString.lowercased()
        let account = Self.profileAccountPrefix + identifier
        let record = StoredProfile(
            id: identifier,
            secretAccount: account,
            displayName: summary.name,
            summary: summary,
            trafficPolicy: .allTraffic,
            importedAt: ISO8601DateFormatter().string(from: Date())
        )
        try keychain.set(profileJSON, account: account)
        do {
            catalog.profiles.append(record)
            catalog.selectedProfileID = identifier
            try save(catalog)
        } catch {
            try? keychain.delete(account: account)
            throw error
        }
        return record
    }

    func profile(id: String) throws -> (StoredProfile, String)? {
        let catalog = try catalog()
        guard let record = catalog.profiles.first(where: { $0.id == id }),
              let profile = try keychain.get(account: record.secretAccount) else {
            return nil
        }
        try MobileCore.validateProfile(profile)
        return (record, profile)
    }

    func selectedProfile() throws -> (StoredProfile, String)? {
        let catalog = try catalog()
        guard let identifier = catalog.selectedProfileID else { return nil }
        return try profile(id: identifier)
    }

    func select(id: String) throws {
        var catalog = try catalog()
        guard catalog.profiles.contains(where: { $0.id == id }) else {
            throw ProfileStoreError.profileNotFound
        }
        catalog.selectedProfileID = id
        try save(catalog)
    }

    func rename(id: String, to requestedName: String) throws {
        let name = requestedName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !name.isEmpty, name.count <= 80 else {
            throw ProfileStoreError.invalidDisplayName
        }
        var catalog = try catalog()
        guard let index = catalog.profiles.firstIndex(where: { $0.id == id }) else {
            throw ProfileStoreError.profileNotFound
        }
        catalog.profiles[index].displayName = name
        try save(catalog)
    }

    func setTrafficPolicy(_ policy: TrafficPolicy, for id: String) throws {
        var catalog = try catalog()
        guard let index = catalog.profiles.firstIndex(where: { $0.id == id }) else {
            throw ProfileStoreError.profileNotFound
        }
        catalog.profiles[index].trafficPolicy = policy
        try save(catalog)
    }

    /// Stores bypass routes for one profile. The entries are sanitized by
    /// save, so the caller may pass raw text split into candidates.
    func setBypassRoutes(_ routes: [String], for id: String) throws {
        var catalog = try catalog()
        guard let index = catalog.profiles.firstIndex(where: { $0.id == id }) else {
            throw ProfileStoreError.profileNotFound
        }
        catalog.profiles[index].bypassRoutes = routes
        try save(catalog)
    }

    func setBypassChinaDirect(_ enabled: Bool, for id: String) throws {
        var catalog = try catalog()
        guard let index = catalog.profiles.firstIndex(where: { $0.id == id }) else {
            throw ProfileStoreError.profileNotFound
        }
        catalog.profiles[index].bypassChinaDirect = enabled
        try save(catalog)
    }

    /// Stores the rule list as text, exactly as the user wrote it.
    ///
    /// It is not parsed or reordered on the way in. The core is what acts on
    /// these rules and what reports on them, and a store that quietly rewrote
    /// the text would hand the user back something they did not type while the
    /// tunnel ran something else again.
    func setRoutingRules(_ rules: String, for id: String) throws {
        var catalog = try catalog()
        guard let index = catalog.profiles.firstIndex(where: { $0.id == id }) else {
            throw ProfileStoreError.profileNotFound
        }
        catalog.profiles[index].routingRules = rules
        try save(catalog)
    }

    /// Writes the whole on-demand policy at once. Splitting it into three
    /// setters would let the catalog hold a half-applied policy between saves,
    /// and this one decides when the tunnel comes up on its own.
    func setOnDemandPolicy(_ policy: OnDemandPolicy, for id: String) throws {
        var catalog = try catalog()
        guard let index = catalog.profiles.firstIndex(where: { $0.id == id }) else {
            throw ProfileStoreError.profileNotFound
        }
        catalog.profiles[index].onDemandEnabled = policy.isEnabled
        catalog.profiles[index].trustedNetworks = policy.trustedNetworks
        catalog.profiles[index].connectOnCellular = policy.connectOnCellular
        try save(catalog)
    }

    func replaceProfile(_ profileJSON: String, id: String) throws {
        try MobileCore.validateProfile(profileJSON)
        let summary = try decodeSummary(profileJSON)
        var catalog = try catalog()
        guard let index = catalog.profiles.firstIndex(where: { $0.id == id }) else {
            throw ProfileStoreError.profileNotFound
        }
        guard catalog.profiles[index].summary.deviceID == summary.deviceID else {
            throw ProfileStoreError.identityChanged
        }
        try keychain.set(profileJSON, account: catalog.profiles[index].secretAccount)
        catalog.profiles[index].summary = summary
        try save(catalog)
    }

    func delete(id: String) throws {
        var catalog = try catalog()
        guard let index = catalog.profiles.firstIndex(where: { $0.id == id }) else {
            return
        }
        let removed = catalog.profiles.remove(at: index)
        if catalog.selectedProfileID == id {
            catalog.selectedProfileID = catalog.profiles.first?.id
        }
        try save(catalog)
        try keychain.delete(account: removed.secretAccount)
    }

    func hasEnrollmentDraft() throws -> Bool {
        try keychain.get(account: KeychainStore.enrollmentDraftAccount) != nil
    }

    func enrollmentDraft() throws -> String? {
        try keychain.get(account: KeychainStore.enrollmentDraftAccount)
    }

    func saveEnrollmentDraft(_ draft: String) throws {
        try keychain.set(draft, account: KeychainStore.enrollmentDraftAccount)
    }

    func discardEnrollmentDraft() throws {
        try keychain.delete(account: KeychainStore.enrollmentDraftAccount)
    }

    private func save(_ catalog: ProfileCatalog) throws {
        var normalized = catalog
        normalized.normalize()
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let data = try encoder.encode(normalized)
        guard let encoded = String(data: data, encoding: .utf8) else {
            throw ProfileStoreError.invalidCatalog
        }
        try keychain.set(encoded, account: Self.catalogAccount)
    }

    private func decodeCatalog(_ encoded: String) throws -> ProfileCatalog {
        guard let data = encoded.data(using: .utf8) else {
            throw ProfileStoreError.invalidCatalog
        }
        let catalog = try JSONDecoder().decode(ProfileCatalog.self, from: data)
        guard catalog.version == ProfileCatalog.currentVersion else {
            throw ProfileStoreError.unsupportedCatalogVersion
        }
        return catalog
    }

    private func decodeSummary(_ profileJSON: String) throws -> ProfileSummary {
        let encoded = try MobileCore.profileSummary(profileJSON)
        guard let data = encoded.data(using: .utf8) else {
            throw ProfileStoreError.invalidSummary
        }
        return try JSONDecoder().decode(ProfileSummary.self, from: data)
    }

    private func migrateLegacyProfile() throws -> ProfileCatalog {
        var catalog = ProfileCatalog()
        guard let legacy = try keychain.get(account: KeychainStore.profileAccount) else {
            try save(catalog)
            return catalog
        }
        try MobileCore.validateProfile(legacy)
        let summary = try decodeSummary(legacy)
        let identifier = UUID().uuidString.lowercased()
        let account = Self.profileAccountPrefix + identifier
        let record = StoredProfile(
            id: identifier,
            secretAccount: account,
            displayName: summary.name,
            summary: summary,
            trafficPolicy: .allTraffic,
            importedAt: ISO8601DateFormatter().string(from: Date())
        )
        try keychain.set(legacy, account: account)
        catalog.profiles = [record]
        catalog.selectedProfileID = identifier
        do {
            try save(catalog)
        } catch {
            try? keychain.delete(account: account)
            throw error
        }
        // The catalog is now authoritative. A failed legacy cleanup may leave
        // an unreachable duplicate, but must never invalidate the migrated profile.
        try? keychain.delete(account: KeychainStore.profileAccount)
        return catalog
    }
}

enum ProfileStoreError: LocalizedError {
    case profileNotFound
    case invalidDisplayName
    case identityChanged
    case invalidCatalog
    case unsupportedCatalogVersion
    case invalidSummary

    var errorDescription: String? {
        switch self {
        case .profileNotFound:
            return "The selected Queqiao profile no longer exists."
        case .invalidDisplayName:
            return "Profile names must contain between 1 and 80 characters."
        case .identityChanged:
            return "A renewed profile attempted to change the enrolled device identity."
        case .invalidCatalog:
            return "The encrypted profile catalog is not valid UTF-8."
        case .unsupportedCatalogVersion:
            return "This profile catalog was written by an unsupported Queqiao version."
        case .invalidSummary:
            return "The Queqiao core returned an invalid profile summary."
        }
    }
}

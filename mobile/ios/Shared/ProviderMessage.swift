import Foundation

/// What the app can ask the running packet tunnel to do.
///
/// The channel existed before this type did, but it carried one unread word:
/// the provider answered every message with metrics regardless of content. A
/// second request had to be tellable from the first before routing could be
/// changed without disconnecting.
enum ProviderRequest: String, Sendable {
    /// Answer with the metrics JSON.
    case metrics
    /// Re-read the active profile's routing rules and install them on the live
    /// interface. The rules themselves are not carried in the message: the
    /// Keychain catalog is the one source both sides already read, and copying
    /// them into the payload would create a second one that could disagree.
    case reloadRouting = "reload-routing"

    var payload: Data { Data(rawValue.utf8) }

    init?(payload: Data) {
        guard let text = String(data: payload, encoding: .utf8),
              let request = ProviderRequest(rawValue: text) else {
            return nil
        }
        self = request
    }
}

/// The provider's answer to `reloadRouting`.
///
/// Carries the failure text rather than just a flag because the settings screen
/// has to say why a change did not reach the tunnel — a rule the user believes
/// is applied but is not is the failure this whole path exists to avoid.
struct RoutingReloadResult: Codable, Equatable, Sendable {
    var applied: Bool
    var excludedRouteCount: Int = 0
    var failure: String?

    static func succeeded(excludedRouteCount: Int) -> RoutingReloadResult {
        RoutingReloadResult(applied: true, excludedRouteCount: excludedRouteCount)
    }

    static func failed(_ failure: String) -> RoutingReloadResult {
        RoutingReloadResult(applied: false, failure: failure)
    }

    var payload: Data { (try? JSONEncoder().encode(self)) ?? Data() }

    init(applied: Bool, excludedRouteCount: Int = 0, failure: String? = nil) {
        self.applied = applied
        self.excludedRouteCount = excludedRouteCount
        self.failure = failure
    }

    init(payload: Data) throws {
        self = try JSONDecoder().decode(RoutingReloadResult.self, from: payload)
    }
}

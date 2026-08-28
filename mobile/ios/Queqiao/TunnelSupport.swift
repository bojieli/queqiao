import Foundation

final class NotificationToken: @unchecked Sendable {
    private let token: NSObjectProtocol

    init(_ token: NSObjectProtocol) {
        self.token = token
    }

    deinit {
        NotificationCenter.default.removeObserver(token)
    }
}

final class TimerToken: @unchecked Sendable {
    private let timer: Timer

    init(_ timer: Timer) {
        self.timer = timer
    }

    func invalidate() {
        timer.invalidate()
    }

    deinit {
        timer.invalidate()
    }
}

struct PresentedError: Identifiable {
    let id = UUID()
    let title: String
    let message: String
}

enum ModelError: LocalizedError {
    case missingProfile
    case emptyCoreResult
    case invalidPacketTunnelIdentifier
    case disconnectBeforeEditing
    case emptyProviderResponse
    case routingNotApplied(String)
    case invalidBypassRoutes([String])

    var errorDescription: String? {
        switch self {
        case .missingProfile:
            return "Import and select a Queqiao profile before connecting."
        case .emptyCoreResult:
            return "The Queqiao core returned an empty result."
        case .invalidPacketTunnelIdentifier:
            return "The packet-tunnel bundle identifier is not configured."
        case .disconnectBeforeEditing:
            return "Disconnect the VPN before changing which profile it uses."
        case .emptyProviderResponse:
            return "The packet-tunnel extension returned no response."
        case .routingNotApplied(let reason):
            return "The routing change was saved, but the running tunnel is still using "
                + "the previous rules: \(reason)"
        case .invalidBypassRoutes(let entries):
            let listed = entries.prefix(5).joined(separator: ", ")
            let suffix = entries.count > 5 ? " and \(entries.count - 5) more" : ""
            return "Not a network address or CIDR block: \(listed)\(suffix)."
        }
    }
}

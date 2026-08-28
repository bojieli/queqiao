import Foundation

/// The failures the packet tunnel reports to iOS and to the diagnostic ring.
///
/// Split out of PacketTunnelProvider so that file stays about the provider's
/// lifecycle rather than about its vocabulary of errors.
enum TunnelError: LocalizedError {
    case missingProfile
    case missingProfileSelection
    case coreStopped
    case startCancelled

    var errorDescription: String? {
        switch self {
        case .missingProfile:
            return "No enrolled Queqiao device identity is available."
        case .missingProfileSelection:
            return "The VPN configuration does not identify a Queqiao profile. Open the app and connect again."
        case .coreStopped:
            return "The Queqiao packet engine stopped unexpectedly."
        case .startCancelled:
            return "Tunnel startup was cancelled."
        }
    }
}

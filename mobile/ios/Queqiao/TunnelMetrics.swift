import Foundation

struct TunnelMetrics: Equatable, Sendable {
    static let empty = TunnelMetrics(bytesUp: 0, bytesDown: 0, activeFlows: 0)

    let bytesUp: UInt64
    let bytesDown: UInt64
    let activeFlows: Int64

    static func decode(_ data: Data) throws -> TunnelMetrics {
        let root = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        guard let transport = root?["transport"] as? [String: Any] else {
            throw ModelError.emptyProviderResponse
        }
        return TunnelMetrics(
            bytesUp: (transport["BytesUp"] as? NSNumber)?.uint64Value ?? 0,
            bytesDown: (transport["BytesDown"] as? NSNumber)?.uint64Value ?? 0,
            activeFlows: (transport["ActiveFlows"] as? NSNumber)?.int64Value ?? 0
        )
    }
}

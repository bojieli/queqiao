import Foundation
import Mobilecore

struct PacketTunnelResources: @unchecked Sendable {
    let session: MobilecoreSession?
    let bridge: PacketFlowBridge?
    let startCompletion: OneShotErrorCompletion?
}

struct StartupAttempt: Sendable {
    let generation: UInt64
    let startedAt: UInt64

    var elapsedMilliseconds: UInt64 {
        let now = DispatchTime.now().uptimeNanoseconds
        guard now >= startedAt else { return 0 }
        return (now - startedAt) / 1_000_000
    }
}

/// Serializes state shared by NetworkExtension callbacks and Go callbacks.
final class PacketTunnelLifecycle: @unchecked Sendable {
    private let lock = NSLock()
    private var generation: UInt64 = 0
    private var stopping = false
    private var session: MobilecoreSession?
    private var bridge: PacketFlowBridge?
    private var profileID: String?
    /// Kept so a routing change applied while connected can rebuild the
    /// interface settings without resolving the provider endpoint again. A
    /// second resolution could return a different address, which would move the
    /// tunnel to another gateway as a side effect of editing a bypass rule.
    private var remoteAddress: String?
    private var startCompletion: OneShotErrorCompletion?

    func beginStartup(completion: OneShotErrorCompletion) -> StartupAttempt {
        lock.lock()
        defer { lock.unlock() }
        stopping = false
        generation &+= 1
        startCompletion = completion
        return StartupAttempt(
            generation: generation,
            startedAt: DispatchTime.now().uptimeNanoseconds
        )
    }

    func selectProfile(_ id: String, for startup: StartupAttempt) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        guard !stopping, startup.generation == generation else { return false }
        profileID = id
        return true
    }

    func recordRemoteAddress(_ address: String, for startup: StartupAttempt) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        guard !stopping, startup.generation == generation else { return false }
        remoteAddress = address
        return true
    }

    func isActive(_ startup: StartupAttempt) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        return !stopping && startup.generation == generation
    }

    func install(
        session newSession: MobilecoreSession,
        bridge newBridge: PacketFlowBridge,
        for startup: StartupAttempt
    ) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        guard !stopping, startup.generation == generation else { return false }
        session = newSession
        bridge = newBridge
        return true
    }

    func invalidate(_ startup: StartupAttempt) {
        lock.lock()
        if startup.generation == generation {
            generation &+= 1
            profileID = nil
            remoteAddress = nil
            startCompletion = nil
        }
        lock.unlock()
    }

    func finish(_ startup: StartupAttempt) {
        lock.lock()
        if startup.generation == generation {
            startCompletion = nil
        }
        lock.unlock()
    }

    func beginStop() -> PacketTunnelResources {
        lock.lock()
        defer { lock.unlock() }
        stopping = true
        generation &+= 1
        let resources = PacketTunnelResources(
            session: session,
            bridge: bridge,
            startCompletion: startCompletion
        )
        session = nil
        bridge = nil
        profileID = nil
        remoteAddress = nil
        startCompletion = nil
        return resources
    }

    var currentSession: MobilecoreSession? {
        lock.lock()
        defer { lock.unlock() }
        return session
    }

    var activeProfileID: String? {
        lock.lock()
        defer { lock.unlock() }
        return profileID
    }

    /// The profile and endpoint a live routing change has to rebuild against,
    /// read together so the pair cannot be torn by a concurrent stop.
    var activeRouting: (profileID: String, remoteAddress: String)? {
        lock.lock()
        defer { lock.unlock() }
        guard !stopping, let profileID, let remoteAddress else { return nil }
        return (profileID, remoteAddress)
    }

    var isStopping: Bool {
        lock.lock()
        defer { lock.unlock() }
        return stopping
    }
}

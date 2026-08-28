import Foundation
import NetworkExtension
import OSLog
import Mobilecore

final class PacketTunnelProvider: NEPacketTunnelProvider, MobilecoreObserverProtocol, @unchecked Sendable {
    private let logger = Logger(subsystem: "io.github.bojieli.queqiao", category: "packet-tunnel")
    private let engineQueue = DispatchQueue(
        label: "io.github.bojieli.queqiao.packet-engine",
        qos: .userInitiated
    )
    private let lifecycle = PacketTunnelLifecycle()

    override func startTunnel(
        options: [String: NSObject]?,
        completionHandler: @escaping (Error?) -> Void
    ) {
        let completion = OneShotErrorCompletion(completionHandler)
        let startup = lifecycle.beginStartup(completion: completion)
        do {
            guard let configuration = protocolConfiguration as? NETunnelProviderProtocol,
                  let profileID = configuration.providerConfiguration?["profileID"] as? String,
                  !profileID.isEmpty else {
                throw TunnelError.missingProfileSelection
            }
            let store = try ProfileStore()
            guard let (record, profile) = try store.profile(id: profileID) else {
                throw TunnelError.missingProfile
            }
            try MobileCore.validateProfile(profile)
            guard lifecycle.selectProfile(profileID, for: startup) else {
                throw TunnelError.startCancelled
            }
            // The stored record is the only routing source. It used to be read
            // from the saved provider configuration first, which meant a change
            // saved while connected was invisible until the configuration was
            // rewritten — and the two could disagree in the meantime.
            recordDiagnostic(
                level: .info,
                "Starting profile \(record.displayName) with \(record.routingMode.title.lowercased())"
            )
            resolveAndConfigureTunnel(
                endpoint: record.summary.endpoint,
                profile: profile,
                routing: record.routing,
                startup: startup,
                completion: completion
            )
        } catch {
            recordDiagnostic(level: .error, "Tunnel startup failed: \(error.localizedDescription)")
            completion.call(error)
            lifecycle.invalidate(startup)
        }
    }

    override func stopTunnel(
        with reason: NEProviderStopReason,
        completionHandler: @escaping () -> Void
    ) {
        let completion = OneShotVoidCompletion(completionHandler)
        let resources = lifecycle.beginStop()
        resources.startCompletion?.call(TunnelError.startCancelled)
        recordDiagnostic(
            level: .info,
            "Tunnel stopped: \(reason.diagnosticName) (iOS reason \(reason.rawValue))"
        )
        engineQueue.async { [self] in
            resources.bridge?.close()
            do {
                try resources.session?.stopChecked()
            } catch {
                let detail = DiagnosticStore.sanitize(error.localizedDescription)
                logger.error("Tunnel stop reported an error: \(detail, privacy: .private)")
            }
            completion.call()
        }
    }

    override func sleep(completionHandler: @escaping () -> Void) {
        recordDiagnostic(level: .info, "Device sleeping; tunnel remains configured")
        completionHandler()
    }

    override func wake() {
        recordDiagnostic(level: .info, "Device woke; tunnel provider resumed")
    }

    override func handleAppMessage(_ messageData: Data, completionHandler: ((Data?) -> Void)? = nil) {
        guard let request = ProviderRequest(payload: messageData) else {
            completionHandler?(nil)
            return
        }
        switch request {
        case .metrics:
            let metrics = lifecycle.currentSession?.metricsJSON()
                ?? "{\"version\":2,\"state\":\"stopped\"}"
            completionHandler?(metrics.data(using: .utf8))
        case .reloadRouting:
            reloadRouting(completionHandler: completionHandler)
        }
    }

    func onStateChanged(_ state: String?) {
        guard let state else { return }
        logger.info("Tunnel state: \(state, privacy: .public)")
        recordDiagnostic(level: .info, "Packet engine state: \(state)")
        if state == MobilecoreStateFailed && !lifecycle.isStopping {
            recordDiagnostic(level: .error, "Packet engine entered the failed state")
            cancelTunnelWithError(TunnelError.coreStopped)
        }
    }

    func onLog(_ level: String?, message: String?) {
        guard let message else { return }
        let normalizedLevel = level?.uppercased()
        // Core failures can contain provider addresses or credential-shaped
        // values supplied by lower layers. Keep useful text in the encrypted
        // diagnostic ring, but never publish the raw interpolation to OSLog.
        let sanitized = DiagnosticStore.sanitize(message)
        if normalizedLevel == "ERROR" {
            logger.error("\(sanitized, privacy: .private)")
            recordDiagnostic(level: .error, sanitized)
        } else if normalizedLevel == "WARN" || normalizedLevel == "WARNING" {
            logger.warning("\(sanitized, privacy: .private)")
            recordDiagnostic(level: .warning, sanitized)
        } else {
            logger.info("\(sanitized, privacy: .private)")
        }
    }

    private func recordDiagnostic(level: DiagnosticLevel, _ message: String) {
        try? DiagnosticStore().append(level: level, component: "Packet tunnel", message: message)
    }

    private func elapsedMilliseconds(since start: UInt64) -> UInt64 {
        let now = DispatchTime.now().uptimeNanoseconds
        return now >= start ? (now - start) / 1_000_000 : 0
    }

    private func configureTunnel(
        profile: String,
        routing: RoutingConfiguration,
        remoteAddress: String,
        startup: StartupAttempt,
        completion: OneShotErrorCompletion
    ) {
        let planStartedAt = DispatchTime.now().uptimeNanoseconds
        let plan = routePlan(for: routing)
        let settings = TunnelNetworkSettings.make(plan: plan, remoteAddress: remoteAddress)
        recordDiagnostic(
            level: .info,
            "Route plan: \(plan.diagnosticSummary), built in \(elapsedMilliseconds(since: planStartedAt)) ms"
        )
        if plan.excludesDefaultRoute {
            // Not an error — someone may want exactly this while testing — but
            // it means the tunnel connects and then carries nothing, which is
            // indistinguishable from a broken gateway unless it is said here.
            recordDiagnostic(
                level: .warning,
                "A bypass route covers an entire address family, so that traffic will not use the tunnel"
            )
        }
        // Installing several thousand exclusions is the one part of startup that
        // scales with configuration, and docs/RELEASE-CHECKLIST.md gates the
        // bundled country set on measuring it. Timing it here means the number
        // comes out of a device's own diagnostics rather than out of Instruments.
        let settingsStartedAt = DispatchTime.now().uptimeNanoseconds
        setTunnelNetworkSettings(settings) { [self] error in
            let settingsMilliseconds = elapsedMilliseconds(since: settingsStartedAt)
            if let error {
                recordDiagnostic(
                    level: .error,
                    "Could not configure the iOS tunnel: \(error.localizedDescription)"
                )
                completion.call(error)
                lifecycle.invalidate(startup)
                return
            }
            recordDiagnostic(
                level: .info,
                "Applied \(plan.excluded.count) bypass routes in \(settingsMilliseconds) ms"
            )
            // Returning from Apple's settings callback promptly is important:
            // cold initialization of the statically linked Go/gVisor runtime
            // must not block NetworkExtension's internal callback queue.
            engineQueue.async { [self] in
                startPacketEngine(profile: profile, routing: routing, startup: startup, completion: completion)
            }
        }
    }

    private func resolveAndConfigureTunnel(
        endpoint: String,
        profile: String,
        routing: RoutingConfiguration,
        startup: StartupAttempt,
        completion: OneShotErrorCompletion
    ) {
        engineQueue.async { [self] in
            do {
                guard lifecycle.isActive(startup) else {
                    completion.call(TunnelError.startCancelled)
                    return
                }
                let remoteAddress = try ProviderEndpoint.resolvedAddress(from: endpoint)
                recordDiagnostic(level: .info, "Provider endpoint resolved to \(remoteAddress)")
                guard lifecycle.recordRemoteAddress(remoteAddress, for: startup) else {
                    completion.call(TunnelError.startCancelled)
                    return
                }
                configureTunnel(
                    profile: profile,
                    routing: routing,
                    remoteAddress: remoteAddress,
                    startup: startup,
                    completion: completion
                )
            } catch {
                recordDiagnostic(level: .error, "Provider resolution failed: \(error.localizedDescription)")
                completion.call(error)
                lifecycle.invalidate(startup)
            }
        }
    }

    private func startPacketEngine(
        profile: String,
        routing: RoutingConfiguration,
        startup: StartupAttempt,
        completion: OneShotErrorCompletion
    ) {
        guard lifecycle.isActive(startup) else {
            completion.call(TunnelError.startCancelled)
            return
        }
        recordDiagnostic(level: .info, "iOS tunnel interface configured; initializing packet engine")
        let packetBridge = PacketFlowBridge(packetFlow: packetFlow)
        do {
            guard let newSession = MobilecoreNewSession(self, nil) else {
                throw TunnelError.coreStopped
            }
            installRouting(on: newSession, routing: routing)
            packetBridge.start()
            try newSession.startChecked(
                profile: profile,
                packetIO: packetBridge,
                mtu: TunnelNetworkSettings.mtu
            )
            guard lifecycle.install(
                session: newSession,
                bridge: packetBridge,
                for: startup
            ) else {
                packetBridge.close()
                try? newSession.stopChecked()
                completion.call(TunnelError.startCancelled)
                return
            }
            if completion.call(nil) {
                lifecycle.finish(startup)
                recordDiagnostic(
                    level: .info,
                    "Tunnel ready in \(startup.elapsedMilliseconds) ms"
                )
            }
        } catch {
            packetBridge.close()
            recordDiagnostic(level: .error, "Packet engine startup failed: \(error.localizedDescription)")
            completion.call(error)
            lifecycle.invalidate(startup)
        }
    }

    func onProfileUpdated(_ profileJSON: String?) -> Bool {
        let profileID = lifecycle.activeProfileID
        guard let profileJSON, let profileID else { return false }
        do {
            try MobileCore.validateProfile(profileJSON)
            try ProfileStore().replaceProfile(profileJSON, id: profileID)
            return true
        } catch {
            logger.error("Could not persist renewed device identity: \(error.localizedDescription, privacy: .public)")
            return false
        }
    }

}

private extension PacketTunnelProvider {
    /// Installs the profile's current routing rules on the running interface.
    ///
    /// iOS allows the interface description to be replaced while the tunnel is
    /// up, which is what makes a bypass rule editable without disconnecting.
    /// The packet engine and its uplink are deliberately left running, so the
    /// tunnel is not renegotiated for a routing edit. iOS still re-plumbs the
    /// interface's routes, so a connection can be disturbed by the change —
    /// that is a far smaller interruption than the disconnect this replaces,
    /// but it is not nothing.
    func reloadRouting(completionHandler: ((Data?) -> Void)?) {
        guard let active = lifecycle.activeRouting else {
            completionHandler?(RoutingReloadResult.failed("The tunnel is not running.").payload)
            return
        }
        engineQueue.async { [self] in
            do {
                guard let (record, _) = try ProfileStore().profile(id: active.profileID) else {
                    throw TunnelError.missingProfile
                }
                let plan = routePlan(for: record.routing)
                let settings = TunnelNetworkSettings.make(
                    plan: plan,
                    remoteAddress: active.remoteAddress
                )
                setTunnelNetworkSettings(settings) { [self] error in
                    if let error {
                        let detail = error.localizedDescription
                        recordDiagnostic(level: .error, "Could not apply new routing: \(detail)")
                        completionHandler?(RoutingReloadResult.failed(detail).payload)
                        return
                    }
                        if let session = lifecycle.currentSession {
                        installRouting(on: session, routing: record.routing)
                    }
                    recordDiagnostic(
                        level: .info,
                        "Routing changed while connected: \(plan.diagnosticSummary)"
                    )
                    completionHandler?(
                        RoutingReloadResult.succeeded(excludedRouteCount: plan.excluded.count).payload
                    )
                }
            } catch {
                let detail = error.localizedDescription
                recordDiagnostic(level: .error, "Could not read new routing rules: \(detail)")
                completionHandler?(RoutingReloadResult.failed(detail).payload)
            }
        }
    }

    /// The set of destinations that stay off the tunnel for this profile.
    ///
    /// How the mode and the rules combine lives in RoutePlan, so the settings
    /// screen previews exactly what this installs rather than reimplementing
    /// it. What stays here is the part the screen cannot do: reading the
    /// bundled set off disk, and saying so when it cannot be read.
    func routePlan(for routing: RoutingConfiguration) -> RoutePlan {
        var chinaDirect: [IPPrefix] = []
        if routing.bypassChinaDirect {
            do {
                chinaDirect = try CountryRoutes.chinaDirect()
            } catch {
                // A missing or unreadable set is worth saying out loud, but it
                // is not worth refusing to connect over: the tunnel still
                // carries everything, which is the safe direction to fail in.
                recordDiagnostic(
                    level: .error,
                    "Bundled China route set unavailable: \(error.localizedDescription)"
                )
            }
        }
        return RoutePlan.make(for: routing, chinaDirect: chinaDirect)
    }
}

/// Installing routing is a self-contained step rather than part of the
/// provider's lifecycle, and the provider's own body is at the length the lint
/// configuration allows. Keeping it here also keeps the two failure paths --
/// a country set that will not load, a rule list with bad lines -- beside each
/// other, since both are reported and neither stops the tunnel.
private extension PacketTunnelProvider {
    /// Hands the core the rule list and the country set behind any GEOIP rule,
    /// before anything is carried.
    ///
    /// Both are reported rather than enforced. A rule list with bad lines still
    /// loads the good ones and says which failed; a country set that will not
    /// load leaves GEOIP rules deciding nothing. Neither stops the tunnel: a
    /// user whose rule file has a typo wants their connection, and a diagnostic
    /// they can find, rather than a refusal to start.
    private func installRouting(on session: MobilecoreSession, routing: RoutingConfiguration) {
        if let data = CountryRoutes.packedChinaSet() {
            do {
                try session.setCountrySet("CN", blob: data)
            } catch {
                recordDiagnostic(
                    level: .error,
                    "The bundled China set did not load, so GEOIP rules will not match: "
                        + error.localizedDescription
                )
            }
        }
        guard !routing.rules.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            return
        }
        let report = session.setRoutingRules(routing.rules)
        recordDiagnostic(level: .info, "Routing rules: \(report)")
    }
}

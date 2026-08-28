import Foundation
import NetworkExtension

extension TunnelModel {
    var selectedProfile: StoredProfile? {
        profiles.first(where: { $0.id == selectedProfileID })
    }

    var hasProfiles: Bool { !profiles.isEmpty }

    var isTunnelActive: Bool {
        switch manager?.connection.status {
        case .connected, .connecting, .disconnecting, .reasserting:
            return true
        default:
            return false
        }
    }

    var canChangeProfile: Bool { !isBusy && !isTunnelActive }

    func renewSelectedProfileIfNeeded() async throws -> StoredProfile {
        try await Task.detached(priority: .userInitiated) {
            let store = try ProfileStore()
            guard var (record, profile) = try store.selectedProfile() else {
                throw ModelError.missingProfile
            }
            if try MobileCore.profileNeedsRenewal(profile) {
                profile = try MobileCore.renewProfile(profile)
                try store.replaceProfile(profile, id: record.id)
                guard let refreshed = try store.profile(id: record.id)?.0 else {
                    throw ModelError.missingProfile
                }
                record = refreshed
            }
            return record
        }.value
    }

    func loadManager(reason: String? = nil) async {
        guard !isBusy else {
            managerReloadPending = true
            return
        }
        guard !managerReloadInProgress else {
            managerReloadPending = true
            return
        }
        managerReloadInProgress = true
        managerReloadPending = false
        publishManagerLoaded(false)
        if manager == nil {
            publishStatus("Loading VPN configuration…")
        }
        do {
            let managers = try await NETunnelProviderManager.loadAllFromPreferences()
            manager = preferredManager(from: managers)
            publishManagerLoaded(true)
            if let reason {
                await recordDiagnostic(level: .info, message: "\(reason); VPN manager reloaded")
            }
            updateStatus()
        } catch {
            publishStatus("VPN configuration unavailable")
            await recordDiagnostic(
                level: .error,
                message: "VPN manager reload failed: \(error.localizedDescription)"
            )
            present(error, title: "VPN configuration is unavailable")
        }
        managerReloadInProgress = false
        if managerReloadPending && !isBusy {
            await loadManager(reason: "queued VPN configuration change")
        }
    }

    func preferredManager(from managers: [NETunnelProviderManager]) -> NETunnelProviderManager? {
        let activeStatuses: Set<NEVPNStatus> = [.connecting, .connected, .reasserting, .disconnecting]
        if let active = managers.first(where: { activeStatuses.contains($0.connection.status) }) {
            return active
        }
        guard let providerIdentifier = Bundle.main.object(
            forInfoDictionaryKey: "QueqiaoPacketTunnelBundleIdentifier"
        ) as? String else {
            return managers.first
        }
        return managers.first {
            ($0.protocolConfiguration as? NETunnelProviderProtocol)?.providerBundleIdentifier ==
                providerIdentifier
        } ?? managers.first
    }

    func configuredManager(for record: StoredProfile) async throws -> NETunnelProviderManager {
        let manager = manager ?? NETunnelProviderManager()
        let configuration = NETunnelProviderProtocol()
        guard let providerIdentifier = Bundle.main.object(
            forInfoDictionaryKey: "QueqiaoPacketTunnelBundleIdentifier"
        ) as? String,
              !providerIdentifier.isEmpty,
              !providerIdentifier.contains("$(") else {
            throw ModelError.invalidPacketTunnelIdentifier
        }
        configuration.providerBundleIdentifier = providerIdentifier
        configuration.serverAddress = record.summary.endpoint
        configuration.disconnectOnSleep = false
        // The profile identity, and nothing else. Routing used to be copied in
        // here too and read back in preference to the stored record, so a rule
        // could only take effect by rewriting the saved configuration — which
        // is precisely what cannot be done while the tunnel is up.
        configuration.providerConfiguration = ["profileID": record.id]
        manager.protocolConfiguration = configuration
        manager.localizedDescription = "Queqiao"
        manager.isEnabled = true
        apply(record.onDemandPolicy, to: manager)
        try await manager.saveToPreferences()
        try await manager.loadFromPreferences()
        return manager
    }

    /// Installs a profile's on-demand policy on the saved configuration.
    ///
    /// Writing the rules and the flag in one place is deliberate: an enabled
    /// flag with a stale rule list is a tunnel that comes up on a network the
    /// user marked trusted, which is the failure this feature must not have.
    func apply(_ policy: OnDemandPolicy, to manager: NETunnelProviderManager) {
        let rules = OnDemandRules.rules(for: policy)
        manager.onDemandRules = rules
        manager.isOnDemandEnabled = !rules.isEmpty
    }

    /// Pushes the selected profile's on-demand policy to the saved
    /// configuration outside a connect.
    ///
    /// Without this, turning automatic connection off would not take effect
    /// until the next manual connect — and in the meantime the rules saved by
    /// the last connect would still be bringing the tunnel up.
    func syncOnDemandPolicy() async {
        guard let manager, manager.protocolConfiguration != nil else { return }
        let policy = selectedProfile?.onDemandPolicy ?? .off
        apply(policy, to: manager)
        do {
            try await manager.saveToPreferences()
            try await manager.loadFromPreferences()
            await recordDiagnostic(
                level: .info,
                message: "Automatic connection updated: \(OnDemandRules.summary(for: policy))"
            )
        } catch {
            present(error, title: "Could not update automatic connection")
        }
    }

    /// Clears on-demand before a deliberate disconnect.
    ///
    /// An enabled connect rule would bring the tunnel straight back up, so a
    /// button labelled Disconnect would not disconnect. Pressing Connect
    /// reinstalls the policy from the profile.
    func suspendOnDemandForManualDisconnect() async {
        guard let manager, manager.isOnDemandEnabled else { return }
        manager.isOnDemandEnabled = false
        do {
            try await manager.saveToPreferences()
            try await manager.loadFromPreferences()
            await recordDiagnostic(
                level: .info,
                message: "Automatic connection paused by a manual disconnect; it resumes on the next connect"
            )
        } catch {
            await recordDiagnostic(
                level: .error,
                message: "Could not pause automatic connection before disconnecting: \(error.localizedDescription)"
            )
        }
    }

    func updateStatus() {
        guard isManagerLoaded else { return }
        guard let connection = manager?.connection else {
            let missedDisconnect = disconnectRecoveryMarker.needsDisconnectRecovery
            disconnectRecoveryMarker.resolveDisconnect()
            statusTracker.reset()
            publishStatus("Disconnected")
            stopMetricsUpdates(reset: true)
            if missedDisconnect {
                Task { [weak self] in
                    guard let self else { return }
                    await recordDiagnostic(
                        level: .error,
                        message: "The active VPN configuration disappeared before a disconnect status was observed"
                    )
                    await refreshDiagnostics()
                }
            }
            return
        }
        let connectionStatus = connection.status
        let observation = statusTracker.observe(connectionStatus)
        let recovery = updateDisconnectRecovery(for: connectionStatus, observation: observation)
        record(observation, recovery: recovery, connection: connection)
        if connectionStatus == .disconnected || connectionStatus == .invalid {
            disconnectRequested = false
        }
        apply(connectionStatus)
    }

    func updateDisconnectRecovery(
        for status: NEVPNStatus,
        observation: VPNStatusObservation
    ) -> (shouldFetch: Bool, missedWhileUnavailable: Bool) {
        let endedInTerminalStatus = status == .disconnected || status == .invalid
        let missedWhileUnavailable = observation.previousStatus == nil &&
            endedInTerminalStatus && disconnectRecoveryMarker.needsDisconnectRecovery
        let endedUnexpectedly = observation.endedActiveEpisode && !disconnectRequested
        if status == .connected {
            disconnectRecoveryMarker.markConnected()
        } else if observation.endedActiveEpisode || missedWhileUnavailable {
            disconnectRecoveryMarker.resolveDisconnect()
        }
        return (endedUnexpectedly || missedWhileUnavailable, missedWhileUnavailable)
    }

    func record(
        _ observation: VPNStatusObservation,
        recovery: (shouldFetch: Bool, missedWhileUnavailable: Bool),
        connection: NEVPNConnection
    ) {
        guard observation.transitionDescription != nil || recovery.shouldFetch else { return }
        Task { [weak self] in
            guard let self else { return }
            if let transitionDescription = observation.transitionDescription {
                await recordDiagnostic(level: .info, message: transitionDescription)
            }
            guard recovery.shouldFetch else { return }
            let context = recovery.missedWhileUnavailable
                ? "The VPN ended while the app was not running"
                : "iOS ended the VPN without a disconnect request"
            await recordDiagnostic(level: .error, message: context)
            await recordLastDisconnectCause(from: connection)
            await refreshDiagnostics()
        }
    }

    func apply(_ connectionStatus: NEVPNStatus) {
        switch connectionStatus {
        case .connected:
            invalidStatusReloadAttempted = false
            publishStatus("Connected")
            startMetricsUpdates()
        case .connecting:
            invalidStatusReloadAttempted = false
            publishStatus("Connecting…")
            stopMetricsUpdates(reset: false)
        case .disconnecting:
            invalidStatusReloadAttempted = false
            publishStatus("Disconnecting…")
            stopMetricsUpdates(reset: false)
        case .reasserting:
            invalidStatusReloadAttempted = false
            publishStatus("Reconnecting…")
            stopMetricsUpdates(reset: false)
        case .disconnected:
            invalidStatusReloadAttempted = false
            publishStatus("Disconnected")
            stopMetricsUpdates(reset: true)
        case .invalid:
            publishStatus("VPN configuration unavailable")
            stopMetricsUpdates(reset: true)
            if !invalidStatusReloadAttempted {
                invalidStatusReloadAttempted = true
                Task { [weak self] in
                    await self?.loadManager(reason: "VPN manager entered the invalid state")
                }
            }
        @unknown default:
            publishStatus("Unavailable")
            stopMetricsUpdates(reset: true)
        }
    }

    func recordLastDisconnectCause(from connection: NEVPNConnection) async {
        if let error = await VPNDiagnostics.fetchLastDisconnectError(from: connection) {
            await recordDiagnostic(
                level: .error,
                message: "Last disconnect: \(VPNDiagnostics.describeDisconnectError(error))"
            )
        } else {
            await recordDiagnostic(
                level: .warning,
                message: "iOS did not provide a last-disconnect error"
            )
        }
    }
    func startMetricsUpdates() {
        guard metricsTimer == nil else { return }
        Task { await refreshMetrics() }
        let timer = Timer.scheduledTimer(withTimeInterval: 1, repeats: true) { [weak self] _ in
            Task { @MainActor in await self?.refreshMetrics() }
        }
        metricsTimer = TimerToken(timer)
    }
    func stopMetricsUpdates(reset: Bool) {
        metricsTimer?.invalidate()
        metricsTimer = nil
        if reset { publishMetrics(.empty) }
    }

    /// Sends one request to the running provider and returns its answer.
    ///
    /// Returns nil when no tunnel is running, which is a normal state for both
    /// callers rather than a failure: metrics simply have nothing to report,
    /// and a routing change has nothing live to apply itself to.
    func sendProviderRequest(_ request: ProviderRequest) async throws -> Data? {
        guard let session = manager?.connection as? NETunnelProviderSession,
              session.status == .connected else { return nil }
        return try await withCheckedThrowingContinuation { continuation in
            do {
                try session.sendProviderMessage(request.payload) { data in
                    guard let data else {
                        continuation.resume(throwing: ModelError.emptyProviderResponse)
                        return
                    }
                    continuation.resume(returning: data)
                }
            } catch {
                continuation.resume(throwing: error)
            }
        }
    }

    /// Pushes the stored routing rules onto the running tunnel.
    ///
    /// The rules are already saved by the time this runs, so a failure here
    /// means the tunnel is still carrying traffic by the previous plan. That
    /// has to be said out loud: silently leaving the two out of step is how a
    /// user ends up believing a destination is off the tunnel when it is not.
    func applyRoutingToRunningTunnel() async {
        do {
            guard let response = try await sendProviderRequest(.reloadRouting) else { return }
            let result = try RoutingReloadResult(payload: response)
            guard result.applied else {
                present(
                    ModelError.routingNotApplied(result.failure ?? "the extension declined it"),
                    title: "Routing not applied"
                )
                return
            }
        } catch {
            present(ModelError.routingNotApplied(error.localizedDescription), title: "Routing not applied")
        }
    }

    func refreshMetrics() async {
        do {
            guard let response = try await sendProviderRequest(.metrics) else { return }
            publishMetrics(try TunnelMetrics.decode(response))
        } catch {
            // Metrics are operational decoration; a transient extension IPC failure must not alter tunnel state.
        }
    }

    func present(_ error: Error, title: String) {
        presentedError = PresentedError(title: title, message: error.localizedDescription)
    }

    func recordDiagnostic(level: DiagnosticLevel, message: String) async {
        await Task.detached {
            try? DiagnosticStore().append(level: level, component: "App", message: message)
        }.value
    }
}

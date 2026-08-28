import SwiftUI

struct ProfilesView: View {
    @EnvironmentObject private var model: TunnelModel

    var body: some View {
        NavigationStack {
            Group {
                if model.profiles.isEmpty {
                    ContentUnavailableView(
                        "No Profiles",
                        systemImage: "rectangle.stack.badge.plus",
                        description: Text(
                            "Import a Queqiao invitation to enroll this device with a provider."
                        )
                    )
                } else {
                    List {
                        Section {
                            Button {
                                Task { await model.testAllProfiles() }
                            } label: {
                                Label("Test all connections", systemImage: "gauge.with.dots.needle.67percent")
                            }
                            .disabled(!model.canTestProfiles)
                        } footer: {
                            Text("Tests provider reachability and device authorization without opening a destination.")
                        }

                        Section("Connections") {
                            ForEach(model.profiles) { profile in
                                NavigationLink(value: profile.id) {
                                    ProfileRow(
                                        profile: profile,
                                        isSelected: profile.id == model.selectedProfileID,
                                        probeState: model.profileProbeStates[profile.id]
                                    )
                                }
                            }
                        }
                    }
                    .listStyle(.insetGrouped)
                }
            }
            .navigationTitle("Profiles")
            .toolbar {
                ToolbarItem(placement: .primaryAction) {
                    Button { model.isImporterPresented = true } label: {
                        Label("Import profile", systemImage: "plus")
                    }
                }
            }
            .navigationDestination(for: String.self) { profileID in
                ProfileDetailView(profileID: profileID)
            }
            .safeAreaInset(edge: .bottom) {
                if model.isTunnelActive {
                    Label(
                        "Disconnect before switching or editing profiles",
                        systemImage: "lock.fill"
                    )
                    .font(.footnote)
                    .padding(10)
                    .frame(maxWidth: .infinity)
                    .background(.thinMaterial)
                }
            }
        }
    }
}

private struct ProfileRow: View {
    let profile: StoredProfile
    let isSelected: Bool
    let probeState: ProfileProbeState?

    var body: some View {
        HStack(spacing: 12) {
            Image(systemName: "server.rack")
                .font(.title3)
                .foregroundStyle(isSelected ? .teal : .secondary)
                .frame(width: 30)
            VStack(alignment: .leading, spacing: 3) {
                HStack {
                    Text(profile.displayName)
                        .font(.headline)
                    if isSelected {
                        Text("SELECTED")
                            .font(.caption2.bold())
                            .foregroundStyle(.teal)
                    }
                }
                Text(profile.summary.endpoint)
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                Text("Device: \(profile.summary.deviceName)")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                if let probeState {
                    Label(probeState.summary, systemImage: probeState.symbol)
                        .font(.caption.bold())
                        .foregroundStyle(probeColor(probeState))
                }
            }
        }
        .padding(.vertical, 4)
    }

    private func probeColor(_ state: ProfileProbeState) -> Color {
        switch state {
        case .testing: return .secondary
        case .available: return .green
        case .unavailable: return .red
        }
    }
}

private struct ProfileDetailView: View {
    @EnvironmentObject private var model: TunnelModel
    @Environment(\.dismiss) private var dismiss
    let profileID: String
    @State private var confirmDelete = false
    @State private var showRename = false
    @State private var requestedName = ""

    var body: some View {
        Group {
            if let profile = model.profile(id: profileID) {
                Form {
                    summarySection(profile)
                    connectionTestSection(profile)
                    ProfileRoutingSection(profile: profile)
                    ProfileRulesSection(profile: profile)
                    ProfileOnDemandSection(profile: profile)
                    actionsSection(profile)
                    identitySection(profile)
                }
                .navigationTitle(profile.displayName)
                .navigationBarTitleDisplayMode(.inline)
                .confirmationDialog(
                    "Delete this profile?",
                    isPresented: $confirmDelete,
                    titleVisibility: .visible
                ) {
                    Button("Delete profile", role: .destructive) {
                        Task {
                            await model.deleteProfile(id: profile.id)
                            if model.profile(id: profile.id) == nil { dismiss() }
                        }
                    }
                    Button("Cancel", role: .cancel) {}
                } message: {
                    Text(
                        "This permanently deletes the device key. " +
                        "A new invitation is required to enroll again."
                    )
                }
                .alert("Rename profile", isPresented: $showRename) {
                    TextField("Profile name", text: $requestedName)
                    Button("Save") {
                        Task {
                            await model.renameProfile(id: profile.id, name: requestedName)
                        }
                    }
                    Button("Cancel", role: .cancel) {}
                }
            } else {
                ContentUnavailableView(
                    "Profile Not Found",
                    systemImage: "questionmark.folder"
                )
            }
        }
    }

    private func connectionTestSection(_ profile: StoredProfile) -> some View {
        let probeState = model.profileProbeStates[profile.id]
        return Section {
            if let probeState {
                LabeledContent("Result", value: probeState.summary)
            }
            Button {
                Task { await model.testProfile(id: profile.id) }
            } label: {
                Label(
                    probeState == .testing ? "Testing connection…" : "Test connection",
                    systemImage: "gauge.with.dots.needle.67percent"
                )
            }
            .disabled(!model.canTestProfiles)
            if let detail = probeState?.detail {
                Text(detail)
                    .font(.footnote)
                    .foregroundStyle(.red)
                    .textSelection(.enabled)
            }
        } header: {
            Text("Connection test")
        } footer: {
            Text("Measures an authenticated provider control round trip. No destination is contacted.")
        }
    }

    private func summarySection(_ profile: StoredProfile) -> some View {
        Section {
            LabeledContent(
                "Status",
                value: profile.id == model.selectedProfileID ? "Selected profile" : "Available"
            )
            LabeledContent("Provider", value: profile.summary.name)
            LabeledContent("Endpoint", value: profile.summary.endpoint)
            LabeledContent("Active device", value: profile.summary.deviceName)
            LabeledContent(
                "Certificate expires",
                value: formattedExpiry(profile.summary.certificateExpiry)
            )
        } header: {
            Text("Connection profile")
        }
    }

    private func actionsSection(_ profile: StoredProfile) -> some View {
        Section {
            if profile.id == model.selectedProfileID {
                LabeledContent("Selected for the next connection", value: "Yes")
            } else {
                Button("Use this profile") {
                    Task { await model.selectProfile(id: profile.id) }
                }
                .disabled(!model.canChangeProfile)
            }
            Button("Rename profile") {
                requestedName = profile.displayName
                showRename = true
            }
            Button("Delete profile", role: .destructive) { confirmDelete = true }
                .disabled(!model.canChangeProfile)
        }
    }

    private func identitySection(_ profile: StoredProfile) -> some View {
        Section("Identity") {
            LabeledContent("Provider ID", value: profile.summary.providerID)
            LabeledContent("Device ID", value: profile.summary.deviceID)
        }
        .font(.footnote)
    }

    private func formattedExpiry(_ encoded: String) -> String {
        guard let date = ISO8601DateFormatter().date(from: encoded) else { return encoded }
        return date.formatted(date: .abbreviated, time: .shortened)
    }
}

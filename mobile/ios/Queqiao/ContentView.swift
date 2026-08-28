import SwiftUI

enum AppTab: Hashable {
    case home
    case profiles
    case settings
}

struct ContentView: View {
    @EnvironmentObject private var model: TunnelModel
    @State private var selectedTab: AppTab = .home

    var body: some View {
        TabView(selection: $selectedTab) {
            ConnectionView(selectedTab: $selectedTab)
                .tabItem { Label("Home", systemImage: "shield.lefthalf.filled") }
                .tag(AppTab.home)

            ProfilesView()
                .tabItem { Label("Profiles", systemImage: "rectangle.stack.fill") }
                .tag(AppTab.profiles)

            SettingsView()
                .tabItem { Label("Settings", systemImage: "gearshape.fill") }
                .tag(AppTab.settings)
        }
        .tint(.teal)
        .sheet(isPresented: $model.isImporterPresented) {
            ImportProfileView()
        }
        .task { await model.start() }
        .alert(item: $model.presentedError) { error in
            Alert(
                title: Text(error.title),
                message: Text(error.message),
                dismissButton: .default(Text("OK"))
            )
        }
    }
}

private struct ConnectionView: View {
    @EnvironmentObject private var model: TunnelModel
    @Binding var selectedTab: AppTab

    var body: some View {
        NavigationStack {
            ScrollView {
                VStack(spacing: 18) {
                    connectionCard
                    if let profile = model.selectedProfile {
                        profileCard(profile)
                        metricsCard
                    } else {
                        emptyProfileCard
                    }
                    privacyNote
                }
                .padding()
            }
            .background(Color(uiColor: .systemGroupedBackground))
            .navigationTitle("Queqiao")
            .refreshable { await model.refreshProfiles() }
        }
    }

    private var connectionCard: some View {
        VStack(spacing: 18) {
            ZStack {
                Circle()
                    .fill(statusColor.opacity(0.14))
                    .frame(width: 132, height: 132)
                Circle()
                    .stroke(statusColor.opacity(0.32), lineWidth: 2)
                    .frame(width: 112, height: 112)
                Image(systemName: model.status == "Connected" ? "shield.fill" : "power")
                    .font(.system(size: 42, weight: .semibold))
                    .foregroundStyle(statusColor)
                    .symbolEffect(.pulse, isActive: model.isBusy)
            }

            VStack(spacing: 4) {
                Text(model.status)
                    .font(.title2.bold())
                Text(connectionSubtitle)
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
                    .multilineTextAlignment(.center)
            }

            Button {
                if model.isTunnelActive {
                    Task { await model.disconnect() }
                } else {
                    Task { await model.connect() }
                }
            } label: {
                Text(model.isTunnelActive ? "Disconnect" : "Connect")
                    .font(.headline)
                    .frame(maxWidth: .infinity)
                    .padding(.vertical, 7)
            }
            .buttonStyle(.borderedProminent)
            .tint(model.isTunnelActive ? .red : .teal)
            .controlSize(.large)
            .disabled(
                model.isBusy || !model.isManagerLoaded ||
                    (!model.hasProfiles && !model.isTunnelActive)
            )
            .accessibilityHint(connectionAccessibilityHint)
        }
        .padding(22)
        .frame(maxWidth: .infinity)
        .background(.background, in: RoundedRectangle(cornerRadius: 24, style: .continuous))
    }

    private func profileCard(_ profile: StoredProfile) -> some View {
        VStack(alignment: .leading, spacing: 14) {
            HStack {
                Text(model.isTunnelActive ? "Current connection" : "Selected profile")
                    .font(.headline)
                Spacer()
                Button("Profiles") { selectedTab = .profiles }
                    .font(.subheadline)
            }
            Divider()
            LabeledContent("Profile", value: profile.displayName)
            LabeledContent("Provider", value: profile.summary.endpoint)
            LabeledContent("Routing", value: profile.routing.summary)
            LabeledContent("Active device", value: profile.summary.deviceName)
        }
        .padding()
        .background(.background, in: RoundedRectangle(cornerRadius: 18, style: .continuous))
    }

    private var metricsCard: some View {
        VStack(alignment: .leading, spacing: 14) {
            Text("This connection")
                .font(.headline)
            HStack(spacing: 12) {
                MetricView(
                    label: "Downloaded",
                    value: formatBytes(model.metrics.bytesDown),
                    symbol: "arrow.down"
                )
                MetricView(
                    label: "Uploaded",
                    value: formatBytes(model.metrics.bytesUp),
                    symbol: "arrow.up"
                )
                MetricView(
                    label: "Active flows",
                    value: "\(model.metrics.activeFlows)",
                    symbol: "point.3.connected.trianglepath.dotted"
                )
            }
        }
        .padding()
        .background(.background, in: RoundedRectangle(cornerRadius: 18, style: .continuous))
    }

    private var emptyProfileCard: some View {
        ContentUnavailableView {
            Label("No VPN Profile", systemImage: "rectangle.stack.badge.plus")
        } description: {
            Text("Import a one-time invitation to create your first device-bound profile.")
        } actions: {
            Button("Import invitation") { model.isImporterPresented = true }
                .buttonStyle(.borderedProminent)
        }
        .frame(maxWidth: .infinity)
        .padding(.vertical, 22)
        .background(.background, in: RoundedRectangle(cornerRadius: 18, style: .continuous))
    }

    private var privacyNote: some View {
        Label {
            Text(
                "Your selected provider can observe destinations, timing, " +
                "and traffic that is not protected end-to-end."
            )
        } icon: {
            Image(systemName: "hand.raised.fill")
        }
        .font(.caption)
        .foregroundStyle(.secondary)
        .padding(.horizontal, 4)
    }

    private var statusColor: Color {
        if model.status == "Connected" { return .green }
        if model.isTunnelActive || model.isBusy || !model.isManagerLoaded { return .orange }
        return .teal
    }

    private var connectionSubtitle: String {
        if let profile = model.selectedProfile {
            return "\(profile.displayName) · \(profile.summary.endpoint)"
        }
        return "Import a profile to get started"
    }

    private var connectionAccessibilityHint: String {
        model.isTunnelActive
            ? "Stops the Queqiao VPN"
            : "Starts the selected Queqiao VPN profile"
    }

    private func formatBytes(_ bytes: UInt64) -> String {
        ByteCountFormatter.string(fromByteCount: Int64(clamping: bytes), countStyle: .binary)
    }
}

private struct MetricView: View {
    let label: String
    let value: String
    let symbol: String

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            Image(systemName: symbol)
                .foregroundStyle(.teal)
            Text(value)
                .font(.headline.monospacedDigit())
                .lineLimit(1)
                .minimumScaleFactor(0.7)
            Text(label)
                .font(.caption2)
                .foregroundStyle(.secondary)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}

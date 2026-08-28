import SwiftUI

/// Everything about where one profile's traffic goes: how much of it the tunnel
/// carries, and which destinations are carved back out of it.
///
/// Split out of ProfilesView because iOS gives Queqiao no consumer app to hand
/// routing policy to, so this is the whole of the routing surface and it is the
/// part most likely to keep growing.
///
/// The mode and the rules are one screen because they are one decision. They
/// were previously three controls — a two-value traffic policy, a bundled-set
/// toggle, and a route list — where two of them applied no matter what the
/// third said, so "All traffic" was not true whenever either of the others was
/// on. Nothing here is disabled while the tunnel is up: these are the settings
/// most worth changing precisely then.
struct ProfileRoutingSection: View {
    @EnvironmentObject private var model: TunnelModel
    let profile: StoredProfile
    @State private var bypassDraft = ""
    /// How many blocks the bundled set holds, read from its header once. nil
    /// until it is read, and stays nil if the resource is missing — in which
    /// case the screen says nothing rather than guessing a number.
    @State private var bundledBlocks: Int?

    var body: some View {
        Group {
            modeSection
            rulesSection
            customRoutesSection
        }
        .task(id: profile.id) {
            bypassDraft = storedBypassText
            bundledBlocks = try? CountryRoutes.blockCount()
        }
        .onChange(of: profile.bypassRoutes) { _, _ in bypassDraft = storedBypassText }
    }

    /// The live record, so a toggle reflects the store rather than the snapshot
    /// this view was built with.
    private var routing: RoutingConfiguration {
        model.profile(id: profile.id)?.routing ?? profile.routing
    }

    private var modeSection: some View {
        Section {
            Picker("Routing", selection: modeBinding) {
                ForEach(RoutingMode.allCases) { mode in
                    Text(mode.title).tag(mode)
                }
            }
            .pickerStyle(.inline)
            .labelsHidden()

            Text(routing.mode.detail)
                .font(.footnote)
                .foregroundStyle(.secondary)
        } header: {
            Text("Routing")
        } footer: {
            Text(
                "DNS resolves through the Queqiao tunnel in both modes, so a rule " +
                "below can only match an address — never a domain name."
            )
        }
    }

    private var rulesSection: some View {
        Section {
            Toggle("Local networks", isOn: ruleBinding(\.bypassLocalNetworks))
            Text("Private and link-local destinations, such as a home router or a printer.")
                .font(.footnote)
                .foregroundStyle(.secondary)

            Toggle("Chinese addresses", isOn: ruleBinding(\.bypassChinaDirect))
            if let bundledBlocks {
                LabeledContent(
                    "Blocks in the set",
                    value: bundledBlocks.formatted(.number)
                )
            }
        } header: {
            Text("Bypass rules")
        } footer: {
            Text(rulesFooter)
        }
    }

    private var customRoutesSection: some View {
        Section {
            TextField(
                "203.0.113.0/24\n2001:db8::/32",
                text: $bypassDraft,
                axis: .vertical
            )
            .lineLimit(3...8)
            .autocorrectionDisabled()
            .textInputAutocapitalization(.never)
            .font(.callout.monospaced())

            if hasUnsavedEdits {
                Button("Save custom routes") {
                    Task { await model.setBypassRoutes(from: bypassDraft, for: profile.id) }
                }
                Button("Discard changes", role: .cancel) { bypassDraft = storedBypassText }
            }

            LabeledContent(
                "In use",
                value: "\(routing.customRoutes.count) of \(StoredProfile.maximumBypassRoutes)"
            )

            if coversWholeFamily {
                Label(
                    "A route here covers an entire address family, so that traffic will "
                        + "not use the tunnel at all.",
                    systemImage: "exclamationmark.triangle"
                )
                .font(.footnote)
                .foregroundStyle(.orange)
            }
        } header: {
            Text("Custom routes")
        } footer: {
            Text(
                "Addresses and CIDR blocks listed here stay off the tunnel and use " +
                "the device's normal network. One per line, or separated by commas."
            )
        }
    }

    /// Says plainly whether the rules are doing anything, because a screen full
    /// of switched-on toggles that route nothing is the confusion this layout
    /// exists to remove.
    private var rulesFooter: String {
        let bundled = "The bundled set adds the address blocks APNIC records as delegated " +
            "to China. The registry records where a block was allocated, not where it is " +
            "used, so the match is approximate — and a Chinese site answering with an " +
            "address outside the set still takes the tunnel."
        guard routing.rulesApply else {
            return "These rules are saved but not applied while the mode above is " +
                "\(RoutingMode.allTraffic.title.lowercased()).\n\n" + bundled
        }
        return "Destinations matched here stay off the tunnel. Everything else goes " +
            "through Queqiao.\n\n" + bundled
    }

    private var modeBinding: Binding<RoutingMode> {
        Binding(
            get: { routing.mode },
            set: { mode in
                Task { await model.updateRouting(for: profile.id) { $0.mode = mode } }
            }
        )
    }

    private func ruleBinding(
        _ field: WritableKeyPath<RoutingConfiguration, Bool>
    ) -> Binding<Bool> {
        Binding(
            get: { routing[keyPath: field] },
            set: { enabled in
                Task {
                    await model.updateRouting(for: profile.id) { $0[keyPath: field] = enabled }
                }
            }
        )
    }

    /// Whether the stored rules take all of IPv4 or all of IPv6 off the tunnel.
    /// Built through RoutePlan so the screen and the tunnel agree on what a
    /// default route is. The bundled set is left out because it is a bounded
    /// list of allocations and cannot cover a whole family.
    private var coversWholeFamily: Bool {
        RoutePlan.make(for: routing).excludesDefaultRoute
    }

    private var storedBypassText: String {
        routing.customRoutes.joined(separator: "\n")
    }

    /// Compares against the stored list rather than tracking an edited flag, so
    /// typing a route and typing it back out again leaves no stale prompt.
    private var hasUnsavedEdits: Bool {
        StoredProfile.routeEntries(from: bypassDraft) != routing.customRoutes
    }
}

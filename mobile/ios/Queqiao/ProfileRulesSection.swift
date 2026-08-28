import SwiftUI

/// The routing rule list: where the user writes, pastes, or imports the rules
/// that decide which flows take the tunnel.
///
/// The list is text rather than a row-per-rule editor, and that is the point.
/// Everyone arriving here already has a list -- exported from Clash, mihomo,
/// sing-box or Shadowrocket -- and the fastest path from that file to a working
/// tunnel is paste. A structured editor would mean retyping hundreds of lines
/// into a form.
///
/// What the screen owes in return is an honest account of what it made of the
/// text, which is why the summary below counts rules and names failures by line
/// rather than saying "saved".
struct ProfileRulesSection: View {
    @EnvironmentObject private var model: TunnelModel
    let profile: StoredProfile

    @State private var draft: String = ""
    @State private var loaded = false

    private var review: RuleReview { RuleReview(text: draft) }

    var body: some View {
        Section {
            TextEditor(text: $draft)
                .font(.system(.footnote, design: .monospaced))
                .frame(minHeight: 160)
                .autocorrectionDisabled()
                .textInputAutocapitalization(.never)
                .accessibilityLabel("Routing rules")

            summary

            if !review.problems.isEmpty {
                ForEach(review.problems, id: \.self) { problem in
                    Label(problem, systemImage: "exclamationmark.triangle")
                        .font(.caption)
                        .foregroundStyle(.orange)
                }
            }

            HStack {
                Button("Save rules") { save() }
                    .disabled(draft == profile.routingRules)
                Spacer()
                Button("Use the China preset") { draft = RuleReview.chinaPreset }
                    .font(.callout)
            }
        } header: {
            Text("Routing rules")
        } footer: {
            Text(
                """
                One rule per line, first match wins: TYPE,VALUE,ACTION. \
                DOMAIN, DOMAIN-SUFFIX, DOMAIN-KEYWORD, IP-CIDR, GEOIP and \
                DST-PORT, with PROXY, DIRECT or REJECT. A flow no rule \
                matches takes the tunnel, so end with FINAL if you want \
                otherwise. Saved rules reach a running tunnel immediately; flows \
                already open keep the rules they started under.
                """
            )
        }
        .onAppear {
            guard !loaded else { return }
            draft = profile.routingRules
            loaded = true
        }
    }

    @ViewBuilder
    private var summary: some View {
        if review.isEmpty {
            Text("No rules. Every flow takes the tunnel.")
                .font(.caption)
                .foregroundStyle(.secondary)
        } else {
            Text(review.summary)
                .font(.caption)
                .foregroundStyle(review.problems.isEmpty ? .secondary : .primary)
        }
    }

    private func save() {
        model.updateRoutingRules(draft, for: profile.id)
    }
}

/// A local reading of the rule text, for the screen only.
///
/// This deliberately does not decide anything. The core owns the grammar and is
/// the only thing that acts on a rule; duplicating the full parser here would
/// give the device two answers to the same question and no way to tell which
/// one the tunnel used. What this does is cheap and structural -- count the
/// lines that look like rules, and name the ones that clearly are not -- so
/// that a typo is visible before connecting rather than after.
struct RuleReview {
    let count: Int
    let problems: [String]

    static let chinaPreset = """
        # Keep China direct by name as well as by address. The GEOIP line uses
        # the bundled registry set; the name rules are what an address set
        # cannot do, because a Chinese domain can answer with a CDN address
        # outside it.
        DOMAIN-SUFFIX,cn,DIRECT
        DOMAIN-KEYWORD,-cn,DIRECT
        GEOIP,CN,DIRECT
        FINAL,PROXY
        """

    private static let knownTypes: Set<String> = [
        "DOMAIN", "DOMAIN-SUFFIX", "DOMAIN-KEYWORD",
        "IP-CIDR", "IP-CIDR6", "IP6-CIDR",
        "GEOIP", "DST-PORT", "PORT", "FINAL", "MATCH"
    ]
    private static let knownActions: Set<String> = [
        "PROXY", "QUEQIAO", "DIRECT", "REJECT", "REJECT-DROP"
    ]

    init(text: String) {
        var counted = 0
        var found: [String] = []
        for (index, rawLine) in text.split(separator: "\n", omittingEmptySubsequences: false).enumerated() {
            let line = rawLine.trimmingCharacters(in: .whitespaces)
            if line.isEmpty || line.hasPrefix("#") || line.hasPrefix(";") { continue }
            let fields = line.split(separator: ",").map {
                $0.trimmingCharacters(in: .whitespaces).uppercased()
            }
            guard let type = fields.first, Self.knownTypes.contains(type) else {
                found.append("Line \(index + 1): not a rule type")
                continue
            }
            let isFinal = type == "FINAL" || type == "MATCH"
            let expected = isFinal ? 2 : 3
            guard fields.count >= expected else {
                found.append("Line \(index + 1): \(type) needs \(isFinal ? "an action" : "a value and an action")")
                continue
            }
            let action = fields[expected - 1]
            guard Self.knownActions.contains(action) else {
                // The common case by far is a file written for a client with
                // several outbounds, where this field names a proxy group.
                // Saying so is more useful than "invalid".
                found.append(
                    "Line \(index + 1): \"\(fields[expected - 1].lowercased())\" is not an action. "
                        + "This client has one tunnel, so PROXY, DIRECT or REJECT."
                )
                continue
            }
            counted += 1
        }
        count = counted
        problems = Array(found.prefix(10))
    }

    var isEmpty: Bool { count == 0 && problems.isEmpty }

    var summary: String {
        var text = "\(count) rule\(count == 1 ? "" : "s")"
        if !problems.isEmpty {
            text += ", \(problems.count) line\(problems.count == 1 ? "" : "s") the core will not load"
        }
        return text
    }
}

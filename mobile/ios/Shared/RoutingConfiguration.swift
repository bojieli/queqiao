import Foundation

/// How much of the device's traffic the tunnel carries.
///
/// The mode is deliberately coarse. Which destinations are kept off the tunnel
/// is not a mode — it is a set of rules, and those live in
/// `RoutingConfiguration` so that turning the tunnel global does not mean
/// dismantling a bypass list the user spent time building.
enum RoutingMode: String, Codable, CaseIterable, Identifiable, Sendable {
    /// Everything goes through the tunnel. Bypass rules stay stored but idle.
    case allTraffic = "all-traffic"
    /// Everything except the enabled bypass rules goes through the tunnel.
    case bypassRules = "bypass-rules"

    var id: String { rawValue }

    var title: String {
        switch self {
        case .allTraffic:
            return "Route all traffic"
        case .bypassRules:
            return "Use bypass rules"
        }
    }

    var detail: String {
        switch self {
        case .allTraffic:
            return "Every IPv4, IPv6, and DNS destination goes through the selected " +
                "Queqiao provider. Bypass rules below are kept but not applied."
        case .bypassRules:
            return "Everything goes through Queqiao except the destinations matched " +
                "by the rules below."
        }
    }
}

/// Everything that decides where one profile's traffic goes.
///
/// This used to be three settings that did not know about each other: a
/// two-value traffic policy, a bundled-set toggle, and a typed route list. Two
/// of them applied regardless of what the third said, so "All traffic" was not
/// true whenever a bypass route or the country set was also on. They are one
/// type now because they were always answering one question.
///
/// Queqiao routes on addresses only. DNS resolves through the tunnel, so no
/// rule here can match a domain name.
struct RoutingConfiguration: Equatable, Sendable {
    var mode: RoutingMode = .allTraffic
    /// Private and link-local destinations, as one rule rather than as a mode.
    var bypassLocalNetworks = false
    /// The bundled registry set for China. Experimental and approximate: see
    /// CountryRoutes.
    var bypassChinaDirect = false
    /// Destinations typed by the user, in canonical CIDR text.
    var customRoutes: [String] = []
    /// The rule list as the user wrote or imported it, one rule per line. Kept
    /// here rather than beside it because a rule list is also an answer to
    /// "where does this profile's traffic go", and the whole point of this type
    /// is that there is one answer. The core parses and acts on the text; the
    /// route plan below ignores it.
    var rules: String = ""

    /// Whether the rules below the mode picker currently affect anything. The
    /// UI keeps them editable either way, and says plainly when they are idle.
    var rulesApply: Bool { mode == .bypassRules }

    /// Whether any rule is switched on, regardless of the mode. Used to tell
    /// "no rules configured" apart from "rules configured but not applied".
    var hasEnabledRules: Bool {
        bypassLocalNetworks || bypassChinaDirect || !customRoutes.isEmpty
    }

    /// How many lines the rule list holds, ignoring blanks and comments, for
    /// screens that report on it without parsing it.
    var ruleLineCount: Int {
        rules.split(whereSeparator: \.isNewline)
            .map { $0.trimmingCharacters(in: .whitespaces) }
            .filter { !$0.isEmpty && !$0.hasPrefix("#") }
            .count
    }

    /// A one-line description for the connection summary card. Names the rules
    /// actually in force rather than the mode alone, because "use bypass rules"
    /// with every rule off carries exactly as much traffic as routing all of it
    /// and should not read differently.
    var summary: String {
        var parts: [String] = []
        if ruleLineCount > 0 {
            parts.append("\(ruleLineCount) \(ruleLineCount == 1 ? "rule" : "rules")")
        }
        guard rulesApply, hasEnabledRules else {
            return parts.isEmpty
                ? RoutingMode.allTraffic.title
                : "Routing by " + parts.formatted(.list(type: .and))
        }
        if bypassLocalNetworks { parts.append("local networks") }
        if bypassChinaDirect { parts.append("Chinese addresses") }
        if !customRoutes.isEmpty {
            parts.append("\(customRoutes.count) custom \(customRoutes.count == 1 ? "route" : "routes")")
        }
        return "Bypassing " + parts.formatted(.list(type: .and))
    }
}

extension RoutePlan {
    /// The plan a routing configuration installs.
    ///
    /// The country set is passed in rather than read here because loading it
    /// can fail, and the packet tunnel and the settings screen want to report
    /// that failure differently. Both still compose the plan through this one
    /// function, so what the screen previews is what the tunnel installs.
    static func make(
        for routing: RoutingConfiguration,
        chinaDirect: [IPPrefix] = [],
        limit: Int = defaultLimit
    ) -> RoutePlan {
        guard routing.rulesApply else {
            return make(userRoutes: [], builtIn: [], limit: limit)
        }
        var userRoutes = routing.customRoutes
        if routing.bypassLocalNetworks {
            userRoutes = localNetworks + userRoutes
        }
        return make(
            userRoutes: userRoutes,
            builtIn: routing.bypassChinaDirect ? chinaDirect : [],
            limit: limit
        )
    }
}

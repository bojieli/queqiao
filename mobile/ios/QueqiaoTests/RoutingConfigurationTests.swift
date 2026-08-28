import XCTest
@testable import Queqiao

/// The routing configuration is the merge of what used to be three unrelated
/// settings, so these tests are mostly about the states the old shape allowed
/// and this one must not: rules that apply when the mode says they do not, and
/// a mode that claims to carry everything while it does not.
final class RoutingConfigurationTests: XCTestCase {
    private var countrySet: [IPPrefix] {
        IPPrefix.parseList(["203.0.113.0/24", "198.51.100.0/24"]).parsed
    }

    /// The reason the three old settings were merged. "All traffic" used to be
    /// a lie: the bypass list and the country set were applied regardless of
    /// what the policy said, so a user could read "All traffic" off the screen
    /// while whole countries left the tunnel.
    func testGlobalModeInstallsNothingEvenWithEveryRuleSwitchedOn() {
        let routing = RoutingConfiguration(
            mode: .allTraffic,
            bypassLocalNetworks: true,
            bypassChinaDirect: true,
            customRoutes: ["10.0.0.0/8"]
        )

        let plan = RoutePlan.make(for: routing, chinaDirect: countrySet)

        XCTAssertTrue(plan.excluded.isEmpty)
        XCTAssertTrue(plan.ipv4Routes.isEmpty)
        XCTAssertFalse(plan.excludesDefaultRoute)
    }

    func testBypassModeAppliesLocalNetworksOnlyWhenThatRuleIsOn() {
        var routing = RoutingConfiguration(mode: .bypassRules)

        XCTAssertTrue(RoutePlan.make(for: routing).excluded.isEmpty)

        routing.bypassLocalNetworks = true
        let plan = RoutePlan.make(for: routing)

        XCTAssertFalse(plan.excluded.isEmpty)
        XCTAssertTrue(plan.excluded.contains { $0.cidrText == "192.168.0.0/16" })
    }

    func testBypassModeAppliesTheCountrySetOnlyWhenThatRuleIsOn() {
        var routing = RoutingConfiguration(mode: .bypassRules)

        XCTAssertTrue(RoutePlan.make(for: routing, chinaDirect: countrySet).excluded.isEmpty)

        routing.bypassChinaDirect = true
        let plan = RoutePlan.make(for: routing, chinaDirect: countrySet)

        XCTAssertTrue(plan.excluded.contains { $0.cidrText == "203.0.113.0/24" })
    }

    func testCustomRoutesAndRuleSetsCombineIntoOnePlan() {
        let routing = RoutingConfiguration(
            mode: .bypassRules,
            bypassLocalNetworks: true,
            bypassChinaDirect: true,
            customRoutes: ["8.8.8.8/32"]
        )

        let plan = RoutePlan.make(for: routing, chinaDirect: countrySet)

        XCTAssertTrue(plan.excluded.contains { $0.cidrText == "8.8.8.8/32" })
        XCTAssertTrue(plan.excluded.contains { $0.cidrText == "203.0.113.0/24" })
        XCTAssertTrue(plan.excluded.contains { $0.cidrText == "192.168.0.0/16" })
    }

    /// The summary is what the connection card shows, so it has to describe
    /// what is in force rather than which mode is selected.
    func testTheSummaryDescribesTheRulesActuallyInForce() {
        XCTAssertEqual(
            RoutingConfiguration(mode: .allTraffic).summary,
            RoutingMode.allTraffic.title
        )
        // Bypass mode with nothing enabled carries just as much as global mode.
        XCTAssertEqual(
            RoutingConfiguration(mode: .bypassRules).summary,
            RoutingMode.allTraffic.title
        )
        XCTAssertEqual(
            RoutingConfiguration(
                mode: .bypassRules,
                bypassLocalNetworks: true,
                bypassChinaDirect: true
            ).summary,
            "Bypassing local networks and Chinese addresses"
        )
        XCTAssertEqual(
            RoutingConfiguration(mode: .bypassRules, customRoutes: ["10.0.0.0/8"]).summary,
            "Bypassing 1 custom route"
        )
    }
}

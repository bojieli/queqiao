import XCTest

@testable import Queqiao

/// The screen's reading of a rule list. It decides nothing -- the core owns the
/// grammar -- but what it says has to agree with what the core will do, or the
/// user is told one thing and the tunnel does another.
final class RuleReviewTests: XCTestCase {
    func testItCountsTheRulesAndIgnoresCommentsAndBlanks() {
        let review = RuleReview(
            text: """
                # a comment
                ; another

                DOMAIN-SUFFIX,example.com,DIRECT
                GEOIP,CN,DIRECT
                FINAL,PROXY
                """
        )
        XCTAssertEqual(review.count, 3)
        XCTAssertTrue(review.problems.isEmpty)
        XCTAssertEqual(review.summary, "3 rules")
    }

    func testAnEmptyListSaysEveryFlowTakesTheTunnel() {
        let review = RuleReview(text: "\n  \n# only a comment\n")
        XCTAssertTrue(review.isEmpty)
        XCTAssertEqual(review.count, 0)
    }

    func testItNamesTheLineThatIsNotARuleType() {
        let review = RuleReview(text: "DOMAIN-SUFFIX,example.com,DIRECT\nNOT-A-TYPE,x,DIRECT")
        XCTAssertEqual(review.count, 1)
        XCTAssertEqual(review.problems.count, 1)
        XCTAssertTrue(review.problems[0].contains("Line 2"), review.problems[0])
    }

    /// The common failure by far: a list exported from a client with several
    /// outbounds, where the action names a proxy group. Saying so is more use
    /// than "invalid", and it is what the core says too.
    func testANamedProxyGroupIsExplained() {
        let review = RuleReview(text: "DOMAIN-SUFFIX,example.com,my-hong-kong-group")
        XCTAssertEqual(review.count, 0)
        XCTAssertEqual(review.problems.count, 1)
        XCTAssertTrue(review.problems[0].contains("one tunnel"), review.problems[0])
    }

    func testFinalTakesAnActionAndNoValue() {
        XCTAssertEqual(RuleReview(text: "FINAL,PROXY").count, 1)
        XCTAssertEqual(RuleReview(text: "MATCH,DIRECT").count, 1)
        let short = RuleReview(text: "FINAL")
        XCTAssertEqual(short.count, 0)
        XCTAssertEqual(short.problems.count, 1)
    }

    func testTheSpellingsInCirculationAreAccepted() {
        let review = RuleReview(
            text: """
                domain-suffix,example.com,direct
                IP-CIDR6,2001:db8::/32,REJECT
                PORT,443,PROXY
                GEOIP,cn,DIRECT,no-resolve
                """
        )
        XCTAssertEqual(review.count, 4, "rejected lines other tools write: \(review.problems)")
    }

    /// The preset is what a first-time user taps, so it has to be a list the
    /// core loads without complaint.
    func testTheChinaPresetIsItselfValid() {
        let review = RuleReview(text: RuleReview.chinaPreset)
        XCTAssertTrue(review.problems.isEmpty, "the preset does not review clean: \(review.problems)")
        XCTAssertGreaterThan(review.count, 4)
    }

    /// The preset exists because an address set cannot answer these.
    ///
    /// Each of these resolves to space the registry does not call Chinese, so
    /// `GEOIP,CN` never matches it and the flow takes the tunnel while the
    /// toggle says direct — measured, not assumed: ip138.com answers 138.113.x
    /// and bilibili.com 148.153.x from Chinese and foreign resolvers alike. If
    /// one of these lines is ever dropped, that symptom comes back, so the
    /// names are asserted rather than left to a reviewer's eye.
    func testThePresetNamesTheServicesAnAddressSetCannotReach() {
        let preset = RuleReview.chinaPreset
        for host in ["ip138.com", "bilibili.com", "lxdns.com", "hdslb.com"] {
            XCTAssertTrue(
                preset.contains("DOMAIN-SUFFIX,\(host),DIRECT"),
                "the preset no longer keeps \(host) direct by name"
            )
        }
    }

    /// First match wins, so a name rule placed after the address rule would
    /// never be reached for anything the address rule already claims.
    func testTheAddressRuleComesAfterTheNameRules() throws {
        let lines = RuleReview.chinaPreset
            .split(separator: "\n")
            .map { $0.trimmingCharacters(in: .whitespaces) }
        let geoip = try XCTUnwrap(lines.firstIndex(of: "GEOIP,CN,DIRECT"))
        let lastName = try XCTUnwrap(lines.lastIndex { $0.hasPrefix("DOMAIN-") })
        XCTAssertLessThan(lastName, geoip, "a name rule sits below GEOIP and cannot be reached")
    }

    /// A pasted list can be long and wrong in many places; the screen shows the
    /// first few rather than a wall of text.
    func testTheProblemListIsBounded() {
        let text = (1...40).map { "BAD-TYPE-\($0),x,DIRECT" }.joined(separator: "\n")
        XCTAssertLessThanOrEqual(RuleReview(text: text).problems.count, 10)
    }
}

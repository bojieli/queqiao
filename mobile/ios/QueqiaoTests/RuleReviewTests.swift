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
        XCTAssertEqual(review.count, 4)
    }

    /// A pasted list can be long and wrong in many places; the screen shows the
    /// first few rather than a wall of text.
    func testTheProblemListIsBounded() {
        let text = (1...40).map { "BAD-TYPE-\($0),x,DIRECT" }.joined(separator: "\n")
        XCTAssertLessThanOrEqual(RuleReview(text: text).problems.count, 10)
    }
}

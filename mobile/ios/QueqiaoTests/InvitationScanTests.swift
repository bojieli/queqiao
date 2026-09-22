import XCTest
@testable import Queqiao

final class InvitationScanTests: XCTestCase {
    func testRejectsCodesThatAreNotInvitations() {
        XCTAssertEqual(
            InvitationScan.evaluate("https://example.com/menu"),
            .rejected("That code is not a Queqiao invitation.")
        )
    }

    func testRejectsMalformedInvitationsBeforeTheyReachTheForm() {
        guard case .rejected(let reason) = InvitationScan.evaluate("queqiao://enroll/not-base64!") else {
            return XCTFail("a malformed invitation must be rejected at the scanner")
        }
        XCTAssertTrue(reason.hasPrefix("That invitation cannot be used"), reason)
    }

    func testTrimsWhitespaceBeforeJudgingTheScheme() {
        guard case .rejected(let reason) = InvitationScan.evaluate("  queqiao://enroll/\n") else {
            return XCTFail("an empty invitation body must be rejected, not accepted")
        }
        XCTAssertFalse(reason.contains("not a Queqiao invitation"), reason)
    }
}

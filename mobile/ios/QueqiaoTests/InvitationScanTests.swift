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

final class InvitationBatchTests: XCTestCase {
    /// A structurally valid invitation the core accepts. Nothing is enrolled
    /// in these tests, so the identifiers and token need only be well formed.
    private func invitation(name: String, endpoint: String, seed: UInt8) -> String {
        let bytes = { (value: UInt8) in
            Data(repeating: value, count: 32).base64EncodedString()
                .replacingOccurrences(of: "+", with: "-")
                .replacingOccurrences(of: "/", with: "_")
                .replacingOccurrences(of: "=", with: "")
        }
        let formatter = ISO8601DateFormatter()
        let payload: [String: Any] = [
            "v": 1,
            "name": name,
            "provider": String(repeating: "a", count: 32),
            "endpoint": endpoint,
            "gateway": String(repeating: "b", count: 32),
            "root": bytes(1),
            "token": bytes(seed),
            "expires": formatter.string(from: Date().addingTimeInterval(3600))
        ]
        let data = try? JSONSerialization.data(withJSONObject: payload, options: [.sortedKeys])
        let body = (data ?? Data()).base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
        return "queqiao://enroll/" + body
    }

    func testExtractsEveryInvitationFromOnePaste() {
        let first = invitation(name: "rtx-pro", endpoint: "198.51.100.1:12540", seed: 2)
        let second = invitation(name: "rtx-pro-hk", endpoint: "203.0.113.9:12548", seed: 3)
        let text = "rtx-pro: \(first)\n\n\"\(second)\",\n\(first)\nnot an invitation"
        XCTAssertEqual(InvitationBatch.extract(text), [first, second])
    }

    func testExtractStopsAtAFullStopEndingASentence() {
        let uri = invitation(name: "rtx-pro", endpoint: "198.51.100.1:12540", seed: 8)
        XCTAssertEqual(InvitationBatch.extract("Your invitation is \(uri)."), [uri])
    }

    func testTypedTextErrorSaysWhetherAnythingWasRecognised() {
        XCTAssertEqual(
            InvitationTextError.noUsableInvitation(0).errorDescription,
            "The text holds no Queqiao invitation."
        )
        XCTAssertEqual(
            InvitationTextError.noUsableInvitation(2).errorDescription,
            "The text holds no usable Queqiao invitation; 2 could not be used."
        )
    }

    func testExtractFindsNothingInUnrelatedText() {
        XCTAssertEqual(InvitationBatch.extract("vless://example\nhttps://example.com"), [])
    }

    func testEvaluateKeepsValidInvitationsAndCountsTheRest() {
        let valid = invitation(name: "proxy-a", endpoint: "198.51.100.2:12540", seed: 4)
        let result = InvitationBatch.evaluate("\(valid)\nqueqiao://enroll/not-base64!")
        XCTAssertEqual(result.accepted, [valid])
        XCTAssertEqual(result.rejected, 1)
    }

    func testDescribeLabelsAnInvitationFromItsOwnPayload() {
        let uri = invitation(name: "proxy-b-cn", endpoint: "203.0.113.7:12542", seed: 5)
        let item = InvitationBatch.describe(uri)
        XCTAssertEqual(item.name, "proxy-b-cn")
        XCTAssertEqual(item.endpoint, "203.0.113.7:12542")
        XCTAssertEqual(item.uri, uri)
    }

    func testDescribeFallsBackRatherThanFailing() {
        let item = InvitationBatch.describe("queqiao://enroll/%%%")
        XCTAssertEqual(item.name, "Queqiao invitation")
        XCTAssertEqual(item.endpoint, "")
    }

    func testPasteSummaryNamesWhatHappened() {
        XCTAssertEqual(
            InvitationBatch.pasteSummary(added: 0, alreadyQueued: 0, rejected: 0),
            "The clipboard holds no Queqiao invitation."
        )
        XCTAssertEqual(InvitationBatch.pasteSummary(added: 1, alreadyQueued: 0, rejected: 0), "Added 1 invitation.")
        XCTAssertEqual(
            InvitationBatch.pasteSummary(added: 9, alreadyQueued: 2, rejected: 1),
            "Added 9 invitations; 2 already queued; 1 could not be used."
        )
    }

    @MainActor
    func testQueueingSkipsInvitationsAlreadyQueued() {
        let model = TunnelModel()
        let first = invitation(name: "rtx-pro", endpoint: "198.51.100.1:12540", seed: 6)
        let second = invitation(name: "proxy-a", endpoint: "198.51.100.2:12540", seed: 7)
        XCTAssertEqual(model.queueInvitations([first, second]), 2)
        XCTAssertEqual(model.queueInvitations([second, first]), 0)
        XCTAssertEqual(model.queuedInvitations.map(\.name), ["rtx-pro", "proxy-a"])
    }
}

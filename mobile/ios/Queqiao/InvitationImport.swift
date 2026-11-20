import Foundation

/// One invitation waiting in the import queue. Only `uri` is ever sent
/// anywhere; `name` and `endpoint` are read locally from the invitation's own
/// payload so the queue can say which provider each entry is before anything
/// is enrolled.
struct QueuedInvitation: Identifiable, Equatable {
    let uri: String
    let name: String
    let endpoint: String

    var id: String { uri }
}

/// Batch handling for pasted or scanned invitations, kept apart from the views
/// so it can be tested without a pasteboard or camera.
enum InvitationBatch {
    /// Characters that cannot occur inside an invitation URI and commonly
    /// surround one in a message, a list or a quoted line. A full stop is one:
    /// neither the scheme nor base64url contains it, and prose ends with it.
    /// So are Markdown's backtick and asterisk, which wrap a pasted code span.
    private static let separators = CharacterSet.whitespacesAndNewlines
        .union(CharacterSet(charactersIn: "\"'<>()[]{},;|.`*"))

    /// Every `queqiao://` invitation in `text`, in order, without duplicates.
    /// A provider operator can hand over several invitations in one message --
    /// one per line, or as the output of several `provider invite` runs -- and
    /// one paste should take all of them. Candidates are not validated here.
    static func extract(_ text: String) -> [String] {
        var seen = Set<String>()
        var invitations: [String] = []
        for token in text.components(separatedBy: separators)
        where token.hasPrefix(InvitationScan.scheme) && seen.insert(token).inserted {
            invitations.append(token)
        }
        return invitations
    }

    /// The invitations in `text` that validate, and how many candidates did not.
    static func evaluate(_ text: String) -> (accepted: [String], rejected: Int) {
        var accepted: [String] = []
        var rejected = 0
        for candidate in extract(text) {
            if case .accepted(let invitation) = InvitationScan.evaluate(candidate) {
                accepted.append(invitation)
            } else {
                rejected += 1
            }
        }
        return (accepted, rejected)
    }

    /// Reads the display fields from an invitation's payload. The payload is
    /// validated by the core before it is queued; this only labels it, and
    /// falls back to a neutral label rather than failing.
    static func describe(_ uri: String) -> QueuedInvitation {
        let fallback = QueuedInvitation(uri: uri, name: "Queqiao invitation", endpoint: "")
        guard let range = uri.range(of: "\(InvitationScan.scheme)enroll/") else { return fallback }
        var payload = String(uri[range.upperBound...])
            .replacingOccurrences(of: "-", with: "+")
            .replacingOccurrences(of: "_", with: "/")
        payload += String(repeating: "=", count: (4 - payload.count % 4) % 4)
        guard let data = Data(base64Encoded: payload),
              let object = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            return fallback
        }
        let name = (object["name"] as? String).flatMap { $0.isEmpty ? nil : $0 } ?? fallback.name
        return QueuedInvitation(uri: uri, name: name, endpoint: object["endpoint"] as? String ?? "")
    }

    /// What one paste did, in one sentence.
    static func pasteSummary(added: Int, alreadyQueued: Int, rejected: Int) -> String {
        if added == 0 && alreadyQueued == 0 && rejected == 0 {
            return "The clipboard holds no Queqiao invitation."
        }
        var parts: [String] = []
        if added > 0 {
            parts.append(added == 1 ? "Added 1 invitation" : "Added \(added) invitations")
        }
        if alreadyQueued > 0 {
            parts.append("\(alreadyQueued) already queued")
        }
        if rejected > 0 {
            parts.append(rejected == 1 ? "1 could not be used" : "\(rejected) could not be used")
        }
        return parts.joined(separator: "; ") + "."
    }
}

extension TunnelModel {
    /// Adds invitations to the import queue, skipping any already queued, and
    /// returns how many were new.
    @discardableResult
    func queueInvitations(_ invitations: [String]) -> Int {
        var added = 0
        for uri in invitations where !queuedInvitations.contains(where: { $0.uri == uri }) {
            queuedInvitations.append(InvitationBatch.describe(uri))
            added += 1
        }
        return added
    }

    /// Validates the manual-entry text like a paste and queues what it holds.
    /// Text with no usable invitation stays in the field so it can be
    /// corrected, and the import does not start.
    func queueTypedInvitations() -> Bool {
        guard !invitation.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return true }
        let typed = InvitationBatch.evaluate(invitation)
        guard !typed.accepted.isEmpty else {
            present(InvitationTextError.noUsableInvitation(typed.rejected), title: "Profile import failed")
            return false
        }
        queueInvitations(typed.accepted)
        invitation = ""
        return true
    }

    func presentImporter(scanning: Bool) {
        importerStartsScanning = scanning
        isImporterPresented = true
    }

    /// Generates the device key for one invitation and persists the draft
    /// before anything is sent, exactly as a single import always has.
    nonisolated static func prepareEnrollmentDraft(invitation: String, deviceName: String) async throws {
        try await Task.detached(priority: .userInitiated) {
            let store = try ProfileStore()
            // The cached hasDraft can be stale; the store is the authority.
            // Replacing a draft would lose a device key whose invitation may
            // already be consumed.
            guard try store.enrollmentDraft() == nil else { throw ModelError.pendingEnrollment }
            try MobileCore.validateInvitation(invitation)
            let draft = try MobileCore.prepareEnrollment(invitation: invitation, deviceName: deviceName)
            try store.saveEnrollmentDraft(draft)
        }.value
    }

    /// Sends the saved draft, stores the resulting profile, and only then
    /// discards the draft, so an interrupted exchange can be resumed.
    nonisolated static func completeEnrollmentDraft() async throws {
        try await Task.detached(priority: .userInitiated) {
            let store = try ProfileStore()
            guard let draft = try store.enrollmentDraft() else { throw ModelError.emptyCoreResult }
            let profile = try MobileCore.completeEnrollment(draft: draft)
            _ = try store.importProfile(profile)
            try store.discardEnrollmentDraft()
        }.value
    }
}

/// Why typed text could not be imported.
enum InvitationTextError: LocalizedError, Equatable {
    case noUsableInvitation(Int)

    var errorDescription: String? {
        switch self {
        case .noUsableInvitation(let rejected):
            return rejected == 0
                ? "The text holds no Queqiao invitation."
                : "The text holds no usable Queqiao invitation; \(rejected) could not be used."
        }
    }
}

import SwiftUI
import UIKit

/// Collects any number of invitations -- scanned one after another, pasted
/// several at a time, or typed -- and imports them as one batch. Each
/// invitation still becomes its own device-bound profile with its own key.
struct ImportProfileView: View {
    @EnvironmentObject private var model: TunnelModel
    @Environment(\.dismiss) private var dismiss
    @State private var confirmDiscard = false
    @State private var isScannerPresented = false
    @State private var pasteNote: String?
    @State private var isManualEntryExpanded = false

    var body: some View {
        NavigationStack {
            Form {
                if model.hasDraft {
                    pendingSection
                }
                addSection
                if !model.queuedInvitations.isEmpty {
                    queueSection
                }
                manualSection
                deviceSection
                importSection
            }
            .navigationTitle("Import Profiles")
            .navigationBarTitleDisplayMode(.inline)
            .interactiveDismissDisabled(model.isBusy)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                        .disabled(model.isBusy)
                }
            }
            .sheet(isPresented: $isScannerPresented) {
                InvitationScannerView()
            }
            .onAppear {
                if model.importerStartsScanning {
                    model.importerStartsScanning = false
                    isScannerPresented = InvitationScannerView.isAvailable
                }
            }
            .confirmationDialog(
                "Discard pending enrollment?",
                isPresented: $confirmDiscard,
                titleVisibility: .visible
            ) {
                Button("Discard", role: .destructive) {
                    Task { await model.discardEnrollmentDraft() }
                }
                Button("Cancel", role: .cancel) {}
            } message: {
                Text(
                    "The pending device key will be deleted. The invitation may already " +
                    "be consumed and might not be reusable."
                )
            }
        }
    }

    private var pendingSection: some View {
        Section {
            Label(
                "An enrollment is ready to resume with its original device key.",
                systemImage: "arrow.clockwise.circle.fill"
            )
            .foregroundStyle(.orange)
            Button("Discard pending enrollment", role: .destructive) {
                confirmDiscard = true
            }
            .disabled(model.isBusy)
        } header: {
            Text("Pending import")
        }
    }

    private var addSection: some View {
        Section {
            if InvitationScannerView.isAvailable {
                Button {
                    isScannerPresented = true
                } label: {
                    Label("Scan QR codes", systemImage: "qrcode.viewfinder")
                }
            }
            // PasteButton reads the clipboard only when tapped, and without
            // the system's paste-permission prompt.
            PasteButton(payloadType: String.self) { values in
                Task { @MainActor in paste(values) }
            }
            .labelStyle(.titleAndIcon)
            if let pasteNote {
                Text(pasteNote)
                    .font(.footnote)
                    .foregroundStyle(.secondary)
            }
        } header: {
            Text("Add invitations")
        } footer: {
            Text(
                "Scan or paste as many invitations as you have; a paste takes every " +
                "invitation on the clipboard. Each one is used once, and its permanent " +
                "device key is generated on this iPhone and never leaves it."
            )
        }
        .disabled(model.isBusy)
    }

    private var queueSection: some View {
        Section {
            ForEach(model.queuedInvitations) { item in
                VStack(alignment: .leading, spacing: 2) {
                    Text(item.name)
                    if !item.endpoint.isEmpty {
                        Text(item.endpoint)
                            .font(.caption.monospaced())
                            .foregroundStyle(.secondary)
                    }
                }
            }
            .onDelete { offsets in
                model.queuedInvitations.remove(atOffsets: offsets)
            }
            .deleteDisabled(model.isBusy)
        } header: {
            Text("Ready to import (\(model.queuedInvitations.count))")
        }
    }

    private var manualSection: some View {
        Section {
            DisclosureGroup("Enter invitation text", isExpanded: $isManualEntryExpanded) {
                TextEditor(text: $model.invitation)
                    .frame(minHeight: 112)
                    .font(.footnote.monospaced())
                    .textInputAutocapitalization(.never)
                    .autocorrectionDisabled()
                    .privacySensitive()
                    .accessibilityLabel("Queqiao invitations")
            }
        }
        .disabled(model.isBusy)
    }

    private var deviceSection: some View {
        Section("This device") {
            TextField("Device name", text: $model.deviceName)
                .textInputAutocapitalization(.words)
        }
        .disabled(model.isBusy)
    }

    private var importSection: some View {
        Section {
            Button(importTitle) {
                Task { await model.enroll() }
            }
            .frame(maxWidth: .infinity)
            .disabled(model.isBusy || !hasRequiredInput)
            if model.isBusy {
                HStack(spacing: 8) {
                    ProgressView()
                    Text(model.status)
                        .font(.footnote)
                        .foregroundStyle(.secondary)
                }
                .frame(maxWidth: .infinity)
            }
        }
    }

    private var pendingCount: Int {
        let typed = InvitationBatch.extract(model.invitation).filter { uri in
            !model.queuedInvitations.contains(where: { $0.uri == uri })
        }
        return model.queuedInvitations.count + typed.count + (model.hasDraft ? 1 : 0)
    }

    private var importTitle: String {
        if model.hasDraft && pendingCount <= 1 {
            return "Resume import"
        }
        return pendingCount > 1 ? "Import \(pendingCount) profiles" : "Import profile"
    }

    private var hasRequiredInput: Bool {
        model.hasDraft || (
            pendingCount > 0 &&
            !model.deviceName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        )
    }

    private func paste(_ values: [String]) {
        let result = InvitationBatch.evaluate(values.joined(separator: "\n"))
        let added = model.queueInvitations(result.accepted)
        pasteNote = InvitationBatch.pasteSummary(
            added: added,
            alreadyQueued: result.accepted.count - added,
            rejected: result.rejected
        )
    }
}

/// Offers to import from the clipboard when it has changed and holds text,
/// the way other proxy apps notice a copied link. Only `hasStrings` and
/// `changeCount` are consulted until the user taps the system paste button,
/// so iOS shows no paste prompt and nothing is read in the background.
struct ClipboardInvitationBanner: View {
    @EnvironmentObject private var model: TunnelModel
    @State private var visible = false
    @State private var dismissedChangeCount = -1
    @State private var note: String?

    var body: some View {
        Group {
            if visible {
                VStack(alignment: .leading, spacing: 8) {
                    HStack {
                        Label("Copied invitations?", systemImage: "doc.on.clipboard")
                            .font(.subheadline.weight(.semibold))
                        Spacer()
                        Button {
                            dismiss()
                        } label: {
                            Image(systemName: "xmark.circle.fill")
                                .foregroundStyle(.secondary)
                        }
                        .accessibilityLabel("Dismiss")
                    }
                    PasteButton(payloadType: String.self) { values in
                        Task { @MainActor in paste(values) }
                    }
                    .labelStyle(.titleAndIcon)
                    if let note {
                        Text(note)
                            .font(.footnote)
                            .foregroundStyle(.secondary)
                    }
                }
                .padding(12)
                .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 14, style: .continuous))
                .padding(.horizontal)
            }
        }
        .onAppear(perform: refresh)
        .onReceive(NotificationCenter.default.publisher(for: UIApplication.didBecomeActiveNotification)) { _ in
            refresh()
        }
        .onReceive(NotificationCenter.default.publisher(for: UIPasteboard.changedNotification)) { _ in
            refresh()
        }
    }

    private func refresh() {
        let pasteboard = UIPasteboard.general
        visible = !model.isBusy && pasteboard.hasStrings && pasteboard.changeCount != dismissedChangeCount
        if !visible {
            note = nil
        }
    }

    private func dismiss() {
        dismissedChangeCount = UIPasteboard.general.changeCount
        visible = false
        note = nil
    }

    private func paste(_ values: [String]) {
        let result = InvitationBatch.evaluate(values.joined(separator: "\n"))
        let added = model.queueInvitations(result.accepted)
        if result.accepted.isEmpty {
            note = InvitationBatch.pasteSummary(added: 0, alreadyQueued: 0, rejected: result.rejected)
            return
        }
        dismiss()
        if added > 0 || !model.queuedInvitations.isEmpty {
            model.presentImporter(scanning: false)
        }
    }
}

import SwiftUI
import UIKit

struct ImportProfileView: View {
    @EnvironmentObject private var model: TunnelModel
    @Environment(\.dismiss) private var dismiss
    @State private var confirmDiscard = false
    @State private var isScannerPresented = false

    var body: some View {
        NavigationStack {
            Form {
                if model.hasDraft {
                    pendingSection
                } else {
                    invitationSection
                    deviceSection
                }
                importSection
            }
            .navigationTitle("Import Profile")
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
        } header: {
            Text("Pending import")
        }
    }

    private var invitationSection: some View {
        Section {
            TextEditor(text: $model.invitation)
                .frame(minHeight: 112)
                .font(.footnote.monospaced())
                .textInputAutocapitalization(.never)
                .autocorrectionDisabled()
                .privacySensitive()
                .accessibilityLabel("Queqiao invitation")
            if InvitationScannerView.isAvailable {
                Button {
                    isScannerPresented = true
                } label: {
                    Label("Scan QR code", systemImage: "qrcode.viewfinder")
                }
            }
            Button {
                if let value = UIPasteboard.general.string {
                    model.invitation = value.trimmingCharacters(in: .whitespacesAndNewlines)
                }
            } label: {
                Label("Paste invitation", systemImage: "doc.on.clipboard")
            }
        } header: {
            Text("One-time invitation")
        } footer: {
            Text(
                "The invitation is exchanged once. The permanent device key is generated " +
                "on this iPhone and never leaves it."
            )
        }
    }

    private var deviceSection: some View {
        Section("This device") {
            TextField("Device name", text: $model.deviceName)
                .textInputAutocapitalization(.words)
        }
    }

    private var importSection: some View {
        Section {
            Button(model.hasDraft ? "Resume import" : "Import profile") {
                Task { await model.enroll() }
            }
            .frame(maxWidth: .infinity)
            .disabled(model.isBusy || !hasRequiredInput)
        }
    }

    private var hasRequiredInput: Bool {
        model.hasDraft || (
            !model.invitation.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty &&
            !model.deviceName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        )
    }
}

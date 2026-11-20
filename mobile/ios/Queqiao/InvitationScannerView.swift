import AVFoundation
import SwiftUI
import UIKit

/// The scanner's verdict on one decoded code, kept apart from the view so it
/// can be tested without a camera.
enum InvitationScan: Equatable {
    case accepted(String)
    case rejected(String)

    static let scheme = "queqiao://"

    static func evaluate(_ value: String) -> InvitationScan {
        let candidate = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard candidate.hasPrefix(scheme) else {
            return .rejected("That code is not a Queqiao invitation.")
        }
        do {
            try MobileCore.validateInvitation(candidate)
        } catch {
            return .rejected("That invitation cannot be used: \(error.localizedDescription)")
        }
        return .accepted(candidate)
    }
}

/// Reads queqiao:// invitations from QR codes with the camera, for the case
/// where the invitations are on another screen and typing five hundred
/// characters of base64 is not an option. It keeps scanning: each new valid
/// code joins the import queue with a haptic tick, so a page of codes is read
/// by sweeping the camera across it and tapping Done once. Detection is
/// AVFoundation's own, so nothing but a system framework sees the frame, and
/// the decoded strings go only into the import form the user opened this
/// from. The session is held only while the sheet is on screen.
struct InvitationScannerView: View {
    static let defaultHint = "Point the camera at an invitation QR code. Scan as many as you like, then tap Done."
    static let permissionHint = "Queqiao needs the camera to read the code. Allow camera access for " +
        "Queqiao in Settings, or paste the invitation instead."

    static var isAvailable: Bool {
        AVCaptureDevice.default(for: .video) != nil
    }

    @EnvironmentObject private var model: TunnelModel
    @Environment(\.dismiss) private var dismiss
    @State private var authorization = CameraAuthorization.undetermined
    @State private var message = InvitationScannerView.defaultHint
    @State private var lastRejection = Date.distantPast
    @State private var lastDuplicate = Date.distantPast

    var body: some View {
        NavigationStack {
            ZStack {
                Color.black.ignoresSafeArea()
                content
                VStack {
                    Spacer()
                    Text(message)
                        .font(.callout)
                        .foregroundStyle(.white)
                        .multilineTextAlignment(.center)
                        .padding()
                        .frame(maxWidth: .infinity)
                        .background(.black.opacity(0.6))
                }
            }
            .navigationTitle(scanTitle)
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("Done") { dismiss() }
                }
            }
            .sensoryFeedback(.success, trigger: model.queuedInvitations.count)
            .task { await requestAccess() }
        }
    }

    @ViewBuilder
    private var content: some View {
        switch authorization {
        case .authorized:
            CameraPreview(onCode: handle)
                .ignoresSafeArea()
        case .denied:
            unavailable(Self.permissionHint, systemImage: "camera.badge.ellipsis")
        case .unavailable:
            unavailable("This device has no camera Queqiao can use.", systemImage: "camera.metering.unknown")
        case .undetermined:
            ProgressView()
                .tint(.white)
        }
    }

    private func unavailable(_ text: String, systemImage: String) -> some View {
        VStack(spacing: 12) {
            Image(systemName: systemImage)
                .font(.largeTitle)
            Text(text)
                .multilineTextAlignment(.center)
        }
        .foregroundStyle(.white)
        .padding(32)
    }

    private func requestAccess() async {
        guard Self.isAvailable else {
            authorization = .unavailable
            return
        }
        switch AVCaptureDevice.authorizationStatus(for: .video) {
        case .authorized:
            authorization = .authorized
        case .notDetermined:
            authorization = await AVCaptureDevice.requestAccess(for: .video) ? .authorized : .denied
        default:
            authorization = .denied
        }
    }

    private var scanTitle: String {
        let count = model.queuedInvitations.count
        return count == 0 ? "Scan invitations" : "\(count) ready to import"
    }

    private func handle(_ value: String) {
        let candidate = value.trimmingCharacters(in: .whitespacesAndNewlines)
        // The camera reports a code many times a second while it stays in
        // view; one already queued is acknowledged, not re-validated.
        if model.queuedInvitations.contains(where: { $0.uri == candidate }) {
            let now = Date()
            if now.timeIntervalSince(lastDuplicate) > 2 {
                lastDuplicate = now
                show("Already added. Point the camera at the next code, or tap Done.")
            }
            return
        }
        switch InvitationScan.evaluate(candidate) {
        case .accepted(let invitation):
            model.queueInvitations([invitation])
            // The same code stays in view for the next frames; keep them from
            // replacing this confirmation with "Already added" at once.
            lastDuplicate = Date()
            let name = InvitationBatch.describe(invitation).name
            show("Added \(name). Scan the next code, or tap Done.")
        case .rejected(let reason):
            reject(reason)
        }
    }

    private func show(_ text: String) {
        message = text
        Task {
            try? await Task.sleep(for: .seconds(3))
            if message == text {
                message = Self.defaultHint
            }
        }
    }

    private func reject(_ reason: String) {
        let now = Date()
        guard now.timeIntervalSince(lastRejection) > 1.5 else { return }
        lastRejection = now
        message = reason
        Task {
            try? await Task.sleep(for: .seconds(2.5))
            if message == reason {
                message = Self.defaultHint
            }
        }
    }
}

private enum CameraAuthorization {
    case undetermined
    case authorized
    case denied
    case unavailable
}

private struct CameraPreview: UIViewControllerRepresentable {
    let onCode: @MainActor (String) -> Void

    func makeUIViewController(context: Context) -> CameraPreviewController {
        CameraPreviewController(onCode: onCode)
    }

    func updateUIViewController(_ controller: CameraPreviewController, context: Context) {}
}

/// AVCaptureSession is not marked Sendable, but starting and stopping it off
/// the main thread is what Apple asks for. The box carries it to the session
/// queue without the compiler having to trust every closure that touches it.
private final class SessionBox: @unchecked Sendable {
    let session = AVCaptureSession()
}

private final class CameraPreviewController: UIViewController {
    private let box = SessionBox()
    private let sessionQueue = DispatchQueue(label: "io.github.bojieli.queqiao.scanner")
    private let onCode: @MainActor (String) -> Void
    private var previewLayer: AVCaptureVideoPreviewLayer?

    init(onCode: @escaping @MainActor (String) -> Void) {
        self.onCode = onCode
        super.init(nibName: nil, bundle: nil)
    }

    required init?(coder: NSCoder) {
        nil
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .black
        let layer = AVCaptureVideoPreviewLayer(session: box.session)
        layer.videoGravity = .resizeAspectFill
        view.layer.addSublayer(layer)
        previewLayer = layer
        configureSession()
    }

    override func viewDidLayoutSubviews() {
        super.viewDidLayoutSubviews()
        previewLayer?.frame = view.bounds
        updateRotation()
    }

    override func viewWillAppear(_ animated: Bool) {
        super.viewWillAppear(animated)
        let box = box
        sessionQueue.async {
            if !box.session.isRunning {
                box.session.startRunning()
            }
        }
    }

    override func viewWillDisappear(_ animated: Bool) {
        super.viewWillDisappear(animated)
        let box = box
        sessionQueue.async {
            if box.session.isRunning {
                box.session.stopRunning()
            }
        }
    }

    private func configureSession() {
        let device = AVCaptureDevice.default(.builtInWideAngleCamera, for: .video, position: .back)
            ?? AVCaptureDevice.default(for: .video)
        guard let device, let input = try? AVCaptureDeviceInput(device: device) else { return }
        let session = box.session
        session.beginConfiguration()
        defer { session.commitConfiguration() }
        guard session.canAddInput(input) else { return }
        session.addInput(input)
        let output = AVCaptureMetadataOutput()
        guard session.canAddOutput(output) else { return }
        session.addOutput(output)
        output.setMetadataObjectsDelegate(self, queue: .main)
        if output.availableMetadataObjectTypes.contains(.qr) {
            output.metadataObjectTypes = [.qr]
        }
    }

    /// The preview layer does not follow the interface on its own; a code is
    /// readable at any angle, but a sideways viewfinder is not usable.
    private func updateRotation() {
        guard let connection = previewLayer?.connection,
              let orientation = view.window?.windowScene?.interfaceOrientation else { return }
        let angle: CGFloat
        switch orientation {
        case .landscapeLeft:
            angle = 180
        case .landscapeRight:
            angle = 0
        case .portraitUpsideDown:
            angle = 270
        default:
            angle = 90
        }
        if connection.isVideoRotationAngleSupported(angle) {
            connection.videoRotationAngle = angle
        }
    }
}

extension CameraPreviewController: AVCaptureMetadataOutputObjectsDelegate {
    nonisolated func metadataOutput(
        _ output: AVCaptureMetadataOutput,
        didOutput metadataObjects: [AVMetadataObject],
        from connection: AVCaptureConnection
    ) {
        // Only the string crosses to the main actor. The delegate queue is
        // the main queue, so this is already there; the compiler cannot see
        // that through the Objective-C protocol.
        let value = metadataObjects.lazy.compactMap { object -> String? in
            guard let code = object as? AVMetadataMachineReadableCodeObject, code.type == .qr else { return nil }
            return code.stringValue
        }.first
        guard let value else { return }
        MainActor.assumeIsolated {
            onCode(value)
        }
    }
}

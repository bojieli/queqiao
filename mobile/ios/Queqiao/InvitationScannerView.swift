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

/// Reads a queqiao:// invitation from a QR code with the camera, for the case
/// where the invitation is on another screen and typing five hundred
/// characters of base64 is not an option. Detection is AVFoundation's own, so
/// nothing but a system framework sees the frame, and the decoded string goes
/// straight into the import form the user opened this from. The session is
/// held only while the sheet is on screen.
struct InvitationScannerView: View {
    static let defaultHint = "Point the camera at the invitation QR code shown on your computer."
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
    @State private var delivered = false

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
            .navigationTitle("Scan invitation")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
            }
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

    private func handle(_ value: String) {
        guard !delivered else { return }
        switch InvitationScan.evaluate(value) {
        case .accepted(let invitation):
            delivered = true
            model.invitation = invitation
            dismiss()
        case .rejected(let reason):
            reject(reason)
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

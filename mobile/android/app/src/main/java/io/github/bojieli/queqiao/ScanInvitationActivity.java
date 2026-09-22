package io.github.bojieli.queqiao;

import android.Manifest;
import android.app.Activity;
import android.content.Context;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.graphics.Color;
import android.graphics.ImageFormat;
import android.graphics.Matrix;
import android.graphics.RectF;
import android.graphics.SurfaceTexture;
import android.graphics.Typeface;
import android.hardware.camera2.CameraAccessException;
import android.hardware.camera2.CameraCaptureSession;
import android.hardware.camera2.CameraCharacteristics;
import android.hardware.camera2.CameraDevice;
import android.hardware.camera2.CameraManager;
import android.hardware.camera2.CameraMetadata;
import android.hardware.camera2.CaptureRequest;
import android.hardware.camera2.params.OutputConfiguration;
import android.hardware.camera2.params.SessionConfiguration;
import android.hardware.camera2.params.StreamConfigurationMap;
import android.media.Image;
import android.media.ImageReader;
import android.os.Bundle;
import android.os.Handler;
import android.os.HandlerThread;
import android.os.SystemClock;
import android.util.Size;
import android.view.Gravity;
import android.view.Surface;
import android.view.TextureView;
import android.view.View;
import android.view.WindowInsets;
import android.widget.Button;
import android.widget.FrameLayout;
import android.widget.LinearLayout;
import android.widget.TextView;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import mobilecore.Mobilecore;

/**
 * Reads a queqiao:// invitation out of a QR code with the device
 * camera, for the case where the invitation is on another screen and typing
 * five hundred characters of base64 is not an option.
 *
 * <p>The camera is the platform Camera2 API and the decoder is the Go core, so
 * no vision SDK is linked and no frame leaves the process. Frames arrive as
 * YUV_420_888; only the luminance plane is copied out and handed to Go, one
 * frame at a time, and whatever arrives while the previous frame is still
 * being decoded is dropped rather than queued. The camera is held only while
 * this activity is in front.
 *
 * <p>The result travels back to the caller in EXTRA_INVITATION. It
 * is a bearer credential: the activity neither stores nor logs it, and it
 * refuses codes that are not a Queqiao invitation rather than returning them.
 */
public final class ScanInvitationActivity extends Activity {
    static final String EXTRA_INVITATION = "io.github.bojieli.queqiao.INVITATION";

    private static final int REQUEST_CAMERA = 7101;
    private static final String INVITATION_SCHEME = "queqiao://";
    /**
     * The decoder's cost grows with frame area, and a code that fills a third
     * of a 720p frame is already six pixels per module for a full-size
     * invitation. Bigger frames would only make each attempt slower.
     */
    private static final int MAX_DECODE_SIDE = 1280;
    private static final int MAX_PREVIEW_PIXELS = 1920 * 1080;
    private static final long REJECTION_INTERVAL_MILLIS = 1500;
    private static final long HINT_RESTORE_MILLIS = 2500;
    private static final String DEFAULT_HINT =
            "Point the camera at the invitation QR code shown on your computer.";
    private static final String PERMISSION_HINT =
            "Queqiao needs the camera to read the code. Allow camera access for Queqiao "
                    + "in system settings, or paste the invitation instead.";

    private final ExecutorService decoder = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "queqiao-qr-decode");
        thread.setDaemon(true);
        return thread;
    });
    private final AtomicBoolean decoding = new AtomicBoolean();
    private final Object cameraLock = new Object();

    private UiKit ui;
    private TextureView preview;
    private TextView hint;
    private HandlerThread cameraThread;
    private Handler cameraHandler;
    private CameraDevice camera;
    private CameraCaptureSession session;
    private ImageReader reader;
    private Size previewSize;
    private int sensorOrientation;
    private boolean opening;
    private boolean permissionRequested;
    private boolean finished;
    private long lastRejectionAt;

    private final TextureView.SurfaceTextureListener surfaceListener =
            new TextureView.SurfaceTextureListener() {
                @Override
                public void onSurfaceTextureAvailable(SurfaceTexture surface, int width, int height) {
                    openCamera();
                }

                @Override
                public void onSurfaceTextureSizeChanged(SurfaceTexture surface, int width, int height) {
                    configureTransform(width, height);
                }

                @Override
                public boolean onSurfaceTextureDestroyed(SurfaceTexture surface) {
                    return true;
                }

                @Override
                public void onSurfaceTextureUpdated(SurfaceTexture surface) {
                }
            };

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        ui = new UiKit(this);
        setContentView(buildContent());
    }

    @Override
    protected void onResume() {
        super.onResume();
        if (checkSelfPermission(Manifest.permission.CAMERA) == PackageManager.PERMISSION_GRANTED) {
            startCamera();
        } else if (!permissionRequested) {
            permissionRequested = true;
            requestPermissions(new String[]{Manifest.permission.CAMERA}, REQUEST_CAMERA);
        }
    }

    @Override
    protected void onPause() {
        stopCamera();
        super.onPause();
    }

    @Override
    protected void onDestroy() {
        decoder.shutdownNow();
        super.onDestroy();
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, String[] permissions, int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
        if (requestCode != REQUEST_CAMERA) {
            return;
        }
        if (grantResults.length > 0 && grantResults[0] == PackageManager.PERMISSION_GRANTED) {
            startCamera();
        } else {
            hint.setText(PERMISSION_HINT);
        }
    }

    private View buildContent() {
        FrameLayout root = new FrameLayout(this);
        root.setBackgroundColor(Color.BLACK);

        preview = new TextureView(this);
        root.addView(preview, new FrameLayout.LayoutParams(
                FrameLayout.LayoutParams.MATCH_PARENT,
                FrameLayout.LayoutParams.MATCH_PARENT));

        TextView title = ui.text("Scan invitation", 18, Typeface.BOLD);
        title.setTextColor(Color.WHITE);
        title.setGravity(Gravity.CENTER_HORIZONTAL);
        title.setPadding(ui.dp(20), ui.dp(16), ui.dp(20), ui.dp(16));
        title.setBackgroundColor(0x99000000);
        root.addView(title, new FrameLayout.LayoutParams(
                FrameLayout.LayoutParams.MATCH_PARENT,
                FrameLayout.LayoutParams.WRAP_CONTENT,
                Gravity.TOP));

        LinearLayout panel = new LinearLayout(this);
        panel.setOrientation(LinearLayout.VERTICAL);
        panel.setPadding(ui.dp(20), ui.dp(16), ui.dp(20), ui.dp(16));
        panel.setBackgroundColor(0x99000000);
        hint = ui.text(DEFAULT_HINT, 15, Typeface.NORMAL);
        hint.setTextColor(Color.WHITE);
        hint.setGravity(Gravity.CENTER_HORIZONTAL);
        panel.addView(hint, UiKit.matchWrap());
        Button cancel = ui.secondaryButton("Cancel");
        cancel.setOnClickListener(view -> {
            setResult(RESULT_CANCELED);
            finish();
        });
        panel.addView(cancel, ui.topSpaced());
        root.addView(panel, new FrameLayout.LayoutParams(
                FrameLayout.LayoutParams.MATCH_PARENT,
                FrameLayout.LayoutParams.WRAP_CONTENT,
                Gravity.BOTTOM));

        root.setOnApplyWindowInsetsListener((view, insets) -> {
            android.graphics.Insets bars = insets.getInsets(WindowInsets.Type.systemBars());
            title.setPadding(ui.dp(20), bars.top + ui.dp(16), ui.dp(20), ui.dp(16));
            panel.setPadding(ui.dp(20), ui.dp(16), ui.dp(20), bars.bottom + ui.dp(16));
            return insets;
        });
        return root;
    }

    private void startCamera() {
        synchronized (cameraLock) {
            if (cameraThread == null) {
                cameraThread = new HandlerThread("queqiao-camera");
                cameraThread.start();
                cameraHandler = new Handler(cameraThread.getLooper());
            }
        }
        if (preview.isAvailable()) {
            openCamera();
        } else {
            preview.setSurfaceTextureListener(surfaceListener);
        }
    }

    private void stopCamera() {
        synchronized (cameraLock) {
            opening = false;
            if (session != null) {
                session.close();
                session = null;
            }
            if (camera != null) {
                camera.close();
                camera = null;
            }
            if (reader != null) {
                reader.close();
                reader = null;
            }
            if (cameraThread != null) {
                cameraThread.quitSafely();
                cameraThread = null;
                cameraHandler = null;
            }
        }
        preview.setSurfaceTextureListener(null);
    }

    private void openCamera() {
        // Lint wants the check in the method that opens the camera, and it is
        // right: onResume gates on it, but a surface can become available later.
        if (checkSelfPermission(Manifest.permission.CAMERA) != PackageManager.PERMISSION_GRANTED) {
            return;
        }
        synchronized (cameraLock) {
            if (cameraHandler == null || camera != null || opening || finished) {
                return;
            }
            opening = true;
        }
        CameraManager manager = getSystemService(CameraManager.class);
        try {
            String cameraId = chooseCamera(manager);
            if (cameraId == null) {
                showUnavailable("This device has no camera Queqiao can use.");
                return;
            }
            CameraCharacteristics characteristics = manager.getCameraCharacteristics(cameraId);
            StreamConfigurationMap streams = characteristics.get(
                    CameraCharacteristics.SCALER_STREAM_CONFIGURATION_MAP);
            Integer orientation = characteristics.get(CameraCharacteristics.SENSOR_ORIENTATION);
            if (streams == null) {
                showUnavailable("The camera reports no usable output size.");
                return;
            }
            sensorOrientation = orientation == null ? 0 : orientation;
            Size decodeSize = chooseDecodeSize(streams.getOutputSizes(ImageFormat.YUV_420_888));
            if (decodeSize == null) {
                showUnavailable("The camera reports no usable output size.");
                return;
            }
            previewSize = choosePreviewSize(streams.getOutputSizes(SurfaceTexture.class), decodeSize);
            runOnUiThread(() -> configureTransform(preview.getWidth(), preview.getHeight()));
            ImageReader frames = ImageReader.newInstance(
                    decodeSize.getWidth(), decodeSize.getHeight(), ImageFormat.YUV_420_888, 2);
            Handler handler;
            synchronized (cameraLock) {
                handler = cameraHandler;
                if (handler == null) {
                    frames.close();
                    return;
                }
                reader = frames;
            }
            frames.setOnImageAvailableListener(this::onFrame, handler);
            manager.openCamera(cameraId, new CameraDevice.StateCallback() {
                @Override
                public void onOpened(CameraDevice device) {
                    synchronized (cameraLock) {
                        opening = false;
                        if (cameraHandler == null) {
                            device.close();
                            return;
                        }
                        camera = device;
                    }
                    createSession(device);
                }

                @Override
                public void onDisconnected(CameraDevice device) {
                    device.close();
                    synchronized (cameraLock) {
                        opening = false;
                        if (camera == device) {
                            camera = null;
                        }
                    }
                }

                @Override
                public void onError(CameraDevice device, int error) {
                    onDisconnected(device);
                    showUnavailable("The camera could not be opened (error " + error + ").");
                }
            }, handler);
        } catch (CameraAccessException | IllegalArgumentException | SecurityException exception) {
            synchronized (cameraLock) {
                opening = false;
            }
            showUnavailable("The camera could not be opened: " + exception.getMessage());
        }
    }

    private void createSession(CameraDevice device) {
        SurfaceTexture texture = preview.getSurfaceTexture();
        ImageReader frames;
        Handler handler;
        synchronized (cameraLock) {
            frames = reader;
            handler = cameraHandler;
        }
        if (texture == null || frames == null || handler == null) {
            return;
        }
        texture.setDefaultBufferSize(previewSize.getWidth(), previewSize.getHeight());
        Surface previewSurface = new Surface(texture);
        try {
            CaptureRequest.Builder request = device.createCaptureRequest(CameraDevice.TEMPLATE_PREVIEW);
            request.addTarget(previewSurface);
            request.addTarget(frames.getSurface());
            request.set(CaptureRequest.CONTROL_AF_MODE,
                    CameraMetadata.CONTROL_AF_MODE_CONTINUOUS_PICTURE);
            List<OutputConfiguration> outputs = new ArrayList<>();
            outputs.add(new OutputConfiguration(previewSurface));
            outputs.add(new OutputConfiguration(frames.getSurface()));
            device.createCaptureSession(new SessionConfiguration(
                    SessionConfiguration.SESSION_REGULAR,
                    outputs,
                    handler::post,
                    new CameraCaptureSession.StateCallback() {
                        @Override
                        public void onConfigured(CameraCaptureSession configured) {
                            synchronized (cameraLock) {
                                if (camera != device) {
                                    configured.close();
                                    return;
                                }
                                session = configured;
                            }
                            try {
                                configured.setRepeatingRequest(request.build(), null, handler);
                            } catch (CameraAccessException | IllegalStateException exception) {
                                showUnavailable("The camera stopped: " + exception.getMessage());
                            }
                        }

                        @Override
                        public void onConfigureFailed(CameraCaptureSession failed) {
                            showUnavailable("The camera refused the preview configuration.");
                        }
                    }));
        } catch (CameraAccessException | IllegalStateException | IllegalArgumentException exception) {
            showUnavailable("The camera could not start: " + exception.getMessage());
        }
    }

    private static String chooseCamera(CameraManager manager) throws CameraAccessException {
        String fallback = null;
        for (String id : manager.getCameraIdList()) {
            CameraCharacteristics characteristics = manager.getCameraCharacteristics(id);
            Integer facing = characteristics.get(CameraCharacteristics.LENS_FACING);
            if (facing != null && facing == CameraCharacteristics.LENS_FACING_BACK) {
                return id;
            }
            if (fallback == null) {
                fallback = id;
            }
        }
        return fallback;
    }

    /** The largest frame the decoder is asked to read; see MAX_DECODE_SIDE. */
    private static Size chooseDecodeSize(Size[] candidates) {
        Size best = null;
        if (candidates == null) {
            return null;
        }
        for (Size size : candidates) {
            if (size.getWidth() > MAX_DECODE_SIDE || size.getHeight() > MAX_DECODE_SIDE) {
                continue;
            }
            if (best == null || area(size) > area(best)) {
                best = size;
            }
        }
        if (best == null && candidates.length > 0) {
            // Every size is enormous. Take the smallest and let the decoder be slow.
            best = candidates[0];
            for (Size size : candidates) {
                if (area(size) < area(best)) {
                    best = size;
                }
            }
        }
        return best;
    }

    /**
     * The preview must share the decode stream's aspect ratio, or the camera
     * crops one of them and the code the user has centred on screen is not
     * the one the decoder sees.
     */
    private static Size choosePreviewSize(Size[] candidates, Size decodeSize) {
        Size best = decodeSize;
        if (candidates == null) {
            return best;
        }
        double wanted = (double) decodeSize.getWidth() / decodeSize.getHeight();
        for (Size size : candidates) {
            double ratio = (double) size.getWidth() / size.getHeight();
            if (Math.abs(ratio - wanted) > 0.01 || area(size) > MAX_PREVIEW_PIXELS) {
                continue;
            }
            if (area(size) > area(best)) {
                best = size;
            }
        }
        return best;
    }

    private static long area(Size size) {
        return (long) size.getWidth() * size.getHeight();
    }

    /**
     * The system makes the preview upright for the device's natural
     * orientation, then the view stretches it to its own bounds. This undoes
     * the stretch and covers the view (a viewfinder wants no letterbox), and
     * rotates it back when the display itself is turned.
     */
    private void configureTransform(int viewWidth, int viewHeight) {
        Size size = previewSize;
        if (size == null || viewWidth == 0 || viewHeight == 0 || getDisplay() == null) {
            return;
        }
        int rotation = getDisplay().getRotation();
        boolean sensorSideways = sensorOrientation % 180 == 90;
        float uprightWidth = sensorSideways ? size.getHeight() : size.getWidth();
        float uprightHeight = sensorSideways ? size.getWidth() : size.getHeight();
        float centerX = viewWidth / 2f;
        float centerY = viewHeight / 2f;
        Matrix matrix = new Matrix();
        if (rotation == Surface.ROTATION_90 || rotation == Surface.ROTATION_270) {
            RectF viewRect = new RectF(0, 0, viewWidth, viewHeight);
            RectF bufferRect = new RectF(0, 0, uprightWidth, uprightHeight);
            bufferRect.offset(centerX - bufferRect.centerX(), centerY - bufferRect.centerY());
            matrix.setRectToRect(viewRect, bufferRect, Matrix.ScaleToFit.FILL);
            float scale = Math.max(viewHeight / uprightWidth, viewWidth / uprightHeight);
            matrix.postScale(scale, scale, centerX, centerY);
            matrix.postRotate(rotation == Surface.ROTATION_90 ? -90 : 90, centerX, centerY);
        } else {
            float stretchX = viewWidth / uprightWidth;
            float stretchY = viewHeight / uprightHeight;
            float scale = Math.max(stretchX, stretchY);
            matrix.setScale(scale / stretchX, scale / stretchY, centerX, centerY);
            if (rotation == Surface.ROTATION_180) {
                matrix.postRotate(180, centerX, centerY);
            }
        }
        preview.setTransform(matrix);
    }

    private void onFrame(ImageReader source) {
        Image image = source.acquireLatestImage();
        if (image == null) {
            return;
        }
        try {
            if (finished || !decoding.compareAndSet(false, true)) {
                return;
            }
            int width = image.getWidth();
            int height = image.getHeight();
            byte[] luma = copyLuminance(image.getPlanes()[0], width, height);
            try {
                decoder.execute(() -> decode(luma, width, height));
            } catch (RejectedExecutionException ignored) {
                decoding.set(false);
            }
        } catch (IllegalStateException ignored) {
            // The image was closed underneath us because the camera stopped.
            decoding.set(false);
        } finally {
            image.close();
        }
    }

    /**
     * The Y plane is exactly the grayscale frame the decoder wants, except
     * that rows may be padded and, on some devices, pixels interleaved.
     */
    private static byte[] copyLuminance(Image.Plane plane, int width, int height) {
        ByteBuffer buffer = plane.getBuffer();
        int rowStride = plane.getRowStride();
        int pixelStride = plane.getPixelStride();
        byte[] luma = new byte[width * height];
        if (pixelStride == 1 && rowStride == width && buffer.remaining() >= luma.length) {
            buffer.get(luma);
            return luma;
        }
        byte[] row = new byte[rowStride];
        for (int y = 0; y < height; y++) {
            int offset = y * rowStride;
            if (offset >= buffer.limit()) {
                break;
            }
            buffer.position(offset);
            int length = Math.min(rowStride, buffer.remaining());
            buffer.get(row, 0, length);
            for (int x = 0; x < width && x * pixelStride < length; x++) {
                luma[y * width + x] = row[x * pixelStride];
            }
        }
        return luma;
    }

    private void decode(byte[] luma, int width, int height) {
        String text;
        try {
            text = Mobilecore.decodeQRCode(luma, width, height);
        } catch (Exception exception) {
            text = "";
        } finally {
            decoding.set(false);
        }
        if (text == null || text.isBlank()) {
            return;
        }
        String candidate = text.trim();
        if (!candidate.startsWith(INVITATION_SCHEME)) {
            reject("That code is not a Queqiao invitation.");
            return;
        }
        try {
            Mobilecore.validateInvitation(candidate);
        } catch (Exception exception) {
            String reason = exception.getMessage();
            reject(reason == null || reason.isBlank()
                    ? "That invitation cannot be used."
                    : "That invitation cannot be used: " + reason);
            return;
        }
        runOnUiThread(() -> deliver(candidate));
    }

    private void deliver(String invitation) {
        if (finished || isFinishing()) {
            return;
        }
        finished = true;
        setResult(RESULT_OK, new Intent().putExtra(EXTRA_INVITATION, invitation));
        finish();
    }

    private void reject(String message) {
        long now = SystemClock.elapsedRealtime();
        if (now - lastRejectionAt < REJECTION_INTERVAL_MILLIS) {
            return;
        }
        lastRejectionAt = now;
        runOnUiThread(() -> {
            hint.setText(message);
            hint.postDelayed(() -> {
                if (!finished) {
                    hint.setText(DEFAULT_HINT);
                }
            }, HINT_RESTORE_MILLIS);
        });
    }

    private void showUnavailable(String message) {
        runOnUiThread(() -> hint.setText(message));
    }

    static boolean hasCamera(Context context) {
        return context.getPackageManager().hasSystemFeature(PackageManager.FEATURE_CAMERA_ANY);
    }

}

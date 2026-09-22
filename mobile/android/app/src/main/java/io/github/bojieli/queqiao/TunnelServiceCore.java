package io.github.bojieli.queqiao;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Intent;
import android.os.Handler;
import android.os.Looper;
import android.util.Log;

import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import mobilecore.Mobilecore;
import mobilecore.Observer;
import mobilecore.Session;

/**
 * The connection lifecycle both services share: at most one core session,
 * generation-guarded so a superseded start cannot install itself over a newer
 * one, plus profile renewal, state broadcasts, the foreground notification, and
 * periodic metrics.
 *
 * A delegate rather than a base class, because the full tunnel must extend
 * VpnService and the exported proxy must not — they cannot share an ancestor.
 * What differs between them is only how a session is opened, which is what
 * the Backend interface supplies.
 */
final class TunnelServiceCore implements Observer {
    private static final String TAG = "QueqiaoTunnel";
    /** The variant-specific half of a connection. */
    interface Backend {
        /** Mode identifier reported in every broadcast; see TunnelController.modeId. */
        String modeId();

        /**
         * Opens whatever this variant needs and starts the core session. Runs off
         * the main thread. The stillCurrent supplier reports whether this start is
         * still the newest one, so an expensive resource can be skipped once the
         * start has been superseded.
         */
        Session start(ProfileRepository.ActiveProfile profile, Observer observer, BooleanSupplier stillCurrent)
                throws Exception;

        /**
         * Releases whatever start opened. Called on every exit path,
         * including one that races a start, so it must be idempotent.
         */
        void release();

        /** Trailing notification text such as a listen address, or null. */
        String notificationDetail();
    }

    private static final String NOTIFICATION_CHANNEL = "queqiao_connection";
    private static final int NOTIFICATION_ID = 1001;
    private static final long METRICS_INTERVAL_MILLIS = 5000;

    private final Service service;
    private final Backend backend;
    private final Object lifecycleLock = new Object();
    private final AtomicBoolean stopping = new AtomicBoolean();
    private final AtomicBoolean releasingMemory = new AtomicBoolean();
    private final AtomicLong lifecycleGeneration = new AtomicLong();
    private final Handler mainHandler = new Handler(Looper.getMainLooper());
    private boolean startInProgress;
    private Session session;
    private String activeProfileId;
    private String activeProfileName;

    TunnelServiceCore(Service service, Backend backend) {
        this.service = service;
        this.backend = backend;
    }

    private final Runnable metricsPublisher = new Runnable() {
        @Override
        public void run() {
            String state;
            String metrics;
            synchronized (lifecycleLock) {
                if (session == null) {
                    return;
                }
                state = session.state();
                metrics = session.metricsJSON();
            }
            publishState(state, null, metrics);
            if (Mobilecore.StateRunning.equals(state)) {
                mainHandler.postDelayed(this, METRICS_INTERVAL_MILLIS);
            }
        }
    };

    void onCreate() {
        NotificationChannel channel = new NotificationChannel(
                NOTIFICATION_CHANNEL,
                service.getString(R.string.connection_channel_name),
                NotificationManager.IMPORTANCE_LOW);
        channel.setDescription(service.getString(R.string.connection_channel_description));
        service.getSystemService(NotificationManager.class).createNotificationChannel(channel);
    }

    int onStartCommand(Intent intent, int startId) {
        String action = intent == null ? null : intent.getAction();
        if (TunnelBroadcast.ACTION_DISCONNECT.equals(action)) {
            stop(null);
            return Service.START_NOT_STICKY;
        }
        if (TunnelBroadcast.ACTION_STATUS.equals(action)) {
            publishState(currentState(), null, currentMetrics());
            synchronized (lifecycleLock) {
                if (session == null && !startInProgress) {
                    service.stopSelf(startId);
                }
            }
            return Service.START_NOT_STICKY;
        }
        if (!TunnelBroadcast.ACTION_CONNECT.equals(action)) {
            service.stopSelf(startId);
            return Service.START_NOT_STICKY;
        }

        String requestedProfileId = intent.getStringExtra(TunnelBroadcast.EXTRA_PROFILE_ID);
        if (requestedProfileId == null || requestedProfileId.isBlank()) {
            publishState(Mobilecore.StateFailed, "No Queqiao profile was selected", null);
            service.stopSelf(startId);
            return Service.START_NOT_STICKY;
        }

        service.startForeground(NOTIFICATION_ID, notification("Starting", false));
        long generation;
        synchronized (lifecycleLock) {
            if (session != null) {
                if (!requestedProfileId.equals(activeProfileId)) {
                    publishState(
                            session.state(),
                            "Disconnect before switching the active Queqiao profile",
                            session.metricsJSON());
                } else {
                    publishState(session.state(), null, session.metricsJSON());
                }
                return Service.START_STICKY;
            }
            if (startInProgress || stopping.get()) {
                publishState(Mobilecore.StateStarting, null, null);
                return Service.START_STICKY;
            }
            startInProgress = true;
            generation = lifecycleGeneration.incrementAndGet();
            activeProfileId = requestedProfileId;
        }
        new Thread(() -> startSession(requestedProfileId, generation), "queqiao-session-start").start();
        return Service.START_STICKY;
    }

    private void startSession(String profileId, long generation) {
        Session started = null;
        boolean installed = false;
        try {
            ProfileRepository repository = new ProfileRepository(service);
            ProfileRepository.ActiveProfile active = repository.profile(profileId);
            if (active == null) {
                throw new GeneralSecurityException("The selected Queqiao profile no longer exists");
            }
            if (Mobilecore.profileNeedsRenewal(active.profileJson) != 0) {
                String renewed = Mobilecore.renewProfile(active.profileJson);
                Mobilecore.validateProfile(renewed);
                repository.replaceProfile(profileId, renewed);
                active = repository.profile(profileId);
                if (active == null) {
                    throw new GeneralSecurityException("The renewed Queqiao profile is unavailable");
                }
            }
            String profileName = active.record.displayName;
            synchronized (lifecycleLock) {
                if (generation != lifecycleGeneration.get() || stopping.get()) {
                    return;
                }
                activeProfileName = profileName;
            }
            started = backend.start(active, new GenerationObserver(generation), () -> isCurrentStart(generation));
            synchronized (lifecycleLock) {
                installed = generation == lifecycleGeneration.get() && !stopping.get();
                if (installed) {
                    session = started;
                }
            }
            if (installed) {
                mainHandler.post(metricsPublisher);
            }
        } catch (Exception exception) {
            // The UI shows a safe summary; the cause has to be findable somewhere.
            Log.w(TAG, "The connection could not be started", exception);
            if (generation == lifecycleGeneration.get() && !stopping.get()) {
                publishState(Mobilecore.StateFailed, safeMessage(exception), null);
                stop("Connection failed");
            }
        } finally {
            if (!installed) {
                if (started != null) {
                    try {
                        started.stop();
                    } catch (Exception ignored) {
                        // A superseded startup owns no observable state.
                    }
                }
                backend.release();
            }
            synchronized (lifecycleLock) {
                if (generation == lifecycleGeneration.get()) {
                    startInProgress = false;
                }
            }
        }
    }

    private boolean isCurrentStart(long generation) {
        synchronized (lifecycleLock) {
            return generation == lifecycleGeneration.get() && startInProgress && !stopping.get();
        }
    }

    /**
     * Filters callbacks from a session that has since been superseded, so a slow
     * teardown cannot overwrite the state of the connection that replaced it.
     */
    private final class GenerationObserver implements Observer {
        private final long generation;

        GenerationObserver(long generation) {
            this.generation = generation;
        }

        private boolean current() {
            return generation == lifecycleGeneration.get() && !stopping.get();
        }

        @Override
        public void onStateChanged(String state) {
            if (current()) {
                TunnelServiceCore.this.onStateChanged(state);
            }
        }

        @Override
        public void onLog(String level, String message) {
            if (current()) {
                TunnelServiceCore.this.onLog(level, message);
            }
        }

        @Override
        public boolean onProfileUpdated(String profileJson) {
            return current() && TunnelServiceCore.this.onProfileUpdated(profileJson);
        }
    }

    @Override
    public void onStateChanged(String state) {
        publishState(state, null, currentMetrics());
        service.getSystemService(NotificationManager.class).notify(
                NOTIFICATION_ID,
                notification(displayState(state), Mobilecore.StateRunning.equals(state)));
        if (Mobilecore.StateRunning.equals(state)) {
            mainHandler.removeCallbacks(metricsPublisher);
            mainHandler.post(metricsPublisher);
        } else if (Mobilecore.StateFailed.equals(state)) {
            stop("Connection failed");
        }
    }

    @Override
    public void onLog(String level, String message) {
        if ("ERROR".equalsIgnoreCase(level)) {
            publishState(currentState(), message, currentMetrics());
        }
    }

    @Override
    public boolean onProfileUpdated(String profileJson) {
        String profileId;
        synchronized (lifecycleLock) {
            profileId = activeProfileId;
        }
        if (profileId == null) {
            return false;
        }
        try {
            new ProfileRepository(service).replaceProfile(profileId, profileJson);
            return true;
        } catch (Exception exception) {
            publishState(
                    currentState(),
                    "Could not persist renewed device identity; renewal will be retried",
                    currentMetrics());
            return false;
        }
    }

    /**
     * Surfaces an advisory message against the current state and redraws the
     * notification, without pretending the connection changed.
     *
     * A backend uses this for a condition it learns about after the session is
     * already running — the exported listener discovering a VPN underneath it,
     * for instance. Reporting it as a state change would misrepresent a healthy
     * session as a failing one.
     */
    void advise(String message) {
        String state = currentState();
        publishState(state, message, currentMetrics());
        service.getSystemService(NotificationManager.class).notify(
                NOTIFICATION_ID,
                notification(displayState(state), Mobilecore.StateRunning.equals(state)));
    }

    void onDestroy() {
        boolean active;
        synchronized (lifecycleLock) {
            active = session != null || startInProgress;
        }
        if (active) {
            stop(null);
        } else {
            mainHandler.removeCallbacks(metricsPublisher);
        }
    }

    void onTrimMemory(int level) {
        if (level < Service.TRIM_MEMORY_RUNNING_LOW) {
            return;
        }
        releaseIdleMemory();
    }

    void onLowMemory() {
        releaseIdleMemory();
    }

    private void releaseIdleMemory() {
        if (!releasingMemory.compareAndSet(false, true)) {
            return;
        }
        new Thread(() -> {
            try {
                Mobilecore.releaseMemory();
            } finally {
                releasingMemory.set(false);
            }
        }, "queqiao-memory-release").start();
    }

    void stop(String reason) {
        if (!stopping.compareAndSet(false, true)) {
            return;
        }
        lifecycleGeneration.incrementAndGet();
        mainHandler.removeCallbacks(metricsPublisher);
        Session oldSession;
        synchronized (lifecycleLock) {
            oldSession = session;
            session = null;
            startInProgress = false;
        }
        if (oldSession != null) {
            try {
                oldSession.stop();
            } catch (Exception ignored) {
                // The state callback has already surfaced the failure.
            }
        }
        backend.release();
        publishState(Mobilecore.StateStopped, reason, null);
        synchronized (lifecycleLock) {
            activeProfileId = null;
            activeProfileName = null;
        }
        service.stopForeground(Service.STOP_FOREGROUND_REMOVE);
        service.stopSelf();
    }

    private void publishState(String state, String message, String metrics) {
        Intent update = new Intent(TunnelBroadcast.ACTION_STATE)
                .setPackage(service.getPackageName())
                .putExtra(TunnelBroadcast.EXTRA_MODE, backend.modeId())
                .putExtra(TunnelBroadcast.EXTRA_STATE, state);
        synchronized (lifecycleLock) {
            if (activeProfileId != null) {
                update.putExtra(TunnelBroadcast.EXTRA_PROFILE_ID, activeProfileId);
            }
        }
        if (message != null) {
            update.putExtra(TunnelBroadcast.EXTRA_MESSAGE, message);
        }
        if (metrics != null) {
            update.putExtra(TunnelBroadcast.EXTRA_METRICS, metrics);
        }
        service.sendBroadcast(update);
    }

    private String currentState() {
        synchronized (lifecycleLock) {
            if (session != null) {
                return session.state();
            }
            return startInProgress ? Mobilecore.StateStarting : Mobilecore.StateStopped;
        }
    }

    private String currentMetrics() {
        synchronized (lifecycleLock) {
            return session == null ? null : session.metricsJSON();
        }
    }

    private Notification notification(String state, boolean connected) {
        String profileName;
        synchronized (lifecycleLock) {
            profileName = activeProfileName;
        }
        PendingIntent contentIntent = PendingIntent.getActivity(
                service,
                0,
                new Intent(service, MainActivity.class),
                PendingIntent.FLAG_IMMUTABLE | PendingIntent.FLAG_UPDATE_CURRENT);
        PendingIntent disconnectIntent = PendingIntent.getService(
                service,
                1,
                new Intent(service, service.getClass()).setAction(TunnelBroadcast.ACTION_DISCONNECT),
                PendingIntent.FLAG_IMMUTABLE | PendingIntent.FLAG_UPDATE_CURRENT);
        List<String> parts = new ArrayList<>(3);
        if (profileName != null) {
            parts.add(profileName);
        }
        parts.add(state);
        String detail = backend.notificationDetail();
        if (detail != null && !detail.isBlank()) {
            parts.add(detail);
        }
        Notification.Builder builder = new Notification.Builder(service, NOTIFICATION_CHANNEL)
                .setSmallIcon(R.drawable.ic_queqiao)
                .setContentTitle("Queqiao")
                .setContentText(String.join(" · ", parts))
                .setContentIntent(contentIntent)
                .setCategory(Notification.CATEGORY_SERVICE)
                .setOngoing(connected)
                .setOnlyAlertOnce(true);
        if (connected) {
            builder.addAction(
                    new Notification.Action.Builder(null, "Disconnect", disconnectIntent).build());
        }
        return builder.build();
    }

    private static String displayState(String state) {
        if (state == null || state.isEmpty()) {
            return "Unknown";
        }
        return Character.toUpperCase(state.charAt(0)) + state.substring(1);
    }

    private static String safeMessage(Exception exception) {
        String message = exception.getMessage();
        return message == null || message.isBlank()
                ? exception.getClass().getSimpleName()
                : message;
    }
}

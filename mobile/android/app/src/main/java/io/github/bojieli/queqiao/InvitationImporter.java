package io.github.bojieli.queqiao;

import android.os.Handler;
import android.os.Looper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import mobilecore.Mobilecore;

/**
 * Enrolls queued invitations, one batch at a time for the whole process.
 *
 * <p>The batch outlives the activity that started it: a rotation or a
 * reclaimed activity must not start a second loop over the same queue and
 * draft, because two loops could redeem one draft twice or overwrite one
 * device key with another. So the executor and the running flag are
 * process-wide, every enrollment runs on that one thread, and the activity
 * only registers a listener for progress and the result. A result that
 * arrives while no activity is listening is held for the next one.
 *
 * <p>The decision logic lives in enroll, behind the small Store and
 * Core interfaces, so it can be checked with fakes.
 */
final class InvitationImporter {
    static final InvitationImporter SHARED = new InvitationImporter();

    static final String PENDING_DRAFT =
            "An interrupted enrollment must be resumed or discarded before another invitation is imported.";
    static final String NAME_REQUIRED = "A device name is required to import an invitation.";

    /** The encrypted draft and profile storage the loop needs. */
    interface Store {
        boolean hasDraft() throws Exception;

        String draft() throws Exception;

        void saveDraft(String draft) throws Exception;

        void discardDraft() throws Exception;

        void importProfile(String profile) throws Exception;
    }

    /** The Go core's enrollment calls. */
    interface Core {
        void validate(String invitation) throws Exception;

        String prepare(String invitation, String deviceName) throws Exception;

        String complete(String draft) throws Exception;
    }

    interface Progress {
        void update(String message);
    }

    /** Delivered on the main thread. */
    interface Listener {
        void onImportProgress(String message);

        /**
         * Returns false when the listener cannot show the result (its
         * activity is finishing); the importer then holds it for the next.
         */
        boolean onImportFinished(Result result);
    }

    /** Delivered on the main thread; failure is null when the draft is gone. */
    interface DiscardCallback {
        void onDiscarded(Exception failure);
    }

    static final class Result {
        final int imported;
        final int total;
        final Exception failure;

        Result(int imported, int total, Exception failure) {
            this.imported = imported;
            this.total = total;
            this.failure = failure;
        }
    }

    static final Core CORE = new Core() {
        @Override
        public void validate(String invitation) throws Exception {
            Mobilecore.validateInvitation(invitation);
        }

        @Override
        public String prepare(String invitation, String deviceName) throws Exception {
            return Mobilecore.prepareEnrollment(invitation, deviceName);
        }

        @Override
        public String complete(String draft) throws Exception {
            return Mobilecore.completeEnrollment(draft);
        }
    };

    static Store store(ProfileRepository repository) {
        return new Store() {
            @Override
            public boolean hasDraft() {
                return repository.hasEnrollmentDraft();
            }

            @Override
            public String draft() throws Exception {
                return repository.enrollmentDraft();
            }

            @Override
            public void saveDraft(String draft) throws Exception {
                repository.saveEnrollmentDraft(draft);
            }

            @Override
            public void discardDraft() throws Exception {
                repository.discardEnrollmentDraft();
            }

            @Override
            public void importProfile(String profile) throws Exception {
                repository.importProfile(profile);
            }
        };
    }

    private final ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "queqiao-import");
        thread.setDaemon(true);
        return thread;
    });
    private final Handler main = new Handler(Looper.getMainLooper());

    // Guarded by this.
    private boolean running;
    private String status = "";
    private Listener listener;
    private Result undelivered;

    /**
     * Enrolls the pending draft, if the store holds one, and then each of
     * invitations that is still queued, in order, with one device
     * name. Each invitation leaves the queue only once its draft is saved,
     * because from then on the draft -- not the invitation -- is what a
     * retry must resume; so a failure stops the batch with the failed
     * invitation either still queued (nothing was sent) or held as the
     * resumable draft, and the rest still queued. Invitations queued after
     * the batch started are not part of it.
     */
    static Result enroll(Store store, Core core, InvitationBatch.Queue queue,
            List<String> invitations, String deviceName, Progress progress) {
        int imported = 0;
        int total = invitations.size();
        try {
            boolean resuming = store.hasDraft();
            total += resuming ? 1 : 0;
            if (!invitations.isEmpty() && (deviceName == null || deviceName.trim().isEmpty())) {
                throw new IllegalArgumentException(NAME_REQUIRED);
            }
            if (resuming) {
                progress.update("Resuming import…");
                complete(store, core);
                imported++;
            }
            for (String invitation : invitations) {
                if (!queue.contains(invitation)) {
                    // Removed by the user since the batch started.
                    total--;
                    continue;
                }
                progress.update(total > 1
                        ? "Importing profile " + (imported + 1) + " of " + total + "…"
                        : "Importing profile…");
                // The store is the authority, not anything the screen cached.
                // Replacing a draft would lose a device key whose invitation
                // may already be consumed.
                if (store.hasDraft()) {
                    throw new IllegalStateException(PENDING_DRAFT);
                }
                core.validate(invitation);
                store.saveDraft(core.prepare(invitation, deviceName.trim()));
                queue.remove(invitation);
                complete(store, core);
                imported++;
            }
            return new Result(imported, total, null);
        } catch (Exception failure) {
            return new Result(imported, total, failure);
        }
    }

    /**
     * Sends the saved draft, stores the resulting profile, and only then
     * discards the draft, so an interrupted exchange can be resumed.
     */
    private static void complete(Store store, Core core) throws Exception {
        String draft = store.draft();
        if (draft == null) {
            throw new IllegalStateException("The pending enrollment is unavailable.");
        }
        store.importProfile(core.complete(draft));
        store.discardDraft();
    }

    /**
     * Starts a batch over the invitations queued right now. Returns false,
     * and starts nothing, when a batch is already running.
     */
    synchronized boolean start(Store store, InvitationBatch.Queue queue, String deviceName) {
        if (running) {
            return false;
        }
        running = true;
        status = "Importing…";
        List<String> invitations = new ArrayList<>();
        for (InvitationBatch.QueuedInvitation item : queue.snapshot()) {
            invitations.add(item.uri);
        }
        executor.execute(() -> {
            Result result = enroll(store, CORE, queue, invitations, deviceName, this::publishProgress);
            finish(result);
        });
        return true;
    }

    /**
     * Deletes the pending draft on the import thread, so a discard is ordered
     * with batches rather than racing one: it can never delete a draft a
     * batch has just saved for an invitation it already dequeued. Returns
     * false, and deletes nothing, while a batch or another discard runs;
     * Import is refused until the discard is done.
     */
    synchronized boolean discard(Store store, DiscardCallback callback) {
        if (running) {
            return false;
        }
        running = true;
        status = "Discarding pending enrollment…";
        executor.execute(() -> {
            Exception failure = null;
            try {
                store.discardDraft();
            } catch (Exception exception) {
                failure = exception;
            }
            Exception outcome = failure;
            main.post(() -> {
                synchronized (this) {
                    running = false;
                    status = "";
                }
                callback.onDiscarded(outcome);
            });
        });
        return true;
    }

    synchronized boolean isRunning() {
        return running;
    }

    synchronized String status() {
        return status;
    }

    /** Registers the current activity; delivers a result it missed. */
    synchronized void setListener(Listener value) {
        listener = value;
        if (value != null && undelivered != null) {
            Result result = undelivered;
            undelivered = null;
            main.post(() -> deliver(value, result));
        }
    }

    synchronized void clearListener(Listener value) {
        if (listener == value) {
            listener = null;
        }
    }

    private void publishProgress(String message) {
        synchronized (this) {
            status = message;
        }
        main.post(() -> {
            Listener current;
            synchronized (this) {
                current = listener;
            }
            if (current != null) {
                current.onImportProgress(message);
            }
        });
    }

    private void finish(Result result) {
        main.post(() -> {
            Listener current;
            synchronized (this) {
                running = false;
                status = "";
                current = listener;
                if (current == null) {
                    undelivered = result;
                    return;
                }
            }
            offer(current, result);
        });
    }

    private void deliver(Listener target, Result result) {
        synchronized (this) {
            if (listener != target) {
                undelivered = result;
                return;
            }
        }
        offer(target, result);
    }

    /** Hands the result over, or holds it when the listener cannot show it. */
    private void offer(Listener target, Result result) {
        if (!target.onImportFinished(result)) {
            synchronized (this) {
                undelivered = result;
            }
        }
    }
}

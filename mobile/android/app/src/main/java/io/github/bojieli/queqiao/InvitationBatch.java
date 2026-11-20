package io.github.bojieli.queqiao;

import org.json.JSONException;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import mobilecore.Mobilecore;

/**
 * Batch handling for pasted, shared, or scanned invitations, kept apart from
 * the activities so it can be checked without a clipboard or camera. Mirrors
 * the iOS InvitationBatch.
 */
final class InvitationBatch {
    static final String SCHEME = "queqiao://";
    static final String FALLBACK_NAME = "Queqiao invitation";
    private static final String ENROLL_PREFIX = SCHEME + "enroll/";
    /**
     * Characters that cannot occur inside an invitation URI and commonly
     * surround one in a message, a list or a quoted line. A full stop is one:
     * neither the scheme nor base64url contains it, and prose ends with it.
     * Whitespace is handled separately.
     * Backticks and asterisks come from Markdown code spans and emphasis.
     */
    private static final String SEPARATORS = "\"'<>()[]{},;|.`*";

    private InvitationBatch() {
    }

    /** Checks one invitation; throws with a reason when it cannot be used. */
    @FunctionalInterface
    interface Validator {
        void validate(String invitation) throws Exception;
    }

    /** The Go core's own check, the one enrollment applies. */
    static final Validator CORE = Mobilecore::validateInvitation;

    /**
     * One invitation waiting in the import queue. Only uri is ever
     * sent anywhere; name and endpoint are read locally from
     * the invitation's own payload so the queue can say which provider each
     * entry is before anything is enrolled.
     */
    static final class QueuedInvitation {
        final String uri;
        final String name;
        final String endpoint;

        QueuedInvitation(String uri, String name, String endpoint) {
            this.uri = uri;
            this.name = name;
            this.endpoint = endpoint;
        }
    }

    /** The invitations in a text that validate, and how many candidates did not. */
    static final class Evaluation {
        final List<String> accepted;
        final int rejected;

        Evaluation(List<String> accepted, int rejected) {
            this.accepted = Collections.unmodifiableList(accepted);
            this.rejected = rejected;
        }
    }

    /**
     * Every queqiao:// invitation in text, in order, without
     * duplicates. A provider operator can hand over several invitations in
     * one message -- one per line, or as the output of several
     * provider invite runs -- and one paste should take all of them.
     * Candidates are not validated here.
     */
    static List<String> extract(String text) {
        Set<String> seen = new LinkedHashSet<>();
        if (text == null) {
            return new ArrayList<>();
        }
        StringBuilder token = new StringBuilder();
        for (int index = 0; index <= text.length(); index++) {
            char value = index < text.length() ? text.charAt(index) : ' ';
            if (isSeparator(value)) {
                if (token.length() > 0) {
                    String candidate = token.toString();
                    if (candidate.startsWith(SCHEME)) {
                        seen.add(candidate);
                    }
                    token.setLength(0);
                }
            } else {
                token.append(value);
            }
        }
        return new ArrayList<>(seen);
    }

    private static boolean isSeparator(char value) {
        return Character.isWhitespace(value)
                || Character.isSpaceChar(value)
                || SEPARATORS.indexOf(value) >= 0;
    }

    /** Extracts every candidate in text and validates each one. */
    static Evaluation evaluate(String text, Validator validator) {
        List<String> accepted = new ArrayList<>();
        int rejected = 0;
        for (String candidate : extract(text)) {
            try {
                validator.validate(candidate);
                accepted.add(candidate);
            } catch (Exception invalid) {
                rejected++;
            }
        }
        return new Evaluation(accepted, rejected);
    }

    /**
     * Reads the display fields from an invitation's payload. The payload is
     * validated by the core before it is queued; this only labels it, and
     * falls back to a neutral label rather than failing.
     */
    static QueuedInvitation describe(String uri) {
        QueuedInvitation fallback = new QueuedInvitation(uri, FALLBACK_NAME, "");
        int start = uri == null ? -1 : uri.indexOf(ENROLL_PREFIX);
        if (start < 0) {
            return fallback;
        }
        try {
            byte[] payload = Base64.getUrlDecoder().decode(uri.substring(start + ENROLL_PREFIX.length()));
            JSONObject object = new JSONObject(new String(payload, StandardCharsets.UTF_8));
            Object name = object.opt("name");
            Object endpoint = object.opt("endpoint");
            return new QueuedInvitation(
                    uri,
                    name instanceof String && !((String) name).isEmpty() ? (String) name : FALLBACK_NAME,
                    endpoint instanceof String ? (String) endpoint : "");
        } catch (IllegalArgumentException | JSONException undecodable) {
            return fallback;
        }
    }

    /** What one paste did, in one sentence. */
    static String pasteSummary(int added, int alreadyQueued, int rejected) {
        if (added == 0 && alreadyQueued == 0 && rejected == 0) {
            return "The clipboard holds no Queqiao invitation.";
        }
        List<String> parts = new ArrayList<>();
        if (added > 0) {
            parts.add(added == 1 ? "Added 1 invitation" : "Added " + added + " invitations");
        }
        if (alreadyQueued > 0) {
            parts.add(alreadyQueued + " already queued");
        }
        if (rejected > 0) {
            parts.add(rejected + " could not be used");
        }
        return String.join("; ", parts) + ".";
    }

    /**
     * Why typed text was not imported. The text stays in the field so the
     * invitation can be corrected.
     */
    static String typedTextError(int added, int alreadyQueued, int rejected) {
        if (added == 0 && alreadyQueued == 0) {
            return rejected == 0
                    ? "The text holds no Queqiao invitation."
                    : "The text holds no usable Queqiao invitation; " + rejected + " could not be used.";
        }
        return pasteSummary(added, alreadyQueued, rejected);
    }

    /**
     * The import button's label. pending counts the queue, any typed
     * invitations not yet queued, and the resumable draft.
     */
    static String importTitle(int pending, boolean hasDraft) {
        if (hasDraft && pending <= 1) {
            return "Resume import";
        }
        return pending > 1 ? "Import " + pending + " profiles" : "Import profile";
    }

    /**
     * Invitations scanned, pasted, or shared but not yet enrolled, in import
     * order. Held in memory only and never persisted: an unused invitation is
     * a bearer credential, and the crash-safe state is the encrypted draft,
     * not this queue. One process-wide instance is shared by the import
     * dialog and the scanner, so scanned codes survive either activity being
     * recreated while the other is in front. Cancelling the dialog keeps the
     * queue, as on iOS, so stepping away does not lose what was scanned; an
     * entry leaves it when it is enrolled, when the user removes it, or when
     * the process ends.
     */
    static final class Queue {
        static final Queue SHARED = new Queue();

        private final List<QueuedInvitation> items = new ArrayList<>();

        /** Adds invitations not already queued, and returns how many were new. */
        synchronized int add(List<String> invitations) {
            int added = 0;
            for (String uri : invitations) {
                if (!contains(uri)) {
                    items.add(describe(uri));
                    added++;
                }
            }
            return added;
        }

        synchronized boolean contains(String uri) {
            for (QueuedInvitation item : items) {
                if (item.uri.equals(uri)) {
                    return true;
                }
            }
            return false;
        }

        synchronized boolean remove(String uri) {
            for (int index = 0; index < items.size(); index++) {
                if (items.get(index).uri.equals(uri)) {
                    items.remove(index);
                    return true;
                }
            }
            return false;
        }

        synchronized QueuedInvitation first() {
            return items.isEmpty() ? null : items.get(0);
        }

        synchronized List<QueuedInvitation> snapshot() {
            return new ArrayList<>(items);
        }

        synchronized int size() {
            return items.size();
        }

        synchronized void clear() {
            items.clear();
        }
    }
}

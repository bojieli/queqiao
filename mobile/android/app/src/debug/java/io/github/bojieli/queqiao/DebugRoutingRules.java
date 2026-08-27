package io.github.bojieli.queqiao;

import android.content.Context;
import android.util.Log;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import mobilecore.Session;

/**
 * Loads routing rules into a session, for the debug VPN build only.
 *
 * <p>The released app is not a VPN and has no routing engine — it exports a
 * SOCKS5 endpoint and the consumer client that owns the device tunnel supplies
 * rules, per-app policy, and DNS. That is the arrangement
 * {@code docs/KNOWN-LIMITATIONS.md} describes and the release checklist asserts
 * against the assembled artifact, which declares no {@code BIND_VPN_SERVICE}.
 * So rules here can only ever serve the debug tunnel, and that is what this
 * class is: the same core, the same rule syntax, the same country set, reachable
 * from a build that has a TUN to apply them to.
 *
 * <p>The rules come from a file rather than a settings screen. The debug build
 * exists to exercise the core against a real device, and a file that can be
 * pushed with {@code adb push} is the shortest path from a rule list to a
 * running tunnel — no UI to navigate on every iteration, and the same file can
 * be handed to the iOS client's editor.
 */
final class DebugRoutingRules {
    private static final String TAG = "QueqiaoRouting";

    /** Where the list is read from, inside the app's own files directory. */
    static final String RULES_FILE = "routing-rules.conf";

    /** The packed country set, copied into debug assets by the build. */
    static final String COUNTRY_ASSET = "cn-direct.bin";
    static final String COUNTRY_CODE = "CN";

    private DebugRoutingRules() {
    }

    /**
     * Installs whatever is available onto the session, before it starts.
     *
     * <p>Nothing here is fatal. A missing rule file means the tunnel carries
     * everything, which is what it did before rules existed; a country set that
     * will not load leaves GEOIP rules deciding nothing. Both are logged,
     * because the failure this whole feature exists to remove is routing that
     * silently does not do what it says.
     */
    static void install(Context context, Session session) {
        installCountrySet(context, session);
        installRules(context, session);
    }

    private static void installCountrySet(Context context, Session session) {
        try (InputStream stream = context.getAssets().open(COUNTRY_ASSET)) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            byte[] chunk = new byte[8192];
            int read;
            while ((read = stream.read(chunk)) != -1) {
                buffer.write(chunk, 0, read);
            }
            session.setCountrySet(COUNTRY_CODE, buffer.toByteArray());
            Log.i(TAG, "Loaded the " + COUNTRY_CODE + " route set for GEOIP rules");
        } catch (IOException exception) {
            Log.i(TAG, "No bundled country set; GEOIP rules will not match", exception);
        } catch (Exception exception) {
            Log.w(TAG, "The bundled country set did not load; GEOIP rules will not match", exception);
        }
    }

    private static void installRules(Context context, Session session) {
        File file = new File(context.getFilesDir(), RULES_FILE);
        if (!file.isFile()) {
            Log.i(TAG, "No " + RULES_FILE + "; every flow takes the tunnel");
            return;
        }
        String text;
        try {
            text = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
        } catch (IOException exception) {
            Log.w(TAG, "Could not read " + RULES_FILE + "; every flow takes the tunnel", exception);
            return;
        }
        if (text.trim().isEmpty()) {
            return;
        }
        // The report names every line the core would not load. Logging it is
        // the debug build's equivalent of the iOS editor's problem list: a rule
        // list is somebody stating where their traffic may not go, and a line
        // dropped in silence is that statement not being enforced.
        Log.i(TAG, "Routing rules: " + session.setRoutingRules(text));
    }
}

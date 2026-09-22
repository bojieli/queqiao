package io.github.bojieli.queqiao;

import android.util.Log;
import mobilecore.Session;

/**
 * Hands a profile's rule list and the bundled country set to a session before it
 * starts, so no flow is decided by a different list than the one in force.
 *
 * <p>Nothing here is fatal, as on iOS: a list with bad lines still loads the good
 * ones and the core's report names the rest; a country set that will not load
 * leaves GEOIP rules deciding nothing. Both are logged under the QueqiaoRouting
 * tag, because routing that silently does not do what it says is the failure
 * rules exist to remove.
 */
final class RoutingRules {
    private static final String TAG = "QueqiaoRouting";

    private RoutingRules() {
    }

    static void install(Session session, RoutingConfiguration routing, byte[] packedChinaSet) {
        if (packedChinaSet != null) {
            try {
                session.setCountrySet(CountryRoutes.CHINA_CODE, packedChinaSet);
            } catch (Exception exception) {
                Log.w(TAG, "The bundled country set did not load; GEOIP rules will not match", exception);
            }
        }
        if (routing.rules.trim().isEmpty()) {
            return;
        }
        Log.i(TAG, "Routing rules: " + session.setRoutingRules(routing.rules));
    }
}

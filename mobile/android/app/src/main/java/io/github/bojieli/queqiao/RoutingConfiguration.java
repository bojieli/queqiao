package io.github.bojieli.queqiao;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * One profile's routing settings. The same model as RoutingConfiguration.swift:
 * a mode that says whether address bypasses apply, the bypasses themselves, and
 * a rule list the core evaluates whichever mode is chosen.
 */
final class RoutingConfiguration {
    /** How many hand-entered bypass routes one profile may hold; iOS uses the same cap. */
    static final int MAX_CUSTOM_ROUTES = 256;

    enum Mode {
        ALL_TRAFFIC(
                "all-traffic",
                "Route all traffic",
                "Every IPv4, IPv6, and DNS destination goes through the selected Queqiao provider. "
                        + "Bypass rules below are kept but not applied."),
        BYPASS_RULES(
                "bypass-rules",
                "Use bypass rules",
                "Everything goes through Queqiao except the destinations matched by the rules below.");

        final String wireValue;
        final String title;
        final String detail;

        Mode(String wireValue, String title, String detail) {
            this.wireValue = wireValue;
            this.title = title;
            this.detail = detail;
        }

        static Mode fromWireValue(String value) {
            for (Mode mode : values()) {
                if (mode.wireValue.equals(value)) {
                    return mode;
                }
            }
            return ALL_TRAFFIC;
        }
    }

    static final RoutingConfiguration DEFAULT =
            new RoutingConfiguration(Mode.ALL_TRAFFIC, false, false, Collections.emptyList(), "");

    // What a build from before routing rules stored; still written so that build can read the catalog.
    private static final String LEGACY_ALL_TRAFFIC = "all-traffic";
    private static final String LEGACY_EXCLUDE_LOCAL = "exclude-local-networks";

    final Mode mode;
    final boolean bypassLocalNetworks;
    final boolean bypassChinaDirect;
    final List<String> customRoutes;
    final String rules;

    RoutingConfiguration(
            Mode mode,
            boolean bypassLocalNetworks,
            boolean bypassChinaDirect,
            List<String> customRoutes,
            String rules) {
        this.mode = mode;
        this.bypassLocalNetworks = bypassLocalNetworks;
        this.bypassChinaDirect = bypassChinaDirect;
        this.customRoutes = Collections.unmodifiableList(new ArrayList<>(customRoutes));
        this.rules = rules;
    }

    RoutingConfiguration withMode(Mode replacement) {
        return new RoutingConfiguration(replacement, bypassLocalNetworks, bypassChinaDirect, customRoutes, rules);
    }

    RoutingConfiguration withBypassLocalNetworks(boolean enabled) {
        return new RoutingConfiguration(mode, enabled, bypassChinaDirect, customRoutes, rules);
    }

    RoutingConfiguration withBypassChinaDirect(boolean enabled) {
        return new RoutingConfiguration(mode, bypassLocalNetworks, enabled, customRoutes, rules);
    }

    RoutingConfiguration withCustomRoutes(List<String> routes) {
        return new RoutingConfiguration(mode, bypassLocalNetworks, bypassChinaDirect, routes, rules);
    }

    RoutingConfiguration withRules(String text) {
        return new RoutingConfiguration(mode, bypassLocalNetworks, bypassChinaDirect, customRoutes, text);
    }

    boolean rulesApply() {
        return mode == Mode.BYPASS_RULES;
    }

    boolean hasEnabledRules() {
        return bypassLocalNetworks || bypassChinaDirect || !customRoutes.isEmpty();
    }

    int ruleLineCount() {
        int count = 0;
        for (String line : rules.split("\\R")) {
            String trimmed = line.trim();
            if (!trimmed.isEmpty() && !trimmed.startsWith("#")) {
                count++;
            }
        }
        return count;
    }

    String summary() {
        List<String> parts = new ArrayList<>();
        int ruleLines = ruleLineCount();
        if (ruleLines > 0) {
            parts.add(ruleLines + (ruleLines == 1 ? " rule" : " rules"));
        }
        if (!rulesApply() || !hasEnabledRules()) {
            return parts.isEmpty() ? Mode.ALL_TRAFFIC.title : "Routing by " + joinList(parts);
        }
        if (bypassLocalNetworks) {
            parts.add("local networks");
        }
        if (bypassChinaDirect) {
            parts.add("Chinese addresses");
        }
        if (!customRoutes.isEmpty()) {
            parts.add(customRoutes.size() + (customRoutes.size() == 1 ? " custom route" : " custom routes"));
        }
        return "Bypassing " + joinList(parts);
    }

    private static String joinList(List<String> parts) {
        if (parts.size() == 1) {
            return parts.get(0);
        }
        if (parts.size() == 2) {
            return parts.get(0) + " and " + parts.get(1);
        }
        return String.join(", ", parts.subList(0, parts.size() - 1)) + ", and " + parts.get(parts.size() - 1);
    }

    void writeTo(JSONObject record) throws JSONException {
        record.put("traffic_policy",
                rulesApply() && bypassLocalNetworks ? LEGACY_EXCLUDE_LOCAL : LEGACY_ALL_TRAFFIC);
        record.put("routing_mode", mode.wireValue);
        record.put("bypass_local_networks", bypassLocalNetworks);
        record.put("bypass_china_direct", bypassChinaDirect);
        record.put("bypass_routes", new JSONArray(customRoutes));
        record.put("routing_rules", rules);
    }

    static RoutingConfiguration readFrom(JSONObject record) {
        boolean legacyExcludesLocal = LEGACY_EXCLUDE_LOCAL.equals(record.optString("traffic_policy"));
        if (!record.has("routing_mode")) {
            // The old policy only ever decided whether local networks were excluded, so a
            // catalog that excluded them has to load as bypass rules or upgrading would
            // push those destinations back through the tunnel.
            return new RoutingConfiguration(
                    legacyExcludesLocal ? Mode.BYPASS_RULES : Mode.ALL_TRAFFIC,
                    legacyExcludesLocal, false, Collections.emptyList(), "");
        }
        List<String> routes = new ArrayList<>();
        JSONArray stored = record.optJSONArray("bypass_routes");
        for (int index = 0; stored != null && index < stored.length(); index++) {
            routes.add(stored.optString(index));
        }
        return new RoutingConfiguration(
                Mode.fromWireValue(record.optString("routing_mode")),
                record.optBoolean("bypass_local_networks"),
                record.optBoolean("bypass_china_direct"),
                routes,
                record.optString("routing_rules"));
    }
}

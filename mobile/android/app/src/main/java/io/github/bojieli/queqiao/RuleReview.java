package io.github.bojieli.queqiao;

import android.content.Context;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * A quick read of a rule list before it is saved, the same checks as RuleReview
 * in ProfileRulesSection.swift. The core's own report is the authority once a
 * session loads the list; this exists so a typo is seen in the editor.
 */
final class RuleReview {
    /** The China preset, one text shared with the iOS client; see scripts/test_mobile_route_parity.py. */
    static final String CHINA_PRESET_ASSET = "china-preset.conf";

    private static final Set<String> KNOWN_TYPES = new HashSet<>(Arrays.asList(
            "DOMAIN", "DOMAIN-SUFFIX", "DOMAIN-KEYWORD",
            "IP-CIDR", "IP-CIDR6", "IP6-CIDR",
            "GEOIP", "DST-PORT", "PORT", "FINAL", "MATCH"));
    private static final Set<String> KNOWN_ACTIONS = new HashSet<>(Arrays.asList(
            "PROXY", "QUEQIAO", "DIRECT", "REJECT", "REJECT-DROP"));
    private static final int MAX_PROBLEMS = 10;

    final int count;
    final List<String> problems;

    RuleReview(String text) {
        int counted = 0;
        List<String> found = new ArrayList<>();
        String[] lines = text.split("\n", -1);
        for (int index = 0; index < lines.length; index++) {
            String line = lines[index].trim();
            if (line.isEmpty() || line.startsWith("#") || line.startsWith(";")) {
                continue;
            }
            String[] fields = line.split(",");
            for (int field = 0; field < fields.length; field++) {
                fields[field] = fields[field].trim().toUpperCase(Locale.ROOT);
            }
            String type = fields[0];
            if (!KNOWN_TYPES.contains(type)) {
                found.add("Line " + (index + 1) + ": not a rule type");
                continue;
            }
            boolean isFinal = type.equals("FINAL") || type.equals("MATCH");
            int expected = isFinal ? 2 : 3;
            if (fields.length < expected) {
                found.add("Line " + (index + 1) + ": " + type + " needs "
                        + (isFinal ? "an action" : "a value and an action"));
                continue;
            }
            String action = fields[expected - 1];
            if (!KNOWN_ACTIONS.contains(action)) {
                // Usually a file written for a client with several outbounds, where
                // this field names a proxy group; saying so beats "invalid".
                found.add("Line " + (index + 1) + ": \"" + action.toLowerCase(Locale.ROOT)
                        + "\" is not an action. This client has one tunnel, so PROXY, DIRECT or REJECT.");
                continue;
            }
            counted++;
        }
        count = counted;
        problems = found.size() > MAX_PROBLEMS ? found.subList(0, MAX_PROBLEMS) : found;
    }

    boolean isEmpty() {
        return count == 0 && problems.isEmpty();
    }

    String summary() {
        String text = count + (count == 1 ? " rule" : " rules");
        if (!problems.isEmpty()) {
            text += ", " + problems.size() + (problems.size() == 1 ? " line" : " lines") + " the core will not load";
        }
        return text;
    }

    static String chinaPreset(Context context) throws IOException {
        try (InputStream stream = context.getAssets().open(CHINA_PRESET_ASSET)) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            byte[] chunk = new byte[4096];
            int read;
            while ((read = stream.read(chunk)) != -1) {
                buffer.write(chunk, 0, read);
            }
            return buffer.toString(StandardCharsets.UTF_8.name());
        }
    }
}

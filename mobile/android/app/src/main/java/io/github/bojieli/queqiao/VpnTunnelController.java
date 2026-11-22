package io.github.bojieli.queqiao;

import android.annotation.SuppressLint;
import android.content.Intent;
import android.graphics.Typeface;
import android.net.VpnService;
import android.provider.Settings;
import android.text.InputType;
import android.view.Gravity;
import android.view.View;
import android.widget.Button;
import android.widget.CheckBox;
import android.widget.EditText;
import android.widget.LinearLayout;
import android.widget.RadioButton;
import android.widget.RadioGroup;
import android.widget.TextView;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Drives the full-device tunnel. Android asks for VPN consent once per app, the
 * traffic policy decides which destinations the interface claims, and the
 * system VPN settings screen is the only place a user can revoke it.
 */
final class VpnTunnelController implements TunnelController {
    private final TunnelHost host;

    VpnTunnelController(TunnelHost host) {
        this.host = host;
    }

    @Override
    public String modeId() {
        return QueqiaoVpnService.MODE;
    }

    @Override
    public String title() {
        return "Full device tunnel";
    }

    @Override
    public String summary() {
        return "Queqiao installs a VPN interface and carries every app's traffic.";
    }

    @Override
    public String noun() {
        return "tunnel";
    }

    @Override
    public Intent consentIntent() {
        return VpnService.prepare(host.activity());
    }

    @Override
    public void connect(String profileId) {
        TunnelBroadcast.connect(host.activity(), QueqiaoVpnService.class, profileId);
    }

    @Override
    public void disconnect() {
        TunnelBroadcast.disconnect(host.activity(), QueqiaoVpnService.class);
    }

    @Override
    public void requestStatus() {
        TunnelBroadcast.requestStatus(host.activity(), QueqiaoVpnService.class);
    }

    @Override
    public void renderConnectionDetails(
            UiKit ui, LinearLayout card, ProfileRepository.ProfileRecord profile) {
        ui.addLabelValue(card, "Routing", profile.routing.summary());
    }

    @Override
    @SuppressLint("SetTextI18n")
    public void renderProfileOptions(
            UiKit ui,
            LinearLayout content,
            ProfileRepository.ProfileRecord profile,
            boolean editable) {
        // Several controls edit one value, so each change starts from the last one
        // saved in this dialog rather than from the record it was opened with.
        RoutingConfiguration[] current = {profile.routing};

        addSectionTitle(ui, content, "Routing");
        RadioGroup modes = new RadioGroup(host.activity());
        for (RoutingConfiguration.Mode mode : RoutingConfiguration.Mode.values()) {
            RadioButton option = new RadioButton(host.activity());
            option.setId(View.generateViewId());
            option.setText(mode.title + "\n" + mode.detail);
            option.setTextSize(14);
            option.setTag(mode);
            option.setChecked(current[0].mode == mode);
            option.setEnabled(editable);
            modes.addView(option, UiKit.matchWrap());
        }
        modes.setOnCheckedChangeListener((group, checkedId) -> {
            RadioButton option = group.findViewById(checkedId);
            if (option != null && option.getTag() instanceof RoutingConfiguration.Mode) {
                current[0] = current[0].withMode((RoutingConfiguration.Mode) option.getTag());
                saveRouting(profile.id, current[0]);
            }
        });
        content.addView(modes, UiKit.matchWrap());
        ui.addBodyText(content, "DNS resolves through the Queqiao tunnel in both modes. "
                + "Changes apply the next time this profile connects.");

        addSectionTitle(ui, content, "Bypass rules");
        CheckBox localNetworks = new CheckBox(host.activity());
        localNetworks.setText("Local networks\nPrivate and link-local destinations, such as a home router or a printer.");
        localNetworks.setTextSize(14);
        localNetworks.setChecked(current[0].bypassLocalNetworks);
        localNetworks.setEnabled(editable);
        localNetworks.setOnCheckedChangeListener((view, checked) -> {
            current[0] = current[0].withBypassLocalNetworks(checked);
            saveRouting(profile.id, current[0]);
        });
        content.addView(localNetworks, UiKit.matchWrap());

        CheckBox chinaDirect = new CheckBox(host.activity());
        chinaDirect.setText("Chinese addresses\n" + chinaSetDetail());
        chinaDirect.setTextSize(14);
        chinaDirect.setChecked(current[0].bypassChinaDirect);
        chinaDirect.setEnabled(editable && RoutePolicy.supportsCountryRoutes());
        chinaDirect.setOnCheckedChangeListener((view, checked) -> {
            current[0] = current[0].withBypassChinaDirect(checked);
            saveRouting(profile.id, current[0]);
        });
        content.addView(chinaDirect, UiKit.matchWrap());

        addSectionTitle(ui, content, "Custom routes");
        EditText customRoutes = multilineField(ui, String.join("\n", current[0].customRoutes),
                "One address or CIDR block per line", editable);
        content.addView(customRoutes, UiKit.matchWrap());
        ui.addBodyText(content, "Addresses and CIDR blocks listed here stay off the tunnel and use "
                + "the device's ordinary connection. Up to " + RoutingConfiguration.MAX_CUSTOM_ROUTES + ".");
        Button saveRoutes = ui.secondaryButton("Save custom routes");
        saveRoutes.setEnabled(editable);
        saveRoutes.setOnClickListener(view -> {
            List<String> accepted = new ArrayList<>();
            List<String> rejected = new ArrayList<>();
            for (String line : customRoutes.getText().toString().split("[\\s,]+")) {
                if (line.isEmpty()) {
                    continue;
                }
                try {
                    String canonical = RoutePolicy.RouteSpec.parse(line).cidr();
                    if (!accepted.contains(canonical)) {
                        accepted.add(canonical);
                    }
                } catch (UnknownHostException exception) {
                    rejected.add(line);
                }
            }
            if (!rejected.isEmpty() || accepted.size() > RoutingConfiguration.MAX_CUSTOM_ROUTES) {
                host.failure("Custom routes were not saved", new IllegalArgumentException(
                        rejected.isEmpty()
                                ? "A profile holds at most " + RoutingConfiguration.MAX_CUSTOM_ROUTES + " custom routes"
                                : "Not an address or CIDR block: " + String.join(", ", rejected)));
                return;
            }
            customRoutes.setText(String.join("\n", accepted));
            current[0] = current[0].withCustomRoutes(accepted);
            saveRouting(profile.id, current[0]);
        });
        content.addView(saveRoutes, ui.topSpaced());

        addSectionTitle(ui, content, "Routing rules");
        EditText rules = multilineField(ui, current[0].rules, "DOMAIN-SUFFIX,example.com,DIRECT", editable);
        rules.setTypeface(Typeface.MONOSPACE);
        content.addView(rules, UiKit.matchWrap());
        TextView review = ui.text(reviewText(current[0].rules), 12, Typeface.NORMAL);
        content.addView(review, UiKit.matchWrap());
        ui.addBodyText(content, "One rule per line, first match wins: DOMAIN, DOMAIN-SUFFIX, DOMAIN-KEYWORD, "
                + "IP-CIDR, GEOIP, DST-PORT or FINAL, then PROXY, DIRECT or REJECT. Rules are evaluated in "
                + "either routing mode.");
        Button usePreset = ui.secondaryButton("Use the China preset");
        usePreset.setEnabled(editable);
        usePreset.setOnClickListener(view -> {
            try {
                rules.setText(RuleReview.chinaPreset(host.activity()));
                review.setText(reviewText(rules.getText().toString()) + " — not saved yet");
            } catch (IOException exception) {
                host.failure("The China preset is unavailable", exception);
            }
        });
        content.addView(usePreset, ui.topSpaced());
        Button saveRules = ui.secondaryButton("Save rules");
        saveRules.setEnabled(editable);
        saveRules.setOnClickListener(view -> {
            String text = rules.getText().toString();
            review.setText(reviewText(text));
            current[0] = current[0].withRules(text);
            saveRouting(profile.id, current[0]);
        });
        content.addView(saveRules, ui.topSpaced());
    }

    private static void addSectionTitle(UiKit ui, LinearLayout content, String title) {
        TextView view = ui.sectionTitle(title);
        view.setPadding(0, ui.dp(18), 0, ui.dp(2));
        content.addView(view, UiKit.matchWrap());
    }

    private EditText multilineField(UiKit ui, String value, String hint, boolean editable) {
        EditText field = new EditText(host.activity());
        field.setInputType(InputType.TYPE_CLASS_TEXT
                | InputType.TYPE_TEXT_FLAG_MULTI_LINE
                | InputType.TYPE_TEXT_FLAG_NO_SUGGESTIONS);
        field.setMinLines(3);
        field.setMaxLines(12);
        field.setGravity(Gravity.TOP | Gravity.START);
        field.setTextSize(13);
        field.setHint(hint);
        field.setText(value);
        field.setEnabled(editable);
        return field;
    }

    private String chinaSetDetail() {
        if (!RoutePolicy.supportsCountryRoutes()) {
            return "Needs Android 13 or later to install as routes. A GEOIP,CN,DIRECT rule keeps them direct here.";
        }
        try {
            return CountryRoutes.blockCount(CountryRoutes.packedChinaSet(host.activity()))
                    + " address blocks APNIC records as delegated to China.";
        } catch (IOException exception) {
            return "The bundled address set is missing from this build.";
        }
    }

    private static String reviewText(String rules) {
        RuleReview review = new RuleReview(rules);
        if (review.isEmpty()) {
            return "No rules: every flow takes the tunnel.";
        }
        StringBuilder text = new StringBuilder(review.summary());
        for (String problem : review.problems) {
            text.append("\n").append(problem);
        }
        return text.toString();
    }

    @Override
    public void renderSettings(UiKit ui, LinearLayout content) {
        LinearLayout card = ui.card();
        card.addView(ui.sectionTitle("VPN interface"), UiKit.matchWrap());
        ui.addBodyText(
                card,
                "Android grants VPN consent to one app at a time. Revoke it from the system settings screen.");
        Button systemSettings = ui.secondaryButton("Open Android VPN settings");
        systemSettings.setOnClickListener(view -> openVpnSettings());
        card.addView(systemSettings, ui.topSpaced());
        content.addView(card, ui.spacedCard());
    }

    private void openVpnSettings() {
        try {
            host.activity().startActivity(new Intent(Settings.ACTION_VPN_SETTINGS));
        } catch (Exception exception) {
            host.failure("VPN settings are unavailable", exception);
        }
    }

    private void saveRouting(String profileId, RoutingConfiguration routing) {
        if (host.connectionActive()) {
            host.failure(
                    "Disconnect first",
                    new IllegalStateException("Disconnect before changing how this profile routes traffic"));
            return;
        }
        host.background(() -> {
            try {
                host.repository().setRouting(profileId, routing);
                host.activity().runOnUiThread(host::refresh);
            } catch (Exception exception) {
                host.failure("Could not update routing", exception);
            }
        });
    }
}

package io.github.bojieli.queqiao;

import android.app.AlertDialog;
import android.content.ClipData;
import android.content.ClipboardManager;
import android.content.Intent;
import android.os.Build;
import android.text.InputType;
import android.view.Gravity;
import android.widget.Button;
import android.widget.EditText;
import android.widget.LinearLayout;
import android.widget.Toast;

import java.security.GeneralSecurityException;

/**
 * Drives export mode: Queqiao opens an authenticated SOCKS5 listener on
 * loopback and stops there. Another VPN client — v2rayNG, mihomo, sing-box —
 * owns the device's routing rules, DNS, and per-app policy, and treats Queqiao
 * as one outbound among many.
 *
 * The one thing the consumer must do that it would not do for a remote proxy is
 * exclude Queqiao from its own tunnel. Loopback is shared, but Queqiao's uplink
 * to the gateway is not: if the consumer captures it, that uplink is sent back
 * into the consumer's own outbound, which is this listener, and the connection
 * loops until it times out. Nothing on the device can detect that reliably from
 * the outside, so the setup text leads with the exclusion step and the profile
 * connection test is the check that reports the failure.
 */
final class ProxyTunnelController implements TunnelController {
    private final TunnelHost host;

    ProxyTunnelController(TunnelHost host) {
        this.host = host;
    }

    @Override
    public String modeId() {
        return QueqiaoProxyService.MODE;
    }

    @Override
    public String title() {
        return "Export to another VPN app";
    }

    @Override
    public String summary() {
        return "Queqiao serves a local SOCKS5 endpoint and your routing app decides what to send into it.";
    }

    @Override
    public String noun() {
        return "connection";
    }

    /** Export mode installs no interface, so Android asks for nothing. */
    @Override
    public Intent consentIntent() {
        return null;
    }

    @Override
    public void connect(String profileId) {
        TunnelBroadcast.connect(host.activity(), QueqiaoProxyService.class, profileId);
    }

    @Override
    public void disconnect() {
        TunnelBroadcast.disconnect(host.activity(), QueqiaoProxyService.class);
    }

    @Override
    public void requestStatus() {
        TunnelBroadcast.requestStatus(host.activity(), QueqiaoProxyService.class);
    }

    @Override
    public void renderConnectionDetails(
            UiKit ui, LinearLayout card, ProfileRepository.ProfileRecord profile) {
        ProxyEndpoint endpoint = endpointOrNull();
        ui.addLabelValue(card, "SOCKS5 endpoint", endpoint == null ? "unavailable" : endpoint.listenAddress());
    }

    /**
     * Export mode has no per-profile options. Which destinations travel through
     * Queqiao is the consumer app's rule engine to decide, and duplicating that
     * choice here would produce two answers to one question.
     */
    @Override
    public void renderProfileOptions(
            UiKit ui,
            LinearLayout content,
            ProfileRepository.ProfileRecord profile,
            boolean editable) {
    }

    @Override
    public void renderSettings(UiKit ui, LinearLayout content) {
        content.addView(exclusionCard(ui), ui.spacedCard());
        content.addView(endpointCard(ui), ui.spacedCard());
        content.addView(clientCard(ui), ui.spacedCard());
    }

    /** First, because every other step is wasted if this one is skipped. */
    private LinearLayout exclusionCard(UiKit ui) {
        LinearLayout card = ui.card();
        card.addView(ui.sectionTitle("First: exclude Queqiao"), UiKit.matchWrap());
        ui.addBodyText(
                card,
                "In the app that owns the VPN interface, exempt Queqiao from its tunnel. Every client names "
                        + "this differently — per-app proxy, access control, split tunnelling — but all of them "
                        + "offer it.");
        ui.addBodyText(
                card,
                "Skip it and the consumer captures Queqiao's own uplink, sends it back into its own outbound, "
                        + "and that outbound is this listener. Traffic then loops until it times out rather than "
                        + "failing outright.");
        ui.addBodyText(
                card,
                "Open a profile and run Test connection with the consumer's tunnel up. A loop shows there as a "
                        + "provider that cannot be reached.");
        return card;
    }

    private LinearLayout endpointCard(UiKit ui) {
        LinearLayout card = ui.card();
        card.addView(ui.sectionTitle("Local SOCKS5 endpoint"), UiKit.matchWrap());
        ProxyEndpoint endpoint = endpointOrNull();
        if (endpoint == null) {
            ui.addBodyText(card, "The endpoint credentials could not be read from the device keystore.");
            return card;
        }
        ui.addLabelValue(card, "Address", endpoint.listenAddress());
        ui.addLabelValue(card, "Username", endpoint.username);
        ui.addLabelValue(card, "Password", endpoint.password);
        ui.addBodyText(
                card,
                "Loopback is shared with every app on the device, so these credentials are the only thing "
                        + "between the gateway and anything else running here. They never leave the device.");

        Button copy = ui.secondaryButton("Copy endpoint and credentials");
        copy.setOnClickListener(view -> copyToClipboard(
                "Queqiao SOCKS5",
                endpoint.listenAddress() + "\n" + endpoint.username + "\n" + endpoint.password));
        card.addView(copy, ui.topSpaced());

        Button port = ui.secondaryButton("Change port");
        port.setOnClickListener(view -> promptForPort(endpoint));
        card.addView(port, ui.topSpaced());

        Button regenerate = ui.secondaryButton("Regenerate credentials");
        regenerate.setOnClickListener(view -> confirmRegenerate());
        card.addView(regenerate, ui.topSpaced());
        return card;
    }

    private LinearLayout clientCard(UiKit ui) {
        LinearLayout card = ui.card();
        card.addView(ui.sectionTitle("Client setup"), UiKit.matchWrap());
        ui.addBodyText(
                card,
                "Queqiao is an ordinary authenticated SOCKS5 proxy to these clients. UDP travels over "
                        + "UDP ASSOCIATE, so keep UDP enabled on the outbound or QUIC-based sites fall back to TCP.");
        ProxyEndpoint endpoint = endpointOrNull();
        String host = ProxyEndpoint.HOST;
        String port = endpoint == null ? String.valueOf(ProxyEndpoint.DEFAULT_PORT) : String.valueOf(endpoint.port);
        String user = endpoint == null ? "USERNAME" : endpoint.username;
        String pass = endpoint == null ? "PASSWORD" : endpoint.password;

        addClient(
                ui,
                card,
                "v2rayNG / Xray",
                "Settings, then Per-app proxy: turn it on, choose bypass mode, and select Queqiao.",
                "{\n"
                        + "  \"protocol\": \"socks\",\n"
                        + "  \"tag\": \"queqiao\",\n"
                        + "  \"settings\": {\n"
                        + "    \"servers\": [{\n"
                        + "      \"address\": \"" + host + "\",\n"
                        + "      \"port\": " + port + ",\n"
                        + "      \"users\": [{\n"
                        + "        \"user\": \"" + user + "\",\n"
                        + "        \"pass\": \"" + pass + "\"\n"
                        + "      }]\n"
                        + "    }]\n"
                        + "  }\n"
                        + "}");
        addClient(
                ui,
                card,
                "mihomo / ClashMetaForAndroid",
                "Settings, then Access control: choose deny selected apps and check Queqiao.",
                "proxies:\n"
                        + "  - name: queqiao\n"
                        + "    type: socks5\n"
                        + "    server: " + host + "\n"
                        + "    port: " + port + "\n"
                        + "    username: \"" + user + "\"\n"
                        + "    password: \"" + pass + "\"\n"
                        + "    udp: true");
        addClient(
                ui,
                card,
                "sing-box / NekoBox",
                "Settings, then Per-app proxy: choose exclude mode and select Queqiao.",
                "{\n"
                        + "  \"type\": \"socks\",\n"
                        + "  \"tag\": \"queqiao\",\n"
                        + "  \"server\": \"" + host + "\",\n"
                        + "  \"server_port\": " + port + ",\n"
                        + "  \"version\": \"5\",\n"
                        + "  \"username\": \"" + user + "\",\n"
                        + "  \"password\": \"" + pass + "\"\n"
                        + "}");
        return card;
    }

    private void addClient(UiKit ui, LinearLayout card, String name, String bypass, String snippet) {
        card.addView(ui.text(name, 15, android.graphics.Typeface.BOLD), ui.topSpaced());
        ui.addBodyText(card, bypass);
        card.addView(ui.codeBlock(snippet), ui.topSpaced());
        Button copy = ui.secondaryButton("Copy " + name + " outbound");
        copy.setOnClickListener(view -> copyToClipboard(name, snippet));
        card.addView(copy, ui.topSpaced());
    }

    private ProxyEndpoint endpointOrNull() {
        try {
            return ProxyEndpoint.load(host.activity());
        } catch (GeneralSecurityException exception) {
            return null;
        }
    }

    private void copyToClipboard(String label, String value) {
        ClipboardManager clipboard = host.activity().getSystemService(ClipboardManager.class);
        clipboard.setPrimaryClip(ClipData.newPlainText(label, value));
        // Android 13 confirms a copy itself; below that nothing does.
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) {
            Toast.makeText(host.activity(), "Copied", Toast.LENGTH_SHORT).show();
        }
    }

    private void promptForPort(ProxyEndpoint endpoint) {
        if (refuseWhileConnected("Disconnect before changing the listen port")) {
            return;
        }
        EditText field = new EditText(host.activity());
        field.setInputType(InputType.TYPE_CLASS_NUMBER);
        field.setHint("1024-65535");
        field.setText(String.valueOf(endpoint.port));
        field.setGravity(Gravity.CENTER_HORIZONTAL);
        LinearLayout frame = new LinearLayout(host.activity());
        int inset = frame.getResources().getDisplayMetrics().densityDpi / 6;
        frame.setPadding(inset, inset / 2, inset, 0);
        frame.addView(field, UiKit.matchWrap());
        new AlertDialog.Builder(host.activity())
                .setTitle("Listen port")
                .setMessage("Pick a port no other app on this device is already using.")
                .setView(frame)
                .setPositiveButton("Save", (dialog, which) -> applyPort(field.getText().toString()))
                .setNegativeButton("Cancel", null)
                .show();
    }

    private void applyPort(String entered) {
        int port;
        try {
            port = Integer.parseInt(entered.trim());
        } catch (NumberFormatException exception) {
            host.failure("Invalid port", exception);
            return;
        }
        if (!ProxyEndpoint.validPort(port)) {
            host.failure(
                    "Invalid port",
                    new IllegalArgumentException("Choose a port between "
                            + ProxyEndpoint.MINIMUM_PORT + " and " + ProxyEndpoint.MAXIMUM_PORT));
            return;
        }
        host.background(() -> {
            try {
                ProxyEndpoint.withPort(host.activity(), port);
                host.activity().runOnUiThread(host::refresh);
            } catch (Exception exception) {
                host.failure("Could not change the port", exception);
            }
        });
    }

    private void confirmRegenerate() {
        if (refuseWhileConnected("Disconnect before regenerating credentials")) {
            return;
        }
        new AlertDialog.Builder(host.activity())
                .setTitle("Regenerate credentials")
                .setMessage("Every client already configured with the current credentials stops working until "
                        + "you paste the new ones into it.")
                .setPositiveButton("Regenerate", (dialog, which) -> host.background(() -> {
                    try {
                        ProxyEndpoint.regenerate(host.activity());
                        host.activity().runOnUiThread(host::refresh);
                    } catch (Exception exception) {
                        host.failure("Could not regenerate credentials", exception);
                    }
                }))
                .setNegativeButton("Cancel", null)
                .show();
    }

    private boolean refuseWhileConnected(String reason) {
        if (!host.connectionActive()) {
            return false;
        }
        host.failure("Disconnect first", new IllegalStateException(reason));
        return true;
    }
}

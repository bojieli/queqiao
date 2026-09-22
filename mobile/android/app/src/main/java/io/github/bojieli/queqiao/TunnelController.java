package io.github.bojieli.queqiao;

import android.content.Intent;
import android.widget.LinearLayout;

/**
 * One way this app can carry traffic: the full-device tunnel, or an exported
 * SOCKS listener that another VPN client routes into.
 *
 * The activity owns the profile catalog, the connection button, and the metric
 * counters, which are identical across modes. Everything a mode does
 * differently is here: the consent it needs, the service it drives, and the
 * views it contributes to the three pages.
 */
interface TunnelController {
    /** Stable identifier, matching the serving TunnelServiceCore.Backend.modeId. */
    String modeId();

    /** Title for the mode picker. */
    String title();

    /** One explanatory line under that title. */
    String summary();

    /** The noun the rest of the UI uses for this connection, e.g. "tunnel". */
    String noun();

    /**
     * Consent this mode must obtain before connecting, launched for result, or
     * null when the platform requires none.
     */
    Intent consentIntent();


    void connect(String profileId);

    void disconnect();

    /** Asks the serving service to broadcast its current state. */
    void requestStatus();

    /** Mode-specific rows on the home page's selected-profile card. */
    void renderConnectionDetails(UiKit ui, LinearLayout card, ProfileRepository.ProfileRecord profile);

    /** Mode-specific options in the per-profile dialog. */
    void renderProfileOptions(
            UiKit ui,
            LinearLayout content,
            ProfileRepository.ProfileRecord profile,
            boolean editable);

    /** Mode-specific cards appended to the settings page. */
    void renderSettings(UiKit ui, LinearLayout content);
}

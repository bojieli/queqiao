package io.github.bojieli.queqiao;

import android.Manifest;
import android.annotation.SuppressLint;
import android.app.Activity;
import android.app.AlertDialog;
import android.content.BroadcastReceiver;
import android.content.ClipData;
import android.content.ClipboardManager;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.content.pm.PackageManager;
import android.content.res.ColorStateList;
import android.graphics.Color;
import android.graphics.Typeface;
import android.os.Build;
import android.os.Bundle;
import android.text.Editable;
import android.text.InputType;
import android.text.TextWatcher;
import android.view.Gravity;
import android.view.View;
import android.view.WindowInsets;
import android.widget.Button;
import android.widget.EditText;
import android.widget.FrameLayout;
import android.widget.LinearLayout;
import android.widget.RadioButton;
import android.widget.RadioGroup;
import android.widget.TextView;

import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.text.DateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import mobilecore.Mobilecore;

public final class MainActivity extends Activity implements TunnelHost {
    private static final int REQUEST_CONSENT = 7001;
    private static final int REQUEST_NOTIFICATIONS = 7002;
    private static final int REQUEST_SCAN_INVITATION = 7003;
    private static final String PREFERENCES = "io.github.bojieli.queqiao.ui";
    private static final String PREFERENCE_MODE = "mode";

    private enum Page {
        HOME,
        PROFILES,
        SETTINGS
    }

    private final ExecutorService worker = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "queqiao-app-work");
        thread.setDaemon(true);
        return thread;
    });
    // Connection tests run side by side, up to the same four at a time as iOS: a
    // slow or unreachable provider should not hold up the verdict on the others.
    private static final int MAX_CONCURRENT_PROBES = 4;
    private final ExecutorService probePool = Executors.newFixedThreadPool(MAX_CONCURRENT_PROBES, runnable -> {
        Thread thread = new Thread(runnable, "queqiao-probe");
        thread.setDaemon(true);
        return thread;
    });
    private UiKit ui;
    private List<TunnelController> modes;
    private TunnelController controller;
    private ProfileRepository repository;
    private ProfileRepository.Catalog catalog = new ProfileRepository.Catalog();
    private FrameLayout pageContainer;
    private Button homeTab;
    private Button profilesTab;
    private Button settingsTab;
    private TextView statusView;
    private TextView connectionSubtitle;
    private TextView downloadedView;
    private TextView uploadedView;
    private TextView flowsView;
    private TextView routedView;
    private Button connectionButton;
    private Page currentPage = Page.HOME;
    private String tunnelState = Mobilecore.StateStopped;
    private String tunnelMessage;
    private String serviceProfileId;
    private String pendingConnectProfileId;
    private boolean busy;
    private long bytesUp;
    private long bytesDown;
    private long activeFlows;
    private String routedSummary = "";
    private final Map<String, ConnectionProbe> profileProbes = new HashMap<>();
    // Scanned, pasted, and shared invitations wait here, in memory only, until
    // the import dialog enrolls them. The dialog stays up while the scanner is
    // in front and re-renders the queue when it returns; if the system
    // reclaimed this activity meanwhile, the dialog is rebuilt around it.
    private final InvitationBatch.Queue importQueue = InvitationBatch.Queue.SHARED;
    private ImportForm importForm;
    private boolean testingProfiles;

    private final BroadcastReceiver stateReceiver = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            if (!TunnelBroadcast.ACTION_STATE.equals(intent.getAction())) {
                return;
            }
            String state = valueOr(intent.getStringExtra(TunnelBroadcast.EXTRA_STATE), Mobilecore.StateStopped);
            String mode = intent.getStringExtra(TunnelBroadcast.EXTRA_MODE);
            // A mode this screen is not showing only gets to speak when it is
            // actually carrying traffic, which happens when the app is reopened
            // while the other mode's service is still running.
            if (mode != null && !mode.equals(controller.modeId())) {
                if (Mobilecore.StateStopped.equals(state) || !selectMode(mode)) {
                    return;
                }
            }
            tunnelState = state;
            tunnelMessage = intent.getStringExtra(TunnelBroadcast.EXTRA_MESSAGE);
            serviceProfileId = intent.getStringExtra(TunnelBroadcast.EXTRA_PROFILE_ID);
            parseMetrics(intent.getStringExtra(TunnelBroadcast.EXTRA_METRICS));
            busy = isTransitioning();
            renderConnectionState();
        }
    };

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        ui = new UiKit(this);
        repository = new ProfileRepository(this);
        modes = TunnelModes.available(this);
        controller = restoreMode();
        setContentView(buildShell());
        showPage(Page.HOME);
        refreshCatalog();
        // A recreated activity still carries the share that first opened it;
        // its invitations were queued then and may since have been used.
        if (savedInstanceState == null) {
            handleIncomingIntent(getIntent());
        }
    }

    @Override
    @SuppressLint("InlinedApi")
    protected void onStart() {
        super.onStart();
        IntentFilter filter = new IntentFilter(TunnelBroadcast.ACTION_STATE);
        registerReceiver(stateReceiver, filter, null, null, Context.RECEIVER_NOT_EXPORTED);
        // Every mode is asked, not just the shown one: the other mode's service
        // may still be running from a previous visit, and its reply re-selects it.
        for (TunnelController mode : modes) {
            mode.requestStatus();
        }
    }

    @Override
    protected void onStop() {
        unregisterReceiver(stateReceiver);
        super.onStop();
    }

    @Override
    protected void onDestroy() {
        worker.shutdownNow();
        probePool.shutdownNow();
        super.onDestroy();
    }

    @Override
    protected void onNewIntent(Intent intent) {
        super.onNewIntent(intent);
        setIntent(intent);
        handleIncomingIntent(intent);
    }

    @SuppressLint("SetTextI18n")
    private View buildShell() {
        LinearLayout root = new LinearLayout(this);
        root.setOrientation(LinearLayout.VERTICAL);
        root.setBackgroundColor(ui.themeColor(android.R.attr.colorBackground));
        root.setOnApplyWindowInsetsListener((view, insets) -> {
            android.graphics.Insets bars = insets.getInsets(WindowInsets.Type.systemBars());
            view.setPadding(0, bars.top, 0, bars.bottom);
            return insets;
        });

        pageContainer = new FrameLayout(this);
        root.addView(pageContainer, new LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                0,
                1));

        LinearLayout navigation = new LinearLayout(this);
        navigation.setOrientation(LinearLayout.HORIZONTAL);
        navigation.setPadding(ui.dp(8), ui.dp(4), ui.dp(8), ui.dp(6));
        navigation.setBackgroundColor(ui.themeColor(android.R.attr.colorBackgroundFloating));
        homeTab = navigationButton("Home", Page.HOME);
        profilesTab = navigationButton("Profiles", Page.PROFILES);
        settingsTab = navigationButton("Settings", Page.SETTINGS);
        navigation.addView(homeTab, UiKit.weightedWrap());
        navigation.addView(profilesTab, UiKit.weightedWrap());
        navigation.addView(settingsTab, UiKit.weightedWrap());
        root.addView(navigation, UiKit.matchWrap());
        return root;
    }

    private Button navigationButton(String label, Page page) {
        Button button = new Button(this);
        button.setText(label);
        button.setAllCaps(false);
        button.setMinHeight(ui.dp(52));
        button.setElevation(0);
        button.setStateListAnimator(null);
        button.setBackgroundTintList(ColorStateList.valueOf(Color.TRANSPARENT));
        button.setOnClickListener(view -> showPage(page));
        return button;
    }

    private void showPage(Page page) {
        currentPage = page;
        homeTab.setSelected(page == Page.HOME);
        profilesTab.setSelected(page == Page.PROFILES);
        settingsTab.setSelected(page == Page.SETTINGS);
        styleNavigationButton(homeTab, page == Page.HOME);
        styleNavigationButton(profilesTab, page == Page.PROFILES);
        styleNavigationButton(settingsTab, page == Page.SETTINGS);
        pageContainer.removeAllViews();
        switch (page) {
            case HOME:
                pageContainer.addView(buildHomePage());
                break;
            case PROFILES:
                pageContainer.addView(buildProfilesPage());
                break;
            case SETTINGS:
                pageContainer.addView(buildSettingsPage());
                break;
        }
    }

    @SuppressLint("SetTextI18n")
    private View buildHomePage() {
        LinearLayout content = pageContent("Queqiao", "Private access through your selected provider");

        LinearLayout connectionCard = ui.card();
        statusView = ui.text("Disconnected", 27, Typeface.BOLD);
        statusView.setGravity(Gravity.CENTER_HORIZONTAL);
        connectionCard.addView(statusView, UiKit.matchWrap());
        connectionSubtitle = ui.text("Import a profile to get started", 14, Typeface.NORMAL);
        connectionSubtitle.setGravity(Gravity.CENTER_HORIZONTAL);
        connectionSubtitle.setPadding(0, ui.dp(5), 0, ui.dp(14));
        connectionCard.addView(connectionSubtitle, UiKit.matchWrap());
        connectionButton = ui.primaryButton("Connect");
        connectionButton.setOnClickListener(view -> toggleConnection());
        connectionCard.addView(connectionButton, UiKit.matchWrap());
        content.addView(connectionCard, ui.spacedCard());

        ProfileRepository.ProfileRecord selected = selectedRecord();
        if (selected == null) {
            LinearLayout empty = ui.card();
            empty.addView(ui.text("No profile", 19, Typeface.BOLD), UiKit.matchWrap());
            TextView detail = ui.text(
                    "Import a one-time invitation to create a device-bound profile.",
                    14,
                    Typeface.NORMAL);
            detail.setPadding(0, ui.dp(6), 0, ui.dp(12));
            empty.addView(detail, UiKit.matchWrap());
            Button add = ui.primaryButton("Import invitation");
            add.setOnClickListener(view -> showImportDialog(null, false));
            empty.addView(add, UiKit.matchWrap());
            content.addView(empty, ui.spacedCard());
        } else {
            LinearLayout current = ui.card();
            current.addView(
                    ui.sectionTitle(isTunnelActive() ? "Current connection" : "Selected profile"),
                    UiKit.matchWrap());
            ui.addLabelValue(current, "Profile", selected.displayName);
            ui.addLabelValue(current, "Provider", selected.summary.endpoint);
            controller.renderConnectionDetails(ui, current, selected);
            // This is status information, deliberately rendered as text rather than a button.
            ui.addLabelValue(current, "Active device", selected.summary.deviceName);
            Button manage = ui.secondaryButton("Manage profiles");
            manage.setOnClickListener(view -> showPage(Page.PROFILES));
            current.addView(manage, ui.topSpaced());
            content.addView(current, ui.spacedCard());

            LinearLayout metrics = ui.card();
            metrics.addView(ui.sectionTitle("This connection"), UiKit.matchWrap());
            LinearLayout row = new LinearLayout(this);
            row.setOrientation(LinearLayout.HORIZONTAL);
            downloadedView = ui.metric("Downloaded", formatBytes(bytesDown));
            uploadedView = ui.metric("Uploaded", formatBytes(bytesUp));
            flowsView = ui.metric("Active flows", Long.toString(activeFlows));
            row.addView(downloadedView, UiKit.weightedWrap());
            row.addView(uploadedView, UiKit.weightedWrap());
            row.addView(flowsView, UiKit.weightedWrap());
            metrics.addView(row, UiKit.matchWrap());
            // What the rule list decided, so "my rules are working" is something
            // the screen can show rather than something the user has to infer.
            routedView = ui.text(routedSummary, 13, Typeface.NORMAL);
            routedView.setPadding(0, ui.dp(10), 0, 0);
            routedView.setVisibility(routedSummary.isEmpty() ? View.GONE : View.VISIBLE);
            metrics.addView(routedView, UiKit.matchWrap());
            content.addView(metrics, ui.spacedCard());
        }

        TextView privacy = ui.text(
                "Your provider can observe destinations, timing, and traffic that is not protected end-to-end.",
                12,
                Typeface.NORMAL);
        privacy.setPadding(ui.dp(6), ui.dp(4), ui.dp(6), ui.dp(20));
        content.addView(privacy, UiKit.matchWrap());
        renderConnectionState();
        return ui.scroll(content);
    }

    @SuppressLint("SetTextI18n")
    private View buildProfilesPage() {
        LinearLayout content = pageContent(
                "Profiles", "Choose the identity and provider used by the " + controller.noun());
        Button add = ui.primaryButton("Import invitations");
        add.setOnClickListener(view -> showImportDialog(null, false));
        content.addView(add, ui.spacedCard());
        if (ScanInvitationActivity.hasCamera(this)) {
            // Opens the import dialog straight onto the camera, so a page of
            // codes can be read without first finding the scan button.
            Button scan = ui.secondaryButton("Scan invitation QR codes");
            scan.setOnClickListener(view -> showImportDialog(null, true));
            content.addView(scan, ui.spacedCard());
        }

        Button testAll = ui.secondaryButton(testingProfiles ? "Testing connections…" : "Test all connections");
        testAll.setEnabled(canTestProfiles() && !catalog.profiles.isEmpty());
        testAll.setOnClickListener(view -> testAllProfiles());
        content.addView(testAll, ui.spacedCard());
        TextView testExplanation = ui.text(
                "Tests provider reachability and device authorization without opening a destination.",
                12,
                Typeface.NORMAL);
        testExplanation.setPadding(ui.dp(8), 0, ui.dp(8), ui.dp(8));
        content.addView(testExplanation, UiKit.matchWrap());

        if (repository.hasEnrollmentDraft()) {
            LinearLayout pending = ui.card();
            pending.addView(ui.text("Pending enrollment", 17, Typeface.BOLD), UiKit.matchWrap());
            pending.addView(ui.text(
                    "Resume with the original device key before importing another invitation.",
                    14,
                    Typeface.NORMAL), UiKit.matchWrap());
            Button resume = ui.secondaryButton("Resume import");
            resume.setOnClickListener(view -> showImportDialog(null, false));
            pending.addView(resume, ui.topSpaced());
            content.addView(pending, ui.spacedCard());
        }

        if (catalog.profiles.isEmpty()) {
            TextView empty = ui.text("No Queqiao profiles have been imported.", 15, Typeface.NORMAL);
            empty.setGravity(Gravity.CENTER);
            empty.setPadding(ui.dp(20), ui.dp(42), ui.dp(20), ui.dp(42));
            content.addView(empty, UiKit.matchWrap());
        } else {
            for (ProfileRepository.ProfileRecord profile : catalog.profiles) {
                Button row = new Button(this);
                boolean selected = profile.id.equals(catalog.selectedProfileId);
                String marker = selected ? "SELECTED  ·  " : "";
                ConnectionProbe probe = profileProbes.get(profile.id);
                String probeLine = probe == null ? "" : "\nTest: " + probe.summary();
                row.setText(marker + profile.displayName + "\n"
                        + profile.summary.endpoint + "\nDevice: " + profile.summary.deviceName
                        + probeLine);
                row.setGravity(Gravity.START | Gravity.CENTER_VERTICAL);
                row.setAllCaps(false);
                row.setTextSize(15);
                row.setPadding(ui.dp(16), ui.dp(13), ui.dp(16), ui.dp(13));
                row.setOnClickListener(view -> showProfileDialog(profile.id));
                row.setContentDescription(
                        profile.displayName + (selected ? ", selected profile" : ", available profile"));
                content.addView(row, ui.spacedCard());
            }
        }
        if (isTunnelActive()) {
            TextView locked = ui.text(
                    "Disconnect before switching or editing profiles.",
                    13,
                    Typeface.BOLD);
            locked.setPadding(ui.dp(8), ui.dp(8), ui.dp(8), ui.dp(16));
            content.addView(locked, UiKit.matchWrap());
        }
        return ui.scroll(content);
    }

    private View buildSettingsPage() {
        LinearLayout content = pageContent("Settings", "Privacy, security, and application information");

        LinearLayout privacy = ui.card();
        privacy.addView(ui.sectionTitle("Traffic and privacy"), UiKit.matchWrap());
        ui.addBodyText(privacy, "No ads or analytics.");
        ui.addBodyText(privacy, "Aggregate connection counters remain in memory only.");
        ui.addBodyText(
                privacy,
                "The active provider can observe destinations, timing, sizes, and content that is not protected end-to-end.");
        content.addView(privacy, ui.spacedCard());

        LinearLayout security = ui.card();
        security.addView(ui.sectionTitle("Profile security"), UiKit.matchWrap());
        ui.addBodyText(security, "Device keys are encrypted by Android Keystore and excluded from backup.");
        ui.addBodyText(
                security,
                "Queqiao imports one-time invitations instead of portable private profile files. Deleting a profile requires a new invitation.");
        content.addView(security, ui.spacedCard());

        LinearLayout about = ui.card();
        about.addView(ui.sectionTitle("About"), UiKit.matchWrap());
        ui.addLabelValue(about, "Version", applicationVersion());
        Button licenses = ui.secondaryButton("Open-source licenses");
        licenses.setOnClickListener(view -> showLicenses());
        about.addView(licenses, ui.topSpaced());
        content.addView(about, ui.spacedCard());

        controller.renderSettings(ui, content);
        if (modes.size() > 1) {
            content.addView(buildModeCard(), ui.spacedCard());
        }
        return ui.scroll(content);
    }

    private void toggleConnection() {
        if (isTunnelActive()) {
            disconnect();
        } else {
            prepareConnection();
        }
    }

    private void prepareConnection() {
        if (selectedRecord() == null) {
            showImportDialog(null, false);
            return;
        }
        if (Build.VERSION.SDK_INT >= 33
                && checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS)
                != PackageManager.PERMISSION_GRANTED) {
            requestPermissions(
                    new String[]{Manifest.permission.POST_NOTIFICATIONS},
                    REQUEST_NOTIFICATIONS);
            return;
        }
        prepareConnectionAfterNotificationPermission();
    }

    private void prepareConnectionAfterNotificationPermission() {
        ProfileRepository.ProfileRecord selected = selectedRecord();
        if (selected == null) {
            showImportDialog(null, false);
            return;
        }
        setBusy(true, "Validating profile…");
        worker.execute(() -> {
            try {
                ProfileRepository.ActiveProfile active = repository.profile(selected.id);
                if (active == null) {
                    throw new GeneralSecurityException("The selected Queqiao profile no longer exists");
                }
                String profile = active.profileJson;
                if (Mobilecore.profileNeedsRenewal(profile) != 0) {
                    runOnUiThread(() -> updateStatusText("Renewing identity…", null));
                    profile = Mobilecore.renewProfile(profile);
                    repository.replaceProfile(selected.id, profile);
                }
                pendingConnectProfileId = selected.id;
                runOnUiThread(this::requestConsent);
            } catch (Exception exception) {
                showFailure("Cannot connect", exception);
            }
        });
    }

    private void requestConsent() {
        Intent consent = controller.consentIntent();
        if (consent == null) {
            startConnection();
        } else {
            startActivityForResult(consent, REQUEST_CONSENT);
        }
    }

    @Override
    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        super.onActivityResult(requestCode, resultCode, data);
        if (requestCode == REQUEST_SCAN_INVITATION) {
            // The scanner queued what it read; show the queue it left.
            if (importForm != null && importForm.dialog.isShowing()) {
                renderImportForm(importForm);
            } else if (importQueue.size() > 0) {
                showImportDialog(null, false);
            }
            return;
        }
        if (requestCode != REQUEST_CONSENT) {
            return;
        }
        if (resultCode == RESULT_OK) {
            startConnection();
        } else {
            setBusy(false, "Permission was not granted");
        }
    }

    @Override
    public void onRequestPermissionsResult(
            int requestCode,
            String[] permissions,
            int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
        if (requestCode == REQUEST_NOTIFICATIONS) {
            // Android permits the foreground service even when notification
            // permission is declined; the system still surfaces it in Task Manager.
            prepareConnectionAfterNotificationPermission();
        }
    }

    private void startConnection() {
        String profileId = pendingConnectProfileId;
        pendingConnectProfileId = null;
        if (profileId == null) {
            showFailure(
                    "Cannot connect",
                    new GeneralSecurityException("The selected Queqiao profile is unavailable"));
            return;
        }
        controller.connect(profileId);
        tunnelState = Mobilecore.StateStarting;
        serviceProfileId = profileId;
        busy = true;
        renderConnectionState();
    }

    private void disconnect() {
        controller.disconnect();
        tunnelState = Mobilecore.StateStopping;
        busy = true;
        renderConnectionState();
    }

    /**
     * The import dialog collects any number of invitations -- scanned one
     * after another, pasted or shared several at a time, or typed -- and
     * imports them as one batch. Each invitation still becomes its own
     * device-bound profile with its own key and its own crash-safe draft.
     */
    @SuppressLint("SetTextI18n")
    private void showImportDialog(String note, boolean startScanning) {
        ImportForm open = importForm;
        if (open != null && open.dialog.isShowing()) {
            if (note != null) {
                open.pasteNote.setText(note);
                open.pasteNote.setVisibility(View.VISIBLE);
            }
            renderImportForm(open);
            if (startScanning && !open.importing) {
                startScanner();
            }
            return;
        }
        LinearLayout content = new LinearLayout(this);
        content.setOrientation(LinearLayout.VERTICAL);
        content.setPadding(ui.dp(20), ui.dp(8), ui.dp(20), ui.dp(8));

        TextView pending = ui.text(
                "An interrupted enrollment is ready to resume with its original device key. "
                        + "It is imported first.",
                14,
                Typeface.NORMAL);
        pending.setPadding(0, 0, 0, ui.dp(8));
        content.addView(pending, UiKit.matchWrap());

        LinearLayout actions = new LinearLayout(this);
        actions.setOrientation(LinearLayout.HORIZONTAL);
        Button scan = null;
        if (ScanInvitationActivity.hasCamera(this)) {
            scan = ui.secondaryButton("Scan QR codes");
            scan.setOnClickListener(view -> startScanner());
            actions.addView(scan, UiKit.weightedWrap());
        }
        Button paste = ui.secondaryButton("Paste invitations");
        actions.addView(paste, UiKit.weightedWrap());
        content.addView(actions, UiKit.matchWrap());

        TextView pasteNote = ui.text(note == null ? "" : note, 13, Typeface.NORMAL);
        pasteNote.setPadding(0, ui.dp(4), 0, 0);
        pasteNote.setVisibility(note == null ? View.GONE : View.VISIBLE);
        content.addView(pasteNote, UiKit.matchWrap());

        TextView explanation = ui.text(
                "Scan or paste as many invitations as you have; a paste takes every invitation "
                        + "on the clipboard. Each one is used once, and its permanent device key is "
                        + "generated on this device and never leaves it.",
                12,
                Typeface.NORMAL);
        explanation.setPadding(0, ui.dp(6), 0, 0);
        content.addView(explanation, UiKit.matchWrap());

        TextView queueHeader = ui.text("", 15, Typeface.BOLD);
        queueHeader.setPadding(0, ui.dp(12), 0, ui.dp(4));
        content.addView(queueHeader, UiKit.matchWrap());
        LinearLayout queueList = new LinearLayout(this);
        queueList.setOrientation(LinearLayout.VERTICAL);
        content.addView(queueList, UiKit.matchWrap());

        EditText invitation = new EditText(this);
        invitation.setHint("Or enter queqiao:// invitations");
        invitation.setInputType(
                InputType.TYPE_CLASS_TEXT
                        | InputType.TYPE_TEXT_FLAG_MULTI_LINE
                        | InputType.TYPE_TEXT_FLAG_NO_SUGGESTIONS
                        | InputType.TYPE_TEXT_VARIATION_URI);
        // The invitation is a short-lived bearer credential. Keep Android
        // from offering it to Autofill or saving it in view-instance state.
        invitation.setImportantForAutofill(View.IMPORTANT_FOR_AUTOFILL_NO);
        invitation.setSaveEnabled(false);
        invitation.setMinLines(2);
        invitation.setGravity(Gravity.TOP | Gravity.START);
        content.addView(invitation, ui.topSpaced());

        EditText deviceName = new EditText(this);
        deviceName.setHint("Device name");
        deviceName.setText(Build.MODEL);
        deviceName.setSelectAllOnFocus(true);
        deviceName.setInputType(
                InputType.TYPE_CLASS_TEXT | InputType.TYPE_TEXT_FLAG_CAP_SENTENCES);
        content.addView(deviceName, ui.topSpaced());

        TextView progress = ui.text("", 13, Typeface.NORMAL);
        progress.setPadding(0, ui.dp(8), 0, 0);
        progress.setVisibility(View.GONE);
        content.addView(progress, UiKit.matchWrap());

        AlertDialog dialog = new AlertDialog.Builder(this)
                .setTitle("Import profiles")
                .setView(ui.scroll(content))
                .setNegativeButton("Cancel", null)
                .setNeutralButton("Discard pending", null)
                .setPositiveButton("Import profile", null)
                .create();
        ImportForm form = new ImportForm(
                dialog, pending, scan, paste, pasteNote, queueHeader, queueList, invitation, deviceName, progress);
        importForm = form;
        paste.setOnClickListener(view -> {
            form.pasteNote.setText(queueFromText(readClipboard()));
            form.pasteNote.setVisibility(View.VISIBLE);
            renderImportForm(form);
        });
        invitation.addTextChangedListener(new TextWatcher() {
            @Override
            public void beforeTextChanged(CharSequence text, int start, int count, int after) {
            }

            @Override
            public void onTextChanged(CharSequence text, int start, int before, int count) {
            }

            @Override
            public void afterTextChanged(Editable text) {
                renderImportForm(form);
            }
        });
        dialog.setOnDismissListener(ignored -> {
            if (importForm == form) {
                importForm = null;
            }
        });
        dialog.setOnShowListener(ignored -> {
            dialog.getButton(AlertDialog.BUTTON_POSITIVE).setOnClickListener(view -> startImport(form));
            Button discard = dialog.getButton(AlertDialog.BUTTON_NEUTRAL);
            discard.setTextColor(Color.RED);
            discard.setOnClickListener(view -> confirmDiscardEnrollment(form));
            renderImportForm(form);
        });
        dialog.show();
        if (startScanning) {
            startScanner();
        }
    }

    @SuppressLint("SetTextI18n")
    private void renderImportForm(ImportForm form) {
        boolean hasDraft = repository.hasEnrollmentDraft();
        boolean editable = !form.importing;
        form.pending.setVisibility(hasDraft ? View.VISIBLE : View.GONE);

        List<InvitationBatch.QueuedInvitation> queued = importQueue.snapshot();
        form.queueHeader.setText("Ready to import (" + queued.size() + ")");
        form.queueHeader.setVisibility(queued.isEmpty() ? View.GONE : View.VISIBLE);
        form.queueList.removeAllViews();
        for (InvitationBatch.QueuedInvitation item : queued) {
            LinearLayout row = new LinearLayout(this);
            row.setOrientation(LinearLayout.HORIZONTAL);
            row.setGravity(Gravity.CENTER_VERTICAL);
            TextView label = ui.text(
                    item.endpoint.isEmpty() ? item.name : item.name + "\n" + item.endpoint,
                    14,
                    Typeface.NORMAL);
            row.addView(label, UiKit.weightedWrap());
            Button remove = ui.secondaryButton("Remove");
            remove.setEnabled(editable);
            remove.setContentDescription("Remove " + item.name);
            remove.setOnClickListener(view -> {
                importQueue.remove(item.uri);
                renderImportForm(form);
            });
            row.addView(remove, new LinearLayout.LayoutParams(
                    LinearLayout.LayoutParams.WRAP_CONTENT,
                    LinearLayout.LayoutParams.WRAP_CONTENT));
            form.queueList.addView(row, UiKit.matchWrap());
        }

        if (form.scan != null) {
            form.scan.setEnabled(editable);
        }
        form.paste.setEnabled(editable);
        form.invitation.setEnabled(editable);
        form.deviceName.setEnabled(editable);
        form.progress.setVisibility(form.importing ? View.VISIBLE : View.GONE);
        form.dialog.setCancelable(editable);

        int pendingCount = queued.size() + (hasDraft ? 1 : 0);
        for (String typed : InvitationBatch.extract(form.invitation.getText().toString())) {
            if (!importQueue.contains(typed)) {
                pendingCount++;
            }
        }
        Button importButton = form.dialog.getButton(AlertDialog.BUTTON_POSITIVE);
        if (importButton != null) {
            importButton.setText(InvitationBatch.importTitle(pendingCount, hasDraft));
            importButton.setEnabled(editable && pendingCount > 0);
        }
        Button discard = form.dialog.getButton(AlertDialog.BUTTON_NEUTRAL);
        if (discard != null) {
            discard.setVisibility(hasDraft ? View.VISIBLE : View.GONE);
            discard.setEnabled(editable);
        }
        Button cancel = form.dialog.getButton(AlertDialog.BUTTON_NEGATIVE);
        if (cancel != null) {
            cancel.setEnabled(editable);
        }
    }

    private void startScanner() {
        startActivityForResult(new Intent(this, ScanInvitationActivity.class), REQUEST_SCAN_INVITATION);
    }

    /** Queues every valid invitation in {@code text} and says what happened. */
    private String queueFromText(String text) {
        InvitationBatch.Evaluation result = InvitationBatch.evaluate(text, InvitationBatch.CORE);
        int added = importQueue.add(result.accepted);
        return InvitationBatch.pasteSummary(added, result.accepted.size() - added, result.rejected);
    }

    /** Read only here, in response to the user's tap on Paste. */
    private String readClipboard() {
        ClipboardManager clipboard = getSystemService(ClipboardManager.class);
        if (clipboard == null || !clipboard.hasPrimaryClip()) {
            return "";
        }
        ClipData clip = clipboard.getPrimaryClip();
        if (clip == null) {
            return "";
        }
        StringBuilder text = new StringBuilder();
        for (int index = 0; index < clip.getItemCount(); index++) {
            CharSequence value = clip.getItemAt(index).coerceToText(this);
            if (value != null) {
                text.append(value).append('\n');
            }
        }
        return text.toString();
    }

    private void startImport(ImportForm form) {
        String typed = form.invitation.getText().toString();
        if (!InvitationBatch.extract(typed).isEmpty()) {
            InvitationBatch.Evaluation result = InvitationBatch.evaluate(typed, InvitationBatch.CORE);
            int added = importQueue.add(result.accepted);
            if (result.rejected > 0) {
                // Keep the text so the invitation that failed can be corrected.
                form.invitation.setError(InvitationBatch.pasteSummary(
                        added, result.accepted.size() - added, result.rejected));
                renderImportForm(form);
                return;
            }
            form.invitation.setText("");
        } else if (!typed.isBlank()) {
            form.invitation.setError("That is not a Queqiao invitation");
            return;
        }
        boolean resuming = repository.hasEnrollmentDraft();
        String deviceName = form.deviceName.getText().toString().trim();
        if (!resuming && importQueue.size() == 0) {
            form.invitation.setError("Scan, paste, or enter an invitation");
            return;
        }
        if (importQueue.size() > 0 && deviceName.isEmpty()) {
            form.deviceName.setError("Device name is required");
            return;
        }
        form.invitation.setError(null);
        form.deviceName.setError(null);
        form.importing = true;
        renderImportForm(form);
        importBatch(form, resuming, deviceName);
    }

    /**
     * Enrolls the pending draft, if any, and then every queued invitation, one
     * after another with the same device name. Each invitation leaves the
     * queue only once its draft is saved, because from then on the draft --
     * not the invitation -- is what a retry must resume; so a failure stops
     * the batch with the failed invitation either still queued (nothing was
     * sent) or held as the resumable draft, and the rest still queued.
     */
    private void importBatch(ImportForm form, boolean resuming, String deviceName) {
        String first = resuming ? "Resuming import…" : "Importing profile…";
        setBusy(true, first);
        form.progress.setText(first);
        worker.execute(() -> {
            int imported = 0;
            int total = importQueue.size() + (resuming ? 1 : 0);
            try {
                if (resuming) {
                    completeEnrollmentDraft();
                    imported++;
                }
                InvitationBatch.QueuedInvitation next;
                while ((next = importQueue.first()) != null) {
                    total = imported + importQueue.size();
                    String message = total > 1
                            ? "Importing profile " + (imported + 1) + " of " + total + "…"
                            : "Importing profile…";
                    runOnUiThread(() -> {
                        form.progress.setText(message);
                        setBusy(true, message);
                    });
                    Mobilecore.validateInvitation(next.uri);
                    String draft = Mobilecore.prepareEnrollment(next.uri, deviceName);
                    repository.saveEnrollmentDraft(draft);
                    importQueue.remove(next.uri);
                    runOnUiThread(() -> renderImportForm(form));
                    completeEnrollmentDraft();
                    imported++;
                }
                int done = imported;
                runOnUiThread(() -> {
                    form.importing = false;
                    form.dialog.dismiss();
                    setBusy(false, done == 1 ? "Profile imported" : done + " profiles imported");
                    refreshCatalog();
                });
            } catch (Exception exception) {
                int done = imported;
                int planned = total;
                runOnUiThread(() -> {
                    form.importing = false;
                    renderImportForm(form);
                    refreshCatalog();
                });
                showFailure(done == 0
                        ? "Profile import failed"
                        : "Imported " + done + " of " + planned + " profiles, then import failed", exception);
            }
        });
    }

    /**
     * Sends the saved draft, stores the resulting profile, and only then
     * discards the draft, so an interrupted exchange can be resumed.
     */
    private void completeEnrollmentDraft() throws Exception {
        String draft = repository.enrollmentDraft();
        if (draft == null) {
            throw new GeneralSecurityException("The pending enrollment is unavailable");
        }
        String profile = Mobilecore.completeEnrollment(draft);
        repository.importProfile(profile);
        repository.discardEnrollmentDraft();
    }

    private void confirmDiscardEnrollment(ImportForm form) {
        new AlertDialog.Builder(this)
                .setTitle("Discard pending enrollment?")
                .setMessage(
                        "The pending device key will be deleted. The invitation may already be consumed and might not be reusable.")
                .setNegativeButton("Cancel", null)
                .setPositiveButton("Discard", (dialog, which) -> worker.execute(() -> {
                    try {
                        repository.discardEnrollmentDraft();
                        runOnUiThread(() -> {
                            if (importQueue.size() == 0) {
                                form.dialog.dismiss();
                            } else {
                                renderImportForm(form);
                            }
                            refreshCatalog();
                        });
                    } catch (Exception exception) {
                        showFailure("Could not discard enrollment", exception);
                    }
                }))
                .show();
    }

    /** The views of the open import dialog, which re-renders as the queue changes. */
    private static final class ImportForm {
        final AlertDialog dialog;
        final TextView pending;
        final Button scan;
        final Button paste;
        final TextView pasteNote;
        final TextView queueHeader;
        final LinearLayout queueList;
        final EditText invitation;
        final EditText deviceName;
        final TextView progress;
        boolean importing;

        ImportForm(
                AlertDialog dialog,
                TextView pending,
                Button scan,
                Button paste,
                TextView pasteNote,
                TextView queueHeader,
                LinearLayout queueList,
                EditText invitation,
                EditText deviceName,
                TextView progress) {
            this.dialog = dialog;
            this.pending = pending;
            this.scan = scan;
            this.paste = paste;
            this.pasteNote = pasteNote;
            this.queueHeader = queueHeader;
            this.queueList = queueList;
            this.invitation = invitation;
            this.deviceName = deviceName;
            this.progress = progress;
        }
    }

    @SuppressLint("SetTextI18n")
    private void showProfileDialog(String profileId) {
        ProfileRepository.ProfileRecord profile = catalog.find(profileId);
        if (profile == null) {
            refreshCatalog();
            return;
        }
        boolean selected = profile.id.equals(catalog.selectedProfileId);
        boolean editable = !isTunnelActive() && !busy && !testingProfiles;

        LinearLayout content = new LinearLayout(this);
        content.setOrientation(LinearLayout.VERTICAL);
        content.setPadding(ui.dp(20), ui.dp(4), ui.dp(20), 0);
        ui.addLabelValue(content, "Status", selected ? "Selected profile" : "Available");
        ui.addLabelValue(content, "Provider", profile.summary.name);
        ui.addLabelValue(content, "Endpoint", profile.summary.endpoint);
        ui.addLabelValue(content, "Active device", profile.summary.deviceName);
        ui.addLabelValue(content, "Certificate expires", formatExpiry(profile.summary.certificateExpiry));

        ConnectionProbe probe = profileProbes.get(profile.id);
        ui.addLabelValue(content, "Connection test", probe == null ? "Not tested" : probe.summary());
        if (probe != null && probe.detail != null) {
            TextView detail = ui.text(probe.detail, 12, Typeface.NORMAL);
            detail.setTextColor(Color.RED);
            detail.setTextIsSelectable(true);
            content.addView(detail, UiKit.matchWrap());
        }
        Button testConnection = ui.secondaryButton(
                probe != null && probe.status == ConnectionProbe.Status.TESTING
                        ? "Testing connection…"
                        : "Test connection");
        testConnection.setEnabled(canTestProfiles());
        content.addView(testConnection, ui.topSpaced());

        controller.renderProfileOptions(ui, content, profile, editable);

        AlertDialog dialog = new AlertDialog.Builder(this)
                .setTitle(profile.displayName)
                .setView(ui.scroll(content))
                .setNegativeButton("Close", null)
                .setNeutralButton("Delete", null)
                .setPositiveButton(selected ? "Rename" : "Use profile", null)
                .create();
        dialog.setOnShowListener(ignored -> {
            testConnection.setOnClickListener(view -> {
                dialog.dismiss();
                testProfiles(java.util.Collections.singletonList(profile.id));
            });
            dialog.getButton(AlertDialog.BUTTON_NEUTRAL).setTextColor(Color.RED);
            dialog.getButton(AlertDialog.BUTTON_NEUTRAL).setEnabled(editable);
            dialog.getButton(AlertDialog.BUTTON_NEUTRAL).setOnClickListener(view ->
                    confirmDeleteProfile(profile, dialog));
            dialog.getButton(AlertDialog.BUTTON_POSITIVE).setOnClickListener(view -> {
                if (selected) {
                    showRenameDialog(profile, dialog);
                } else if (editable) {
                    selectProfile(profile.id, dialog);
                }
            });
            dialog.getButton(AlertDialog.BUTTON_POSITIVE).setEnabled(editable);
        });
        dialog.show();
    }

    private void selectProfile(String profileId, AlertDialog dialog) {
        worker.execute(() -> {
            try {
                repository.select(profileId);
                runOnUiThread(() -> {
                    dialog.dismiss();
                    refreshCatalog();
                });
            } catch (Exception exception) {
                showFailure("Could not select profile", exception);
            }
        });
    }

    private void showRenameDialog(ProfileRepository.ProfileRecord profile, AlertDialog parent) {
        EditText name = new EditText(this);
        name.setText(profile.displayName);
        name.setSelectAllOnFocus(true);
        int padding = ui.dp(20);
        name.setPadding(padding, ui.dp(8), padding, ui.dp(8));
        AlertDialog dialog = new AlertDialog.Builder(this)
                .setTitle("Rename profile")
                .setView(name)
                .setNegativeButton("Cancel", null)
                .setPositiveButton("Save", null)
                .create();
        dialog.setOnShowListener(ignored ->
                dialog.getButton(AlertDialog.BUTTON_POSITIVE).setOnClickListener(view -> {
                    String requested = name.getText().toString().trim();
                    if (requested.isEmpty()) {
                        name.setError("Profile name is required");
                        return;
                    }
                    worker.execute(() -> {
                        try {
                            repository.rename(profile.id, requested);
                            runOnUiThread(() -> {
                                dialog.dismiss();
                                parent.dismiss();
                                refreshCatalog();
                            });
                        } catch (Exception exception) {
                            showFailure("Could not rename profile", exception);
                        }
                    });
                }));
        dialog.show();
    }

    private void confirmDeleteProfile(ProfileRepository.ProfileRecord profile, AlertDialog parent) {
        new AlertDialog.Builder(this)
                .setTitle("Delete " + profile.displayName + "?")
                .setMessage(
                        "This permanently deletes the device key. A new invitation is required to enroll again.")
                .setNegativeButton("Cancel", null)
                .setPositiveButton("Delete", (dialog, which) -> worker.execute(() -> {
                    try {
                        repository.delete(profile.id);
                        runOnUiThread(() -> {
                            parent.dismiss();
                            refreshCatalog();
                        });
                    } catch (Exception exception) {
                        showFailure("Could not delete profile", exception);
                    }
                }))
                .show();
    }

    private void testAllProfiles() {
        java.util.List<String> profileIds = new ArrayList<>();
        for (ProfileRepository.ProfileRecord profile : catalog.profiles) {
            profileIds.add(profile.id);
        }
        testProfiles(profileIds);
    }

    private void testProfiles(java.util.List<String> profileIds) {
        if (!canTestProfiles() || profileIds.isEmpty()) {
            return;
        }
        testingProfiles = true;
        for (String profileId : profileIds) {
            profileProbes.put(profileId, ConnectionProbe.testing());
        }
        showPage(currentPage);
        renderConnectionState();
        int[] remaining = {profileIds.size()};
        for (String profileId : profileIds) {
            probePool.execute(() -> {
                ConnectionProbe outcome;
                try {
                    ProfileRepository.ActiveProfile active = repository.profile(profileId);
                    if (active == null) {
                        throw new GeneralSecurityException("The Queqiao profile no longer exists");
                    }
                    outcome = ConnectionProbe.available(
                            Mobilecore.probeProfileJSON(active.profileJson, 10_000));
                } catch (Exception exception) {
                    outcome = ConnectionProbe.unavailable(exception, VpnExclusion.current(this));
                }
                ConnectionProbe completed = outcome;
                // The count lives on the UI thread, where every completion lands.
                runOnUiThread(() -> {
                    profileProbes.put(profileId, completed);
                    if (--remaining[0] == 0) {
                        testingProfiles = false;
                        renderConnectionState();
                    }
                    if (currentPage == Page.PROFILES) {
                        showPage(Page.PROFILES);
                    }
                });
            });
        }
    }

    private void refreshCatalog() {
        worker.execute(() -> {
            try {
                ProfileRepository.Catalog refreshed = repository.catalog();
                runOnUiThread(() -> {
                    catalog = refreshed;
                    profileProbes.keySet().removeIf(id -> catalog.find(id) == null);
                    showPage(currentPage);
                });
            } catch (Exception exception) {
                showFailure("Stored profiles are unavailable", exception);
            }
        });
    }

    private void renderConnectionState() {
        if (statusView == null || connectionButton == null || connectionSubtitle == null) {
            return;
        }
        String display = displayState(tunnelState);
        updateStatusText(display, tunnelMessage);
        ProfileRepository.ProfileRecord selected = selectedRecord();
        if (selected == null) {
            connectionSubtitle.setText(R.string.import_profile_intro);
        } else {
            connectionSubtitle.setText(getString(
                    R.string.connection_profile_summary,
                    selected.displayName,
                    selected.summary.endpoint));
        }
        boolean active = isTunnelActive();
        connectionButton.setText(active ? "Disconnect" : "Connect");
        boolean connectionEnabled = !busy && !testingProfiles && (active || selected != null);
        connectionButton.setEnabled(connectionEnabled);
        connectionButton.setBackgroundTintList(ColorStateList.valueOf(
                !connectionEnabled
                        ? Color.rgb(158, 158, 158)
                        : active
                        ? Color.rgb(183, 28, 28)
                        : ui.themeColor(android.R.attr.colorAccent)));
        if (downloadedView != null) {
            downloadedView.setText(getString(
                    R.string.downloaded_metric,
                    formatBytes(bytesDown)));
            uploadedView.setText(getString(
                    R.string.uploaded_metric,
                    formatBytes(bytesUp)));
            flowsView.setText(getString(R.string.active_flows_metric, activeFlows));
            routedView.setText(routedSummary);
            routedView.setVisibility(routedSummary.isEmpty() ? View.GONE : View.VISIBLE);
        }
    }

    private void updateStatusText(String state, String message) {
        if (statusView != null) {
            statusView.setText(message == null || message.isBlank() ? state : state + "\n" + message);
        }
    }

    private void setBusy(boolean value, String message) {
        busy = value;
        tunnelMessage = message;
        renderConnectionState();
    }

    private void parseMetrics(String encoded) {
        if (encoded == null) {
            if (!isTunnelActive()) {
                bytesUp = 0;
                bytesDown = 0;
                activeFlows = 0;
                routedSummary = "";
            }
            return;
        }
        try {
            JSONObject transport = new JSONObject(encoded).optJSONObject("transport");
            if (transport != null) {
                bytesUp = transport.optLong("BytesUp", 0);
                bytesDown = transport.optLong("BytesDown", 0);
                activeFlows = transport.optLong("ActiveFlows", 0);
            }
            JSONObject packets = new JSONObject(encoded).optJSONObject("packets");
            JSONObject routing = packets == null ? null : packets.optJSONObject("routing");
            routedSummary = routing == null || routing.optInt("rules", 0) == 0
                    ? ""
                    : "Rules sent " + routing.optLong("proxied", 0) + " flows through Queqiao, "
                            + routing.optLong("directed", 0) + " direct, "
                            + routing.optLong("rejected", 0) + " rejected";
        } catch (Exception ignored) {
            // Metrics are optional UI decoration and never affect tunnel state.
        }
    }

    /**
     * A user-selected share of plain text. Every invitation in it is validated
     * and queued, so a message carrying several invitations imports them all.
     */
    private void handleIncomingIntent(Intent intent) {
        if (intent == null
                || !Intent.ACTION_SEND.equals(intent.getAction())
                || !"text/plain".equals(intent.getType())) {
            return;
        }
        String text = intent.getStringExtra(Intent.EXTRA_TEXT);
        if (InvitationBatch.extract(text).isEmpty()) {
            return;
        }
        showImportDialog(queueFromText(text), false);
    }

    private void showLicenses() {
        try {
            String notices;
            try (java.io.InputStream input = getAssets().open("THIRD_PARTY_NOTICES.txt")) {
                ByteArrayOutputStream output = new ByteArrayOutputStream(64 * 1024);
                byte[] buffer = new byte[8 * 1024];
                int total = 0;
                int count;
                while ((count = input.read(buffer)) != -1) {
                    total += count;
                    if (total > 1024 * 1024) {
                        throw new IOException("License notice exceeds 1 MiB");
                    }
                    output.write(buffer, 0, count);
                }
                notices = output.toString(StandardCharsets.UTF_8.name());
            }
            TextView text = ui.text(notices, 11, Typeface.MONOSPACE.getStyle());
            text.setTextIsSelectable(true);
            text.setTypeface(Typeface.MONOSPACE);
            text.setPadding(ui.dp(16), ui.dp(16), ui.dp(16), ui.dp(16));
            new AlertDialog.Builder(this)
                    .setTitle("Open-source licenses")
                    .setView(ui.scroll(text))
                    .setPositiveButton("Close", null)
                    .show();
        } catch (IOException exception) {
            showFailure("License notices are unavailable", exception);
        }
    }

    private void showFailure(String title, Exception exception) {
        String message = exception.getMessage();
        if (message == null || message.isBlank()) {
            message = exception.getClass().getSimpleName();
        }
        String finalMessage = message;
        runOnUiThread(() -> {
            busy = false;
            new AlertDialog.Builder(this)
                    .setTitle(title)
                    .setMessage(finalMessage)
                    .setPositiveButton("OK", null)
                    .show();
            renderConnectionState();
        });
    }

    @Override
    public Activity activity() {
        return this;
    }

    @Override
    public ProfileRepository repository() {
        return repository;
    }

    @Override
    public void background(Runnable work) {
        worker.execute(work);
    }

    @Override
    public void failure(String title, Exception exception) {
        showFailure(title, exception);
    }

    @Override
    public void refresh() {
        refreshCatalog();
    }

    @Override
    public boolean connectionActive() {
        return isTunnelActive();
    }

    /**
     * Switching while connected would leave the other service running with
     * nothing on screen driving it.
     */
    @SuppressLint("SetTextI18n")
    private View buildModeCard() {
        LinearLayout card = ui.card();
        card.addView(ui.sectionTitle("Connection mode"), UiKit.matchWrap());
        boolean editable = !isTunnelActive() && !busy && !testingProfiles;
        RadioGroup group = new RadioGroup(this);
        for (TunnelController mode : modes) {
            RadioButton option = new RadioButton(this);
            option.setId(View.generateViewId());
            option.setText(mode.title() + "\n" + mode.summary());
            option.setTextSize(14);
            option.setTag(mode);
            option.setChecked(mode.modeId().equals(controller.modeId()));
            option.setEnabled(editable);
            group.addView(option, UiKit.matchWrap());
        }
        group.setOnCheckedChangeListener((ignored, checkedId) -> {
            RadioButton option = group.findViewById(checkedId);
            if (option != null && option.getTag() instanceof TunnelController) {
                chooseMode(((TunnelController) option.getTag()).modeId());
            }
        });
        card.addView(group, UiKit.matchWrap());
        return card;
    }

    private void chooseMode(String modeId) {
        if (modeId.equals(controller.modeId())) {
            return;
        }
        if (isTunnelActive()) {
            showFailure(
                    "Disconnect first",
                    new IllegalStateException("Disconnect before changing the connection mode"));
            return;
        }
        if (!selectMode(modeId)) {
            return;
        }
        getSharedPreferences(PREFERENCES, MODE_PRIVATE)
                .edit()
                .putString(PREFERENCE_MODE, modeId)
                .apply();
        showPage(currentPage);
    }

    /** Points the screen at a mode without persisting the choice. */
    private boolean selectMode(String modeId) {
        for (TunnelController mode : modes) {
            if (mode.modeId().equals(modeId)) {
                controller = mode;
                return true;
            }
        }
        return false;
    }

    private TunnelController restoreMode() {
        String stored = getSharedPreferences(PREFERENCES, MODE_PRIVATE)
                .getString(PREFERENCE_MODE, null);
        for (TunnelController mode : modes) {
            if (mode.modeId().equals(stored)) {
                return mode;
            }
        }
        return modes.get(0);
    }

    private LinearLayout pageContent(String title, String subtitle) {
        LinearLayout content = new LinearLayout(this);
        content.setOrientation(LinearLayout.VERTICAL);
        content.setPadding(ui.dp(18), ui.dp(16), ui.dp(18), ui.dp(24));
        content.addView(ui.text(title, 30, Typeface.BOLD), UiKit.matchWrap());
        TextView subtitleView = ui.text(subtitle, 14, Typeface.NORMAL);
        subtitleView.setPadding(0, ui.dp(3), 0, ui.dp(12));
        content.addView(subtitleView, UiKit.matchWrap());
        return content;
    }

    private void styleNavigationButton(Button button, boolean selected) {
        button.setTextColor(selected
                ? ui.themeColor(android.R.attr.colorAccent)
                : ui.themeColor(android.R.attr.textColorSecondary));
        button.setTypeface(Typeface.DEFAULT, selected ? Typeface.BOLD : Typeface.NORMAL);
    }

    private ProfileRepository.ProfileRecord selectedRecord() {
        return catalog.find(catalog.selectedProfileId);
    }

    private boolean isTunnelActive() {
        return Mobilecore.StateRunning.equals(tunnelState)
                || Mobilecore.StateStarting.equals(tunnelState)
                || Mobilecore.StateStopping.equals(tunnelState);
    }

    private boolean canTestProfiles() {
        // A test may run while connected in either mode: the full tunnel excludes
        // this app's UID, and export mode holds no interface, so the probe always
        // leaves by the device's ordinary route rather than through Queqiao.
        return !busy && !testingProfiles;
    }

    private boolean isTransitioning() {
        return Mobilecore.StateStarting.equals(tunnelState)
                || Mobilecore.StateStopping.equals(tunnelState);
    }

    private String displayState(String state) {
        if (state == null || state.isEmpty()) {
            return "Unavailable";
        }
        if (Mobilecore.StateStopped.equals(state)) {
            return "Disconnected";
        }
        if (Mobilecore.StateRunning.equals(state)) {
            return "Connected";
        }
        return Character.toUpperCase(state.charAt(0)) + state.substring(1) + "…";
    }

    private static String valueOr(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }

    private String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        double value = bytes;
        String[] units = {"KiB", "MiB", "GiB", "TiB"};
        int unit = -1;
        do {
            value /= 1024.0;
            unit++;
        } while (value >= 1024 && unit < units.length - 1);
        return String.format(java.util.Locale.getDefault(), "%.1f %s", value, units[unit]);
    }

    private String formatExpiry(String encoded) {
        try {
            Date date = Date.from(Instant.parse(encoded));
            return DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(date);
        } catch (Exception ignored) {
            return encoded;
        }
    }

    private String applicationVersion() {
        try {
            android.content.pm.PackageInfo info = getPackageManager().getPackageInfo(getPackageName(), 0);
            return info.versionName + " (" + info.getLongVersionCode() + ")";
        } catch (PackageManager.NameNotFoundException exception) {
            return "Unknown";
        }
    }

}

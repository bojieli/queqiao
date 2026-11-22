package io.github.bojieli.queqiao;

import android.content.Context;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.security.GeneralSecurityException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

import mobilecore.Mobilecore;

final class ProfileRepository {
    static final String CATALOG = "profile_catalog_v1";
    static final String PROFILE_PREFIX = "client_profile_v1.";

    private static final int CATALOG_VERSION = 1;
    private static final Object CATALOG_LOCK = new Object();

    private final SecureStore store;

    ProfileRepository(Context context) {
        store = new SecureStore(context);
    }

    Catalog catalog() throws Exception {
        synchronized (CATALOG_LOCK) {
            return loadCatalogLocked();
        }
    }

    ProfileRecord importProfile(String profileJson) throws Exception {
        Mobilecore.validateProfile(profileJson);
        ProfileSummary summary = ProfileSummary.fromJson(
                new JSONObject(Mobilecore.profileSummaryJSON(profileJson)));
        synchronized (CATALOG_LOCK) {
            Catalog catalog = loadCatalogLocked();
            for (int index = 0; index < catalog.profiles.size(); index++) {
                ProfileRecord existing = catalog.profiles.get(index);
                if (existing.summary.deviceId.equals(summary.deviceId)) {
                    store.put(existing.secretAccount, profileJson);
                    ProfileRecord refreshed = existing.withSummary(summary);
                    catalog.profiles.set(index, refreshed);
                    catalog.selectedProfileId = existing.id;
                    saveCatalogLocked(catalog);
                    return refreshed;
                }
            }

            String identifier = UUID.randomUUID().toString().toLowerCase(Locale.ROOT);
            String account = PROFILE_PREFIX + identifier;
            ProfileRecord record = new ProfileRecord(
                    identifier,
                    account,
                    summary.name,
                    summary,
                    RoutingConfiguration.DEFAULT,
                    Instant.now().toString());
            store.put(account, profileJson);
            try {
                catalog.profiles.add(record);
                catalog.selectedProfileId = identifier;
                saveCatalogLocked(catalog);
            } catch (Exception exception) {
                store.delete(account);
                throw exception;
            }
            return record;
        }
    }

    ActiveProfile selectedProfile() throws Exception {
        synchronized (CATALOG_LOCK) {
            Catalog catalog = loadCatalogLocked();
            if (catalog.selectedProfileId == null) {
                return null;
            }
            return findLocked(catalog, catalog.selectedProfileId);
        }
    }

    ActiveProfile profile(String id) throws Exception {
        synchronized (CATALOG_LOCK) {
            return findLocked(loadCatalogLocked(), id);
        }
    }

    void select(String id) throws Exception {
        synchronized (CATALOG_LOCK) {
            Catalog catalog = loadCatalogLocked();
            if (catalog.find(id) == null) {
                throw new GeneralSecurityException("The selected Queqiao profile no longer exists");
            }
            catalog.selectedProfileId = id;
            saveCatalogLocked(catalog);
        }
    }

    void rename(String id, String requestedName) throws Exception {
        String name = requestedName == null ? "" : requestedName.trim();
        if (name.isEmpty() || name.length() > 80) {
            throw new GeneralSecurityException("Profile names must contain between 1 and 80 characters");
        }
        synchronized (CATALOG_LOCK) {
            Catalog catalog = loadCatalogLocked();
            int index = catalog.indexOf(id);
            if (index < 0) {
                throw new GeneralSecurityException("The selected Queqiao profile no longer exists");
            }
            catalog.profiles.set(index, catalog.profiles.get(index).withDisplayName(name));
            saveCatalogLocked(catalog);
        }
    }

    void setRouting(String id, RoutingConfiguration routing) throws Exception {
        synchronized (CATALOG_LOCK) {
            Catalog catalog = loadCatalogLocked();
            int index = catalog.indexOf(id);
            if (index < 0) {
                throw new GeneralSecurityException("The selected Queqiao profile no longer exists");
            }
            catalog.profiles.set(index, catalog.profiles.get(index).withRouting(routing));
            saveCatalogLocked(catalog);
        }
    }

    void replaceProfile(String id, String profileJson) throws Exception {
        Mobilecore.validateProfile(profileJson);
        ProfileSummary summary = ProfileSummary.fromJson(
                new JSONObject(Mobilecore.profileSummaryJSON(profileJson)));
        synchronized (CATALOG_LOCK) {
            Catalog catalog = loadCatalogLocked();
            int index = catalog.indexOf(id);
            if (index < 0) {
                throw new GeneralSecurityException("The active Queqiao profile no longer exists");
            }
            ProfileRecord existing = catalog.profiles.get(index);
            if (!existing.summary.deviceId.equals(summary.deviceId)) {
                throw new GeneralSecurityException("A renewed profile changed the enrolled device identity");
            }
            store.put(existing.secretAccount, profileJson);
            catalog.profiles.set(index, existing.withSummary(summary));
            saveCatalogLocked(catalog);
        }
    }

    void delete(String id) throws Exception {
        synchronized (CATALOG_LOCK) {
            Catalog catalog = loadCatalogLocked();
            int index = catalog.indexOf(id);
            if (index < 0) {
                return;
            }
            ProfileRecord removed = catalog.profiles.remove(index);
            if (id.equals(catalog.selectedProfileId)) {
                catalog.selectedProfileId = catalog.profiles.isEmpty()
                        ? null
                        : catalog.profiles.get(0).id;
            }
            saveCatalogLocked(catalog);
            store.delete(removed.secretAccount);
        }
    }

    String enrollmentDraft() throws GeneralSecurityException {
        return store.get(SecureStore.ENROLLMENT_DRAFT);
    }

    boolean hasEnrollmentDraft() {
        return store.contains(SecureStore.ENROLLMENT_DRAFT);
    }

    void saveEnrollmentDraft(String draft) throws GeneralSecurityException {
        store.put(SecureStore.ENROLLMENT_DRAFT, draft);
    }

    void discardEnrollmentDraft() throws GeneralSecurityException {
        store.delete(SecureStore.ENROLLMENT_DRAFT);
    }

    private ActiveProfile findLocked(Catalog catalog, String id) throws Exception {
        ProfileRecord record = catalog.find(id);
        if (record == null) {
            return null;
        }
        String profileJson = store.get(record.secretAccount);
        if (profileJson == null) {
            throw new GeneralSecurityException("The encrypted profile data is missing");
        }
        Mobilecore.validateProfile(profileJson);
        return new ActiveProfile(record, profileJson);
    }

    private Catalog loadCatalogLocked() throws Exception {
        String encoded = store.get(CATALOG);
        if (encoded == null) {
            return migrateLegacyProfileLocked();
        }
        Catalog catalog = Catalog.fromJson(new JSONObject(encoded));
        if (catalog.version != CATALOG_VERSION) {
            throw new GeneralSecurityException("Unsupported encrypted profile catalog version");
        }
        if (catalog.normalize()) {
            saveCatalogLocked(catalog);
        }
        return catalog;
    }

    private Catalog migrateLegacyProfileLocked() throws Exception {
        Catalog catalog = new Catalog();
        String legacy = store.get(SecureStore.PROFILE);
        if (legacy == null) {
            saveCatalogLocked(catalog);
            return catalog;
        }
        Mobilecore.validateProfile(legacy);
        ProfileSummary summary = ProfileSummary.fromJson(
                new JSONObject(Mobilecore.profileSummaryJSON(legacy)));
        String identifier = UUID.randomUUID().toString().toLowerCase(Locale.ROOT);
        String account = PROFILE_PREFIX + identifier;
        ProfileRecord record = new ProfileRecord(
                identifier,
                account,
                summary.name,
                summary,
                RoutingConfiguration.DEFAULT,
                Instant.now().toString());
        store.put(account, legacy);
        catalog.profiles.add(record);
        catalog.selectedProfileId = identifier;
        try {
            saveCatalogLocked(catalog);
        } catch (Exception exception) {
            store.delete(account);
            throw exception;
        }
        // The catalog is authoritative after it commits. Legacy cleanup must
        // not invalidate a successfully migrated profile.
        try {
            store.delete(SecureStore.PROFILE);
        } catch (GeneralSecurityException ignored) {
            // An unreachable encrypted duplicate is safe and can be removed by a later build.
        }
        return catalog;
    }

    private void saveCatalogLocked(Catalog catalog) throws GeneralSecurityException, JSONException {
        catalog.normalize();
        store.put(CATALOG, catalog.toJson().toString());
    }

    static final class ActiveProfile {
        final ProfileRecord record;
        final String profileJson;

        ActiveProfile(ProfileRecord record, String profileJson) {
            this.record = record;
            this.profileJson = profileJson;
        }
    }

    static final class Catalog {
        int version = CATALOG_VERSION;
        String selectedProfileId;
        final List<ProfileRecord> profiles = new ArrayList<>();

        ProfileRecord find(String id) {
            int index = indexOf(id);
            return index < 0 ? null : profiles.get(index);
        }

        int indexOf(String id) {
            if (id == null) {
                return -1;
            }
            for (int index = 0; index < profiles.size(); index++) {
                if (id.equals(profiles.get(index).id)) {
                    return index;
                }
            }
            return -1;
        }

        boolean normalize() {
            boolean changed = false;
            Set<String> identifiers = new HashSet<>();
            for (int index = profiles.size() - 1; index >= 0; index--) {
                String id = profiles.get(index).id;
                if (id.isEmpty() || !identifiers.add(id)) {
                    profiles.remove(index);
                    changed = true;
                }
            }
            if (selectedProfileId == null || find(selectedProfileId) == null) {
                String replacement = profiles.isEmpty() ? null : profiles.get(0).id;
                if (replacement != null || selectedProfileId != null) {
                    selectedProfileId = replacement;
                    changed = true;
                }
            }
            return changed;
        }

        JSONObject toJson() throws JSONException {
            JSONArray encodedProfiles = new JSONArray();
            for (ProfileRecord profile : profiles) {
                encodedProfiles.put(profile.toJson());
            }
            JSONObject object = new JSONObject()
                    .put("version", version)
                    .put("profiles", encodedProfiles);
            if (selectedProfileId != null) {
                object.put("selected_profile_id", selectedProfileId);
            }
            return object;
        }

        static Catalog fromJson(JSONObject object) throws JSONException {
            Catalog catalog = new Catalog();
            catalog.version = object.getInt("version");
            catalog.selectedProfileId = object.optString("selected_profile_id", null);
            JSONArray profiles = object.getJSONArray("profiles");
            for (int index = 0; index < profiles.length(); index++) {
                catalog.profiles.add(ProfileRecord.fromJson(profiles.getJSONObject(index)));
            }
            return catalog;
        }
    }

    static final class ProfileRecord {
        final String id;
        final String secretAccount;
        final String displayName;
        final ProfileSummary summary;
        final RoutingConfiguration routing;
        final String importedAt;

        ProfileRecord(
                String id,
                String secretAccount,
                String displayName,
                ProfileSummary summary,
                RoutingConfiguration routing,
                String importedAt) {
            this.id = id;
            this.secretAccount = secretAccount;
            this.displayName = displayName;
            this.summary = summary;
            this.routing = routing;
            this.importedAt = importedAt;
        }

        ProfileRecord withDisplayName(String name) {
            return new ProfileRecord(id, secretAccount, name, summary, routing, importedAt);
        }

        ProfileRecord withRouting(RoutingConfiguration replacement) {
            return new ProfileRecord(id, secretAccount, displayName, summary, replacement, importedAt);
        }

        ProfileRecord withSummary(ProfileSummary replacement) {
            return new ProfileRecord(id, secretAccount, displayName, replacement, routing, importedAt);
        }

        JSONObject toJson() throws JSONException {
            JSONObject object = new JSONObject()
                    .put("id", id)
                    .put("secret_account", secretAccount)
                    .put("display_name", displayName)
                    .put("summary", summary.toJson())
                    .put("imported_at", importedAt);
            routing.writeTo(object);
            return object;
        }

        static ProfileRecord fromJson(JSONObject object) throws JSONException {
            return new ProfileRecord(
                    object.getString("id"),
                    object.getString("secret_account"),
                    object.getString("display_name"),
                    ProfileSummary.fromJson(object.getJSONObject("summary")),
                    RoutingConfiguration.readFrom(object),
                    object.getString("imported_at"));
        }
    }

    static final class ProfileSummary {
        final int version;
        final String name;
        final String endpoint;
        final String providerId;
        final String gatewayId;
        final String accountId;
        final String deviceId;
        final String deviceName;
        final String certificateExpiry;

        ProfileSummary(
                int version,
                String name,
                String endpoint,
                String providerId,
                String gatewayId,
                String accountId,
                String deviceId,
                String deviceName,
                String certificateExpiry) {
            this.version = version;
            this.name = name;
            this.endpoint = endpoint;
            this.providerId = providerId;
            this.gatewayId = gatewayId;
            this.accountId = accountId;
            this.deviceId = deviceId;
            this.deviceName = deviceName;
            this.certificateExpiry = certificateExpiry;
        }

        JSONObject toJson() throws JSONException {
            return new JSONObject()
                    .put("version", version)
                    .put("name", name)
                    .put("endpoint", endpoint)
                    .put("provider_id", providerId)
                    .put("gateway_id", gatewayId)
                    .put("account_id", accountId)
                    .put("device_id", deviceId)
                    .put("device_name", deviceName)
                    .put("certificate_expiry", certificateExpiry);
        }

        static ProfileSummary fromJson(JSONObject object) throws JSONException {
            return new ProfileSummary(
                    object.getInt("version"),
                    object.getString("name"),
                    object.getString("endpoint"),
                    object.getString("provider_id"),
                    object.getString("gateway_id"),
                    object.getString("account_id"),
                    object.getString("device_id"),
                    object.getString("device_name"),
                    object.getString("certificate_expiry"));
        }
    }
}

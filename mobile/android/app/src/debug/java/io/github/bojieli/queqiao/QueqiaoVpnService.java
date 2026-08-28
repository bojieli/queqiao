package io.github.bojieli.queqiao;

import android.content.Intent;
import android.net.VpnService;
import android.os.IBinder;
import android.os.ParcelFileDescriptor;

import java.io.IOException;
import java.util.function.BooleanSupplier;

import mobilecore.Mobilecore;
import mobilecore.Observer;
import mobilecore.Protector;
import mobilecore.Session;

/**
 * The full-device tunnel: Android hands us a TUN descriptor and the core owns
 * every packet on the device. It is the packet stack's only host, and the only
 * component that can exempt its own sockets from the interface it installs.
 */
public final class QueqiaoVpnService extends VpnService
        implements Protector, TunnelServiceCore.Backend {
    static final String MODE = "tunnel";

    // The MTU, resolvers, and interface addresses below are declared again in
    // iOS's TunnelNetworkSettings and, for the MTU, in the Go packet stack.
    // scripts/test_mobile_route_parity.py holds the three copies together.
    private static final int MTU = 1280;

    private final TunnelServiceCore core = new TunnelServiceCore(this, this);
    private final Object tunnelLock = new Object();
    private ParcelFileDescriptor tunnel;

    @Override
    public void onCreate() {
        super.onCreate();
        core.onCreate();
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        return core.onStartCommand(intent, startId);
    }

    @Override
    public String modeId() {
        return MODE;
    }

    @Override
    public Session start(
            ProfileRepository.ActiveProfile profile,
            Observer observer,
            BooleanSupplier stillCurrent) throws Exception {
        Builder builder = new Builder()
                .setSession("Queqiao — " + profile.record.displayName)
                .setMtu(MTU)
                // Excluding our UID ensures identity renewal cannot re-enter the VPN.
                // Individual outer sockets are protected as an independent boundary.
                .addDisallowedApplication(getPackageName())
                .addAddress("10.77.0.2", 32)
                .addAddress("fd77:7171:6f::2", 128)
                .addDnsServer("1.1.1.1")
                .addDnsServer("2606:4700:4700::1111")
                .setBlocking(false);
        RoutePolicy.apply(builder, profile.record.trafficPolicy);
        if (!stillCurrent.getAsBoolean()) {
            throw new IOException("The connection was superseded before the interface was installed");
        }
        ParcelFileDescriptor established = builder.establish();
        if (established == null) {
            throw new IOException("Android refused to establish the VPN interface");
        }
        synchronized (tunnelLock) {
            tunnel = established;
        }
        Session session = Mobilecore.newSession(observer, this);
        // Rules are installed before the session starts, so no flow is ever
        // carried under a different list than the one it was decided by.
        DebugRoutingRules.install(this, session);
        session.start(profile.profileJson, established.getFd(), 0, MTU, true);
        return session;
    }

    @Override
    public void release() {
        ParcelFileDescriptor established;
        synchronized (tunnelLock) {
            established = tunnel;
            tunnel = null;
        }
        if (established == null) {
            return;
        }
        try {
            established.close();
        } catch (IOException ignored) {
            // Descriptor teardown is best effort after the core has stopped.
        }
    }

    @Override
    public String notificationDetail() {
        return "Full device tunnel";
    }

    @Override
    public boolean protect(long fileDescriptor) {
        return fileDescriptor >= 0
                && fileDescriptor <= Integer.MAX_VALUE
                && protect((int) fileDescriptor);
    }

    @Override
    public void onRevoke() {
        core.stop("VPN permission revoked");
        super.onRevoke();
    }

    @Override
    public void onDestroy() {
        core.onDestroy();
        super.onDestroy();
    }

    @Override
    public void onTrimMemory(int level) {
        super.onTrimMemory(level);
        core.onTrimMemory(level);
    }

    @Override
    public void onLowMemory() {
        super.onLowMemory();
        core.onLowMemory();
    }

    @Override
    public IBinder onBind(Intent intent) {
        return super.onBind(intent);
    }
}

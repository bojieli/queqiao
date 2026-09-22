package io.github.bojieli.queqiao;

import java.util.List;

/** The connection modes this app offers; the first is the default for a new install. */
final class TunnelModes {
    private TunnelModes() {
    }

    static List<TunnelController> available(TunnelHost host) {
        return List.of(new VpnTunnelController(host), new ProxyTunnelController(host));
    }
}

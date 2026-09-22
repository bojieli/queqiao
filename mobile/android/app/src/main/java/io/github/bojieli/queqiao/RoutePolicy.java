package io.github.bojieli.queqiao;

import android.net.InetAddresses;
import android.net.IpPrefix;
import android.net.VpnService;
import android.os.Build;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

final class RoutePolicy {
    // The same list as RoutePlan.localNetworks on iOS. Neither client can see
    // the other, so scripts/test_mobile_route_parity.py compares them.
    private static final String[] LOCAL_EXCLUSIONS = {
            "10.0.0.0/8",
            "100.64.0.0/10",
            "127.0.0.0/8",
            "169.254.0.0/16",
            "172.16.0.0/12",
            "192.168.0.0/16",
            "::1/128",
            "fc00::/7",
            "fe80::/10"
    };

    private RoutePolicy() {
    }

    /** The most bypass routes one interface is given; RoutePlan.defaultLimit on iOS. */
    static final int ROUTE_LIMIT = 8_192;

    /** What was installed, so the caller can say so rather than leave routing silent. */
    static final class Plan {
        final List<RouteSpec> excluded;
        final List<String> rejected;
        final int truncated;

        Plan(List<RouteSpec> excluded, List<String> rejected, int truncated) {
            this.excluded = excluded;
            this.rejected = rejected;
            this.truncated = truncated;
        }

        String diagnosticSummary() {
            StringBuilder text = new StringBuilder(excluded.size() + " bypass routes");
            if (truncated > 0) {
                text.append(", ").append(truncated).append(" dropped at the route limit");
            }
            if (!rejected.isEmpty()) {
                text.append(", ").append(rejected.size()).append(" rejected as invalid");
            }
            return text.toString();
        }
    }

    /**
     * The same plan RoutePlan.make builds on iOS: hand-entered routes win over the
     * bundled set, and when the limit is reached the widest blocks are the ones kept.
     */
    static Plan plan(RoutingConfiguration routing, List<RouteSpec> chinaDirect) {
        if (!routing.rulesApply()) {
            return new Plan(Collections.emptyList(), Collections.emptyList(), 0);
        }
        List<String> userRoutes = new ArrayList<>();
        if (routing.bypassLocalNetworks) {
            Collections.addAll(userRoutes, LOCAL_EXCLUSIONS);
        }
        userRoutes.addAll(routing.customRoutes);
        List<RouteSpec> parsed = new ArrayList<>();
        List<String> rejected = new ArrayList<>();
        for (String encoded : userRoutes) {
            try {
                parsed.add(RouteSpec.parse(encoded));
            } catch (UnknownHostException exception) {
                rejected.add(encoded);
            }
        }
        int truncated = 0;
        List<RouteSpec> kept = coalesce(parsed);
        if (kept.size() > ROUTE_LIMIT) {
            truncated += kept.size() - ROUTE_LIMIT;
            kept = widestFirst(kept, ROUTE_LIMIT);
        }
        List<RouteSpec> builtIn = routing.bypassChinaDirect ? chinaDirect : Collections.emptyList();
        int room = ROUTE_LIMIT - kept.size();
        if (room > 0 && !builtIn.isEmpty()) {
            List<RouteSpec> additions = builtIn;
            if (additions.size() > room) {
                truncated += additions.size() - room;
                additions = widestFirst(additions, room);
            }
            List<RouteSpec> combined = new ArrayList<>(kept);
            combined.addAll(additions);
            kept = coalesce(combined);
        } else if (!builtIn.isEmpty()) {
            truncated += builtIn.size();
        }
        return new Plan(kept, rejected, truncated);
    }

    static Plan apply(VpnService.Builder builder, RoutingConfiguration routing, List<RouteSpec> chinaDirect) {
        Plan plan = plan(routing, chinaDirect);
        try {
            if (plan.excluded.isEmpty()) {
                builder.addRoute("0.0.0.0", 0);
                builder.addRoute("::", 0);
            } else if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                builder.addRoute("0.0.0.0", 0);
                builder.addRoute("::", 0);
                for (RouteSpec route : plan.excluded) {
                    // Android refuses a loopback prefix outright, and loopback never
                    // reaches a VPN interface, so the shared local list's entries for
                    // it have nothing to exclude here.
                    if (route.inetAddress().isLoopbackAddress()) {
                        continue;
                    }
                    builder.excludeRoute(new IpPrefix(route.inetAddress(), route.prefixLength));
                }
            } else {
                // Before Android 13 a bypass can only be expressed as the routes that
                // remain, which is practical for a handful of blocks and not for a
                // registry set of thousands; GEOIP rules still keep those direct.
                for (RouteSpec route : remainderAfter(plan.excluded)) {
                    builder.addRoute(route.inetAddress(), route.prefixLength);
                }
            }
        } catch (UnknownHostException exception) {
            throw new IllegalStateException("A bypass route could not be installed", exception);
        }
        return plan;
    }

    /** Whether this device can carry the bundled country set as routes. */
    static boolean supportsCountryRoutes() {
        return Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU;
    }

    static List<RouteSpec> remainderAfter(List<RouteSpec> exclusions) throws UnknownHostException {
        List<RouteSpec> routes = new ArrayList<>();
        routes.add(RouteSpec.parse("0.0.0.0/0"));
        routes.add(RouteSpec.parse("::/0"));
        for (RouteSpec exclusion : exclusions) {
            List<RouteSpec> next = new ArrayList<>();
            for (RouteSpec route : routes) {
                next.addAll(route.subtract(exclusion));
            }
            routes = next;
        }
        return Collections.unmodifiableList(routes);
    }

    /** Drops duplicates and any block already covered by a wider one. */
    static List<RouteSpec> coalesce(List<RouteSpec> prefixes) {
        List<RouteSpec> sorted = new ArrayList<>(prefixes);
        sorted.sort(Comparator.<RouteSpec>comparingInt(route -> route.bitCount)
                .thenComparing(route -> route.network)
                .thenComparingInt(route -> route.prefixLength));
        List<RouteSpec> kept = new ArrayList<>();
        for (RouteSpec candidate : sorted) {
            RouteSpec last = kept.isEmpty() ? null : kept.get(kept.size() - 1);
            if (last != null && last.covers(candidate)) {
                continue;
            }
            kept.add(candidate);
        }
        return kept;
    }

    private static List<RouteSpec> widestFirst(List<RouteSpec> prefixes, int limit) {
        List<RouteSpec> sorted = new ArrayList<>(prefixes);
        sorted.sort(Comparator.comparingInt(route -> route.prefixLength));
        return new ArrayList<>(sorted.subList(0, limit));
    }

    static final class RouteSpec {
        final BigInteger network;
        final int prefixLength;
        final int bitCount;

        RouteSpec(BigInteger network, int prefixLength, int bitCount) {
            this.bitCount = bitCount;
            this.prefixLength = prefixLength;
            this.network = normalize(network, prefixLength, bitCount);
        }

        static RouteSpec parse(String encoded) throws UnknownHostException {
            String text = encoded.trim();
            int separator = text.lastIndexOf('/');
            String host = separator < 0 ? text : text.substring(0, separator);
            // Numeric only: a name here would be resolved, and a bypass list is
            // typed by hand, so a typo must not become a DNS query.
            if (host.isEmpty() || !InetAddresses.isNumericAddress(host)) {
                throw new UnknownHostException("Invalid address: " + encoded);
            }
            InetAddress address = InetAddresses.parseNumericAddress(host);
            int bitCount = address.getAddress().length * 8;
            final int prefixLength;
            try {
                prefixLength = separator < 0 ? bitCount : Integer.parseInt(text.substring(separator + 1));
            } catch (NumberFormatException exception) {
                throw new UnknownHostException("Invalid CIDR prefix: " + encoded);
            }
            if (prefixLength < 0 || prefixLength > bitCount) {
                throw new UnknownHostException("Invalid CIDR prefix: " + encoded);
            }
            return new RouteSpec(new BigInteger(1, address.getAddress()), prefixLength, bitCount);
        }

        InetAddress inetAddress() throws UnknownHostException {
            return InetAddress.getByAddress(toFixedWidth(network, bitCount / 8));
        }

        String address() throws UnknownHostException {
            return inetAddress().getHostAddress();
        }

        /** Canonical CIDR text, the form a bypass list is stored in. */
        String cidr() throws UnknownHostException {
            return address() + "/" + prefixLength;
        }

        boolean covers(RouteSpec other) {
            return bitCount == other.bitCount
                    && prefixLength <= other.prefixLength
                    && normalize(other.network, prefixLength, bitCount).equals(network);
        }

        boolean contains(String address) throws UnknownHostException {
            InetAddress parsed = InetAddress.getByName(address);
            if (parsed.getAddress().length * 8 != bitCount) {
                return false;
            }
            BigInteger value = new BigInteger(1, parsed.getAddress());
            return normalize(value, prefixLength, bitCount).equals(network);
        }

        List<RouteSpec> subtract(RouteSpec exclusion) {
            if (bitCount != exclusion.bitCount || !overlaps(exclusion)) {
                return Collections.singletonList(this);
            }
            if (exclusion.prefixLength <= prefixLength) {
                return Collections.emptyList();
            }
            int childPrefix = prefixLength + 1;
            BigInteger childSize = BigInteger.ONE.shiftLeft(bitCount - childPrefix);
            RouteSpec first = new RouteSpec(network, childPrefix, bitCount);
            RouteSpec second = new RouteSpec(network.add(childSize), childPrefix, bitCount);
            List<RouteSpec> result = new ArrayList<>();
            result.addAll(first.subtract(exclusion));
            result.addAll(second.subtract(exclusion));
            return result;
        }

        private boolean overlaps(RouteSpec other) {
            int commonPrefix = Math.min(prefixLength, other.prefixLength);
            return normalize(network, commonPrefix, bitCount)
                    .equals(normalize(other.network, commonPrefix, bitCount));
        }

        private static BigInteger normalize(BigInteger value, int prefixLength, int bitCount) {
            if (prefixLength == 0) {
                return BigInteger.ZERO;
            }
            int hostBits = bitCount - prefixLength;
            return value.shiftRight(hostBits).shiftLeft(hostBits);
        }

        private static byte[] toFixedWidth(BigInteger value, int width) {
            byte[] encoded = value.toByteArray();
            byte[] result = new byte[width];
            int sourceOffset = Math.max(0, encoded.length - width);
            int count = Math.min(width, encoded.length);
            System.arraycopy(encoded, sourceOffset, result, width - count, count);
            return result;
        }
    }
}

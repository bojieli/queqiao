"""The tunnel constants the two mobile clients declare separately.

iOS and Android build the same tunnel through APIs that share no code:
NEPacketTunnelNetworkSettings on one side, VpnService.Builder on the
other, with the Go packet stack underneath both. Every value they must
agree on is therefore written twice, and today the only thing holding
them together is a comment in RoutePlan.swift saying "the two must not
drift".

Drift here is quiet. A local-exclusion list that gains an entry on one
platform is a route that reaches the LAN on one phone and the gateway on
another; an MTU that changes on one side fragments every flow on that
side alone. None of it fails a build, and neither client can see the
other. This is the check that does.

Update a value on purpose by updating it in every file listed here.
"""

import pathlib
import re
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
IOS_SETTINGS = ROOT / "mobile/ios/Shared/TunnelNetworkSettings.swift"
IOS_ROUTES = ROOT / "mobile/ios/Shared/RoutePlan.swift"
ANDROID_VPN = (
    ROOT / "mobile/android/app/src/main/java/io/github/bojieli/queqiao"
    "/QueqiaoVpnService.java"
)
ANDROID_ROUTES = (
    ROOT / "mobile/android/app/src/main/java/io/github/bojieli/queqiao"
    "/RoutePolicy.java"
)
GO_PACKET_STACK = ROOT / "mobile/core/packetstack.go"
IOS_RULES = ROOT / "mobile/ios/Queqiao/ProfileRulesSection.swift"
IOS_ROUTING = ROOT / "mobile/ios/Shared/RoutingConfiguration.swift"
IOS_CATALOG = ROOT / "mobile/ios/Shared/ProfileCatalog.swift"
ANDROID_PRESET = ROOT / "mobile/android/app/src/main/assets/china-preset.conf"
ANDROID_ROUTING = (
    ROOT / "mobile/android/app/src/main/java/io/github/bojieli/queqiao"
    "/RoutingConfiguration.java"
)
ANDROID_REVIEW = (
    ROOT / "mobile/android/app/src/main/java/io/github/bojieli/queqiao"
    "/RuleReview.java"
)


def read(path):
    return path.read_text(encoding="utf-8")


def bracketed_strings(source, marker, closing):
    """The quoted entries of a list literal introduced by `marker`."""
    start = source.index(marker) + len(marker)
    return re.findall(r'"([^"]+)"', source[start : source.index(closing, start)])


def integer(source, pattern):
    match = re.search(pattern, source)
    assert match, f"no match for {pattern!r}"
    return int(match.group(1).replace("_", ""))


class MobileTunnelParityTests(unittest.TestCase):
    def test_the_local_exclusion_lists_are_the_same_list(self):
        ios = bracketed_strings(read(IOS_ROUTES), "static let localNetworks = [", "]")
        android = bracketed_strings(read(ANDROID_ROUTES), "LOCAL_EXCLUSIONS = {", "}")
        self.assertEqual(ios, android)
        # A guard against a parse that silently found nothing on both sides.
        self.assertIn("192.168.0.0/16", ios)
        self.assertIn("fe80::/10", ios)

    def test_the_tunnel_mtu_is_one_number_in_three_languages(self):
        ios = integer(read(IOS_SETTINGS), r"static let mtu: Int64 = ([\d_]+)")
        android = integer(read(ANDROID_VPN), r"int MTU = ([\d_]+);")
        core = integer(read(GO_PACKET_STACK), r"defaultMTU\s+= ([\d_]+)")
        self.assertEqual((ios, android), (core, core))
        # 1280 is the IPv6 minimum every path must carry without fragmenting.
        # Raising it is a measurement, not an edit.
        self.assertEqual(core, 1280)

    def test_both_clients_resolve_through_the_same_servers(self):
        ios = bracketed_strings(read(IOS_SETTINGS), "static let dnsServers = [", "]")
        android = re.findall(r'addDnsServer\("([^"]+)"\)', read(ANDROID_VPN))
        self.assertEqual(ios, android)
        self.assertTrue(ios, "no resolvers were parsed from either client")

    def test_the_interface_addresses_do_not_diverge(self):
        settings = read(IOS_SETTINGS)
        ios = [
            re.search(r'static let ipv4Address = "([^"]+)"', settings).group(1),
            re.search(r'static let ipv6Address = "([^"]+)"', settings).group(1),
        ]
        android = re.findall(r'addAddress\("([^"]+)", \d+\)', read(ANDROID_VPN))
        self.assertEqual(ios, android)

    def test_the_china_preset_is_one_text_in_both_clients(self):
        literal = re.search(
            r'static let chinaPreset = """\n(.*?)\n(\s*)"""', read(IOS_RULES), re.S
        )
        self.assertIsNotNone(literal, "the Swift preset literal was not found")
        indent = literal.group(2)
        ios = "\n".join(
            line[len(indent):] if line.startswith(indent) else line.lstrip()
            for line in literal.group(1).split("\n")
        ) + "\n"
        self.assertEqual(ios, read(ANDROID_PRESET))
        self.assertIn("GEOIP,CN,DIRECT\nFINAL,PROXY\n", ios)

    def test_the_routing_modes_share_their_wire_values_and_titles(self):
        ios = re.findall(r'case \w+ = "([a-z-]+)"', read(IOS_ROUTING))
        android = re.findall(r'^\s+[A-Z_]+\(\n\s+"([a-z-]+)",', read(ANDROID_ROUTING), re.M)
        self.assertEqual(ios, android)
        self.assertEqual(ios, ["all-traffic", "bypass-rules"])
        for title in ("Route all traffic", "Use bypass rules"):
            self.assertIn(f'"{title}"', read(IOS_ROUTING))
            self.assertIn(f'"{title}"', read(ANDROID_ROUTING))

    def test_the_bypass_limits_are_the_same_numbers(self):
        ios_routes = integer(read(IOS_CATALOG), r"static let maximumBypassRoutes = ([\d_]+)")
        android_routes = integer(read(ANDROID_ROUTING), r"int MAX_CUSTOM_ROUTES = ([\d_]+);")
        self.assertEqual(ios_routes, android_routes)
        ios_limit = integer(read(IOS_ROUTES), r"static let defaultLimit = ([\d_]+)")
        android_limit = integer(read(ANDROID_ROUTES), r"int ROUTE_LIMIT = ([\d_]+);")
        self.assertEqual(ios_limit, android_limit)

    def test_the_rule_lint_accepts_the_same_vocabulary(self):
        def quoted(source, marker, closing):
            return sorted(bracketed_strings(source, marker, closing))

        swift, java = read(IOS_RULES), read(ANDROID_REVIEW)
        self.assertEqual(
            quoted(swift, "knownTypes: Set<String> = [", "]"),
            quoted(java, "KNOWN_TYPES = new HashSet<>(Arrays.asList(", "));"),
        )
        self.assertEqual(
            quoted(swift, "knownActions: Set<String> = [", "]"),
            quoted(java, "KNOWN_ACTIONS = new HashSet<>(Arrays.asList(", "));"),
        )


if __name__ == "__main__":
    unittest.main()

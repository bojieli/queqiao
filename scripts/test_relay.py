"""A relay hop in front of the gateway ([relay])."""
import configparser
import importlib
import io
import pathlib
import subprocess
import sys
import unittest
from unittest import mock

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))
ab = importlib.import_module("ab_campaign")
tm = importlib.import_module("test_minimal")

BASE = tm.BASE.replace("tls = false", "tls = false\ntrials = bulk\nidle_wake = false") + """
[relay]
host = rl
address = 198.51.100.7
[production:rl]
host = rl
units = queqiao-relay-x
"""


def parse(text):
    cp = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=(";", "#"))
    cp.read_string(text)
    return ab.parse_config(cp)


class RelayHop(unittest.TestCase):
    def test_clients_dial_the_relay_which_forwards_each_arms_port(self):
        cfg = parse(BASE)
        self.assertIn("rl", cfg.hosts())
        self.assertEqual(cfg.endpoint_address, "198.51.100.7")
        self.assertEqual(sorted(p for p, _ in cfg.planned_ports()["rl"]), [23471, 23472])
        camp = ab.Campaign(cfg, "<o>", ".", dry=True, stream=io.StringIO())
        self.assertEqual(camp.host_cpu_hosts("a"), ["gw", "rl", "c1"])
        out = io.StringIO()
        with mock.patch.object(subprocess, "run", side_effect=AssertionError("dry run must not run a command")):
            ab.Campaign(cfg, "<o>", ".", dry=True, stream=out).run(dry_blocks=1)
        text = out.getvalue()
        # The invitation points the clients at the relay, not the gateway.
        self.assertIn("--endpoint 198.51.100.7:23471", text)
        self.assertNotIn("--endpoint 192.0.2.1:", text)
        # One relay per arm, on that arm's port, to the gateway's same port;
        # both up before any client enrolls.
        for port in (23471, 23472):
            self.assertIn(f"relay --listen :{port} --upstream 192.0.2.1:{port} --dial-timeout 3s --idle-timeout 15m "
                          "--max-connections 8192", text)
        self.assertLess(text.index("[rl] start relay-cand"), text.index("enroll a with main"))
        self.assertIn("clients dial 198.51.100.7 (relay on rl", text)

    def test_the_runner_owns_the_relays_listen_and_upstream(self):
        with self.assertRaises(ab.ConfigError):
            parse(BASE.replace("address = 198.51.100.7", "address = 198.51.100.7\nflags = --listen :1"))
        with self.assertRaises(ab.ConfigError):
            parse(BASE.replace("[production:rl]\nhost = rl\nunits = queqiao-relay-x\n", ""))   # an undeclared host

    def test_without_a_relay_clients_dial_the_gateway(self):
        cfg = parse(tm.BASE)
        self.assertIsNone(cfg.relay)
        self.assertEqual(cfg.endpoint_address, "192.0.2.1")


if __name__ == "__main__":
    unittest.main()

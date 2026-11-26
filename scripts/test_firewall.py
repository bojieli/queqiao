"""The temporary firewall step ([firewall:*]): opt-in, reverted on every exit."""
import configparser
import importlib
import io
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))
ab = importlib.import_module("ab_campaign")
tm = importlib.import_module("test_minimal")

BASE = tm.BASE.replace("tls = false", "tls = false\ntrials = bulk\nidle_wake = false").replace(
    "host = gw\naddress", "host = local\naddress").replace("[production:gw]\nhost = gw", "[production:gw]\nhost = local") \
    .replace("[client:a]\nhost = c1", "[client:a]\nhost = local\nsocks_port_base = 24581\nmetrics_port_base = 24591") \
    .replace("[production:c1]\nhost = c1\n", "")
FW = BASE + "[firewall:gw]\nhost = local\nallow = 23471:23472/tcp 23471:23472/udp\n"

# A stub sudo (drops -n) and a stub ufw that keeps its rules in a file.
UFW = r'''#!/bin/sh
f="$UFW_RULES"
case "$1" in
  status) echo "Status: active"; echo; n=0; while read -r r; do n=$((n+1)); printf '[%2d] %-26s ALLOW IN    Anywhere\n' $n "$r"; done < "$f";;
  allow) echo "$2" >> "$f"; echo "Rule added";;
  delete) [ -n "$UFW_DELETE_FAILS" ] && { echo "ERROR: delete failed" >&2; exit 1; }
          grep -v -x -F -- "$3" "$f" > "$f.n"; mv "$f.n" "$f"; echo "Rule deleted";;
esac
echo "$*" >> "$UFW_LOG"
'''


def parse(text):
    cp = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=(";", "#"))
    cp.read_string(text)
    return ab.parse_config(cp)


class Firewall(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        d = self.tmp.name
        for name, body in (("sudo", '#!/bin/sh\n[ "$1" = -n ] && shift\nexec "$@"\n'), ("ufw", UFW)):
            p = os.path.join(d, name)
            pathlib.Path(p).write_text(body)
            os.chmod(p, 0o755)
        self.rules = os.path.join(d, "rules")
        pathlib.Path(self.rules).write_text("22/tcp\n12540/tcp\n12540/udp\n")
        self.log = os.path.join(d, "ufw.log")
        env = {"PATH": d + os.pathsep + os.environ["PATH"], "UFW_RULES": self.rules, "UFW_LOG": self.log}
        p = mock.patch.dict(os.environ, env)
        p.start()
        self.addCleanup(p.stop)

    def camp(self, text=FW):
        cfg = parse(text)
        camp = ab.Campaign(cfg, os.path.join(self.tmp.name, "out"), ".", dry=False, stream=io.StringIO())
        camp.hosts["local"].home = self.tmp.name
        return camp

    def calls(self):
        return pathlib.Path(self.log).read_text().splitlines() if os.path.exists(self.log) else []

    def test_rules_are_added_and_the_rule_set_restored(self):
        camp = self.camp()
        original = pathlib.Path(self.rules).read_text()
        camp.open_firewall()
        self.assertIn("23471:23472/udp", pathlib.Path(self.rules).read_text())
        camp.close_firewall()
        self.assertEqual(pathlib.Path(self.rules).read_text(), original)
        self.assertEqual(camp.firewall_state, {"local": "restored"})
        self.assertEqual(camp.firewall_failures, [])
        self.assertEqual([c for c in self.calls() if not c.startswith("status")],
                         ["allow 23471:23472/tcp", "allow 23471:23472/udp",
                          "delete allow 23471:23472/udp", "delete allow 23471:23472/tcp"])

    def test_a_failed_revert_is_reported_loudly(self):
        camp = self.camp()
        camp.open_firewall()
        with mock.patch.dict(os.environ, {"UFW_DELETE_FAILS": "1"}):
            camp.close_firewall()
        self.assertEqual(camp.firewall_state, {"local": "REVERT FAILED"})
        self.assertTrue(any("ufw delete allow 23471:23472/tcp failed" in x for x in camp.firewall_failures))
        self.assertTrue(any("differ from those before" in x for x in camp.firewall_failures))

    def test_an_existing_rule_is_refused_before_anything_is_added(self):
        pathlib.Path(self.rules).write_text("22/tcp\n23471:23472/udp\n")
        camp = self.camp()
        with self.assertRaises(ab.Refused):
            camp.open_firewall()
        self.assertEqual([c for c in self.calls() if not c.startswith("status")], [])

    def test_only_the_campaigns_own_ports_may_be_opened(self):
        with self.assertRaises(ab.ConfigError):
            parse(FW.replace("23471:23472/udp", "12540/udp"))
        with self.assertRaises(ab.ConfigError):
            parse(FW.replace("23471:23472/udp", "23471:23472/icmp"))
        self.assertEqual(parse(BASE).firewalls, [])        # opt-in

    def test_every_exit_path_reverts(self):
        # Setup fails after the rules went in: the run aborts, and the rules
        # come out again; a failed revert sets exit 8 and status.json says so.
        original = pathlib.Path(self.rules).read_text()
        for fail_delete, want in ((False, 1), (True, 8)):
            pathlib.Path(self.rules).write_text(original)
            camp = self.camp()
            os.makedirs(camp.out, exist_ok=True)
            env = {"UFW_DELETE_FAILS": "1"} if fail_delete else {}
            with mock.patch.object(ab.Campaign, "preflight"), mock.patch.object(ab.Campaign, "build", return_value={}), \
                    mock.patch.object(ab.Campaign, "setup", side_effect=RuntimeError("boom")), \
                    mock.patch.object(ab.Campaign, "teardown"), mock.patch.object(ab.Campaign, "verify_teardown", return_value=[]), \
                    mock.patch.object(ab.Campaign, "clean", return_value=[]), mock.patch.object(ab.Campaign, "check_production"), \
                    mock.patch.dict(os.environ, env):
                with mock.patch.object(ab.os, "makedirs"):
                    rc = camp.run()
            self.assertEqual(rc, want)
            status = json.loads(pathlib.Path(camp.out, "status.json").read_text())
            if fail_delete:
                self.assertEqual(status["firewall"], {"local": "REVERT FAILED"})
                self.assertIn("FIREWALL REVERT FAILED", camp.stream.getvalue())
            else:
                self.assertEqual(status["firewall"], {"local": "restored"})
                self.assertEqual(pathlib.Path(self.rules).read_text(), original)
            os.remove(os.path.join(camp.out, "status.json"))
            os.remove(os.path.join(camp.out, "plan.json"))

    def test_the_dry_run_prints_the_rules_and_only_these_and_the_snapshot_use_sudo(self):
        text = FW.replace("[production:gw]\nhost = local", "[production:gw]\nhost = local\nunits = queqiaod\nsnapshot_sudo = true")
        out = io.StringIO()
        with mock.patch.object(subprocess, "run", side_effect=AssertionError("dry run must not run a command")):
            ab.Campaign(parse(text), "<o>", ".", dry=True, stream=out).run(dry_blocks=1)
        blocks, cur = [], None
        for line in out.getvalue().splitlines():
            if line.startswith("[") and line.rstrip().endswith(":"):
                cur = [line, []]
                blocks.append(cur)
            elif cur is not None and line.startswith("    "):
                cur[1].append(line)
        sudo = [b[0] for b in blocks if any("sudo" in l for l in b[1])]
        self.assertTrue(all("production snapshot" in h or "firewall" in h for h in sudo), sudo)
        cmds = [l.strip() for b in blocks if "firewall" in b[0] for l in b[1]]
        self.assertEqual([c for c in cmds if "allow" in c],
                         ["sudo -n ufw allow 23471:23472/tcp", "sudo -n ufw allow 23471:23472/udp",
                          "sudo -n ufw delete allow 23471:23472/udp", "sudo -n ufw delete allow 23471:23472/tcp"])


if __name__ == "__main__":
    unittest.main()

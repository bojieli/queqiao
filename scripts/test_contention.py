"""Production snapshot through sudo, and the host CPU contention void rule."""
import configparser
import importlib
import io
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))
ab = importlib.import_module("ab_campaign")
an = importlib.import_module("ab_analyze")
tm = importlib.import_module("test_minimal")

BASE = tm.BASE.replace("tls = false", "tls = false\ntrials = bulk, mixed-67\nidle_wake = false")


def parse(text):
    cp = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=(";", "#"))
    cp.read_string(text)
    return ab.parse_config(cp)


class SnapshotThroughSudo(unittest.TestCase):
    def run_snapshot(self, sudo_ok):
        d = tempfile.mkdtemp()
        log = os.path.join(d, "sudo.log")
        with open(os.path.join(d, "systemctl"), "w") as f:
            f.write('#!/bin/sh\ncase "$*" in *--value*) echo 0;; '
                    '*) printf "LoadState=loaded\\nActiveState=inactive\\nMainPID=0\\nNRestarts=0\\n";; esac\n')
        with open(os.path.join(d, "sudo"), "w") as f:
            f.write(f'#!/bin/sh\necho "$*" >> {log}\n' + ("shift\nexec \"$@\"\n" if sudo_ok else "exit 1\n"))
        for n in ("systemctl", "sudo"):
            os.chmod(os.path.join(d, n), 0o755)
        script = ab.SNAPSHOT_SH.format(work="/nonexistent/qq-ab-x", units="", sudo_units="queqiaod", launchd="",
                                       pattern="'queqiao'")
        p = subprocess.run(["bash", "-s"], input=script, capture_output=True, text=True,
                           env=dict(os.environ, PATH=d + os.pathsep + os.environ["PATH"]))
        calls = open(log).read().splitlines() if os.path.exists(log) else []
        return ab.parse_snapshot(p.stdout), calls

    def test_the_snapshot_observes_a_unit_through_sudo_n(self):
        snap, calls = self.run_snapshot(sudo_ok=True)
        self.assertIn("queqiaod", snap["units"])
        self.assertTrue(calls and all(c.startswith("-n systemctl show queqiaod") for c in calls), calls)

    def test_a_failing_sudo_is_an_observation_error(self):
        snap, calls = self.run_snapshot(sudo_ok=False)
        self.assertEqual(snap["unit_errors"], ["queqiaod"])
        self.assertTrue(calls)

    def test_only_the_snapshot_uses_sudo(self):
        text = BASE.replace("[production:gw]\nhost = gw", "[production:gw]\nhost = gw\nunits = queqiaod\nsnapshot_sudo = true")
        cfg = parse(text)
        self.assertTrue(next(p for p in cfg.production if p.host == "gw").snapshot_sudo)
        out = io.StringIO()
        with mock.patch.object(subprocess, "run", side_effect=AssertionError("dry run must not run a command")):
            ab.Campaign(cfg, "<o>", ".", dry=True, stream=out).run(dry_blocks=1)
        text = out.getvalue()
        # Split the printed commands into blocks by their "[host] what:" header.
        blocks, cur = [], None
        for line in text.splitlines():
            if line.startswith("[") and line.rstrip().endswith(":"):
                cur = [line, []]
                blocks.append(cur)
            elif cur is not None and line.startswith("    "):
                cur[1].append(line)
        with_sudo = [b[0] for b in blocks if any("sudo" in l for l in b[1])]
        self.assertTrue(with_sudo)
        self.assertTrue(all("read-only production snapshot" in h for h in with_sudo), with_sudo)
        snap = next(b for b in blocks if b[0].startswith("[gw] read-only production snapshot"))
        self.assertTrue(any('for u in queqiaod; do observe "sudo -n"' in l for l in snap[1]))


def write_hostcpu(root, t, host, busy, prod=0.0, steal=0.0, every=2.0):
    """Samples over trial t's window: `busy` (a function of the interval's
    index, or a number) of one CPU, production taking `prod` of it, and the
    hypervisor stealing `steal` of it."""
    rows, user, idle, pt, st_ = [], 0.0, 0.0, 0.0, 0.0
    n = int((t.end - t.start) / every) + 2
    for i in range(n):
        when = t.start - 0.5 + i * every
        rows.append(f"{int(when * 1000)} {user:.0f} 0 0 {idle:.0f} 0 0 0 {st_:.0f} 0.5 {pt:.0f} 1")
        b = busy(i) if callable(busy) else busy
        user += 200 * (b - steal)
        st_ += 200 * steal
        idle += 200 * (1 - b)
        pt += 200 * prod
    os.makedirs(os.path.join(root, "hostcpu"), exist_ok=True)
    pathlib.Path(root, "hostcpu", f"{t.tag}-s{t.slot}-{t.arm}-{host}.tsv").write_text("\n".join(rows) + "\n")


class ContentionVoid(unittest.TestCase):
    def campaign(self, d, hot=(), prod_heavy=(), missing=(), stolen=(), cand_busy=0.5):
        root = os.path.join(d, "c")
        tm.unit_campaign(root, [(b, "a", "bulk") for b in range(1, 8)])
        plan = json.loads(pathlib.Path(root, "plan.json").read_text())
        plan["config"]["host_cpu"] = True
        plan["hostcpu_hosts"] = {"a": ["gw", "c1"]}
        plan["gateway_host"] = "gw"
        pathlib.Path(root, "plan.json").write_text(json.dumps(plan))
        _, trials = an.read_campaign(root)
        for t in trials.values():
            for host in ("gw", "c1"):
                if (t.block, host) in missing:
                    continue
                busy = 0.95 if (t.block, host) in hot else (cand_busy if t.arm == "cand" else 0.3)
                write_hostcpu(root, t, host, busy, 0.25 if (t.block, host) in prod_heavy else 0.05,
                              0.15 if (t.block, host) in stolen else 0.0)
        return root

    def gates(self):
        g = an.load_gates()
        g["resamples"] = 500
        return g

    def test_calm_hosts_void_nothing_and_cpu_is_reported_per_arm(self):
        with tempfile.TemporaryDirectory() as d:
            text, v = an.report(self.campaign(d), self.gates())
        self.assertNotIn("void under contention", text)
        self.assertEqual(v[("a", "cand", "rule:bulk.goodput_bps")], "pass")
        line = next(l for l in text.splitlines() if l.startswith("| gateway host CPU busy, mean"))
        self.assertEqual(line, "| gateway host CPU busy, mean | 30% | 50% |")      # main, cand
        self.assertIn("| gateway host production CPU share, mean | 5.0% | 5.0% |", text)

    def test_a_host_busy_with_the_test_is_reported_not_voided(self):
        # The test's own load saturating a 1-CPU host is not contention of
        # production: no block is void, and the busy share is reported.
        with tempfile.TemporaryDirectory() as d:
            text, v = an.report(self.campaign(d, hot={(3, "gw")}), self.gates())
        self.assertNotIn("void under contention", text)
        self.assertIn("| gateway host share of the window at or above 90% CPU, mean |", text)
        self.assertEqual(v[("a", "cand", "rule:bulk.goodput_bps")], "pass")

    def test_an_arm_busy_for_most_of_its_trials_is_flagged_cpu_bound(self):
        with tempfile.TemporaryDirectory() as d:
            text, _ = an.report(self.campaign(d, cand_busy=0.97), self.gates())
        self.assertIn("**CPU-bound:** `cand` on the gateway host (14 of 14 trials", text)
        self.assertNotIn("`main` on the", text)
        self.assertNotIn("void under contention", text)

    def test_hypervisor_steal_voids_the_block_which_is_not_evidence(self):
        with tempfile.TemporaryDirectory() as d:
            text, v = an.report(self.campaign(d, stolen={(3, "gw")}), self.gates())
        self.assertIn("1 block(s) void under contention", text)
        self.assertIn("block 3 on a bulk: 15% of gw's CPU stolen by the hypervisor", text)
        row = next(l for l in text.splitlines() if l.startswith("| bulk.goodput_bps | atleast"))
        self.assertIn("| cand | 6 |", row)                 # 7 blocks ran, 6 are evidence

    def test_production_taking_too_much_cpu_voids_the_block(self):
        with tempfile.TemporaryDirectory() as d:
            text, _ = an.report(self.campaign(d, prod_heavy={(5, "c1")}), self.gates())
        self.assertIn("block 5 on a bulk: production took 25% of c1's CPU", text)

    def test_no_cpu_record_is_not_evidence(self):
        with tempfile.TemporaryDirectory() as d:
            text, _ = an.report(self.campaign(d, missing={(4, "c1")}), self.gates())
        self.assertIn("block 4 on a bulk: no CPU record of c1", text)

    def test_the_bounds_are_registered(self):
        c = an.load_gates()["contention"]
        self.assertEqual(c, {"busy_threshold": 0.90, "production_share_max": 0.20, "steal_max_fraction": 0.10})


class Sampler(unittest.TestCase):
    def test_each_trial_runs_under_host_cpu_samplers(self):
        cfg = parse(BASE.replace("blocks = 12", "blocks = 12\nhost_cpu = true"))
        self.assertTrue(cfg.host_cpu)
        camp = ab.Campaign(cfg, "<o>", ".", dry=True, stream=io.StringIO())
        calls = []
        with mock.patch.object(ab.Campaign, "start_host_cpu", side_effect=lambda n, p: calls.append(("start", n)) or {}), \
                mock.patch.object(ab.Campaign, "stop_host_cpu", side_effect=lambda n, s: calls.append(("stop", n))), \
                mock.patch.object(ab.Campaign, "run_phase", side_effect=lambda *a: calls.append(("phase",))):
            camp.run_block(1, "a", [s for s in ab.schedule(cfg) if s.block == 1 and s.trial == "bulk"], None, "bulk")
        starts = [c for c in calls if c[0] == "start"]
        self.assertEqual(len(starts), 4)
        for i, c in enumerate(calls):
            if c[0] == "phase":
                self.assertEqual([x[0] for x in calls[:i] if x[0] != "phase"][-1], "start")
        self.assertEqual(camp.host_cpu_hosts("a"), ["gw", "c1"])

    @unittest.skipUnless(os.path.exists("/proc/stat"), "needs /proc/stat (Linux)")
    def test_the_sampler_reads_the_host_and_stops_on_its_file(self):
        d = tempfile.mkdtemp()
        stop = os.path.join(d, "stop")
        p = subprocess.Popen(["bash", "-s"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        p.stdin.write(ab.host_cpu_sampler(stop, []))
        p.stdin.close()
        time.sleep(4.5)
        pathlib.Path(stop).touch()
        out = p.stdout.read()
        p.wait(timeout=10)
        lines = [l.split() for l in out.splitlines() if l.startswith("S ")]
        self.assertGreaterEqual(len(lines), 2)
        self.assertTrue(all(len(l) == 13 for l in lines), lines)
        self.assertFalse(os.path.exists(stop))


if __name__ == "__main__":
    unittest.main()

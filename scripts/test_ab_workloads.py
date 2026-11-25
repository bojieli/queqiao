import contextlib
import datetime as dt
import importlib
import io
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).parent))
ab = importlib.import_module("ab_campaign")
an = importlib.import_module("ab_analyze")
wl = importlib.import_module("ab_workloads")

PLUGIN = textwrap.dedent('''
    import ab_stats as st
    import ab_workloads as w

    def script(campaign, job, arm, duration, start_at=0):
        return f"echo toy {job.params['echoes']} via {job.client.socks_port_base + arm.index}"

    w.generator("toy-echo", w.Generator(script, lambda out, job, d: [], lambda job: 0))

    @w.register
    class Toy(w.Workload):
        """A test workload: no byte outcome, an endpoint, one measure."""
        name = "toy"
        primary = False

        def configure(self, cfg, cp):
            cfg.toy_echoes = int(self.params(cp).get("echoes", "3"))

        def endpoints(self, cfg):
            return [w.Endpoint("ep-toy", "python3 {W}/toy_endpoint.py --port 23499", tcp_ports=[(23499, "echo")])]

        def phases(self, cfg, path):
            return [w.Phase("main", cfg.duration,
                            [w.Job(cfg.clients[path], "toy-echo", 1, params={"echoes": cfg.toy_echoes})])]

        def extract(self, trial, plan):
            rows = [r for r in trial.rows if r["kind"] == "toy-echo"]
            return {"main.echo_p99_s": st.quantile([float(r["seconds"]) for r in rows], 0.99)}
''')

CONFIG = """
[campaign]
name = toy
blocks = 2
workload = toy
duration = 10
[workload:toy]
echoes = 7
[gateway]
host = gw
address = 192.0.2.1
[client:a]
host = c1
[arm:main]
role = main
binary_linux_amd64 = /bin/true
[arm:cand]
binary_linux_amd64 = /bin/true
[production:gw]
host = gw
[production:c1]
host = c1
"""


class Plugins(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        pathlib.Path(cls.tmp.name, "toy.py").write_text(PLUGIN)
        wl.load_plugins(cls.tmp.name)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def config(self):
        p = pathlib.Path(self.tmp.name, "toy.local.ini")
        p.write_text(CONFIG)
        return ab.load_config(str(p))

    def test_builtin_workloads_are_registered(self):
        for name in ("bulk", "single", "interactive", "dc"):
            self.assertIn(name, wl.WORKLOADS)
        for name in ("bulk", "requests", "probe"):
            self.assertIn(name, wl.GENERATORS)

    def test_a_plugin_registers_and_reads_its_section(self):
        cfg = self.config()
        self.assertEqual(cfg.wl.name, "toy")
        self.assertEqual(cfg.toy_echoes, 7)

    def test_a_plugin_endpoint_is_reserved_and_started(self):
        cfg = self.config()
        self.assertIn((23499, "ep-toy echo"), cfg.planned_ports()["gw"])
        out = io.StringIO()
        forbidden = mock.Mock(side_effect=AssertionError("dry run must not run a command"))
        p = pathlib.Path(self.tmp.name, "toy.local.ini")
        with mock.patch.object(subprocess, "run", forbidden), contextlib.redirect_stdout(out):
            self.assertEqual(ab.main(["--config", str(p), "--dry-run"]), 0)
        text = out.getvalue()
        self.assertIn("23499 (ep-toy echo)", text)               # preflight checks its port
        self.assertIn("start ep-toy", text)                       # setup starts it
        self.assertIn("python3 ~/qq-ab-toy/toy_endpoint.py --port 23499", text)
        self.assertIn("main: toy-echo x1 via a/cand", text)       # its generator runs
        self.assertIn("echo toy 7 via 23582", text)

    def test_duplicate_names_are_refused(self):
        with self.assertRaises(ValueError):
            wl.register(type("Bulk2", (wl.Workload,), {"name": "bulk"}))
        with self.assertRaises(ValueError):
            wl.generator("bulk", wl.GENERATORS["bulk"])

    def write_campaign(self, root, cand_scale):
        os.makedirs(root)
        plan = {"main": "main", "workload": "toy", "paths": ["a"], "schedule": [], "config": {"duration": 10},
                "arms": {"main": {"role": "main", "flags": []}, "cand": {"role": "candidate", "flags": []}},
                "clients": {"a": {"small": True}}}
        cols = ab.TRIAL_COLUMNS
        rows = []
        t = 1_780_000_000.0
        for b in range(1, 13):
            for slot, arm in enumerate(["main", "cand", "cand", "main"], start=1):
                plan["schedule"].append({"block": b, "path": "a", "slot": slot, "arm": arm})
                for k in range(20):
                    secs = (0.05 + 0.002 * k) * (1 + 0.01 * b) * (cand_scale if arm == "cand" else 1)
                    iso = dt.datetime.fromtimestamp(t, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
                    rows.append([b, "a", slot, arm, "x", 1, 1, "main", 0, "a", "toy-echo", f"p{k}", iso, iso,
                                 200, 10, f"{secs:.4f}", 0, 1, 10, "", ""])
                t += 20
        pathlib.Path(root, "plan.json").write_text(json.dumps(plan))
        pathlib.Path(root, "status.json").write_text(json.dumps({"status": "complete", "production": "unchanged"}))
        with open(os.path.join(root, "trials.tsv"), "w") as f:
            f.write("\t".join(cols) + "\n")
            for r in rows:
                f.write("\t".join(str(x) for x in r) + "\n")

    def gates(self):
        g = an.load_gates()
        g["resamples"] = 2000
        g["workloads"] = {"toy": {"main.echo_p99_s": ("atmost", 1.10)}}
        return g

    def test_rules_judge_a_plugins_measures(self):
        root = os.path.join(self.tmp.name, "eq")
        self.write_campaign(root, 1.0)
        text, v = an.report(root, self.gates())
        self.assertEqual(v[("a", "cand", "rule:main.echo_p99_s")], "pass")
        self.assertEqual(v[("a", "cand", "overall")], "PASS")
        self.assertNotIn("Primary gate", text)  # no byte outcome to judge
        root = os.path.join(self.tmp.name, "slow")
        self.write_campaign(root, 1.5)
        _, v = an.report(root, self.gates())
        self.assertEqual(v[("a", "cand", "rule:main.echo_p99_s")], "**FAIL**")
        self.assertEqual(v[("a", "cand", "overall")], "FAIL")

    def test_a_workload_with_no_rules_is_not_passed(self):
        root = os.path.join(self.tmp.name, "norules")
        self.write_campaign(root, 1.0)
        g = self.gates()
        g["workloads"] = {}
        _, v = an.report(root, g)
        self.assertEqual(v[("a", "cand", "overall")], "INCOMPLETE")


class Rules(unittest.TestCase):
    def test_rule_contrasts(self):
        self.assertEqual(an.rule_contrasts("atleast", 0.9, [90, 100], [100, 100]), [0.0, 10.0])
        self.assertAlmostEqual(an.rule_contrasts("atmost", 1.1, [0.11], [0.1])[0], 0.0)
        self.assertAlmostEqual(an.rule_contrasts("higher", 0.02, [0.97], [1.0])[0], -0.01)
        self.assertAlmostEqual(an.rule_contrasts("lower", 0.005, [0.004], [0.0])[0], 0.001)

    def test_gates_rules_are_validated(self):
        with tempfile.TemporaryDirectory() as d:
            p = pathlib.Path(d, "g.ini")
            p.write_text(pathlib.Path(an.GATES).read_text() + "\n[workload:x]\nmain.a = sideways 1\n")
            with self.assertRaises(SystemExit):
                an.load_gates(str(p))
            p.write_text(pathlib.Path(an.GATES).read_text() + "\n[workload:x]\nmain.a = atmost 1.10\n")
            self.assertEqual(an.load_gates(str(p))["workloads"]["x"], {"main.a": ("atmost", 1.10)})


if __name__ == "__main__":
    unittest.main()

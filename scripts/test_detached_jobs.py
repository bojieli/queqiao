"""Detached jobs that survive ssh drops, and control-plane voids."""
import importlib
import io
import json
import os
import pathlib
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
tac = importlib.import_module("test_ab_campaign")

LOCAL = tac.BASE.replace("host = gw", "host = local").replace("host = c1", "host = local") \
    .replace("[client:a]\nhost = local", "[client:a]\nhost = local\nsocks_port_base = 24681\nmetrics_port_base = 24691")


class RunJob(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.camp = ab.Campaign(tac.parse(LOCAL), os.path.join(self.tmp.name, "out"), ".", dry=False, stream=io.StringIO())
        self.camp.hosts["local"].home = os.path.realpath(self.tmp.name)
        os.makedirs(os.path.join(self.camp.w("local"), "run"))
        self.camp.JOB_POLL = 0.1

    def test_a_detached_job_returns_its_output_and_status(self):
        rc, out = self.camp.run_job("local", "echo hello; echo world; exit 3", 30, "test")
        self.assertEqual((rc, out.split()), (3, ["hello", "world"]))
        # Its files are cleaned up (the descriptor too, where ownership can be
        # verified: on a host that cannot, it stays for teardown).
        left = {p.suffix for p in pathlib.Path(self.camp.w("local"), "run").glob("job-*")}
        self.assertTrue(left <= {".pgid"}, left)

    def test_the_job_runs_on_when_its_launching_session_ends(self):
        # Launch returns at once; the job (a second of work) keeps running.
        h = self.camp.hosts["local"]
        launch, files = self.camp.as_job("local", "sleep 1; echo done")
        t = time.monotonic()
        rc, out = h.run(launch)
        self.assertEqual(rc, 0)
        self.assertLess(time.monotonic() - t, 0.9)
        self.assertFalse(os.path.exists(files["rc"]))
        for _ in range(50):
            if os.path.exists(files["rc"]):
                break
            time.sleep(0.1)
        self.assertEqual(pathlib.Path(files["out"]).read_text().strip(), "done")

    def test_ssh_drops_are_retried_and_cost_nothing(self):
        # Two dropped sessions while polling, then the job's result.
        real = ab.Host.run
        drops = {"n": 0}

        def flaky(self_, script, timeout=120, what=""):
            if "QQ_AB_RUNNING" in script and drops["n"] < 2:
                drops["n"] += 1
                return 255, "ssh: connection reset"
            return real(self_, script, timeout, what)
        with mock.patch.object(ab.Host, "run", flaky), mock.patch.object(ab.time, "sleep"):
            rc, out = self.camp.run_job("local", "echo survived", 30, "test")
        self.assertEqual((rc, out.strip()), (0, "survived"))
        self.assertIn("ssh session failed", self.camp.stream.getvalue())

    def test_a_host_unreachable_through_every_retry_is_a_control_plane_failure(self):
        real = ab.Host.run

        def down(self_, script, timeout=120, what=""):
            if "QQ_AB_RUNNING" in script:
                return 255, "ssh: connect: no route to host"
            return real(self_, script, timeout, what)
        with mock.patch.object(ab.Host, "run", down), mock.patch.object(ab.time, "sleep"):
            rc, out = self.camp.run_job("local", "sleep 0.2", 30, "test")
        self.assertEqual(rc, 255)
        self.assertEqual(self.camp.stream.getvalue().count("ssh session failed"), ab.Campaign.JOB_SSH_TRIES)

    def test_a_killed_job_is_lost_not_waited_for(self):
        import threading
        res = {}
        th = threading.Thread(target=lambda: res.update(r=self.camp.run_job("local", "sleep 30", 60, "test")))
        th.start()
        for _ in range(100):
            d = list(pathlib.Path(self.camp.w("local"), "run").glob("job-*.pgid"))
            if d and d[0].read_text().strip():
                break
            time.sleep(0.05)
        os.kill(int(d[0].read_text().split()[0]), 9)      # the job's wrapper, killed
        th.join(15)
        self.assertEqual(res.get("r", (None,))[0], -3)
        import subprocess
        subprocess.run(["pkill", "-f", f"{self.camp.w('local')}/run/job-"], capture_output=True)

    def test_a_job_that_left_no_record_is_a_control_plane_failure(self):
        slot = ab.Slot(1, "a", 1, "main", 1, True, "bulk")
        job = mock.Mock(client=mock.Mock(name_="a"), kind="bulk", n=1)
        job.client.name = "a"
        gen = mock.Mock(expected=lambda j: 0, records=None, parse=lambda out, j, d: [])
        self.camp.record_job(slot, self.camp.cfg.arms[0], mock.Mock(name="bulk"), 0, job, gen,
                             (0.0, 1.0, 0, "   \n"), lambda row: None, None, 60)
        self.assertEqual([(j["failure"], j["exit"]) for j in self.camp.job_failures], [("control-plane", -2)])


class ControlPlaneVoid(unittest.TestCase):
    def campaign(self, d, failed_blocks=(), partial=()):
        root = os.path.join(d, "c")
        tm.unit_campaign(root, [(b, "a", "bulk") for b in range(1, 8)])
        status = json.loads(pathlib.Path(root, "status.json").read_text())
        status["job_failures"] = [{"failure": "control-plane", "kind": "bulk", "block": b, "path": "a", "trial": "bulk", "slot": 2,
                                   "arm": "cand", "phase": "bulk", "client": "a", "exit": 255} for b in failed_blocks]
        pathlib.Path(root, "status.json").write_text(json.dumps(status))
        if partial:
            # A transfer cut inside the tunnel (curl exit 18): the arm's evidence.
            tsv = pathlib.Path(root, "trials.tsv").read_text().splitlines()
            out = [tsv[0]]
            for line in tsv[1:]:
                f = line.split("\t")
                if (int(f[0]), f[3]) in partial:
                    f[17] = "18"
                out.append("\t".join(f))
            pathlib.Path(root, "trials.tsv").write_text("\n".join(out) + "\n")
        return root

    def gates(self):
        g = an.load_gates()
        g["resamples"] = 500
        return g

    def test_a_lost_job_voids_its_block_not_the_campaign(self):
        with tempfile.TemporaryDirectory() as d:
            text, v = an.report(self.campaign(d, failed_blocks=(3,)), self.gates())
        self.assertNotIn("No verdict: the campaign is incomplete", text)
        self.assertIn("block 3 on a bulk: control plane", text)
        row = next(l for l in text.splitlines() if l.startswith("| bulk.goodput_bps | atleast"))
        self.assertIn("| cand | 6 |", row)                 # 7 blocks ran, 6 are evidence
        self.assertEqual(v[("a", "cand", "rule:bulk.goodput_bps")], "pass")

    def test_more_than_two_lost_blocks_leave_the_type_without_a_verdict(self):
        with tempfile.TemporaryDirectory() as d:
            text, v = an.report(self.campaign(d, failed_blocks=(2, 4, 6)), self.gates())
        self.assertIn("3 block(s) of bulk on a lost a job to the control plane, more than the 2 allowed", text)
        self.assertEqual(v[("a", "cand", "overall")], "INCOMPLETE")

    def test_a_workload_failure_is_evidence_and_reported(self):
        with tempfile.TemporaryDirectory() as d:
            text, v = an.report(self.campaign(d, partial={(5, "cand")}), self.gates())
        self.assertNotIn("block(s) void", text)
        self.assertIn("Workload failures (recorded by the jobs; evidence against the arm): block 5 on a bulk: cand 2", text)
        self.assertEqual(an.load_gates()["harness"], {"control_plane_void_max": 2})


if __name__ == "__main__":
    unittest.main()

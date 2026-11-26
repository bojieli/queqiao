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

    def test_a_job_past_its_window_is_stopped_and_is_a_timeout(self):
        # The generator hangs (a stalled tunnel, a hung client): at its
        # deadline the runner ends the job's processes and reports a timeout.
        t = time.monotonic()
        rc, out = self.camp.run_job("local", "echo started; sleep 30; echo never", 1.5, "hanging job")
        self.assertEqual(rc, ab.JOB_TIMEOUT)
        self.assertLess(time.monotonic() - t, 20)
        # What it had is kept where the runner could stop it (Linux); where
        # ownership cannot be established nothing is signalled and the
        # timeout carries no output.
        self.assertIn(out.split(), ([], ["started"]))
        import subprocess
        subprocess.run(["pkill", "-f", f"{self.camp.w('local')}/run/job-"], capture_output=True)

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

    def record(self, rc, out="", kind="wl-llm"):
        import ab_workloads as w
        w.load_plugins()
        slot = ab.Slot(1, "a", 2, "cand", 1, True, "mixed-67")
        job = w.Job(self.camp.cfg.clients["a"], kind, 1, params={})
        recs = os.path.join(self.tmp.name, "measures.jsonl")
        rows = []
        self.camp.record_job(slot, self.camp.cfg.arms[1], w.Phase("mixed-67", 90, []), 0, job, w.GENERATORS[kind],
                             (100.0, 190.0, rc, out), rows.append, recs, 90)
        written = []
        if os.path.exists(recs):
            with open(recs) as f:
                written = [json.loads(l) for l in f]
        return rows, written

    def test_failures_on_a_reachable_host_are_the_arms_and_never_void(self):
        for rc, why in ((ab.JOB_TIMEOUT, "timeout"), (ab.JOB_NO_RECORD, "no record"), (ab.JOB_LOST, "killed")):
            self.camp.job_failures.clear()
            self.camp.workload_failures.clear()
            rows, recs = self.record(rc)
            self.assertEqual(self.camp.job_failures, [])                       # not the control plane
            self.assertEqual([f["reason"] for f in self.camp.workload_failures], [why])
            # The measuring job's arm gets a failed session it can be judged on.
            self.assertEqual([(r["kind"], r["fail"], r["_arm"]) for r in recs][-1], ("llm", why, "cand"))
        rows, recs = self.record(0, "   \n")
        self.assertEqual(self.camp.workload_failures[-1]["reason"], "no record")

    def test_a_bulk_job_past_its_window_is_failed_transfers_for_its_arm(self):
        rows, _ = self.record(ab.JOB_TIMEOUT, "", kind="bulk")
        self.assertEqual(self.camp.job_failures, [])
        self.assertTrue(rows)
        # Its flows are rows that failed (0 bytes, not complete), for the
        # candidate: in the denominator, against the arm.
        self.assertEqual({(r[3], r[15], r[18]) for r in rows}, {("cand", "0", 0)})

    def test_only_an_unreachable_host_or_the_runners_own_kill_is_the_control_plane(self):
        for rc in (ab.JOB_UNREACHABLE, ab.JOB_KILLED_BY_RUNNER):
            self.camp.job_failures.clear()
            self.record(rc)
            self.assertEqual([(j["failure"], j["exit"], j["arm"]) for j in self.camp.job_failures],
                             [("control-plane", rc, "cand")])


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

    def test_an_exit_that_is_not_the_control_plane_never_voids(self):
        # Entries a runner recorded for a job past its window (-1), silent
        # (-2) or lost (-3) on a reachable host are not void.
        with tempfile.TemporaryDirectory() as d:
            root = self.campaign(d, failed_blocks=(3,))
            status = json.loads(pathlib.Path(root, "status.json").read_text())
            for j, rc in zip(status["job_failures"], (-1,)):
                j["exit"] = rc
            pathlib.Path(root, "status.json").write_text(json.dumps(status))
            text, v = an.report(root, self.gates())
        self.assertNotIn("block(s) void", text)
        self.assertNotIn("No verdict", text)

    def test_a_candidate_hanging_past_its_window_fails_the_candidate_not_the_block(self):
        # The runner recorded the candidate's LLM job as a timeout (and wrote
        # it a failed session); nothing was lost to the control plane.
        with tempfile.TemporaryDirectory() as d:
            root = self.campaign(d)
            status = json.loads(pathlib.Path(root, "status.json").read_text())
            status["workload_failures"] = [{"block": 3, "path": "a", "trial": "bulk", "slot": 2, "arm": "cand",
                                            "phase": "bulk", "client": "a", "kind": "wl-llm", "reason": "timeout"}]
            pathlib.Path(root, "status.json").write_text(json.dumps(status))
            text, v = an.report(root, self.gates())
        self.assertNotIn("block(s) void", text)
        row = next(l for l in text.splitlines() if l.startswith("| bulk.goodput_bps | atleast"))
        self.assertIn("| cand | 7 |", row)                 # every block is evidence
        pa = sys.modules["ab_workload_phase_a"]
        ok = {"kind": "llm", "fail": "", "ttft": 0.6, "T": 0.5, "deltas": [[0, 0.6], [25, 0.625]], "done": 1.0,
              "N": 2, "c": 0.025, "U": 100000}
        self.assertEqual(pa.llm_metrics([ok, {"kind": "llm", "fail": "timeout"}])["call_failures"], 0.5)

    def test_a_workload_failure_is_evidence_and_reported(self):
        with tempfile.TemporaryDirectory() as d:
            text, v = an.report(self.campaign(d, partial={(5, "cand")}), self.gates())
        self.assertNotIn("block(s) void", text)
        self.assertIn("Workload failures (recorded by the jobs; evidence against the arm): block 5 on a bulk: cand 2", text)
        self.assertEqual(an.load_gates()["harness"], {"control_plane_void_max": 2})


if __name__ == "__main__":
    unittest.main()


class SshReuse(unittest.TestCase):
    def test_every_session_to_a_host_shares_one_connection(self):
        h = ab.Host("gw", ["-o", "ConnectTimeout=20"], False, control_dir="/tmp/qqab-ssh-x")
        argv = h._argv()
        self.assertIn("ControlMaster=auto", argv)
        self.assertIn("ControlPath=/tmp/qqab-ssh-x/%C", argv)
        with mock.patch.object(ab.subprocess, "run") as run:
            h.close()
        self.assertEqual(run.call_args[0][0][-3:], ["-O", "exit", "gw"])
        self.assertEqual(ab.Host("local", [], False, control_dir="/tmp/x")._argv(), ["bash", "-s"])

    def test_the_socket_directory_is_the_campaigns_own(self):
        with tempfile.TemporaryDirectory(dir="/tmp") as d:
            short = os.path.join(d, "o")
            self.assertEqual(ab.ssh_control_dir(short), os.path.join(os.path.abspath(short), "ssh"))
            long_ = os.path.join(d, "x" * 80)
            self.assertTrue(ab.ssh_control_dir(long_).startswith("/tmp/qqab-ssh-"))

    def test_parallel_runs_together_and_reraises(self):
        t = time.monotonic()
        ab.parallel(lambda: time.sleep(0.3), lambda: time.sleep(0.3), lambda: time.sleep(0.3))
        self.assertLess(time.monotonic() - t, 0.8)
        with self.assertRaises(ValueError):
            ab.parallel(lambda: None, lambda: (_ for _ in ()).throw(ValueError("x")))


class PatientTeardown(unittest.TestCase):
    def test_a_session_that_fails_to_connect_is_retried_while_tearing_down(self):
        h = ab.Host("gw", [], False, out=io.StringIO())
        results = [255, 255, 0]
        calls = []

        def fake(argv, **kw):
            calls.append(argv)
            return mock.Mock(returncode=results[len(calls) - 1], stdout="ok", stderr="")
        with mock.patch.object(ab.subprocess, "run", side_effect=fake), mock.patch.object(ab.time, "sleep"):
            self.assertEqual(h.run("true")[0], 255)          # by default: one session
            calls.clear()
            h.retry_255 = 5
            self.assertEqual(h.run("true")[0], 0)            # tearing down: retried until it connects
        self.assertEqual(len(calls), 3)

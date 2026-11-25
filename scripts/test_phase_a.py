import contextlib
import importlib
import io
import json
import os
import pathlib
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))
ab = importlib.import_module("ab_campaign")
an = importlib.import_module("ab_analyze")
wl = importlib.import_module("ab_workloads")
wl.load_plugins()
pa = sys.modules["ab_workload_phase_a"]

SERVER = HERE / "validation" / "wlserver.py"
CLIENT = HERE / "validation" / "wlclient.py"


def call(T=0.5, N=4, c=0.025, ttft=0.6, deltas=None, fail="", R=0.2, done=None, connect=None):
    deltas = deltas if deltas is not None else [[1000.0 + 25 * i, 0.6 + 0.025 * i] for i in range(N)]
    rec = {"kind": "llm", "T": T, "N": N, "c": c, "ttft": ttft, "deltas": deltas, "fail": fail, "R": R,
           "done": done if done is not None else ttft + c * N}
    if connect is not None:
        rec["connect"] = connect
    return rec


class LLMMetrics(unittest.TestCase):
    def test_lateness_is_clock_free(self):
        # Server clock 1000 s ahead of the client's: the offset cancels.
        d = [[1_000_000.0, 0.10], [1_000_025.0, 0.125], [1_000_050.0, 0.40], [1_000_075.0, 0.175]]
        self.assertEqual([round(x, 6) for x in pa.lateness(d)], [0.0, 0.0, 0.25, 0.0])

    def test_gaps_stalls_and_freezes(self):
        # 400 deltas at 25 ms, except one 0.6 s gap and one 2.5 s gap.
        r, out = 0.0, []
        for i in range(400):
            r += 0.6 if i == 100 else 2.5 if i == 300 else 0.025
            out.append([25.0 * i, r])
        m = pa.llm_metrics([call(N=400, deltas=out)])
        minutes = (out[-1][1] - out[0][1]) / 60
        self.assertAlmostEqual(m["stalls_per_min"], 2 / minutes)     # both are >= 0.5 s
        self.assertAlmostEqual(m["freezes_per_min"], 1 / minutes)    # one is >= 2 s
        self.assertAlmostEqual(m["gap_max_s"], 2.5)
        self.assertAlmostEqual(m["stall_time_ratio"], ((0.6 - 0.25) + (2.5 - 0.25)) / (out[-1][1] - out[0][1]))

    def test_ttft_net_failures_and_r(self):
        calls = [call(ttft=0.6 + 0.1 * i, R=0.1 * i, connect=0.3) for i in range(5)] + [call(fail="EOFError")]
        m = pa.llm_metrics(calls)
        self.assertAlmostEqual(m["ttft_net_p50_s"], 0.3)             # 0.1..0.5 net of T = 0.5
        self.assertAlmostEqual(m["call_failures"], 1 / 6)
        self.assertAlmostEqual(m["upload_R_p50_s"], 0.2)
        self.assertAlmostEqual(m["connect_p50_s"], 0.3)

    def test_completion_excess(self):
        m = pa.llm_metrics([call(T=0.5, N=4, c=0.025, done=0.7)])
        self.assertAlmostEqual(m["completion_excess_p50_s"], 0.1)


class TermMetrics(unittest.TestCase):
    def session(self, a_ms, c_ms, bursts=(40.0,)):
        keys = [[1 + i * 0.15, a_ms / 1000] for i in range(200)]                   # phase A, before 40 s
        keys += [[42.5 + i * 0.15, c_ms / 1000] for i in range(100)]               # phase C, after the excluded 2 s
        keys += [[40.5, 1.5]]                                                      # within 2 s of the burst: excluded
        return {"kind": "term", "keys": keys, "bursts": list(bursts), "fail": ""}

    def test_phases_and_latch_ratio(self):
        m = pa.term_metrics([self.session(30, 60)])
        self.assertAlmostEqual(m["echo_p99_a_s"], 0.030)
        self.assertAlmostEqual(m["echo_p99_c_s"], 0.060)
        self.assertAlmostEqual(m["latch_ratio"], 2.0)
        self.assertEqual(m["over1s_c"], 0.0)                                       # the 1.5 s echo was excluded

    def test_over_one_second(self):
        s = self.session(30, 30)
        s["keys"][:10] = [[1 + i * 0.15, 1.2] for i in range(10)]
        self.assertAlmostEqual(pa.term_metrics([s])["over1s_a"], 10 / 200)

    def test_canary_has_phase_a_only(self):
        s = self.session(30, 30, bursts=())
        m = pa.term_metrics([s])
        self.assertIn("echo_p99_a_s", m)
        self.assertNotIn("latch_ratio", m)


class Coverage(unittest.TestCase):
    def rows(self, spans):
        return [{"t_start": f"{a:.3f}", "t_end": f"{b:.3f}"} for a, b in spans]

    def test_a_background_must_span_the_foreground(self):
        self.assertTrue(pa.covered((10, 60), self.rows([(5, 30), (30.2, 70)])))   # back-to-back objects
        self.assertFalse(pa.covered((10, 60), self.rows([(5, 20)])))              # died early
        self.assertFalse(pa.covered((10, 60), []))


CONFIG = """
[campaign]
name = pa
blocks = 2
workload = {workload}
[workload:{workload}]
{extra}
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


class Plans(unittest.TestCase):
    def dry(self, workload, extra=""):
        with tempfile.TemporaryDirectory() as d:
            p = pathlib.Path(d, "x.local.ini")
            p.write_text(CONFIG.format(workload=workload, extra=extra))
            out = io.StringIO()
            forbidden = mock.Mock(side_effect=AssertionError("dry run must not run a command"))
            with mock.patch.object(subprocess, "run", forbidden), contextlib.redirect_stdout(out):
                self.assertEqual(ab.main(["--config", str(p), "--dry-run"]), 0)
            return out.getvalue()

    def test_llm_plan(self):
        text = self.dry("llm")
        for cell in ("single-100k-none", "single-400k-down4", "loop-100k-up1", "loop-400k-none"):
            self.assertIn(f"[c1] {cell}: wl-llm x1 via a/cand", text)
        self.assertIn("23485 (wlserver http)", text)                     # the endpoint's ports are reserved
        self.assertIn("start wlserver", text)
        self.assertIn("python3 ~/qq-ab-pa/wlserver.py --http 23485 --term 23486 --dir ~/qq-ab-pa", text)
        self.assertIn("copy " + str(CLIENT) + " -> ~/qq-ab-pa/wlclient.py", text)
        self.assertIn("--sequence loop --upload 400000", text)
        self.assertIn("wlclient.py bgdown --socks 127.0.0.1:23582 --target 127.0.0.1:23480", text)
        self.assertIn("wlclient.py bgup", text)
        self.assertRegex(text, r"wlclient.py bgdown .*--stop-file ~/qq-ab-pa/run/stop-probe-a-cand")

    def test_swarm_and_no_tls(self):
        text = self.dry("llm", "sequences = swarm\nuploads_kb = 100\nbackgrounds = none\ntls = false")
        self.assertIn("swarm-100k-none", text)
        self.assertIn("--sequence swarm", text)
        self.assertIn("--no-tls", text)

    def test_term_plan(self):
        text = self.dry("term")
        for cell in ("term-none", "term-down4", "term-up1"):
            self.assertIn(f"[c1] {cell}: wl-term x1 via a/main", text)
        self.assertIn("--burst-at 40.0 --burst-every 20.0 --burst-bytes 300000", text)

    def test_mixed_plan(self):
        text = self.dry("mixed")
        for cell in ("m1-100k", "m1-400k", "m2-100k", "m2-400k"):
            self.assertIn(f"[c1] {cell}: wl-llm x1 via a/cand", text)
            self.assertIn(f"[c1] {cell}: wl-term x1 via a/cand", text)
        self.assertIn("--canary", text)

    def test_bad_values_are_rejected(self):
        for wlname, extra, frag in (("llm", "sequences = burst", "sequence 'burst'"),
                                    ("llm", "backgrounds = up9", "background 'up9'"),
                                    ("mixed", "scenarios = m3", "scenario 'm3'")):
            with tempfile.TemporaryDirectory() as d:
                p = pathlib.Path(d, "x.local.ini")
                p.write_text(CONFIG.format(workload=wlname, extra=extra))
                with self.assertRaises(ab.ConfigError) as cm:
                    ab.load_config(str(p))
                self.assertIn(frag, str(cm.exception))


def write_llm_campaign(root, cand_late=0.0, bg_dies=False, blocks=12, seed=5):
    """An llm campaign with two cells (single-100k-none, single-100k-down4).
    Every call streams 40 deltas at 25 ms; the candidate's deltas are
    `cand_late` s later in their second half. With bg_dies, the down4
    background of every trial stops 2 s into a 10 s foreground."""
    import random
    rng = random.Random(seed)
    os.makedirs(root)
    plan = {"main": "main", "workload": "llm", "paths": ["a"], "schedule": [], "config": {"duration": 90},
            "arms": {"main": {"role": "main", "flags": []}, "cand": {"role": "candidate", "flags": []}},
            "clients": {"a": {"small": True}}}
    rows, recs = [], []
    t = 1_780_000_000.0
    iso = lambda x: __import__("datetime").datetime.fromtimestamp(x, __import__("datetime").timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    for b in range(1, blocks + 1):
        for slot, arm in enumerate(["main", "cand", "cand", "main"], start=1):
            plan["schedule"].append({"block": b, "path": "a", "slot": slot, "arm": arm})
            for ph in ("single-100k-none", "single-100k-down4"):
                base = [b, "a", slot, arm, "x", 1, 1, ph]
                if ph.endswith("down4"):
                    end = t + (2 if bg_dies else 14)
                    for i in range(1, 5):
                        rows.append(base + [0, "a", "wl-bgdown", f"b{i}o1", iso(t), iso(end), 200, 10_000_000,
                                            end - t, 28, 0, end - t, f"{t:.3f}", f"{end:.3f}"])
                for k in range(5):
                    c0 = t + 1 + k * 2.0
                    jitter = rng.gauss(0, 0.002)
                    deltas = [[100000.0 + 25 * i, 0.6 + 0.025 * i + jitter
                               + (cand_late if arm == "cand" and i >= 20 else 0.0)] for i in range(40)]
                    recs.append({"kind": "llm", "seq": "single", "agent": 0, "turn": k, "U": 100000, "t0": c0,
                                 "T": 0.5, "N": 40, "c": 0.025, "fail": "", "connect": 0.2, "cold": True,
                                 "ttft": 0.6 + jitter, "R": 0.1, "bytes": 8000, "done": 0.6 + 1.0 + jitter,
                                 "deltas": deltas, "_block": b, "_path": "a", "_slot": slot, "_arm": arm, "_phase": ph,
                                 "_client": "a", "_job": 1})
                    rows.append(base + [1, "a", "wl-llm", f"c0t{k}", iso(c0), iso(c0 + 1.6), 200, 8000, 1.6, 0, 1, 1.6,
                                        f"{c0:.3f}", f"{c0 + 1.6:.3f}"])
                t += 15
    pathlib.Path(root, "plan.json").write_text(json.dumps(plan))
    pathlib.Path(root, "status.json").write_text(json.dumps({"status": "complete", "production": "unchanged"}))
    with open(os.path.join(root, "trials.tsv"), "w") as f:
        f.write("\t".join(ab.TRIAL_COLUMNS) + "\n")
        for r in rows:
            f.write("\t".join(str(x) for x in r) + "\n")
    with open(os.path.join(root, "measures.jsonl"), "w") as f:
        for r in recs:
            f.write(json.dumps(r) + "\n")


class Report(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.gates = an.load_gates()
        self.gates["resamples"] = 1000

    def tearDown(self):
        self.tmp.cleanup()

    def without_class_rule(self):
        """The gates as if class correctness were not required: its rule
        removed and the measure declared report-only (dropping the rule
        alone leaves an unruled measure, which is INCOMPLETE)."""
        g = dict(self.gates)
        g["workloads"] = dict(g["workloads"])
        g["workloads"]["llm"] = {k: v for k, v in g["workloads"]["llm"].items() if k != "*.class_interactive"}
        llm = wl.WORKLOADS["llm"]
        patch = mock.patch.object(llm, "report_only", llm.report_only + ("class_interactive",))
        patch.start()
        self.addCleanup(patch.stop)
        return g

    def test_gates_file_has_the_phase_a_rules(self):
        r = self.gates["workloads"]
        self.assertEqual(r["llm"]["*.ttft_net_p95_s"], ("atmost", (1.15, 0.05)))
        self.assertEqual(r["llm"]["*.class_interactive"], ("floor", (0.99,)))
        self.assertEqual(r["term"]["*.latch_ratio"], ("ceiling", (1.15,)))
        self.assertEqual(r["mixed"]["*.bg_bytes"], ("atleast", (0.90,)))

    def test_every_cell_is_judged_and_class_telemetry_is_required(self):
        root = os.path.join(self.tmp.name, "eq")
        write_llm_campaign(root)
        text, v = an.report(root, self.gates)
        for cell in ("single-100k-none", "single-100k-down4"):
            self.assertEqual(v[("a", "cand", f"rule:{cell}.lateness_p99_s")], "pass")
            self.assertEqual(v[("a", "cand", f"rule:{cell}.ttft_net_p50_s")], "pass")
            self.assertEqual(v[("a", "cand", f"rule:{cell}.class_interactive")], "n/a")
        # Without per-byte class telemetry the workload cannot pass.
        self.assertEqual(v[("a", "cand", "overall")], "INCOMPLETE")
        _, v = an.report(root, self.without_class_rule())
        self.assertEqual(v[("a", "cand", "overall")], "PASS")

    def test_late_deltas_fail(self):
        root = os.path.join(self.tmp.name, "late")
        write_llm_campaign(root, cand_late=0.15)
        _, v = an.report(root, self.without_class_rule())
        self.assertEqual(v[("a", "cand", "rule:single-100k-none.lateness_p99_s")], "**FAIL**")
        self.assertEqual(v[("a", "cand", "overall")], "FAIL")

    def test_a_background_that_died_leaves_its_cell_unmeasured(self):
        root = os.path.join(self.tmp.name, "bgdied")
        write_llm_campaign(root, bg_dies=True)
        _, v = an.report(root, self.without_class_rule())
        self.assertEqual(v[("a", "cand", "rule:single-100k-down4.lateness_p99_s")], "n/a")
        self.assertEqual(v[("a", "cand", "rule:single-100k-none.lateness_p99_s")], "pass")
        self.assertEqual(v[("a", "cand", "overall")], "INCOMPLETE")


class Flip(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.gates = an.load_gates()
        self.gates["resamples"] = 1000
        self.gates["workloads"] = dict(self.gates["workloads"])
        self.gates["workloads"]["llm"] = {k: v for k, v in self.gates["workloads"]["llm"].items()
                                          if k != "*.class_interactive"}
        llm = wl.WORKLOADS["llm"]
        p = mock.patch.object(llm, "report_only", llm.report_only + ("class_interactive",))
        p.start()
        self.addCleanup(p.stop)
        self.addCleanup(self.tmp.cleanup)

    def test_every_cell_must_pass_and_every_workload_be_there(self):
        good = os.path.join(self.tmp.name, "good")
        write_llm_campaign(good)
        text, ok = an.flip_report([good], self.gates)
        self.assertFalse(ok)                                   # bulk, term, ... are missing
        self.assertIn("Missing workloads", text)
        with mock.patch.object(an, "FLIP_WORKLOADS", ("llm",)):
            text, ok = an.flip_report([good], self.gates)
            self.assertTrue(ok, text)
            self.assertIn("| llm | a | cand | single-100k-down4 | PASS |", text)
            late = os.path.join(self.tmp.name, "late")
            write_llm_campaign(late, cand_late=0.15)
            text, ok = an.flip_report([late], self.gates)
            self.assertFalse(ok)
            self.assertIn("| llm | a | cand | single-100k-none | **FAIL** |", text)
            with contextlib.redirect_stdout(io.StringIO()), mock.patch.object(an, "load_gates", return_value=self.gates):
                self.assertEqual(an.main([late, "--flip"]), 1)
                self.assertEqual(an.main([good, "--flip"]), 0)


# --- end to end on this machine: wlserver, a tiny SOCKS5 proxy, wlclient ----------------

class Socks5(threading.Thread):
    """CONNECT-only SOCKS5 relay for the tests, on loopback."""

    def __init__(self):
        super().__init__(daemon=True)
        self.srv = socket.socket()
        self.srv.bind(("127.0.0.1", 0))
        self.srv.listen(64)
        self.port = self.srv.getsockname()[1]

    def run(self):
        while True:
            try:
                c, _ = self.srv.accept()
            except OSError:
                return
            threading.Thread(target=self.serve, args=(c,), daemon=True).start()

    def serve(self, c):
        try:
            c.recv(3)
            c.sendall(b"\x05\x00")
            head = c.recv(4)
            if head[3] == 3:
                n = c.recv(1)[0]
                host = c.recv(n).decode()
            else:
                host = socket.inet_ntoa(c.recv(4))
            port = struct.unpack(">H", c.recv(2))[0]
            u = socket.create_connection((host, port))
            c.sendall(b"\x05\x00\x00\x01" + b"\x00" * 6)
        except OSError:
            c.close()
            return

        def pump(a, b):
            try:
                while True:
                    x = a.recv(65536)
                    if not x:
                        break
                    b.sendall(x)
            except OSError:
                pass
            for s in (a, b):
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
        threading.Thread(target=pump, args=(c, u), daemon=True).start()
        pump(u, c)


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


class EndToEnd(unittest.TestCase):
    """The real server and client, through a SOCKS5 relay, on loopback."""

    def start(self, tls):
        self.tmp = tempfile.TemporaryDirectory()
        self.http, self.term = free_port(), free_port()
        args = [sys.executable, str(SERVER), "--http", str(self.http), "--term", str(self.term), "--dir", self.tmp.name]
        if not tls:
            args.append("--no-tls")
        self.server = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(100):
            try:
                socket.create_connection(("127.0.0.1", self.term), timeout=0.2).close()
                break
            except OSError:
                time.sleep(0.05)
        self.socks = Socks5()
        self.socks.start()

    def tearDown(self):
        self.server.kill()
        self.server.wait()
        self.socks.srv.close()
        self.tmp.cleanup()

    def client(self, *args, tls=True, timeout=60):
        cmd = [sys.executable, str(CLIENT), args[0], "--socks", f"127.0.0.1:{self.socks.port}"] + list(args[1:])
        if not tls:
            cmd.append("--no-tls")
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        self.assertEqual(p.returncode, 0, p.stderr)
        recs = [json.loads(l[2:]) for l in p.stdout.splitlines() if l.startswith("J\t")]
        rows = [l.split("\t") for l in p.stdout.splitlines() if not l.startswith("J\t")]
        return recs, rows

    def llm(self, tls):
        self.start(tls)
        recs, rows = self.client("llm", "--target", f"127.0.0.1:{self.http}", "--sequence", "loop", "--turns", "2",
                                 "--upload", "40000", "--think-ms", "50", "--deltas", "20", "--cadence-ms", "10",
                                 "--turn-gap", "0.1", tls=tls)
        self.assertEqual(len(recs), 2)
        for r in recs:
            self.assertEqual(r["fail"], "")
            self.assertEqual(len(r["deltas"]), 20)
            self.assertGreater(r["ttft"], 0.05)          # the server's think time
            self.assertIsNotNone(r["R"])
        self.assertTrue(recs[0]["cold"])
        self.assertFalse(recs[1]["cold"])                 # the loop reuses its connection
        self.assertEqual(recs[1]["U"], 48000)             # turn k uploads U + k x 8 KB
        m = pa.llm_metrics(recs)
        self.assertEqual(m["call_failures"], 0.0)
        self.assertLess(m["lateness_p99_s"], 0.5)
        self.assertEqual([r[2] for r in rows], ["c0t0", "c0t1"])

    def test_llm_over_tls(self):
        if not shutil.which("openssl"):
            self.skipTest("openssl not available")
        self.llm(tls=True)

    def test_llm_plain(self):
        self.llm(tls=False)

    def test_term_with_a_burst(self):
        self.start(tls=False)
        recs, rows = self.client("term", "--target", f"127.0.0.1:{self.term}", "--duration", "3", "--burst-at", "1",
                                 "--burst-every", "10", "--burst-bytes", "200000", "--mean-gap", "0.02", "--floor", "0.01",
                                 tls=False)
        s = recs[0]
        self.assertEqual(s["fail"], "")
        self.assertEqual(len(s["bursts"]), 1)
        self.assertGreater(len(s["keys"]), 30)
        m = pa.term_metrics(recs)
        self.assertLess(m["echo_p99_a_s"], 0.5)

    def test_backgrounds_stop_on_the_stop_file(self):
        self.start(tls=False)
        stop = os.path.join(self.tmp.name, "stop")
        threading.Timer(1.0, lambda: open(stop, "w").close()).start()
        t0 = time.time()
        _, rows = self.client("bgup", "--target", f"127.0.0.1:{self.http}", "--flows", "1", "--duration", "30",
                              "--object-bytes", "100000000", "--stop-file", stop, tls=False)
        self.assertLess(time.time() - t0, 10)
        self.assertTrue(rows and rows[-1][2].startswith("u1o"))


if __name__ == "__main__":
    unittest.main()

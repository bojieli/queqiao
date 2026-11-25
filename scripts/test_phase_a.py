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
        self.assertAlmostEqual(m["upload_r_p50_s"], 0.2)
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

    def test_background_rate_is_over_the_foreground_window(self):
        # Same 10 MB/s background, foregrounds of 45 s and 60 s: equal rates,
        # though the raw totals differ by a third.
        rows = lambda a, b, rate: [{"http": "200", "t_start": f"{a:.3f}", "t_end": f"{b:.3f}", "bytes": str(int(rate * (b - a)))}]
        g1, w1 = pa.rate_in_window(rows(0, 100, 10e6), (10, 55))
        g2, w2 = pa.rate_in_window(rows(0, 100, 10e6), (10, 70))
        self.assertAlmostEqual(g1 / w1, 10e6)
        self.assertAlmostEqual(g2 / w2, 10e6)
        self.assertNotAlmostEqual(g1, g2)
        # Only the part of a transfer inside the window counts.
        g, w = pa.rate_in_window(rows(50, 150, 1e6), (0, 100))
        self.assertAlmostEqual(g, 50e6)

    def test_every_background_flow_must_span_the_foreground(self):
        rows = lambda flow, a, b, n=1000: {"flow": flow, "t_start": f"{a:.3f}", "t_end": f"{b:.3f}", "bytes": str(n)}
        four = [rows(f"b{i}o1", 5, 70) for i in range(1, 5)]
        self.assertTrue(pa.covered_each((10, 60), four, 4))
        # Three downloads failed at once (no bytes); one alone covers.
        one = [rows("b1o1", 5, 70)] + [rows(f"b{i}o1", 5, 70, 0) for i in range(2, 5)]
        self.assertFalse(pa.covered_each((10, 60), one, 4))

    def test_the_foreground_span_uses_its_real_end(self):
        self.assertEqual(pa.fg_span([{"t0": 100.0, "t1": 112.5, "done": 10.0}]), (100.0, 112.5))

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


class Evidence(unittest.TestCase):
    def test_a_failed_canary_is_not_evidence(self):
        mixed = wl.WORKLOADS["mixed"]()
        llm = {"kind": "llm"}
        self.assertTrue(mixed.has_evidence("m1-100k", [llm, {"kind": "term", "canary": True, "fail": "", "keys": [[1, 0.1]]}]))
        self.assertFalse(mixed.has_evidence("m1-100k", [llm, {"kind": "term", "canary": True, "fail": "ConnectionRefusedError",
                                                             "keys": []}]))
        self.assertFalse(mixed.has_evidence("m1-100k", [llm, {"kind": "term", "canary": True, "fail": "", "keys": []}]))


class ClientInProcess(unittest.TestCase):
    """wlclient's own code paths with a scripted socket."""

    def setUp(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location("wlclient_t", CLIENT)
        self.wc = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.wc)
        self.out = io.StringIO()
        p = mock.patch.object(self.wc.sys, "stdout", self.out)
        p.start()
        self.addCleanup(p.stop)

    def records(self):
        return [json.loads(l[2:]) for l in self.out.getvalue().splitlines() if l.startswith("J\t")]

    def test_a_write_error_records_a_failed_session(self):
        class Sock:
            def __init__(self):
                self.ev = threading.Event()

            def settimeout(self, t):
                pass

            def sendall(self, b):
                raise ConnectionResetError("reset by peer")

            def recv(self, n):
                self.ev.wait(5)
                raise OSError("closed")

            def close(self):
                self.ev.set()
        args = self.wc.main.__globals__["argparse"].Namespace(
            delay=0, seed=1, timeout=1, duration=5, canary=True, burst_at=0, burst_every=0, burst_bytes=0,
            mean_gap=0.01, floor=0.01, stop_file="")
        with mock.patch.object(self.wc, "open_conn", return_value=Sock()):
            self.wc.cmd_term(args)
        recs = self.records()
        self.assertEqual(len(recs), 1)
        self.assertIn("ConnectionResetError", recs[0]["fail"])

    def test_ttft_is_the_first_payload_byte(self):
        # The first event arrives split: 10 bytes, then 0.3 s later the rest.
        ev = b'data: {"i":0,"s":1000.0,"R":0.1,"p":"x"}\n\n'
        chunk = b"%x\r\n" % len(ev) + ev + b"\r\n"
        head = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
        cut = chunk.index(b"\r\n") + 2 + 10
        script = [head + chunk[:cut], ("sleep", 0.3), chunk[cut:] + b"0\r\n\r\n"]

        class Sock:
            def settimeout(self, t):
                pass

            def sendall(self, b):
                pass

            def recv(self, n):
                item = script.pop(0)
                if isinstance(item, tuple):
                    time.sleep(item[1])
                    item = script.pop(0)
                return item
        s = Sock()
        args = self.wc.main.__globals__["argparse"].Namespace(think_ms=0, deltas=1, cadence_ms=25, timeout=5)
        r = self.wc.llm_call(s, self.wc.Reader(s), args, 10, seed=1)
        self.assertLess(r["ttft"], 0.2)                  # the first byte, before the 0.3 s stall
        self.assertGreater(r["deltas"][0][1], 0.25)      # the delta itself completed after it


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
            "arms": {"main": {"role": "main", "flags": [], "commit": "a" * 40},
                     "cand": {"role": "candidate", "flags": [], "commit": "b" * 40}},
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
                        # The flow's progress timeline (wlclient bgprog).
                        recs.append({"kind": "bgprog", "dir": "down", "flow": i,
                                     "samples": [[t + k * 0.5, int(10_000_000 * k * 0.5 / (end - t))]
                                                 for k in range(int((end - t) / 0.5) + 1)],
                                     "_block": b, "_path": "a", "_slot": slot, "_arm": arm, "_phase": ph,
                                     "_client": "a", "_job": 0})
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
        self.assertEqual(r["mixed"]["*.bg_rate_bps"], ("atleast", (0.90,)))

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

    def test_a_trial_without_its_records_leaves_the_cell_unmeasured(self):
        # One trial of block 3 lost its raw records: the other trial's must
        # not stand in for the block.
        root = os.path.join(self.tmp.name, "lost")
        write_llm_campaign(root)
        recs = [json.loads(l) for l in pathlib.Path(root, "measures.jsonl").read_text().splitlines()]
        recs = [r for r in recs if not (r["_block"] == 3 and r["_slot"] == 2 and r["_phase"] == "single-100k-none")]
        pathlib.Path(root, "measures.jsonl").write_text("".join(json.dumps(r) + "\n" for r in recs))
        _, v = an.report(root, self.without_class_rule())
        self.assertEqual(v[("a", "cand", "rule:single-100k-none.lateness_p99_s")], "n/a")

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
        self.assertIn("Missing:", text)
        with mock.patch.object(an, "FLIP_WORKLOADS", ("llm",)):
            text, ok = an.flip_report([good], self.gates)
            self.assertTrue(ok, text)
            self.assertIn("| llm | a | cand | single-100k-down4 | PASS |", text)
            late = os.path.join(self.tmp.name, "late")
            write_llm_campaign(late, cand_late=0.15)
            text, ok = an.flip_report([late], self.gates)
            self.assertFalse(ok)
            self.assertIn("| llm | a | cand | single-100k-none | **FAIL** |", text)
            # Every workload on every path, and one release across campaigns.
            other = os.path.join(self.tmp.name, "otherpath")
            write_llm_campaign(other)
            plan = json.loads(pathlib.Path(other, "plan.json").read_text())
            plan["paths"] = ["b"]
            for sl in plan["schedule"]:
                sl["path"] = "b"
            pathlib.Path(other, "plan.json").write_text(json.dumps(plan))
            tsv = pathlib.Path(other, "trials.tsv").read_text().splitlines()
            pathlib.Path(other, "trials.tsv").write_text("\n".join(
                [tsv[0]] + ["\t".join(f[:1] + ["b"] + f[2:]) for f in (l.split("\t") for l in tsv[1:])]) + "\n")
            recs = [json.loads(l) for l in pathlib.Path(other, "measures.jsonl").read_text().splitlines()]
            pathlib.Path(other, "measures.jsonl").write_text("".join(json.dumps(dict(r, _path="b")) + "\n" for r in recs))
            with mock.patch.object(an, "FLIP_WORKLOADS", ("llm", "bulk")):
                bulk = os.path.join(self.tmp.name, "bulkonly")
                write_llm_campaign(bulk)
                bplan = json.loads(pathlib.Path(bulk, "plan.json").read_text())
                bplan["workload"] = "bulk"
                bplan["arms"]["cand"]["commit"] = "f" * 40
                pathlib.Path(bulk, "plan.json").write_text(json.dumps(bplan))
                text, ok = an.flip_report([good, bulk], self.gates)
                self.assertFalse(ok)
                # The newer campaign's build is the final bundle; llm ran on
                # an earlier one and counts only if carried forward.
                self.assertIn("Not on the final bundle: llm single-100k-down4 on a for cand", text)
                text, ok = an.flip_report([good, bulk], self.gates, carry=["single-100k-down4", "single-100k-none"])
                self.assertNotIn("Not on the final bundle", text)
                self.assertIn("| llm | a | cand | single-100k-none | PASS | carried forward from good |", text)
            text, ok = an.flip_report([good, other], self.gates)
            self.assertIn("| llm | b | cand |", text)
            self.assertTrue(ok, text)   # llm on both paths
            # Different candidates per campaign: each covers only part of the
            # matrix, so neither passes it.
            other2 = os.path.join(self.tmp.name, "candb")
            write_llm_campaign(other2)
            p2 = json.loads(pathlib.Path(other2, "plan.json").read_text())
            p2["arms"]["candB"] = p2["arms"].pop("cand")
            for sl in p2["schedule"]:
                sl["arm"] = "candB" if sl["arm"] == "cand" else sl["arm"]
            pathlib.Path(other2, "plan.json").write_text(json.dumps(p2))
            tsv = pathlib.Path(other2, "trials.tsv").read_text().replace("\tcand\t", "\tcandB\t")
            pathlib.Path(other2, "trials.tsv").write_text(tsv)
            recs = [json.loads(l) for l in pathlib.Path(other2, "measures.jsonl").read_text().splitlines()]
            pathlib.Path(other2, "measures.jsonl").write_text(
                "".join(json.dumps(dict(r, _arm="candB" if r["_arm"] == "cand" else r["_arm"])) + "\n" for r in recs))
            with mock.patch.object(an, "FLIP_WORKLOADS", ("llm", "bulk")):
                b2 = os.path.join(self.tmp.name, "bulkcand")
                write_llm_campaign(b2)
                bp = json.loads(pathlib.Path(b2, "plan.json").read_text())
                bp["workload"] = "bulk"
                pathlib.Path(b2, "plan.json").write_text(json.dumps(bp))
                text, ok = an.flip_report([good, b2, other2], self.gates)
                self.assertFalse(ok)
                self.assertIn("for candB", text)

            with contextlib.redirect_stdout(io.StringIO()), mock.patch.object(an, "load_gates", return_value=self.gates):
                self.assertEqual(an.main([late, "--flip"]), 1)
                self.assertEqual(an.main([good, "--flip"]), 0)

    def test_a_dc_campaign_alone_is_not_a_flip(self):
        dc = importlib.import_module("dc_gate")
        root = os.path.join(self.tmp.name, "dconly")
        os.makedirs(root)
        pathlib.Path(root, "plan.json").write_text(json.dumps(
            {"workload": "dc", "main": "legacy", "paths": ["dc"],
             "arms": {"legacy": {"commit": "a" * 40}, "cand": {"commit": "b" * 40}}}))
        with mock.patch.object(dc, "evaluate", return_value=("", {"cand": "PASS"})):
            text, ok = an.flip_report([root], self.gates)
        self.assertFalse(ok)
        self.assertIn("minimal (no campaign)", text)

    def test_every_cell_must_be_covered_on_every_path(self):
        # Path a ran both cells; path b ran only one of them.
        a_ = os.path.join(self.tmp.name, "cells-a")
        write_llm_campaign(a_)
        b_ = os.path.join(self.tmp.name, "cells-b")
        write_llm_campaign(b_)
        plan = json.loads(pathlib.Path(b_, "plan.json").read_text())
        plan["paths"] = ["b"]
        for sl in plan["schedule"]:
            sl["path"] = "b"
        pathlib.Path(b_, "plan.json").write_text(json.dumps(plan))
        tsv = pathlib.Path(b_, "trials.tsv").read_text().splitlines()
        keep = [tsv[0]] + ["\t".join(f[:1] + ["b"] + f[2:]) for f in (l.split("\t") for l in tsv[1:])
                            if f[7] != "single-100k-down4"]
        pathlib.Path(b_, "trials.tsv").write_text("\n".join(keep) + "\n")
        recs = [json.loads(l) for l in pathlib.Path(b_, "measures.jsonl").read_text().splitlines()]
        pathlib.Path(b_, "measures.jsonl").write_text("".join(json.dumps(dict(r, _path="b")) + "\n" for r in recs
                                                              if r["_phase"] != "single-100k-down4"))
        with mock.patch.object(an, "FLIP_WORKLOADS", ("llm",)):
            text, ok = an.flip_report([a_, b_], self.gates)
        self.assertFalse(ok)
        self.assertIn("llm single-100k-down4 on b for cand", text)

    def test_a_pass_on_other_flags_is_stale_unless_carried_forward(self):
        # flags-a ran both cells; the newer flags-b (another flag bundle: the
        # final bundle) reran only one of them.
        a_ = os.path.join(self.tmp.name, "flags-a")
        write_llm_campaign(a_)
        b_ = os.path.join(self.tmp.name, "flags-b")
        write_llm_campaign(b_)
        plan = json.loads(pathlib.Path(b_, "plan.json").read_text())
        plan["arms"]["cand"]["flags"] = ["--sched=class"]
        pathlib.Path(b_, "plan.json").write_text(json.dumps(plan))
        tsv = pathlib.Path(b_, "trials.tsv").read_text().splitlines()
        pathlib.Path(b_, "trials.tsv").write_text("\n".join(
            [tsv[0]] + [l for l in tsv[1:] if l.split("\t")[7] != "single-100k-down4"]) + "\n")
        recs = [json.loads(l) for l in pathlib.Path(b_, "measures.jsonl").read_text().splitlines()]
        pathlib.Path(b_, "measures.jsonl").write_text("".join(json.dumps(r) + "\n" for r in recs
                                                              if r["_phase"] != "single-100k-down4"))
        with mock.patch.object(an, "FLIP_WORKLOADS", ("llm",)):
            text, ok = an.flip_report([a_, b_], self.gates)
            self.assertFalse(ok)
            self.assertIn("llm single-100k-down4 on a for cand ran on an earlier build or other flags (flags-a; "
                          "the final bundle is flags-b)", text)
            # The cell flags-b reran is taken from the final bundle.
            self.assertIn("| llm | a | cand | single-100k-none | PASS |  |", text)
            text, ok = an.flip_report([a_, b_], self.gates, carry=["single-100k-down4"])
            self.assertTrue(ok, text)
            self.assertIn("| llm | a | cand | single-100k-down4 | PASS | carried forward from flags-a |", text)
            with contextlib.redirect_stdout(io.StringIO()), mock.patch.object(an, "load_gates", return_value=self.gates):
                self.assertEqual(an.main([a_, b_, "--flip", "--carry", "single-100k-down4"]), 0)

    def test_a_failure_on_the_final_bundle_is_not_rescued_by_an_earlier_pass(self):
        a_ = os.path.join(self.tmp.name, "early")
        write_llm_campaign(a_)
        b_ = os.path.join(self.tmp.name, "final")
        write_llm_campaign(b_, cand_late=0.15)
        plan = json.loads(pathlib.Path(b_, "plan.json").read_text())
        plan["arms"]["cand"]["commit"] = "e" * 40
        pathlib.Path(b_, "plan.json").write_text(json.dumps(plan))
        with mock.patch.object(an, "FLIP_WORKLOADS", ("llm",)):
            text, ok = an.flip_report([a_, b_], self.gates, carry=["single-100k-none"])
        self.assertFalse(ok)
        self.assertIn("| llm | a | cand | single-100k-none | **FAIL** |  |", text)

    def test_binary_digests_accumulate_across_campaigns(self):
        gw = {"gateway": {"platform": "linux/amd64", "sha256": "1" * 64}}
        first = {"binaries": dict(gw, **{"client:p": {"platform": "linux/amd64", "sha256": "2" * 64}})}
        second = {"binaries": dict(gw, **{"client:q": {"platform": "linux/arm64", "sha256": "3" * 64}})}
        third = {"binaries": dict(gw, **{"client:r": {"platform": "linux/arm64", "sha256": "4" * 64}})}
        acc = an.build_identity(first)
        self.assertTrue(an.same_build(acc, an.build_identity(second)))
        acc = an.merge_build(acc, an.build_identity(second))
        # The third campaign's arm64 client differs from the second's.
        self.assertFalse(an.same_build(acc, an.build_identity(third)))

    def test_prebuilt_binaries_are_compared_by_digest(self):
        a = {"binaries": {"gateway": {"platform": "linux/amd64", "sha256": "1" * 64},
                          "client:p": {"platform": "linux/amd64", "sha256": "2" * 64}}}
        b = {"binaries": {"gateway": {"platform": "linux/amd64", "sha256": "1" * 64},
                          "client:q": {"platform": "linux/amd64", "sha256": "3" * 64}}}
        self.assertTrue(an.same_build(an.build_identity(a), an.build_identity(a)))
        self.assertFalse(an.same_build(an.build_identity(a), an.build_identity(b)))   # another client binary
        self.assertFalse(an.same_build(an.build_identity({}), an.build_identity({})))   # nothing to compare


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

    def test_turns_the_phase_did_not_reach_are_failed_calls(self):
        self.start(tls=False)
        recs, rows = self.client("llm", "--target", f"127.0.0.1:{self.http}", "--sequence", "loop", "--turns", "4",
                                 "--upload", "1000", "--think-ms", "600", "--deltas", "5", "--cadence-ms", "10",
                                 "--turn-gap", "0.1", "--duration", "1", tls=False)
        self.assertEqual(len(recs), 4)
        skipped = [r for r in recs if r["fail"].startswith("not run")]
        self.assertTrue(skipped)
        self.assertGreater(pa.llm_metrics(recs)["call_failures"], 0)

    def test_unanswered_keystrokes_are_censored_not_dropped(self):
        self.start(tls=False)
        # A listener that accepts and never echoes.
        mute = socket.socket()
        mute.bind(("127.0.0.1", 0))
        mute.listen(4)
        held = []
        threading.Thread(target=lambda: held.append(mute.accept()), daemon=True).start()
        recs, _ = self.client("term", "--target", f"127.0.0.1:{mute.getsockname()[1]}", "--duration", "1",
                              "--canary", "--mean-gap", "0.05", "--floor", "0.02", "--timeout", "5", tls=False)
        mute.close()
        s = recs[0]
        self.assertGreater(s["unanswered"], 5)
        self.assertEqual(len(s["keys"]), s["unanswered"])
        self.assertGreater(pa.term_metrics(recs)["echo_p99_a_s"], 1.9)

    def test_a_stalled_tls_handshake_does_not_block_others(self):
        if not shutil.which("openssl"):
            self.skipTest("openssl not available")
        self.start(tls=True)
        stall = socket.create_connection(("127.0.0.1", self.http))    # connects, never handshakes
        try:
            t0 = time.time()
            recs, _ = self.client("llm", "--target", f"127.0.0.1:{self.http}", "--calls", "1", "--upload", "1000",
                                  "--think-ms", "10", "--deltas", "3", "--cadence-ms", "10", tls=True)
            self.assertEqual(recs[0]["fail"], "")
            self.assertLess(time.time() - t0, 10)
        finally:
            stall.close()

    def test_backgrounds_stop_on_the_stop_file(self):
        self.start(tls=False)
        stop = os.path.join(self.tmp.name, "stop")
        threading.Timer(1.0, lambda: open(stop, "w").close()).start()
        t0 = time.time()
        _, rows = self.client("bgup", "--target", f"127.0.0.1:{self.http}", "--flows", "1", "--duration", "30",
                              "--object-bytes", "100000000", "--stop-file", stop, tls=False)
        self.assertLess(time.time() - t0, 25)   # far short of the 30 s window
        self.assertTrue(rows and rows[-1][2].startswith("u1o"))
        # The upload the stop cut counts what the sink received, as a 200
        # with the window's exit (28); never the bytes handed to the socket.
        last = rows[-1]
        self.assertEqual((last[3], last[6]), ("200", "28"))
        self.assertGreater(int(last[4]), 0)
        self.assertLess(int(last[4]), 100_000_000)


if __name__ == "__main__":
    unittest.main()

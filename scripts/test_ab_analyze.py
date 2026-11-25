import datetime as dt
import gzip
import importlib.util
import json
import os
import pathlib
import random
import sys
import tempfile
import unittest

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))
SPEC = importlib.util.spec_from_file_location("ab_analyze", HERE / "ab_analyze.py")
an = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules["ab_analyze"] = an
SPEC.loader.exec_module(an)

COLUMNS = ("block", "path", "slot", "arm", "role", "arm_trial", "cold", "phase", "job", "client", "kind", "flow",
           "start_utc", "end_utc", "http", "bytes", "seconds", "curl_exit", "complete", "wall")
T0 = 1_780_000_000.0


def iso(t):
    return dt.datetime.fromtimestamp(t, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def write_campaign(root, cand_ratio, blocks=12, noise=0.05, seed=1, flows=4, late_every=5, zero_main=(), cand_stalls=3):
    """A two-arm campaign on one path with known outcomes.

    Main delivers ~40 MB per trial (four 10 MB flows) with 20% block-to-block
    variation; the candidate delivers cand_ratio times main in the same block,
    with `noise` independent variation. Every trial has 40 flow ACKs: the
    gateway reads each 0.2 s after the client wrote it, except every
    `late_every`-th, which is a further 0.3 s late (still in order, as on one
    stream). Queue delay is 50 ms;
    reissued bytes are 1% of source bytes; wire bytes are 1.3x useful.
    """
    rng = random.Random(seed)
    os.makedirs(root)
    plan = {"main": "main", "workload": "bulk", "paths": ["p"], "dc": None,
            "arms": {"main": {"role": "main", "flags": [], "commit": "a" * 40},
                     "cand": {"role": "candidate", "flags": ["--cc-api=v2"], "commit": "b" * 40}},
            "clients": {"p": {"small": True, "platform": "linux/amd64"}}}
    with open(os.path.join(root, "plan.json"), "w") as f:
        json.dump(plan, f)
    rows = []
    t = T0
    for b in range(1, blocks + 1):
        level = 10e6 * (1 + rng.gauss(0, 0.2))
        order = ["main", "cand", "cand", "main"] if b % 2 else ["cand", "main", "main", "cand"]
        seen = {}
        for slot, arm in enumerate(order, start=1):
            seen[arm] = seen.get(arm, 0) + 1
            per_flow = level if arm == "main" else level * cand_ratio * (1 + rng.gauss(0, noise))
            if arm == "main" and b in zero_main:
                per_flow = 0
            start, end = t, t + 20
            useful = 0
            for i in range(1, flows + 1):
                size = int(per_flow)
                useful += size
                rows.append([b, "p", slot, arm, "main" if arm == "main" else "candidate", seen[arm], int(seen[arm] == 1),
                             "main", 0, "p", "bulk", f"b{i}", iso(start), iso(end), 200 if size else "000", size, 20.0,
                             0 if size else 28, 1 if size else 0, 20.0])
            tag = f"b{b:03d}-p"
            name = f"{tag}-s{slot}-{arm}"
            md = os.path.join(root, "metrics")
            os.makedirs(md, exist_ok=True)
            with open(os.path.join(md, name + "-pre.txt"), "w") as f:
                f.write("queqiao_quic_packets_sent 0\nqueqiao_quic_loss_observed_packets_total 0\n"
                        "queqiao_quic_bytes_sent 0\nqueqiao_flow_stalls_detected_total 5\n")
            with open(os.path.join(md, name + "-post.txt"), "w") as f:
                f.write(f"queqiao_quic_packets_sent 1000\nqueqiao_quic_loss_observed_packets_total 100\n"
                        f"queqiao_quic_bytes_sent {useful * 1.3}\nqueqiao_flow_stalls_detected_total {5 + (3 if arm == 'main' else cand_stalls)}\n")
            srv = os.path.join(root, "logs", f"gw-{arm}")
            cli = os.path.join(root, "logs", f"cl-p-{arm}")
            os.makedirs(srv, exist_ok=True)
            os.makedirs(cli, exist_ok=True)
            with gzip.open(os.path.join(srv, f"server-{tag}.jsonl.gz"), "at") as sf, \
                    open(os.path.join(cli, f"client-{tag}-s{slot}.jsonl"), "w") as cf:
                bundle = "legacy" if arm == "main" else "cc-api=v2"
                for fh in (sf, cf):
                    fh.write(json.dumps({"time": iso(start), "msg": "runtime configuration", "bundle": bundle,
                                         "congestion": "erasure"}) + "\n")
                for k in range(40):
                    tc = start + 1 + k * 0.4
                    ts_ = tc + 0.2 + (0.3 if k % late_every == late_every - 1 else 0.0)
                    cf.write(json.dumps({"time": iso(tc), "msg": "lane performance snapshot", "type": "lane_metrics",
                                         "flow_id": 7 + slot, "acksout": 1}) + "\n")
                    sf.write(json.dumps({"time": iso(ts_), "msg": "lane performance snapshot", "type": "lane_metrics",
                                         "flow_id": 7 + slot, "acksin": 1, "srtt": 150.0, "minrtt": 100.0,
                                         "inflight": 5000, "pktsent": k + 1000 * slot, "sent": k,
                                         "breissued": 10 * (k + 1), "source": 1000 * (k + 1)}) + "\n")
            with open(os.path.join(cli, f"client-{tag}-s{slot}.cpu"), "w") as f:
                for s in range(22):
                    f.write(f"{start + s:.0f} {s * 50}\n")  # 50 ticks/s = 0.5 core
            t = end + 5
    with open(os.path.join(root, "trials.tsv"), "w") as f:
        f.write("\t".join(COLUMNS) + "\n")
        for r in rows:
            f.write("\t".join(str(x) for x in r) + "\n")


class Analyzer(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.gates = an.load_gates()
        self.gates["resamples"] = 4000

    def tearDown(self):
        self.tmp.cleanup()

    def campaign(self, name, **kw):
        root = os.path.join(self.tmp.name, name)
        write_campaign(root, **kw)
        return root

    def test_gates_file_holds_the_design_values(self):
        g = an.load_gates()
        self.assertEqual(g["ratio"], 0.90)
        self.assertEqual(g["alpha"], 0.05)
        self.assertEqual(g["pilot_blocks"], 12)
        self.assertEqual(g["power"], 0.80)
        self.assertEqual(g["true_ratio"], 1.0)

    def test_equal_candidate_passes(self):
        text, v = an.report(self.campaign("eq", cand_ratio=1.0), self.gates)
        self.assertEqual(v[("p", "cand", "primary")], "**PASS**")
        self.assertEqual(v[("p", "cand", "overall")], "PASS")
        self.assertIn("| cand | 12 |", text)
        self.assertNotIn("interactive p99", text)  # bulk workload: not applicable

    def test_worse_candidate_fails(self):
        text, v = an.report(self.campaign("bad", cand_ratio=0.75), self.gates)
        self.assertEqual(v[("p", "cand", "primary")], "**FAIL**")
        self.assertIn("lower bound not above zero", text)

    def test_zero_main_blocks_void_the_campaign(self):
        _, v = an.report(self.campaign("void", cand_ratio=1.0, zero_main=(1, 2)), self.gates)
        self.assertEqual(v[("p", "cand", "primary")], "VOID")

    def test_trial_measures_have_known_values(self):
        root = self.campaign("known", cand_ratio=1.0)
        plan, trials = an.read_campaign(root)
        method = an.measure(root, plan, trials)
        self.assertEqual(method, "count-matched")
        t = next(iter(trials.values()))
        self.assertAlmostEqual(t.m["loss"], 0.1)
        self.assertEqual(t.m["stalls_per_trial"], 3)
        self.assertAlmostEqual(t.m["wire_per_useful"], 1.3, places=6)
        self.assertAlmostEqual(t.m["queue_p90_ms"], 50.0)
        self.assertAlmostEqual(t.m["reissued_per_source"], 0.01)
        self.assertEqual(t.m["completion"], 1.0)
        # 8 of 40 ACKs are a further 0.3 s late: p50 excess 0, p90 excess 0.3.
        self.assertAlmostEqual(t.m["transit_p50_s"], 0.0, places=3)
        self.assertAlmostEqual(t.m["transit_p90_s"], 0.3, places=3)
        self.assertAlmostEqual(t.m["cpu_p90"], 0.5, places=6)
        self.assertAlmostEqual(t.m["goodput_mbps"], t.m["useful_bytes"] * 8 / 20 / 1e6)

    def test_seq_matched_transit_is_preferred(self):
        sent = [{"flow_id": 1, "seq": i, "_t": 100.0 + i} for i in range(10)]
        recv = [{"flow_id": 1, "seq": i, "_t": 100.5 + i + (2.0 if i == 9 else 0)} for i in range(10)]
        ex = an.transit_exact(sent, recv)
        self.assertEqual(len(ex), 10)
        self.assertAlmostEqual(max(ex), 2.0)
        self.assertAlmostEqual(min(ex), 0.0)

    def test_report_writes_files_and_a_markdown_table(self):
        root = self.campaign("files", cand_ratio=1.0)
        text, _ = an.report(root, self.gates)
        self.assertTrue(os.path.exists(os.path.join(root, "per_trial.tsv")))
        self.assertIn("- `main` gateway: `bundle=legacy congestion=erasure`", text)
        self.assertIn("- `cand` client: `bundle=cc-api=v2 congestion=erasure`", text)
        for want in ("### p: 12 blocks", "| metric | main | cand |", "flow-ACK transit p50 / p90",
                     "queue p90", "**Primary gate (section 15.4), p**", "stalls / trial", "wire bytes / useful byte"):
            self.assertIn(want, text)

    def test_secondary_bounds_are_evaluated(self):
        _, v = an.report(self.campaign("sec", cand_ratio=1.0), self.gates)
        # Identical secondary measures in both arms: every bound passes.
        for key in ("stalls_per_trial", "wire_per_useful", "queue_p90_ms", "completion", "cpu_p90"):
            self.assertEqual(v[("p", "cand", key)], "pass", key)

    def test_a_failed_secondary_fails_the_gate(self):
        text, v = an.report(self.campaign("stalls", cand_ratio=1.0, cand_stalls=9), self.gates)
        self.assertEqual(v[("p", "cand", "primary")], "**PASS**")
        self.assertEqual(v[("p", "cand", "stalls_per_trial")], "**FAIL**")
        self.assertEqual(v[("p", "cand", "overall")], "FAIL")

    def test_pilot_estimates_blocks(self):
        text, res = an.pilot_report(self.campaign("pilot", cand_ratio=1.0, noise=0.10), self.gates)
        n = res[("p", "cand")]
        self.assertIsInstance(n, int)
        self.assertGreaterEqual(n, 2)
        self.assertIn("blocks needed", text)

    def test_cli_writes_summary(self):
        root = self.campaign("cli", cand_ratio=1.0)
        import contextlib
        import io
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(an.main([root]), 0)
            self.assertEqual(an.main([root, "--pilot"]), 0)
        self.assertTrue(os.path.exists(os.path.join(root, "summary.md")))
        self.assertTrue(os.path.exists(os.path.join(root, "pilot.md")))


if __name__ == "__main__":
    unittest.main()

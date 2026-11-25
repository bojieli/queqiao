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
# Import by name from scripts/, so every test module shares one copy (the
# workload registry and the exception classes live in these modules).
sys.path.insert(0, str(pathlib.Path(__file__).parent))
an = importlib.import_module("ab_analyze")

COLUMNS = ("block", "path", "slot", "arm", "role", "arm_trial", "cold", "phase", "job", "client", "kind", "flow",
           "start_utc", "end_utc", "http", "bytes", "seconds", "curl_exit", "complete", "wall", "t_start", "t_end")
T0 = 1_780_000_000.0


def iso(t):
    return dt.datetime.fromtimestamp(t, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def write_campaign(root, cand_ratio, blocks=12, noise=0.05, seed=1, flows=4, late_every=5, zero_main=(), cand_stalls=3,
                   status="complete", drop_slot=None, workload="bulk", cand_probe=None, cand_fallbacks=0, missing_queue=(),
                   cand_transport="auto", cand_capped=False, fetch_failures=(), cand_tcp_lane=False):
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
    plan = {"main": "main", "workload": workload, "paths": ["p"], "dc": None, "schedule": [], "config": {"duration": 20},
            "arms": {"main": {"role": "main", "flags": [], "commit": "a" * 40},
                     "cand": {"role": "candidate", "flags": ["--cc-api=v2"], "commit": "b" * 40}},
            "clients": {"p": {"small": True, "platform": "linux/amd64"}}}
    rows = []
    t = T0
    for b in range(1, blocks + 1):
        level = 10e6 * (1 + rng.gauss(0, 0.2))
        order = ["main", "cand", "cand", "main"] if b % 2 else ["cand", "main", "main", "cand"]
        seen = {}
        for slot, arm in enumerate(order, start=1):
            seen[arm] = seen.get(arm, 0) + 1
            plan["schedule"].append({"block": b, "path": "p", "slot": slot, "arm": arm})
            if drop_slot == (b, slot):
                continue
            per_flow = level if arm == "main" else level * cand_ratio * (1 + rng.gauss(0, noise))
            if arm == "main" and b in zero_main:
                per_flow = 0
            start, end = t, t + 20
            useful = 0
            for i in range(1, flows + 1):
                size = int(per_flow)
                useful += size
                # Backlogged flows: one object each, cut by the 20 s window
                # (exit 28), unless cand_capped makes the candidate's finish in 5 s.
                capped = arm == "cand" and cand_capped
                secs = 5.0 if capped else 20.0
                rows.append([b, "p", slot, arm, "main" if arm == "main" else "candidate", seen[arm], int(seen[arm] == 1),
                             "main", 0, "p", "bulk", f"b{i}o1", iso(start), iso(end), 200 if size else "000", size, secs,
                             (0 if capped else 28) if size else 7, 1 if capped and size else 0, 20.0,
                             f"{start:.3f}", f"{start + secs:.3f}"])
            for k in range(1, 21) if workload == "interactive" else ():
                # Main's probes take 0.1 s; the candidate's take cand_probe,
                # where None means every other one fails.
                ok = arm == "main" or cand_probe is not None or k % 2
                secs = 0.1 if arm == "main" else (cand_probe if cand_probe is not None else 0.05)
                rows.append([b, "p", slot, arm, "x", seen[arm], 0, "main", 1, "p", "probe", f"p{k}", iso(start), iso(end),
                             200 if ok else "000", 1024 if ok else 0, secs if ok else 10.0, 0 if ok else 28, int(bool(ok)), 20.0,
                             f"{start + k * 0.5:.3f}", f"{start + k * 0.5 + (secs if ok else 10.0):.3f}"])
            tag = f"b{b:03d}-p"
            name = f"{tag}-s{slot}-{arm}"
            md = os.path.join(root, "metrics")
            os.makedirs(md, exist_ok=True)
            with open(os.path.join(md, name + "-pre.txt"), "w") as f:
                f.write("queqiao_quic_packets_sent 0\nqueqiao_quic_loss_observed_packets_total 0\n"
                        "queqiao_quic_bytes_sent 0\nqueqiao_flow_stalls_detected_total 5\n")
            with open(os.path.join(md, name + "-pre-client-p.txt"), "w") as f:
                f.write("queqiao_fallbacks_total 2\n")
            with open(os.path.join(md, name + "-post-client-p.txt"), "w") as f:
                f.write(f"queqiao_fallbacks_total {2 + (0 if arm == 'main' else cand_fallbacks)}\n")
            with open(os.path.join(md, name + "-post.txt"), "w") as f:
                f.write(f"queqiao_quic_packets_sent 1000\nqueqiao_quic_loss_observed_packets_total 100\n"
                        f"queqiao_quic_bytes_sent {useful * 1.3}\nqueqiao_flow_stalls_detected_total {5 + (3 if arm == 'main' else cand_stalls)}\n")
            srv = os.path.join(root, "logs", f"gw-{arm}")
            cli = os.path.join(root, "logs", f"cl-p-{arm}")
            os.makedirs(srv, exist_ok=True)
            os.makedirs(cli, exist_ok=True)
            with gzip.open(os.path.join(srv, f"server-{tag}.jsonl.gz"), "at") as sf, \
                    open(os.path.join(cli, f"client-{tag}-s{slot}.jsonl"), "w") as cf:
                lanes = {"1": {"Kind": "quic", "Sent": useful, "Received": 100}}
                if arm == "cand" and cand_tcp_lane:
                    # An AUTO rescue moved part of the flow to TCP: no client
                    # fallback is counted, but the lane shows it.
                    lanes["2"] = {"Kind": "tcp", "Sent": 1000, "Received": 0}
                sf.write(json.dumps({"time": iso(start + 10), "msg": "remote flow complete", "transport": "quic",
                                     "lane_bytes": lanes}) + "\n")
                bundle = "legacy" if arm == "main" else "cc-api=v2"
                for fh in (sf, cf):
                    fh.write(json.dumps({"time": iso(start), "msg": "runtime configuration", "bundle": bundle,
                                         "congestion": "erasure",
                                         "transport": "auto" if arm == "main" else cand_transport}) + "\n")
                for k in range(40):
                    tc = start + 1 + k * 0.4
                    ts_ = tc + 0.2 + (0.3 if k % late_every == late_every - 1 else 0.0)
                    cf.write(json.dumps({"time": iso(tc), "msg": "lane performance snapshot", "type": "lane_metrics",
                                         "flow_id": 7 + slot, "acksout": 1}) + "\n")
                    srtt = 150.0 if (b, slot) not in missing_queue else 0.0
                    sf.write(json.dumps({"time": iso(ts_), "msg": "lane performance snapshot", "type": "lane_metrics",
                                         "flow_id": 7 + slot, "acksin": 1, "srtt": srtt, "minrtt": 100.0,
                                         "inflight": 5000, "pktsent": k + 1000 * slot, "sent": k,
                                         "breissued": 10 * (k + 1), "source": 1000 * (k + 1)}) + "\n")
            with open(os.path.join(cli, f"client-{tag}-s{slot}.cpu"), "w") as f:
                for s in range(22):
                    f.write(f"{start + s:.0f} {s * 50}\n")  # 50 ticks/s = 0.5 core
            t = end + 5
    with open(os.path.join(root, "plan.json"), "w") as f:
        json.dump(plan, f)
    if status:
        with open(os.path.join(root, "status.json"), "w") as f:
            json.dump({"status": status, "exit": 0 if status == "complete" else 1, "production": "unchanged",
                       "fetch_failures": list(fetch_failures)}, f)
    with open(os.path.join(root, "trials.tsv"), "w") as f:
        f.write("\t".join(COLUMNS) + "\n")
        for r in rows:
            f.write("\t".join(str(x) for x in r) + "\n")


class Analyzer(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.gates = an.load_gates()
        self.gates["resamples"] = 4000
        # The same gates without the two secondaries that need stage 0
        # instrumentation, to test the verdict logic on measurable ones.
        self.measurable = dict(self.gates, secondary={k: v for k, v in self.gates["secondary"].items()
                                                      if k not in ("credit_delay_p95_s", "false_promotion_rate")})

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
        text, v = an.report(self.campaign("eq", cand_ratio=1.0), self.measurable)
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
        self.assertIn("- `main` gateway: `bundle=legacy congestion=erasure transport=auto`", text)
        self.assertIn("- `cand` client: `bundle=cc-api=v2 congestion=erasure transport=auto`", text)
        for want in ("### p: 12 blocks", "| metric | main | cand |", "flow-ACK transit p50 / p90",
                     "queue p90", "**Primary gate (section 15.4), p**", "stalls / trial", "wire bytes / useful byte", "TCP fallbacks"):
            self.assertIn(want, text)

    def test_secondary_bounds_are_evaluated(self):
        _, v = an.report(self.campaign("sec", cand_ratio=1.0), self.measurable)
        # Identical secondary measures in both arms: every bound passes.
        for key in ("stalls_per_trial", "wire_per_useful", "queue_p90_ms", "completion", "cpu_p90"):
            self.assertEqual(v[("p", "cand", key)], "pass", key)

    def test_required_secondaries_without_instrumentation_leave_the_gate_incomplete(self):
        text, v = an.report(self.campaign("noinstr", cand_ratio=1.0), self.gates)
        self.assertEqual(v[("p", "cand", "primary")], "**PASS**")
        self.assertEqual(v[("p", "cand", "credit_delay_p95_s")], "n/a")
        self.assertEqual(v[("p", "cand", "false_promotion_rate")], "n/a")
        self.assertEqual(v[("p", "cand", "overall")], "INCOMPLETE")

    def test_partially_measured_secondary_is_not_judged(self):
        _, v = an.report(self.campaign("partial", cand_ratio=1.0, missing_queue=((3, 2),)), self.measurable)
        self.assertEqual(v[("p", "cand", "queue_p90_ms")], "n/a")
        self.assertEqual(v[("p", "cand", "overall")], "INCOMPLETE")

    def test_a_tcp_rescue_lane_has_no_quic_wire_cost(self):
        root = self.campaign("tcplane", cand_ratio=1.0, cand_tcp_lane=True)
        plan, trials = an.read_campaign(root)
        an.measure(root, plan, trials)
        cand = [t for t in trials.values() if t.arm == "cand"]
        self.assertTrue(all(t.m["wire_per_useful"] != t.m["wire_per_useful"] for t in cand))
        self.assertTrue(all(t.m["tcp_flows"] == 1 for t in cand))
        self.assertTrue(an.tcp_evidence({"transport": "quic", "lane_bytes": {"2": {"Kind": "tcp", "Sent": 5}}}))
        self.assertFalse(an.tcp_evidence({"transport": "quic", "lane_bytes": {"2": {"Kind": "tcp", "Sent": 0}}}))

    def test_tcp_fallback_trials_have_no_wire_cost(self):
        root = self.campaign("fallback", cand_ratio=1.0, cand_fallbacks=1)
        plan, trials = an.read_campaign(root)
        an.measure(root, plan, trials)
        cand = [t for t in trials.values() if t.arm == "cand"]
        self.assertTrue(all(t.m["wire_per_useful"] != t.m["wire_per_useful"] for t in cand))
        _, v = an.report(root, self.measurable)
        self.assertEqual(v[("p", "cand", "wire_per_useful")], "n/a")

    def test_a_tcp_transport_client_has_no_quic_wire_cost(self):
        _, v = an.report(self.campaign("tcp", cand_ratio=1.0, cand_transport="tcp"), self.measurable)
        self.assertEqual(v[("p", "cand", "wire_per_useful")], "n/a")

    def test_probes_outside_the_load_are_dropped(self):
        r = lambda a, b: {"t_start": a, "t_end": b}
        load = [r("1000.000", "1010.000"), r("1001.000", "1012.000")]
        probes = [r("1004.900", "1005.000"), r("1019.900", "1020.000"), r("994.900", "995.000"), r("1005", "1006")]
        self.assertEqual(an.under_load(probes, load), [probes[0]])
        # Without timed load, overlap cannot be shown: nothing counts.
        self.assertEqual(an.under_load(probes, [r("", "")]), [])
        # A probe in the idle gap between two waves does not count.
        waves = [r("1008.000", "1010.000"), r("1028.000", "1030.000")]
        self.assertEqual(an.under_load([r("1019.500", "1020.000")], waves), [])
        # Whole seconds could not tell these apart: a 100 ms load ending at
        # 10.1 and a 100 ms probe from 11.0 to 11.1 did not overlap.
        self.assertEqual(an.under_load([r("11.000", "11.100")], [r("10.000", "10.100")]), [])
        # Marginal overlap, below what clock error could explain, is ambiguous.
        self.assertEqual(an.under_load([r("10.050", "10.150")], [r("10.000", "10.100")]), [])
        self.assertEqual(len(an.under_load([r("10.000", "10.500")], [r("10.100", "11.000")])), 1)
        # A fast probe well inside a long download counts, however short.
        self.assertEqual(len(an.under_load([r("30.000", "30.020")], [r("0.000", "60.000")])), 1)
        # At the edge of the load, within the clock uncertainty, it does not.
        self.assertEqual(an.under_load([r("59.950", "59.970")], [r("0.000", "60.000")]), [])

    def test_byte_capped_trials_void_the_gate_and_the_pilot(self):
        # The candidate's flows finish their objects in 5 of 20 s: its bytes
        # equal main's although it was four times faster, or a regression
        # could hide the same way.
        root = self.campaign("capped", cand_ratio=1.0, cand_capped=True)
        text, v = an.report(root, self.measurable)
        self.assertEqual(v[("p", "cand", "primary")], "VOID")
        self.assertIn("ran out of bytes before the window closed", text)
        text, res = an.pilot_report(root, self.gates)
        self.assertIsNone(res[("p", "cand")])
        self.assertIn("VOID", text)

    def test_an_http_error_is_a_failure_not_a_cap(self):
        rows = [{"flow": "b1o1", "complete": "0", "curl_exit": "0", "http": "404", "seconds": "0.2",
                 "t_start": "10.000", "t_end": "10.200", "client": "p"}]
        self.assertEqual(an.capped_flows(rows, 20), 0)
        self.assertFalse(an.flow_ok(rows))

    def test_capping_is_checked_per_client(self):
        # The same flow name on two leaves: one ran the whole window, the
        # other ran out of bytes after 5 s. Grouped together the cap hides.
        full = {"flow": "b1o1", "complete": "0", "curl_exit": "28", "http": "200", "seconds": "60",
                "t_start": "0.000", "t_end": "60.000", "client": "la"}
        short = {"flow": "b1o1", "complete": "1", "curl_exit": "0", "http": "200", "seconds": "5",
                 "t_start": "0.000", "t_end": "5.000", "client": "lb"}
        self.assertEqual(an.capped_by_client([short, full], 60), 1)

    def test_a_pilot_with_zero_main_blocks_recommends_nothing(self):
        text, res = an.pilot_report(self.campaign("zeropilot", cand_ratio=1.0, zero_main=(1, 2)), self.gates)
        self.assertIsNone(res[("p", "cand")])
        self.assertIn("delivered nothing", text)

    def test_crossover_arms_show_both_commits(self):
        root = self.campaign("xover", cand_ratio=1.0)
        plan = json.loads(pathlib.Path(root, "plan.json").read_text())
        plan["arms"]["cand"] = {"role": "candidate", "flags": [], "ref": "origin/main", "gateway_ref": "origin/main",
                                "client_ref": "feat/x", "gateway_commit": "a" * 40, "client_commit": "c" * 40}
        pathlib.Path(root, "plan.json").write_text(json.dumps(plan))
        text, _ = an.report(root, self.measurable)
        self.assertIn("gateway aaaaaaaaaaaa, client cccccccccccc", text)

    def test_looped_flow_completion(self):
        row = lambda f, c, e, h="200": {"flow": f, "complete": c, "curl_exit": e, "http": h}
        self.assertTrue(an.flow_ok([row("b1o1", "1", "0"), row("b1o2", "0", "28")]))
        self.assertFalse(an.flow_ok([row("b1o1", "0", "7", "000")]))
        self.assertFalse(an.flow_ok([row("b1", "0", "28")]))  # a one-object flow must finish it
        self.assertFalse(an.flow_ok([row("b1o1", "0", "56"), row("b1o2", "0", "28")]))

    def test_unfetched_logs_withhold_the_trials_log_measures(self):
        root = self.campaign("fetch", cand_ratio=1.0,
                             fetch_failures=("gw:/home/u/qq-ab-x/gw-cand/logs/server-b004-p.jsonl.1.gz",))
        plan, trials = an.read_campaign(root)
        an.measure(root, plan, trials)
        hit = [t for t in trials.values() if t.block == 4 and t.arm == "cand"]
        other = [t for t in trials.values() if t.block == 4 and t.arm == "main"]
        self.assertTrue(all(t.m["queue_p90_ms"] != t.m["queue_p90_ms"] for t in hit))
        self.assertTrue(all(t.m["queue_p90_ms"] == 50.0 for t in other))
        text, v = an.report(root, self.measurable)
        self.assertEqual(v[("p", "cand", "queue_p90_ms")], "n/a")
        self.assertEqual(v[("p", "cand", "overall")], "INCOMPLETE")

    def test_http_error_bodies_are_not_useful_bytes(self):
        t = an.Trial(1, "p", 1, "main", "main", 1, True)
        t.rows = [{"kind": "bulk", "http": "200", "bytes": "100"}, {"kind": "bulk", "http": "404", "bytes": "50"},
                  {"kind": "bulk", "http": "000", "bytes": "0"}]
        self.assertEqual(t.useful_bytes(), 100)

    def test_power_beyond_the_limit_is_reported_not_capped(self):
        with self.assertRaises(OverflowError):
            an.st.blocks_for_power(1e9, 1.0, max_blocks=50)

    def test_incomplete_overrides_a_failing_verdict_too(self):
        _, v = an.report(self.campaign("badnostatus", cand_ratio=0.6, status=None), self.gates)
        self.assertEqual(v[("p", "cand", "primary")], "INCOMPLETE")

    def test_a_failed_secondary_fails_the_gate(self):
        text, v = an.report(self.campaign("stalls", cand_ratio=1.0, cand_stalls=9), self.measurable)
        self.assertEqual(v[("p", "cand", "primary")], "**PASS**")
        self.assertEqual(v[("p", "cand", "stalls_per_trial")], "**FAIL**")
        self.assertEqual(v[("p", "cand", "overall")], "FAIL")

    def test_unfinished_campaign_gets_no_verdict(self):
        text, v = an.report(self.campaign("nostatus", cand_ratio=1.0, status=None), self.gates)
        self.assertEqual(v[("p", "cand", "primary")], "INCOMPLETE")
        self.assertEqual(v[("p", "cand", "overall")], "INCOMPLETE")
        self.assertIn("did not finish", text)
        _, v = an.report(self.campaign("aborted", cand_ratio=1.0, status="aborted"), self.gates)
        self.assertEqual(v[("p", "cand", "overall")], "INCOMPLETE")

    def test_interrupted_block_is_left_out(self):
        text, v = an.report(self.campaign("hole", cand_ratio=1.0, drop_slot=(5, 3)), self.gates)
        self.assertIn("| cand | 11 |", text)
        self.assertIn("1 of 12 planned blocks are missing trials (first: block 5 on p)", text)
        self.assertEqual(v[("p", "cand", "overall")], "INCOMPLETE")

    def test_failed_probes_count_at_their_timeout(self):
        # The candidate's probes are faster when they finish but half of them
        # fail: its p99 must be the timeout, not its fast survivors.
        root = self.campaign("probes", cand_ratio=1.0, workload="interactive")
        plan, trials = an.read_campaign(root)
        an.measure(root, plan, trials)
        cand = [t for t in trials.values() if t.arm == "cand"]
        main = [t for t in trials.values() if t.arm == "main"]
        self.assertAlmostEqual(cand[0].m["interactive_p99_s"], an.PROBE_TIMEOUT)
        self.assertAlmostEqual(main[0].m["interactive_p99_s"], 0.1)
        _, v = an.report(root, self.gates)
        self.assertEqual(v[("p", "cand", "interactive_p99_s")], "**FAIL**")

    def test_pilot_estimates_blocks(self):
        text, res = an.pilot_report(self.campaign("pilot", cand_ratio=1.0, noise=0.10), self.gates)
        n = res[("p", "cand")]
        self.assertIsInstance(n, int)
        self.assertGreaterEqual(n, 2)
        self.assertIn("blocks needed", text)

    def test_a_short_or_incomplete_pilot_recommends_nothing(self):
        text, res = an.pilot_report(self.campaign("short", cand_ratio=1.0, blocks=6), self.gates)
        self.assertIsNone(res[("p", "cand")])
        self.assertIn("withheld: fewer than 12 blocks", text)
        text, res = an.pilot_report(self.campaign("unfinishedpilot", cand_ratio=1.0, status=None), self.gates)
        self.assertIsNone(res[("p", "cand")])
        self.assertIn("No block count is recommended", text)

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

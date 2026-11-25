import datetime as dt
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
SPEC = importlib.util.spec_from_file_location("dc_gate", HERE / "dc_gate.py")
dc = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules["dc_gate"] = dc
SPEC.loader.exec_module(dc)

COLUMNS = ("block", "path", "slot", "arm", "role", "arm_trial", "cold", "phase", "job", "client", "kind", "flow",
           "start_utc", "end_utc", "http", "bytes", "seconds", "curl_exit", "complete", "wall", "epoch")
LOADED = ["la", "lb", "lc"]


def iso(t):
    return dt.datetime.fromtimestamp(t, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def write_dc(root, cand_shares=(1.0, 1.0, 1.0), cand_total=1.0, binding=0.5, blocks=12, seed=3,
             cand_request=1.0, cand_control=1.0, zero_legacy=(), drop_control=(), finished=False, no_solo=(),
             profile="dc-long-haul", fail_requests=(), stagger=(), gw_args=True):
    """A DC campaign with known shape.

    Legacy: the three loaded leaves share the ancestor evenly (Jain 1, spread
    1.0), 100 MB each per trial, requests 0.30 s, control 0.20 s. The
    candidate's leaves get `cand_shares` of an aggregate `cand_total` times
    legacy's; their request times scale inversely with their share. Solo runs
    deliver legacy_leaf / binding each, so the ancestor binds iff binding < 1.
    """
    rng = random.Random(seed)
    os.makedirs(root)
    plan = {"main": "legacy", "workload": "dc", "paths": ["dc"],
            "dc": {"loaded": LOADED, "control": "ctl", "request_timeout": 30},
            "arms": {"legacy": {"role": "main", "flags": []}, "cand": {"role": "candidate", "flags": ["--congestion=path"]}},
            "clients": {c: {"small": True} for c in LOADED + ["ctl"]}, "schedule": []}
    rows = []
    t = 1_780_000_000.0
    norm = sum(cand_shares) / len(cand_shares)
    for b in range(1, blocks + 1):
        level = 100e6 * (1 + rng.gauss(0, 0.15))
        order = ["legacy", "cand", "cand", "legacy"] if b % 2 else ["cand", "legacy", "legacy", "cand"]
        seen = {}
        for slot, arm in enumerate(order, start=1):
            seen[arm] = seen.get(arm, 0) + 1
            plan["schedule"].append({"block": b, "path": "dc", "slot": slot, "arm": arm})
            noise = 1 + rng.gauss(0, 0.02)
            base = [arm, "main" if arm == "legacy" else "candidate", seen[arm], int(seen[arm] == 1)]
            for j, leaf in enumerate(LOADED):
                share = 1.0 if arm == "legacy" else cand_shares[j] / norm * cand_total
                size = 0 if (arm == "legacy" and b in zero_legacy) else int(level * share * noise / 2)
                for i in (1, 2):
                    rows.append([b, "dc", slot] + base + ["sustained", j, leaf, "bulk", f"b{i}", iso(t), iso(t + 60),
                                                          200, size, 60, 0 if finished else 28, int(finished), 60, int(t + 60)])
                rt = 0.30 * (1.0 if arm == "legacy" else cand_request / max(share, 1e-3)) * noise
                failed = arm in fail_requests
                for w in (1, 2, 3):
                    # Wave w runs in [t+60+10w-1, t+60+10w] on every leaf, unless
                    # this (block, leaf) is staggered past the others.
                    end = int(t + 60 + 10 * w + (5 if (b, leaf) in stagger else 0))
                    for r in range(1, 9):
                        rows.append([b, "dc", slot] + base + ["requests", j, leaf, "requests", f"w{w}r{r}", iso(t + 60),
                                                              iso(t + 70), "000" if failed else 200, 0 if failed else 307200,
                                                              30 if failed else f"{rt:.4f}", 28 if failed else 0, 0 if failed else 1, 10, end])
            ctl = 0.20 * (1.0 if arm == "legacy" else cand_control) * noise
            for phase in ("sustained", "requests"):
                if (b, slot, phase) in drop_control:
                    continue
                for p in range(1, 15):
                    ep = int(t + 1 + 4 * p) if phase == "sustained" else int(t + 70 + 10 * (p % 3))
                    rows.append([b, "dc", slot] + base + [phase, 3, "ctl", "probe", f"p{p}", iso(t), iso(t + 60),
                                                          200, 307200, f"{ctl:.4f}", 0, 1, 60, ep])
            t += 80
        for j, leaf in enumerate(LOADED):
            if (b, leaf) in no_solo:
                continue
            for i in (1, 2):
                rows.append([b, "dc", 100 + j, "legacy", "main", 0, 0, "solo", 0, leaf, "bulk", f"b{i}", iso(t),
                             iso(t + 60), 200, int(level / binding / 2), 60, 28, 0, 60, int(t + 60)])
            t += 65
    os.makedirs(os.path.join(root, "effective"), exist_ok=True)
    for arm in ("legacy", "cand"):
        if gw_args:
            with open(os.path.join(root, "effective", f"gw-{arm}.args"), "w") as f:
                f.write(f"server-b001-dc\t/x/gw-{arm}/queqiaod server --path-profile dc-long-haul\n")
    for arm in ("legacy", "cand"):
        for d in [f"gw-{arm}"] + [f"cl-{c}-{arm}" for c in LOADED + ["ctl"]]:
            os.makedirs(os.path.join(root, "logs", d), exist_ok=True)
            with open(os.path.join(root, "logs", d, "x-b001-dc.jsonl"), "w") as f:
                f.write(json.dumps({"time": iso(t), "msg": "runtime configuration",
                                    "path_profile": profile if arm == "cand" else "dc-long-haul"}) + "\n")
    with open(os.path.join(root, "plan.json"), "w") as f:
        json.dump(plan, f)
    with open(os.path.join(root, "status.json"), "w") as f:
        json.dump({"status": "complete", "exit": 0, "production": "unchanged"}, f)
    with open(os.path.join(root, "trials.tsv"), "w") as f:
        f.write("\t".join(COLUMNS) + "\n")
        for r in rows:
            f.write("\t".join(str(x) for x in r) + "\n")


class DCGate(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.bounds = dc.load_bounds()
        self.bounds["resamples"] = 3000

    def tearDown(self):
        self.tmp.cleanup()

    def run_gate(self, name, **kw):
        root = os.path.join(self.tmp.name, name)
        write_dc(root, **kw)
        return dc.evaluate(root, self.bounds)

    @staticmethod
    def criterion(text, name):
        """The criteria-table row (the one with a contrast formula) for name."""
        return next(l for l in text.splitlines() if l.startswith(f"| {name}") and "`" in l)

    def test_bounds_are_frozen_with_their_provenance(self):
        text = (HERE / "validation" / "dc_gate.ini").read_text()
        for cite in ("section 11.3", "DESIGN-DC-PROFILE.md:149-151", "468-475", "2.5x", "3.5x", "FROZEN"):
            self.assertIn(cite, text)
        b = dc.load_bounds()
        self.assertEqual(b["goodput_ratio"], 0.90)
        self.assertEqual(b["latency_max_ratio"], 1.10)
        self.assertEqual(b["spread_ceiling"], 3.5)

    def test_equal_candidate_passes(self):
        text, v = self.run_gate("eq")
        self.assertEqual(v["cand"], "PASS", text)

    def test_more_aggregate_bought_with_unfairness_fails(self):
        # The independent-relay trade: more total, one leaf starved, groups
        # spread out. This is what the fairness criterion exists to reject.
        text, v = self.run_gate("unfair", cand_shares=(1.6, 1.0, 0.4), cand_total=1.3)
        self.assertEqual(v["cand"], "FAIL")
        self.assertIn("**FAIL**", self.criterion(text, "worst leaf's service"))
        self.assertIn("**FAIL**", self.criterion(text, "completion spread"))
        self.assertIn("**FAIL**", self.criterion(text, "cross-leaf fairness (Jain)"))
        self.assertIn("pass", self.criterion(text, "sustained goodput"))

    def test_throttled_control_leaf_fails(self):
        text, v = self.run_gate("ctl", cand_control=1.4)
        self.assertEqual(v["cand"], "FAIL")
        self.assertIn("**FAIL**", self.criterion(text, "control leaf request p50"))

    def test_slower_requests_fail(self):
        _, v = self.run_gate("slow", cand_request=1.3)
        self.assertEqual(v["cand"], "FAIL")

    def test_unbound_ancestor_voids_the_campaign(self):
        text, v = self.run_gate("free", binding=0.95)
        self.assertEqual(v["cand"], "VOID")
        self.assertIn("did not bind", text)

    def test_zero_legacy_blocks_void_rather_than_fail(self):
        text, v = self.run_gate("zero", zero_legacy=(1, 2))
        self.assertEqual(v["cand"], "VOID")
        self.assertIn("delivered nothing", text)

    def test_missing_control_observations_leave_no_verdict(self):
        text, v = self.run_gate("noctl", drop_control=((4, 2, "requests"),))
        self.assertEqual(v["cand"], "INCOMPLETE")
        self.assertIn("control leaf request p50: 11 of 12 blocks measured", text)

    def test_objects_that_finish_inside_the_window_void_the_campaign(self):
        text, v = self.run_gate("finished", finished=True)
        self.assertEqual(v["cand"], "VOID")
        self.assertIn("raise object_bytes", text)

    def test_binding_is_judged_on_rates(self):
        # Same bytes alone and together, but together took three times as
        # long: the ancestor bound, and a byte ratio would have said 1.0.
        root = os.path.join(self.tmp.name, "rates")
        write_dc(root, binding=1.0)
        path = os.path.join(root, "trials.tsv")
        lines = pathlib.Path(path).read_text().splitlines()
        out = [lines[0]]
        for line in lines[1:]:
            f = line.split("\t")
            if f[7] == "solo":
                f[16] = "20"  # the solo run moved its bytes in a third of the time
            out.append("\t".join(f))
        pathlib.Path(path).write_text("\n".join(out) + "\n")
        text, v = dc.evaluate(root, self.bounds)
        self.assertIn("sum of solo rates, mean over 12 blocks): 0.33", text)
        self.assertNotEqual(v["cand"], "VOID")

    def test_every_judged_block_needs_its_own_calibration(self):
        text, v = self.run_gate("uncal", no_solo=((7, "lb"),))
        self.assertEqual(v["cand"], "INCOMPLETE")
        self.assertIn("lack a usable solo calibration of every loaded leaf (first: block 7)", text)

    def test_the_wrong_profile_gets_no_verdict(self):
        text, v = self.run_gate("profile", profile="wan-shared-bottleneck")
        self.assertEqual(v["cand"], "INCOMPLETE")
        self.assertIn("not only dc-long-haul", text)

    def test_failing_every_request_fails_rather_than_hiding_in_timeouts(self):
        text, v = self.run_gate("cfail", fail_requests=("cand",))
        self.assertEqual(v["cand"], "FAIL")
        self.assertIn("**FAIL**", self.criterion(text, "request completion"))

    def test_a_workload_that_fails_for_both_arms_is_void(self):
        text, v = self.run_gate("bothfail", fail_requests=("cand", "legacy"))
        self.assertEqual(v["cand"], "VOID")
        self.assertIn("the workload does not work", text)

    def test_waves_that_did_not_overlap_leave_no_verdict(self):
        text, v = self.run_gate("stagger", stagger=((3, "lb"),))
        self.assertEqual(v["cand"], "INCOMPLETE")
        self.assertIn("request latency p50: 11 of 12 blocks measured", text)

    def test_gateway_profile_needs_its_own_evidence(self):
        text, v = self.run_gate("nogw", gw_args=False)
        self.assertEqual(v["cand"], "INCOMPLETE")
        self.assertIn("gateway legacy: no evidence it ran --path-profile dc-long-haul", text)

    def test_unfinished_campaign_gets_no_verdict(self):
        root = os.path.join(self.tmp.name, "unfinished")
        write_dc(root)
        os.remove(os.path.join(root, "status.json"))
        text, v = dc.evaluate(root, self.bounds)
        self.assertEqual(v["cand"], "INCOMPLETE")

    def test_pilot_ignores_calibration(self):
        import ab_analyze as an
        root = os.path.join(self.tmp.name, "pilot")
        write_dc(root)
        g = an.load_gates()
        g["resamples"] = 500
        text, res = an.pilot_report(root, g)
        self.assertIn(("dc", "cand"), res)
        self.assertNotIn("incomplete", text)

    def test_measures(self):
        root = os.path.join(self.tmp.name, "m")
        write_dc(root, blocks=2, cand_shares=(2.0, 1.0, 1.0))
        plan, trials = dc.read_campaign(root)
        t = next(t for t in trials.values() if t.arm == "cand")
        m = dc.dc_measures(t, LOADED, "ctl", 30)
        self.assertAlmostEqual(m["jain"], (4 ** 2) / (3 * (4 + 1 + 1)), places=3)
        self.assertAlmostEqual(m["spread"], 2.0, places=2)
        self.assertEqual(m["requests"], 72)


if __name__ == "__main__":
    unittest.main()

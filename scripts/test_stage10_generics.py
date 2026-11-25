"""Sender-side credit delay p95 and the shadow false-promotion rate (design
section 15.4): their extraction from the endpoints' /metrics, and their
gating, on synthetic campaigns: gated rules of every trial type at stage 10,
reported and not gated at stages 6 and 7."""
import importlib
import json
import math
import os
import pathlib
import sys
import tempfile
import unittest

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))
an = importlib.import_module("ab_analyze")
tm = importlib.import_module("test_minimal")
shadow = importlib.import_module("carrier_shadow_summary")

BOUNDS = ["0.001", "0.002", "0.005", "0.01", "0.015", "0.02", "0.03", "0.04", "0.05", "0.075", "0.1", "0.15",
          "0.2", "0.3", "0.4", "0.5", "0.75", "1", "1.5", "2", "3", "5", "10", "30", "+Inf"]


def histogram(samples):
    """/metrics lines of a credit delay histogram holding `samples` (s)."""
    out = {}
    for le in BOUNDS:
        top = math.inf if le == "+Inf" else float(le)
        out[f'queqiao_credit_delay_seconds_bucket{{le="{le}"}}'] = sum(1 for x in samples if x <= top)
    return out


def write_metrics(root, name, values):
    os.makedirs(os.path.join(root, "metrics"), exist_ok=True)
    with open(os.path.join(root, "metrics", f"{name}.txt"), "w") as f:
        for k, v in values.items():
            f.write(f"{k} {v}\n")


def runtime_config(path, shadow_on):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(json.dumps({"time": "2026-06-01T00:00:00Z", "msg": "runtime configuration", "transport": "quic",
                            "carrier_shadow": shadow_on}) + "\n")


def campaign(root, stage, cand_delay=0.012, cand_false=(0, 0), cand_shadow=True, blocks=6):
    """A bulk-only minimal campaign of `blocks` blocks. Each trial's gateway
    and client scraped a credit delay histogram (main's samples around 12
    ms; the candidate's around `cand_delay`) and the carrier-shadow
    counters (the candidate's (false replacements, promotions) per trial
    `cand_false`; main promoted nothing)."""
    tm.unit_campaign(root, [(b, "a", "bulk") for b in range(1, blocks + 1)])
    plan = json.loads(pathlib.Path(root, "plan.json").read_text())
    plan["config"]["minimal_stage"] = stage
    pathlib.Path(root, "plan.json").write_text(json.dumps(plan))
    for b in range(1, blocks + 1):
        tag = f"b{b:03d}-a-bulk"
        for slot, arm in enumerate(("main", "cand", "cand", "main"), start=1):
            delay = cand_delay if arm == "cand" else 0.012
            false_, promoted = cand_false if arm == "cand" else (0, 0)
            # Twenty samples per endpoint, spread a little around the delay.
            samples = [delay * (0.9 + 0.01 * i) + 0.0001 * b for i in range(20)]
            for suffix in ("", "-client-a"):
                pre = dict(histogram([]))
                pre["queqiao_carrier_shadow_false_replacements_total"] = 5
                pre['queqiao_carrier_shadow_transitions_total{to="replace"}'] = 9
                post = dict(histogram(samples))
                post["queqiao_carrier_shadow_false_replacements_total"] = 5 + (false_ if suffix == "" else 0)
                post['queqiao_carrier_shadow_transitions_total{to="replace"}'] = 9 + (promoted if suffix == "" else 0)
                write_metrics(root, f"{tag}-s{slot}-{arm}-pre{suffix}", pre)
                write_metrics(root, f"{tag}-s{slot}-{arm}-post{suffix}", post)
        for arm in ("main", "cand"):
            on = cand_shadow if arm == "cand" else True
            runtime_config(os.path.join(root, "logs", f"gw-{arm}", f"server-{tag}.jsonl"), on)
            runtime_config(os.path.join(root, "logs", f"cl-a-{arm}", f"client-{tag}.jsonl"), on)


def gates():
    g = an.load_gates()
    g["resamples"] = 500
    return g


class Extraction(unittest.TestCase):
    def test_the_quantile_is_interpolated_inside_its_bucket(self):
        cum = [(0.01, 0.0), (0.02, 50.0), (0.05, 100.0), (math.inf, 100.0)]
        self.assertAlmostEqual(an.histogram_quantile(0.5, cum), 0.02)
        self.assertAlmostEqual(an.histogram_quantile(0.95, cum), 0.02 + 0.03 * 45 / 50)
        # In the +Inf bucket: the last finite bound, not infinity.
        self.assertEqual(an.histogram_quantile(0.95, [(0.01, 1.0), (math.inf, 10.0)]), 0.01)
        self.assertTrue(math.isnan(an.histogram_quantile(0.95, [(0.01, 0.0), (math.inf, 0.0)])))

    def test_the_p95_pools_every_endpoints_movement(self):
        fast = [0.004] * 90
        slow = [0.25] * 10
        gw = (histogram([0.004] * 7), histogram([0.004] * 7 + fast))     # scraped before and after
        cl = (histogram([]), histogram(slow))
        p95 = an.credit_delay_p95([gw, cl])
        self.assertGreater(p95, 0.2)                    # the client's slow tenth is the pooled tail
        self.assertLessEqual(p95, 0.3)
        self.assertLess(an.credit_delay_p95([gw]), 0.005)
        # An endpoint that does not export it leaves the trial unmeasured.
        self.assertTrue(math.isnan(an.credit_delay_p95([gw, ({}, {})])))

    def test_the_false_promotion_rate_is_the_summarys(self):
        pre = {"queqiao_carrier_shadow_false_replacements_total": 1,
               'queqiao_carrier_shadow_transitions_total{to="replace"}': 2}
        post = {"queqiao_carrier_shadow_false_replacements_total": 4,
                'queqiao_carrier_shadow_transitions_total{to="replace"}': 6}
        self.assertAlmostEqual(an.false_promotions([(pre, post)], True), 0.75)
        # No promotion: none was false.
        self.assertEqual(an.false_promotions([(pre, pre)], True), 0.0)
        # Without the observer it is not measured, never zero.
        self.assertTrue(math.isnan(an.false_promotions([(pre, post)], False)))
        self.assertEqual(shadow.false_promotion_rate(3, 4), 0.75)

    def test_the_summary_reports_the_rate(self):
        ep = shadow.Endpoint("client")
        ep.records = [{"msg": shadow.STATE, "to": "replace", "_t": 1.0, "_seq": 0, "_process": 1, "carrier_id": 1},
                      {"msg": shadow.STATE, "to": "active", "false_replacement": True, "_t": 2.0, "_seq": 1,
                       "_process": 1, "carrier_id": 1},
                      {"msg": shadow.STATE, "to": "replace", "_t": 3.0, "_seq": 2, "_process": 1, "carrier_id": 2}]
        self.assertEqual(shadow.calibration(ep)["false_promotion_rate"], 0.5)


class Gating(unittest.TestCase):
    def report(self, **kw):
        with tempfile.TemporaryDirectory() as d:
            root = os.path.join(d, "c")
            campaign(root, **kw)
            return an.report(root, gates())

    def test_stage_10_gates_both_on_every_trial_type(self):
        text, v = self.report(stage=10)
        self.assertEqual(v[("a", "cand", "rule:bulk.credit_delay_p95_s")], "pass", text)
        self.assertEqual(v[("a", "cand", "rule:bulk.false_promotion_rate")], "pass", text)
        m = tm.wl.WORKLOADS["minimal"]()
        plan = {"config": {"minimal_stage": 10}}
        for ph in ("bulk", "mixed-67", "mixed-all"):
            self.assertIn("credit_delay_p95_s", m.generics(plan)[ph])
            self.assertIn("false_promotion_rate", m.generics(plan)[ph])

    def test_slower_credit_fails_stage_10(self):
        text, v = self.report(stage=10, cand_delay=0.5)
        self.assertEqual(v[("a", "cand", "rule:bulk.credit_delay_p95_s")], "**FAIL**", text)
        self.assertEqual(v[("a", "cand", "overall")], "FAIL")

    def test_false_promotions_fail_stage_10(self):
        text, v = self.report(stage=10, cand_false=(3, 4))
        self.assertEqual(v[("a", "cand", "rule:bulk.false_promotion_rate")], "**FAIL**", text)

    def test_without_the_observer_stage_10_is_incomplete(self):
        text, v = self.report(stage=10, cand_shadow=False)
        self.assertEqual(v[("a", "cand", "rule:bulk.false_promotion_rate")], "n/a", text)
        self.assertEqual(tm.an.cell_verdicts(v, "a", "cand")["bulk"], "INCOMPLETE")

    def test_stages_6_and_7_report_them_without_gating(self):
        text, v = self.report(stage=7, cand_delay=0.5, cand_false=(3, 4))
        self.assertNotIn(("a", "cand", "rule:bulk.credit_delay_p95_s"), v)
        self.assertNotIn(("a", "cand", "rule:bulk.false_promotion_rate"), v)
        self.assertIn("| bulk.credit_delay_p95_s (report only) |", text)
        self.assertIn("| bulk.false_promotion_rate (report only) |", text)
        self.assertIn("reported and not gated at this campaign's stage", text)

    def test_the_stage_follows_the_trial_types_unless_set(self):
        m = tm.wl.WORKLOADS["minimal"]()
        self.assertEqual(m.reported({"config": {"minimal_trials": ["bulk", "mixed-67"]}})["bulk"],
                         ("credit_delay_p95_s", "false_promotion_rate"))
        self.assertEqual(m.reported({"config": {"minimal_trials": ["mixed-all"]}}), {})
        cfg = tm.parse(tm.BASE)
        self.assertEqual(cfg.minimal_stage, 10 if "mixed-all" in cfg.minimal_trials else 7)
        with self.assertRaises(tm.ab.ConfigError):
            tm.parse(tm.BASE.replace("[workload:minimal]", "[workload:minimal]\nstage = 9"))


if __name__ == "__main__":
    unittest.main()

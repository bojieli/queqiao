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
import unittest.mock

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))
an = importlib.import_module("ab_analyze")
tm = importlib.import_module("test_minimal")
shadow = importlib.import_module("carrier_shadow_summary")
gm = importlib.import_module("generic_metrics")
st = importlib.import_module("ab_stats")

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
                            "carrier_shadow": shadow_on, "carrier_shadow_ks": 0, "carrier_shadow_kp": 0}) + "\n")


def shadow_counters(false_, resolved, base=(3, 7)):
    """The observer's episode counters at k_p = 4 (the default)."""
    return {'queqiao_carrier_shadow_pto_recovered_total{reached="4"}': base[0] + false_,
            'queqiao_carrier_shadow_pto_episodes_total{reached="4"}': base[1] + resolved}


def campaign(root, stage, cand_delay=0.012, cand_false=(0, 0), main_false=(0, 0), cand_shadow=True, blocks=6):
    """A bulk-only minimal campaign of `blocks` blocks. Each trial's gateway
    and client scraped a credit delay histogram (main's samples around 12
    ms; the candidate's around `cand_delay`) and the carrier-shadow
    counters (each arm's (false, resolved) promotions per trial at the
    gateway)."""
    tm.unit_campaign(root, [(b, "a", "bulk") for b in range(1, blocks + 1)])
    plan = json.loads(pathlib.Path(root, "plan.json").read_text())
    plan["config"]["minimal_stage"] = stage
    pathlib.Path(root, "plan.json").write_text(json.dumps(plan))
    for b in range(1, blocks + 1):
        tag = f"b{b:03d}-a-bulk"
        for slot, arm in enumerate(("main", "cand", "cand", "main"), start=1):
            delay = cand_delay if arm == "cand" else 0.012
            false_, promoted = cand_false if arm == "cand" else main_false
            # Twenty samples per endpoint, spread a little around the delay.
            samples = [delay * (0.9 + 0.01 * i) + 0.0001 * b for i in range(20)]
            for suffix in ("", "-client-a"):
                pre = dict(histogram([]), **shadow_counters(0, 0))
                post = dict(histogram(samples), **(shadow_counters(false_, promoted) if suffix == "" else
                                                   shadow_counters(0, 0)))
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
        h = [(0.01, 0.0), (0.02, 50.0), (0.05, 100.0), (math.inf, 100.0)]
        self.assertAlmostEqual(gm.quantile(0.5, h), 0.02)
        self.assertAlmostEqual(gm.quantile(0.95, h), 0.02 + 0.03 * 45 / 50)
        # In the +Inf bucket: the last finite bound, not infinity.
        self.assertEqual(gm.quantile(0.95, [(0.01, 1.0), (math.inf, 10.0)]), 0.01)
        self.assertTrue(math.isnan(gm.quantile(0.95, [(0.01, 0.0), (math.inf, 0.0)])))
        # As samples, for pooling by block: one per count, inside its bucket.
        xs = gm.samples(h)
        self.assertEqual(len(xs), 100)
        self.assertTrue(all(0.01 < x < 0.02 for x in xs[:50]) and all(0.02 < x < 0.05 for x in xs[50:]))

    def test_the_p95_pools_every_endpoints_movement(self):
        gw = (histogram([0.004] * 7), histogram([0.004] * 97))     # scraped before and after
        cl = (histogram([]), histogram([0.25] * 10))
        p95 = gm.quantile(0.95, gm.credit_histogram([gw, cl]))
        self.assertGreater(p95, 0.2)                    # the client's slow tenth is the pooled tail
        self.assertLessEqual(p95, 0.3)
        self.assertLess(gm.quantile(0.95, gm.credit_histogram([gw])), 0.005)
        # An endpoint that does not export it leaves the trial unmeasured.
        self.assertIsNone(gm.credit_histogram([gw, ({}, {})]))

    def test_a_truncated_or_restarted_histogram_is_unmeasured(self):
        pre, post = histogram([0.004] * 3), histogram([0.004] * 9)
        cut = {k: v for k, v in post.items() if "+Inf" not in k}
        self.assertIsNone(gm.histogram_movement(pre, cut))                 # no +Inf: truncated
        self.assertIsNone(gm.histogram_movement(pre, dict(list(post.items())[:5])))
        self.assertIsNone(gm.histogram_movement(post, pre))                # went back: a restart
        self.assertIsNotNone(gm.histogram_movement(pre, post))

    def test_the_false_promotion_rate_is_over_resolved_promotions(self):
        pre, post = shadow_counters(0, 0), shadow_counters(3, 4)
        self.assertEqual(gm.promotions([(pre, post)], [4]), (3, 4))
        self.assertAlmostEqual(gm.rate(gm.promotions([(pre, post)], [4])), 0.75)
        # Its own k_p: at k_p = 5 these counters are not the promotions.
        self.assertIsNone(gm.promotions([(pre, post)], [5]))
        # No promotion resolved: a zero denominator, not a zero rate.
        self.assertTrue(math.isnan(gm.rate(gm.promotions([(pre, pre)], [4]))))
        self.assertEqual(gm.outcomes(gm.promotions([(pre, pre)], [4])), [])
        # Without the observer at an endpoint it is not measured, never zero.
        self.assertIsNone(gm.promotions([(pre, post), (pre, post)], [4, None]))
        self.assertEqual(gm.effective_kp({"carrier_shadow_ks": 0, "carrier_shadow_kp": 0}), 4)
        self.assertEqual(gm.effective_kp({"carrier_shadow_ks": 6, "carrier_shadow_kp": 0}), 6)

    def test_the_summary_reports_the_rate_at_k_p(self):
        with tempfile.TemporaryDirectory() as d:
            log = os.path.join(d, "c.jsonl")
            with open(log, "w") as f:
                f.write(json.dumps({"time": "2026-06-01T00:00:00Z", "msg": "runtime configuration",
                                    "carrier_shadow": True, "carrier_shadow_ks": 0, "carrier_shadow_kp": 0}) + "\n")
                f.write(json.dumps({"time": "2026-06-01T00:00:09Z", "msg": shadow.ENDED, "carrier_id": 1,
                                    "pto_reached": [8, 6, 5, 4] + [0] * 12, "pto_recovered": [5, 3, 2, 1] + [0] * 12,
                                    "pto_dead": [3, 3, 3, 3] + [0] * 12}) + "\n")
            ep = shadow.Endpoint("client")
            ep.load([log])
            cal = shadow.calibration(ep)
            self.assertEqual((cal["k_p"], cal["promotions_resolved"]), (4, 4))
            self.assertEqual(cal["false_promotion_rate"], 0.25)
            # A run without the observer: not measured, not zero.
            with open(log, "w") as f:
                f.write(json.dumps({"time": "2026-06-01T00:00:00Z", "msg": "runtime configuration",
                                    "carrier_shadow": False}) + "\n")
            ep = shadow.Endpoint("client")
            ep.load([log])
            cal = shadow.calibration(ep)
            self.assertFalse(cal["observer"])
            self.assertIsNone(cal["false_promotion_rate"])

    def test_a_proportion_is_pooled_by_block(self):
        c = [[1.0, 0.0], [0.0], [1.0, 1.0, 0.0]]
        m = [[0.0], [0.0, 0.0], [0.0]]
        obs, lo, hi = st.pooled_quantile_bounds(c, m, None, lambda a, b: b - a, 0.05, 200, 1)
        self.assertAlmostEqual(obs, -3 / 6)
        self.assertLessEqual(lo, obs)


class Gating(unittest.TestCase):
    def report(self, **kw):
        with tempfile.TemporaryDirectory() as d:
            root = os.path.join(d, "c")
            campaign(root, **kw)
            return an.report(root, gates())

    def test_stage_10_gates_both_on_every_trial_type(self):
        text, v = self.report(stage=10, cand_false=(0, 2), main_false=(0, 2))
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
        text, v = self.report(stage=10, cand_false=(3, 4), main_false=(0, 2))
        self.assertEqual(v[("a", "cand", "rule:bulk.false_promotion_rate")], "**FAIL**", text)

    def test_no_promotion_at_all_is_reported_not_gated(self):
        # A zero denominator (section 15.4): neither a pass on no evidence
        # nor a missing measure that blocks the cell.
        text, v = self.report(stage=10)
        self.assertEqual(v[("a", "cand", "rule:bulk.false_promotion_rate")], "reported", text)
        self.assertIn("zero denominator", text)
        cells = {("a", "cand", "rule:bulk.x"): "pass", ("a", "cand", "rule:bulk.y"): "reported",
                 ("a", "cand", "primary"): "n/a"}
        self.assertEqual(an.cell_verdicts(cells, "a", "cand")["bulk"], "PASS")

    def test_a_few_slow_samples_do_not_outweigh_many_fast_ones(self):
        # The block's p95 is over its trials' pooled histograms, not the mean
        # of per-trial p95s.
        fast = {"_credit_hist": gm.credit_histogram([(histogram([]), histogram([0.004] * 1000))])}
        slow = {"_credit_hist": gm.credit_histogram([(histogram([]), histogram([2.0]))])}
        m = tm.wl.WORKLOADS["minimal"]()
        T = tm.collections.namedtuple("T", "rows records trial m")
        ts_ = [T([], [], "bulk", fast), T([], [], "bulk", slow)]
        with unittest.mock.patch.object(type(m), "phase_measures", lambda *a: {}):
            out = m.block_extract(ts_, {"config": {"minimal_stage": 10}})
        self.assertLess(out["bulk.credit_delay_p95_s"], 0.005)

    def test_without_the_observer_stage_10_is_incomplete(self):
        text, v = self.report(stage=10, cand_shadow=False, cand_false=(0, 2), main_false=(0, 2))
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

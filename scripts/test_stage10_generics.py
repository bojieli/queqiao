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
        self.assertEqual(len(gm.outcomes(gm.promotions([(pre, pre)], [4]))), 0)
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


class RoundTwo(unittest.TestCase):
    def test_c_cannot_run_below_stage_10(self):
        with self.assertRaises(tm.ab.ConfigError):
            tm.parse(tm.BASE.replace("[workload:minimal]", "[workload:minimal]\ntrials = mixed-all\nstage = 7"))

    def test_a_stage_7_cell_is_not_the_final_bundles_own_evidence(self):
        # The same build and flags at stage 7 and at stage 10: only the
        # stage-10 campaign gated the new secondaries, so a stage-7 cell on
        # the final build counts only when carried forward.
        with tempfile.TemporaryDirectory() as d:
            dirs = {}
            for stage in (7, 10):
                root = dirs[stage] = os.path.join(d, f"s{stage}")
                os.makedirs(root)
                pathlib.Path(root, "plan.json").write_text(json.dumps(
                    {"workload": "minimal", "main": "main", "paths": ["a"], "config": {"minimal_stage": stage},
                     "arms": {"main": {"commit": "a" * 40, "flags": []}, "cand": {"commit": "b" * 40, "flags": []}}}))
                pathlib.Path(root, "status.json").write_text(json.dumps({"finished_utc": f"2026-06-0{stage // 5}"}))
            v = {("a", "cand", "primary"): "n/a", ("a", "cand", "rule:bulk.x"): "pass"}
            with unittest.mock.patch.object(an, "report", return_value=("", v)), \
                    unittest.mock.patch.object(an, "FLIP_WORKLOADS", ("minimal",)), \
                    unittest.mock.patch.object(an, "FLIP_PROFILES", dict(an.FLIP_PROFILES, minimal={"minimal": ("bulk",)})):
                text, ok = an.flip_report([dirs[10]], gates())
                self.assertTrue(ok, text)
                text, ok = an.flip_report([dirs[7]], gates())
                self.assertFalse(ok, text)
                text, ok = an.flip_report([dirs[7]], gates(), carry=["bulk"])
                self.assertTrue(ok, text)
                self.assertIn("carried forward", text)

    def test_a_sparse_p95_is_the_pooled_histograms(self):
        # Ten blocks, one sample each in (1 s, 1.5 s]: the pooled statistic
        # is the histogram's interpolated p95, not a bucket midpoint.
        h = gm.credit_histogram([(histogram([]), histogram([1.2]))])
        blocks = [gm.Tail(h)] * 10
        obs, lo, hi = st.pooled_quantile_bounds(blocks, blocks, 0.95, lambda c, m: c, 0.05, 50, 1,
                                                combine=gm.Tail.quantile)
        self.assertAlmostEqual(obs, gm.quantile(0.95, gm.pool([h] * 10)))
        self.assertAlmostEqual(obs, 1.475)

    def test_the_full_profiles_false_promotions_are_event_weighted(self):
        # A byte-primary campaign: the rate is pooled over blocks' events, and
        # with no promotion it is reported, not a missing measure.
        def trials(cand_counts, main_counts):
            T = an.Trial
            out = {}
            for b, (cc, mc) in enumerate(zip(cand_counts, main_counts), start=1):
                for slot, (arm, counts) in enumerate((("main", mc), ("cand", cc)), start=1):
                    t = T(b, "a", slot, arm, "x", 1, False)
                    t.m["_promotions"] = counts
                    out[(b, "a", slot)] = t
            return out
        ev = lambda ts_: gm.outcomes(gm.pooled_promotions([t.m.get("_promotions") for t in ts_]))
        tr = trials([(1, 1)] + [(0, 50)] * 5, [(0, 1)] + [(0, 50)] * 5)
        c, m, common = an.paired(tr, "a", "main", "cand", ev)
        verdict, d_, lo, n_c, n_m = an.proportion_verdict(list(zip(c, m)), 6, lambda x, y: an.RULES["lower"](x, y, (0.01,)),
                                                          None, gates(), False)
        self.assertEqual((n_c, n_m), (251, 251))
        self.assertAlmostEqual(d_, 0.01 - 1 / 251)       # one false of 251, not a block mean of 1/6
        verdict, *_ = an.proportion_verdict([([], [])] * 6, 6, lambda x, y: 0.0, None, gates(), False)
        self.assertEqual(verdict, "reported")

    def test_a_zero_denominator_abstains_at_a_look(self):
        with tempfile.TemporaryDirectory() as d:
            root = os.path.join(d, "c")
            campaign(root, stage=10)
            v = {("a", "cand", "primary"): "n/a", ("a", "cand", "rule:bulk.x"): "pass",
                 ("a", "cand", "rule:bulk.false_promotion_rate"): "reported"}
            with unittest.mock.patch.object(an, "report", return_value=("", v)):
                dec = an.interim_decisions(root, gates())
        self.assertEqual(dec[("a", "bulk")], ["pass"])

    def test_a_small_clients_slot_has_its_own_configuration(self):
        with tempfile.TemporaryDirectory() as d:
            root = os.path.join(d, "c")
            campaign(root, stage=10, cand_false=(0, 2), main_false=(0, 2))
            plan = json.loads(pathlib.Path(root, "plan.json").read_text())
            plan["clients"] = {"a": {"small": True}}
            pathlib.Path(root, "plan.json").write_text(json.dumps(plan))
            # Only slot 2's own record exists for the candidate's client.
            for b in range(1, 7):
                tag = f"b{b:03d}-a-bulk"
                os.rename(os.path.join(root, "logs", "cl-a-cand", f"client-{tag}.jsonl"),
                          os.path.join(root, "logs", "cl-a-cand", f"client-{tag}-s2.jsonl"))
            _, v = an.report(root, gates())
        self.assertEqual(v[("a", "cand", "rule:bulk.false_promotion_rate")], "n/a")

    def test_the_summary_needs_one_k_p_across_processes(self):
        with tempfile.TemporaryDirectory() as d:
            logs = []
            for pid, kp in ((1, 2), (2, 4)):
                log = os.path.join(d, f"{pid}.jsonl")
                with open(log, "w") as f:
                    f.write(json.dumps({"time": "2026-06-01T00:00:00Z", "msg": "runtime configuration", "pid": pid,
                                        "carrier_shadow": True, "carrier_shadow_kp": kp}) + "\n")
                logs.append(log)
            ep = shadow.Endpoint("client")
            ep.load(logs)
            cal = shadow.calibration(ep)
        self.assertIsNone(cal["k_p"])
        self.assertIsNone(cal["false_promotion_rate"])


class RoundThree(unittest.TestCase):
    def test_a_hard_override_of_the_rate_is_a_hard_assertion(self):
        g = gates()
        g["workloads"]["minimal"] = dict(g["workloads"]["minimal"], **{"bulk.false_promotion_rate": ("ceiling", (0.5,))})
        with tempfile.TemporaryDirectory() as d:
            root = os.path.join(d, "c")
            campaign(root, stage=10, cand_false=(3, 4), main_false=(0, 2))
            text, v = an.report(root, g)
        self.assertEqual(v[("a", "cand", "rule:bulk.false_promotion_rate")], "**FAIL**", text)

    def test_the_full_profiles_sparse_credit_delay_is_pooled(self):
        # One slow sample per block: pooled over blocks, its p95 is the pooled
        # histogram's, and it fails against a fast main.
        def trials(cand, main):
            out = {}
            for b in range(1, 7):
                for slot, (arm, xs) in enumerate((("main", main), ("cand", cand)), start=1):
                    t = an.Trial(b, "a", slot, arm, "x", 1, False)
                    t.m["_credit_hist"] = gm.credit_histogram([(histogram([]), histogram(xs))])
                    out[(b, "a", slot)] = t
            return out
        tr = trials([1.2], [0.01])
        hb = lambda ts_: gm.pool([t.m.get("_credit_hist") for t in ts_])
        hc, hm, _ = an.paired(tr, "a", "main", "cand", hb)
        d_, lo, _ = st.pooled_quantile_bounds([gm.Tail(x) for x in hc], [gm.Tail(y) for y in hm], 0.95,
                                              lambda c, m: an.RULES["lower"](c, m, (0.05,)), 0.05, 200, 1,
                                              combine=gm.Tail.quantile)
        self.assertLess(lo, 0)

    def test_the_summary_reads_thresholds_from_shadow_records_too(self):
        with tempfile.TemporaryDirectory() as d:
            a, b = os.path.join(d, "a.jsonl"), os.path.join(d, "b.jsonl")
            with open(a, "w") as f:
                f.write(json.dumps({"time": "2026-06-01T00:00:00Z", "msg": "runtime configuration", "pid": 1,
                                    "carrier_shadow": True, "carrier_shadow_kp": 4}) + "\n")
            with open(b, "w") as f:      # another process, no configuration record, k_p = 2
                f.write(json.dumps({"time": "2026-06-01T00:00:01Z", "msg": shadow.STATE, "pid": 2, "carrier_id": 1,
                                    "from": "suspect", "to": "replace", "k_s": 2, "k_p": 2}) + "\n")
            ep = shadow.Endpoint("client")
            ep.load([a, b])
            self.assertIsNone(shadow.calibration(ep)["false_promotion_rate"])


class LeadReview(unittest.TestCase):
    def test_draws_without_the_event_count_against_the_bound(self):
        # One resolved, non-false promotion per arm, in one block of six:
        # about a third of block resamples leave it out. Those draws have no
        # rate; dropping them made every remaining draw equal and the lower
        # bound positive, a pass on one event.
        none = gm.Events((0, 0))
        one = gm.Events((0, 1))
        c = [one] + [none] * 5
        m = [one] + [none] * 5
        obs, lo, hi = st.pooled_quantile_bounds(c, m, None, lambda x, y: (y + 0.01) - x, 0.05, 2000, 1,
                                                combine=gm.Events.rate)
        self.assertAlmostEqual(obs, 0.01)
        self.assertEqual(lo, -math.inf)
        self.assertEqual(hi, math.inf)
        verdict, *_ = an.proportion_verdict(list(zip(c, m)), 6, lambda x, y: (y + 0.01) - x, None, gates(), False)
        self.assertEqual(verdict, "**FAIL**")      # never a pass: undecided at a fixed look is a failure
        # With every draw defined the bounds are the ordinary percentiles.
        full = [gm.Events((0, 20))] * 6
        _, lo, hi = st.pooled_quantile_bounds(full, full, None, lambda x, y: (y + 0.01) - x, 0.05, 500, 1,
                                              combine=gm.Events.rate)
        self.assertAlmostEqual(lo, 0.01)
        self.assertAlmostEqual(hi, 0.01)

    def test_a_pair_without_credit_samples_is_not_coverage(self):
        # Six blocks, five of them with complete but unmoved histograms in
        # the candidate: one sampled pair is not six measured blocks.
        g = gates()
        root_trials = {}
        for b in range(1, 7):
            for slot, arm in enumerate(("main", "cand"), start=1):
                t = an.Trial(b, "a", slot, arm, "x", 1, False)
                xs = [0.01] if (arm == "main" or b == 1) else []
                t.m["_credit_hist"] = gm.credit_histogram([(histogram([]), histogram(xs))])
                t.m.update(useful_bytes=1, ok=1, flows=1)
                root_trials[(b, "a", slot)] = t
        hb = lambda ts_: gm.pool([t.m.get("_credit_hist") for t in ts_])
        hc, hm, _ = an.paired(root_trials, "a", "main", "cand", hb)
        have = [(x, y) for x, y in zip(hc, hm) if len(gm.Tail(x)) and len(gm.Tail(y))]
        self.assertEqual(len(have), 1)
        # And through the full profile's [secondary] gate: unmeasured.
        with tempfile.TemporaryDirectory() as d:
            root = os.path.join(d, "c")
            os.makedirs(root)
            plan = {"main": "main", "workload": "bulk", "paths": ["a"], "schedule": [], "config": {"duration": 60},
                    "arms": {"main": {"role": "main", "flags": []}, "cand": {"role": "candidate", "flags": []}}}
            pathlib.Path(root, "plan.json").write_text(json.dumps(plan))
            with unittest.mock.patch.object(an, "read_campaign", return_value=(plan, root_trials)), \
                    unittest.mock.patch.object(an, "completeness", return_value=([], root_trials)), \
                    unittest.mock.patch.object(an, "measure", return_value="none"), \
                    unittest.mock.patch.object(an, "write_per_trial"):
                text, v = an.report(root, g)
        self.assertEqual(v[("a", "cand", "credit_delay_p95_s")], "n/a", text)

    def test_k_p_1_with_a_carrier_still_open_is_unmeasured(self):
        with tempfile.TemporaryDirectory() as d:
            log = os.path.join(d, "c.jsonl")
            with open(log, "w") as f:
                f.write(json.dumps({"time": "2026-06-01T00:00:00Z", "msg": "runtime configuration",
                                    "carrier_shadow": True, "carrier_shadow_ks": 1, "carrier_shadow_kp": 1}) + "\n")
                f.write(json.dumps({"time": "2026-06-01T00:00:05Z", "msg": shadow.ENDED, "carrier_id": 1,
                                    "pto_reached": [4] + [0] * 15, "pto_recovered": [1] + [0] * 15,
                                    "pto_dead": [3] + [0] * 15}) + "\n")
            ep = shadow.Endpoint("client")
            ep.load([log])
            self.assertEqual(shadow.calibration(ep)["false_promotion_rate"], 0.25)
            # A second carrier, still open: its one-expiry episodes are in no log.
            with open(log, "a") as f:
                f.write(json.dumps({"time": "2026-06-01T00:00:06Z", "msg": shadow.STATE, "carrier_id": 2,
                                    "from": "active", "to": "suspect", "k_s": 1, "k_p": 1,
                                    "pto_since_progress": 1, "transport_epoch": 3}) + "\n")
            ep = shadow.Endpoint("client")
            ep.load([log])
            cal = shadow.calibration(ep)
            self.assertIsNone(cal["false_promotion_rate"])
            self.assertIn("open", cal["incomplete"])


if __name__ == "__main__":
    unittest.main()

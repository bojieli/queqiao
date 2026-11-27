"""The direction of every rule kind, checked on synthetic blocks: a candidate
clearly better than main passes, one equal to main passes (it is inside the
margin), one clearly worse fails, for each kind and for every measure the
impaired workload gates, with the bounds frozen in gates.ini.

Semantics (ab_analyze.RULES): the per-block contrast is the candidate's
headroom inside its bound, positive when inside whichever way the measure
points --

  lower v      d = (main + v) - cand          cand <= main + v
  higher v     d = cand - (main - v)          cand >= main - v
  atmost m a   d = (m x main + a) - cand      cand <= m x main + a
  atleast m a  d = cand - (m x main - a)      cand >= m x main - a

A rule passes when both one-sided lower bounds (t and bootstrap) of the mean
contrast are above zero; it fails when an upper bound is below zero, and an
undecided rule fails at the block cap.
"""
import configparser
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import ab_analyze as an
import ab_stats as st

HERE = os.path.dirname(os.path.abspath(__file__))
ALPHA = 0.05


def frozen_rules():
    cp = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=(";", "#"))
    cp.read(os.path.join(HERE, "validation", "gates.ini"))
    out = {}
    for k, v in cp["workload:impaired"].items():
        parts = v.split()
        if parts[0] in an.RULES:
            out[k.split(".", 1)[1]] = (parts[0], tuple(float(x) for x in parts[1:]))
    for k in ("stalls_per_trial",):
        better, margin = cp["secondary"][k].split()
        out[k] = (better, (float(margin),))
    return out


# Six blocks of main's values per measure, and which way is better.
MAIN = {
    "ttft_net_p99_s": ([5.0, 6.1, 4.8, 5.5, 7.0, 5.2], "lower"),
    "echo_p99_s": ([12.0, 9.5, 14.2, 11.1, 10.0, 13.3], "lower"),
    "recover_s": ([6.0, 7.5, 5.2, 8.1, 6.6, 7.0], "lower"),
    "reopen_ttfb_s": ([4.0, 5.5, 3.2, 6.1, 4.6, 5.0], "lower"),
    "resume_download_s": ([5.0, 6.5, 4.2, 7.1, 5.6, 6.0], "lower"),
    "resume_upload_s": ([3.0, 4.5, 2.2, 5.1, 3.6, 4.0], "lower"),
    "ttft_clear_p99_s": ([2.0, 2.4, 1.9, 2.6, 2.2, 2.1], "lower"),
    "ttft_after_restore_s": ([3.0, 4.5, 2.2, 5.1, 3.6, 4.0], "lower"),
    "credit_delay_clear_p95_s": ([0.30, 0.42, 0.28, 0.51, 0.36, 0.40], "lower"),
    "stalls_per_trial": ([36, 28, 40, 31, 25, 33], "lower"),
}


def candidate(main, better, how):
    if how == "equal":
        return list(main)
    good = how == "better"
    if better == "lower":
        return [x * 0.2 for x in main] if good else [x * 3 + 5 for x in main]
    return [1.0] * len(main) if good else [0.0] * len(main)


def judge(kind, values, cand, main):
    d = an.rule_contrasts(kind, values, cand, main)
    _, tl, bl, ok = st.lower_bounds(d, ALPHA, 2000, 7)
    seq = st.Sequential(first=4, cap=6, alpha=ALPHA, spending="pocock").decide(d, 2000, 7)
    return d, ok, seq


class Kinds(unittest.TestCase):
    """Each kind alone, on a measure that points its way."""

    def test_each_kind(self):
        main = [10.0, 12.0, 9.0, 11.0, 10.5, 11.5]
        cases = {
            ("lower", (1.0,)): ([x - 5 for x in main], main, [x + 20 for x in main]),
            ("atmost", (1.1, 0.05)): ([x * 0.5 for x in main], main, [x * 3 for x in main]),
            ("higher", (0.02,)): ([x + 5 for x in main], main, [x - 20 for x in main]),
            ("atleast", (0.9,)): ([x * 1.5 for x in main], main, [x * 0.3 for x in main]),
        }
        for (kind, values), (better, equal, worse) in cases.items():
            with self.subTest(kind=kind):
                d, ok, seq = judge(kind, values, better, main)
                self.assertTrue(all(x > 0 for x in d) and ok and seq == "pass", (kind, "better", d))
                d, ok, seq = judge(kind, values, equal, main)
                self.assertTrue(ok and seq == "pass", (kind, "equal", d))
                d, ok, seq = judge(kind, values, worse, main)
                self.assertTrue(all(x < 0 for x in d) and not ok and seq == "fail", (kind, "worse", d))


class ImpairedMeasures(unittest.TestCase):
    """Every measure the impaired workload gates, under its frozen bound."""

    def test_every_measure_points_the_right_way(self):
        rules = frozen_rules()
        self.assertEqual(set(MAIN), set(rules))
        for name, (main, better) in MAIN.items():
            kind, values = rules[name]
            for how, want in (("better", "pass"), ("equal", "pass"), ("worse", "fail")):
                with self.subTest(measure=name, candidate=how):
                    d, ok, seq = judge(kind, values, candidate(main, better, how), main)
                    self.assertEqual(seq, want, (name, how, kind, values, d))
                    self.assertEqual(ok, want == "pass", (name, how, d))

    def test_event_rates(self):
        cp = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=(";", "#"))
        cp.read(os.path.join(HERE, "validation", "gates.ini"))
        for name in ("call_failures", "silent_rate"):
            kind, v, n = cp["workload:impaired"][f"*.{name}"].split()
            self.assertEqual(kind, "events")
            v, n = float(v), int(n)
            # n is the fewest at which a zero-event candidate passes a
            # zero-event main: (1 - v)^n <= alpha.
            self.assertLessEqual((1 - v) ** n, ALPHA)
            self.assertGreater((1 - v) ** (n - 2), ALPHA)
            per = n // 4 + 1                       # opportunities per block, 4 blocks
            def blocks(ec, em):
                return [((ec[b], per), (em[b], per)) for b in range(4)]
            fin = lambda bl: st.events_decision(bl, v, n, ALPHA, final=True)[0]
            with self.subTest(measure=name):
                self.assertEqual(fin(blocks([0, 0, 0, 0], [1, 2, 0, 3])), "pass")     # better
                self.assertEqual(fin(blocks([0, 0, 0, 0], [0, 0, 0, 0])), "pass")     # equal, none
                self.assertEqual(fin(blocks([1, 0, 1, 0], [1, 0, 1, 0])), "fail")     # equal, some: undecided at the end
                self.assertEqual(fin(blocks([9, 8, 10, 7], [0, 1, 0, 0])), "fail")    # worse
                self.assertEqual(st.events_decision(blocks([1, 0, 1, 0], [1, 0, 1, 0]), v, n, ALPHA, final=False)[0],
                                 "continue")
                # Too few opportunities: unmeasured at the end, undecided before.
                few = [((0, 10), (0, 10))] * 4
                self.assertEqual(st.events_decision(few, v, n, ALPHA, final=True)[0], "n/a")
                self.assertEqual(st.events_decision(few, v, n, ALPHA, final=False)[0], "continue")

    def test_exact_and_permutation_pieces(self):
        self.assertAlmostEqual(st.binom_cdf(0, 150, 0.02), 0.98 ** 150)
        self.assertAlmostEqual(st.binom_cdf(3, 10, 0.5) + st.binom_sf(4, 10, 0.5), 1.0)
        # One block, the candidate worse: half the assignments are as bad.
        self.assertEqual(st.block_permutation_worse([((5, 10), (0, 10))]), 0.5)
        # Every block worse: only the observed assignment is as bad.
        self.assertEqual(st.block_permutation_worse([((5, 10), (0, 10))] * 6), 1 / 64)

    def test_silent_deaths_is_a_hard_ceiling_on_the_candidate(self):
        self.assertTrue(an.HARD["ceiling"](0, (0,)))
        self.assertFalse(an.HARD["ceiling"](1, (0,)))


class Rendering(unittest.TestCase):
    def test_an_unfinished_campaign_shows_undecided_not_fail(self):
        import test_minimal as tm
        with tempfile.TemporaryDirectory() as d:
            root = os.path.join(d, "c")
            tm.unit_campaign(root, [(b, "a", "bulk") for b in range(1, 4)])
            os.remove(os.path.join(root, "status.json"))
            g = an.load_gates()
            g["resamples"] = 500
            text, _ = an.report(root, g)
        rows = [l for l in text.splitlines() if l.startswith("| bulk.")]
        self.assertTrue(rows)
        self.assertFalse([r for r in rows if "FAIL" in r], rows)
        self.assertIn("how far the candidate is inside its bound", text)


if __name__ == "__main__":
    unittest.main()


class PooledCompletion(unittest.TestCase):
    """Flow completion as a pooled event rate (v3): failed over attempted
    short downloads, `events 0.02 149`: 30 a trial, so 6 blocks give 180."""

    def rule(self):
        cp = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=(";", "#"))
        cp.read(os.path.join(HERE, "validation", "gates.ini"))
        kind, v, n = cp["workload:impaired"]["*.fetch_failures"].split()
        self.assertEqual(kind, "events")
        self.assertNotIn("*.completion", cp["workload:impaired"])
        return float(v), int(n)

    def test_every_block_favouring_the_candidate_passes(self):
        v, n = self.rule()
        # imp4 slow-lossy's shape: the candidate completed all 8 every block,
        # main about 7 of 8.
        blocks = [((0, 30), (m, 30)) for m in (3, 4, 2, 1, 3, 2)]
        self.assertEqual(st.events_decision(blocks, v, n, ALPHA, final=True)[0], "pass")

    def test_clearly_worse_fails_and_too_few_attempts_is_unmeasured(self):
        v, n = self.rule()
        self.assertEqual(st.events_decision([((3, 30), (0, 30))] * 6, v, n, ALPHA, final=True)[0], "fail")
        self.assertEqual(st.events_decision([((0, 30), (1, 30))] * 4, v, n, ALPHA, final=True)[0], "n/a")

    def test_two_perfect_arms_pass_in_six_blocks(self):
        v, n = self.rule()
        self.assertLessEqual((1 - v) ** n, ALPHA)
        self.assertEqual(st.events_decision([((0, 30), (0, 30))] * 6, v, n, ALPHA, final=True)[0], "pass")
        # Slack: one block void, or a few downloads cut at the window's end,
        # still leaves 149 attempts.
        self.assertEqual(st.events_decision([((0, 30), (0, 30))] * 5, v, n, ALPHA, final=True)[0], "pass")
        cut = [((0, 29), (0, 28))] + [((0, 30), (0, 30))] * 4
        # 149 / 148: the candidate is what the rule tests, so its 149 attempts
        # are the sample size that matters. Main's shorter count (it is the
        # slower arm; trial windows cut its downloads) used to leave the rule
        # unmeasured for good, which is how imp7's completion gate went
        # unmeasured at 0/150 against 0/109.
        self.assertEqual(st.events_decision(cut, v, n, ALPHA, final=True)[0], "pass")
        thin = [((0, 2), (0, 30))] + [((0, 30), (0, 30))] * 4
        self.assertEqual(st.events_decision(thin, v, n, ALPHA, final=True)[0], "n/a")   # 122 attempts: short
        cut6 = [((0, 27), (0, 28))] + [((0, 30), (0, 30))] * 5
        self.assertEqual(st.events_decision(cut6, v, n, ALPHA, final=True)[0], "pass")   # 177 / 178

    def test_a_trial_has_30_downloads_of_1_mib(self):
        import ab_workloads as w
        w.load_plugins()
        imp = sys.modules["ab_workload_impaired"]
        self.assertEqual((imp.FETCHES, imp.BULK_OBJECT), (30, 1 << 20))

    def test_attempts_leave_out_the_windows_cut(self):
        import types
        import ab_workloads as w
        w.load_plugins()
        imp = sys.modules["ab_workload_impaired"]
        rows = [{"phase": "policed", "kind": "imp-fetch", "flow": f"w1r{k}", "complete": c, "curl_exit": e}
                for k, c, e in ((1, "1", "0"), (2, "1", "0"), (3, "0", "56"), (4, "0", "28"))]
        recs = [{"_phase": "policed", "kind": "fetch", "k": 4, "window_cut": 1}]
        t_ = types.SimpleNamespace(rows=rows, records=recs, trial="policed")
        self.assertEqual(imp.fetch_attempts([t_], "policed"), (1, 3))
        self.assertEqual(imp.Impaired().block_events([t_], {})["policed.fetch_failures"], (1, 3))

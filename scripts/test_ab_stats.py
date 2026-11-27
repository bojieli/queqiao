import importlib.util
import json
import math
import pathlib
import random
import sys
import unittest

SCRIPT = pathlib.Path(__file__).with_name("ab_stats.py")
# Import by name from scripts/, so every test module shares one copy (the
# workload registry and the exception classes live in these modules).
sys.path.insert(0, str(pathlib.Path(__file__).parent))
st = importlib.import_module("ab_stats")


class Quantiles(unittest.TestCase):
    def test_t_quantiles_match_tables(self):
        # Standard Student t table values.
        for p, df, want in [(0.95, 1, 6.3138), (0.95, 4, 2.1318), (0.95, 10, 1.8125), (0.95, 30, 1.6973),
                            (0.80, 10, 0.8791), (0.975, 5, 2.5706), (0.99, 20, 2.5280)]:
            self.assertAlmostEqual(st.t_quantile(p, df), want, places=3, msg=(p, df))

    def test_t_quantile_is_symmetric_and_inverts_the_cdf(self):
        self.assertAlmostEqual(st.t_quantile(0.05, 7), -st.t_quantile(0.95, 7), places=10)
        self.assertEqual(st.t_quantile(0.5, 3), 0.0)
        for df in (2, 9, 50):
            self.assertAlmostEqual(st.t_cdf(st.t_quantile(0.9, df), df), 0.9, places=9)

    def test_t_approaches_normal(self):
        self.assertAlmostEqual(st.t_quantile(0.95, 1e6), st.normal_quantile(0.95), places=4)
        self.assertAlmostEqual(st.normal_quantile(0.95), 1.6448536, places=6)
        self.assertAlmostEqual(st.normal_quantile(0.80), 0.8416212, places=6)

    def test_sample_quantile_interpolates(self):
        self.assertEqual(st.quantile([4, 1, 3, 2], 0.5), 2.5)
        self.assertAlmostEqual(st.quantile([1, 2, 3, 4], 0.9), 3.7)
        self.assertEqual(st.quantile([5], 0.99), 5)
        self.assertTrue(math.isnan(st.quantile([], 0.5)))
        self.assertEqual(st.quantile([1, float("nan"), 3], 0.5), 2)


class Contrast(unittest.TestCase):
    def test_contrast_is_candidate_minus_ratio_times_main(self):
        self.assertEqual(st.contrasts([90, 100, 0], [100, 100, 50], 0.90), [0.0, 10.0, -45.0])

    def test_contrast_is_defined_for_zero_outcomes(self):
        self.assertEqual(st.contrasts([0, 10], [0, 0]), [0.0, 10.0])

    def test_contrast_requires_pairs(self):
        with self.assertRaises(ValueError):
            st.contrasts([1, 2], [1])

    def test_t_lower_bound_known_answer(self):
        # mean 3, sd sqrt(2.5), se sqrt(0.5), t(0.95, 4) = 2.131847
        self.assertAlmostEqual(st.t_lower_bound([1, 2, 3, 4, 5]), 3 - 2.131847 * math.sqrt(0.5), places=5)
        self.assertTrue(math.isnan(st.t_lower_bound([1])))


class Bootstrap(unittest.TestCase):
    def test_constant_data_has_its_constant_as_bound(self):
        self.assertEqual(st.bootstrap_lower_bound([2.5] * 8, resamples=500), 2.5)

    def test_two_point_distribution_is_exact(self):
        # Resampled means of [0, 1] are 0, 0.5, 1 with probability 1/4, 1/2, 1/4.
        self.assertEqual(st.bootstrap_bound([0, 1], alpha=0.05, resamples=20000, seed=3), 0.0)
        self.assertEqual(st.bootstrap_bound([0, 1], alpha=0.05, resamples=20000, seed=3, upper=True), 1.0)
        self.assertEqual(st.bootstrap_bound([0, 1], alpha=0.40, resamples=20000, seed=3), 0.5)

    def test_seeded_and_deterministic(self):
        xs = [random.Random(1).gauss(0, 1) for _ in range(30)]
        self.assertEqual(st.bootstrap_lower_bound(xs, seed=9, resamples=2000),
                         st.bootstrap_lower_bound(xs, seed=9, resamples=2000))

    def test_agrees_with_t_on_normal_data(self):
        rng = random.Random(5)
        xs = [rng.gauss(10, 2) for _ in range(200)]
        se = st.stdev(xs) / math.sqrt(len(xs))
        self.assertLess(abs(st.bootstrap_lower_bound(xs, resamples=20000) - st.t_lower_bound(xs)), 0.1 * se + 0.02)

    def test_false_pass_rate_under_the_null_is_about_alpha(self):
        # True mean contrast exactly zero: a one-sided 95% bound should clear
        # zero about 5% of the time.
        rng = random.Random(11)
        reps, t_pass, b_pass = 300, 0, 0
        for i in range(reps):
            d = [rng.gauss(0, 1) for _ in range(25)]
            t_pass += st.t_lower_bound(d) > 0
            b_pass += st.bootstrap_lower_bound(d, resamples=400, seed=i) > 0
        self.assertTrue(0.015 <= t_pass / reps <= 0.095, t_pass)
        self.assertTrue(0.015 <= b_pass / reps <= 0.11, b_pass)


class Gate(unittest.TestCase):
    def blocks(self, ratio, n=30, noise=0.03, seed=2):
        rng = random.Random(seed)
        main = [100.0 * (1 + rng.gauss(0, 0.2)) for _ in range(n)]
        cand = [m * ratio * (1 + rng.gauss(0, noise)) for m in main]
        return cand, main

    def test_equal_arms_pass(self):
        g = st.gate(*self.blocks(1.0), resamples=4000)
        self.assertTrue(g.passed, g)
        self.assertFalse(g.void)
        self.assertAlmostEqual(g.ratio_of_means, 1.0, delta=0.03)

    def test_exactly_the_threshold_does_not_pass(self):
        main = [100.0, 120.0, 80.0, 90.0, 110.0, 105.0]
        g = st.gate([0.9 * m for m in main], main, resamples=2000)
        self.assertFalse(g.passed)
        self.assertEqual(g.mean_contrast, 0.0)

    def test_clearly_worse_candidate_fails(self):
        g = st.gate(*self.blocks(0.75), resamples=4000)
        self.assertFalse(g.passed)
        self.assertLess(g.t_lower, 0)
        self.assertLess(g.boot_lower, 0)

    def test_too_many_zero_main_blocks_voids(self):
        cand, main = self.blocks(1.0, n=20)
        main[:3] = [0.0, 0.0, 0.0]
        g = st.gate(cand, main, resamples=1000, max_zero_main_fraction=0.10)
        self.assertTrue(g.void)
        self.assertFalse(g.passed)
        self.assertAlmostEqual(g.zero_main_fraction, 0.15)

    def test_zero_candidate_blocks_count_against_it(self):
        cand, main = self.blocks(1.0, n=12)
        cand[:4] = [0.0] * 4
        self.assertFalse(st.gate(cand, main, resamples=2000).passed)

    def test_lower_bounds_helper(self):
        m, tl, bl, ok = st.lower_bounds([1.0, 1.1, 0.9, 1.05, 0.95, 1.0], resamples=2000)
        self.assertTrue(ok)
        self.assertAlmostEqual(m, 1.0)
        self.assertFalse(st.lower_bounds([1.0, -1.0, 0.5, -0.5], resamples=2000)[3])


class Power(unittest.TestCase):
    def test_smallest_n_satisfying_the_t_condition(self):
        sd, main = 30.0, 100.0  # delta = 10
        n = st.blocks_for_power(sd, main)
        cond = lambda k: math.sqrt(k) >= (st.t_quantile(0.95, k - 1) + st.t_quantile(0.8, k - 1)) * sd / 10.0
        self.assertTrue(cond(n))
        self.assertFalse(cond(n - 1))

    def test_large_n_matches_the_normal_formula(self):
        # ((z_.95 + z_.80) * sd / delta)^2 = (2.486475 * 10)^2 = 618.3
        n = st.blocks_for_power(1.0, 1.0)
        self.assertTrue(618 <= n <= 625, n)

    def test_alternative_must_be_positive(self):
        with self.assertRaises(ValueError):
            st.blocks_for_power(1.0, 100.0, ratio=1.0, true_ratio=1.0)
        self.assertEqual(st.blocks_for_power(0.0, 100.0), 2)


class Misc(unittest.TestCase):
    def test_jain(self):
        self.assertAlmostEqual(st.jain([5, 5, 5]), 1.0)
        self.assertAlmostEqual(st.jain([9, 0, 0]), 1 / 3)
        self.assertAlmostEqual(st.jain([1, 3]), 16 / 20)

    def test_paired_secondary(self):
        m, bound, n = st.paired_secondary([2, 3, float("nan"), 5], [1, 1, 1, 1], resamples=2000)
        self.assertEqual(n, 3)
        self.assertAlmostEqual(m, (1 + 2 + 4) / 3)
        self.assertLessEqual(bound, m)
        m, ub, n = st.paired_secondary([2, 3, 5], [1, 1, 1], resamples=2000, upper=True)
        self.assertGreaterEqual(ub, m)



class SequentialStopping(unittest.TestCase):
    def test_levels_come_from_pocock_type_spending(self):
        # Design section 15.4: Lan-DeMets Pocock-type spending over the 7
        # looks (6..12 blocks), total one-sided alpha 0.05.
        sq = st.Sequential(first=6, cap=12, alpha=0.05)
        self.assertEqual(sq.looks, 7)
        self.assertEqual(len(sq.levels), 7)
        # The first look spends alpha x ln(1 + (e - 1) x 6/12) on its own.
        self.assertAlmostEqual(sq.alpha_at(6), 0.05 * math.log(1 + (math.e - 1) * 0.5), places=6)
        self.assertAlmostEqual(st.pocock_spending(1.0, 0.05), 0.05)
        for n in range(6, 13):
            self.assertTrue(0.01 < sq.alpha_at(n) < 0.05, (n, sq.alpha_at(n)))
        self.assertEqual(sq.alpha_at(20), sq.alpha_at(12))       # past the cap: the last look's

    def test_the_levels_spend_exactly_alpha_on_a_random_walk(self):
        # The nominal levels, applied at every look to a standardised random
        # walk under the null, cross with total probability alpha.
        sq = st.Sequential()
        c = [st.normal_quantile(1 - a) for a in sq.levels]
        rng = random.Random(5)
        reps, hits = 40000, 0
        for _ in range(reps):
            s = 0.0
            for n in range(1, 13):
                s += rng.gauss(0, 1)
                if n >= 6 and s / math.sqrt(n) > c[n - 6]:
                    hits += 1
                    break
        self.assertAlmostEqual(hits / reps, 0.05, delta=0.005)

    def test_bonferroni_is_the_fallback(self):
        sq = st.Sequential(spending="bonferroni")
        self.assertEqual(sq.levels, [0.05 / 7] * 7)
        with self.assertRaises(ValueError):
            st.Sequential(spending="haybittle")
        with self.assertRaises(ValueError):
            st.Sequential(alpha=1.5)

    def test_frozen_levels_round_trip_through_a_plan(self):
        sq = st.Sequential()
        again = st.Sequential(**json.loads(json.dumps(sq.__dict__)))
        self.assertEqual(again.levels, sq.levels)
        with self.assertRaises(ValueError):
            st.Sequential(levels=[0.05] * 3)                       # not 7 looks
        with self.assertRaises(ValueError):
            st.Sequential(levels=[0.9] * 7)                        # spends more than alpha x looks

    def test_a_clear_negative_bound_fails_at_the_first_look(self):
        sq = st.Sequential()
        rng = random.Random(8)
        self.assertEqual(sq.decide([-5 + rng.gauss(0, 1) for _ in range(6)], resamples=2000), "fail")

    def test_undecided_at_the_cap_fails(self):
        sq = st.Sequential()
        rng = random.Random(9)
        murky = [rng.gauss(0.05, 1) for _ in range(12)]
        self.assertEqual(sq.decide(murky[:11], resamples=2000), "continue")
        self.assertEqual(sq.decide(murky, resamples=2000), "fail")

    def test_upper_bound_known_answer(self):
        self.assertAlmostEqual(st.t_upper_bound([1, 2, 3, 4, 5]), 3 + 2.131847 * math.sqrt(0.5), places=5)

    def test_clear_effects_stop_early_and_ambiguous_ones_run_to_the_cap(self):
        sq = st.Sequential()
        rng = random.Random(4)
        clear = [10 + rng.gauss(0, 1) for _ in range(6)]
        self.assertEqual(sq.decide(clear, resamples=2000), "pass")
        self.assertEqual(sq.decide([-x for x in clear], resamples=2000), "fail")
        murky = [rng.gauss(0.2, 1) for _ in range(6)]
        self.assertEqual(sq.decide(murky, resamples=2000), "continue")
        self.assertEqual(sq.decide(murky[:5], resamples=2000), "continue")   # before the first look
        self.assertIn(sq.decide([rng.gauss(0.2, 1) for _ in range(12)], resamples=2000), ("pass", "fail"))

    def test_the_whole_procedure_keeps_its_error_rate(self):
        # True mean contrast exactly zero (the margin): how often does the
        # procedure, looking after every block from 6 to 12, wrongly pass?
        sq = st.Sequential()
        rng = random.Random(21)
        reps, passes = 400, 0
        for r in range(reps):
            d = [rng.gauss(0, 1) for _ in range(12)]
            for n in range(sq.first, sq.cap + 1):
                v = sq.decide(d[:n], resamples=300, seed=r)
                if v != "continue":
                    passes += v == "pass"
                    break
        self.assertLessEqual(passes / reps, 0.07, passes)   # nominal 0.05, with Monte Carlo slack

    def test_overall_stop(self):
        self.assertTrue(st.overall_stop(["pass", "fail", "continue"]))
        self.assertTrue(st.overall_stop(["pass", "pass"]))
        self.assertFalse(st.overall_stop(["pass", "continue"]))
        self.assertFalse(st.overall_stop([]))

class EventsDecision(unittest.TestCase):
    def test_zero_events_judged_when_the_slow_arm_has_the_fewer_opportunities(self):
        # imp7's completion rule, "events 0.02 min 149": 0 failures in 150
        # candidate opportunities and in 109 of main's. The slower arm's
        # count used to leave the rule unmeasured for good.
        blocks = [((0, 25), (0, 18)), ((0, 25), (0, 18)), ((0, 25), (0, 18)),
                  ((0, 25), (0, 18)), ((0, 25), (0, 18)), ((0, 25), (0, 19))]
        dec, info = st.events_decision(blocks, 0.02, 149, 0.05, True)
        self.assertEqual((dec, info["ec"], info["em"]), ("pass", 0, 0))

    def test_a_thin_candidate_is_unmeasured_not_judged(self):
        blocks = [((0, 2), (0, 200))]
        self.assertEqual(st.events_decision(blocks, 0.02, 149, 0.05, True)[0], "n/a")
        self.assertEqual(st.events_decision(blocks, 0.02, 149, 0.05, False)[0], "continue")

    def test_no_opportunity_in_main_is_unmeasured(self):
        blocks = [((0, 200), (0, 0))]
        self.assertEqual(st.events_decision(blocks, 0.02, 149, 0.05, True)[0], "n/a")

    def test_an_excess_of_events_fails(self):
        blocks = [((30, 100), (0, 100)), ((30, 100), (0, 100))]
        self.assertEqual(st.events_decision(blocks, 0.02, 149, 0.05, True)[0], "fail")


if __name__ == "__main__":
    unittest.main()

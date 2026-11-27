"""Analysis changes adopted before imp3's first look, outcome-blind: wildcard
rules a trial type is exempt from, broken keystroke sessions censored at the
window's end, and per-candidate sequential stopping."""
import io
import json
import os
import pathlib
import sys
import tempfile
import types
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import ab_analyze as an
import ab_campaign as ab
import ab_workloads as w

w.load_plugins()
imp = sys.modules["ab_workload_impaired"]


class WildcardExemption(unittest.TestCase):
    def test_blackhole_is_not_judged_on_the_all_calls_ttft(self):
        wl = imp.Impaired()
        rules = {"*.ttft_net_p99_s": ("atmost", (1.1, 0.2)), "*.echo_p99_s": ("atmost", (1.1, 0.05))}
        keys = ["slow-lossy.ttft_net_p99_s", "slow-lossy.echo_p99_s", "blackhole.echo_p99_s",
                "blackhole.ttft_all_p99_s"]
        judged, unruled = an.expand_rules(keys, rules, ["slow-lossy", "blackhole"], wl, {"config": {}})
        self.assertIn("slow-lossy.ttft_net_p99_s", judged)
        self.assertIn("blackhole.echo_p99_s", judged)
        self.assertNotIn("blackhole.ttft_net_p99_s", judged)      # not even as unmeasured
        self.assertEqual(unruled, [])                              # ttft_all_p99_s is report-only

    def test_without_an_exemption_a_missing_measure_is_unmeasured(self):
        wl = types.SimpleNamespace(report_only=(), rule_exempt={})
        judged, _ = an.expand_rules([], {"*.x": ("lower", (1.0,))}, ["a"], wl, {})
        self.assertIn("a.x", judged)


class CensoredEchoes(unittest.TestCase):
    def trial(self, sessions, first_fetch=1000.0):
        rows = [{"phase": "blackhole", "kind": "imp-fetch", "flow": "w1r1", "t_start": str(first_fetch)}]
        return types.SimpleNamespace(rows=rows, records=[dict(s, _phase="blackhole", kind="term") for s in sessions])

    def test_a_broken_session_counts_its_unechoed_keys_to_the_window_end(self):
        # Session starts at 1005 (the window ends at 1120); keys at +10 and
        # +20 echoed, +70 and +80 never (it broke during the outage).
        s = {"t0": 1005.0, "fail": "ConnectionResetError: reset", "unanswered": 2,
             "keys": [[10.0, 0.3], [20.0, 0.4], [70.0, 2.0], [80.0, 1.0]]}
        got = imp.echo_samples(self.trial([s]), "blackhole")
        self.assertEqual(got[:2], [0.3, 0.4])
        self.assertAlmostEqual(got[2], 1120 - 1005 - 70)
        self.assertAlmostEqual(got[3], 1120 - 1005 - 80)

    def test_a_clean_session_is_unchanged(self):
        s = {"t0": 1005.0, "fail": "", "unanswered": 0, "keys": [[10.0, 0.3], [20.0, 0.4]]}
        self.assertEqual(imp.echo_samples(self.trial([s]), "blackhole"), [0.3, 0.4])

    def test_without_the_window_the_session_end_censoring_stands(self):
        s = {"t0": 1005.0, "fail": "x", "unanswered": 1, "keys": [[10.0, 0.3], [70.0, 2.0]]}
        t = types.SimpleNamespace(rows=[], records=[dict(s, _phase="blackhole", kind="term")])
        self.assertEqual(imp.echo_samples(t, "blackhole"), [0.3, 2.0])


class PerCandidateStopping(unittest.TestCase):
    def campaign(self):
        import test_impair as ti
        cfg = ti.parse(ti.BASE)
        return ab.Campaign(cfg, "/nonexistent", ".", dry=False, stream=io.StringIO())

    def test_one_candidates_fail_does_not_stop_the_type_for_the_other(self):
        c = self.campaign()
        look = {("a", "blackhole", "s7"): ["fail", "continue"], ("a", "blackhole", "s8"): ["continue", "pass"]}
        with mock.patch.object(an, "interim_decisions_by_candidate", return_value=look), \
                mock.patch.object(an, "load_gates", return_value={}):
            stop, why = c.interim("a", "blackhole", 4)
        self.assertFalse(stop)
        self.assertEqual(c.stopped_candidates, {"a/blackhole/s7": 4})
        look = {("a", "blackhole", "s7"): ["fail", "fail"], ("a", "blackhole", "s8"): ["pass", "pass"]}
        with mock.patch.object(an, "interim_decisions_by_candidate", return_value=look) as m, \
                mock.patch.object(an, "load_gates", return_value={}):
            stop, why = c.interim("a", "blackhole", 5)
        self.assertTrue(stop)
        self.assertEqual(c.stopped_candidates, {"a/blackhole/s7": 4, "a/blackhole/s8": 5})
        # The look is told which candidates are already decided.
        self.assertEqual(m.call_args[0][3]["stopped_candidates"], {"a/blackhole/s7": 4})
        self.assertIn("s7: decided after block 4", why)

    def test_a_decided_candidate_is_judged_on_its_blocks_up_to_the_look(self):
        import test_minimal as tm
        with tempfile.TemporaryDirectory() as d:
            root = os.path.join(d, "c")
            tm.unit_campaign(root, [(b, "a", "bulk") for b in range(1, 7)])
            status = json.loads(pathlib.Path(root, "status.json").read_text())
            status["stopped_candidates"] = {"a/bulk/cand": 4}
            pathlib.Path(root, "status.json").write_text(json.dumps(status))
            g = an.load_gates()
            g["resamples"] = 500
            text, _ = an.report(root, g)
        row = next(l for l in text.splitlines() if l.startswith("| bulk.goodput_bps |") and "| cand |" in l)
        self.assertIn("| cand | 4 |", row)


if __name__ == "__main__":
    unittest.main()

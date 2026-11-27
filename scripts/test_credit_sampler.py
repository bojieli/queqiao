"""Credit delay sampled about once a second (gates v6, adopted after imp6,
not re-judged): blackhole is judged clear of its outages."""
import io
import json
import math
import os
import sys
import tempfile
import types
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import ab_campaign as ab
import ab_workloads as w
import generic_metrics as gm
import test_impair as ti

w.load_plugins()
imp = sys.modules["ab_workload_impaired"]
OUTS = [[30.0, 38.0], [70.0, 95.0]]


def cum(**kw):
    """A cumulative histogram sample {le: count} with buckets 0.2, 1, 10, +Inf."""
    vals = [kw.get("a", 0), kw.get("b", 0), kw.get("c", 0), kw.get("d", 0)]
    out, run = {}, 0.0
    for le, v in zip(("0.2", "1", "10", "+Inf"), vals):
        run += v
        out[le] = run
    return out


class Windows(unittest.TestCase):
    def trial(self, samples, outs=OUTS):
        recs = [{"_phase": "blackhole", "kind": "impair", "fail": "", "outages": outs},
                {"_phase": "blackhole", "kind": "credit_ts", "end": "gateway", "samples": samples}]
        return types.SimpleNamespace(trial="blackhole", rows=[], records=recs)

    def test_increments_go_to_the_window_each_interval_ends_in(self):
        samples = [[10.0, cum()], [11.0, cum(a=10)],            # clear: 10 short waits
                   [35.0, cum(a=10, c=1)],                     # within the 8 s outage
                   [40.0, cum(a=10, b=3, c=1)],                # within 5 s after it
                   [60.0, cum(a=30, b=3, c=1)]]                # clear again: 20 more
        win = imp.credit_windows(self.trial(samples), "blackhole")
        self.assertEqual(win["clear"][-1][1], 30)
        self.assertEqual(win["outage"][-1][1], 1)
        self.assertEqual(win["after"][-1][1], 3)
        self.assertLessEqual(gm.quantile(0.95, win["clear"]), 0.2)
        self.assertGreater(gm.quantile(0.95, win["after"]), 0.2)

    def test_without_samples_the_windows_are_unmeasured(self):
        t = types.SimpleNamespace(trial="blackhole", rows=[], records=[])
        self.assertEqual(imp.credit_windows(t, "blackhole"), {"clear": None, "outage": None, "after": None})

    def test_blackhole_is_judged_clear_of_the_outages(self):
        import configparser
        cp = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=(";", "#"))
        cp.read(os.path.join(os.path.dirname(os.path.abspath(__file__)), "validation", "gates.ini"))
        self.assertEqual(cp["workload:impaired"]["blackhole.credit_delay_clear_p95_s"], "lower 0.05")
        samples = [[10.0, cum()], [20.0, cum(a=40)], [37.0, cum(a=40, d=5)], [50.0, cum(a=80, d=5)]]
        t = self.trial(samples)
        m = imp.Impaired().phase_measures("blackhole", t.records, [], [t])
        self.assertLessEqual(m["credit_delay_clear_p95_s"], 0.2)            # the outage's long waits left out
        self.assertTrue(math.isinf(gm.quantile(1.0, imp.credit_windows(t, "blackhole")["outage"])) or
                        m["credit_delay_outage_p95_s"] >= 10)
        tails = imp.Impaired().block_tails([t], {"config": {"minimal_stage": 10}})
        self.assertEqual(len(tails["blackhole.credit_delay_clear_p95_s"][1]), 80)


class Sampler(unittest.TestCase):
    def test_each_endpoint_is_sampled_and_kept_as_a_record(self):
        cfg = ti.parse(ti.BASE)
        with tempfile.TemporaryDirectory() as d:
            c = ab.Campaign(cfg, d, ".", dry=False, stream=io.StringIO())
            c.measures_path = os.path.join(d, "measures.jsonl")
            started = []

            def start_job(host, script, what, tries=0):
                started.append((host, script))
                return {"rc": "/r", "out": "/o", "pgid": "/p"}, 0
            out = "C\t1000.5\t0.2\t3\nC\t1000.5\t+Inf\t4\nC\t1001.5\t0.2\t5\nC\t1001.5\t+Inf\t7\n"
            with mock.patch.object(c, "start_job", side_effect=start_job), \
                    mock.patch.object(c, "session", return_value=(0, "")), \
                    mock.patch.object(c, "wait_job", return_value=(0, out)):
                s = c.start_credit_sampler("b001-a-blackhole-s1-main", cfg.arms[0], [cfg.clients["a"]])
                c.stop_credit_sampler(ab.Slot(1, "a", 1, "main", 1, True, "blackhole"), cfg.arms[0], "blackhole", s)
            self.assertEqual(sorted(h for h, _ in started), ["c1", "gw"])
            self.assertTrue(all("queqiao_credit_delay_seconds_bucket" in sc and "/metrics" in sc for _, sc in started))
            recs = [json.loads(l) for l in open(c.measures_path)]
        self.assertEqual(sorted(r["end"] for r in recs), ["client-a", "gateway"])
        self.assertEqual(recs[0]["samples"], [[1000.5, {"0.2": 3.0, "+Inf": 4.0}], [1001.5, {"0.2": 5.0, "+Inf": 7.0}]])
        self.assertEqual((recs[0]["_phase"], recs[0]["_arm"], recs[0]["_slot"]), ("blackhole", "main", 1))


if __name__ == "__main__":
    unittest.main()

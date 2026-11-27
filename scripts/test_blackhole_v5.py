"""Gates v5 for the impaired blackhole (adopted after imp6, not re-judged):
one pooled transfer-failure rule over short downloads and background
transfers, and blackhole's own sequential cap of 12 blocks."""
import io
import json
import os
import sys
import tempfile
import types
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import ab_analyze as an
import ab_campaign as ab
import ab_workloads as w
import test_impair as ti

w.load_plugins()
imp = sys.modules["ab_workload_impaired"]
SEQ = ti.BASE.replace("blocks = 6\nsequential = true\nfirst_blocks = 4",
                      "blocks = 6\nsequential = true\nfirst_blocks = 4")


class CombinedTransfers(unittest.TestCase):
    def test_short_and_background_transfers_are_pooled_in_blackhole(self):
        rows = ([{"phase": "blackhole", "kind": "imp-fetch", "flow": f"w1r{k}", "complete": c, "curl_exit": e}
                 for k, c, e in ((1, "1", "0"), (2, "0", "56"))] +
                [{"phase": "blackhole", "kind": "wl-bgup", "curl_exit": e, "http": h, "flow": "u1"}
                 for e, h in (("0", "200"), ("7", "0"), ("28", "200"))])
        t = types.SimpleNamespace(rows=rows, records=[], trial="blackhole")
        ev = imp.Impaired().block_events([t], {})
        self.assertEqual(ev["blackhole.fetch_failures"], (1, 2))
        self.assertEqual(ev["blackhole.bg_transfer_failures"], (1, 2))
        self.assertEqual(ev["blackhole.transfer_failures"], (2, 4))

    def test_blackhole_is_judged_on_the_pooled_rule_only(self):
        wl = imp.Impaired()
        rules = {"*.fetch_failures": ("events", (0.02, 149)), "blackhole.transfer_failures": ("events", (0.02, 149))}
        keys = ["blackhole.fetch_failures", "blackhole.bg_transfer_failures", "blackhole.transfer_failures",
                "policed.fetch_failures"]
        judged, unruled = an.expand_rules(keys, rules, ["blackhole", "policed"], wl, {"config": {}})
        self.assertEqual(set(judged), {"blackhole.transfer_failures", "policed.fetch_failures"})
        self.assertEqual(unruled, [])                      # the other two are reported


class BlackholeCap(unittest.TestCase):
    def cfg(self):
        return ti.parse(ti.BASE)

    def test_blackhole_runs_to_twelve_and_the_others_to_six(self):
        cfg = self.cfg()
        self.assertEqual((cfg.type_sequential("blackhole").first, cfg.type_cap("blackhole")), (6, 12))
        self.assertEqual((cfg.type_sequential("policed").first, cfg.type_cap("policed")), (4, 6))
        self.assertEqual(cfg.type_sequential("blackhole").alpha, cfg.sequential.alpha)
        blocks = {}
        for s in ab.schedule(cfg):
            blocks.setdefault(s.trial, set()).add(s.block)
        self.assertEqual(max(blocks["blackhole"]), 12)
        self.assertEqual(max(blocks["policed"]), 6)
        self.assertEqual(max(blocks["slow-lossy"]), 6)

    def test_the_analyzer_judges_each_type_by_its_own_frozen_design(self):
        cfg = self.cfg()
        plan = {"sequential": cfg.sequential.__dict__,
                "sequential_by_type": {t: s.__dict__ for t, s in cfg.sequential_by_type.items()}}
        self.assertEqual(an.sequential_of(plan, "blackhole").cap, 12)
        self.assertEqual(an.sequential_of(plan, "blackhole").levels, cfg.sequential_by_type["blackhole"].levels)
        self.assertEqual(an.sequential_of(plan, "policed").cap, 6)
        self.assertEqual(an.sequential_of(plan).cap, 6)
        self.assertEqual(an.sequential_of({"sequential": None}, "blackhole"), None)

    def test_looks_follow_each_types_design(self):
        cfg = self.cfg()
        with tempfile.TemporaryDirectory() as d:
            c = ab.Campaign(cfg, os.path.join(d, "o"), ".", dry=False, stream=io.StringIO())
            looks = []
            for name in ("setup", "record_effective", "start_beside", "finish_beside", "teardown", "clean",
                         "reserve", "canonical_path_reasons", "check_production"):
                setattr(c, name, mock.Mock(return_value=[]))
            c.build = mock.Mock(return_value={})
            c.verify_teardown = mock.Mock(return_value=[])
            c.snapshot_all = mock.Mock(return_value={})
            c.run_block = mock.Mock()
            c.impair = mock.Mock(before="", leftovers=[])
            c.impair.preflight_reasons.return_value = []
            c.impair.describe.return_value = {}
            c.interim = mock.Mock(side_effect=lambda p, t, b: looks.append((t, b)) or (False, "continue"))
            with mock.patch.object(ab.time, "sleep"), mock.patch.object(ab.Host, "run", return_value=(0, "/home/u")), \
                    mock.patch.object(ab, "refusal_reasons", return_value=[]):
                c.run()
        self.assertEqual(sorted(b for t, b in looks if t == "blackhole"), [6, 7, 8, 9, 10, 11])
        self.assertEqual(sorted(b for t, b in looks if t == "policed"), [4, 5])

    def test_the_estimate_counts_blackholes_longer_cap(self):
        cfg = self.cfg()
        c = ab.Campaign(cfg, "/nonexistent", ".", dry=True, stream=io.StringIO())
        lo, hi = c.estimate()["a"]
        b = {t: c.block_seconds("a", t) for t in cfg.wl.trial_types(cfg)}
        self.assertAlmostEqual(hi - lo, (12 - 6) * b["blackhole"] + (6 - 4) * (b["policed"] + b["slow-lossy"]))


if __name__ == "__main__":
    unittest.main()

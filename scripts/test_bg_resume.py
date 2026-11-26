"""Gates v4 for the impaired blackhole (adopted after imp5, which is not
re-judged): resume per background flow kind, compared only where the flow
was alive at the outage's start in both arms; background transfer failures
as a pooled event rate; background clients that go on after a failed object
with a timeout above the longest outage."""
import configparser
import io
import json
import os
import sys
import threading
import types
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import ab_stats as st
import ab_workloads as w

w.load_plugins()
imp = sys.modules["ab_workload_impaired"]
HERE = os.path.dirname(os.path.abspath(__file__))
OUTS = [[30.0, 38.0], [70.0, 95.0]]


def prog(direction, samples):
    return {"kind": "bgprog", "dir": direction, "samples": [[a, b] for a, b in samples]}


class ResumeByKind(unittest.TestCase):
    def test_a_flow_alive_at_the_outage_is_timed_and_a_dead_one_is_not(self):
        down = prog("down", [(1, 0), (29, 10), (38, 10), (39.5, 20), (69, 30), (95, 30), (96, 40), (119, 50)])
        up = prog("up", [(1, 0), (20, 5), (21, 5)])          # ended before either outage
        self.assertEqual(imp.resume_by_kind(OUTS, [down, up], 120.0, "down"), [1.5, 1.0])
        self.assertEqual(imp.resume_by_kind(OUTS, [down, up], 120.0, "up"), [None, None])

    def test_a_flow_that_never_moves_again_is_censored_and_a_failure(self):
        up = prog("up", [(1, 0), (29, 10), (50, 10), (69, 10), (95, 10), (120, 10)])
        self.assertEqual(imp.resume_by_kind(OUTS, [up], 120.0, "up"), [32.0, 25.0])
        self.assertEqual(imp.never_resumed(OUTS, [up], 120.0), 2)

    def test_a_kind_dead_in_one_arm_leaves_the_block_out(self):
        # The block measure is NaN for the arm whose upload was dead; the
        # analyzer compares a paired_only measure only where both have it.
        self.assertIn("resume_upload_s", imp.Impaired.paired_only)
        rows = []
        recs = [dict(r, _phase="blackhole") for r in
                ({"kind": "impair", "fail": "", "outages": OUTS},
                 prog("down", [(1, 0), (29, 10), (39, 20), (96, 30), (119, 40)]))]
        t = types.SimpleNamespace(trial="blackhole", rows=rows, records=recs, m={})
        m = imp.Impaired().phase_measures("blackhole", recs, rows, [t])
        self.assertEqual(m["resume_download_s"], 1.0)
        self.assertNotEqual(m["resume_upload_s"], m["resume_upload_s"])     # NaN


class BgTransfers(unittest.TestCase):
    def test_failures_over_attempts_the_windows_cut_left_out(self):
        rows = [{"phase": "blackhole", "kind": k, "curl_exit": e, "http": h} for k, e, h in
                (("wl-bgdown", "0", "200"), ("wl-bgdown", "7", "000"), ("wl-bgdown", "28", "200"),
                 ("wl-bgup", "0", "200"), ("wl-bgup", "0", "200"), ("wl-bgup", "7", "0"))]
        recs = [{"_phase": "blackhole", "kind": "impair", "fail": "", "outages": OUTS},
                dict(prog("up", [(1, 0), (29, 10), (120, 10)]), _phase="blackhole")]
        t = types.SimpleNamespace(rows=rows, records=recs, trial="blackhole")
        # 5 attempted (the cut one left out), 2 failed, plus the upload that
        # never resumed after either outage: 2 more failures.
        self.assertEqual(imp.bg_transfers(t, "blackhole"), (4, 7))
        self.assertEqual(imp.Impaired().block_events([t], {})["blackhole.bg_transfer_failures"], (4, 7))

    def test_the_rule_is_frozen_and_equal_perfect_arms_can_pass(self):
        cp = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=(";", "#"))
        cp.read(os.path.join(HERE, "validation", "gates.ini"))
        sec = cp["workload:impaired"]
        self.assertEqual(sec["blackhole.bg_transfer_failures"].split(), ["events", "0.02", "149"])
        self.assertEqual(sec["blackhole.resume_download_s"], "lower 1.0")
        self.assertEqual(sec["blackhole.resume_upload_s"], "lower 1.0")
        self.assertNotIn("blackhole.inflight_ttfb_s", sec)
        self.assertEqual(st.events_decision([((0, 50), (0, 50))] * 5, 0.02, 149, 0.05, final=True)[0], "pass")
        # A flow that died before the outage counts against its own arm.
        self.assertEqual(st.events_decision([((0, 50), (6, 50))] * 5, 0.02, 149, 0.05, final=True)[0], "pass")
        self.assertEqual(st.events_decision([((6, 50), (0, 50))] * 5, 0.02, 149, 0.05, final=True)[0], "fail")


class Clients(unittest.TestCase):
    def test_the_blackhole_backgrounds_go_on_with_a_long_timeout(self):
        import test_impair as ti
        cfg = ti.parse(ti.BASE)
        bh = next(p for p in cfg.wl.phases(cfg, "a") if p.name == "blackhole")
        bg = {j.kind: j for j in bh.jobs if j.kind in ("wl-bgdown", "wl-bgup")}
        self.assertEqual(bg["wl-bgdown"].params["object_bytes"], imp.BG_DOWN_OBJECT)
        self.assertEqual(bg["wl-bgup"].params["object_bytes"], imp.BG_UP_OBJECT)
        self.assertGreater(imp.BG_IO_TIMEOUT["blackhole"], max(d for _, d in
                                                              sys.modules["ab_impair"].SCENARIOS["blackhole"].outages))
        pa = sys.modules["ab_workload_phase_a"]
        camp = types.SimpleNamespace(cfg=types.SimpleNamespace(gateway=types.SimpleNamespace(origin_port=1),
                                                               object_bytes=9, scenario_http_port=2,
                                                               scenario_tls=False),
                                     w=lambda h: "/W", stop_file=lambda c, a: "/W/run/stop")
        arm = types.SimpleNamespace(index=0)
        s = pa.bgup_script(camp, bg["wl-bgup"], arm, 120)
        self.assertIn("--io-timeout 60 --continue-after-failure", s)
        self.assertIn(f"--object-bytes {imp.BG_UP_OBJECT}", s)
        self.assertIn(f"/obj-{imp.BG_DOWN_OBJECT}", pa.bgdown_script(camp, bg["wl-bgdown"], arm, 120))
        # Other workloads are unchanged.
        plain = w.Job(bg["wl-bgup"].client, "wl-bgup", 1, load=False)
        self.assertNotIn("--continue-after-failure", pa.bgup_script(camp, plain, arm, 120))

    def test_wlclient_goes_on_after_a_failed_upload(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location("wlclient_bg", os.path.join(HERE, "validation", "wlclient.py"))
        wc = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(wc)
        out = io.StringIO()
        n = {"conns": 0}

        class Sock:
            def __init__(self, fail):
                self.fail = fail

            def settimeout(self, t):
                self.t = t

            def sendall(self, b):
                if self.fail and not b.startswith(b"POST"):
                    raise TimeoutError("timed out")

            def recv(self, k):
                return b"HTTP/1.1 200 OK\r\n\r\n"

            def close(self):
                pass

        def conn(args, timeout, deadline=None):
            n["conns"] += 1
            return Sock(fail=n["conns"] == 1)                 # the first object fails, the rest succeed
        args = wc.main.__globals__["argparse"].Namespace(flows=1, object_bytes=1000, duration=1.5, stop_file="",
                                                         io_timeout=60.0, continue_after_failure=True)
        with mock.patch.object(wc, "open_conn", side_effect=conn), mock.patch.object(wc.sys, "stdout", out), \
                mock.patch.object(wc, "sink_report", return_value=1000):
            wc.cmd_bgup(args)
        rows = [l.split("\t") for l in out.getvalue().splitlines() if not l.startswith("J\t")]
        exits = [r[-1] for r in rows]
        self.assertEqual(exits[0], "7")                       # the failure is recorded
        self.assertIn("0", exits[1:])                         # and the flow went on
        prog_ = [json.loads(l[2:]) for l in out.getvalue().splitlines() if l.startswith("J\t")][0]
        self.assertGreater(prog_["samples"][-1][1], 0)        # its timeline was not frozen at the failure


if __name__ == "__main__":
    unittest.main()

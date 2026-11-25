import collections
import contextlib
import datetime as dt
import importlib
import importlib.util
import io
import json
import os
import pathlib
import random
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))
ab = importlib.import_module("ab_campaign")
an = importlib.import_module("ab_analyze")
st = importlib.import_module("ab_stats")
wl = importlib.import_module("ab_workloads")
wl.load_plugins()
mn = sys.modules["ab_workload_minimal"]

SERVER = HERE / "validation" / "wlserver.py"
CLIENT = HERE / "validation" / "wlclient.py"
MINIMAL = HERE / "validation" / "minimal-campaign.example.ini"


def iso(t):
    return dt.datetime.fromtimestamp(t, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# --- metrics ------------------------------------------------------------------------

def call_record(n_audio=500, frames=150, up=40.0, down=60.0, lost_up=(), lost_down=()):
    """A call: audio packets every 20 ms, video frames of 10 packets; transit
    deltas `up` and `down` ms (on two clocks). Packets whose running index is
    in lost_up never reached the reflector; in lost_down, it got them but they
    never came back."""
    audio, video, k = [], [], 0

    def pkt(frame, t):
        nonlocal k
        k += 1
        if k in lost_up:
            return [frame, t, None, None]
        return [frame, t, up, None if k in lost_down else down]
    for i in range(n_audio):
        audio.append(pkt(i, i * 20.0))
    for f in range(1, frames + 1):
        for j in range(10):
            video.append(pkt(f, f * 33.3))
    return {"kind": "call", "fail": "", "audio": audio, "video": video, "seconds": 10}


class CallMetrics(unittest.TestCase):
    def test_a_clean_call(self):
        m = mn.call_metrics([call_record()])
        for d in ("up", "down"):
            self.assertEqual(m[f"audio_late_loss_{d}"], 0.0)
            self.assertEqual(m[f"frame_loss_{d}"], 0.0)
            self.assertEqual(m[f"freezes_per_min_{d}"], 0.0)
            self.assertAlmostEqual(m[f"lateness_p99_{d}_s"], 0.0)
        self.assertEqual(m["sessions_failed"], 0.0)

    def test_each_loss_is_charged_to_its_own_direction(self):
        # Audio packets 10..12 lost up, 20..22 lost down: one gap each way,
        # each counted only in its own direction (from the reflector's report).
        m = mn.call_metrics([call_record(lost_up={10, 11, 12}, lost_down={20, 21, 22})])
        self.assertAlmostEqual(m["audio_late_loss_up"], 3 / 500)
        self.assertAlmostEqual(m["audio_late_loss_down"], 3 / 497)     # over what reached the reflector
        self.assertAlmostEqual(m["audio_gaps_per_min_up"], 1 / (10 / 60))
        self.assertAlmostEqual(m["audio_gaps_per_min_down"], 1 / (10 / 60))
        self.assertEqual(m["frame_loss_up"], 0.0)
        self.assertEqual(m["frame_loss_down"], 0.0)

    def test_late_packets_are_bad_in_their_direction_only(self):
        rec = call_record()
        for p in rec["audio"][100:103]:
            p[3] += 300.0                          # late on the way down only
        m = mn.call_metrics([rec])
        self.assertEqual(m["audio_late_loss_up"], 0.0)
        self.assertAlmostEqual(m["audio_late_loss_down"], 3 / 500)
        self.assertAlmostEqual(m["late_loss_down"], 3 / 2000)

    def test_a_freeze_is_six_bad_frames_in_a_row(self):
        rec = call_record()
        for p in rec["video"]:
            if 20 <= p[0] < 26 and p[1] is not None:
                p[2] += 500.0                      # six frames late up
        m = mn.call_metrics([rec])
        self.assertAlmostEqual(m["frame_loss_up"], 6 / 150)
        self.assertAlmostEqual(m["freezes_per_min_up"], 1 / (10 / 60))
        self.assertEqual(m["freezes_per_min_down"], 0.0)

    def test_a_call_that_never_started_is_a_failure(self):
        self.assertEqual(mn.call_metrics([{"kind": "call", "fail": "x", "audio": [], "video": []}]),
                         {"sessions_failed": 1.0})


class BrowseMetrics(unittest.TestCase):
    def test_plt_from_the_first_connect(self):
        # Times are from the page's first CONNECT: the HTML's first byte at
        # 0.2 s includes its connection set-up.
        objs = [[0, 30000, 0.15, 0.2, 0.3, ""]] + [[i, 15000, 0.3, 0.5, 0.3 + i * 0.05, ""] for i in range(1, 41)]
        objs[5][5] = "OSError"
        m = mn.browse_metrics([{"kind": "browse", "fail": "", "objects": objs}])
        self.assertAlmostEqual(m["plt_p50_s"], 0.3 + 40 * 0.05)
        self.assertAlmostEqual(m["plt_p95_s"], 0.3 + 40 * 0.05)
        self.assertAlmostEqual(m["html_first_byte_p50_s"], 0.2)
        self.assertAlmostEqual(m["objects_failed"], 1 / 41)
        # A page that never opened counts as all failed.
        m = mn.browse_metrics([{"kind": "browse", "fail": "ConnectionRefusedError", "objects": []}])
        self.assertEqual(m["objects_failed"], 1.0)


def wake_conn(i, penalty=0.05, warm=0.02, late=False, end="", end_at=None, fail="", answered=True):
    """Connection i of 50 as wlclient records it, on the model's plan."""
    at, mode = mn.wake_plan()[i]
    rec = {"kind": "wake", "conn": i, "of": 50, "at": at, "mode": mode, "follow": 5, "fail": fail, "pushes": [],
           "sends": [], "end": end, "end_at": end_at, "sync_done": 0.4}
    extra = 10.0 if late else 0.0
    for j in range(6):
        d = (penalty + extra if j == 0 else warm)
        if mode == "push":
            srv = (1000.0 + at + j) * 1000.0
            rec["pushes"].append([j, srv, at + j + d])
        elif answered or j:
            rec["sends"].append([j, at + j, 2 * (d if j else penalty + 0.02) + (extra if j == 0 else 0.0)])
    return rec


class WakeSamples(unittest.TestCase):
    def test_the_plan_is_17_17_16_push_and_send_alternating(self):
        plan = mn.wake_plan()
        self.assertEqual(len(plan), 50)
        self.assertEqual([sum(1 for at, _ in plan if at == t) for t in (60, 300, 960)], [17, 17, 16])
        for t in (60, 300, 960):
            modes = [m for at, m in plan if at == t]
            self.assertEqual(modes[:4], ["push", "send", "push", "send"])

    def test_penalties_are_clock_free(self):
        s = mn.wake_samples([wake_conn(i, penalty=0.08) for i in range(50)])
        self.assertEqual(s["delivered_60"], [1.0] * 17)
        self.assertEqual(s["delivered_960"], [1.0] * 16)
        for x in s["push_penalty"]:
            self.assertAlmostEqual(x, 0.06, places=6)
        self.assertEqual(len(s["push_penalty"]) + len(s["send_penalty"]), 50)
        self.assertEqual(sum(s["silent"]), 0.0)

    def test_a_late_push_is_undelivered_and_a_silent_death(self):
        s = mn.wake_samples([wake_conn(0, late=True)])        # conn 0: push at 60 s
        self.assertEqual(s["delivered_60"], [0.0])
        self.assertEqual(s["silent"], [1.0])
        self.assertEqual(s["death_silent"], [1.0])
        # The same loss with an EOF seen before the wake is not silent.
        s = mn.wake_samples([wake_conn(0, late=True, end="EOF", end_at=59.0)])
        self.assertEqual(s["silent"], [0.0])
        self.assertEqual(s["death_eof"], [1.0])
        self.assertEqual(s["idle_eof"], [59.0 - 0.4])

    def test_an_unanswered_send_is_a_timeout_and_silent(self):
        s = mn.wake_samples([wake_conn(3, answered=False)])   # conn 3: send at 60 s
        self.assertEqual(s["delivered_60"], [0.0])
        self.assertEqual(s["death_timeout"], [1.0])
        self.assertEqual(s["silent"], [1.0])


# --- analysis: sample-based idle-wake, sequential minimal -------------------------------

def write_wake_campaign(root, cand_penalty=0.05, cand_late=(), main_late=()):
    os.makedirs(root)
    plan = {"main": "main", "workload": "idle-wake", "paths": ["a"], "config": {"duration": 90},
            "schedule": [{"block": 1, "path": "a", "slot": 1, "arm": "main"}, {"block": 1, "path": "a", "slot": 2, "arm": "cand"}],
            "arms": {"main": {"role": "main", "flags": [], "commit": "a" * 40},
                     "cand": {"role": "candidate", "flags": [], "commit": "b" * 40}}, "clients": {"a": {"small": True}}}
    rows, recs = [], []
    t = 1_780_000_000.0
    rng = random.Random(3)
    for slot, arm in ((1, "main"), (2, "cand")):
        for i in range(50):
            c = wake_conn(i, penalty=(cand_penalty if arm == "cand" else 0.05) + rng.uniform(0, 0.01),
                          late=i in (cand_late if arm == "cand" else main_late))
            c.update(_block=1, _path="a", _slot=slot, _arm=arm, _phase="wake", _client="a", _job=0, t0=t, t1=t + 1000)
            recs.append(c)
        rows.append([1, "a", slot, arm, "x", 1, 1, "wake", 0, "a", "wl-wake", "w0", iso(t), iso(t + 1000), 200, 50,
                     1000, 0, 1, 1000, f"{t:.3f}", f"{t + 1000:.3f}", ""])
    pathlib.Path(root, "plan.json").write_text(json.dumps(plan))
    pathlib.Path(root, "status.json").write_text(json.dumps({"status": "complete", "production": "unchanged"}))
    with open(os.path.join(root, "trials.tsv"), "w") as f:
        f.write("\t".join(ab.TRIAL_COLUMNS) + "\n")
        for r in rows:
            f.write("\t".join(str(x) for x in r) + "\n")
    with open(os.path.join(root, "measures.jsonl"), "w") as f:
        for r in recs:
            f.write(json.dumps(r) + "\n")


class IdleWakeGate(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.gates = an.load_gates()
        self.gates["resamples"] = 2000

    def test_equal_arms_pass_over_their_samples(self):
        root = os.path.join(self.tmp.name, "eq")
        write_wake_campaign(root)
        text, v = an.report(root, self.gates)
        for k in ("silent_deaths", "delivered_1m", "delivered_5m", "delivered_16m", "push_penalty_p50_s",
                  "send_penalty_p50_s"):
            self.assertEqual(v[("a", "cand", f"rule:wake.{k}")], "pass", k)
        self.assertEqual(v[("a", "cand", "overall")], "PASS", text)
        # p95, max, death modes and idle EOF times are reported, not gated.
        self.assertIn("wake.push_penalty_p95_s (report only)", text)
        self.assertIn("wake.deaths_silent (report only)", text)

    def test_slower_wakes_fail(self):
        root = os.path.join(self.tmp.name, "slow")
        write_wake_campaign(root, cand_penalty=0.20)
        _, v = an.report(root, self.gates)
        self.assertEqual(v[("a", "cand", "rule:wake.push_penalty_p50_s")], "**FAIL**")

    def test_missing_connections_leave_no_verdict(self):
        # Only 49 of the 50 planned connections' records survived.
        root = os.path.join(self.tmp.name, "missing")
        write_wake_campaign(root)
        recs = [json.loads(l) for l in pathlib.Path(root, "measures.jsonl").read_text().splitlines()]
        recs = [r for r in recs if not (r["_arm"] == "cand" and r["conn"] == 13)]
        pathlib.Path(root, "measures.jsonl").write_text("".join(json.dumps(r) + "\n" for r in recs))
        _, v = an.report(root, self.gates)
        self.assertEqual(v[("a", "cand", "rule:wake.delivered_1m")], "n/a")
        self.assertEqual(v[("a", "cand", "overall")], "INCOMPLETE")

    def test_one_undelivered_wake_at_one_minute_fails_the_hard_rule(self):
        root = os.path.join(self.tmp.name, "lost")
        write_wake_campaign(root, cand_late=(0,))
        _, v = an.report(root, self.gates)
        self.assertEqual(v[("a", "cand", "rule:wake.delivered_1m")], "**FAIL**")
        self.assertEqual(v[("a", "cand", "rule:wake.silent_deaths")], "**FAIL**")

    def test_at_sixteen_minutes_the_count_is_compared_with_mains(self):
        # Both arms lose the same two 16-minute wakes, each seen as an EOF
        # (a middlebox at 15 min): not a candidate regression.
        root = os.path.join(self.tmp.name, "both")
        write_wake_campaign(root)
        recs = [json.loads(l) for l in pathlib.Path(root, "measures.jsonl").read_text().splitlines()]
        for r in recs:
            if r["conn"] in (2, 5):                          # 960 s wakes
                r.update(pushes=[], sends=[], end="EOF", end_at=900.0)
        pathlib.Path(root, "measures.jsonl").write_text("".join(json.dumps(r) + "\n" for r in recs))
        _, v = an.report(root, self.gates)
        self.assertEqual(v[("a", "cand", "rule:wake.delivered_16m")], "pass")
        self.assertEqual(v[("a", "cand", "rule:wake.silent_deaths")], "pass")
        # One more lost on the candidate alone: fewer than main's.
        for r in recs:
            if r["_arm"] == "cand" and r["conn"] == 8:
                r.update(pushes=[], sends=[], end="EOF", end_at=900.0)
        pathlib.Path(root, "measures.jsonl").write_text("".join(json.dumps(r) + "\n" for r in recs))
        _, v = an.report(root, self.gates)
        self.assertEqual(v[("a", "cand", "rule:wake.delivered_16m")], "**FAIL**")


class SequentialAnalysis(unittest.TestCase):
    """The analyzer judges a sequential campaign at its stopping look."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def campaign(self, ratio, blocks, blocks_run, sequential=True):
        root = os.path.join(self.tmp.name, f"seq{ratio}-{blocks_run}")
        tests = importlib.import_module("test_ab_analyze")
        tests.write_campaign(root, cand_ratio=ratio, blocks=blocks, noise=0.01)
        plan = json.loads(pathlib.Path(root, "plan.json").read_text())
        if sequential:
            plan["sequential"] = st.Sequential().__dict__
        pathlib.Path(root, "plan.json").write_text(json.dumps(plan))
        status = json.loads(pathlib.Path(root, "status.json").read_text())
        status["blocks_run"] = blocks_run
        pathlib.Path(root, "status.json").write_text(json.dumps(status))
        return root

    def test_a_campaign_stopped_early_is_complete_and_judged_at_its_look(self):
        g = an.load_gates()
        g["resamples"] = 2000
        root = self.campaign(1.3, blocks=6, blocks_run=6)
        # The plan's schedule only holds the blocks run; the rest never ran.
        text, v = an.report(root, g)
        self.assertNotIn("missing trials", text)
        self.assertEqual(v[("p", "cand", "primary")], "**PASS**")

    def test_interim_decisions_continue_while_undecided(self):
        g = an.load_gates()
        g["resamples"] = 1000
        root = self.campaign(0.90, blocks=6, blocks_run=6)
        os.remove(os.path.join(root, "status.json"))
        dec = an.interim_decisions(root, g)
        # A candidate at the 0.90 margin is undecided after six blocks at
        # the first look's level: the gate's verdict is not determined.
        self.assertEqual(set(dec), {("p", "")})
        self.assertIn("continue", dec[("p", "")])
        self.assertFalse(st.overall_stop(dec[("p", "")]))


def unit_campaign(root, units_run, missing=()):
    """A minimal campaign's skeleton: per unit (block, path, type) four slots
    in the plan and, unless the unit is in `missing`, four trials' rows."""
    os.makedirs(root)
    sched, rows = [], []
    t = 1_780_000_000.0
    for b, p, ty in units_run:
        base = {"bulk": 0, "mixed-67": 4, "mixed-all": 8}[ty]
        for k, arm in enumerate(("main", "cand", "cand", "main"), start=1):
            sched.append({"block": b, "path": p, "slot": base + k, "arm": arm, "trial": ty})
            if (b, p, ty) not in missing:
                rows.append([b, p, base + k, arm, "x", 1 + (k in (2, 3) and arm == "cand" and k == 3 or k == 4),
                             int(k in (1, 2)), ty, 0, p, "bulk", "b1o1", iso(t), iso(t + 60), 200, 1, 60, 0, 1, 60,
                             f"{t:.3f}", f"{t + 60:.3f}", ty])
                t += 70
    plan = {"main": "main", "workload": "minimal", "paths": ["a"], "schedule": sched, "config": {"duration": 60},
            "sequential": st.Sequential().__dict__,
            "arms": {"main": {"role": "main", "flags": []}, "cand": {"role": "candidate", "flags": []}}}
    pathlib.Path(root, "plan.json").write_text(json.dumps(plan))
    pathlib.Path(root, "status.json").write_text(json.dumps(
        {"status": "complete", "production": "unchanged", "units_run": [list(u) for u in units_run]}))
    with open(os.path.join(root, "trials.tsv"), "w") as f:
        f.write("\t".join(ab.TRIAL_COLUMNS) + "\n")
        for r in rows:
            f.write("\t".join(str(x) for x in r) + "\n")


class TrialTypeUnits(unittest.TestCase):
    def test_a_type_that_stopped_early_planned_no_more_blocks(self):
        with tempfile.TemporaryDirectory() as d:
            root = os.path.join(d, "c")
            units = [(b, "a", "bulk") for b in range(1, 7)] + [(b, "a", "mixed-67") for b in range(1, 9)]
            unit_campaign(root, units)
            plan, trials = an.read_campaign(root)
            problems, kept = an.completeness(root, plan, trials)
            self.assertEqual(problems, [])
            self.assertEqual(len(kept), 4 * len(units))
            self.assertEqual({t.tag for t in kept.values() if t.block == 7}, {"b007-a-mixed-67"})

    def test_a_missing_unit_is_left_out_on_its_own(self):
        with tempfile.TemporaryDirectory() as d:
            root = os.path.join(d, "c")
            units = [(1, "a", "bulk"), (1, "a", "mixed-67")]
            unit_campaign(root, units, missing={(1, "a", "mixed-67")})
            plan, trials = an.read_campaign(root)
            problems, kept = an.completeness(root, plan, trials)
            self.assertTrue(any("missing trials" in p for p in problems), problems)
            self.assertEqual({t.trial for t in kept.values()}, {"bulk"})

    def test_block_extract_uses_only_the_trials_of_the_phases_type(self):
        # An arm's block holds trials of every type; a phase's evidence check
        # must look at its own type's trials only, not demand that the bulk
        # trials carry the LLM's and the terminal's records.
        m = wl.WORKLOADS["minimal"]()
        T = collections.namedtuple("T", "rows records trial")
        bulk = T([{"phase": "bulk", "kind": "bulk", "bytes": "100", "http": "200", "wall": "1"}], [], "bulk")
        recs = [{"kind": "llm", "_phase": "mixed-67"}, {"kind": "term", "_phase": "mixed-67"}]
        b67 = T([{"phase": "mixed-67", "kind": "wl-llm"}], recs, "mixed-67")
        empty = T([], [], "mixed-67")                      # its jobs wrote nothing at all
        with mock.patch.object(type(m), "phase_measures", lambda self_, ph, r, rows, trials: {"x": float(len(trials))}):
            out = m.block_extract([bulk, bulk, b67, b67], {})
            self.assertEqual(out["mixed-67.x"], 2.0)
            self.assertEqual(out["bulk.x"], 2.0)
            out = m.block_extract([bulk, bulk, b67, empty], {})
            self.assertNotEqual(out["mixed-67.x"], out["mixed-67.x"])     # one trial cannot stand for two


# --- the runner: trial types, sequential stopping, estimates, plans ------------------

BASE = """
[campaign]
name = mini
blocks = 12
sequential = true
first_blocks = 6
workload = minimal
[workload:minimal]
tls = false
[gateway]
host = gw
address = 192.0.2.1
[client:a]
host = c1
[arm:main]
role = main
binary_linux_amd64 = /bin/true
[arm:cand]
binary_linux_amd64 = /bin/true
[production:gw]
host = gw
paths = /var/lib/queqiao /usr/local/bin/queqiaod
[production:c1]
host = c1
"""


def parse(text):
    import configparser
    cp = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=(";", "#"))
    cp.read_string(text)
    return ab.parse_config(cp)


class Runner(unittest.TestCase):
    def run_campaign(self, decide, text=BASE):
        """decide(path, trial, blocks_of_that_type) -> decisions at a look."""
        with tempfile.TemporaryDirectory() as tmp:
            camp = ab.Campaign(parse(text), os.path.join(tmp, "out"), ".", dry=False, stream=io.StringIO())
            ok = {"gw": ab.parse_snapshot("listen-ok\nps-ok"), "c1": ab.parse_snapshot("listen-ok\nps-ok")}
            units, beside = [], []

            def run_block(self_, b, path, slots, writer, trial=""):
                units.append((b, trial))
                self.assertEqual({s.trial for s in slots}, {trial})
                self.assertEqual(len(slots), 4)

            def looks(campaign, gates, units_run):
                out = {}
                for b, p, ty in units_run:
                    out[(p, ty)] = decide(p, ty, sum(1 for u in units_run if (u[1], u[2]) == (p, ty)))
                return out
            with mock.patch.object(ab.Campaign, "snapshot_all", return_value=ok), \
                    mock.patch.object(ab.Campaign, "reserve"), mock.patch.object(ab.Campaign, "build", return_value={}), \
                    mock.patch.object(ab.Campaign, "canonical_path_reasons", return_value=[]), \
                    mock.patch.object(ab.Campaign, "setup"), mock.patch.object(ab.Campaign, "record_effective"), \
                    mock.patch.object(ab.Campaign, "run_block", run_block), \
                    mock.patch.object(ab.Campaign, "start_beside", lambda self_, b: beside.append(("start", len(units)))), \
                    mock.patch.object(ab.Campaign, "finish_beside", lambda self_, b: beside.append(("finish", len(units)))), \
                    mock.patch.object(ab.Campaign, "teardown"), mock.patch.object(ab.Campaign, "verify_teardown", return_value=[]), \
                    mock.patch.object(ab.Campaign, "clean", return_value=[]), \
                    mock.patch.object(ab.Host, "run", return_value=(0, "/home/u")), mock.patch.object(ab.time, "sleep"), \
                    mock.patch.object(an, "interim_decisions", side_effect=looks):
                rc = camp.run()
            status = json.loads(pathlib.Path(tmp, "out", "status.json").read_text())
            plan = json.loads(pathlib.Path(tmp, "out", "plan.json").read_text())
        return rc, units, status, plan, beside

    def test_types_run_round_robin_with_c_first_while_d_runs(self):
        rc, units, status, plan, beside = self.run_campaign(lambda p, ty, n: ["continue"])
        self.assertEqual(rc, 0)
        # D starts first, and its 17 minutes are covered by C's first two
        # blocks, back to back; then every type's block b before block b + 1.
        self.assertEqual(units[:6], [(1, "mixed-all"), (2, "mixed-all"), (1, "bulk"), (1, "mixed-67"),
                                     (2, "bulk"), (2, "mixed-67")])
        self.assertEqual(units[6:9], [(3, "bulk"), (3, "mixed-67"), (3, "mixed-all")])
        self.assertEqual(beside, [("start", 0), ("finish", 36)])
        self.assertEqual(len(units), 36)                     # undecided: every type to the cap
        self.assertEqual(status["blocks_run"], 12)
        self.assertEqual(status["stopped"], "")
        self.assertEqual(len(status["units_run"]), 36)

    def test_each_type_stops_on_its_own(self):
        # bulk is decided (pass) at its first look, mixed-67 fails at its
        # second, mixed-all never decides.
        def decide(p, ty, n):
            return {"bulk": ["pass", "pass"], "mixed-67": ["continue"] if n < 7 else ["fail", "continue"]}.get(ty, ["continue"])
        rc, units, status, plan, _ = self.run_campaign(decide)
        per = collections.Counter(t for _, t in units)
        self.assertEqual(per, {"bulk": 6, "mixed-67": 7, "mixed-all": 12})
        self.assertIn("a/bulk", status["stopped_types"])
        self.assertIn("after block 7", status["stopped_types"]["a/mixed-67"])
        self.assertEqual(plan["sequential"]["first"], 6)
        self.assertEqual(len(plan["sequential"]["levels"]), 7)      # frozen before the first block

    def test_without_d_the_types_simply_round_robin(self):
        text = BASE.replace("tls = false", "tls = false\ntrials = bulk, mixed-67\nidle_wake = false")
        rc, units, status, plan, beside = self.run_campaign(lambda p, ty, n: ["pass"], text)
        self.assertEqual(units, [(b, t) for b in range(1, 7) for t in ("bulk", "mixed-67")])
        self.assertEqual(beside, [])

    def test_slots_are_unique_within_a_block_and_cold_per_type(self):
        cfg = parse(BASE)
        slots = ab.schedule(cfg)
        b1 = [s for s in slots if s.block == 1]
        self.assertEqual(sorted(s.slot for s in b1), list(range(1, 13)))
        for ty in ("bulk", "mixed-67", "mixed-all"):
            us = [s for s in b1 if s.trial == ty]
            self.assertEqual(sorted(s.arm for s in us if s.cold), ["cand", "main"])
            self.assertIn([s.arm for s in us], (["main", "cand", "cand", "main"], ["cand", "main", "main", "cand"]))

    def test_estimates_and_plan_in_the_dry_run(self):
        out = io.StringIO()
        forbidden = mock.Mock(side_effect=AssertionError("dry run must not run a command"))
        with mock.patch.object(subprocess, "run", forbidden), contextlib.redirect_stdout(out):
            self.assertEqual(ab.main(["--config", str(MINIMAL), "--dry-run"]), 0)
        text = out.getvalue()
        self.assertIn("# sequential: looks after blocks 6..12, one-sided alpha 0.05 spent by the Pocock-type function", text)
        self.assertIn("6: 0.0310", text)
        # Section 15.4's budget: one block each of A, B, C and DC is about 26 min.
        self.assertIn("bulk 5 min, mixed-67 7 min, mixed-all 9 min", text)
        self.assertIn("one block each of A, B, C and DC is about 26 min a path", text)
        self.assertIn("2.6 h at 6 blocks", text)
        self.assertRegex(text, r"# estimated wall time on path-a: \d+ min \(stopping at the first look\) to \d+ min \(cap\)")
        for cell in ("bulk: bulk x4", "mixed-67: wl-llm x1", "mixed-67: wl-term x1", "mixed-67: wl-bgdown x2",
                     "mixed-all: wl-call x1", "mixed-all: wl-browse x1", "mixed-all: wl-bgup x1"):
            self.assertIn(cell, text)
        self.assertIn("23487 (wlserver call reflector)", text)
        self.assertIn("23488 (wlserver wake)", text)
        self.assertIn("gateway main-wake", text)
        # D starts before the first block, on its own instances, and the
        # blocks wait for its syncs (the ready marker).
        d_start = text.index("idle-wake: starting its sessions beside the blocks")
        self.assertLess(d_start, text.index("mixed-all: wl-call x1"))
        self.assertRegex(text, r"wlclient\.py wake .*--conns 50 --sync 200000 --wake-times 60,300,960 --follow 5 .*--ready-file")
        camp = ab.Campaign(ab.load_config(str(MINIMAL)), "<o>", ".", dry=True, stream=io.StringIO())
        lo, hi = camp.estimate()["path-a"]
        self.assertLess(lo, hi)
        self.assertLess(lo, 3 * 3600)     # inside the profile's budget when it stops at the first look

    def test_the_trial_settings_are_the_models(self):
        cfg = parse(BASE)
        ph = {p.name: p for p in cfg.wl.phases(cfg, "a")}
        self.assertEqual([j.n for j in ph["bulk"].jobs], [4])
        self.assertEqual(ph["bulk"].duration, 60)
        jobs = {j.kind: j for j in ph["mixed-67"].jobs}
        self.assertEqual(jobs["wl-bgdown"].n, 2)
        self.assertEqual(jobs["wl-llm"].params["sequence"], "loop")
        self.assertEqual((jobs["wl-llm"].params["upload"], jobs["wl-llm"].params["turns"]), (100_000, 5))
        self.assertEqual((jobs["wl-term"].params["seconds"], jobs["wl-term"].params["burst_at"],
                          jobs["wl-term"].params["burst_bytes"]), (90, 40, 300_000))
        jobs = {j.kind: j for j in ph["mixed-all"].jobs}
        # Backgrounds start 5 s ahead; then the call for the whole 120 s, the
        # page at 20 s (h2), the LLM call at 50 s, keystrokes without bursts.
        self.assertEqual(jobs["wl-call"].params, {"seconds": 120, "delay": 5})
        self.assertEqual((jobs["wl-browse"].params["delay"], jobs["wl-browse"].params["mode"]), (25, "h2"))
        self.assertEqual((jobs["wl-llm"].params["delay"], jobs["wl-llm"].params["sequence"]), (55, "single"))
        self.assertTrue(jobs["wl-term"].params["canary"])
        self.assertEqual((jobs["wl-bgdown"].n, jobs["wl-bgup"].n), (1, 1))
        with self.assertRaises(ab.ConfigError):
            parse(BASE.replace("tls = false", "tls = false\nmixed67_seconds = 60"))

    def test_d_is_fixed_and_runs_beside_the_blocks(self):
        cfg = parse(BASE)
        b = cfg.wl.beside(cfg)
        self.assertEqual((b.name, b.workload), ("idle-wake", "idle-wake"))
        self.assertEqual(b.phase.jobs[0].params, {"conns": 50, "sync": 200_000, "follow": 5})
        self.assertEqual([a.index for a in cfg.beside_arms()], [2, 3])
        # D alone as a campaign would run its arms one after the other.
        with self.assertRaises(ab.ConfigError):
            parse(BASE.replace("workload = minimal", "workload = idle-wake").replace("sequential = true", "sequential = false"))
        # On a small client host D's instances need an explicit opt-in.
        small = BASE.replace("[client:a]\nhost = c1", "[client:a]\nhost = c1\nsmall = true")
        with self.assertRaises(ab.ConfigError):
            parse(small)
        parse(small.replace("tls = false", "tls = false\nidle_wake_on_small = true"))

    def test_sequential_needs_a_rule_judged_workload(self):
        # bulk is judged by the primary gate and [secondary] bounds, which the
        # stopping looks do not decide: no sequential mode for it.
        with self.assertRaises(ab.ConfigError):
            parse(BASE.replace("workload = minimal", "workload = bulk"))

    def test_bad_sequential_settings_are_refused_at_load(self):
        for bad in ("interim_alpha = 0.001", "spending = haybittle", "alpha = 1.5", "alpha = 0"):
            with self.assertRaises(ab.ConfigError, msg=bad):
                parse(BASE.replace("first_blocks = 6", f"first_blocks = 6\n{bad}"))
        self.assertEqual(parse(BASE.replace("first_blocks = 6", "first_blocks = 6\nspending = bonferroni")).sequential.levels,
                         [0.05 / 7] * 7)

    def test_sequential_is_refused_for_the_dc_gate(self):
        text = (HERE / "validation" / "dc-campaign.example.ini").read_text().replace("blocks = 12", "blocks = 12\nsequential = true")
        with self.assertRaises(ab.ConfigError):
            parse(text)


class ProductionPaths(unittest.TestCase):
    def test_paths_are_compared_as_resolved_on_the_host(self):
        # A flag path that reaches a production directory through a symlink,
        # and one through '..', are refused; an unrelated path is not.
        with tempfile.TemporaryDirectory() as d:
            d = os.path.realpath(d)
            prod = os.path.join(d, "prod")
            os.makedirs(prod)
            os.symlink(prod, os.path.join(d, "alias"))
            os.makedirs(os.path.join(d, "home"))
            text = BASE.replace("host = gw\naddress", "host = local\naddress").replace("[production:gw]\nhost = gw\npaths = /var/lib/queqiao /usr/local/bin/queqiaod",
                                                                                     f"[production:gw]\nhost = local\npaths = {prod}")
            text = text.replace("[arm:cand]\nbinary_linux_amd64 = /bin/true",
                                f"[arm:cand]\nbinary_linux_amd64 = /bin/true\ngateway_flags = --cert {d}/alias/x.pem --key {d}/other/k.pem")
            cfg = parse(text)
            camp = ab.Campaign(cfg, "/o", ".", dry=False, stream=io.StringIO())
            camp.hosts["local"].home = os.path.join(d, "home")
            r = camp.canonical_path_reasons()
            self.assertEqual(len(r), 1, r)
            self.assertIn(f"{d}/alias/x.pem resolves to {prod}/x.pem", r[0])
            camp.hosts["local"].home = os.path.join(prod, "sub", "..")
            r = camp.canonical_path_reasons()
            self.assertTrue(any("work directory" in x for x in r), r)

    def test_flags_may_not_name_production_paths(self):
        for flags in ("--log-dir=/var/lib/queqiao/x", "--cert /usr/local/bin/queqiaod"):
            with self.assertRaises(ab.ConfigError) as cm:
                parse(BASE.replace("[arm:cand]\nbinary_linux_amd64 = /bin/true",
                                   f"[arm:cand]\nbinary_linux_amd64 = /bin/true\nflags = {flags}"))
            self.assertIn("production path", str(cm.exception))

    def test_a_work_directory_inside_a_production_path_is_refused(self):
        cfg = parse(BASE)
        snaps = {"gw": ab.parse_snapshot("listen-ok\nps-ok"), "c1": ab.parse_snapshot("listen-ok\nps-ok")}
        snaps["gw"]["home"] = "/var/lib/queqiao"
        r = ab.refusal_reasons(cfg, snaps)
        self.assertTrue(any("inside production path" in x for x in r), r)
        snaps["gw"]["home"] = "/home/u"
        self.assertEqual(ab.refusal_reasons(cfg, snaps), [])


class RoundOneFixes(unittest.TestCase):
    def test_a_workload_without_a_primary_gate_can_stop(self):
        with tempfile.TemporaryDirectory() as d:
            pathlib.Path(d, "plan.json").write_text(json.dumps({"workload": "minimal"}))
            v = {("a", "cand", "primary"): "n/a", ("a", "cand", "rule:bulk.goodput_bps"): "pass",
                 ("a", "cand", "overall"): "INCOMPLETE"}
            with mock.patch.object(an, "report", return_value=("", v)):
                dec = an.interim_decisions(d, an.load_gates())
        self.assertEqual(dec, {("a", "bulk"): ["pass"]})
        self.assertTrue(st.overall_stop(dec[("a", "bulk")]))

    def test_a_failed_session_is_evidence_and_a_failure(self):
        m = wl.WORKLOADS["minimal"]()
        recs = [{"kind": k, "fail": "ConnectionRefusedError"} for k in ("llm", "term", "call", "browse")]
        self.assertTrue(m.has_evidence("mixed-all", recs))

    def test_an_early_stop_leaves_undecided_gates_undecided(self):
        tests = importlib.import_module("test_ab_analyze")
        with tempfile.TemporaryDirectory() as d:
            root = os.path.join(d, "c")
            tests.write_campaign(root, cand_ratio=0.92, blocks=6, noise=0.05)
            plan = json.loads(pathlib.Path(root, "plan.json").read_text())
            plan["sequential"] = st.Sequential().__dict__
            pathlib.Path(root, "plan.json").write_text(json.dumps(plan))
            status = json.loads(pathlib.Path(root, "status.json").read_text())
            status.update(blocks_run=6, stopped="after block 6: 1 fail")
            pathlib.Path(root, "status.json").write_text(json.dumps(status))
            g = an.load_gates()
            g["resamples"] = 1000
            text, v = an.report(root, g)
        self.assertEqual(v[("p", "cand", "primary")], "undecided")
        self.assertIn("alpha 0.031", text)               # the bounds shown are the look's

    def test_the_minimal_dc_example_is_six_blocks(self):
        cfg = ab.load_config(str(HERE / "validation" / "dc-minimal.example.ini"))
        self.assertEqual(cfg.blocks, 6)
        lo, hi = ab.Campaign(cfg, "<o>", ".", dry=True, stream=io.StringIO()).estimate()["dc"]
        self.assertLess(hi, 2 * 3600)


class Flip(unittest.TestCase):
    def test_optional_campaigns_do_not_change_the_matrix(self):
        pa_tests = importlib.import_module("test_phase_a")
        with tempfile.TemporaryDirectory() as d:
            good = os.path.join(d, "good")
            pa_tests.write_llm_campaign(good)
            extra = os.path.join(d, "extra")
            pa_tests.write_llm_campaign(extra)
            plan = json.loads(pathlib.Path(extra, "plan.json").read_text())
            plan.update(workload="term", paths=["z"])
            plan["arms"]["cand"]["commit"] = "9" * 40           # even another build
            pathlib.Path(extra, "plan.json").write_text(json.dumps(plan))
            g = an.load_gates()
            g["resamples"] = 500
            g["workloads"]["llm"] = {k: v for k, v in g["workloads"]["llm"].items() if k != "*.class_interactive"}
            llm = wl.WORKLOADS["llm"]
            with mock.patch.object(an, "FLIP_WORKLOADS", ("llm",)), \
                    mock.patch.object(llm, "report_only", llm.report_only + ("class_interactive",)):
                text, ok = an.flip_report([good, extra], g)
        self.assertTrue(ok, text)

    def test_the_minimal_profile_is_the_default_requirement(self):
        self.assertEqual(set(an.FLIP_PROFILES["minimal"]), {"minimal", "idle-wake", "dc"})
        self.assertEqual(an.FLIP_PROFILES["minimal"]["minimal"], ("bulk", "mixed-67", "mixed-all"))
        # The long-tail workloads are not required by default.
        for optional in ("llm", "term", "mixed"):
            self.assertNotIn(optional, an.FLIP_PROFILES["minimal"])
            self.assertIn(optional, an.FLIP_PROFILES["full"])


class PooledTails(unittest.TestCase):
    """Section 15.4: a tail percentile with too few samples per arm in a
    block (20 for a p95) is tested over every block's samples pooled, by a
    bootstrap that resamples blocks."""

    def test_the_pooled_bound_resamples_blocks(self):
        rng = random.Random(2)
        c = [[1.0 + rng.random() * 0.1 for _ in range(5)] for _ in range(12)]
        m = [[1.0 + rng.random() * 0.1 for _ in range(5)] for _ in range(12)]
        obs, lo, hi = st.pooled_quantile_bounds(c, m, 0.95, lambda a, b: 1.15 * b + 0.05 - a, 0.05, 2000, 1)
        self.assertGreater(lo, 0)
        self.assertLessEqual(lo, obs)
        self.assertLessEqual(obs, hi)
        slow = [[x + 1.0 for x in blk] for blk in c]
        obs, lo, hi = st.pooled_quantile_bounds(slow, m, 0.95, lambda a, b: 1.15 * b + 0.05 - a, 0.05, 2000, 1)
        self.assertLess(hi, 0)

    def write(self, root, cand_slow=0.0):
        pa_tests = importlib.import_module("test_phase_a")
        pa_tests.write_llm_campaign(root)
        recs = [json.loads(l) for l in pathlib.Path(root, "measures.jsonl").read_text().splitlines()]
        for r in recs:
            if r["_arm"] == "cand":
                r["ttft"] += cand_slow
        pathlib.Path(root, "measures.jsonl").write_text("".join(json.dumps(r) + "\n" for r in recs))

    def report(self, root):
        def tails(self_, trials, plan):
            out = {}
            for ph in ("single-100k-none", "single-100k-down4"):
                xs = [r["ttft"] - r["T"] for t in trials for r in t.records if r.get("_phase") == ph]
                out[f"{ph}.ttft_net_p95_s"] = (0.95, xs)
            return out
        g = an.load_gates()
        g["resamples"] = 1000
        llm = wl.WORKLOADS["llm"]
        with mock.patch.object(llm, "block_tails", tails):
            return an.report(root, g)

    def test_a_p95_with_ten_samples_per_block_is_pooled(self):
        with tempfile.TemporaryDirectory() as d:
            root = os.path.join(d, "eq")
            self.write(root)
            text, v = self.report(root)
            self.assertIn("(pooled, 120 samples)", text)
            self.assertEqual(v[("a", "cand", "rule:single-100k-none.ttft_net_p95_s")], "pass")
            root = os.path.join(d, "slow")
            self.write(root, cand_slow=0.3)
            text, v = self.report(root)
            self.assertEqual(v[("a", "cand", "rule:single-100k-none.ttft_net_p95_s")], "**FAIL**")


class ClassCorrectness(unittest.TestCase):
    def flow(self, up, inter, bulk):
        return {"msg": "remote flow complete", "bytes_from_client": up, "bytes_to_client": inter + bulk,
                "data_bytes_new_coded": 0, "data_bytes_new_stream": 0, "data_bytes_interactive_coded": inter,
                "data_bytes_interactive_stream": 0, "data_bytes_bulk_coded": 0, "data_bytes_bulk_stream": bulk}

    def test_the_llm_flow_is_the_one_that_carried_the_prompt(self):
        T = collections.namedtuple("T", "flow_records")
        bg = self.flow(500, 0, 50_000_000)                   # a download: not the LLM's
        t1 = T([bg, self.flow(600_000, 99_500, 500)])
        t2 = T([bg, self.flow(100_500, 100_000, 0)])
        self.assertAlmostEqual(mn.class_interactive([t1, t2], 100_000), 199_500 / 200_000)
        # A trial without the LLM's record, or without class telemetry, is unmeasured.
        self.assertNotEqual(mn.class_interactive([t1, T([bg])], 100_000), mn.class_interactive([t1, T([bg])], 100_000))
        bare = {k: v for k, v in self.flow(100_500, 1, 0).items() if not k.startswith("data_bytes")}
        x = mn.class_interactive([T([bare])], 100_000)
        self.assertNotEqual(x, x)


class DCAtMinimalSize(unittest.TestCase):
    def cfg(self, extra=""):
        text = (HERE / "validation" / "dc-minimal.example.ini").read_text()
        import configparser
        cp = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=(";", "#"))
        cp.read_string(text + extra)
        return ab.parse_config(cp)

    def test_w4_first_then_the_sustained_phase(self):
        cfg = self.cfg()
        ph = cfg.wl.phases(cfg, "dc")
        self.assertEqual([p.name for p in ph], ["requests", "sustained"])
        self.assertEqual({j.kind for j in ph[0].jobs}, {"dc-rpc", "probe"})
        self.assertEqual(cfg.dc.rpc_requests, 10)
        script = wl.GENERATORS["dc-rpc"].script(ab.Campaign(cfg, "<o>", ".", dry=True, stream=io.StringIO()),
                                                 ph[0].jobs[0], cfg.arms[0], 30)
        self.assertIn("--count 10 --spacing 2 ", script)
        self.assertIn("--asr-up 355000 --asr-down 200 --tts-up 300 --tts-down 300000", script)
        self.assertEqual([e.tag for e in cfg.wl.endpoints(cfg)], ["wlserver"])
        # One block of four 60 s trials and the restarts: about 5 minutes.
        camp = ab.Campaign(cfg, "<o>", ".", dry=True, stream=io.StringIO())
        self.assertAlmostEqual(camp.block_seconds("dc") / 60, 5, delta=0.5)

    def test_calibration_runs_in_the_first_block_only(self):
        cfg = self.cfg()
        self.assertEqual(cfg.dc.calibrate, "first")
        camp = mock.Mock(cfg=cfg)
        for b in (1, 2, 3):
            cfg.wl.after_block(camp, b, "dc", f"b{b:03d}-dc", None)
        self.assertEqual({c.args[0].block for c in camp.run_phase.call_args_list}, {1})
        self.assertEqual(camp.run_phase.call_count, 3)          # one solo run per loaded leaf


class BesideSession(unittest.TestCase):
    def test_d_is_written_as_a_campaign_of_its_own(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "out")
            os.makedirs(out)
            cfg = parse(BASE)
            camp = ab.Campaign(cfg, out, ".", dry=False, stream=io.StringIO())
            pathlib.Path(out, "plan.json").write_text(json.dumps({"workload": "minimal", "main": "main",
                                                                  "arms": {"main": {"role": "main", "flags": []}, "cand": {"role": "candidate", "flags": []}}, "paths": ["a"]}))
            beside = cfg.wl.beside(cfg)
            recs = [dict(wake_conn(i), t0=1.0, t1=2.0) for i in range(50)]
            outp = "\n".join("J\t" + json.dumps(r) for r in recs) + "\n1.000\t2.000\tw0\t200\t50\t1.0\t0\n"
            camp.beside_slots = []
            for n, arm in enumerate(cfg.arms, start=1):
                job = wl.Job(cfg.clients["a"], "wl-wake", 1, params=dict(beside.phase.jobs[0].params))
                camp.beside_slots.append((ab.Slot(1, "a", n, arm.name, 1, True, ""), arm, job))
            camp.beside_results = {("a", "main"): (1.0, 2.0, 0, outp), ("a", "cand"): (1.0, 2.0, 0, outp)}
            with mock.patch.object(ab.Campaign, "stop_client"), mock.patch.object(ab.Campaign, "stop"), \
                    mock.patch.object(ab.Campaign, "collect"):
                camp.finish_beside(beside)
            sub = pathlib.Path(out, "idle-wake")
            plan = json.loads((sub / "plan.json").read_text())
            self.assertEqual((plan["workload"], plan["sequential"]), ("idle-wake", None))
            self.assertEqual(len((sub / "measures.jsonl").read_text().splitlines()), 100)
            pathlib.Path(sub, "status.json").write_text(json.dumps({"status": "complete", "production": "unchanged"}))
            g = an.load_gates()
            g["resamples"] = 500
            _, v = an.report(str(sub), g)
        self.assertEqual(v[("a", "cand", "overall")], "PASS")


# --- end to end on this machine -----------------------------------------------------

class Socks5UDP(threading.Thread):
    """SOCKS5 relay for the tests: CONNECT and UDP ASSOCIATE, on loopback."""

    def __init__(self):
        super().__init__(daemon=True)
        self.srv = socket.socket()
        self.srv.bind(("127.0.0.1", 0))
        self.srv.listen(64)
        self.port = self.srv.getsockname()[1]

    def run(self):
        while True:
            try:
                c, _ = self.srv.accept()
            except OSError:
                return
            threading.Thread(target=self.serve, args=(c,), daemon=True).start()

    def serve(self, c):
        try:
            c.recv(3)
            c.sendall(b"\x05\x00")
            head = c.recv(4)
            if head[3] == 3:
                host = c.recv(c.recv(1)[0]).decode()
            else:
                host = socket.inet_ntoa(c.recv(4))
            port = struct.unpack(">H", c.recv(2))[0]
            if head[1] == 3:
                self.associate(c)
                return
            u = socket.create_connection((host, port))
            c.sendall(b"\x05\x00\x00\x01" + b"\x00" * 6)
        except OSError:
            c.close()
            return

        def pump(a, b):
            try:
                while True:
                    x = a.recv(65536)
                    if not x:
                        break
                    b.sendall(x)
            except OSError:
                pass
            for s_ in (a, b):
                try:
                    s_.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
        threading.Thread(target=pump, args=(c, u), daemon=True).start()
        pump(u, c)

    def associate(self, c):
        relay = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        relay.bind(("127.0.0.1", 0))
        c.sendall(b"\x05\x00\x00\x01" + socket.inet_aton("127.0.0.1") + struct.pack(">H", relay.getsockname()[1]))
        client = [None]
        relay.settimeout(0.2)
        while True:
            try:
                data, peer = relay.recvfrom(65535)
            except socket.timeout:
                try:
                    c.setblocking(False)
                    if c.recv(1) == b"":
                        return
                except BlockingIOError:
                    pass
                except OSError:
                    return
                finally:
                    c.setblocking(True)
                continue
            if peer[0] == "127.0.0.1" and (client[0] is None or peer == client[0]) and data[:3] == b"\x00\x00\x00":
                client[0] = peer
                dst = (socket.inet_ntoa(data[4:8]), struct.unpack(">H", data[8:10])[0])
                relay.sendto(data[10:], dst)
            elif client[0]:
                relay.sendto(b"\x00\x00\x00\x01" + socket.inet_aton(peer[0]) + struct.pack(">H", peer[1]) + data, client[0])


def mn_page_sizes(seed):
    spec = importlib.util.spec_from_file_location("wlclient_for_tests", CLIENT)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m.page_objects(seed)


def free_port(kind=socket.SOCK_STREAM):
    s = socket.socket(socket.AF_INET, kind)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


class EndToEnd(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.http, self.term, self.udp, self.wake = free_port(), free_port(), free_port(socket.SOCK_DGRAM), free_port()
        self.server = subprocess.Popen([sys.executable, str(SERVER), "--http", str(self.http), "--term", str(self.term),
                                        "--udp", str(self.udp), "--wake", str(self.wake), "--dir", self.tmp.name,
                                        "--no-tls"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(100):
            try:
                socket.create_connection(("127.0.0.1", self.wake), timeout=0.2).close()
                break
            except OSError:
                time.sleep(0.05)
        self.socks = Socks5UDP()
        self.socks.start()

    def tearDown(self):
        self.server.kill()
        self.server.wait()
        self.socks.srv.close()
        self.tmp.cleanup()

    def client(self, *args, timeout=60):
        p = subprocess.run([sys.executable, str(CLIENT), args[0], "--socks", f"127.0.0.1:{self.socks.port}", "--no-tls"]
                           + list(args[1:]), capture_output=True, text=True, timeout=timeout)
        self.assertEqual(p.returncode, 0, p.stderr)
        return [json.loads(l[2:]) for l in p.stdout.splitlines() if l.startswith("J\t")]

    def test_a_call_through_udp_associate(self):
        recs = self.client("call", "--target", f"127.0.0.1:{self.udp}", "--duration", "2",
                           "--report-target", f"127.0.0.1:{self.http}")
        c = recs[0]
        self.assertEqual(c["fail"], "")
        self.assertGreater(len(c["audio"]), 80)
        self.assertGreater(len(c["video"]), 500)
        # Every packet has its up transit from the reflector's own report.
        self.assertTrue(all(p[2] is not None for p in c["audio"] + c["video"]))
        m = mn.call_metrics(recs)
        self.assertLess(m["late_loss_up"], 0.05)
        self.assertLess(m["frame_loss_up"], 0.2)
        self.assertLess(m["frame_loss_down"], 0.2)
        # Without the report the call is a failure, not a guess.
        recs = self.client("call", "--target", f"127.0.0.1:{self.udp}", "--duration", "1",
                           "--report-target", f"127.0.0.1:{free_port()}")
        self.assertIn("no reflector report", recs[0]["fail"])

    def test_a_browsing_burst(self):
        for mode in ("h1", "h2"):
            recs = self.client("browse", "--target", f"127.0.0.1:{self.http}", "--seed", "3", "--mode", mode)
            p = recs[0]
            self.assertEqual((p["fail"], p["mode"]), ("", mode))
            self.assertEqual(sorted(o[0] for o in p["objects"]), list(range(41)))
            self.assertEqual(sum(o[1] for o in p["objects"]), 30_000 + sum(mn_page_sizes(3)))
            m = mn.browse_metrics(recs)
            self.assertEqual(m["objects_failed"], 0.0, mode)
            self.assertGreater(m["plt_p50_s"], 0)

    def test_transactional_requests(self):
        p = subprocess.run([sys.executable, str(CLIENT), "rpc", "--socks", f"127.0.0.1:{self.socks.port}", "--no-tls",
                            "--target", f"127.0.0.1:{self.http}", "--count", "4", "--spacing", "0.2"],
                           capture_output=True, text=True, timeout=60)
        rows = [l.split("\t") for l in p.stdout.splitlines()]
        self.assertEqual([r[2] for r in rows], ["w1r1", "w2r1", "w3r1", "w4r1"])
        self.assertEqual([r[3] for r in rows], ["200"] * 4)
        # ASR and TTS alternate: 200 bytes back, then 300 KB.
        self.assertEqual([r[4] for r in rows], ["200", "300000", "200", "300000"])
        self.assertEqual({r[6] for r in rows}, {"0"})

    def test_idle_connections_wake(self):
        ready = os.path.join(self.tmp.name, "ready")
        recs = self.client("wake", "--target", f"127.0.0.1:{self.wake}", "--conns", "4", "--sync", "20000",
                           "--wake-times", "1,2", "--follow", "2", "--duration", "20", "--ready-file", ready)
        self.assertTrue(os.path.exists(ready))
        self.assertEqual(len(recs), 4)
        self.assertEqual([(r["at"], r["mode"]) for r in sorted(recs, key=lambda r: r["conn"])],
                         [(1, "push"), (2, "push"), (1, "send"), (2, "send")])
        s = mn.wake_samples(recs)
        self.assertEqual(s["delivered_1"] + s["delivered_2"], [1.0] * 4)
        self.assertEqual(len(s["push_penalty"]), 2)
        self.assertEqual(len(s["send_penalty"]), 2)
        self.assertEqual(s["silent"], [0.0] * 4)


if __name__ == "__main__":
    unittest.main()

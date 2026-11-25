import contextlib
import datetime as dt
import importlib
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
WAKE = HERE / "validation" / "idle-wake.example.ini"


def iso(t):
    return dt.datetime.fromtimestamp(t, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# --- metrics ------------------------------------------------------------------------

def call_record(n_audio=500, frames=150, up=40.0, down=60.0, late_every=0, drop_down=(), drop_up=0, count=None):
    """A call: audio packets every 20 ms, video frames of 10 packets; transit
    deltas `up` and `down` ms (on two clocks), every late_every-th packet
    200 ms late up; packets whose index is in drop_down never came back."""
    audio, video, k = [], [], 0
    for i in range(n_audio):
        k += 1
        late = 200.0 if late_every and k % late_every == 0 else 0.0
        audio.append([i, i * 20.0] + ([] if k in drop_down else [up + late, down]))
    for f in range(1, frames + 1):
        for j in range(10):
            k += 1
            video.append([f, f * 33.3] + ([] if k in drop_down else [up, down]))
    sent = len(audio) + len(video)
    back = sent - len(drop_down)
    return {"kind": "call", "fail": "", "audio": audio, "video": video, "seconds": 10,
            "count": count if count is not None else back + 0}


class CallMetrics(unittest.TestCase):
    def test_a_clean_call(self):
        m = mn.call_metrics([call_record()])
        self.assertEqual(m["late_loss_up"], 0.0)
        self.assertEqual(m["late_loss_down"], 0.0)
        self.assertEqual(m["frame_loss"], 0.0)
        self.assertEqual(m["freezes_per_min"], 0.0)
        self.assertAlmostEqual(m["lateness_p99_up_s"], 0.0)

    def test_losses_split_by_the_reflectors_count(self):
        # 10 packets never came back; the reflector counted all but 4 of them:
        # 4 were lost up, 6 down.
        drops = set(range(600, 610))
        sent = 500 + 1500
        m = mn.call_metrics([call_record(drop_down=drops, count=sent - 4)])
        self.assertAlmostEqual(m["late_loss_up"], 4 / sent)
        self.assertAlmostEqual(m["late_loss_down"], 6 / (sent - 4))   # over what the reflector sent back

    def test_late_audio_and_gaps(self):
        # Three consecutive late audio packets make one audible gap.
        rec = call_record()
        for p in rec["audio"][100:103]:
            p[2] += 300.0
        m = mn.call_metrics([rec])
        self.assertAlmostEqual(m["audio_late_loss"], 3 / 500)
        self.assertAlmostEqual(m["audio_gaps_per_min"], 1 / (10 / 60))

    def test_a_freeze_is_six_bad_frames_in_a_row(self):
        rec = call_record()
        bad = [p for p in rec["video"] if 20 <= p[0] < 26]
        for p in bad:
            del p[2:]
        m = mn.call_metrics([rec])
        self.assertAlmostEqual(m["frame_loss"], 6 / 150)
        self.assertAlmostEqual(m["freezes_per_min"], 1 / (10 / 60))


class BrowseMetrics(unittest.TestCase):
    def test_plt_html_and_failures(self):
        objs = [[0, 30000, 0.0, 0.2, 0.3, ""]] + [[i, 15000, 0.3, 0.5, 0.3 + i * 0.05, ""] for i in range(1, 41)]
        objs[5][5] = "OSError"
        m = mn.browse_metrics([{"kind": "browse", "fail": "", "objects": objs}])
        self.assertAlmostEqual(m["plt_p50_s"], 0.3 + 40 * 0.05)
        self.assertAlmostEqual(m["html_first_byte_p50_s"], 0.2)
        self.assertAlmostEqual(m["objects_failed"], 1 / 41)
        # A page that never opened counts as all failed.
        m = mn.browse_metrics([{"kind": "browse", "fail": "ConnectionRefusedError", "objects": []}])
        self.assertEqual(m["objects_failed"], 1.0)


class WakeSamples(unittest.TestCase):
    def conn(self, wake_delay=0.05, warm=0.02, late=False, end="", fail=""):
        pushes = []
        for g, at in enumerate((60, 300, 960)):
            base = g * 6
            for j in range(6):
                srv = (1000.0 + at + j) * 1000.0
                delay = (wake_delay if j == 0 else warm) + (10.0 if late and g == 2 and j == 0 else 0.0)
                pushes.append([base + j, srv, at + j + delay])
        sends = []
        for g, at in enumerate((180, 600)):
            sends += [[g * 3 + 1, at, 0.30], [g * 3 + 2, at + 1, 0.20], [g * 3 + 3, at + 2, 0.21]]
        return {"kind": "wake", "fail": fail, "pushes": pushes, "sends": sends, "wakes": [60, 300, 960],
                "follow": 5, "end": end}

    def test_penalties_are_clock_free(self):
        s = mn.wake_samples([self.conn(wake_delay=0.08, warm=0.02)])
        self.assertEqual(s["delivered"], [1.0, 1.0, 1.0])
        for x in s["push_penalty"]:
            self.assertAlmostEqual(x, 0.06, places=6)
        self.assertEqual([round(x, 6) for x in s["send_penalty"]], [0.1, 0.1])
        self.assertEqual(s["silent_deaths"], [0.0])

    def test_a_late_wake_is_undelivered_and_a_silent_death(self):
        s = mn.wake_samples([self.conn(late=True)])
        self.assertEqual(s["delivered"], [1.0, 1.0, 0.0])
        self.assertEqual(s["silent_deaths"], [1.0])
        # The same loss with an EOF seen locally is not silent.
        s = mn.wake_samples([self.conn(late=True, end="EOF at 900.0s")])
        self.assertEqual(s["silent_deaths"], [0.0])


# --- analysis: sample-based idle-wake, sequential minimal -------------------------------

def write_wake_campaign(root, cand_penalty=0.05, cand_late=False):
    os.makedirs(root)
    plan = {"main": "main", "workload": "idle-wake", "paths": ["a"], "config": {"duration": 90},
            "schedule": [{"block": 1, "path": "a", "slot": 1, "arm": "main"}, {"block": 1, "path": "a", "slot": 2, "arm": "cand"}],
            "arms": {"main": {"role": "main", "flags": [], "commit": "a" * 40},
                     "cand": {"role": "candidate", "flags": [], "commit": "b" * 40}}, "clients": {"a": {"small": True}}}
    rows, recs = [], []
    t = 1_780_000_000.0
    rng = random.Random(3)
    w_ = WakeSamples()
    for slot, arm in ((1, "main"), (2, "cand")):
        for i in range(50):
            c = w_.conn(wake_delay=(cand_penalty if arm == "cand" else 0.05) + rng.uniform(0, 0.01), warm=0.02,
                        late=(cand_late and arm == "cand" and i == 7))
            c.update(conn=i, of=50, _block=1, _path="a", _slot=slot, _arm=arm, _phase="wake", _client="a", _job=0, t0=t, t1=t + 1000)
            recs.append(c)
        rows.append([1, "a", slot, arm, "x", 1, 1, "wake", 0, "a", "wl-wake", "w0", iso(t), iso(t + 1000), 200, 50,
                     1000, 0, 1, 1000, f"{t:.3f}", f"{t + 1000:.3f}"])
        t += 1100
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
        for k in ("delivered", "silent_deaths", "push_penalty_p50_s", "push_penalty_p99_s",
                  "send_penalty_p50_s", "send_penalty_p99_s"):
            self.assertEqual(v[("a", "cand", f"rule:wake.{k}")], "pass", k)
        self.assertEqual(v[("a", "cand", "overall")], "PASS")

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
        self.assertEqual(v[("a", "cand", "rule:wake.delivered")], "n/a")
        self.assertEqual(v[("a", "cand", "overall")], "INCOMPLETE")

    def test_one_undelivered_wake_fails_the_hard_rule(self):
        root = os.path.join(self.tmp.name, "lost")
        write_wake_campaign(root, cand_late=True)
        _, v = an.report(root, self.gates)
        self.assertEqual(v[("a", "cand", "rule:wake.delivered")], "**FAIL**")
        self.assertEqual(v[("a", "cand", "rule:wake.silent_deaths")], "**FAIL**")


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
            plan["sequential"] = {"first": 6, "cap": 12, "alpha": 0.05, "interim_alpha": 0.001}
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
        root = self.campaign(0.92, blocks=6, blocks_run=6)
        os.remove(os.path.join(root, "status.json"))
        dec = an.interim_decisions(root, g)
        # A candidate near the 0.90 margin is undecided after six blocks at
        # the interim alpha: the gate's verdict is not determined, so it goes on.
        self.assertIn("continue", dec)
        self.assertFalse(st.overall_stop(dec))


# --- the runner: sequential stopping, estimates, plans -------------------------------

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
    def run_campaign(self, decisions):
        with tempfile.TemporaryDirectory() as tmp:
            camp = ab.Campaign(parse(BASE), os.path.join(tmp, "out"), ".", dry=False, stream=io.StringIO())
            ok = {"gw": ab.parse_snapshot("listen-ok\nps-ok"), "c1": ab.parse_snapshot("listen-ok\nps-ok")}
            blocks = []
            with mock.patch.object(ab.Campaign, "snapshot_all", return_value=ok), \
                    mock.patch.object(ab.Campaign, "reserve"), mock.patch.object(ab.Campaign, "build", return_value={}), \
                    mock.patch.object(ab.Campaign, "canonical_path_reasons", return_value=[]), \
                    mock.patch.object(ab.Campaign, "setup"), mock.patch.object(ab.Campaign, "record_effective"), \
                    mock.patch.object(ab.Campaign, "run_block", lambda self_, b, *a: blocks.append(b)), \
                    mock.patch.object(ab.Campaign, "teardown"), mock.patch.object(ab.Campaign, "verify_teardown", return_value=[]), \
                    mock.patch.object(ab.Campaign, "clean", return_value=[]), \
                    mock.patch.object(ab.Host, "run", return_value=(0, "/home/u")), mock.patch.object(ab.time, "sleep"), \
                    mock.patch.object(an, "interim_decisions", side_effect=decisions):
                rc = camp.run()
            status = json.loads(pathlib.Path(tmp, "out", "status.json").read_text())
            plan = json.loads(pathlib.Path(tmp, "out", "plan.json").read_text())
        return rc, blocks, status, plan

    def test_stops_once_the_gate_is_decided(self):
        looks = iter([["pass", "continue"], ["pass", "pass"]])
        rc, blocks, status, plan = self.run_campaign(lambda *a: next(looks))
        self.assertEqual(rc, 0)
        self.assertEqual(blocks, [1, 2, 3, 4, 5, 6, 7])           # looks after 6 and 7
        self.assertEqual(status["blocks_run"], 7)
        self.assertIn("after block 7", status["stopped"])
        self.assertIn("wall_seconds", status)
        self.assertEqual(plan["sequential"]["first"], 6)

    def test_a_decided_failure_stops_too(self):
        rc, blocks, status, _ = self.run_campaign(lambda *a: ["fail", "continue"])
        self.assertEqual(blocks, [1, 2, 3, 4, 5, 6])

    def test_runs_to_the_cap_while_undecided(self):
        rc, blocks, status, _ = self.run_campaign(lambda *a: ["continue"])
        self.assertEqual(blocks, list(range(1, 13)))
        self.assertEqual(status["blocks_run"], 12)
        self.assertEqual(status["stopped"], "")

    def test_estimates_and_plan_in_the_dry_run(self):
        out = io.StringIO()
        forbidden = mock.Mock(side_effect=AssertionError("dry run must not run a command"))
        with mock.patch.object(subprocess, "run", forbidden), contextlib.redirect_stdout(out):
            self.assertEqual(ab.main(["--config", str(MINIMAL), "--dry-run"]), 0)
        text = out.getvalue()
        self.assertIn("# sequential: looks after blocks 6..12", text)
        self.assertRegex(text, r"# estimated wall time on path-a: \d+ min \(stopping at the first look\) to \d+ min \(cap\)")
        for cell in ("bulk: bulk x4", "mixed-67: wl-llm x1", "mixed-67: wl-term x1", "mixed-67: wl-bgdown x2",
                     "mixed-all: wl-call x1", "mixed-all: wl-browse x1", "mixed-all: wl-bgup x1"):
            self.assertIn(cell, text)
        self.assertIn("23487 (wlserver call reflector)", text)
        camp = ab.Campaign(ab.load_config(str(MINIMAL)), "<o>", ".", dry=True, stream=io.StringIO())
        lo, hi = camp.estimate()["path-a"]
        self.assertLess(lo, hi)
        self.assertLess(lo, 3 * 3600)     # inside the profile's budget when it stops at the first look

    def test_idle_wake_runs_one_session_per_arm(self):
        cfg = ab.load_config(str(WAKE))
        slots = ab.schedule(cfg)
        self.assertEqual(sorted(s.arm for s in slots), ["cand", "main"])

    def test_sequential_needs_a_rule_judged_workload(self):
        # bulk is judged by the primary gate and [secondary] bounds, which the
        # stopping looks do not decide: no sequential mode for it.
        with self.assertRaises(ab.ConfigError):
            parse(BASE.replace("workload = minimal", "workload = bulk"))

    def test_bad_alphas_are_refused_at_load(self):
        for bad in ("interim_alpha = 0", "interim_alpha = -0.1", "interim_alpha = 0.5", "alpha = 1.5"):
            with self.assertRaises(ab.ConfigError, msg=bad):
                parse(BASE.replace("first_blocks = 6", f"first_blocks = 6\n{bad}"))

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
        self.assertEqual(dec, ["pass"])
        self.assertTrue(st.overall_stop(dec))

    def test_a_failed_session_is_evidence_and_a_failure(self):
        m = wl.WORKLOADS["minimal"]()
        recs = [{"kind": k, "fail": "ConnectionRefusedError"} for k in ("llm", "term", "call", "browse")]
        self.assertTrue(m.has_evidence("mixed-all", recs))
        self.assertEqual(mn.call_metrics([{"kind": "call", "fail": "x", "audio": [], "video": []}]), {"call_failures": 1.0})

    def test_send_trios_survive_a_lost_acknowledgement(self):
        c = {"kind": "wake", "fail": "", "pushes": [], "wakes": [], "follow": 5, "end": "",
             "sends": [[1, 180, 0.30], [3, 182, 0.20], [4, 600, 0.50], [5, 601, 0.21], [6, 602, 0.20]]}
        self.assertEqual([round(x, 6) for x in mn.wake_samples([c])["send_penalty"]], [0.1, 0.3])

    def test_an_early_stop_leaves_undecided_gates_undecided(self):
        tests = importlib.import_module("test_ab_analyze")
        with tempfile.TemporaryDirectory() as d:
            root = os.path.join(d, "c")
            tests.write_campaign(root, cand_ratio=0.92, blocks=6, noise=0.05)
            plan = json.loads(pathlib.Path(root, "plan.json").read_text())
            plan["sequential"] = {"first": 6, "cap": 12, "alpha": 0.05, "interim_alpha": 0.001}
            pathlib.Path(root, "plan.json").write_text(json.dumps(plan))
            status = json.loads(pathlib.Path(root, "status.json").read_text())
            status.update(blocks_run=6, stopped="after block 6: 1 fail")
            pathlib.Path(root, "status.json").write_text(json.dumps(status))
            g = an.load_gates()
            g["resamples"] = 1000
            text, v = an.report(root, g)
        self.assertEqual(v[("p", "cand", "primary")], "undecided")
        self.assertIn("alpha 0.001", text)               # the bounds shown are the look's

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
        recs = self.client("call", "--target", f"127.0.0.1:{self.udp}", "--duration", "2")
        c = recs[0]
        self.assertEqual(c["fail"], "")
        self.assertGreater(len(c["audio"]), 80)
        self.assertGreater(len(c["video"]), 500)
        self.assertGreater(c["count"], 0)
        m = mn.call_metrics(recs)
        self.assertLess(m["late_loss_up"], 0.05)
        self.assertLess(m["frame_loss"], 0.2)

    def test_a_browsing_burst(self):
        recs = self.client("browse", "--target", f"127.0.0.1:{self.http}", "--seed", "3")
        p = recs[0]
        self.assertEqual(p["fail"], "")
        self.assertEqual(len(p["objects"]), 41)
        m = mn.browse_metrics(recs)
        self.assertEqual(m["objects_failed"], 0.0)
        self.assertGreater(m["plt_p50_s"], 0)

    def test_idle_connections_wake(self):
        recs = self.client("wake", "--target", f"127.0.0.1:{self.wake}", "--conns", "3", "--sync", "20000",
                           "--wakes", "1,3", "--sends", "2", "--follow", "2", "--duration", "20")
        self.assertEqual(len(recs), 3)
        s = mn.wake_samples(recs)
        self.assertEqual(s["delivered"], [1.0] * 6)
        self.assertEqual(len(s["push_penalty"]), 6)
        self.assertEqual(len(s["send_penalty"]), 3)
        self.assertEqual(s["silent_deaths"], [0.0] * 3)


if __name__ == "__main__":
    unittest.main()

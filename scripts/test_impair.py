"""The network impairment ([impair], ab_impair) and the impaired workload."""
import io
import os
import sys
import tempfile
import types
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import ab_campaign as ab
import ab_impair as ai
import ab_workloads as w

w.load_plugins()
imp = sys.modules["ab_workload_impaired"]

BASE = """
[campaign]
name = imp
blocks = 6
sequential = true
first_blocks = 4
workload = impaired
[workload:impaired]
tls = false
idle_wake = true
idle_wake_on_small = true
[impair]
host = c1
[gateway]
host = gw
address = 192.0.2.1
port_base = 23471
[relay]
host = rl
address = 198.51.100.7
platform = linux/amd64
[client:a]
host = c1
small = true
[arm:main]
role = main
binary_linux_amd64 = /bin/true
[arm:s7]
binary_linux_amd64 = /bin/true
[arm:s8]
binary_linux_amd64 = /bin/true
[production:gw]
host = gw
ports = 22
[production:c1]
host = c1
ports = 22 12541
[production:rl]
host = rl
ports = 22 12541
"""


def parse(text):
    import configparser
    cp = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=(";", "#"))
    cp.read_string(text)
    return ab.parse_config(cp)


class FakeHost:
    """Answers scripts by substring; records what ran."""

    def __init__(self, answers):
        self.answers = answers
        self.ran = []
        self.dest = "c1"
        self.home = "/home/t"

    def run(self, script, timeout=120, what=""):
        self.ran.append((what, script))
        for key, ans in self.answers:
            if key in script or key in what:
                return ans() if callable(ans) else ans
        return 0, ""


def campaign(cfg, host):
    c = ab.Campaign(cfg, "/nonexistent", ".", dry=False, stream=io.StringIO())
    c.hosts["c1"] = host
    c.impair.dev = "eth0"
    c.impair.before = "== eth0\nqdisc fq_codel 0: dev eth0 root\n"
    return c


class Config(unittest.TestCase):
    def test_the_impaired_workload_needs_impair_and_impair_needs_it(self):
        with self.assertRaisesRegex(ab.ConfigError, "needs an \\[impair\\] section"):
            parse(BASE.replace("[impair]\nhost = c1\n", ""))
        with self.assertRaisesRegex(ab.ConfigError, "no impairment scenarios"):
            parse(BASE.replace("workload = impaired", "workload = minimal").replace("[workload:impaired]",
                                                                                  "[workload:minimal]"))

    def test_filters_cover_the_arm_ports_and_d_its_own(self):
        cfg = parse(BASE)
        c = ab.Campaign(cfg, "/nonexistent", ".", dry=True, stream=io.StringIO())
        self.assertEqual(c.impair.ports, {"b": [23471, 23472, 23473], "w": [23474, 23475, 23476]})
        self.assertEqual(c.impair.remote, "198.51.100.7")          # what clients dial: the relay
        self.assertEqual(c.impair.remote_host(), "rl")
        self.assertEqual(c.impair.config_reasons(), [])
        self.assertEqual(cfg.wl.trials_per_arm, 1)
        self.assertEqual(cfg.wl.impairment(cfg, "blackhole"), "blackhole")
        self.assertEqual(cfg.wl.beside(cfg).impair, "idle-wake-lossy")

    def test_a_filter_port_declared_production_anywhere_is_refused(self):
        cfg = parse(BASE.replace("[production:rl]\nhost = rl\nports = 22 12541", "[production:rl]\nhost = rl\nports = 22 23473"))
        c = ab.Campaign(cfg, "/nonexistent", ".", dry=True, stream=io.StringIO())
        self.assertTrue(any("23473" in r and "production" in r for r in c.impair.config_reasons()))
        c.impair.dev = "eth0"
        with self.assertRaisesRegex(ai.ImpairError, "production"):
            c.impair.apply("b", "blackhole", 100)

    def test_the_remote_must_be_an_ipv4_literal_and_the_host_the_paths(self):
        cfg = parse(BASE.replace("address = 198.51.100.7", "address = relay.example"))
        c = ab.Campaign(cfg, "/nonexistent", ".", dry=True, stream=io.StringIO())
        self.assertTrue(any("IPv4 literal" in r for r in c.impair.config_reasons()))
        cfg = parse(BASE.replace("[impair]\nhost = c1", "[impair]\nhost = gw"))
        c = ab.Campaign(cfg, "/nonexistent", ".", dry=True, stream=io.StringIO())
        self.assertTrue(any("not the client host" in r for r in c.impair.config_reasons()))

    def test_ephemeral_ranges(self):
        self.assertEqual(ai.ephemeral_reasons([23471], "h", "32768\t60999\n"), [])
        self.assertTrue(ai.ephemeral_reasons([23471], "h", "10000 60999"))
        self.assertTrue(ai.ephemeral_reasons([23471], "h", ""))       # unreadable: refused


class Scenarios(unittest.TestCase):
    def test_frozen_scenarios(self):
        s = ai.SCENARIOS
        self.assertEqual(s["slow-lossy"].up.netem(),
                         "delay 125ms 14ms distribution normal loss gemodel 1% 30% 100% 0% rate 5000kbit limit 131")
        # Gilbert-Elliott: p / (p + r) of packets in the bad state, all lost.
        self.assertAlmostEqual(0.01 / (0.01 + 0.30), 0.032, places=3)
        self.assertIn("loss random 100%", s["blackhole"].down.netem(drop=True))
        self.assertNotIn("loss", s["blackhole"].down.netem())
        self.assertEqual(s["blackhole"].outages, ((30, 8), (70, 25)))
        self.assertEqual((s["policed"].police_kbit, s["policed"].police_burst), (4000, 12000))
        self.assertEqual(s["idle-wake-lossy"].up.netem(), "delay 100ms loss random 2% limit 10000")

    def test_filters_match_only_the_remote_and_its_ports(self):
        cfg = parse(BASE)
        c = ab.Campaign(cfg, "/nonexistent", ".", dry=True, stream=io.StringIO())
        c.impair.dev = "eth0"
        script = c.impair.apply_script("b", ai.SCENARIOS["policed"], 100)
        filters = [l for l in script.splitlines() if "tc filter add" in l]
        self.assertEqual(len(filters), 4)                    # udp and tcp, egress and ingress
        for f in filters:
            self.assertRegex(f, r"(dst_ip 198\.51\.100\.7 dst_port 23471-23473|src_ip 198\.51\.100\.7 src_port 23471-23473)")
            self.assertIn("police rate 4000kbit burst 12000 conform-exceed drop/pipe", f)
        # The interface's root qdisc is never replaced: only clsact hooks.
        self.assertNotIn("dev eth0 root", script)
        self.assertIn("tc qdisc add dev eth0 clsact", script)
        # A failed step removes what was applied.
        self.assertIn('run() { "$@" || { echo "IMPAIR-FAIL: $*"; bash $R/impair-remove-b.sh; exit 1; }; }', script)
        self.assertIn("modprobe ifb numifbs=0", script)

    def test_the_remove_script_touches_only_its_own(self):
        cfg = parse(BASE)
        c = ab.Campaign(cfg, "/nonexistent", ".", dry=True, stream=io.StringIO())
        c.impair.dev = "eth0"
        r = c.impair.remove_script("w")
        self.assertIn("tc filter del dev eth0 egress pref 49111", r)
        self.assertNotIn("49101", r)
        self.assertIn("impair-clsact", r)            # only a clsact this campaign added, once empty


class Actions(unittest.TestCase):
    def test_remove_checks_the_snapshot_and_reports_a_watchdog(self):
        cfg = parse(BASE)
        state = ["== eth0\nqdisc fq_codel 0: dev eth0 root\n"]
        host = FakeHost([("impair: apply", (0, "IMPAIR-APPLIED\n")),
                         ("impair: remove", lambda: (0, out[0])), ("read the tc state", lambda: (0, state[0]))])
        out = ["IMPAIR-REMOVED\n"]
        c = campaign(cfg, host)
        c.impair.apply("b", "slow-lossy", 90)
        self.assertIsNone(c.impair.remove("b"))
        c.impair.apply("b", "slow-lossy", 90)
        out[0] = "WATCHDOG-FIRED 1795660000\nIMPAIR-REMOVED\n"
        self.assertEqual(c.impair.remove("b"), 1795660000.0)
        # Left behind: loud.
        c.impair.apply("b", "slow-lossy", 90)
        out[0] = "IMPAIR-LEFT device qqimp-bu\n"
        with self.assertRaisesRegex(ai.ImpairError, "left state behind"):
            c.impair.remove("b")
        # Removed, but the state is not the snapshot: loud.
        c.impair.apply("b", "slow-lossy", 90)
        out[0] = "IMPAIR-REMOVED\n"
        state[0] = "== eth0\nqdisc fq_codel 0: dev eth0 root\nqdisc clsact ffff: dev eth0\n"
        with self.assertRaisesRegex(ai.ImpairError, "differs from the snapshot"):
            c.impair.remove("b")

    def test_a_failed_apply_raises(self):
        cfg = parse(BASE)
        host = FakeHost([("impair: apply", (1, "IMPAIR-FAIL: tc qdisc add dev eth0 clsact\n"))])
        c = campaign(cfg, host)
        with self.assertRaisesRegex(ai.ImpairError, "applying blackhole failed"):
            c.impair.apply("b", "blackhole", 120)
        self.assertEqual(c.impair.active, {})

    def test_every_trial_is_impaired_and_the_impairment_removed_even_when_it_fails(self):
        cfg = parse(BASE)
        c = ab.Campaign(cfg, "/nonexistent", ".", dry=False, stream=io.StringIO())
        calls = []
        c.impair = mock.Mock()
        c.impair.apply.side_effect = lambda s, sc, secs: calls.append(("apply", s, sc))
        c.impair.remove.side_effect = lambda s: calls.append(("remove", s)) or None
        for name in ("stop", "start_gateway", "start_client", "stop_client", "scrape", "scrape_client", "collect"):
            setattr(c, name, mock.Mock())
        c.run_phase = mock.Mock(side_effect=RuntimeError("job host gone"))
        slots = [s for s in ab.schedule(cfg) if s.block == 1 and s.trial == "policed"]
        with mock.patch.object(ab.time, "sleep"):
            with self.assertRaises(RuntimeError):
                c.run_block(1, "a", slots, lambda row: None, "policed")
        self.assertEqual(calls, [("apply", "b", "policed"), ("remove", "b")])

    def test_a_watchdog_that_fired_voids_the_block(self):
        cfg = parse(BASE)
        c = ab.Campaign(cfg, "/nonexistent", ".", dry=False, stream=io.StringIO())
        c.impair = mock.Mock()
        c.impair.remove.return_value = 1795660000.0
        for name in ("stop", "start_gateway", "start_client", "stop_client", "scrape", "scrape_client", "collect",
                     "run_phase"):
            setattr(c, name, mock.Mock())
        slots = [s for s in ab.schedule(cfg) if s.block == 1 and s.trial == "slow-lossy"]
        with mock.patch.object(ab.time, "sleep"):
            c.run_block(1, "a", slots, lambda row: None, "slow-lossy")
        self.assertEqual({(j["kind"], j["exit"]) for j in c.job_failures}, {("impairment", ab.JOB_KILLED_BY_RUNNER)})

    def test_a_failed_outage_job_is_the_control_plane(self):
        cfg = parse(BASE)
        c = ab.Campaign(cfg, "/nonexistent", ".", dry=False, stream=io.StringIO())
        slot = ab.Slot(1, "a", 1, "s8", 1, True, "blackhole")
        job = w.Job(cfg.clients["a"], "impair-outages", 1, params={"scenario": "blackhole"})
        c.record_job(slot, cfg.arms[2], w.Phase("blackhole", 120, []), 0, job, w.GENERATORS["impair-outages"],
                     (0.0, 1.0, 1, "impairment: the outage could not be applied"), lambda r: None, None, 120)
        self.assertEqual([j["exit"] for j in c.job_failures], [ab.JOB_KILLED_BY_RUNNER])
        self.assertEqual(c.workload_failures, [])

    def test_teardown_removes_every_set_before_any_process(self):
        cfg = parse(BASE)
        c = ab.Campaign(cfg, "/nonexistent", ".", dry=False, stream=io.StringIO())
        order = []
        c.impair = mock.Mock()
        c.impair.teardown.side_effect = lambda: order.append("impair") or []
        c.impair.leftovers = []
        c.reserved = ["c1"]
        c.hosts["c1"].run = mock.Mock(side_effect=lambda *a, **k: order.append("processes") or (0, ""))
        c.teardown()
        self.assertEqual(order[0], "impair")


class Measures(unittest.TestCase):
    def test_recovery_is_the_first_probe_completed_after_each_outage(self):
        probes = [{"http": "200", "curl_exit": "0", "t_end": t} for t in ("10.0", "38.9", "40.2", "96.5")]
        probes.append({"http": "000", "curl_exit": "28", "t_end": "38.5"})
        rec = imp.recoveries([[30.0, 38.0], [70.0, 95.0]], probes, 130.0)
        self.assertAlmostEqual(rec[0], 0.9)
        self.assertAlmostEqual(rec[1], 1.5)
        # Nothing completed before the trial's end: censored there.
        self.assertEqual(imp.recoveries([[70.0, 95.0]], [], 120.0), [25.0])

    def test_silent_deaths_are_client_timeouts(self):
        recs = [{"kind": "llm", "fail": "TimeoutError: timed out"}, {"kind": "llm", "fail": "short stream (3 of 400)"},
                {"kind": "term", "fail": "socket.timeout: timeout"}, {"kind": "llm", "fail": "not run: the phase ended first"},
                {"kind": "llm", "fail": ""}]
        self.assertEqual(imp.silent_deaths(recs), 2)

    def test_every_measure_is_ruled_or_reported(self):
        import configparser
        cp = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=(";", "#"))
        cp.read(os.path.join(os.path.dirname(os.path.abspath(__file__)), "validation", "gates.ini"))
        rules = dict(cp["workload:impaired"])
        wl = imp.Impaired()
        t = types.SimpleNamespace(trial="blackhole", rows=[], records=[], m={})
        for ph in imp.TRIAL_TYPES:
            for k in wl.phase_measures(ph, [], [], [t]):
                ruled = f"*.{k}" in rules or f"{ph}.{k}" in rules
                self.assertTrue(ruled or k in wl.report_only, f"{ph}.{k}")


class DryRun(unittest.TestCase):
    def test_the_dry_run_applies_and_removes_around_every_trial(self):
        cfg = parse(BASE)
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as d:
            c = ab.Campaign(cfg, os.path.join(d, "o"), ".", dry=True, stream=out)
            with mock.patch.object(c, "build", return_value={}):
                c.run()
        text = out.getvalue()
        applies = text.count("impair: apply ")
        removes = text.count("impair: remove the blocks set")
        self.assertEqual(applies - 1, removes)              # one apply is D's, on its own set
        self.assertEqual(removes, 3 * len(cfg.arms))         # one block of each type, one trial per arm
        self.assertIn("impair: apply idle-wake-lossy to the beside ports", text)
        self.assertIn("impair: remove the beside set", text)


if __name__ == "__main__":
    unittest.main()


class BesideWatchdog(unittest.TestCase):
    def finish(self, fired, ended):
        cfg = parse(BASE)
        c = ab.Campaign(cfg, "/nonexistent", ".", dry=False, stream=io.StringIO())
        c.impair = mock.Mock()
        c.impair.remove.return_value = fired
        c.beside_threads = []
        c.beside_results = {("a", "main"): (ended - 900, ended, 0, "")}
        for name in ("stop", "stop_client", "collect"):
            setattr(c, name, mock.Mock())
        c.dry = True                     # stop before writing the sub-campaign
        c.finish_beside(cfg.wl.beside(cfg))
        return getattr(c, "beside_impair_fired", "")

    def test_a_watchdog_after_the_session_ended_is_not_a_failure(self):
        self.assertEqual(self.finish(fired=2000.0, ended=1000.0), "")

    def test_a_watchdog_during_the_session_is(self):
        self.assertTrue(self.finish(fired=900.0, ended=1000.0))


class FetchFailures(unittest.TestCase):
    def test_a_beside_sessions_logs_are_its_own(self):
        import ab_analyze as an
        paths = ["gw:/h/qq-ab-x/gw-main.wake/logs/server-idle-wake.jsonl.gz", "gw:/h/qq-ab-x/gw-s7/logs/server-b001.jsonl"]
        self.assertEqual(an.own_fetch_failures(paths, beside=False), paths[1:])
        self.assertEqual(an.own_fetch_failures(paths, beside=True), paths[:1])


class V2Measures(unittest.TestCase):
    def test_reopen_and_inflight_first_bytes(self):
        outs = [[30.0, 38.0], [70.0, 95.0]]
        recs = [{"kind": "probe", "t0": t, "ttfb": 0.3, "http": h} for t, h in
                ((37.9, "200"), (38.25, "000"), (38.5, "200"), (95.0, "200"))]
        self.assertEqual([round(x, 2) for x in imp.reopen_ttfb(outs, recs, 130.0)], [0.8, 0.3])
        # A download in flight across both outages, flat until 39.0 and 96.5.
        smp = [[t, b] for t, b in ((20, 0), (29, 10), (38.0, 10), (38.75, 10), (39.0, 20), (69, 30), (95, 30),
                                    (96.5, 40), (110, 50))]
        recs2 = [{"kind": "bgprog", "samples": smp}, {"kind": "bgprog", "samples": [[50, 0], [60, 5]]}]
        self.assertEqual(imp.inflight_ttfb(outs, recs2, 130.0), [1.0, 1.5])
        # Nothing moved before the trial's end: censored there.
        self.assertEqual(imp.inflight_ttfb([[70.0, 95.0]], [{"kind": "bgprog", "samples": [[60, 0], [120, 0]]}], 120.0),
                         [25.0])

    def test_completion_counts_attempts_not_the_windows_cut(self):
        rows = [{"phase": "policed", "kind": "imp-fetch", "flow": f"w1r{k}", "complete": c, "curl_exit": e}
                for k, c, e in ((1, "1", "0"), (2, "1", "0"), (3, "0", "56"), (4, "0", "28"))]
        recs = [{"_phase": "policed", "kind": "fetch", "k": 4, "window_cut": 1}]
        t = types.SimpleNamespace(rows=rows, records=recs)
        self.assertAlmostEqual(imp.fetch_completion([t], "policed"), 2 / 3)
        recs = [{"_phase": "policed", "kind": "fetch", "k": 4, "window_cut": 0}]     # a stalled one is a failure
        t = types.SimpleNamespace(rows=rows, records=recs)
        self.assertAlmostEqual(imp.fetch_completion([t], "policed"), 2 / 4)

    def test_events_per_block(self):
        recs = ([{"_phase": "slow-lossy", "kind": "llm", "fail": ""}] * 38 +
                [{"_phase": "slow-lossy", "kind": "llm", "fail": "TimeoutError: timed out"}] +
                [{"_phase": "slow-lossy", "kind": "llm", "fail": "short stream (3 of 100)"}] +
                [{"_phase": "slow-lossy", "kind": "term", "fail": ""}])
        t = types.SimpleNamespace(trial="slow-lossy", records=recs, rows=[])
        ev = imp.Impaired().block_events([t], {})
        self.assertEqual(ev["slow-lossy.call_failures"], (2, 40))
        self.assertEqual(ev["slow-lossy.silent_rate"], (1, 41))

    def test_a_trial_has_enough_calls(self):
        cfg = parse(BASE)
        ph = {p.name: p for p in cfg.wl.phases(cfg, "a")}
        llm = next(j for j in ph["slow-lossy"].jobs if j.kind == "wl-llm").params
        self.assertGreaterEqual(llm["agents"] * llm["turns"] * 4, 150)    # 4 valid blocks reach the minimum
        self.assertEqual([j.kind for j in ph["blackhole"].jobs if j.load], ["imp-fetch", "impair-outages"])


class Sampler(unittest.TestCase):
    def test_a_dropped_session_mid_trial_keeps_the_cpu_record(self):
        with tempfile.TemporaryDirectory() as d:
            cfg = parse(BASE.replace("[campaign]", "[campaign]\nhost_cpu = true"))
            c = ab.Campaign(cfg, d, ".", dry=False, stream=io.StringIO())
            c.out = d
            calls = {"n": 0}

            def run(script, timeout=120, what=""):
                calls["n"] += 1
                if "QQ_AB_JOB_EOF" in script:
                    return 0, "QQ_AB_LAUNCHED"
                if "stop the host CPU sampler" in what and calls.setdefault("drops", 0) < 2:
                    calls["drops"] += 1
                    return 255, ""          # the link drops twice while stopping
                if ": poll" in what:
                    return 0, "QQ_AB_RC 0\nS 1\t2\t3\n"
                return 0, ""
            for h in c.hosts.values():
                h.run = run
            c.SAMPLER_SSH_TRIES = 4
            with mock.patch.object(ab.time, "sleep"):
                s = c.start_host_cpu("b001-a-x-s1-main", "a")
                c.stop_host_cpu("b001-a-x-s1-main", s)
            files = os.listdir(os.path.join(d, "hostcpu"))
            self.assertTrue(files)
            for f in files:
                self.assertEqual(open(os.path.join(d, "hostcpu", f)).read(), "1\t2\t3\n")


class FetchVia(unittest.TestCase):
    def test_logs_come_through_the_jump_host_on_their_own_connection(self):
        h = ab.Host("gw", ["-o", "ConnectTimeout=20"], False, control_dir="/tmp/qqab-ssh-x")
        h.fetch_via = "relayhost"
        with tempfile.TemporaryDirectory() as d, mock.patch.object(ab.subprocess, "run") as run:
            run.return_value.returncode = 0
            h.get("/r/log.gz", os.path.join(d, "x", "log.gz"))
        argv = run.call_args[0][0]
        self.assertIn("ProxyJump=relayhost", argv)
        self.assertFalse([a for a in argv if a.startswith("ControlPath")])


class Offsets(unittest.TestCase):
    def test_offsets_are_drawn_per_trial_from_the_seed(self):
        cfg = parse(BASE)
        bh = next(p for p in cfg.wl.phases(cfg, "a") if p.name == "blackhole")
        slots = [s for s in ab.schedule(cfg) if s.trial == "blackhole"]
        arms = {a.name: a for a in cfg.arms}
        drawn = []
        for s in slots:
            p = cfg.wl.per_trial(cfg, s, arms[s.arm], bh)
            llm = next(j for j in p.jobs if j.kind == "wl-llm").params["delay"]
            outs = next(j for j in p.jobs if j.kind == "impair-outages").params["outages"]
            self.assertTrue(imp.LEAD <= llm <= imp.LEAD + imp.LLM_JITTER)
            self.assertTrue(30 <= outs[0][0] <= 40 and outs[0][1] == 8)
            self.assertTrue(70 <= outs[1][0] <= 75 and outs[1][0] + outs[1][1] <= 120 - 20)
            drawn.append((llm, outs[1][0]))
            # Reproducible from the frozen seed.
            again = cfg.wl.per_trial(cfg, s, arms[s.arm], bh)
            self.assertEqual(next(j for j in again.jobs if j.kind == "wl-llm").params["delay"], llm)
        self.assertEqual(len(set(drawn)), len(drawn))           # independent per arm and trial
        # The type's own phase is untouched.
        self.assertEqual(next(j for j in bh.jobs if j.kind == "wl-llm").params["delay"], imp.LEAD)

    def test_the_outage_job_runs_the_drawn_offsets(self):
        cfg = parse(BASE)
        c = ab.Campaign(cfg, "/nonexistent", ".", dry=True, stream=io.StringIO())
        script = c.impair.outage_script("blackhole", [(33.5, 8), (72.25, 25)])
        self.assertIn("t0 + 33.5 - n", script)
        self.assertIn("t0 + 72.25 - n", script)


class TtftSplit(unittest.TestCase):
    def call(self, t0, t1, ttft=0.6, fail=""):
        return {"_phase": "blackhole", "kind": "llm", "t0": t0, "t1": t1, "ttft": ttft, "T": 0.5, "fail": fail}

    def test_clear_calls_leave_out_the_outage_and_the_five_seconds_after(self):
        outs = [[30.0, 38.0], [70.0, 95.0]]
        recs = [{"_phase": "blackhole", "kind": "impair", "fail": "", "outages": outs},
                self.call(10, 14), self.call(28, 31), self.call(40, 44), self.call(44, 48), self.call(96, 99),
                self.call(101, 104)]
        t = types.SimpleNamespace(records=recs)
        self.assertEqual([c["t0"] for c in imp.clear_calls([t], "blackhole")], [10, 44, 101])

    def test_after_restore_is_from_the_restore_to_the_first_token(self):
        outs = [[30.0, 38.0], [70.0, 95.0]]
        recs = [self.call(29.0, 45.0, ttft=10.5),           # first token at 39.5: 1.5 s after the restore
                self.call(20.0, 33.0, ttft=0.5),            # first token before the outage: not caught
                self.call(68.0, 99.0, ttft=28.0),           # first token at 96.0
                self.call(80.0, 110.0, ttft=None, fail="TimeoutError: timed out")]   # none: censored at its end
        self.assertEqual(imp.after_restore(outs, recs, 120.0), [1.5, 15.0])

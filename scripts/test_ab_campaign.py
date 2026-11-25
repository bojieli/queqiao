import collections
import configparser
import contextlib
import importlib.util
import io
import os
import pathlib
import subprocess
import sys
import time
import unittest
from unittest import mock

HERE = pathlib.Path(__file__).parent
SPEC = importlib.util.spec_from_file_location("ab_campaign", HERE / "ab_campaign.py")
ab = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules["ab_campaign"] = ab
SPEC.loader.exec_module(ab)

EXAMPLE = HERE / "validation" / "campaign.example.ini"
DC_EXAMPLE = HERE / "validation" / "dc-campaign.example.ini"


def parse(text):
    cp = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=(";", "#"))
    cp.read_string(text)
    return ab.parse_config(cp)


BASE = """
[campaign]
name = t1
blocks = 4
[gateway]
host = gw
address = 192.0.2.1
[client:a]
host = c1
small = true
[arm:main]
role = main
binary_linux_amd64 = /bin/true
[arm:cand]
binary_linux_amd64 = /bin/true
flags = --cc-api=v2
[production:gw]
host = gw
units = queqiaod
ports = 443 12540-12549
[production:c1]
host = c1
"""


class DryRun(unittest.TestCase):
    def run_dry(self, config):
        out = io.StringIO()
        forbidden = mock.Mock(side_effect=AssertionError("dry run must not run a command"))
        with mock.patch.object(subprocess, "run", forbidden), mock.patch.object(subprocess, "Popen", forbidden), \
                contextlib.redirect_stdout(out):
            rc = ab.main(["--config", str(config), "--dry-run"])
        return rc, out.getvalue()

    def test_dry_run_prints_the_plan_and_touches_nothing(self):
        rc, text = self.run_dry(EXAMPLE)
        self.assertEqual(rc, 0)
        self.assertIn("# campaign example-stage6: workload bulk, 12 block(s)", text)
        self.assertIn("b001 path-a:", text)
        self.assertIn("b012 path-b:", text)
        self.assertIn("read-only production snapshot (before)", text)
        self.assertIn("[preflight] would refuse", text)
        self.assertIn("main: bulk x4 via path-a/cand", text)
        self.assertIn("stop every temporary process under the work directory", text)
        self.assertIn("blocks 2-12 repeat", text)
        # The runner owns these; the arm's bundle is appended after them.
        self.assertIn("--cc-api=v2 --sched=class --congestion=path", text)
        self.assertNotIn("CAMPAIGN-DONE", text)

    def test_probe_stops_when_the_load_finishes(self):
        rc, text = self.run_dry(DC_EXAMPLE)
        lines = text.splitlines()
        i = next(k for k, l in enumerate(lines) if "load finished, stop the probe on leaf-ctl" in l)
        self.assertIn("touch ~/qq-ab-example-dc-gate/run/stop-probe-leaf-ctl-", lines[i + 1])
        self.assertIn("[ ! -e ~/qq-ab-example-dc-gate/run/stop-probe-leaf-ctl-", text)

    def test_every_client_host_gets_a_run_directory(self):
        _, text = self.run_dry(DC_EXAMPLE)
        for host in ("dc-client-a", "dc-client-b", "dc-client-c", "dc-client-d"):
            self.assertIn(f"[{host}] create work directory:\n    mkdir -p ~/qq-ab-example-dc-gate/run", text)

    def test_dc_dry_run_has_phases_and_calibration(self):
        rc, text = self.run_dry(DC_EXAMPLE)
        self.assertEqual(rc, 0)
        for want in ("sustained: bulk x2 via leaf-a/pathauth", "requests: requests x8 via leaf-c/legacy",
                     "sustained: probe x1 via leaf-ctl", "solo: bulk x2 via leaf-b/legacy", "--path-profile dc-long-haul"):
            self.assertIn(want, text)

    def test_small_hosts_never_run_two_temporary_clients(self):
        _, text = self.run_dry(EXAMPLE)
        running = collections.Counter()
        for line in text.splitlines():
            if line.startswith("[small-client-host] start cl-"):
                running["small-client-host"] += 1
                self.assertEqual(running["small-client-host"], 1, line)
            elif line.startswith("[small-client-host] stop cl-"):
                running["small-client-host"] = max(0, running["small-client-host"] - 1)

    def test_runtime_guard_refuses_a_second_client_on_a_small_host(self):
        cfg = parse(BASE)
        camp = ab.Campaign(cfg, "<out>", ".", dry=True, stream=io.StringIO())
        camp.start_client(cfg.clients["a"], cfg.arms[0], "x")
        with self.assertRaises(RuntimeError):
            camp.start_client(cfg.clients["a"], cfg.arms[1], "x")
        camp.stop_client(cfg.clients["a"], cfg.arms[0])
        camp.start_client(cfg.clients["a"], cfg.arms[1], "x")

    def test_stop_signals_only_processes_from_the_work_directory(self):
        cfg = parse(BASE)
        out = io.StringIO()
        camp = ab.Campaign(cfg, "<out>", ".", dry=True, stream=out)
        camp.stop(camp.hosts["gw"], "gw-main")
        camp.teardown()
        text = out.getvalue()
        self.assertIn('grep -q -F -- "~/qq-ab-t1/"', text)
        self.assertNotIn("pkill", text)
        self.assertNotIn("killall", text)


class Schedule(unittest.TestCase):
    def test_two_arms_are_abba_or_baab(self):
        cfg = parse(BASE.replace("blocks = 4", "blocks = 40"))
        orders = collections.Counter()
        for b in range(1, 41):
            slots = [s for s in ab.schedule(cfg) if s.block == b]
            seq = tuple(s.arm for s in slots)
            self.assertIn(seq, {("main", "cand", "cand", "main"), ("cand", "main", "main", "cand")})
            self.assertEqual([s.cold for s in slots], [True, True, False, False])
            self.assertEqual([s.arm_trial for s in slots], [1, 1, 2, 2])
            orders[seq] += 1
        self.assertEqual(len(orders), 2)  # both orders occur

    def test_seed_reproduces_the_schedule(self):
        a = [s.__dict__ for s in ab.schedule(parse(BASE))]
        b = [s.__dict__ for s in ab.schedule(parse(BASE))]
        self.assertEqual(a, b)

    def test_three_arms_rotate_through_every_position(self):
        text = BASE.replace("blocks = 4", "blocks = 6") + "[arm:third]\nbinary_linux_amd64 = /bin/true\n"
        cfg = parse(text)
        pos = collections.defaultdict(collections.Counter)
        for s in ab.schedule(cfg):
            pos[s.arm][s.slot] += 1
            if s.block == 1:
                pass
        for arm, c in pos.items():
            self.assertEqual(sum(c.values()), 12)        # twice per block
            self.assertEqual(set(c.values()), {2}, (arm, c))  # every slot equally often over 2N blocks

    def test_paths_rotate(self):
        text = BASE + "[client:b]\nhost = c1\nsocks_port_base = 24581\nmetrics_port_base = 24591\n"
        firsts = [next(s.path for s in ab.schedule(parse(text)) if s.block == b) for b in (1, 2, 3)]
        self.assertEqual(firsts, ["a", "b", "a"])


class Config(unittest.TestCase):
    def test_examples_parse(self):
        for f in (EXAMPLE, DC_EXAMPLE):
            cfg = ab.load_config(str(f))
            self.assertEqual(cfg.main.role, "main")

    def assertRejected(self, text, fragment):
        with self.assertRaises(ab.ConfigError) as cm:
            parse(text)
        self.assertIn(fragment, str(cm.exception))

    def test_runner_owned_flags_are_rejected(self):
        self.assertRejected(BASE.replace("flags = --cc-api=v2", "flags = --cc-api=v2 --state=/var/lib/queqiao/provider"),
                            "--state is set by the runner")
        self.assertRejected(BASE.replace("flags = --cc-api=v2", "gateway_flags = --listen :443"), "--listen")

    def test_single_dash_spelling_is_also_owned(self):
        # Go's flag package treats -state and --state alike.
        self.assertRejected(BASE.replace("flags = --cc-api=v2", "flags = -state /var/lib/queqiao/provider"), "--state")
        self.assertRejected(BASE.replace("flags = --cc-api=v2", "client_flags = -profile=/etc/p.json"), "--profile")

    def test_hop_ports_are_not_supported(self):
        self.assertRejected(BASE.replace("flags = --cc-api=v2", "gateway_flags = --hop-port-count 100"), "hop-port-count")

    def test_every_host_needs_a_production_section(self):
        self.assertRejected(BASE.replace("[production:c1]\nhost = c1\n", ""), "missing: c1")

    def test_exactly_one_main(self):
        self.assertRejected(BASE.replace("[arm:cand]\n", "[arm:cand]\nrole = main\n"), "exactly one arm")

    def test_bad_names(self):
        self.assertRejected(BASE.replace("name = t1", "name = ../etc"), "campaign name")
        self.assertRejected(BASE.replace("name = t1", "name = Prod"), "campaign name")

    def test_planned_ports_may_not_collide(self):
        text = BASE + "[client:b]\nhost = c1\n"
        self.assertRejected(text, "planned twice")

    def test_dc_leaves_need_their_own_hosts(self):
        text = BASE.replace("workload", "x").replace("blocks = 4", "blocks = 4\nworkload = dc") + \
            "[client:b]\nhost = c1\nsocks_port_base = 24581\nmetrics_port_base = 24591\n" \
            "[client:ctl]\nhost = c1\nsocks_port_base = 25581\nmetrics_port_base = 25591\n" \
            "[dc]\nloaded = a b\ncontrol = ctl\n"
        self.assertRejected(text, "own host")

    def test_crossover_arm_needs_both_ends(self):
        self.assertRejected(BASE.replace("binary_linux_amd64 = /bin/true\nflags", "gateway_ref = origin/main\nflags"),
                            "both gateway_ref")

    def test_crossover_arm_builds_each_ref_once(self):
        text = BASE.replace("[arm:main]\nrole = main\nbinary_linux_amd64 = /bin/true",
                            "[arm:main]\nrole = main\nref = origin/main") \
                   .replace("[arm:cand]\nbinary_linux_amd64 = /bin/true",
                            "[arm:cand]\ngateway_ref = feat/x\nclient_ref = origin/main")
        cfg = parse(text)
        camp = ab.Campaign(cfg, "/out", ".", dry=True, stream=io.StringIO())
        self.assertEqual(list(camp.refs()), ["origin/main", "feat/x"])
        main, cand = cfg.arms
        self.assertEqual(camp.binary(cand, "linux/amd64", "client"), camp.binary(main, "linux/amd64", "gateway"))
        self.assertNotEqual(camp.binary(cand, "linux/amd64", "gateway"), camp.binary(main, "linux/amd64", "gateway"))

    def test_prebuilt_binaries_must_cover_every_platform(self):
        text = BASE + "[client:mac]\nhost = gw\nplatform = darwin/arm64\nsocks_port_base = 24581\nmetrics_port_base = 24591\n"
        self.assertRejected(text, "binary_darwin_arm64")


SNAP = """os Linux
listen-ok
unit queqiaod active 1234 0 99999 exe=/usr/local/bin/queqiaod sha=abc
proc 1234 Tue Nov 24 09:28:24 2026 /usr/local/bin/queqiaod server --listen :12540
listen udp 12540
listen tcp 19090
"""


class Safety(unittest.TestCase):
    def test_snapshot_parse(self):
        s = ab.parse_snapshot(SNAP)
        self.assertEqual(s["units"]["queqiaod"], "active 1234 0 99999 exe=/usr/local/bin/queqiaod sha=abc")
        self.assertEqual(s["listen"], {12540, 19090})
        self.assertEqual(len(s["procs"]), 1)

    def test_identical_snapshots_have_no_diff(self):
        self.assertEqual(ab.snapshot_diff(ab.parse_snapshot(SNAP), ab.parse_snapshot(SNAP)), [])

    def test_restart_is_detected(self):
        after = SNAP.replace("active 1234 0", "active 5678 1").replace("proc 1234 Tue", "proc 5678 Wed")
        d = ab.snapshot_diff(ab.parse_snapshot(SNAP), ab.parse_snapshot(after))
        self.assertTrue(any(x.startswith("unit queqiaod") for x in d), d)
        self.assertTrue(any("ended or restarted" in x for x in d), d)
        self.assertTrue(any("appeared" in x for x in d), d)

    def test_new_binary_is_detected(self):
        d = ab.snapshot_diff(ab.parse_snapshot(SNAP), ab.parse_snapshot(SNAP.replace("sha=abc", "sha=def")))
        self.assertEqual(len(d), 1)

    def test_clean_hosts_are_not_refused(self):
        cfg = parse(BASE)
        snaps = {"gw": ab.parse_snapshot(SNAP), "c1": ab.parse_snapshot("os Linux\nlisten-ok\n")}
        self.assertEqual(ab.refusal_reasons(cfg, snaps), [])

    def test_unreadable_listener_table_is_refused(self):
        cfg = parse(BASE)
        snaps = {"gw": ab.parse_snapshot(SNAP), "c1": ab.parse_snapshot("os Linux\n")}
        r = ab.refusal_reasons(cfg, snaps)
        self.assertEqual(len(r), 1)
        self.assertIn("c1: could not read the listening ports", r[0])

    def test_listening_planned_port_is_refused(self):
        cfg = parse(BASE)
        snaps = {"gw": ab.parse_snapshot(SNAP + "listen tcp 23472\n"), "c1": ab.parse_snapshot("listen-ok\n")}
        r = ab.refusal_reasons(cfg, snaps)
        self.assertEqual(len(r), 1)
        self.assertIn("23472 is already listening", r[0])

    def test_declared_production_port_is_refused(self):
        cfg = parse(BASE.replace("ports = 443 12540-12549", "ports = 443 23470-23480"))
        r = ab.refusal_reasons(cfg, {"gw": ab.parse_snapshot("listen-ok"), "c1": ab.parse_snapshot("listen-ok")})
        self.assertTrue(any("declared production port" in x for x in r), r)
        self.assertTrue(any("origin port 23480" in x for x in r), r)

    def test_leftover_campaign_process_is_refused(self):
        cfg = parse(BASE)
        snaps = {"gw": ab.parse_snapshot(""),
                 "c1": ab.parse_snapshot("leftover 42 /root/qq-ab-t1/cl-a-main/queqiaod client\n")}
        self.assertTrue(any("earlier campaign" in x for x in ab.refusal_reasons(cfg, snaps)))

    def test_refused_campaign_starts_nothing(self):
        cfg = parse(BASE)
        camp = ab.Campaign(cfg, "/nonexistent/out", ".", dry=False, stream=io.StringIO())
        with mock.patch.object(ab.os, "makedirs"), \
                mock.patch.object(ab.Campaign, "snapshot_all",
                                  return_value={"gw": ab.parse_snapshot(SNAP + "listen tcp 23471\n"), "c1": {}}), \
                mock.patch.object(ab.Host, "run", return_value=(0, "/home/u")) as run, \
                mock.patch.object(ab.Campaign, "build") as build, \
                mock.patch.object(ab.Campaign, "setup") as setup:
            with self.assertRaises(ab.Refused):
                camp.run()
        build.assert_not_called()
        setup.assert_not_called()
        # Only the $HOME lookups ran.
        self.assertTrue(all("HOME" in c.args[0] for c in run.call_args_list))


class Parsing(unittest.TestCase):
    def test_missing_flows_become_failed_rows(self):
        out = "1700000000\tb1\t200\t100\t1.5\t0\ngarbage\n1700000001\tb3\n"
        rows = ab.job_rows(out, "bulk", 3, 1, 90)
        self.assertEqual([r[0] for r in rows], ["b1", "b3", "b2"])
        self.assertEqual(rows[1], ("b3", "000", "0", "90.000", "-1"))
        self.assertEqual(rows[2], ("b2", "000", "0", "90.000", "-1"))

    def test_request_waves_are_expected(self):
        rows = ab.job_rows("", "requests", 2, 2, 30)
        self.assertEqual([r[0] for r in rows], ["w1r1", "w1r2", "w2r1", "w2r2"])

    def test_probe_has_no_fixed_count(self):
        self.assertEqual(ab.job_rows("", "probe", 1, 1, 30), [])

    def test_local_address_passes_to_enrollment(self):
        self.assertEqual(ab.local_address_args(["--wire-versions", "2", "--local-address", "if:en0"]),
                         ["--local-address", "if:en0"])
        self.assertEqual(ab.local_address_args(["--local-address=192.0.2.4"]), ["--local-address=192.0.2.4"])


class LocalProcesses(unittest.TestCase):
    """Runs the runner's own shell against this machine only: the read-only
    snapshot, and spawn/stop/teardown on processes the test itself starts in
    a temporary directory. No other host is contacted."""

    def setUp(self):
        import tempfile
        self.tmp = tempfile.TemporaryDirectory()
        self.home = os.path.realpath(self.tmp.name)
        cfg = parse(BASE.replace("host = gw", "host = local").replace("host = c1", "host = local")
                    .replace("[client:a]\nhost = local", "[client:a]\nhost = local\nsocks_port_base = 24581\nmetrics_port_base = 24591"))
        self.cfg = cfg
        self.out = io.StringIO()
        self.camp = ab.Campaign(cfg, os.path.join(self.home, "out"), ".", dry=False, stream=self.out)
        self.host = self.camp.hosts["local"]
        self.host.home = self.home
        self.W = self.camp.w("local")
        os.makedirs(os.path.join(self.W, "run"))
        self.procs = []

    def tearDown(self):
        for p in self.procs:
            if p.poll() is None:
                p.kill()
            p.wait()
        self.tmp.cleanup()

    def alive(self, pid):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        # A zombie child of this test still answers kill -0.
        out = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
        return bool(out) and not out.startswith("Z")

    def test_snapshot_runs_cleanly_and_sees_a_listener_and_a_leftover(self):
        import socket
        srv = socket.socket()
        srv.bind(("127.0.0.1", 0))
        srv.listen()
        port = srv.getsockname()[1]
        left = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)", f"{self.W}/marker"])
        self.procs.append(left)
        try:
            os.makedirs(self.camp.out)
            snap = self.camp.snapshot_all("t")["local"]
        finally:
            srv.close()
        self.assertIn(port, snap["listen"])
        self.assertTrue(snap["listen_ok"])
        self.assertTrue(any(f"{self.W}/marker" in x for x in snap["leftover"]), snap["leftover"])
        self.assertTrue(all(self.W + "/" not in p for p in snap["procs"]))

    def test_snapshot_of_a_clean_host_succeeds(self):
        # Nothing under the work directory: the last grep matches nothing,
        # which must not read as a failed snapshot under pipefail.
        os.makedirs(self.camp.out)
        snap = self.camp.snapshot_all("clean")["local"]
        self.assertEqual(snap["leftover"], [])
        self.assertIn(snap.get("os"), ("Linux", "Darwin"))

    def test_cpu_sampler_reads_a_busy_process(self):
        busy = subprocess.Popen([sys.executable, "-c", "while True: pass"])
        self.procs.append(busy)
        pid_file = os.path.join(self.W, "run", "busy.pid")
        out_file = os.path.join(self.W, "busy.cpu")
        pathlib.Path(pid_file).write_text(str(busy.pid))
        rc, _ = self.host.run(ab.cpu_sampler(pid_file, out_file))
        self.assertEqual(rc, 0)
        time.sleep(3.5)
        busy.kill()
        busy.wait()
        rows = [tuple(map(float, l.split())) for l in pathlib.Path(out_file).read_text().splitlines() if l.strip()]
        self.assertGreaterEqual(len(rows), 2, rows)
        (t0, c0), (t1, c1) = rows[0], rows[-1]
        if t1 > t0:
            # A spinning process uses about one core: 100 ticks a second.
            self.assertGreater((c1 - c0) / 100 / (t1 - t0), 0.3)

    def test_spawn_then_stop_kills_only_work_directory_processes(self):
        decoy = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)", "decoy-not-in-workdir"])
        self.procs.append(decoy)
        self.camp.spawn(self.host, "t1", self.W, f"{sys.executable} -c 'import time; time.sleep(30)' {self.W}/marker")
        pid = int(pathlib.Path(self.W, "run", "t1.pid").read_text())
        self.assertTrue(self.alive(pid))
        # A pid file pointing at a process outside the work directory is not signalled.
        with open(os.path.join(self.W, "run", "t2.pid"), "w") as f:
            f.write(str(decoy.pid))
        self.camp.stop(self.host, "t2")
        self.assertTrue(self.alive(decoy.pid))
        self.camp.stop(self.host, "t1")
        for _ in range(40):
            if not self.alive(pid):
                break
            time.sleep(0.05)
        self.assertFalse(self.alive(pid))
        self.camp.spawn(self.host, "t3", self.W, f"{sys.executable} -c 'import time; time.sleep(30)' {self.W}/marker")
        pid3 = int(pathlib.Path(self.W, "run", "t3.pid").read_text())
        self.camp.teardown()
        for _ in range(60):
            if not self.alive(pid3):
                break
            time.sleep(0.05)
        self.assertFalse(self.alive(pid3))
        self.assertTrue(self.alive(decoy.pid))


if __name__ == "__main__":
    unittest.main()

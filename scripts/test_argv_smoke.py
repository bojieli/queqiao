"""Start every binary role a campaign uses -- gateway, client, relay -- with
the exact command line the runner would give it, for about a second, on
scratch ports on this machine: a wrong subcommand or flag fails here, not
minutes into a live campaign on production hosts.

It builds cmd/queqiaod for this machine (skipped without Go) and checks each
example config whose arms are built from origin/main, and, with
QQ_SMOKE_CONFIG=<path>, a campaign's own config before it starts. Host-specific options
(--local-address, --tcp-congestion) are replaced or left out, since they
depend on the host, not on the build: clients use loopback as their outer
source."""
import configparser
import json
import importlib
import io
import os
import pathlib
import platform
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest

HERE = pathlib.Path(__file__).parent
REPO = HERE.parent
sys.path.insert(0, str(HERE))
ab = importlib.import_module("ab_campaign")

EXAMPLES = ["minimal-campaign.example.ini", "impaired-campaign.example.ini", "campaign.example.ini", "scenario-campaign.example.ini",
            "dc-minimal.example.ini", "dc-campaign.example.ini"]
HOST_SPECIFIC = ("local-address", "tcp-congestion")


_taken = set()


def _bindable(port):
    for kind in (socket.SOCK_STREAM, socket.SOCK_DGRAM):
        s = socket.socket(socket.AF_INET, kind)
        try:
            s.bind(("0.0.0.0", port))
        except OSError:
            return False
        finally:
            s.close()
    return True


def free_port(n=1):
    """A port p with p .. p + n - 1 free for TCP and UDP, below the usual
    ephemeral range (a busy host's outgoing connections take those), and
    not handed out before in this run."""
    import random
    for _ in range(500):
        p = random.randint(20000, 32000)
        ports = range(p, p + n)
        if not any(q in _taken for q in ports) and all(_bindable(q) for q in ports):
            _taken.update(ports)
            return p
    raise RuntimeError("no free port")


def _bindable_host(host):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.bind((host, 0))
        return True
    except OSError:
        return False
    finally:
        s.close()


def free_pair():
    """A port p with p .. p + 7 free: one per arm and per D copy of an arm
    (three arms and D take six), so no two port bases' runs overlap."""
    return free_port(8)


def go_platform():
    return {"Darwin": "darwin", "Linux": "linux"}.get(platform.system(), "linux") + "/" + \
        {"x86_64": "amd64", "AMD64": "amd64", "arm64": "arm64", "aarch64": "arm64"}.get(platform.machine(), "amd64")


def strip_host_specific(words):
    out, skip = [], False
    for w in words:
        if skip:
            skip = False
            continue
        name = w.lstrip("-").split("=", 1)[0]
        if w.startswith("-") and name in HOST_SPECIFIC:
            skip = "=" not in w
            continue
        out.append(w)
    return " ".join(out)


def local_config(example, binary, home):
    """The example's arms, with this machine standing in for every host."""
    cp = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=(";", "#"))
    cp.read(example if os.path.isabs(example) else str(HERE / "validation" / example))
    plat = go_platform()
    base = free_pair()
    g = cp["gateway"]
    g.update(host="local", address="127.0.0.1", platform=plat, port_base=str(base), metrics_port_base=str(free_pair()),
             origin_port=str(free_port()), flags=strip_host_specific(ab._words(g.get("flags", ""))))
    # The DC gate wants every leaf on a host of its own (a leaf is an endpoint
    # pair); here they are all this machine, under names of their own.
    own_hosts = cp["campaign"].get("workload", "") == "dc"
    leaf_hosts = []
    for sec in cp.sections():
        s = cp[sec]
        if sec.startswith("client:"):
            host = "local"
            if own_hosts:
                host = "leafhost-" + sec.split(":", 1)[1]
                leaf_hosts.append(host)
            # This machine's outer source: loopback, where the gateway is.
            s.update(host=host, small="false", platform=plat, socks_port_base=str(free_pair()),
                     metrics_port_base=str(free_pair()),
                     flags=(strip_host_specific(ab._words(s.get("flags", ""))) + " --local-address 127.0.0.1").strip())
        elif sec.startswith("arm:"):
            for k in ("ref", "gateway_ref", "client_ref"):
                s.pop(k, None)
            s["binary_" + plat.replace("/", "_")] = binary
        elif sec.startswith(("production:", "relay")):
            cp.remove_section(sec)
    # The DC upload phase's relays: each on a host of its own, at an address
    # of its own (a leaf is the remote host): 127.0.0.31... Where those
    # loopback addresses do not exist (macOS), the relays are reached at
    # 127.0.0.1 after the config is validated; their ports differ, which is
    # all a command-line check needs.
    upleafs = [sec for sec in cp.sections() if sec.startswith("upleaf:")]
    spare_loopback = all(_bindable_host(f"127.0.0.{31 + i}") for i in range(len(upleafs)))
    for i, sec in enumerate(upleafs):
        host = "relayleaf-" + sec.split(":", 1)[1]
        cp[sec].update(host=host, address=f"127.0.0.{31 + i}", platform=plat)
        leaf_hosts.append(host)
    if upleafs:
        cp["dc"]["upload_port_base"] = str(free_port(20 * len(upleafs)))
    cp["production:local"] = {"host": "local"}
    for host in leaf_hosts:
        cp["production:" + host] = {"host": host}
    # The relay as its own host name (it is this machine too): on a real
    # campaign it shares no host, and so no port, with the gateway.
    cp["production:relayhost"] = {"host": "relayhost"}
    cp["relay"] = {"host": "relayhost", "address": "127.0.0.1", "platform": plat}
    cp["campaign"]["sequential"] = "false"
    if cp.has_section("impair"):
        # The impairment is tc on the client host, not a binary role.
        cp["impair"]["host"] = "local"
    cfg = ab.parse_config(cp)
    if upleafs and not spare_loopback:
        for r in cfg.aux_relays:
            r.address = "127.0.0.1"
        for c in cfg.clients.values():
            c.providers = [(leaf, "127.0.0.1", rb, b) for leaf, _, rb, b in c.providers]
    camp = ab.Campaign(cfg, os.path.join(home, "out"), str(REPO), dry=False, stream=io.StringIO())
    camp.hosts["local"].home = home
    camp.hosts["relayhost"].home = home
    for host in leaf_hosts:
        camp.hosts[host].home = home
    return cfg, camp


def listening(port):
    try:
        socket.create_connection(("127.0.0.1", port), timeout=0.3).close()
        return True
    except OSError:
        return False


@unittest.skipUnless(shutil.which("go"), "needs Go to build cmd/queqiaod")
class ArgvSmoke(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp()
        cls.bin = os.path.join(cls.tmp, "queqiaod")
        subprocess.run(["go", "build", "-o", cls.bin, "./cmd/queqiaod"], cwd=str(REPO), check=True,
                       capture_output=True, timeout=900)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def start(self, cmd, cwd, port, what):
        """Run cmd (the runner's exact command line) until it listens on
        port, then a second more: it must still be running."""
        p = subprocess.Popen(["bash", "-c", "exec " + cmd], cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             text=True)
        self.addCleanup(lambda: (p.poll() is None and p.kill(), p.wait()))
        deadline = time.monotonic() + (15 if platform.system() == "Linux" else 4)
        while time.monotonic() < deadline and p.poll() is None and not listening(port):
            time.sleep(0.1)
        time.sleep(1.0)
        if p.poll() is not None:
            logs = "".join(open(f).read()[-800:] for f in sorted(pathlib.Path(cwd).glob("logs/*.jsonl")))
            self.fail(f"{what} exited ({p.returncode}): {cmd}\n{p.stdout.read()[-800:]}\n{logs}")
        if platform.system() == "Linux":
            # (macOS's application firewall may hold back an unsigned build's
            # listener on every interface; there, running is the check.)
            self.assertTrue(listening(port), f"{what} is not listening on {port}: {cmd}")
        return p

    def check(self, example):
        home = tempfile.mkdtemp(dir=self.tmp)
        cfg, camp = local_config(example, self.bin, home)
        g = cfg.gateway
        W = camp.w("local")
        for arm in cfg.arms:
            d = camp.gw_dir(arm)
            os.makedirs(os.path.join(d, "logs"))
            shutil.copy(self.bin, os.path.join(d, "queqiaod"))
            q = lambda *a: subprocess.run([os.path.join(d, "queqiaod"), *a], cwd=d, capture_output=True, text=True,
                                          timeout=60, check=True).stdout
            q("provider", "init", "--state", "./state", "--name", f"smoke-{arm.name}", "--endpoint",
              f"{cfg.endpoint_address}:{g.port_base + arm.index}")
            q("provider", "add-user", "--state", "./state", "--name", "ab", "--max-flows", "0", "--max-clients", "0")
            gw = self.start(camp.gw_cmd(arm, "smoke"), d, g.metrics_port_base + arm.index, f"{example}: gateway {arm.name}")
            relays = []
            for r in cfg.aux_relays:
                # The DC upload leaves' relays, which the uploader enrolls
                # and runs through.
                rd = os.path.join(camp.w(r.host), "relay")
                os.makedirs(rd, exist_ok=True)
                if not os.path.exists(os.path.join(rd, "queqiaod")):
                    # Every relay host is this machine with one home: never
                    # overwrite a binary a running relay was started from.
                    shutil.copy(self.bin, os.path.join(rd, "queqiaod"))
                relays.append(self.start(camp.aux_relay_cmd(r, arm), rd, r.port_base + arm.index,
                                         f"{example}: relay {r.name} {arm.name}"))
            for client in cfg.clients.values():
                if client.providers:
                    cd = camp.cl_dir(client, arm)
                    os.makedirs(os.path.join(cd, "logs"))
                    shutil.copy(self.bin, os.path.join(cd, "queqiaod"))
                    entries = []
                    for leaf, address, relay_base, base in client.providers:
                        invite = re.search(r"queqiao://\S+", q("provider", "invite", "--state", "./state", "--user", "ab",
                                                               "--expires-in", "1h", "--path",
                                                               f"{leaf}={address}:{relay_base + arm.index}")).group(0)
                        prof = os.path.join(cd, f"profile-{leaf}.json")
                        e = subprocess.run([os.path.join(cd, "queqiaod"), "enroll", "--invite", invite, "--profile", prof,
                                            "--device-name", f"smoke-{leaf}",
                                            *ab.local_address_args(camp.client_flags(client, arm))], cwd=cd,
                                           capture_output=True, text=True, timeout=60)
                        self.assertEqual(e.returncode, 0, f"{example}: enroll {client.name}/{leaf}: {e.stderr[-600:]}")
                        entries.append({"name": leaf, "profile": prof, "listen": f"127.0.0.1:{base + arm.index}"})
                    with open(os.path.join(cd, "providers.json"), "w") as f:
                        json.dump({"version": 1, "providers": entries}, f)
                    cl = self.start(camp.cl_cmd(client, arm, "smoke"), cd, client.socks_port_base + arm.index,
                                    f"{example}: client {client.name} {arm.name} (one process, {len(entries)} providers)")
                    cl.kill()
                    cl.wait()
                    continue
                # One invitation per client, as the runner does: each is single-use.
                invite = re.search(r"queqiao://\S+", q("provider", "invite", "--state", "./state", "--user", "ab",
                                                       "--expires-in", "1h")).group(0)
                cd = camp.cl_dir(client, arm)
                os.makedirs(os.path.join(cd, "logs"))
                shutil.copy(self.bin, os.path.join(cd, "queqiaod"))
                e = subprocess.run([os.path.join(cd, "queqiaod"), "enroll", "--invite", invite, "--profile",
                                    os.path.join(cd, "profile.json"), "--device-name", "smoke",
                                    *ab.local_address_args(camp.client_flags(client, arm))], cwd=cd,
                                   capture_output=True, text=True, timeout=60)
                self.assertEqual(e.returncode, 0, f"{example}: enroll {client.name} with {arm.name}: {e.stderr[-600:]}")
                cl = self.start(camp.cl_cmd(client, arm, "smoke"), cd, client.socks_port_base + arm.index,
                                f"{example}: client {client.name} {arm.name}")
                cl.kill()
                cl.wait()
            for rl in relays:
                rl.kill()
                rl.wait()
            gw.kill()
            gw.wait()
            # The relay hop, on the gateway's port (the gateway is gone).
            os.makedirs(os.path.join(W, "relay"), exist_ok=True)
            shutil.copy(self.bin, os.path.join(W, "relay", "queqiaod"))
            rl = self.start(camp.relay_cmd(arm), os.path.join(W, "relay"), g.port_base + arm.index,
                            f"{example}: relay {arm.name}")
            rl.kill()
            rl.wait()

    def test_minimal_example(self):
        self.check("minimal-campaign.example.ini")

    def test_campaign_example(self):
        self.check("campaign.example.ini")

    def test_scenario_example(self):
        self.check("scenario-campaign.example.ini")

    def test_impaired_example(self):
        # Three arms, the third with stage 8's carrier recovery.
        self.check("impaired-campaign.example.ini")

    def test_dc_minimal_example(self):
        # The stage 9 DC gate: dc-long-haul on both arms, the legacy bundle
        # against the whole new-architecture bundle.
        self.check("dc-minimal.example.ini")

    def test_dc_campaign_example(self):
        self.check("dc-campaign.example.ini")

    @unittest.skipUnless(os.environ.get("QQ_SMOKE_CONFIG"), "set QQ_SMOKE_CONFIG to a campaign config to check it")
    def test_a_given_campaign_config(self):
        # A live campaign's own (git-ignored) config, before it starts.
        self.check(os.path.abspath(os.environ["QQ_SMOKE_CONFIG"]))


if __name__ == "__main__":
    unittest.main()

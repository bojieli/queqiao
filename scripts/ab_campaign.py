#!/usr/bin/env python3
"""N-arm interleaved live A/B campaign runner for the path-authority gates.

    ./scripts/ab_campaign.py --config scripts/validation/campaign.local.ini --dry-run
    ./scripts/ab_campaign.py --config scripts/validation/campaign.local.ini --output DIR

Each arm is a build (a git ref, or prebuilt binaries) plus a flag bundle such
as `--cc-api=v2 --sched=class --congestion=path`. For every arm the runner
starts one temporary gateway on spare ports of a configured remote host, beside
a loopback HTTP origin, and temporary clients on configured client hosts. Arms
are interleaved inside blocks (DESIGN-PATH-AUTHORITY.md section 15.3): two
arms run ABBA or BAAB at random, more arms run a rotated order followed by its
mirror, and every arm appears twice per block. Gateways are restarted before
every block, so an arm's first trial in a block is cold and its second starts
from that arm's own first trial.

Safety, in this order:

* No host name or address is in this file. Everything comes from a config
  file that is git-ignored (`*.local.ini`); an example is committed.
* Every host the campaign touches must have a `[production:...]` section, even
  an empty one, so nothing is touched that was not looked at first.
* Before anything starts, the runner snapshots every production host (systemd
  units' MainPID, NRestarts, state and executable SHA-256; launchd labels;
  every process matching a pattern; every listening port) and refuses to run if
  any planned temporary port is already listening, is a declared production
  port, or if leftovers of an earlier campaign with the same name are running.
* Every temporary file lives under `~/qq-ab-<name>` on each host. Every
  temporary process is started from there with a pid file and is only ever
  signalled after its command line has been checked to contain that directory.
* Production is snapshotted again every `snapshot_every` blocks and at the end;
  any change to a production unit or process aborts the campaign, tears it
  down, and exits 3.
* Teardown always runs (normal exit, error, SIGINT, SIGTERM).

`--dry-run` prints the plan, the schedule, and every command the first block
would run, and contacts no host and builds nothing.
"""
from __future__ import annotations

import argparse
import configparser
import datetime as dt
import hashlib
import json
import os
import random
import re
import shlex
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

# ab_workloads imports this module by name; when this file runs as a script,
# make that name refer to this very module rather than a second copy.
sys.modules.setdefault("ab_campaign", sys.modules[__name__])
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,39}$")
ARM_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,23}$")
WORK_PREFIX = "qq-ab-"
# curl --max-time of one probe request; an unfinished probe counts as this long.
PROBE_TIMEOUT = 10
# Exit status of the ownership guard: the work directory is not this
# invocation's, so nothing was done to it.
NOT_OWNER = 9
# Seconds between dispatching a DC phase's jobs and their shared start.
BARRIER_LEAD = 5
# Millisecond wall-clock time on a campaign host. Whole seconds (the last
# fallback) are not precise enough to show overlap, and the analyzers treat
# such rows as unmeasured rather than guess.
NOW_SH = ("now() { perl -MTime::HiRes=time -e 'printf \"%.3f\", time' 2>/dev/null || "
          "python3 -c 'import time; print(\"%.3f\" % time.time(), end=\"\")' 2>/dev/null || date +%s; }; ")
# The runner owns these options; an arm or host that set them could point a
# temporary process at production state, a production port or a production log.
OWNED_FLAGS = ("state", "listen", "metrics-listen", "log-file", "profile",
               "log-stderr", "log-max-size-mib", "providers", "endpoint")
# Options that open listeners the preflight does not reserve.
UNSUPPORTED_FLAGS = ("hop-port-count",)
CURL_ENV = ("env -u NO_PROXY -u no_proxy -u ALL_PROXY -u all_proxy -u HTTP_PROXY "
            "-u HTTPS_PROXY -u http_proxy -u https_proxy curl --noproxy ''")
TRIAL_COLUMNS = ("block", "path", "slot", "arm", "role", "arm_trial", "cold", "phase", "job",
                 "client", "kind", "flow", "start_utc", "end_utc", "http", "bytes", "seconds",
                 "curl_exit", "complete", "wall", "t_start", "t_end")


class ConfigError(Exception):
    pass


class Refused(Exception):
    """A safety check failed; nothing was started."""


class ProductionChanged(Exception):
    pass


# --- configuration -----------------------------------------------------------

def _words(v: str) -> List[str]:
    return shlex.split(v or "")


def _ports(v: str) -> List[int]:
    out: List[int] = []
    for tok in (v or "").replace(",", " ").split():
        if "-" in tok:
            a, b = (int(x) for x in tok.split("-", 1))
            if not 1 <= a <= b <= 65535:
                # A reversed or out-of-range declaration would silently
                # protect nothing.
                raise ConfigError(f"port range {tok!r} must be low-high within 1-65535")
            out.extend(range(a, b + 1))
        else:
            p = int(tok)
            if not 1 <= p <= 65535:
                raise ConfigError(f"port {tok!r} is out of range")
            out.append(p)
    return out


def _bool(v: str) -> bool:
    if str(v).strip().lower() in ("1", "true", "yes", "on"):
        return True
    if str(v).strip().lower() in ("0", "false", "no", "off", ""):
        return False
    raise ConfigError(f"not a boolean: {v!r}")


def _check_flags(where: str, flags: Sequence[str]) -> None:
    for f in flags:
        if not f.startswith("-"):
            continue
        # Go's flag package accepts -name and --name alike.
        name = f.split("=", 1)[0].lstrip("-")
        if name in OWNED_FLAGS:
            raise ConfigError(f"{where}: --{name} is set by the runner and may not be configured")
        if name in UNSUPPORTED_FLAGS:
            raise ConfigError(f"{where}: --{name} opens listeners the preflight cannot reserve; not supported")


@dataclass
class Arm:
    name: str
    index: int
    role: str
    ref: str = ""
    gateway_ref: str = ""
    client_ref: str = ""
    binaries: Dict[str, str] = field(default_factory=dict)
    flags: List[str] = field(default_factory=list)
    gateway_flags: List[str] = field(default_factory=list)
    client_flags: List[str] = field(default_factory=list)


@dataclass
class Gateway:
    host: str
    address: str
    platform: str
    port_base: int
    metrics_port_base: int
    origin_port: int
    flags: List[str]


@dataclass
class Client:
    name: str
    host: str
    small: bool
    platform: str
    socks_port_base: int
    metrics_port_base: int
    flags: List[str]
    cpu_sample: bool


@dataclass
class Production:
    name: str
    host: str
    units: List[str]
    launchd: List[str]
    ports: List[int]
    process_pattern: str


@dataclass
class Config:
    name: str
    blocks: int
    seed: int
    workload: str
    flows: int
    object_bytes: int
    duration: int
    gap: float
    block_gap: float
    probe_bytes: int
    probe_interval: float
    snapshot_every: int
    lane_trace: bool
    log_max_mib: int
    ssh_options: List[str]
    gateway: Gateway
    clients: Dict[str, Client]
    arms: List[Arm]
    production: List[Production]
    wl: Any = None                  # the ab_workloads.Workload
    dc: Any = None                  # the dc workload's parameters

    @property
    def work_dir(self) -> str:
        return WORK_PREFIX + self.name

    @property
    def main(self) -> Arm:
        return next(a for a in self.arms if a.role == "main")

    def hosts(self) -> List[str]:
        seen: List[str] = []
        for h in [self.gateway.host] + [c.host for c in self.clients.values()]:
            if h not in seen:
                seen.append(h)
        return seen

    def platforms(self) -> List[str]:
        out = [self.gateway.platform]
        for c in self.clients.values():
            if c.platform not in out:
                out.append(c.platform)
        return out

    def paths(self) -> List[str]:
        """Clients that are measured paths. The DC workload runs every leaf at
        once, so it has a single path."""
        return self.wl.paths(self)

    def planned_ports(self) -> Dict[str, List[Tuple[int, str]]]:
        out: Dict[str, List[Tuple[int, str]]] = {}
        g = self.gateway
        for a in self.arms:
            out.setdefault(g.host, []).append((g.port_base + a.index, f"gateway {a.name}"))
            out.setdefault(g.host, []).append((g.metrics_port_base + a.index, f"gateway metrics {a.name}"))
        out.setdefault(g.host, []).append((g.origin_port, "origin"))
        for ep in (self.wl.endpoints(self) if self.wl else []):
            for port, what in ep.tcp_ports + ep.udp_ports:
                out.setdefault(g.host, []).append((port, f"{ep.tag} {what}"))
        for c in self.clients.values():
            for a in self.arms:
                out.setdefault(c.host, []).append((c.socks_port_base + a.index, f"client {c.name} socks {a.name}"))
                out.setdefault(c.host, []).append((c.metrics_port_base + a.index, f"client {c.name} metrics {a.name}"))
        return out


def load_config(path: str) -> Config:
    cp = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=(";", "#"))
    if not cp.read(path):
        raise ConfigError(f"cannot read {path}")
    return parse_config(cp)


def parse_config(cp: configparser.ConfigParser) -> Config:
    if not cp.has_section("campaign") or not cp.has_section("gateway"):
        raise ConfigError("config needs [campaign] and [gateway]")
    c = cp["campaign"]
    name = c.get("name", "")
    if not NAME_RE.match(name):
        raise ConfigError(f"campaign name {name!r} must match {NAME_RE.pattern}")
    workload = c.get("workload", "bulk")
    import ab_workloads
    wl = ab_workloads.get(workload)
    g = cp["gateway"]
    for k in ("host", "address"):
        if not g.get(k):
            raise ConfigError(f"[gateway] {k} is required")
    gw = Gateway(host=g["host"], address=g["address"], platform=g.get("platform", "linux/amd64"),
                 port_base=g.getint("port_base", 23471), metrics_port_base=g.getint("metrics_port_base", 23491),
                 origin_port=g.getint("origin_port", 23480), flags=_words(g.get("flags", "")))
    _check_flags("[gateway] flags", gw.flags)

    clients: Dict[str, Client] = {}
    arms: List[Arm] = []
    prod: List[Production] = []
    for sec in cp.sections():
        s = cp[sec]
        if sec.startswith("client:"):
            cname = sec.split(":", 1)[1]
            if not ARM_RE.match(cname) or cname == "dc":
                raise ConfigError(f"client label {cname!r} must match {ARM_RE.pattern} and not be 'dc'")
            if not s.get("host"):
                raise ConfigError(f"[{sec}] host is required")
            cl = Client(name=cname, host=s["host"], small=_bool(s.get("small", "false")),
                        platform=s.get("platform", "linux/amd64"),
                        socks_port_base=s.getint("socks_port_base", 23581),
                        metrics_port_base=s.getint("metrics_port_base", 23591),
                        flags=_words(s.get("flags", "")), cpu_sample=_bool(s.get("cpu_sample", "true")))
            _check_flags(f"[{sec}] flags", cl.flags)
            clients[cname] = cl
        elif sec.startswith("arm:"):
            aname = sec.split(":", 1)[1]
            if not ARM_RE.match(aname):
                raise ConfigError(f"arm name {aname!r} must match {ARM_RE.pattern}")
            bins = {k[len("binary_"):].replace("_", "/", 1): v for k, v in s.items() if k.startswith("binary_")}
            ref = s.get("ref", "")
            arm = Arm(name=aname, index=len(arms), role=s.get("role", "candidate"), ref=ref,
                      gateway_ref=s.get("gateway_ref", ref), client_ref=s.get("client_ref", ref),
                      binaries=bins, flags=_words(s.get("flags", "")),
                      gateway_flags=_words(s.get("gateway_flags", "")), client_flags=_words(s.get("client_flags", "")))
            if arm.role not in ("main", "candidate"):
                raise ConfigError(f"[{sec}] role must be main or candidate")
            # gateway_ref / client_ref build the two ends from different
            # refs: a crossover arm (new gateway, released client) for the
            # "gateways first" order of stage 10, or a 2x2 attribution.
            has_refs = bool(arm.gateway_ref) or bool(arm.client_ref)
            if has_refs and not (arm.gateway_ref and arm.client_ref):
                raise ConfigError(f"[{sec}] needs ref =, or both gateway_ref = and client_ref =")
            if has_refs == bool(arm.binaries):
                raise ConfigError(f"[{sec}] needs exactly one of ref (or gateway_ref and client_ref) or binary_<os>_<arch> =")
            for w, fl in (("flags", arm.flags), ("gateway_flags", arm.gateway_flags), ("client_flags", arm.client_flags)):
                _check_flags(f"[{sec}] {w}", fl)
            arms.append(arm)
        elif sec.startswith("production:"):
            if not s.get("host"):
                raise ConfigError(f"[{sec}] host is required")
            prod.append(Production(name=sec.split(":", 1)[1], host=s["host"], units=_words(s.get("units", "")),
                                   launchd=_words(s.get("launchd", "")), ports=_ports(s.get("ports", "")),
                                   process_pattern=s.get("process_pattern", "queqiao")))
    if len(arms) < 2:
        raise ConfigError("at least two [arm:...] sections are required")
    if sum(1 for a in arms if a.role == "main") != 1:
        raise ConfigError("exactly one arm must have role = main")
    if not clients:
        raise ConfigError("at least one [client:...] section is required")

    cfg = Config(name=name, blocks=c.getint("blocks", 12), seed=c.getint("seed", 1), workload=workload,
                 flows=c.getint("flows", 4), object_bytes=c.getint("object_bytes", 67108864),
                 duration=c.getint("duration", 90), gap=c.getfloat("gap", 5.0), block_gap=c.getfloat("block_gap", 10.0),
                 probe_bytes=c.getint("probe_bytes", 1024), probe_interval=c.getfloat("probe_interval", 0.5),
                 snapshot_every=c.getint("snapshot_every", 4), lane_trace=_bool(c.get("lane_trace", "true")),
                 log_max_mib=c.getint("log_max_mib", 1024), ssh_options=_words(c.get("ssh_options", "-o ConnectTimeout=20 -o ServerAliveInterval=15")),
                 gateway=gw, clients=clients, arms=arms, production=prod, wl=wl)
    wl.configure(cfg, cp)
    owned = wl.owned_flags()
    for where, fl in [("[gateway] flags", gw.flags)] + [(f"[client:{c.name}] flags", c.flags) for c in clients.values()] + \
            [(f"[arm:{a.name}]", a.flags + a.gateway_flags + a.client_flags) for a in arms]:
        for f in fl:
            if f.startswith("-") and f.lstrip("-").split("=", 1)[0] in owned:
                raise ConfigError(f"{where}: workload {workload} sets --{f.lstrip('-').split('=', 1)[0]} on both ends; "
                                  "do not set it")
    if cfg.blocks < 1:
        raise ConfigError("blocks must be positive")
    # curl --max-time 0 means no limit, which is not a fixed window.
    checks = [("duration", cfg.duration)] + wl.positive(cfg)
    for name, v in checks:
        if not v > 0:
            raise ConfigError(f"{name} must be positive")
    for host, ports in cfg.planned_ports().items():
        seen: Dict[int, str] = {}
        for p, what in ports:
            if p in seen:
                raise ConfigError(f"port {p} on {host} planned twice: {seen[p]} and {what}")
            seen[p] = what
    covered = {p.host for p in prod}
    missing = [h for h in cfg.hosts() if h not in covered]
    if missing:
        raise ConfigError("every host the campaign touches needs a [production:...] section, even one that "
                          "declares nothing; missing: " + ", ".join(missing))
    for arm in arms:
        for plat in cfg.platforms():
            if arm.binaries and plat not in arm.binaries:
                raise ConfigError(f"[arm:{arm.name}] has no binary_{plat.replace('/', '_')}")
    return cfg


# --- schedule ------------------------------------------------------------------

@dataclass
class Slot:
    block: int
    path: str
    slot: int
    arm: str
    arm_trial: int
    cold: bool


def block_order(n_arms: int, block: int, rng: random.Random) -> List[int]:
    """Arm indices for one block; every arm appears exactly twice.

    Two arms: ABBA or BAAB, chosen at random (section 15.3). More arms: a
    rotation of the arm list followed by its mirror, the rotation advancing
    each block and the direction reversing every n blocks, so over n * 2
    blocks every arm takes every position equally often.
    """
    if n_arms == 2:
        return [0, 1, 1, 0] if rng.random() < 0.5 else [1, 0, 0, 1]
    k = (block - 1) % n_arms
    base = [(k + i) % n_arms for i in range(n_arms)]
    if ((block - 1) // n_arms) % 2 == 1:
        base.reverse()
    return base + base[::-1]


def schedule(cfg: Config) -> List[Slot]:
    rng = random.Random(cfg.seed)
    out: List[Slot] = []
    paths = cfg.paths()
    for b in range(1, cfg.blocks + 1):
        k = (b - 1) % len(paths)
        for path in paths[k:] + paths[:k]:
            seen: Dict[int, int] = {}
            for i, ai in enumerate(block_order(len(cfg.arms), b, rng), start=1):
                seen[ai] = seen.get(ai, 0) + 1
                out.append(Slot(b, path, i, cfg.arms[ai].name, seen[ai], seen[ai] == 1))
    return out


# --- hosts -------------------------------------------------------------------

class Host:
    """Runs shell on one host: locally when dest is "local", else over ssh."""

    def __init__(self, dest: str, ssh_options: Sequence[str], dry: bool, out=None):
        self.dest = dest
        self.ssh_options = list(ssh_options)
        self.dry = dry
        self.out = out or sys.stdout
        self.home = "~" if dry else ""
        self.lock = threading.Lock()

    def _argv(self) -> List[str]:
        if self.dest == "local":
            return ["bash", "-s"]
        return ["ssh", *self.ssh_options, "-o", "BatchMode=yes", self.dest, "bash", "-s"]

    def run(self, script: str, timeout: Optional[float] = 120, what: str = "") -> Tuple[int, str]:
        if self.dry:
            with self.lock:
                print(f"[{self.dest}] {what or 'run'}:", file=self.out)
                for line in script.strip().splitlines():
                    print(f"    {line}", file=self.out)
            return 0, ""
        p = subprocess.run(self._argv(), input="set -uo pipefail\n" + script, capture_output=True, text=True,
                           timeout=timeout)
        return p.returncode, p.stdout + (("\n" + p.stderr) if p.returncode and p.stderr else "")

    def put(self, local: str, remote: str) -> None:
        if self.dry:
            print(f"[{self.dest}] copy {local} -> {remote}", file=self.out)
            return
        if self.dest == "local":
            subprocess.run(["cp", local, remote], check=True)
        else:
            subprocess.run(["scp", "-q", *self.ssh_options, local, f"{self.dest}:{remote}"], check=True)

    def get(self, remote: str, local: str) -> bool:
        if self.dry:
            print(f"[{self.dest}] fetch {remote} -> {local}", file=self.out)
            return True
        os.makedirs(os.path.dirname(local), exist_ok=True)
        if self.dest == "local":
            return subprocess.run(["cp", remote, local]).returncode == 0
        return subprocess.run(["scp", "-q", *self.ssh_options, f"{self.dest}:{remote}", local]).returncode == 0


# Snapshot of what production looks like on one host. Read-only: systemctl
# show, launchctl list, ps, ss/lsof, sha256 of executables.
SNAPSHOT_SH = r'''
W={work}
os=$(uname -s)
echo "os $os"
for u in {units}; do
  # unit-error when the unit cannot be observed: the preflight refuses then,
  # rather than comparing two empty observations as "unchanged".
  if ! v=$(systemctl show "$u" -p LoadState -p ActiveState -p MainPID -p NRestarts -p ExecMainStartTimestampMonotonic 2>/dev/null | sort | tr '\n' ' ') \
     || [ -z "$v" ] || echo "$v" | grep -q 'LoadState=not-found'; then
    echo "unit-error $u"; continue
  fi
  pid=$(systemctl show "$u" -p MainPID --value 2>/dev/null)
  exe=""; sha=""
  if [ -n "$pid" ] && [ "$pid" != 0 ]; then exe=$(readlink -f /proc/$pid/exe 2>/dev/null); sha=$( (sha256sum "/proc/$pid/exe" 2>/dev/null || true) | cut -c1-64); fi
  echo "unit $u $v exe=$exe sha=$sha"
done
for l in {launchd}; do
  v=$(launchctl list 2>/dev/null | awk -v l="$l" '$3==l{{print $1, $2}}')
  if [ -n "$v" ]; then echo "launchd $l $v"; else echo "unit-error $l"; fi
done
# "ps-ok" only when the process table was actually read: an empty list from
# a failed ps must not read as "no production processes".
if procs=$(ps -eo pid=,lstart=,args= 2>/dev/null) && [ -n "$procs" ]; then
  echo ps-ok
  printf '%s\n' "$procs" | grep -i -E -- {pattern} | grep -v -F -- "$W/" | grep -v -E 'grep|ps -eo' | sed 's/^ */proc /'
  printf '%s\n' "$procs" | grep -F -- "$W/" | grep -v -F 'grep' | awk '{{$2=$3=$4=$5=$6=""; print}}' | sed 's/^ */leftover /;s/  */ /g'
fi
# "listen-ok" only when the listener table was actually read; without it the
# preflight refuses, rather than trusting an empty list.
if [ "$os" = Linux ]; then
  if l=$(ss -Hltnu 2>/dev/null); then
    echo listen-ok
    printf '%s\n' "$l" | awk '{{print $1, $5}}' | sed -E 's/^([a-z]+) .*:([0-9]+)$/listen \1 \2/'
  fi
elif command -v lsof >/dev/null 2>&1; then
  l=$(lsof -nP -iTCP -sTCP:LISTEN -iUDP 2>/dev/null)
  if [ -n "$l" ]; then
    echo listen-ok
    printf '%s\n' "$l" | awk 'NR>1{{print $8, $9}}' | sed -E 's/^([A-Z]+) .*:([0-9]+)$/listen \1 \2/' | tr 'A-Z' 'a-z'
  fi
fi
exit 0
'''


def parse_snapshot(text: str) -> dict:
    snap = {"units": {}, "launchd": {}, "procs": [], "listen": set(), "leftover": [], "unit_errors": []}
    for line in text.splitlines():
        parts = line.split(" ", 1)
        if len(parts) < 2:
            continue
        kind, rest = parts
        if kind == "unit":
            name, _, v = rest.partition(" ")
            snap["units"][name] = v.strip()
        elif kind == "launchd":
            name, _, v = rest.partition(" ")
            snap["launchd"][name] = v.strip()
        elif kind == "proc":
            snap["procs"].append(rest.strip())
        elif kind == "listen":
            toks = rest.split()
            if len(toks) == 2 and toks[1].isdigit():
                snap["listen"].add(int(toks[1]))
        elif kind == "leftover":
            snap["leftover"].append(rest.strip())
        elif kind == "unit-error":
            snap["unit_errors"].append(rest.strip())
        elif kind == "os":
            snap["os"] = rest.strip()
    snap["listen_ok"] = any(line.strip() == "listen-ok" for line in text.splitlines())
    snap["ps_ok"] = any(line.strip() == "ps-ok" for line in text.splitlines())
    return snap


def snapshot_diff(before: dict, after: dict) -> List[str]:
    """Changes to production units, launchd jobs and matching processes."""
    out = []
    for kind in ("units", "launchd"):
        for name, v in before.get(kind, {}).items():
            w = after.get(kind, {}).get(name)
            if w != v:
                out.append(f"{kind[:-1] if kind == 'units' else kind} {name}: {v!r} -> {w!r}")
    for flag, what in (("ps_ok", "process table"), ("listen_ok", "listening ports")):
        if before.get(flag) and not after.get(flag):
            out.append(f"the {what} can no longer be read, so production cannot be verified")
    for u in sorted(set(after.get("unit_errors", [])) - set(before.get("unit_errors", []))):
        out.append(f"production unit {u} can no longer be observed")
    gone = sorted(set(before.get("procs", [])) - set(after.get("procs", [])))
    new = sorted(set(after.get("procs", [])) - set(before.get("procs", [])))
    out += [f"process ended or restarted: {p}" for p in gone]
    out += [f"process appeared: {p}" for p in new]
    return out


def refusal_reasons(cfg: Config, snaps: Dict[str, dict]) -> List[str]:
    """Why the campaign must not start, given each host's snapshot."""
    reasons = []
    declared: Dict[str, Dict[int, str]] = {}
    for p in cfg.production:
        for port in p.ports:
            declared.setdefault(p.host, {})[port] = p.name
    for host, ports in cfg.planned_ports().items():
        snap = snaps.get(host, {})
        if not snap.get("listen_ok"):
            reasons.append(f"{host}: could not read the listening ports (need ss on Linux, lsof elsewhere)")
        if not snap.get("ps_ok"):
            reasons.append(f"{host}: could not read the process table (ps -eo pid=,lstart=,args=)")
        listening = snap.get("listen", set())
        for port, what in ports:
            if port in declared.get(host, {}):
                reasons.append(f"{host}: {what} port {port} is declared production port of [{declared[host][port]}]")
            if port in listening:
                reasons.append(f"{host}: {what} port {port} is already listening")
        for left in snap.get("leftover", []):
            reasons.append(f"{host}: a process from an earlier campaign is running: {left}")
    for p in cfg.production:
        snap = snaps.get(p.host, {})
        for u in snap.get("unit_errors", []):
            if u in p.units or u in p.launchd:
                reasons.append(f"{p.host}: declared production unit {u} cannot be observed (not found, or no access)")
        for u in p.units:
            v = snap.get("units", {}).get(u, "")
            if cfg_work_in(v, cfg.work_dir):
                reasons.append(f"{p.host}: production unit {u} runs from the campaign directory")
    return reasons


def cfg_work_in(text: str, work: str) -> bool:
    return f"/{work}/" in text


# --- the campaign ------------------------------------------------------------

def flag_value(flags: Sequence[str], name: str) -> Optional[str]:
    """The value Go's flag package would take for -name/--name from these
    flags: either spelling, "=value" or a separate value, last one wins."""
    value = None
    i = 0
    while i < len(flags):
        f = flags[i]
        if f.startswith("-"):
            key, eq, v = f.lstrip("-").partition("=")
            if key == name:
                if eq:
                    value = v
                elif i + 1 < len(flags):
                    value = flags[i + 1]
                    i += 1
        i += 1
    return value


def without_flag(flags: Sequence[str], name: str) -> List[str]:
    """flags with every -name/--name option (and its value) removed."""
    out: List[str] = []
    i = 0
    while i < len(flags):
        f = flags[i]
        if f.startswith("-") and f.lstrip("-").partition("=")[0] == name:
            i += 1 if "=" in f else 2
            continue
        out.append(f)
        i += 1
    return out


def local_address_args(flags: Sequence[str]) -> List[str]:
    """The client's effective --local-address, which enrollment needs too so
    its connection takes the same physical interface. Pass the flags in the
    order the client gets them."""
    v = flag_value(flags, "local-address")
    return ["--local-address", v] if v else []


def _q(s: str) -> str:
    return shlex.quote(s)


FLOW_RE = re.compile(r"^(b[0-9]+o[0-9]+|w[0-9]+r[0-9]+|p[0-9]+)$")
TIME_RE = re.compile(r"^[0-9]+(\.[0-9]+)?$")


def job_rows(out: str, kind: str, n: int, waves: int, duration: float) -> List[Tuple[str, ...]]:
    """Parse a job's output into (flow, http, bytes, seconds, curl_exit,
    t_start, t_end), the times on the job host's clock ("" if unknown).

    Bulk flows are b<i>o<k>: flow i's k-th object. A flow that printed
    nothing usable (curl killed, ssh dropped) is still a row: 0 bytes, HTTP
    000, exit -1, so it counts against completion instead of disappearing
    from the denominator. A probe has no fixed count.
    """
    got: Dict[str, Tuple[str, ...]] = {}
    order: List[str] = []
    for line in out.splitlines():
        parts = line.rstrip("\n").split("\t")
        if len(parts) < 3 or not FLOW_RE.match(parts[2]):
            continue
        t0 = parts[0] if TIME_RE.match(parts[0]) else ""
        t1 = parts[1] if TIME_RE.match(parts[1]) else ""
        flow = parts[2]
        f = (parts[3:] + ["", "", "", ""])[:4]
        code = f[0] if f[0].isdigit() else "000"
        size = f[1] if f[1].isdigit() else "0"
        secs = f[2] or f"{duration:.3f}"
        ex = f[3] if re.match(r"^-?[0-9]+$", f[3] or "") else "-1"
        if flow not in got:
            order.append(flow)
        got[flow] = (flow, code, size, secs, ex, t0, t1)
    if kind == "bulk":
        want = [f"b{i}o1" for i in range(1, n + 1)]
    elif kind == "requests":
        want = [f"w{w}r{i}" for w in range(1, waves + 1) for i in range(1, n + 1)]
    else:
        want = []
    for flow in want:
        if flow not in got:
            order.append(flow)
            got[flow] = (flow, "000", "0", f"{duration:.3f}", "-1", "", "")
    return [got[f] for f in order]


def wait_owned_script(pid_file: str, port: int) -> str:
    """Exit 0 once the pid in pid_file is alive and owns a TCP listener on
    port (ss -p on Linux, lsof elsewhere); 1 if it dies or never does."""
    return f'''p=$(cat {pid_file} 2>/dev/null)
[ -n "$p" ] || {{ echo "no pid file"; exit 1; }}
for i in $(seq 1 80); do
  kill -0 "$p" 2>/dev/null || {{ echo "process $p exited"; exit 1; }}
  if command -v ss >/dev/null 2>&1; then
    ss -Hltnp "sport = :{port}" 2>/dev/null | grep -q "pid=$p," && exit 0
  else
    lsof -nP -a -p "$p" -iTCP:{port} -sTCP:LISTEN >/dev/null 2>&1 && exit 0
  fi
  sleep 0.25
done
echo "process $p does not own port {port}"; exit 1'''


def origin_script(W: str, objects: Sequence[Tuple[str, int]]) -> str:
    """Create the origin's objects at their exact sizes, or fail. A truncated
    object (a full disk, an interrupted earlier run) would make every arm
    benchmark the same undersized payload."""
    mk = [f"mkdir -p {W}/origin {W}/run"]
    for name, size in objects:
        f = f"{W}/origin/{name}"
        mk.append(f"[ \"$(( $(wc -c < {f} 2>/dev/null || echo 0) ))\" -eq {size} ] || "
                  f"{{ head -c {size} /dev/urandom > {f}.tmp && mv {f}.tmp {f}; }}")
        mk.append(f"[ \"$(( $(wc -c < {f}) ))\" -eq {size} ] || {{ echo 'origin object {name} is not {size} bytes'; exit 1; }}")
    return "\n".join(mk)


def cpu_sampler(pid_file: str, out_file: str) -> str:
    """Shell that samples a process's cumulative CPU once a second as
    "epoch ticks" lines, 100 ticks per second: /proc on Linux, ps's
    cumulative time ([[H:]M]M:SS.ss) elsewhere. It ends with the process."""
    return f'''p=$(cat {pid_file})
( while kill -0 $p 2>/dev/null; do
    if [ -r /proc/$p/stat ]; then t=$(awk '{{print $14+$15}}' /proc/$p/stat)
    else t=$(ps -o time= -p $p | awk -F: '{{s=0; for (i=1;i<=NF;i++) s=s*60+$i; printf "%d", s*100}}'); fi
    [ -n "$t" ] && echo "$(date +%s) $t"; sleep 1
  done ) > {out_file} 2>/dev/null < /dev/null &'''


def utc(t: float) -> str:
    return dt.datetime.fromtimestamp(t, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class Campaign:
    def __init__(self, cfg: Config, out_dir: str, repo: str, dry: bool, stream=None):
        stream = stream or sys.stdout
        self.cfg = cfg
        # Builds run with the worktree as their directory: paths must be absolute.
        self.out = out_dir if dry else os.path.abspath(out_dir)
        self.repo = repo
        self.dry = dry
        self.stream = stream
        self.hosts: Dict[str, Host] = {h: Host(h, cfg.ssh_options, dry, stream)
                                      for h in cfg.hosts() + [p.host for p in cfg.production]}
        self.before: Dict[str, dict] = {}
        self.active_small: Dict[str, str] = {}   # host -> running temporary client tag
        self.fetch_failures: List[str] = []
        self.reserved: List[str] = []
        self.production_changed = False
        self.job_seq = 0
        self.token = "dry-run-token" if dry else os.urandom(16).hex()
        self.started = False

    # paths on a host
    def w(self, host: str) -> str:
        h = self.hosts[host]
        return f"{h.home}/{self.cfg.work_dir}" if h.home else f"~/{self.cfg.work_dir}"

    def must(self, host: Host, script: str, what: str, timeout: Optional[float] = 120) -> str:
        """Run a setup step that has to succeed."""
        rc, out = host.run(script, timeout=timeout, what=what)
        if rc != 0 and not self.dry:
            raise RuntimeError(f"{host.dest}: {what} failed (exit {rc}): {out.strip()[:400]}")
        return out

    def log(self, msg: str) -> None:
        print(f"{utc(time.time())} {msg}", file=self.stream, flush=True)

    # -- safety
    def snapshot_all(self, tag: str) -> Dict[str, dict]:
        snaps: Dict[str, dict] = {}
        by_host: Dict[str, List[Production]] = {}
        for p in self.cfg.production:
            by_host.setdefault(p.host, []).append(p)
        for host, prods in by_host.items():
            units = " ".join(_q(u) for p in prods for u in p.units)
            launchd = " ".join(_q(u) for p in prods for u in p.launchd)
            pattern = "|".join(p.process_pattern for p in prods if p.process_pattern) or "queqiao"
            script = SNAPSHOT_SH.format(work=_q(self.w(host)), units=units, launchd=launchd, pattern=_q(pattern))
            rc, text = self.hosts[host].run(script, what=f"read-only production snapshot ({tag})")
            if not self.dry and rc != 0:
                raise Refused(f"{host}: snapshot failed (exit {rc}): {text.strip()[:400]}")
            snaps[host] = parse_snapshot(text)
            if not self.dry:
                with open(os.path.join(self.out, f"prod-{tag}.txt"), "a") as f:
                    f.write(f"### {host} {utc(time.time())}\n{text}\n")
        return snaps

    def check_production(self, tag: str) -> None:
        now = self.snapshot_all(tag)
        if self.dry:
            return
        changes = []
        for host, snap in now.items():
            changes += [f"{host}: {c}" for c in snapshot_diff(self.before.get(host, {}), snap)]
        if changes:
            # Latched: a change seen once stays in the final status even if
            # it is gone by the last snapshot.
            self.production_changed = True
            with open(os.path.join(self.out, f"prod-diff-{tag}.txt"), "w") as f:
                f.write("\n".join(changes) + "\n")
            raise ProductionChanged("; ".join(changes))
        self.log(f"production unchanged ({tag})")

    def preflight(self) -> None:
        if not self.dry:
            for host in self.hosts.values():
                rc, home = host.run('printf %s "$HOME"', what="home directory")
                if rc != 0 or not re.match(r"^/[A-Za-z0-9._/-]+$", home.strip()):
                    # Paths are interpolated into shell unquoted; refuse
                    # anything that could split or expand.
                    raise Refused(f"{host.dest}: $HOME must be an absolute path of [A-Za-z0-9._/-] ({home.strip()[:200]!r})")
                host.home = home.strip()
        self.before = self.snapshot_all("before")
        if self.dry:
            print("[preflight] would refuse if any of these is listening, declared production, or if "
                  f"a process under ~/{self.cfg.work_dir}/ is already running:", file=self.stream)
            for host, ports in self.cfg.planned_ports().items():
                print(f"    {host}: " + ", ".join(f"{p} ({w})" for p, w in ports), file=self.stream)
            self.reserve()
            return
        reasons = refusal_reasons(self.cfg, self.before)
        if reasons:
            raise Refused("\n".join(reasons))
        self.reserve()

    def reserve(self) -> None:
        """Claim ~/qq-ab-<name> on every host with an atomic mkdir and an
        owner token. A second invocation with the same name finds the
        directory and is refused; teardown and cleanup act only on
        directories this invocation owns."""
        for host in self.cfg.hosts():
            W = self.w(host)
            try:
                rc, out = self.hosts[host].run(f"mkdir {W} || exit 3\nmkdir {W}/run && printf %s {self.token} > {W}/.owner",
                                               what="reserve the work directory")
            except BaseException as e:
                # The mkdir may or may not have happened: give back what is
                # confirmed and name the host whose state is unknown.
                self.release()
                raise Refused(f"{host}: reserving {W} failed ({type(e).__name__}: {e}); its outcome is unknown, so check "
                              f"for {W} there (owner token {self.token}) before retrying") from e
            if rc != 0 and not self.dry:
                self.release()
                raise Refused(f"{host}: {W} already exists (another campaign with this name, or a kept one); "
                              "remove it after checking nothing of it is running")
            self.reserved.append(host)

    def owned(self, host: str) -> str:
        """Shell guard: exit unless this invocation owns the work directory."""
        W = self.w(host)
        return f'[ "$(cat {W}/.owner 2>/dev/null)" = "{self.token}" ] || {{ echo "not owner of {W}"; exit {NOT_OWNER}; }}\n'

    def release(self) -> None:
        for host in list(self.reserved):
            W = self.w(host)
            try:
                rc, _ = self.hosts[host].run(self.owned(host) + f"rm -f {W}/.owner; rmdir {W}/run {W} 2>/dev/null; true",
                                             what="release an unused reservation")
                if rc == NOT_OWNER and not self.dry:
                    self.log(f"{host}: {W} is no longer this campaign's; left alone")
                self.reserved.remove(host)
            except Exception as e:
                self.log(f"{host}: could not release {W} ({e}); remove it by hand (owner token {self.token})")

    # -- builds
    def refs(self) -> Dict[str, List[str]]:
        """Each ref to build, with the platforms it is needed for."""
        need: Dict[str, List[str]] = {}
        for arm in self.cfg.arms:
            if arm.binaries:
                continue
            pairs = [(arm.gateway_ref, self.cfg.gateway.platform)] + \
                    [(arm.client_ref, c.platform) for c in self.cfg.clients.values()]
            for ref, plat in pairs:
                if plat not in need.setdefault(ref, []):
                    need[ref].append(plat)
        return need

    def binary(self, arm: Arm, platform: str, end: str) -> str:
        if arm.binaries:
            return arm.binaries[platform]
        ref = arm.gateway_ref if end == "gateway" else arm.client_ref
        k = list(self.refs()).index(ref)
        return os.path.join(self.out, "bin", f"ref{k}", f"queqiaod-{platform.replace('/', '-')}")

    def build(self) -> Dict[str, dict]:
        commits: Dict[str, str] = {}
        for k, (ref, plats) in enumerate(self.refs().items()):
            if self.dry:
                print(f"[local] build {ref}: git worktree add --detach <tmp> {ref}; CGO_ENABLED=0 GOOS=.. GOARCH=.. "
                      f"go build -trimpath ./cmd/queqiaod for {', '.join(plats)}", file=self.stream)
                continue
            sha = subprocess.run(["git", "-C", self.repo, "rev-parse", "--verify", ref + "^{commit}"],
                                 capture_output=True, text=True, check=True).stdout.strip()
            commits[ref] = sha
            tree = os.path.join(self.out, "build", f"ref{k}")
            subprocess.run(["git", "-C", self.repo, "worktree", "add", "--detach", tree, sha],
                           check=True, capture_output=True)
            try:
                for plat in plats:
                    goos, goarch = plat.split("/")
                    dst = os.path.join(self.out, "bin", f"ref{k}", f"queqiaod-{plat.replace('/', '-')}")
                    os.makedirs(os.path.dirname(dst), exist_ok=True)
                    env = dict(os.environ, GOOS=goos, GOARCH=goarch, CGO_ENABLED="0")
                    subprocess.run(["go", "build", "-trimpath", "-o", dst, "./cmd/queqiaod"], cwd=tree,
                                   env=env, check=True)
            finally:
                subprocess.run(["git", "-C", self.repo, "worktree", "remove", "--force", tree], capture_output=True)
        manifest: Dict[str, dict] = {}
        for arm in self.cfg.arms:
            entry = {"role": arm.role, "flags": arm.flags, "gateway_flags": arm.gateway_flags,
                     "client_flags": arm.client_flags, "binaries": {}}
            if not arm.binaries:
                entry.update(ref=arm.ref, gateway_ref=arm.gateway_ref, client_ref=arm.client_ref,
                             gateway_commit=commits.get(arm.gateway_ref, ""), client_commit=commits.get(arm.client_ref, ""))
                if arm.gateway_ref == arm.client_ref:
                    entry["commit"] = commits.get(arm.ref or arm.gateway_ref, "")
            ends = [("gateway", self.cfg.gateway.platform)] + [(f"client:{c.name}", c.platform) for c in self.cfg.clients.values()]
            for end, plat in ends:
                path = self.binary(arm, plat, "gateway" if end == "gateway" else "client")
                sha = "" if self.dry else hashlib.sha256(open(path, "rb").read()).hexdigest()
                entry["binaries"][end] = {"platform": plat, "path": path, "sha256": sha}
            manifest[arm.name] = entry
        return manifest

    # -- setup
    def gw_dir(self, arm: Arm) -> str:
        return f"{self.w(self.cfg.gateway.host)}/gw-{arm.name}"

    def cl_dir(self, client: Client, arm: Arm) -> str:
        return f"{self.w(client.host)}/cl-{client.name}-{arm.name}"

    def objects(self) -> List[Tuple[str, int]]:
        return self.cfg.wl.objects(self.cfg)

    def setup(self) -> None:
        cfg, g = self.cfg, self.cfg.gateway
        gh = self.hosts[g.host]
        W = self.w(g.host)
        self.must(gh, origin_script(W, self.objects()), "create work directory and origin objects", timeout=600)
        for host in cfg.hosts():
            if host != g.host:
                self.must(self.hosts[host], f"mkdir -p {self.w(host)}/run", "create work directory")
        # --directory puts the work directory in the command line, which is
        # what stop() and teardown() check before signalling anything.
        self.spawn(gh, "origin", f"{W}/origin",
                   f"python3 -m http.server {g.origin_port} --bind 127.0.0.1 --directory {W}/origin")
        self.wait_port(gh, g.origin_port, "origin", "origin")
        for ep in cfg.wl.endpoints(cfg):
            # A workload's own service on the gateway's loopback, started from
            # the work directory like every other temporary process.
            for f in ep.files:
                gh.put(f, f"{W}/{os.path.basename(f)}")
            self.spawn(gh, ep.tag, W, ep.command.replace("{W}", W))
            for port, what in ep.tcp_ports:
                self.wait_port(gh, port, f"{ep.tag} {what}", ep.tag)
        for arm in cfg.arms:
            d = self.gw_dir(arm)
            self.must(gh, f"mkdir -p {d}/logs", f"gateway directory for {arm.name}")
            gh.put(self.binary(arm, g.platform, "gateway"), f"{d}/queqiaod")
            # Fresh provider state every campaign: a kept work directory from
            # an earlier run must not supply this one's identity or users. The
            # account has no device or flow limit of its own (0: every device,
            # the gateway's --max-sessions), so a campaign measures the path,
            # not account admission.
            self.must(gh, f"chmod +x {d}/queqiaod\ncd {d} && rm -rf ./state && ./queqiaod provider init --state ./state "
                      f"--name {cfg.work_dir}-{arm.name} --endpoint {g.address}:{g.port_base + arm.index} >/dev/null && "
                      f"./queqiaod provider add-user --state ./state --name ab --max-flows 0 --max-clients 0 >/dev/null",
                      f"temporary provider state for {arm.name}")
        # Enrollment needs a running gateway; start each, enroll every client, stop.
        for arm in cfg.arms:
            self.start_gateway(arm, "enroll", enroll=True)
            for client in cfg.clients.values():
                self.enroll(client, arm)
            self.stop(gh, f"gw-{arm.name}")

    def enroll(self, client: Client, arm: Arm) -> None:
        gh, ch = self.hosts[self.cfg.gateway.host], self.hosts[client.host]
        d = self.cl_dir(client, arm)
        out = self.must(gh, f"cd {self.gw_dir(arm)} && ./queqiaod provider invite --state ./state --user ab --expires-in 1h",
                        f"invitation for {client.name}/{arm.name}")
        m = re.search(r"queqiao://\S+", out)
        invite = m.group(0) if m else "<invite>"
        if not self.dry and not m:
            raise RuntimeError(f"no invitation from gateway {arm.name}: {out.strip()[:300]}")
        self.must(ch, f"mkdir -p {d}/logs", f"client directory {client.name}/{arm.name}")
        ch.put(self.binary(arm, client.platform, "client"), f"{d}/queqiaod")
        local = " ".join(_q(x) for x in local_address_args(self.client_flags(client, arm)))
        # A profile left by an earlier run would point at another gateway:
        # remove it, enroll, and fail the campaign if enrollment fails.
        self.must(ch, f"chmod +x {d}/queqiaod\ncd {d} && rm -f {d}/profile.json && ./queqiaod enroll --invite {_q(invite)} "
                  f"--profile {d}/profile.json --device-name {self.cfg.work_dir}-{client.name} {local} >/dev/null && "
                  f"test -s {d}/profile.json", f"enroll {client.name} with {arm.name}")

    # -- processes
    def spawn(self, host: Host, tag: str, cwd: str, cmd: str, env: str = "") -> None:
        W = self.w(host.dest)
        host.run(f"cd {cwd} && {{ if command -v setsid >/dev/null; then {env} setsid nohup {cmd} > {W}/run/{tag}.out 2>&1 < /dev/null & "
                 f"else {env} nohup {cmd} > {W}/run/{tag}.out 2>&1 < /dev/null & fi; echo $! > {W}/run/{tag}.pid; }}",
                 what=f"start {tag}")

    def stop(self, host: Host, tag: str) -> None:
        W = self.w(host.dest)
        # Signal only a pid whose command line contains the work directory.
        host.run(f'''f={W}/run/{tag}.pid
[ -f "$f" ] || exit 0
p=$(cat "$f")
mine() {{ ps -o args= -p "$p" 2>/dev/null | grep -q -F -- "{W}/"; }}
if mine; then
  kill "$p" 2>/dev/null
  for i in $(seq 1 20); do mine || break; sleep 0.25; done
  # Escalate only if the same process is still there: a reused pid fails mine.
  mine && kill -9 "$p" 2>/dev/null
fi
rm -f "$f"''', what=f"stop {tag}")

    def wait_port(self, host: Host, port: int, what: str, tag: str) -> None:
        """Wait until the process this campaign spawned as `tag` is alive and
        is the one listening on `port`. A connection alone is not enough: a
        child that failed to bind would otherwise be mistaken for another
        campaign's listener on the same port, and its trials measured there."""
        rc, out = host.run(wait_owned_script(f"{self.w(host.dest)}/run/{tag}.pid", port), what=f"wait for {what} on {port}")
        if rc != 0 and not self.dry:
            raise RuntimeError(f"{host.dest}: {what} is not listening on {port} from its own process ({out.strip()[:200]})")

    def client_flags(self, client: Client, arm: Arm) -> List[str]:
        """A client's configured flags in the order its command line has them."""
        return [*client.flags, *arm.flags, *arm.client_flags]

    def gw_cmd(self, arm: Arm, logname: str, enroll: bool = False) -> str:
        g, cfg = self.cfg.gateway, self.cfg
        d = self.gw_dir(arm)
        flags = [*g.flags, *arm.flags, *arm.gateway_flags]
        if enroll:
            # Enrollment always runs over TCP; an arm measured with
            # --transport quic has no TCP listener, so enroll against a
            # TCP-capable gateway and restart with the arm's transport.
            flags = without_flag(flags, "transport") + ["--transport", "auto"]
        args = [f"{d}/queqiaod", "server", "--state", f"{d}/state", "--listen", f":{g.port_base + arm.index}",
                "--metrics-listen", f"127.0.0.1:{g.metrics_port_base + arm.index}", "--log-file", f"{d}/logs/{logname}.jsonl",
                "--log-max-size-mib", str(cfg.log_max_mib), "--log-stderr=false", *flags]
        args += cfg.wl.gateway_flags(cfg)
        return " ".join(_q(a) for a in args)

    def start_gateway(self, arm: Arm, logname: str, enroll: bool = False) -> None:
        gh = self.hosts[self.cfg.gateway.host]
        env = "QUEQIAO_LANE_TRACE=1" if self.cfg.lane_trace else ""
        self.spawn(gh, f"gw-{arm.name}", self.gw_dir(arm), self.gw_cmd(arm, logname, enroll), env)
        self.wait_port(gh, self.cfg.gateway.metrics_port_base + arm.index, f"gateway {arm.name} metrics", f"gw-{arm.name}")
        # The gateway's runtime configuration record has no path profile, so
        # keep its actual command line as evidence of what it ran.
        rc, args = gh.run(f"ps -o args= -p $(cat {self.w(gh.dest)}/run/gw-{arm.name}.pid)", what=f"record gateway {arm.name} command line")
        if not self.dry:
            os.makedirs(os.path.join(self.out, "effective"), exist_ok=True)
            with open(os.path.join(self.out, "effective", f"gw-{arm.name}.args"), "a") as f:
                f.write(f"{logname}\t{args.strip()}\n")

    def cl_cmd(self, client: Client, arm: Arm, logname: str) -> str:
        d = self.cl_dir(client, arm)
        args = [f"{d}/queqiaod", "client", "--profile", f"{d}/profile.json",
                "--listen", f"127.0.0.1:{client.socks_port_base + arm.index}",
                "--metrics-listen", f"127.0.0.1:{client.metrics_port_base + arm.index}",
                "--log-file", f"{d}/logs/{logname}.jsonl", "--log-max-size-mib", str(min(self.cfg.log_max_mib, 256)),
                "--log-stderr=false", *self.client_flags(client, arm)]
        args += self.cfg.wl.client_flags(self.cfg)
        return " ".join(_q(a) for a in args)

    def start_client(self, client: Client, arm: Arm, logname: str) -> None:
        ch = self.hosts[client.host]
        tag = f"cl-{client.name}-{arm.name}"
        if client.small:
            busy = self.active_small.get(client.host)
            if busy and busy != tag:
                raise RuntimeError(f"{client.host} is small and already runs {busy}; refusing to start {tag}")
            self.active_small[client.host] = tag
        env = "QUEQIAO_LANE_TRACE=1" if self.cfg.lane_trace else ""
        self.spawn(ch, tag, self.cl_dir(client, arm), self.cl_cmd(client, arm, logname), env)
        self.wait_port(ch, client.socks_port_base + arm.index, f"client {tag}", tag)
        if client.cpu_sample:
            W = self.w(client.host)
            ch.run(cpu_sampler(f"{W}/run/{tag}.pid", f"{self.cl_dir(client, arm)}/logs/{logname}.cpu"),
                   what=f"sample {tag} CPU once a second")

    def stop_client(self, client: Client, arm: Arm) -> None:
        tag = f"cl-{client.name}-{arm.name}"
        self.stop(self.hosts[client.host], tag)
        if self.active_small.get(client.host) == tag:
            del self.active_small[client.host]

    def scrape_client(self, client: Client, arm: Arm, name: str) -> None:
        """The client's counters around a trial: TCP fallback is counted on
        the client, so the gateway's QUIC bytes alone cannot show it."""
        rc, text = self.hosts[client.host].run(
            f"{CURL_ENV} -s --max-time 10 http://127.0.0.1:{client.metrics_port_base + arm.index}/metrics | grep -E '^queqiao_' || true",
            what=f"scrape client {client.name}/{arm.name} metrics ({name})")
        if not self.dry:
            os.makedirs(os.path.join(self.out, "metrics"), exist_ok=True)
            with open(os.path.join(self.out, "metrics", f"{name}-client-{client.name}.txt"), "w") as f:
                f.write(text)

    def scrape(self, arm: Arm, name: str) -> None:
        gh = self.hosts[self.cfg.gateway.host]
        rc, text = gh.run(f"{CURL_ENV} -s --max-time 10 http://127.0.0.1:{self.cfg.gateway.metrics_port_base + arm.index}/metrics "
                          "| grep -E '^queqiao_' || true", what=f"scrape gateway {arm.name} metrics ({name})")
        if not self.dry:
            os.makedirs(os.path.join(self.out, "metrics"), exist_ok=True)
            with open(os.path.join(self.out, "metrics", f"{name}.txt"), "w") as f:
                f.write(text)

    # -- jobs
    def job_script(self, client: Client, arm: Arm, kind: str, n: int, duration: float, start_at: int = 0,
                   params: Optional[dict] = None) -> str:
        """The shell a registered generator runs for one job."""
        import ab_workloads
        job = ab_workloads.Job(client, kind, n, params=dict(params or {}))
        return ab_workloads.GENERATORS[kind].script(self, job, arm, duration, start_at)

    def stop_file(self, client: Client, arm: Arm) -> str:
        return f"{self.w(client.host)}/run/stop-probe-{client.name}-{arm.name}"

    def as_job(self, host: str, script: str) -> str:
        """Run a job in its own process group, recorded under run/, so that
        teardown can stop it and everything it started (curl, python) even
        when the runner was interrupted while the job's ssh was in flight."""
        W = self.w(host)
        self.job_seq += 1
        f = f"{W}/run/job-{self.job_seq}.pgid"
        return (f"set -m\n(\n{script}\n) &\nj=$!\necho $j > {f}\nwait $j\nrc=$?\nrm -f {f}\nexit $rc")

    def pause(self, seconds: float) -> None:
        if not self.dry:
            time.sleep(seconds)

    def run_phase(self, slot: Slot, arm: Arm, phase: Any, writer) -> None:
        """Run one ab_workloads.Phase: every job at once, load jobs to the end
        of the window, the others (measuring under that load) until the load
        has finished."""
        import ab_workloads
        results: Dict[int, Tuple[float, float, int, str]] = {}
        duration = phase.duration
        gens = [ab_workloads.GENERATORS[j.kind] for j in phase.jobs]

        def go(i, job, gen):
            t0 = time.time()
            try:
                rc, out = self.hosts[job.client.host].run(self.as_job(job.client.host, gen.script(self, job, arm, duration, start_at)),
                                                          timeout=duration + 120 + BARRIER_LEAD,
                                                          what=f"{phase.name}: {job.kind} x{job.n} via {job.client.name}/{arm.name}")
            except Exception as e:  # a hung ssh is a failed job, not a dead campaign
                rc, out = -1, ""
                self.log(f"job {job.kind} on {job.client.name} failed: {e}")
            results[i] = (t0, time.time(), rc, out)

        # Clear any stop signal before the phase starts, not inside the job:
        # a load job that finished before a measuring job's ssh session began
        # would otherwise have its stop signal erased.
        for j in phase.jobs:
            if not j.load:
                self.hosts[j.client.host].run(f"rm -f {self.stop_file(j.client, arm)}",
                                              what=f"{phase.name}: clear the stop file on {j.client.name}")
        # Jobs on several hosts that must load the path together start on a
        # shared remote clock, far enough ahead that every ssh session is up.
        start_at = int(time.time()) + BARRIER_LEAD if phase.barrier else 0
        threads = [threading.Thread(target=go, args=(i, j, g)) for i, (j, g) in enumerate(zip(phase.jobs, gens))]
        for t in threads:
            t.start()
        for t, j in zip(threads, phase.jobs):
            if j.load:
                t.join()
        for j in phase.jobs:
            if not j.load:
                self.hosts[j.client.host].run(f"touch {self.stop_file(j.client, arm)}",
                                              what=f"{phase.name}: load finished, stop the {j.kind} on {j.client.name}")
        for t in threads:
            t.join()
        if self.dry:
            return
        for i, (job, gen) in enumerate(zip(phase.jobs, gens)):
            t0, t1, rc, out = results[i]
            want = gen.expected(job)
            for flow, code, size, secs, ex, ts0, ts1 in gen.parse(out, job, duration):
                complete = int(ex == "0" and code == "200" and (not want or size == str(want)))
                writer([slot.block, slot.path, slot.slot, arm.name, arm.role, slot.arm_trial, int(slot.cold), phase.name, i,
                        job.client.name, job.kind, flow, utc(t0), utc(t1), code, size, secs, ex, complete,
                        f"{t1 - t0:.3f}", ts0, ts1])

    def clients_for(self, path: str) -> List[Client]:
        return self.cfg.wl.clients_for(self.cfg, path)

    def run_block(self, block: int, path: str, slots: List[Slot], writer) -> None:
        cfg = self.cfg
        arms = {a.name: a for a in cfg.arms}
        tagp = f"b{block:03d}-{path}"
        self.log(f"block {block} path {path}: " + " ".join(s.arm for s in slots))
        # Restart every gateway, so each arm's first trial in this block is cold.
        for arm in cfg.arms:
            self.stop(self.hosts[cfg.gateway.host], f"gw-{arm.name}")
            self.start_gateway(arm, f"server-{tagp}")
        clients = self.clients_for(path)
        resident = [c for c in clients if not c.small]
        for c in resident:
            for arm in cfg.arms:
                self.stop_client(c, arm)
                self.start_client(c, arm, f"client-{tagp}")
        for s in slots:
            arm = arms[s.arm]
            for c in clients:
                if c.small:
                    self.start_client(c, arm, f"client-{tagp}-s{s.slot}")
            name = f"{tagp}-s{s.slot}-{arm.name}"
            self.scrape(arm, name + "-pre")
            for c in clients:
                self.scrape_client(c, arm, name + "-pre")
            for phase in cfg.wl.phases(cfg, path):
                self.run_phase(s, arm, phase, writer)
            self.scrape(arm, name + "-post")
            for c in clients:
                self.scrape_client(c, arm, name + "-post")
            for c in clients:
                if c.small:
                    self.stop_client(c, arm)
            if not self.dry:
                time.sleep(cfg.gap)
        cfg.wl.after_block(self, block, path, tagp, writer)
        for c in resident:
            for arm in cfg.arms:
                self.stop_client(c, arm)
        for arm in cfg.arms:
            self.stop(self.hosts[cfg.gateway.host], f"gw-{arm.name}")
        self.collect(tagp, clients)

    def collect(self, tagp: str, clients: List[Client]) -> None:
        """Fetch and delete this block's logs: small hosts have little disk."""
        g = self.cfg.gateway
        gh = self.hosts[g.host]
        for arm in self.cfg.arms:
            d = self.gw_dir(arm)
            gh.run(f"cd {d}/logs && for f in server-{tagp}.jsonl*; do case $f in *.gz) ;; *) [ -f \"$f\" ] && gzip -1f \"$f\";; esac; done; true",
                   what=f"compress gateway {arm.name} logs")
            self._fetch_glob(gh, f"{d}/logs", f"server-{tagp}", os.path.join(self.out, "logs", f"gw-{arm.name}"))
        for c in clients:
            ch = self.hosts[c.host]
            for arm in self.cfg.arms:
                d = self.cl_dir(c, arm)
                ch.run(f"cd {d}/logs && for f in client-{tagp}.jsonl* client-{tagp}-s*.jsonl*; do case $f in *.gz) ;; *) [ -f \"$f\" ] && gzip -1f \"$f\";; esac; done; true",
                       what=f"compress client {c.name}/{arm.name} logs")
                self._fetch_glob(ch, f"{d}/logs", f"client-{tagp}", os.path.join(self.out, "logs", f"cl-{c.name}-{arm.name}"))

    def _fetch_glob(self, host: Host, remote_dir: str, prefix: str, local_dir: str) -> None:
        # NAME.jsonl[.N][.gz], NAME.cpu, or NAME-sSLOT.*: never another path
        # whose label merely starts with this one. Labels are [a-z0-9_-], so
        # the prefix needs no regex escaping.
        # exit 3/4: the directory could not be entered or listed; a listing
        # with no match is exit 0 with no names.
        rc, out = host.run(f"cd {remote_dir} || exit 3\nl=$(ls -1) || exit 4\nprintf '%s\\n' \"$l\" | grep -E '^{prefix}(\\.|-s[0-9]+\\.)'\nexit 0",
                           what=f"list {prefix} logs")
        if rc != 0 and not self.dry:
            # A failed listing is not an empty one: keep the work directory.
            self.fetch_failures.append(f"{host.dest}:{remote_dir}/{prefix}* (listing failed, exit {rc})")
            self.log(f"could not list {host.dest}:{remote_dir}; its logs stay on the host")
            return
        names = [x.strip() for x in out.splitlines() if x.strip().startswith(prefix)] if not self.dry else [prefix + ".*"]
        for n in names:
            ok = host.get(f"{remote_dir}/{n}", os.path.join(local_dir, n)) or \
                host.get(f"{remote_dir}/{n}", os.path.join(local_dir, n))
            if self.dry:
                continue
            if ok:
                host.run(f"rm -f {remote_dir}/{_q(n)}", what=f"delete fetched {n}")
            else:
                # Keep the remote copy; the work directory is then kept too.
                self.fetch_failures.append(f"{host.dest}:{remote_dir}/{n}")
                self.log(f"could not fetch {host.dest}:{remote_dir}/{n}; it stays on the host")

    def record_effective(self) -> None:
        """Publish each endpoint's effective configuration (section 14)."""
        if self.dry:
            return
        out = {}
        for arm in self.cfg.arms:
            out[arm.name] = {"gateway": self.gw_cmd(arm, "<log>"),
                             "clients": {c.name: self.cl_cmd(c, arm, "<log>") for c in self.cfg.clients.values()}}
        with open(os.path.join(self.out, "effective-commands.json"), "w") as f:
            json.dump(out, f, indent=2)

    # -- teardown
    def teardown(self) -> None:
        self.log("teardown")
        # Only directories this invocation reserved, and only while it still
        # owns them: never another campaign's processes.
        for host in self.reserved:
            h = self.hosts[host]
            W = self.w(host)
            try:
                h.run(self.owned(host) + f'''[ -d {W}/run ] || exit 0
# Jobs first: each ran in its own process group (as_job), so the group takes
# its curls and clients with it.
for f in {W}/run/job-*.pgid; do
  [ -f "$f" ] || continue
  g=$(cat "$f")
  [ "$(ps -o pgid= -p "$g" 2>/dev/null | tr -d ' ')" = "$g" ] && kill -TERM -- -"$g" 2>/dev/null
done
sleep 1
for f in {W}/run/job-*.pgid; do
  [ -f "$f" ] || continue
  g=$(cat "$f")
  [ "$(ps -o pgid= -p "$g" 2>/dev/null | tr -d ' ')" = "$g" ] && kill -KILL -- -"$g" 2>/dev/null
  rm -f "$f"
done
# Only the processes this campaign spawned (its pid files), each checked to
# be running from the work directory; anything else that mentions the
# directory is reported as a leftover by the verification, never killed.
mine() {{ ps -o args= -p "$1" 2>/dev/null | grep -q -F -- "{W}/"; }}
for f in {W}/run/*.pid; do
  [ -f "$f" ] || continue
  p=$(cat "$f"); mine "$p" && kill "$p" 2>/dev/null
done
sleep 2
for f in {W}/run/*.pid; do
  [ -f "$f" ] || continue
  p=$(cat "$f"); mine "$p" && kill -9 "$p" 2>/dev/null
done
rm -f {W}/run/*.pid''', what="stop every process this campaign started")
            except Exception as e:  # keep tearing down the other hosts
                self.log(f"teardown on {host} failed: {e}")
        for host, ports in self.cfg.planned_ports().items():
            h = self.hosts[host]
            checks = " ".join(str(p) for p, _ in ports)
            try:
                rc, out = h.run(f"for p in {checks}; do (exec 3<>/dev/tcp/127.0.0.1/$p) 2>/dev/null && echo \"still listening: $p\"; done; true",
                                what="verify no temporary listener remains")
                if out.strip():
                    self.log(f"{host}: {out.strip()}")
            except Exception as e:
                self.log(f"listener check on {host} failed: {e}")

    def clean(self) -> List[str]:
        """Remove every reserved work directory; returns the hosts where that
        failed, which stay reserved (and are reported) rather than assumed clean."""
        failed: List[str] = []
        for host in list(self.reserved):
            W = self.w(host)
            try:
                rc, out = self.hosts[host].run(self.owned(host) + f"case {W} in */{self.cfg.work_dir}) rm -rf {W};; esac\n"
                                               f"[ ! -e {W} ]", what="remove the work directory")
            except Exception as e:
                rc, out = -1, f"{type(e).__name__}: {e}"
            if rc != 0 and not self.dry:
                why = "its ownership could not be verified, so it was left alone" if rc == NOT_OWNER else out.strip()[:200]
                failed.append(f"{host}: {W} could not be removed ({why})")
                continue
            self.reserved.remove(host)
        return failed

    # -- driver
    def run(self, clean: bool = True, dry_blocks: int = 1) -> int:
        cfg = self.cfg
        slots = schedule(cfg)
        if not self.dry:
            os.makedirs(self.out, exist_ok=False)
        self.print_plan(slots)
        # SIGINT/SIGTERM become KeyboardInterrupt from before the first
        # reservation, so every path out of reserve, build and the plan's
        # publication releases what was reserved.
        prev = {}
        for sig in (signal.SIGINT, signal.SIGTERM):
            prev[sig] = signal.signal(sig, _interrupt)
        try:
            self.preflight()
            manifest = self.build()
            plan = {"config": {k: v for k, v in cfg.__dict__.items() if k not in ("gateway", "clients", "arms", "production", "dc", "wl")},
                    "workload": cfg.workload, "analyzer": cfg.wl.analyzer, "paths": cfg.paths(),
                    "dc": cfg.dc.__dict__ if cfg.dc else None,
                    "arms": manifest, "schedule": [s.__dict__ for s in slots], "main": cfg.main.name,
                    "clients": {c.name: {"small": c.small, "platform": c.platform} for c in cfg.clients.values()}}
            if not self.dry:
                with open(os.path.join(self.out, "plan.json"), "w") as f:
                    json.dump(plan, f, indent=2, default=str)
        except BaseException:
            # Nothing was started; give back whatever was reserved so a
            # corrected rerun is not refused.
            self.release()
            for sig, h in prev.items():
                signal.signal(sig, h)
            raise
        status = 0
        tsv = None
        try:
            self.started = True
            self.setup()
            self.record_effective()
            if not self.dry:
                tsv = open(os.path.join(self.out, "trials.tsv"), "w")
                tsv.write("\t".join(TRIAL_COLUMNS) + "\n")

            def writer(row):
                if tsv:
                    tsv.write("\t".join(str(x) for x in row) + "\n")
                    tsv.flush()
            for block in range(1, cfg.blocks + 1):
                if self.dry and block > dry_blocks:
                    print(f"... blocks {block}-{cfg.blocks} repeat the same commands in their own order", file=self.stream)
                    break
                for path in dict.fromkeys(s.path for s in slots if s.block == block):
                    self.run_block(block, path, [s for s in slots if s.block == block and s.path == path], writer)
                    if not self.dry:
                        time.sleep(cfg.block_gap)
                if cfg.snapshot_every and block % cfg.snapshot_every == 0 and block < cfg.blocks:
                    self.check_production(f"mid-b{block:03d}")
            if not self.dry:
                print("CAMPAIGN-DONE", file=self.stream)
        except ProductionChanged as e:
            self.log(f"ABORT: production changed: {e}")
            status = 3
        except KeyboardInterrupt:
            self.log("interrupted")
            status = 130
        except Exception as e:
            self.log(f"ABORT: {type(e).__name__}: {e}")
            status = 1
        finally:
            if tsv:
                tsv.close()
            leftovers: List[str] = []
            clean_failures: List[str] = []
            try:
                self.teardown()
                leftovers = self.verify_teardown()
            except Exception as e:
                leftovers = [f"teardown could not be verified: {type(e).__name__}: {e}"]
            if leftovers:
                self.log("TEARDOWN INCOMPLETE, the work directories are kept for manual cleanup:\n  " + "\n  ".join(leftovers))
            elif self.fetch_failures:
                self.log(f"{len(self.fetch_failures)} log file(s) could not be fetched; the work directories are kept")
            elif clean:
                clean_failures = self.clean()
            for sig, h in prev.items():
                signal.signal(sig, h)
        production_unknown = False
        try:
            self.check_production("after")
        except ProductionChanged as e:
            self.log(f"PRODUCTION CHANGED during the campaign: {e}")
            status = 3
        except Exception as e:
            # A host that cannot be snapshotted at the end leaves production
            # unverified: that is an abort, not "nothing was started".
            self.log(f"the final production snapshot failed: {type(e).__name__}: {e}")
            production_unknown = True
            status = status or 6
        production = "changed" if self.production_changed else "unknown" if production_unknown else "unchanged"
        if leftovers and status == 0:
            status = 5
        if clean_failures:
            self.log("cleanup failed:\n  " + "\n  ".join(clean_failures))
            status = status or 7
        if not self.dry:
            # The analyzers issue a verdict only for a campaign that finished,
            # cleaned up after itself and left production as it found it.
            with open(os.path.join(self.out, "status.json"), "w") as f:
                json.dump({"status": "complete" if status == 0 else "aborted", "exit": status,
                           "production": production, "teardown_leftovers": leftovers, "cleanup_failures": clean_failures,
                           "fetch_failures": self.fetch_failures, "finished_utc": utc(time.time())}, f, indent=2)
        return status

    def verify_teardown(self) -> List[str]:
        """Temporary listeners (TCP or UDP) or processes that survived
        teardown, read from each host's listener and process tables."""
        if self.dry:
            self.snapshot_all("teardown")
            return []
        snaps = self.snapshot_all("teardown")
        out = []
        for host, ports in self.cfg.planned_ports().items():
            snap = snaps.get(host, {})
            if not snap.get("listen_ok"):
                out.append(f"{host}: could not read the listening ports")
            if not snap.get("ps_ok"):
                out.append(f"{host}: could not read the process table, so surviving processes cannot be ruled out")
            for port, what in ports:
                if port in snap.get("listen", set()):
                    out.append(f"{host}: {what} port {port} is still listening")
            out += [f"{host}: still running: {x}" for x in snap.get("leftover", [])]
        return out

    def print_plan(self, slots: List[Slot]) -> None:
        cfg = self.cfg
        p = lambda *a: print(*a, file=self.stream)
        p(f"# campaign {cfg.name}: workload {cfg.workload}, {cfg.blocks} block(s), seed {cfg.seed}")
        p(f"# work directory on every host: ~/{cfg.work_dir}")
        p(f"# gateway host {cfg.gateway.host} ({cfg.gateway.platform}), clients dial {cfg.gateway.address}")
        for a in cfg.arms:
            src = (a.ref if a.gateway_ref == a.client_ref else f"gateway {a.gateway_ref}, client {a.client_ref}") \
                if not a.binaries else ", ".join(f"{k}={v}" for k, v in a.binaries.items())
            p(f"# arm {a.name} [{a.role}] {src}; gateway :{cfg.gateway.port_base + a.index}; flags: "
              f"{' '.join(a.flags + a.gateway_flags) or '(none)'} | client {' '.join(a.flags + a.client_flags) or '(none)'}")
        for c in cfg.clients.values():
            p(f"# client {c.name} on {c.host} ({c.platform}){' small: one temporary client at a time' if c.small else ''}")
        for pr in cfg.production:
            p(f"# production [{pr.name}] on {pr.host}: units {' '.join(pr.units) or '-'}; launchd {' '.join(pr.launchd) or '-'}; "
              f"ports {len(pr.ports)}; processes matching {pr.process_pattern!r}")
        p("# schedule (block path: slot order; * = cold start of that arm in the block)")
        cur = None
        line = ""
        for s in slots:
            if (s.block, s.path) != cur:
                if line:
                    p(line)
                cur = (s.block, s.path)
                line = f"  b{s.block:03d} {s.path}:"
            line += f" {s.arm}{'*' if s.cold else ''}"
        if line:
            p(line)


def _interrupt(signum, frame):
    raise KeyboardInterrupt


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--config", required=True)
    ap.add_argument("--output", help="new campaign directory (must not exist)")
    ap.add_argument("--dry-run", action="store_true", help="print the plan and commands; contact no host, build nothing")
    ap.add_argument("--keep-remote", action="store_true", help="leave ~/qq-ab-<name> on the hosts after teardown")
    ap.add_argument("--repo", default=os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    args = ap.parse_args(argv)
    try:
        cfg = load_config(args.config)
    except (ConfigError, ValueError, configparser.Error) as e:
        print(f"config error: {e}", file=sys.stderr)
        return 2
    if not args.dry_run and not args.output:
        print("--output is required unless --dry-run", file=sys.stderr)
        return 2
    out = args.output or "<campaign-dir>"
    if args.output and f"/{cfg.work_dir}/" in os.path.abspath(args.output) + "/":
        # Teardown stops every process whose command line names that directory.
        print(f"--output must not be inside a directory named {cfg.work_dir}", file=sys.stderr)
        return 2
    camp = Campaign(cfg, out, args.repo, args.dry_run)
    try:
        return camp.run(clean=not args.keep_remote)
    except Refused as e:
        print(f"REFUSED, nothing was started:\n{e}", file=sys.stderr)
        return 4


if __name__ == "__main__":
    sys.exit(main())

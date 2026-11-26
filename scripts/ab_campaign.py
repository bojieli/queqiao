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
import dataclasses
import datetime as dt
import hashlib
import secrets
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
# Portable path resolution on a host: the deepest existing ancestor through
# `cd -P`/`pwd -P` (symlinks resolved), the not-yet-existing rest appended.
# Python's normpath then folds '..' and '//'.
CANON_SH = r'''canon() {
  p="$1"; rest=""; n=0
  # The final component too: a symlink to a production file resolves to it.
  while [ -L "$p" ] && [ $n -lt 40 ]; do
    t=$(readlink "$p") || break
    case "$t" in /*) p="$t" ;; *) p="$(dirname "$p")/$t" ;; esac
    n=$((n+1))
  done
  while [ ! -e "$p" ] && [ "$p" != "/" ] && [ -n "$p" ]; do rest="/$(basename "$p")$rest"; p=$(dirname "$p"); done
  if [ -d "$p" ]; then r=$(cd -P "$p" 2>/dev/null && pwd -P)
  else r="$(cd -P "$(dirname "$p")" 2>/dev/null && pwd -P)/$(basename "$p")"; fi
  echo "${r:-$p}$rest"
}
'''
# Wall-time estimates: seconds the runner adds per block of one trial type
# (endpoint restarts, client starts and stops, scrapes, host CPU samplers,
# job launch and polling, log collection) and once (builds, copies,
# enrollment). Measured on the first stage 6/7 campaign through a relay hop:
# a block of A took about 7.7 min (4 x 60 s of trials) and one of B about
# 10 min (4 x 90 s), so about 230 s per block, and a round of A and B about
# 17 min.
BLOCK_OVERHEAD = 230
SETUP_SECONDS = 240
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
                 "curl_exit", "complete", "wall", "t_start", "t_end", "trial")


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
class Relay:
    """A fixed relay hop in front of the gateway ([relay]), as production
    clients reach their gateway through one: per arm a temporary
    `queqiaod relay` on `host` listening on that arm's gateway port and
    forwarding TCP and UDP to the gateway. Clients enroll with, and dial,
    `address` (the relay's) instead of the gateway's."""
    host: str
    address: str
    platform: str
    flags: List[str]


@dataclass
class Production:
    name: str
    host: str
    units: List[str]
    launchd: List[str]
    ports: List[int]
    process_pattern: str
    # The production service's own files and directories (state, config,
    # binary, logs): no flag may name them and no work directory may be
    # inside them.
    paths: List[str] = field(default_factory=list)
    # Read the units' state and executable through `sudo -n` (the snapshot
    # only; nothing else the campaign does uses sudo). A sudo that fails is
    # an observation error.
    snapshot_sudo: bool = False


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
    # Sequential stopping (ab_stats.Sequential): looks after `first_blocks`
    # ... `blocks`; None for a fixed number of blocks.
    sequential: Any = None
    # Sample every host's CPU (and production's) through each trial, for the
    # contention void rule (gates.ini [contention]).
    host_cpu: bool = False
    # A relay hop in front of the gateway ([relay]); None: clients dial it.
    relay: Optional[Relay] = None
    # Network impairment on the path's client host ([impair], ab_impair).
    impair: Any = None
    # Fetch logs through this ssh host instead of directly ("" for direct).
    fetch_via: str = ""

    @property
    def work_dir(self) -> str:
        return WORK_PREFIX + self.name

    @property
    def main(self) -> Arm:
        return next(a for a in self.arms if a.role == "main")

    def hosts(self) -> List[str]:
        seen: List[str] = []
        for h in [self.gateway.host] + [c.host for c in self.clients.values()] + ([self.relay.host] if self.relay else []):
            if h not in seen:
                seen.append(h)
        return seen

    def platforms(self) -> List[str]:
        out = [self.gateway.platform]
        for c in self.clients.values():
            if c.platform not in out:
                out.append(c.platform)
        if self.relay and self.relay.platform not in out:
            out.append(self.relay.platform)
        return out

    @property
    def endpoint_address(self) -> str:
        """What clients dial: the relay's address when there is one."""
        return self.relay.address if self.relay else self.gateway.address

    def paths(self) -> List[str]:
        """Clients that are measured paths. The DC workload runs every leaf at
        once, so it has a single path."""
        return self.wl.paths(self)

    def beside_arms(self) -> List[Arm]:
        """The instances of a session run beside the blocks (trial D): per arm
        a copy with its own name, directories and ports (index after the
        arms'), which the blocks' restarts never touch."""
        if not (self.wl and self.wl.beside(self)):
            return []
        n = len(self.arms)
        # "." is not allowed in arm names (ARM_RE), so these never collide
        # with an arm's directories, pid tags or log names.
        return [dataclasses.replace(a, name=f"{a.name}.wake", index=n + a.index) for a in self.arms]

    def planned_ports(self) -> Dict[str, List[Tuple[int, str]]]:
        out: Dict[str, List[Tuple[int, str]]] = {}
        g = self.gateway
        for a in self.arms + self.beside_arms():
            out.setdefault(g.host, []).append((g.port_base + a.index, f"gateway {a.name}"))
            out.setdefault(g.host, []).append((g.metrics_port_base + a.index, f"gateway metrics {a.name}"))
        out.setdefault(g.host, []).append((g.origin_port, "origin"))
        for ep in (self.wl.endpoints(self) if self.wl else []):
            for port, what in ep.tcp_ports + ep.udp_ports:
                out.setdefault(g.host, []).append((port, f"{ep.tag} {what}"))
        if self.relay:
            for a in self.arms + self.beside_arms():
                out.setdefault(self.relay.host, []).append((g.port_base + a.index, f"relay {a.name}"))
        for c in self.clients.values():
            for a in self.arms + (self.beside_arms() if c.name in self.paths() else []):
                out.setdefault(c.host, []).append((c.socks_port_base + a.index, f"client {c.name} socks {a.name}"))
                out.setdefault(c.host, []).append((c.metrics_port_base + a.index, f"client {c.name} metrics {a.name}"))
        return out


def load_config(path: str) -> Config:
    cp = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=(";", "#"))
    if not cp.read(path):
        raise ConfigError(f"cannot read {path}")
    return parse_config(cp)


def _relay(cp: configparser.ConfigParser) -> Optional["Relay"]:
    if not cp.has_section("relay"):
        return None
    r = cp["relay"]
    for k in ("host", "address"):
        if not r.get(k):
            raise ConfigError(f"[relay] {k} is required")
    flags = _words(r.get("flags", "--dial-timeout 3s --idle-timeout 15m --max-connections 8192"))
    _check_flags("[relay] flags", flags)
    for f in flags:
        if f.lstrip("-").split("=", 1)[0] in ("listen", "upstream"):
            raise ConfigError("[relay] flags: the runner sets --listen and --upstream")
    return Relay(host=r["host"], address=r["address"], platform=r.get("platform", "linux/amd64"), flags=flags)


def _impair(cp: configparser.ConfigParser) -> Any:
    import ab_impair
    try:
        return ab_impair.parse(cp)
    except ValueError as e:
        raise ConfigError(str(e))


def _sequential(c) -> Any:
    """[campaign] sequential = true: looks after first_blocks (default 6) to
    blocks (the cap, default 12), each at its nominal level from the
    Pocock-type alpha-spending function with total one-sided alpha (default
    0.05), or alpha / looks at every look with spending = bonferroni (section
    15.4). The levels are computed here, before the first block, and frozen
    in the plan."""
    if not _bool(c.get("sequential", "false")):
        return None
    if "interim_alpha" in c:
        raise ConfigError("interim_alpha is gone: looks use alpha spending (spending = pocock or bonferroni)")
    import ab_stats
    try:
        return ab_stats.Sequential(first=c.getint("first_blocks", 6), cap=c.getint("blocks", 12),
                                   alpha=c.getfloat("alpha", 0.05), spending=c.get("spending", "pocock"))
    except ValueError as e:
        raise ConfigError(f"sequential: {e}")


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
                                   process_pattern=s.get("process_pattern", "queqiao"),
                                   paths=_words(s.get("paths", "")),
                                   snapshot_sudo=_bool(s.get("snapshot_sudo", "false"))))
            for path_ in prod[-1].paths:
                if not path_.startswith("/") or path_.rstrip("/") == "":
                    raise ConfigError(f"[{sec}] paths must be absolute directories or files, not {path_!r}")
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
                 sequential=_sequential(c), host_cpu=_bool(c.get("host_cpu", "false")),
                 relay=_relay(cp), impair=_impair(cp), fetch_via=c.get("fetch_via", "").strip(),
                 log_max_mib=c.getint("log_max_mib", 1024), ssh_options=_words(c.get("ssh_options", "-o ConnectTimeout=20 -o ServerAliveInterval=10 -o ServerAliveCountMax=3")),
                 gateway=gw, clients=clients, arms=arms, production=prod, wl=wl)
    wl.configure(cfg, cp)
    if wl.impairs and not cfg.impair:
        raise ConfigError(f"workload {cfg.workload} runs under network impairment: it needs an [impair] section")
    if cfg.impair and not wl.impairs:
        raise ConfigError(f"[impair]: workload {cfg.workload} has no impairment scenarios")
    if cfg.impair and cfg.impair.host not in cfg.hosts():
        raise ConfigError(f"[impair] host {cfg.impair.host} is not a host of the campaign")
    if cfg.sequential and (wl.analyzer != "ab_analyze" or wl.primary or wl.sample_based):
        # The stopping looks decide [workload:...] rules; a workload judged by
        # the primary gate with its [secondary] bounds, by samples, or by
        # dc_gate runs a fixed number of blocks.
        raise ConfigError(f"sequential stopping is not supported for workload {workload}; set a fixed blocks")
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
    # No configured flag may reach a production path, in any spelling
    # (--state=/x, --state /x, or a path inside it).
    prod_paths = [os.path.normpath(p_) for pr in prod for p_ in pr.paths]
    for where, fl in [("[gateway] flags", gw.flags)] + [(f"[client:{c.name}] flags", c.flags) for c in clients.values()] + \
            [(f"[arm:{a.name}]", a.flags + a.gateway_flags + a.client_flags) for a in arms]:
        for f in fl:
            for tok in [f] + ([f.split("=", 1)[1]] if "=" in f else []):
                if relative_path(tok):
                    # Relative to an endpoint's working directory a path can
                    # still climb into production, unseen by the checks below.
                    raise ConfigError(f"{where}: {f!r} is a relative path; path-valued flags must be absolute")
                if not tok.startswith("/"):
                    continue
                tok = os.path.normpath(tok)       # "//x/../y" is "/y"
                for p_ in prod_paths:
                    if tok == p_ or tok.startswith(p_ + "/"):
                        raise ConfigError(f"{where}: {f!r} names production path {p_}")
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
    # The trial type (Workload.trial_types) whose block this slot is in; ""
    # for a workload whose every trial runs all its phases.
    trial: str = ""


def block_order(n_arms: int, block: int, rng: random.Random, trials_per_arm: int = 2) -> List[int]:
    """Arm indices for one block; every arm appears exactly twice.

    Two arms: ABBA or BAAB, chosen at random (section 15.3). More arms: a
    rotation of the arm list followed by its mirror, the rotation advancing
    each block and the direction reversing every n blocks, so over n * 2
    blocks every arm takes every position equally often.
    """
    if trials_per_arm == 1:
        order = list(range(n_arms))
        rng.shuffle(order)
        return order
    if n_arms == 2:
        return [0, 1, 1, 0] if rng.random() < 0.5 else [1, 0, 0, 1]
    k = (block - 1) % n_arms
    base = [(k + i) % n_arms for i in range(n_arms)]
    if ((block - 1) // n_arms) % 2 == 1:
        base.reverse()
    return base + base[::-1]


def schedule(cfg: Config) -> List[Slot]:
    """Every slot: per block, per path, per trial type its own ABBA or BAAB
    order. Slot numbers run on across a block's types on a path, so a
    (block, path, slot) names one trial."""
    rng = random.Random(cfg.seed)
    out: List[Slot] = []
    paths = cfg.paths()
    types = cfg.wl.trial_types(cfg)
    for b in range(1, cfg.blocks + 1):
        k = (b - 1) % len(paths)
        for path in paths[k:] + paths[:k]:
            n = 0
            for trial in types:
                seen: Dict[int, int] = {}
                for ai in block_order(len(cfg.arms), b, rng, cfg.wl.trials_per_arm):
                    n += 1
                    seen[ai] = seen.get(ai, 0) + 1
                    out.append(Slot(b, path, n, cfg.arms[ai].name, seen[ai], seen[ai] == 1, trial))
    return out


def gates_digest(path: Optional[str] = None) -> str:
    """SHA-256 of the gates file the analyzers judge by."""
    p = path or os.path.join(os.path.dirname(os.path.abspath(__file__)), "validation", "gates.ini")
    with open(p, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def run_units(cfg: Config, slots: Sequence[Slot]) -> List[Tuple[int, str, str]]:
    """The order the (block, path, trial type) units run in: the workload's
    run_first units (D's cover: C's first two blocks back to back), then the
    rest round-robin at block granularity, every type's block b before any
    type's block b + 1, so a path that drifts drifts under all of them."""
    units = list(dict.fromkeys((s.block, s.path, s.trial) for s in slots))
    first = [u for b, t in cfg.wl.run_first(cfg) for u in units if u[0] == b and u[2] == t]
    return first + [u for u in units if u not in first]


def unit_tag(block: int, path: str, trial: str = "") -> str:
    """Log and scrape names of one unit: b001-<path>[-<type>]."""
    return f"b{block:03d}-{path}" + (f"-{trial}" if trial else "")


# --- hosts -------------------------------------------------------------------

class Host:
    """Runs shell on one host: locally when dest is "local", else over ssh."""

    def __init__(self, dest: str, ssh_options: Sequence[str], dry: bool, out=None, control_dir: str = ""):
        self.dest = dest
        # One ssh connection per host, reused by every session (ControlMaster):
        # a session then costs a round trip, not a handshake. Its socket lives
        # in the campaign's own directory; close() ends it.
        self.control = (["-o", "ControlMaster=auto", "-o", f"ControlPath={control_dir}/%C", "-o", "ControlPersist=300"]
                        if control_dir and dest != "local" else [])
        self.base_options = list(ssh_options)
        self.ssh_options = list(ssh_options) + self.control
        # Logs are fetched through this host ([campaign] fetch_via) when the
        # direct link is slow; "" fetches directly.
        self.fetch_via = ""
        self.dry = dry
        self.out = out or sys.stdout
        self.home = "~" if dry else ""
        self.lock = threading.Lock()
        # Every session's wall time, for the overhead profile (timing.tsv):
        # set by the campaign once its directory exists.
        self.timing_path = ""

    def _time(self, what: str, t0: float) -> None:
        if self.timing_path:
            with self.lock, open(self.timing_path, "a") as f:
                f.write(f"{t0:.3f}\t{self.dest}\t{time.time() - t0:.3f}\t{what.replace(chr(9), ' ')}\n")

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
        t0 = time.time()
        try:
            p = subprocess.run(self._argv(), input="set -uo pipefail\n" + script, capture_output=True, text=True,
                               timeout=timeout)
        finally:
            self._time(what or "run", t0)
        return p.returncode, p.stdout + (("\n" + p.stderr) if p.returncode and p.stderr else "")

    def put(self, local: str, remote: str, if_changed: bool = False) -> None:
        """Copy a file to the host. if_changed: skip the copy when the host
        already has a file with the same SHA-256 there (a binary from an
        earlier campaign or arm), checked on the host, not assumed."""
        if self.dry:
            print(f"[{self.dest}] copy {local} -> {remote}", file=self.out)
            return
        t0 = time.time()
        if if_changed:
            with open(local, "rb") as f:
                want = hashlib.sha256(f.read()).hexdigest()
            rc, out = self.run(f"sha256sum {_q(remote)} 2>/dev/null || shasum -a 256 {_q(remote)} 2>/dev/null; true",
                               what=f"hash {os.path.basename(remote)}")
            if rc == 0 and out.split()[:1] == [want]:
                self._time(f"copy {os.path.basename(local)} (unchanged)", t0)
                return
        if self.dest == "local":
            subprocess.run(["cp", local, remote], check=True)
        else:
            subprocess.run(["scp", "-q", *self.ssh_options, local, f"{self.dest}:{remote}"], check=True)
        self._time(f"copy {os.path.basename(local)}", t0)

    def get(self, remote: str, local: str) -> bool:
        if self.dry:
            print(f"[{self.dest}] fetch {remote} -> {local}", file=self.out)
            return True
        os.makedirs(os.path.dirname(local), exist_ok=True)
        if self.dest == "local":
            return subprocess.run(["cp", remote, local]).returncode == 0
        if self.fetch_via and self.fetch_via != self.dest:
            # Its own connection through the jump host: a shared connection
            # would carry it over the direct link.
            return subprocess.run(["scp", "-q", *self.base_options, "-o", f"ProxyJump={self.fetch_via}",
                                   f"{self.dest}:{remote}", local]).returncode == 0
        return subprocess.run(["scp", "-q", *self.ssh_options, f"{self.dest}:{remote}", local]).returncode == 0

    def close(self) -> None:
        """End the shared ssh connection, if one was opened."""
        if self.control and not self.dry:
            subprocess.run(["ssh", *self.ssh_options, "-O", "exit", self.dest], capture_output=True, timeout=30)


# Snapshot of what production looks like on one host. Read-only: systemctl
# show, launchctl list, ps, ss/lsof, sha256 of executables.
SNAPSHOT_SH = r'''
W={work}
os=$(uname -s)
echo "os $os"
# observe AS UNIT: AS is "" or "sudo -n" (a [production] section with
# snapshot_sudo, for this observation only; a sudo that fails leaves v empty,
# an observation error).
observe() {{
  S=$1; u=$2
  # unit-error when the unit cannot be observed: the preflight refuses then,
  # rather than comparing two empty observations as "unchanged".
  if ! v=$($S systemctl show "$u" -p LoadState -p ActiveState -p MainPID -p NRestarts -p ExecMainStartTimestampMonotonic 2>/dev/null | sort | tr '\n' ' ') \
     || [ -z "$v" ] || echo "$v" | grep -q 'LoadState=not-found'; then
    echo "unit-error $u"; return
  fi
  pid=$($S systemctl show "$u" -p MainPID --value 2>/dev/null)
  exe=""; sha=""
  if [ -n "$pid" ] && [ "$pid" != 0 ]; then
    exe=$($S readlink -f /proc/$pid/exe 2>/dev/null); sha=$( ($S sha256sum "/proc/$pid/exe" 2>/dev/null || true) | cut -c1-64)
    # A running unit whose executable cannot be read or hashed is not
    # observed: two empty identities must never compare as "unchanged".
    if [ -z "$exe" ] || [ ${{#sha}} -ne 64 ]; then echo "unit-error $u"; return; fi
  fi
  echo "unit $u $v exe=$exe sha=$sha"
}}
for u in {units}; do observe "" "$u"; done
for u in {sudo_units}; do observe "sudo -n" "$u"; done
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
        home = snap.get("home")
        if home:
            W = f"{home}/{cfg.work_dir}"
            for p_ in p.paths:
                if W == p_.rstrip("/") or W.startswith(p_.rstrip("/") + "/"):
                    reasons.append(f"{p.host}: the work directory {W} is inside production path {p_}")
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

def relative_path(tok: str) -> bool:
    """A flag token that names a path, but not from the root: it has a
    directory part ("a/b", "../x", "./s") or starts with "~". A bare name
    resolves inside the endpoint's own directory under the work directory,
    and a URL is not a path."""
    if tok.startswith("-") or "://" in tok or tok.startswith("/"):
        return False
    return tok.startswith("~") or "/" in tok


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


# Ownership of a job's processes, by token only. Every job runs as
# `env QQ_AB_TOKEN=<token> bash <job file>`, so the job shell and every
# process it starts carry the token from exec on. own_mode says how this host
# can read it: "proc" (Linux, /proc/<pid>/environ), "ps" (ps -E shows the
# environment), or "none" -- then nothing is signalled and the job is
# reported for manual cleanup. members TOKEN MODE lists the owners' pids.
JOB_OWN_SH = r'''own_mode() {
  command -v python3 >/dev/null 2>&1 || { echo none; return; }
  if [ -r /proc/self/environ ]; then echo proc; return; fi
  env QQ_AB_PROBE=yes sleep 5 & q=$!
  if ps -E -ww -o command= -p "$q" 2>/dev/null | tr ' ' '\n' | grep -q -x -F 'QQ_AB_PROBE=yes'; then echo ps; else echo none; fi
  kill "$q" 2>/dev/null; wait "$q" 2>/dev/null
}
# members TOKEN MODE JOBPID: the owners' pids on stdout; exit 0 found, 1
# verified none, 2 the processes could not be inspected. An owner has the
# exact entry QQ_AB_TOKEN=<token> in its environment: a whole NUL-separated
# entry of /proc/<pid>/environ, or a whole whitespace-separated word of ps
# -E's line. A process whose environment cannot be read (not dumpable, or
# exiting) is skipped -- except the job's own recorded pid, which this
# campaign spawned and must be able to inspect. A scan that fails as a whole
# (/proc unlistable, ps failing) is exit 2.
members() {
  python3 - "$1" "$2" "$$" "${3:-}" <<'QQ_AB_PY'
import os, subprocess, sys
tok, mode, me, job = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
want = ("QQ_AB_TOKEN=" + tok).encode()
found = []
try:
    if mode == "proc":
        uid = os.getuid()
        for d in os.listdir("/proc"):
            if not d.isdigit() or d in (me, str(os.getpid())):
                continue
            try:
                if os.stat("/proc/" + d).st_uid != uid:
                    continue
                with open("/proc/%s/environ" % d, "rb") as f:
                    env = f.read()
            except OSError:
                if d == job and os.path.exists("/proc/" + d):
                    sys.exit(2)   # the job's own process, alive and unreadable
                continue          # ended meanwhile, or not readable: skipped
            if want in env.split(b"\0"):
                found.append(d)
    elif mode == "ps":
        out = subprocess.run(["ps", "-E", "-ww", "-ax", "-o", "pid=,command="], capture_output=True, check=True).stdout
        for line in out.splitlines():
            words = line.split()
            if len(words) > 1 and want in words[1:] and words[0].decode() not in (me, str(os.getpid())):
                found.append(words[0].decode())
    else:
        sys.exit(2)
except SystemExit:
    raise
except Exception:
    sys.exit(2)
print(" ".join(found))
sys.exit(0 if found else 1)
QQ_AB_PY
}
tok_ok() { [ ${#1} -eq 32 ] || return 1; case "$1" in *[!0-9a-f]*) return 1;; esac; return 0; }'''


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


HOST_CPU_EVERY = 2   # seconds between host CPU samples
# run_job's outcomes besides a job's own exit status. Only a host unreachable
# through every retry, or a job the runner itself killed tearing down, is the
# control plane (the block is void); the others are the arm's failures.
JOB_UNREACHABLE = 255
JOB_KILLED_BY_RUNNER = -4
JOB_TIMEOUT = -1
JOB_NO_RECORD = -2
JOB_LOST = -3
CONTROL_PLANE = (JOB_UNREACHABLE, JOB_KILLED_BY_RUNNER)


def host_cpu_sampler(stop_file: str, units: Sequence[str]) -> str:
    """Read-only shell that samples the host's CPU and production's, every
    HOST_CPU_EVERY s until the stop file appears: lines "S <epoch ms> <the
    /proc/stat cpu fields user nice system idle iowait irq softirq steal>
    <load1> <production units' utime+stime ticks, or nan> <cpus>". Nothing
    here needs or uses sudo: /proc/<pid>/stat is world-readable."""
    return (NOW_SH + f"stop={stop_file}\npids=''\n"
            f"for u in {' '.join(_q(u) for u in units)}; do "
            "p=$(systemctl show -p MainPID --value \"$u\" 2>/dev/null); "
            "[ -n \"$p\" ] && [ \"$p\" != 0 ] && pids=\"$pids $p\"; done\n"
            "n=$(nproc 2>/dev/null || echo 1)\n"
            "while [ ! -e \"$stop\" ]; do\n"
            "  set -- $(head -1 /proc/stat 2>/dev/null)\n"
            "  pt=0; for p in $pids; do x=$(awk '{print $14+$15}' /proc/$p/stat 2>/dev/null); "
            "if [ -n \"$x\" ]; then pt=$((pt + x)); else pt=nan; break; fi; done\n"
            "  [ \"$1\" = cpu ] && echo \"S $(now) $2 $3 $4 $5 $6 $7 $8 $9 $(cut -d' ' -f1 /proc/loadavg) $pt $n\"\n"
            f"  sleep {HOST_CPU_EVERY}\n"
            "done\nrm -f \"$stop\"")


def ssh_control_dir(out: str) -> str:
    """Where the campaign's shared ssh sockets live: run/ssh under its own
    directory, or a private directory under /tmp when that path would be too
    long for a Unix socket (a %C socket name is 40 characters)."""
    d = os.path.join(os.path.abspath(out), "ssh")
    if len(d) + 42 > 100:
        import tempfile
        return tempfile.mkdtemp(prefix="qqab-ssh-", dir="/tmp")
    return d          # created with the campaign directory (run)


def parallel(*calls) -> None:
    """Run the calls at once (threads); re-raise the first error."""
    errs: List[BaseException] = []

    def go(f):
        try:
            f()
        except BaseException as e:   # surfaced below
            errs.append(e)
    ths = [threading.Thread(target=go, args=(f,), daemon=True) for f in calls]
    for t in ths:
        t.start()
    for t in ths:
        t.join()
    if errs:
        raise errs[0]


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
        self.ssh_dir = "" if dry else ssh_control_dir(self.out)
        self.hosts: Dict[str, Host] = {h: Host(h, cfg.ssh_options, dry, stream, self.ssh_dir)
                                      for h in cfg.hosts() + [p.host for p in cfg.production]}
        for h in self.hosts.values():
            h.fetch_via = cfg.fetch_via
        self.before: Dict[str, dict] = {}
        self.active_small: Dict[str, str] = {}   # host -> running temporary client tag
        self.fetch_failures: List[str] = []
        self.reserved: List[str] = []
        self.production_changed = False
        self.measures_path = None if dry else os.path.join(self.out, "measures.jsonl")
        self.job_seq = 0
        self.blocks_run = 0
        self.units_run: List[List[Any]] = []
        self.teardown_notes: List[str] = []
        self.tearing_down = False
        # Failed jobs that are the arm's evidence (a timeout, a job lost or
        # silent on a reachable host), for the report.
        self.workload_failures: List[dict] = []
        # The network impairment ([impair]), if any.
        import ab_impair
        self.impair = ab_impair.Impairer(self, cfg.impair) if cfg.impair else None
        self.stopped = ""
        self.stopped_types: Dict[str, str] = {}
        self.beside_threads: List[threading.Thread] = []
        self.t_start = time.time()
        self.job_failures: List[dict] = []
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
            units = " ".join(_q(u) for p in prods if not p.snapshot_sudo for u in p.units)
            sudo_units = " ".join(_q(u) for p in prods if p.snapshot_sudo for u in p.units)
            launchd = " ".join(_q(u) for p in prods for u in p.launchd)
            pattern = "|".join(p.process_pattern for p in prods if p.process_pattern) or "queqiao"
            script = SNAPSHOT_SH.format(work=_q(self.w(host)), units=units, sudo_units=sudo_units, launchd=launchd,
                                        pattern=_q(pattern))
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
        for host, snap in self.before.items():
            snap["home"] = self.hosts[host].home if self.hosts[host].home != "~" else ""
        if self.dry:
            print("[preflight] would refuse if any of these is listening, declared production, or if "
                  f"a process under ~/{self.cfg.work_dir}/ is already running:", file=self.stream)
            for host, ports in self.cfg.planned_ports().items():
                print(f"    {host}: " + ", ".join(f"{p} ({w})" for p, w in ports), file=self.stream)
            if self.impair:
                print(f"[preflight] impairment on {self.impair.icfg.host}: would refuse unless it is root, the route to "
                      f"{self.impair.remote} leaves by the impaired interface, that interface has no clsact or ingress "
                      f"qdisc, no {'/'.join(self.impair.ifb('b') + self.impair.ifb('w'))} device exists, and no filter "
                      "port is declared production or inside either end's ephemeral range; then snapshot every "
                      "interface's tc state", file=self.stream)
                for r in self.impair.config_reasons():
                    print(f"    REFUSE {r}", file=self.stream)
                for s, ports in self.impair.ports.items():
                    if ports:
                        print(f"    filter set {s}: IPv4 to/from {self.impair.remote}, UDP and TCP, remote port "
                              f"{ports[0]}-{ports[-1]}", file=self.stream)
            self.reserve()
            return
        reasons = refusal_reasons(self.cfg, self.before) + self.canonical_path_reasons()
        if self.impair:
            reasons += self.impair.preflight_reasons()
        if reasons:
            raise Refused("\n".join(reasons))
        self.reserve()

    def flag_paths(self, host: str) -> List[str]:
        """Absolute paths in the flags processes on this host will get."""
        cfg = self.cfg
        flags: List[str] = []
        if host == cfg.gateway.host:
            flags += cfg.gateway.flags + [f for a in cfg.arms for f in a.flags + a.gateway_flags]
        for c in cfg.clients.values():
            if c.host == host:
                flags += c.flags + [f for a in cfg.arms for f in a.flags + a.client_flags]
        out = []
        for f in flags:
            for tok in [f] + ([f.split("=", 1)[1]] if "=" in f else []):
                if tok.startswith("/"):
                    out.append(tok)
        return sorted(set(out))

    def canonical_path_reasons(self) -> List[str]:
        """Compare filesystem identities, not strings: resolve the declared
        production paths, the work directory's parent and every path a flag
        names, on each host (symlinks, '..', '//'), and refuse any overlap."""
        reasons: List[str] = []
        by_host: Dict[str, List[str]] = {}
        for p in self.cfg.production:
            by_host.setdefault(p.host, []).extend(p.paths)
        for host, prod_paths in by_host.items():
            if not prod_paths:
                continue
            h = self.hosts[host]
            items = prod_paths + [h.home] + self.flag_paths(host)
            script = CANON_SH + "".join(f"canon {_q(x)}\n" for x in items)
            rc, out = h.run(script, what="resolve production and flag paths (read-only)")
            lines = out.strip().splitlines()
            if rc != 0 or len(lines) != len(items):
                reasons.append(f"{host}: could not resolve production paths ({out.strip()[:200]})")
                continue
            canon = dict(zip(items, (os.path.normpath(x) for x in lines)))
            prod = [canon[p_] for p_ in prod_paths]
            under = lambda x, root: x == root or x.startswith(root.rstrip("/") + "/")
            work = canon[h.home].rstrip("/") + "/" + self.cfg.work_dir
            for root in prod:
                if under(work, root):
                    reasons.append(f"{host}: the work directory {work} is inside production path {root}")
                for tok in self.flag_paths(host):
                    if under(canon[tok], root):
                        reasons.append(f"{host}: flag path {tok} resolves to {canon[tok]}, inside production path {root}")
        return reasons

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
        # Files a workload's generators run on the client hosts.
        files = cfg.wl.client_files(cfg)
        for host in dict.fromkeys(c.host for c in cfg.clients.values()):
            if files:
                self.must(self.hosts[host], "command -v python3 >/dev/null || { echo 'python3 is required on this "
                          f"client host for workload {cfg.workload}'; exit 1; }}", "check for python3")
            for f in files:
                self.hosts[host].put(f, f"{self.w(host)}/{os.path.basename(f)}")
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
        for arm in cfg.arms + cfg.beside_arms():
            d = self.gw_dir(arm)
            self.must(gh, f"mkdir -p {d}/logs", f"gateway directory for {arm.name}")
            gh.put(self.binary(arm, g.platform, "gateway"), f"{d}/queqiaod", if_changed=True)
            # Fresh provider state every campaign: a kept work directory from
            # an earlier run must not supply this one's identity or users. The
            # account has no device or flow limit of its own (0: every device,
            # the gateway's --max-sessions), so a campaign measures the path,
            # not account admission.
            self.must(gh, f"chmod +x {d}/queqiaod\ncd {d} && rm -rf ./state && ./queqiaod provider init --state ./state "
                      f"--name {cfg.work_dir}-{arm.name} --endpoint {cfg.endpoint_address}:{g.port_base + arm.index} >/dev/null && "
                      f"./queqiaod provider add-user --state ./state --name ab --max-flows 0 --max-clients 0 >/dev/null",
                      f"temporary provider state for {arm.name}")
        if cfg.relay:
            self.start_relays()
        # Enrollment needs a running gateway; start each, enroll every client, stop.
        for arm in cfg.arms + cfg.beside_arms():
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
        ch.put(self.binary(arm, client.platform, "client"), f"{d}/queqiaod", if_changed=True)
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

    def start_client(self, client: Client, arm: Arm, logname: str, beside: bool = False) -> None:
        ch = self.hosts[client.host]
        tag = f"cl-{client.name}-{arm.name}"
        if client.small and not beside:
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

    def as_job(self, host: str, script: str) -> Tuple[str, Dict[str, str]]:
        """A detached job: the shell that launches it, and its files. The job
        is a file run by a wrapper as `env QQ_AB_TOKEN=<token> bash <wrapper>`
        under setsid and nohup, so it survives the ssh session that started
        it, and its shell and every descendant carry the token from exec on
        (JOB_OWN_SH; teardown finds them by it). The wrapper keeps the job's
        output and exit status in run/job-N.out and run/job-N.rc, which the
        runner fetches over fresh ssh sessions (run_job). The descriptor
        run/job-N.pgid holds "<pid> <token>"; the wrapper removes it once the
        job has ended with none of its processes left."""
        W = self.w(host)
        self.job_seq += 1
        n = self.job_seq
        files = {k: f"{W}/run/job-{n}.{k}" for k in ("pgid", "sh", "wrap", "out", "rc")}
        # A fixed-length random token per job: no job's token is a prefix of
        # another's, and none appears in any argument.
        tok = secrets.token_hex(16)
        wrap = (f"bash {files['sh']} > {files['out']} 2>&1\n"
                f"echo $? > {files['rc']}.tmp && mv {files['rc']}.tmp {files['rc']}\n"
                f"{JOB_OWN_SH}\n"
                "m=$(own_mode)\n"
                # Only a verified absence (exit 1) retires the descriptor.
                f"if [ \"$m\" != none ]; then members {tok} $m $$ >/dev/null; [ $? -eq 1 ] && rm -f {files['pgid']}; fi\n")
        launch = (f"cat > {files['sh']} <<'QQ_AB_JOB_EOF'\n{script}\nQQ_AB_JOB_EOF\n"
                  f"cat > {files['wrap']} <<'QQ_AB_WRAP_EOF'\n{wrap}QQ_AB_WRAP_EOF\n"
                  f"rm -f {files['out']} {files['rc']}\n"
                  f"( if command -v setsid >/dev/null; then exec setsid nohup env QQ_AB_TOKEN={tok} bash {files['wrap']}; "
                  f"else exec nohup env QQ_AB_TOKEN={tok} bash {files['wrap']}; fi ) > /dev/null 2>&1 < /dev/null &\n"
                  f"echo \"$! {tok}\" > {files['pgid']}\necho QQ_AB_LAUNCHED")
        return launch, files

    # Polling a detached job: the interval, and how many ssh sessions in a row
    # may fail (with backoff) before its host counts as unreachable.
    JOB_POLL = 1.0
    JOB_SSH_TRIES = 5
    # The CPU sampler's launch, stop and fetch: 9 sessions, about three and a
    # half minutes of backoff, since its record decides whether a block counts.
    SAMPLER_SSH_TRIES = 9

    def run_job(self, host: str, script: str, timeout: float, what: str) -> Tuple[int, str]:
        """Run a job detached on host and return (exit status, output). An ssh
        session that drops costs nothing: the job goes on, and the runner
        polls and fetches its output over fresh sessions, retrying with
        backoff. Only a host unreachable through JOB_SSH_TRIES sessions in a
        row (255), or a job that outlives `timeout` (-1), is a failed job --
        a control-plane failure, which voids its block (gates.ini [harness])."""
        files, rc = self.start_job(host, script, what)
        if self.dry:
            return 0, ""
        if files is None:
            return rc, ""
        return self.wait_job(host, files, timeout, what)

    def session(self, host: str, cmd: str, what_: str, tries: int = 0) -> Tuple[int, str]:
        """One ssh session, retried with backoff (2, 4, ... 30 s) while it
        fails to connect, up to `tries` sessions (JOB_SSH_TRIES by default)."""
        h = self.hosts[host]
        tries = tries or self.JOB_SSH_TRIES
        out = ""
        for k in range(tries):
            try:
                rc, out = h.run(cmd, timeout=120, what=what_)
            except Exception as e:  # a hung ssh is a dropped session
                rc, out = 255, f"{type(e).__name__}: {e}"
            if rc != 255:
                return rc, out
            self.log(f"{host}: ssh session failed ({what_}); retry {k + 1} of {tries - 1}")
            if not self.dry:
                time.sleep(min(30.0, 2.0 * 2 ** k))
        return 255, out

    def start_job(self, host: str, script: str, what: str, tries: int = 0) -> Tuple[Optional[Dict[str, str]], int]:
        """Launch a detached job; (its files, 0), or (None, exit status) when
        it could not be launched (255: its host unreachable)."""
        launch, files = self.as_job(host, script)
        rc, out = self.session(host, launch, what, tries)
        if self.dry:
            return files, 0
        if rc != 0 or "QQ_AB_LAUNCHED" not in out:
            return None, (255 if rc == 255 else rc or 1)
        return files, 0

    def wait_job(self, host: str, files: Dict[str, str], timeout: float, what: str,
                 tries: int = 0) -> Tuple[int, str]:
        """Follow a launched job to its end (run_job's outcomes)."""
        if self.dry:
            return 0, ""
        session = lambda cmd, what_: self.session(host, cmd, what_, tries)
        deadline = time.time() + timeout
        # Done (its status is there), lost (its wrapper is gone without one:
        # killed), or still running.
        poll = (f"if [ -f {files['rc']} ]; then echo \"QQ_AB_RC $(cat {files['rc']})\"; cat {files['out']}; "
                f"elif [ -f {files['pgid']} ] && read -r p t < {files['pgid']} && ! kill -0 \"$p\" 2>/dev/null "
                f"&& [ ! -f {files['rc']} ]; then echo QQ_AB_LOST; else echo QQ_AB_RUNNING; fi")
        while True:
            rc, out = session(poll, f"{what}: poll")
            if rc == 255:
                return 255, ""
            first, _, rest = out.partition("\n")
            if first.strip() == "QQ_AB_LOST":
                # Killed by the runner itself (teardown after an abort) is the
                # harness's; otherwise the job died on a reachable host.
                self.log(f"{host}: {what} ended without an exit status (killed)")
                return (JOB_KILLED_BY_RUNNER if self.tearing_down else JOB_LOST), ""
            if first.startswith("QQ_AB_RC "):
                try:
                    job_rc = int(first.split()[1])
                except (IndexError, ValueError):
                    job_rc = 1
                session(f"rm -f {files['out']} {files['rc']} {files['sh']} {files['wrap']}", f"{what}: clean up")
                return job_rc, rest
            if time.time() > deadline:
                # Past its window: the runner ends the job's own processes (not
                # the wrapper, which then records what it had), and the job is
                # a timeout -- the arm's failure, evidence, never void.
                self.log(f"{host}: {what} still running after {timeout:.0f} s; stopping it (a timeout)")
                session(f"{JOB_OWN_SH}\nm=$(own_mode); [ \"$m\" != none ] && read -r p t < {files['pgid']} && "
                        f"ps_=$(members \"$t\" \"$m\" \"$p\") && kill -KILL $(echo \"$ps_\" | tr ' ' '\\n' | "
                        f"grep -v -x \"$p\") 2>/dev/null; true", f"{what}: stop at its deadline")
                for _ in range(10):
                    rc, out = session(poll, f"{what}: poll after its deadline")
                    if out.startswith("QQ_AB_RC "):
                        return JOB_TIMEOUT, out.partition("\n")[2]
                    time.sleep(1)
                return JOB_TIMEOUT, ""
            time.sleep(self.JOB_POLL)

    def interim(self, path: str, trial: str = "") -> Tuple[bool, str]:
        """(stop?, summary) for one path x trial type's sequential gate on
        the blocks run so far. Each type stops on its own; the others go on."""
        import ab_analyze
        try:
            by_cell = ab_analyze.interim_decisions(self.out, ab_analyze.load_gates(), self.units_run,
                                                   {"job_failures": list(self.job_failures),
                                                    "fetch_failures": list(self.fetch_failures)})
        except Exception as e:  # an analysis error must not end the campaign
            return False, f"interim analysis failed ({type(e).__name__}: {e}); continuing"
        import ab_stats
        decisions = [d for (p_, cell), ds in by_cell.items() if p_ == path and (not trial or cell == trial) for d in ds]
        counts = {d: decisions.count(d) for d in sorted(set(decisions))}
        return ab_stats.overall_stop(decisions), ", ".join(f"{v} {k}" for k, v in counts.items()) or "nothing to judge"

    def pause(self, seconds: float) -> None:
        if not self.dry:
            time.sleep(seconds)

    def run_phase(self, slot: Slot, arm: Arm, phase: Any, writer) -> None:
        """Run one ab_workloads.Phase: every job at once, load jobs to the end
        of the window, the others (measuring under that load) until the load
        has finished."""
        import ab_workloads
        phase = self.cfg.wl.per_trial(self.cfg, slot, arm, phase)
        results: Dict[int, Tuple[float, float, int, str]] = {}
        duration = phase.duration
        gens = [ab_workloads.GENERATORS[j.kind] for j in phase.jobs]

        def go(i, job, gen):
            t0 = time.time()
            try:
                rc, out = self.run_job(job.client.host, gen.script(self, job, arm, duration, start_at),
                                       timeout=duration + 120 + BARRIER_LEAD,
                                       what=f"{phase.name}: {job.kind} x{job.n} via {job.client.name}/{arm.name}")
            except Exception as e:  # the runner could not reach or follow the job
                rc, out = JOB_UNREACHABLE, ""
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
            self.record_job(slot, arm, phase, i, job, gen, results[i], writer, self.measures_path, duration)

    def record_job(self, slot: Slot, arm: Arm, phase: Any, i: int, job: Any, gen: Any,
                   result: Tuple[float, float, int, str], writer, measures_path: Optional[str], duration: float) -> None:
        """A finished job's rows (trials.tsv) and raw records (measures.jsonl)."""
        t0, t1, rc, out = result
        if rc == 0 and not out.strip() and not self.dry:
            rc = JOB_NO_RECORD       # ran, but left no record at all
        if rc != 0 and getattr(gen, "harness", False):
            rc = JOB_KILLED_BY_RUNNER     # the harness's own job (the impairment's outages) failed
        if rc in CONTROL_PLANE:
            # The runner lost the job: its host unreachable through every ssh
            # retry, or the runner itself killed it tearing down. Harness
            # infrastructure, not the arm: the block is void for both arms.
            self.job_failures.append({"failure": "control-plane", "block": slot.block, "path": slot.path,
                                      "trial": slot.trial, "slot": slot.slot, "arm": arm.name, "phase": phase.name,
                                      "client": job.client.name, "kind": job.kind, "exit": rc})
            self.log(f"job {job.kind} on {job.client.name} ({phase.name}) lost to the control plane (exit {rc}): "
                     "its block is void")
        elif rc != 0:
            # Past its window, lost or silent while its host stayed reachable,
            # or its generator failing: the arm's failure, kept as evidence.
            reason = {JOB_TIMEOUT: "timeout", JOB_NO_RECORD: "no record", JOB_LOST: "killed"}.get(rc, f"exit {rc}")
            self.workload_failures.append({"block": slot.block, "path": slot.path, "trial": slot.trial,
                                           "slot": slot.slot, "arm": arm.name, "phase": phase.name,
                                           "client": job.client.name, "kind": job.kind, "reason": reason})
            self.log(f"job {job.kind} on {job.client.name} ({phase.name}, {arm.name}) failed: {reason} (the arm's)")
            if gen.records and gen.record_kind and not (gen.records(out) if out else []):
                # A measuring job with no record of its own: a failed session.
                out = (out + "\n" if out else "") + "J\t" + json.dumps({"kind": gen.record_kind, "fail": reason,
                                                                       "t0": round(t0, 3), "t1": round(t1, 3)})
        want = gen.expected(job)
        # A generator's raw records ("J<TAB>{json}" lines) go to
        # measures.jsonl, keyed to the trial and phase, for the workload's
        # extractor.
        recs = gen.records(out) if gen.records else []
        if recs and measures_path:
            with open(measures_path, "a") as f:
                for rec in recs:
                    rec.update(_block=slot.block, _path=slot.path, _slot=slot.slot, _arm=arm.name,
                               _phase=phase.name, _client=job.client.name, _job=i)
                    f.write(json.dumps(rec, separators=(",", ":")) + "\n")
        for flow, code, size, secs, ex, ts0, ts1 in gen.parse(out, job, duration):
            complete = int(ex == "0" and code == "200" and (not want or size == str(want)))
            writer([slot.block, slot.path, slot.slot, arm.name, arm.role, slot.arm_trial, int(slot.cold), phase.name, i,
                    job.client.name, job.kind, flow, utc(t0), utc(t1), code, size, secs, ex, complete,
                    f"{t1 - t0:.3f}", ts0, ts1, slot.trial])

    # -- a session beside the blocks (trial D)
    def start_beside(self, beside: Any) -> None:
        """Start D: per arm its own gateway and, per path, its own client (the
        blocks' restarts do not touch them), then its session job on each, in
        the background; wait until every session's set-up traffic is done."""
        import ab_workloads
        cfg = self.cfg
        self.beside_results: Dict[Tuple[str, str], Tuple[float, float, int, str]] = {}
        self.beside_slots: List[Tuple[Slot, Arm, Any]] = []
        self.log(f"{beside.name}: starting its sessions beside the blocks")
        if beside.impair and self.impair:
            # Its own ports, for the whole session (its set-up included).
            self.impair.apply("w", beside.impair, beside.phase.duration + beside.ready_seconds + 240)
        for arm in cfg.beside_arms():
            self.start_gateway(arm, f"server-{beside.name}")
        ready: List[Tuple[Host, str]] = []
        gen = ab_workloads.GENERATORS[beside.phase.jobs[0].kind]
        n = 0
        for path in cfg.paths():
            client = cfg.clients[path]
            ch = self.hosts[client.host]
            for real, arm in zip(cfg.arms, cfg.beside_arms()):
                self.start_client(client, arm, f"client-{beside.name}", beside=True)
                f = f"{self.w(client.host)}/run/ready-{beside.name}-{client.name}-{arm.name}"
                ch.run(f"rm -f {f}", what=f"{beside.name}: clear the ready marker of {client.name}/{arm.name}")
                tmpl = beside.phase.jobs[0]
                job = ab_workloads.Job(client, tmpl.kind, tmpl.n, load=True, params=dict(tmpl.params, ready_file=f))
                n += 1
                slot = Slot(1, path, n, real.name, 1, True, "")
                self.beside_slots.append((slot, real, job))
                script = gen.script(self, job, arm, beside.phase.duration)

                def go(key=(path, real.name), host=client.host, script=script, what=f"{beside.name} on {client.name}/{arm.name}"):
                    t0 = time.time()
                    try:
                        rc, out = self.run_job(host, script, timeout=beside.phase.duration + 180, what=what)
                    except Exception as e:
                        rc, out = -1, ""
                        self.log(f"{what} failed: {e}")
                    self.beside_results[key] = (t0, time.time(), rc, out)
                th = threading.Thread(target=go, daemon=True)
                th.start()
                self.beside_threads.append(th)
                ready.append((ch, f))
        if self.dry:
            return
        deadline = time.time() + beside.ready_seconds
        waiting = list(ready)
        while waiting and time.time() < deadline:
            waiting = [(h, f) for h, f in waiting if h.run(f"test -f {f}", what="wait for the session's set-up")[0] != 0]
            if waiting:
                time.sleep(2)
        if waiting:
            # Its syncs would still be loading the path under the first
            # measured block: abort rather than measure under unknown traffic.
            raise RuntimeError(f"{beside.name}: {len(waiting)} session(s) not set up after {beside.ready_seconds:.0f} s; "
                               "no block was run under its set-up traffic")

    def finish_beside(self, beside: Any) -> None:
        """Wait for D's sessions, stop its instances, and write them as a
        campaign of their own (workload `beside.workload`) in the subdirectory
        `beside.name`, for the analyzer and the flip."""
        import ab_workloads
        cfg = self.cfg
        for th in self.beside_threads:
            th.join(timeout=beside.phase.duration + 240)
        if beside.impair and self.impair:
            fired = self.impair.remove("w")
            ended = max((r_[1] for r_ in self.beside_results.values()), default=time.time())
            if fired is not None and fired < ended:
                # The session ran partly unimpaired: the harness's failure.
                self.beside_impair_fired = utc(fired)
                self.log(f"{beside.name}: the impairment watchdog fired at {utc(fired)}, before the session ended")
            elif fired is not None:
                self.log(f"{beside.name}: the impairment watchdog removed it at {utc(fired)}, after the session ended "
                         f"({utc(ended)}): the session ran impaired throughout")
        clients = [cfg.clients[p] for p in cfg.paths()]
        for c in clients:
            for arm in cfg.beside_arms():
                self.stop_client(c, arm)
        for arm in cfg.beside_arms():
            self.stop(self.hosts[cfg.gateway.host], f"gw-{arm.name}")
        if self.dry:
            return
        sub = os.path.join(self.out, beside.name)
        os.makedirs(sub, exist_ok=True)
        before = len(self.fetch_failures)
        self.collect(beside.name, clients, cfg.beside_arms(), out=sub)
        # Its logs are its own campaign's evidence, not the blocks'.
        self.beside_fetch_failures = self.fetch_failures[before:]
        gen = ab_workloads.GENERATORS[beside.phase.jobs[0].kind]
        with open(os.path.join(sub, "trials.tsv"), "w") as tsv:
            tsv.write("\t".join(TRIAL_COLUMNS) + "\n")

            def writer(row):
                tsv.write("\t".join(str(x) for x in row) + "\n")
            failures_before = len(self.job_failures)
            for slot, real, job in self.beside_slots:
                res = self.beside_results.get((slot.path, real.name), (time.time(), time.time(), -1, ""))
                self.record_job(slot, real, beside.phase, 0, job, gen, res, writer,
                                os.path.join(sub, "measures.jsonl"), beside.phase.duration)
            if getattr(self, "beside_impair_fired", ""):
                self.job_failures.append({"failure": "control-plane", "block": 1, "path": cfg.paths()[0], "trial": "",
                                          "slot": 1, "arm": cfg.main.name, "phase": beside.phase.name,
                                          "client": cfg.paths()[0], "kind": "impairment", "exit": JOB_KILLED_BY_RUNNER,
                                          "fired_utc": self.beside_impair_fired})
            self.beside_failures = self.job_failures[failures_before:]
            # Its failures are its own; the blocks' verdicts do not depend on them.
            del self.job_failures[failures_before:]
        with open(os.path.join(self.out, "plan.json")) as f:
            plan = json.load(f)
        plan.update(workload=beside.workload, analyzer="ab_analyze", sequential=None, beside_of=os.path.basename(self.out),
                    schedule=[s.__dict__ for s, _, _ in self.beside_slots])
        with open(os.path.join(sub, "plan.json"), "w") as f:
            json.dump(plan, f, indent=2, default=str)

    def clients_for(self, path: str) -> List[Client]:
        return self.cfg.wl.clients_for(self.cfg, path)

    def run_block(self, block: int, path: str, slots: List[Slot], writer, trial: str = "") -> None:
        """One block of one trial type on one path: every endpoint restarted
        first, so each arm's first trial in it is cold."""
        cfg = self.cfg
        arms = {a.name: a for a in cfg.arms}
        tagp = unit_tag(block, path, trial)
        self.log(f"block {block} path {path}{' ' + trial if trial else ''}: " + " ".join(s.arm for s in slots))
        # Restart every gateway, so each arm's first trial in this block is
        # cold: the arms' gateways at once.
        parallel(*[(lambda arm=arm: (self.stop(self.hosts[cfg.gateway.host], f"gw-{arm.name}"),
                                     self.start_gateway(arm, f"server-{tagp}"))) for arm in cfg.arms])
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
            parallel(lambda: self.scrape(arm, name + "-pre"), *[(lambda c=c: self.scrape_client(c, arm, name + "-pre"))
                                                                 for c in clients])
            samplers = self.start_host_cpu(name, path) if cfg.host_cpu else None
            phases = cfg.wl.type_phases(cfg, path, trial)
            scenario = cfg.wl.impairment(cfg, trial) if self.impair else ""
            try:
                if scenario:
                    # Just before the trial, on every block arm's port alike.
                    self.impair.apply("b", scenario, sum(p.duration for p in phases) + 120 + BARRIER_LEAD)
                try:
                    for phase in phases:
                        self.run_phase(s, arm, phase, writer)
                finally:
                    if scenario and self.impair.remove("b") is not None:
                        # Its watchdog removed the impairment before the trial
                        # ended: the harness's failure, not the arm's.
                        self.job_failures.append({"failure": "control-plane", "block": s.block, "path": s.path,
                                                  "trial": s.trial, "slot": s.slot, "arm": arm.name,
                                                  "phase": trial, "client": path, "kind": "impairment",
                                                  "exit": JOB_KILLED_BY_RUNNER})
                        self.log(f"impairment watchdog fired during {name}: its block is void")
            finally:
                if samplers is not None:
                    self.stop_host_cpu(name, samplers)
            parallel(lambda: self.scrape(arm, name + "-post"),
                     *[(lambda c=c: self.scrape_client(c, arm, name + "-post")) for c in clients])
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

    def host_cpu_hosts(self, path: str) -> List[str]:
        """The hosts a trial on this path loads: the gateway's, the relay's
        and its clients'."""
        relay = [self.cfg.relay.host] if self.cfg.relay else []
        return list(dict.fromkeys([self.cfg.gateway.host] + relay + [c.host for c in self.clients_for(path)]))

    def relay_cmd(self, arm: Arm) -> str:
        """The relay's command line for one arm (on the relay host)."""
        r, g = self.cfg.relay, self.cfg.gateway
        port = g.port_base + arm.index
        args = [f"{self.w(r.host)}/relay/queqiaod", "relay", "run", "--listen", f":{port}",
                "--upstream", f"{g.address}:{port}", *r.flags]
        return " ".join(_q(a) for a in args)

    def start_relays(self) -> None:
        """Per arm, a temporary relay on the relay host on that arm's gateway
        port, forwarding TCP and UDP to the gateway; the main arm's build for
        every arm, so the hop is the same for both. Started from the work
        directory with a pid file, so teardown stops it; it stays up for the
        whole campaign (a relay keeps no path knowledge)."""
        cfg, r, g = self.cfg, self.cfg.relay, self.cfg.gateway
        rh = self.hosts[r.host]
        W = self.w(r.host)
        self.must(rh, f"mkdir -p {W}/relay {W}/run", "relay directory")
        rh.put(self.binary(cfg.main, r.platform, "gateway"), f"{W}/relay/queqiaod", if_changed=True)
        self.must(rh, f"chmod +x {W}/relay/queqiaod", "relay binary")
        for arm in cfg.arms + cfg.beside_arms():
            port = g.port_base + arm.index
            self.spawn(rh, f"relay-{arm.name}", f"{W}/relay", self.relay_cmd(arm))
            self.wait_port(rh, port, f"relay {arm.name}", f"relay-{arm.name}")

    def start_host_cpu(self, name: str, path: str) -> Dict[str, Any]:
        """Start a read-only CPU sampler (a job, so teardown stops it) on every
        host the trial loads; returns what stop_host_cpu needs."""
        out: Dict[str, Any] = {}
        hosts = self.host_cpu_hosts(path)
        parallel(*[(lambda host=host: self.hosts[host].run(f"rm -f {self.w(host)}/run/stop-hostcpu",
                                                            what=f"clear the host CPU sampler's stop file on {host}"))
                   for host in hosts])
        def go(host):
            # Detached, like the jobs, and not followed while it runs: its
            # samples stay in its output file on the host and are fetched
            # once it is stopped, so a dropped ssh session mid-trial costs
            # nothing. Launching is patient (SAMPLER_SSH_TRIES sessions).
            stop = f"{self.w(host)}/run/stop-hostcpu"
            units = [u for p in self.cfg.production if p.host == host for u in p.units]
            try:
                files, _ = self.start_job(host, host_cpu_sampler(stop, units), f"host CPU sampler on {host} ({name})",
                                          self.SAMPLER_SSH_TRIES)
            except Exception as e:  # recorded as missing evidence, not a dead campaign
                files = None
                self.log(f"host CPU sampler on {host} failed to start: {e}")
            out[host] = (files, stop)
        parallel(*[(lambda host=host: go(host)) for host in hosts])
        return out

    def stop_host_cpu(self, name: str, samplers: Dict[str, Any]) -> None:
        """Stop the samplers and keep their samples as hostcpu/<name>-<host>.tsv.
        Stopping and fetching are patient (SAMPLER_SSH_TRIES sessions, about
        three minutes of retries): a brief ssh outage delays the fetch rather
        than losing the trial's CPU record."""
        def go(host, files, stop):
            if files is None:
                return
            self.session(host, f"touch {stop}", f"stop the host CPU sampler on {host}", self.SAMPLER_SSH_TRIES)
            rc, text = self.wait_job(host, files, 60, f"host CPU sampler on {host} ({name})", self.SAMPLER_SSH_TRIES)
            if self.dry:
                return
            os.makedirs(os.path.join(self.out, "hostcpu"), exist_ok=True)
            if rc == 255:
                self.log(f"host CPU sampler on {host} ({name}): its samples could not be fetched")
                return
            with open(os.path.join(self.out, "hostcpu", f"{name}-{host}.tsv"), "w") as f:
                f.write("".join(line[2:] + "\n" for line in (text or "").splitlines() if line.startswith("S ")))
        parallel(*[(lambda host=host, files=files, stop=stop: go(host, files, stop))
                   for host, (files, stop) in samplers.items()])

    def collect(self, tagp: str, clients: List[Client], arms: Optional[List[Arm]] = None,
                out: Optional[str] = None) -> None:
        """Fetch and delete this block's logs: small hosts have little disk."""
        g = self.cfg.gateway
        gh = self.hosts[g.host]
        out = out or self.out
        for arm in arms or self.cfg.arms:
            d = self.gw_dir(arm)
            gh.run(f"cd {d}/logs && for f in server-{tagp}.jsonl*; do case $f in *.gz) ;; *) [ -f \"$f\" ] && gzip -1f \"$f\";; esac; done; true",
                   what=f"compress gateway {arm.name} logs")
            self._fetch_glob(gh, f"{d}/logs", f"server-{tagp}", os.path.join(out, "logs", f"gw-{arm.name}"))
        for c in clients:
            ch = self.hosts[c.host]
            for arm in arms or self.cfg.arms:
                d = self.cl_dir(c, arm)
                ch.run(f"cd {d}/logs && for f in client-{tagp}.jsonl* client-{tagp}-s*.jsonl*; do case $f in *.gz) ;; *) [ -f \"$f\" ] && gzip -1f \"$f\";; esac; done; true",
                       what=f"compress client {c.name}/{arm.name} logs")
                self._fetch_glob(ch, f"{d}/logs", f"client-{tagp}", os.path.join(out, "logs", f"cl-{c.name}-{arm.name}"))

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
        self.tearing_down = True
        if self.impair:
            # Before any process is stopped (the watchdogs among them): every
            # set removed and the tc state compared with the snapshot.
            try:
                left = self.impair.teardown()
            except Exception as e:
                left = [f"impairment: teardown failed: {type(e).__name__}: {e}"]
                self.impair.leftovers = left
            self.log("impairment: " + ("NOT RESTORED: " + "; ".join(left) if left else
                                       "removed; the tc state matches the snapshot"))
        # Only directories this invocation reserved, and only while it still
        # owns them: never another campaign's processes.
        for host in self.reserved:
            h = self.hosts[host]
            W = self.w(host)
            try:
                rc_, text_ = h.run(self.owned(host) + f'''[ -d {W}/run ] || exit 0
# Jobs first, by token only (JOB_OWN_SH): a descriptor is "<pid> <token>",
# each field checked on its own. Where ownership cannot be established
# nothing is signalled and the descriptor stays, reported as unverified.
''' + JOB_OWN_SH + f'''
m=$(own_mode)
if [ ! -r {W}/run ] || [ ! -x {W}/run ]; then echo "UNVERIFIED: cannot read and search {W}/run"; fi
for f in {W}/run/job-*.pgid; do
  [ -f "$f" ] || continue
  read -r p t x < "$f" 2>/dev/null || {{ echo "UNVERIFIED: cannot read $f"; continue; }}
  case "$p" in ""|*[!0-9]*) echo "stale job descriptor $f left alone"; continue;; esac
  tok_ok "$t" && [ -z "$x" ] || {{ echo "stale job descriptor $f left alone"; continue; }}
  [ "$m" = none ] && {{ echo "UNVERIFIED: $f: job ownership cannot be established on this host"; continue; }}
  ps_=$(members "$t" "$m" "$p"); r=$?
  [ $r -eq 2 ] && {{ echo "UNVERIFIED: $f: the processes could not be inspected"; continue; }}
  [ $r -eq 0 ] && kill -TERM $ps_ 2>/dev/null
done
sleep 1
for f in {W}/run/job-*.pgid; do
  [ -f "$f" ] || continue
  read -r p t x < "$f" 2>/dev/null || continue
  case "$p" in ""|*[!0-9]*) continue;; esac
  tok_ok "$t" && [ -z "$x" ] && [ "$m" != none ] || continue
  ps_=$(members "$t" "$m" "$p"); r=$?
  [ $r -eq 0 ] && kill -KILL $ps_ 2>/dev/null
  for i in 1 2 3 4 5 6 7 8 9 10; do members "$t" "$m" "$p" >/dev/null; r=$?; [ $r -ne 0 ] && break; sleep 0.2; done
  # Only a verified absence retires the descriptor.
  case $r in
    1) rm -f "$f" "${{f%.pgid}}.sh" ;;
    0) echo "job processes survive: $f" ;;
    *) echo "UNVERIFIED: $f: the processes could not be inspected" ;;
  esac
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
                self.teardown_notes += [f"{host}: {x.strip()}" for x in text_.splitlines()
                                        if x.startswith(("UNVERIFIED", "job processes survive"))]
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
            os.makedirs(self.ssh_dir, exist_ok=True)
            for h in self.hosts.values():
                h.timing_path = os.path.join(self.out, "timing.tsv")
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
                    "sequential": cfg.sequential.__dict__ if cfg.sequential else None,
                    "arms": manifest, "schedule": [s.__dict__ for s in slots], "main": cfg.main.name,
                    "clients": {c.name: {"small": c.small, "platform": c.platform, "flags": c.flags}
                                for c in cfg.clients.values()},
                    # Behaviour switches every arm's endpoints also get, for
                    # the flip's final-bundle identity.
                    "gateway_flags": cfg.gateway.flags,
                    # Hosts whose CPU each path's trials record (host_cpu).
                    "hostcpu_hosts": {p: self.host_cpu_hosts(p) for p in cfg.paths()} if cfg.host_cpu else None,
                    "gateway_host": cfg.gateway.host,
                    # The impairment's filters and frozen scenarios.
                    "impair": self.impair.describe() if self.impair else None,
                    # The gates in force, frozen before the first block: the
                    # analyzer refuses a verdict under other ones.
                    "gates_sha256": gates_digest(),
                    # The DC gate's frozen bounds likewise (dc_gate.py).
                    "dc_bounds_sha256": gates_digest(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                                  "validation", "dc_gate.ini"))
                    if cfg.wl.analyzer == "dc_gate" else None}
            if not self.dry:
                with open(os.path.join(self.out, "plan.json"), "w") as f:
                    json.dump(plan, f, indent=2, default=str)
                if self.impair:
                    with open(os.path.join(self.out, "impair-before.txt"), "w") as f:
                        f.write(self.impair.before)
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
            beside = cfg.wl.beside(cfg)
            if beside:
                # D: its sessions start first, on their own instances, and the
                # blocks wait for their set-up traffic to end.
                self.start_beside(beside)
            done_types: Dict[Tuple[str, str], str] = {}
            units = run_units(cfg, slots)
            last_block = 0
            told = False
            for block, path, trial in units:
                if self.dry and block > dry_blocks:
                    if not told:
                        print(f"... blocks {dry_blocks + 1}-{cfg.blocks} repeat the same commands in their own order",
                              file=self.stream)
                        told = True
                    continue
                if (path, trial) in done_types:
                    continue
                if block > last_block:
                    if cfg.snapshot_every and last_block and last_block % cfg.snapshot_every == 0:
                        self.check_production(f"mid-b{last_block:03d}")
                    last_block = block
                self.run_block(block, path, [s for s in slots if (s.block, s.path, s.trial) == (block, path, trial)],
                               writer, trial)
                self.units_run.append([block, path, trial])
                self.blocks_run = max(self.blocks_run, block)
                if not self.dry:
                    time.sleep(cfg.block_gap)
                if cfg.sequential and not self.dry and block >= cfg.sequential.first and block < cfg.blocks:
                    # Recompute this type's gate on its blocks so far; stop it
                    # once its verdict is determined (ab_stats.Sequential).
                    if tsv:
                        tsv.flush()
                    stop, why = self.interim(path, trial)
                    self.log(f"look after block {block} on {path}{' ' + trial if trial else ''}: {why}")
                    if stop:
                        done_types[(path, trial)] = f"after block {block}: {why}"
                        self.stopped_types[f"{path}/{trial}" if trial else path] = f"after block {block}: {why}"
            if self.stopped_types:
                self.stopped = "; ".join(f"{k} {v}" for k, v in self.stopped_types.items())
            if beside:
                self.finish_beside(beside)
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
        for h in self.hosts.values():
            try:
                h.close()
            except Exception:   # a connection already gone needs no closing
                pass
        if not self.dry:
            # The analyzers issue a verdict only for a campaign that finished,
            # cleaned up after itself and left production as it found it.
            with open(os.path.join(self.out, "status.json"), "w") as f:
                json.dump({"status": "complete" if status == 0 else "aborted", "exit": status,
                           "production": production, "teardown_leftovers": leftovers, "cleanup_failures": clean_failures,
                           "fetch_failures": [f for f in self.fetch_failures
                                              if f not in getattr(self, "beside_fetch_failures", [])],
                           "job_failures": self.job_failures,
                           "workload_failures": self.workload_failures,
                           "impairment": (None if not self.impair else
                                          "not restored" if self.impair.leftovers else "restored"),
                           "blocks_run": self.blocks_run, "units_run": self.units_run, "stopped": self.stopped,
                           "stopped_types": self.stopped_types,
                           "wall_seconds": round(time.time() - self.t_start, 1),
                           "finished_utc": utc(time.time())}, f, indent=2)
            sub = os.path.join(self.out, cfg.wl.beside(cfg).name) if cfg.wl.beside(cfg) else None
            if sub and os.path.isdir(sub):
                with open(os.path.join(sub, "status.json"), "w") as f:
                    json.dump({"status": "complete" if status == 0 else "aborted", "exit": status,
                               "production": production, "teardown_leftovers": leftovers,
                               "cleanup_failures": clean_failures,
                               "fetch_failures": getattr(self, "beside_fetch_failures", []),
                               "job_failures": getattr(self, "beside_failures", []), "blocks_run": 1,
                               "finished_utc": utc(time.time())}, f, indent=2)
        return status

    def verify_teardown(self) -> List[str]:
        """Temporary listeners (TCP or UDP) or processes that survived
        teardown, read from each host's listener and process tables."""
        if self.dry:
            self.snapshot_all("teardown")
            return []
        snaps = self.snapshot_all("teardown")
        out = list(self.impair.leftovers) if self.impair else []
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
        for host in self.reserved:
            # Teardown removes a job's descriptor only once none of its
            # processes is left: one still there names survivors.
            # An unreadable directory or a failed check is unverified, not clean.
            W = self.w(host)
            rc, text = self.hosts[host].run(
                f"[ -d {W}/run ] && [ -r {W}/run ] && [ -x {W}/run ] || exit 3\n"
                f"for f in {W}/run/job-*.pgid; do [ -e \"$f\" ] && echo \"$f\"; done; exit 0",
                what="verify no job process survived")
            if rc != 0 and not self.dry:
                out.append(f"{host}: could not check for surviving job processes (exit {rc})")
            out += [f"{host}: a job's processes were not verified gone ({x.strip()})" for x in text.splitlines()
                    if x.strip().endswith(".pgid")]
        out += self.teardown_notes
        return out

    def block_seconds(self, path: str, trial: str = "") -> float:
        """Estimated wall seconds of one block of one trial type on a path:
        its trials (with the gap after each), its extra trials, the per-block
        restarts and the gap after the block."""
        cfg = self.cfg
        trials = len(cfg.arms) * cfg.wl.trials_per_arm
        return (trials * (cfg.wl.trial_seconds(cfg, path, trial) + cfg.gap) + cfg.wl.extra_block_seconds(cfg, path)
                + BLOCK_OVERHEAD + cfg.block_gap)

    def estimate(self) -> Dict[str, Tuple[float, float]]:
        """Estimated wall seconds per path: (if every type stops at the first
        look, at the cap), set-up plus one block of each trial type per round."""
        cfg = self.cfg
        out = {}
        first = cfg.sequential.first if cfg.sequential else cfg.blocks
        for path in cfg.paths():
            block = sum(self.block_seconds(path, t) for t in cfg.wl.trial_types(cfg))
            once = SETUP_SECONDS + cfg.wl.extra_once_seconds(cfg, path)
            out[path] = (once + first * block, once + cfg.blocks * block)
        return out

    def print_plan(self, slots: List[Slot]) -> None:
        cfg = self.cfg
        p = lambda *a: print(*a, file=self.stream)
        p(f"# campaign {cfg.name}: workload {cfg.workload}, {cfg.blocks} block(s), seed {cfg.seed}")
        if cfg.sequential:
            sq = cfg.sequential
            p(f"# sequential: looks after blocks {sq.first}..{sq.cap}, one-sided alpha {sq.alpha:g} spent "
              f"{'by the Pocock-type function' if sq.spending == 'pocock' else 'equally (Bonferroni)'}; "
              "nominal level per look " + ", ".join(f"{n}: {a:.4f}" for n, a in zip(range(sq.first, sq.cap + 1), sq.levels)))
        types = cfg.wl.trial_types(cfg)
        for path, (lo, hi) in self.estimate().items():
            if types != [""]:
                p(f"# estimated per block on {path}: " + ", ".join(
                    f"{t} {self.block_seconds(path, t) / 60:.0f} min" for t in types)
                  + f"; {sum(self.block_seconds(path, t) for t in types) / 60:.0f} min a round")
            span = f"{lo / 60:.0f} min" if lo == hi else f"{lo / 60:.0f} min (stopping at the first look) to {hi / 60:.0f} min (cap)"
            p(f"# estimated wall time on {path}: {span}")
        for line in cfg.wl.plan_notes(cfg):
            p(f"# {line}")
        p(f"# work directory on every host: ~/{cfg.work_dir}")
        p(f"# gateway host {cfg.gateway.host} ({cfg.gateway.platform}), clients dial {cfg.endpoint_address}"
          + (f" (relay on {cfg.relay.host}, forwarding each arm's port to {cfg.gateway.address})" if cfg.relay else ""))
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
        p("# schedule, in run order (block path [type]: slot order; * = cold start of that arm in the block)")
        for b, path, trial in run_units(cfg, slots):
            us = [s for s in slots if (s.block, s.path, s.trial) == (b, path, trial)]
            p(f"  b{b:03d} {path}{' ' + trial if trial else ''}:" + "".join(f" {s.arm}{'*' if s.cold else ''}" for s in us))


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

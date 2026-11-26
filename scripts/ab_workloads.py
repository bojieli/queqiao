#!/usr/bin/env python3
"""Workloads for the live A/B runner, registered by name.

A workload is three things that travel together:

* a **generator**: the phases of one trial, each a set of jobs run at once on
  client hosts. A job is one generator script (a registered `Generator`)
  through the arm's temporary SOCKS5 client. Load jobs run for the phase;
  the others measure under that load and stop when it ends.
* an **endpoint**: what the generator talks to, on the gateway host's
  loopback. Downloads use the HTTP origin every campaign starts; a workload
  that needs another service declares an `Endpoint`, whose ports the
  preflight reserves like any other.
* a **metrics extractor**: `extract(trial, plan)` turns one trial's rows into
  named measures, `"<phase>.<metric>"`. The analyzer judges each against the
  rule pre-registered for it in `scripts/validation/gates.ini`, section
  `[workload:<name>]`.

Built in: `bulk`, `single` and `interactive` (section 15.3's workloads, judged
by the section 15.4 primary gate and the `[secondary]` bounds), and `dc` (the
stage 9 gate, judged by `dc_gate.py`). A new workload is a subclass with
`@register`, in this file or in a module under `scripts/workloads/`, which is
imported automatically. See docs/VALIDATING-A-BUNDLE.md, "Adding a workload".
"""
from __future__ import annotations

import configparser
import glob
import importlib.util
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ab_campaign as ab  # noqa: E402  (ab_campaign registers itself under this name)


# --- the parts ------------------------------------------------------------------

@dataclass
class Job:
    client: Any                     # ab_campaign.Client
    kind: str                       # a registered Generator
    n: int = 1                      # concurrency, for generators that have one
    load: bool = True               # False: measures under the load, stops when it ends
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Phase:
    name: str
    duration: float
    jobs: List[Job]
    # Start every job on a shared remote clock (BARRIER_LEAD seconds ahead),
    # for phases whose jobs on different hosts must load the path together.
    barrier: bool = False
    # How long the phase normally takes, for wall-time estimates; `duration`
    # is the cap its jobs' timeouts derive from. 0: the same as duration.
    expected: float = 0.0


@dataclass
class Endpoint:
    """A service on the gateway host's loopback, started from the work
    directory (so stop() can identify it) before the first block."""
    tag: str                        # process tag; the pid file is run/<tag>.pid
    command: str                    # shell, run in the work directory; {W} is the work directory
    tcp_ports: List[Tuple[int, str]] = field(default_factory=list)
    udp_ports: List[Tuple[int, str]] = field(default_factory=list)
    files: List[str] = field(default_factory=list)   # local files copied into the work directory first


@dataclass
class Beside:
    """A paired session that runs beside the blocks (trial D): each arm on its
    own gateway and client instances, which the blocks' restarts do not touch.
    `job` runs once per path and arm; `ready_file` (on the client host, with
    {arm} for the arm) appears once the session's set-up traffic is done, and
    the blocks wait for it; `workload` judges the session as its own campaign
    in the subdirectory `name`."""
    name: str
    workload: str
    phase: Phase
    ready_seconds: float = 120.0


@dataclass
class Generator:
    """script(campaign, job, arm, duration, start_at) -> shell for the client
    host; parse(output, job, duration) -> rows of (flow, http, bytes,
    seconds, curl_exit, t_start, t_end); expected(job) -> the byte count of a
    complete transfer (0 when not applicable)."""
    script: Callable[..., str]
    parse: Callable[..., List[Tuple[str, ...]]]
    expected: Callable[[Job], int]
    # The job's raw records for the extractor; by default its
    # "J<TAB>{json}" lines. None: the generator has none.
    records: Optional[Callable[[str], List[dict]]] = None


def json_records(out: str) -> List[dict]:
    """The "J<TAB>{json}" lines of a job's output."""
    import json
    recs = []
    for line in out.splitlines():
        if line.startswith("J\t"):
            try:
                recs.append(json.loads(line[2:]))
            except ValueError:
                continue
    return recs


GENERATORS: Dict[str, Generator] = {}
WORKLOADS: Dict[str, type] = {}


def generator(name: str, gen: Generator) -> None:
    if name in GENERATORS:
        raise ValueError(f"generator {name!r} registered twice")
    GENERATORS[name] = gen


def register(cls: type) -> type:
    if not cls.name or cls.name in WORKLOADS:
        raise ValueError(f"workload {cls.name!r} must have a unique name")
    WORKLOADS[cls.name] = cls
    return cls


def get(name: str) -> "Workload":
    load_plugins()
    if name not in WORKLOADS:
        raise ab.ConfigError(f"workload must be one of {', '.join(sorted(WORKLOADS))}")
    return WORKLOADS[name]()


_plugins_loaded = False


def load_plugins(directory: Optional[str] = None) -> None:
    """Import every scripts/workloads/*.py once; each registers itself."""
    global _plugins_loaded
    if _plugins_loaded and directory is None:
        return
    d = directory or os.path.join(os.path.dirname(os.path.abspath(__file__)), "workloads")
    for path in sorted(glob.glob(os.path.join(d, "*.py"))):
        name = "ab_workload_" + os.path.splitext(os.path.basename(path))[0]
        if name in sys.modules:
            continue
        spec = importlib.util.spec_from_file_location(name, path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[name] = mod
        assert spec.loader is not None
        spec.loader.exec_module(mod)
    if directory is None:
        _plugins_loaded = True


class Workload:
    """Defaults describe one measured path per client and no extra endpoint."""
    name = ""
    section = ""                    # its config section; "" means [workload:<name>]
    # Measure names (the part after "<phase>.") reported for context but not
    # judged. Every other extracted measure must have a rule in gates.ini,
    # or the workload's verdict is INCOMPLETE.
    report_only: Tuple[str, ...] = ()
    analyzer = "ab_analyze"         # or "dc_gate"
    # True: the section 15.4 primary gate on useful bytes (and the
    # [secondary] bounds) applies; False: only the [workload:<name>] rules.
    primary = True
    # Trials per arm per block: 2 (ABBA, section 15.3), or 1 for a workload
    # too long to repeat (one session per arm, arms in random order).
    trials_per_arm = 2
    # True: judged over its pooled samples (samples()), not per block.
    sample_based = False
    # A rule-judged workload's primary outcomes ("<phase>.<metric>"), voided
    # like section 15.4's primary when too many of main's blocks are zero.
    primaries: Tuple[str, ...] = ()
    # Section 15.4's generic secondaries ([secondary] in gates.ini) that apply
    # to a rule-judged workload's phases: phase -> metric names, judged by
    # their [secondary] bound as rules of that phase.
    generic: Dict[str, Tuple[str, ...]] = {}

    def configure(self, cfg: Any, cp: configparser.ConfigParser) -> None:
        """Read and validate this workload's own config section."""

    def params(self, cp: configparser.ConfigParser) -> Dict[str, str]:
        """This workload's config section, [workload:<name>] by default."""
        section = self.section or f"workload:{self.name}"
        return dict(cp[section]) if cp.has_section(section) else {}

    def positive(self, cfg: Any) -> List[Tuple[str, float]]:
        """(name, value) pairs that must be positive."""
        return []

    def paths(self, cfg: Any) -> List[str]:
        return list(cfg.clients)

    def clients_for(self, cfg: Any, path: str) -> List[Any]:
        return [cfg.clients[path]]

    def phases(self, cfg: Any, path: str) -> List[Phase]:
        raise NotImplementedError

    def trial_types(self, cfg: Any) -> List[str]:
        """Trial types that run as blocks of their own (DESIGN-PATH-AUTHORITY.md
        section 15.3): each is a block of four trials with the endpoints
        restarted before it, the types taken round-robin at block granularity,
        and each stopped on its own under sequential stopping. A type is the
        name of the one phase it runs. The default, [""], is one type that
        runs every phase in each trial."""
        return [""]

    def type_phases(self, cfg: Any, path: str, trial: str) -> List[Phase]:
        """The phases one trial of type `trial` runs."""
        phases = self.phases(cfg, path)
        return phases if not trial else [p for p in phases if p.name == trial]

    def trial_seconds(self, cfg: Any, path: str, trial: str) -> float:
        """Nominal seconds of one trial of this type, for the wall-time
        estimate (the runner adds its per-block restarts)."""
        return float(sum(ph.expected or ph.duration for ph in self.type_phases(cfg, path, trial)))

    def run_first(self, cfg: Any) -> List[Tuple[int, str]]:
        """(block, type) units to run before the round-robin, in this order:
        a session that must overlap particular blocks (D beside C)."""
        return []

    def beside(self, cfg: Any) -> Optional["Beside"]:
        """A paired session run beside the blocks on its own instances (D)."""
        return None

    def plan_notes(self, cfg: Any) -> List[str]:
        """Extra lines for the dry-run plan (estimates of campaigns run apart)."""
        return []

    def objects(self, cfg: Any) -> List[Tuple[str, int]]:
        """Files the HTTP origin serves, as (name, size)."""
        return []

    def endpoints(self, cfg: Any) -> List[Endpoint]:
        return []

    def gateway_flags(self, cfg: Any) -> List[str]:
        return []

    def client_files(self, cfg: Any) -> List[str]:
        """Local files its generators run on client hosts, copied into each
        client host's work directory at setup."""
        return []

    def client_flags(self, cfg: Any) -> List[str]:
        return []

    def owned_flags(self) -> Tuple[str, ...]:
        """Flags this workload sets on both ends, which a config may not set."""
        return ()

    def after_block(self, campaign: Any, block: int, path: str, tagp: str, writer: Any) -> None:
        """Extra trials at the end of each block (the DC calibration)."""

    def extra_block_seconds(self, cfg: Any, path: str) -> float:
        """Seconds of extra trials per block (the DC calibration)."""
        return 0.0

    def extra_once_seconds(self, cfg: Any, path: str) -> float:
        """Seconds of extra trials in the first block only."""
        return 0.0

    def samples(self, trials: Sequence[Any]) -> Dict[str, List[float]]:
        """For a sample_based workload: pooled samples by name."""
        return {}

    def extract(self, trial: Any, plan: dict) -> Dict[str, float]:
        """Per-trial measures, "<phase>.<metric>", for the rules in gates.ini."""
        return {}

    def block_tails(self, trials: Sequence[Any], plan: dict) -> Dict[str, Tuple[float, List[float]]]:
        """For each tail-percentile measure of an arm's block, (q, its pooled
        samples): one with too few samples per block is judged over every
        block's samples pooled instead (section 15.4)."""
        return {}

    def block_extract(self, trials: Sequence[Any], plan: dict) -> Dict[str, float]:
        """An arm's measures for one block. By default the mean of its
        trials' measures, NaN unless every trial has the measure; a workload
        may instead pool its trials' raw samples."""
        per = [self.extract(t, plan) for t in trials]
        keys = set().union(*per) if per else set()
        out = {}
        for k in keys:
            vals = [p.get(k, float("nan")) for p in per]
            out[k] = sum(vals) / len(vals) if vals and all(v == v for v in vals) else float("nan")
        return out


# --- built-in generators ------------------------------------------------------------

def _curl(campaign: Any, job: Job, arm: Any) -> Tuple[str, str]:
    port = job.client.socks_port_base + arm.index
    url = f"http://127.0.0.1:{campaign.cfg.gateway.origin_port}/"
    curl = (f"{ab.CURL_ENV} --socks5-hostname 127.0.0.1:{port} -o /dev/null -s "
            f"-w '%{{http_code}}\\t%{{size_download}}\\t%{{time_total}}\\t%{{exitcode}}'")
    return url, curl


# Every request prints "start end flow http bytes seconds exit", the times in
# milliseconds on the job host's clock (NOW_SH), so overlap with other hosts'
# load can be shown rather than assumed.
_ROW = "printf '%s\\t%s\\t%s\\t%s\\n' \"$t0\" \"$t1\""


def _barrier(start_at: int) -> str:
    return f"while [ $(date +%s) -lt {start_at} ]; do sleep 0.1; done; " if start_at else ""


def bulk_script(campaign: Any, job: Job, arm: Any, duration: float, start_at: int = 0) -> str:
    """n flows, each fetching object after object until the window closes, so
    it stays backlogged: a fast arm cannot run out of bytes and be capped at
    the same total as a slow one."""
    url, curl = _curl(campaign, job, arm)
    obj = job.params.get("object_bytes", campaign.cfg.object_bytes)
    # One sub-second deadline for every flow; each object gets exactly the
    # time left (curl takes a fractional --max-time).
    return (ab.NOW_SH + _barrier(start_at) + f"end=$(awk -v n=\"$(now)\" 'BEGIN{{printf \"%.3f\", n + {float(duration)}}}'); "
            f"for i in $(seq 1 {job.n}); do ( k=0; while :; do "
            f"rem=$(awk -v e=\"$end\" -v n=\"$(now)\" 'BEGIN{{r = e - n; if (r < 0.05) print 0; else printf \"%.3f\", r}}'); "
            f"[ \"$rem\" != 0 ] || break; "
            f"k=$((k+1)); t0=$(now); r=$({curl} --max-time $rem {url}obj-{obj} 2>/dev/null); t1=$(now); "
            f"{_ROW} \"b${{i}}o$k\" \"$r\"; "
            f"[ \"${{r##*$'\\t'}}\" = 0 ] && [ \"${{r%%$'\\t'*}}\" = 200 ] || break; done ) & done; wait")


def requests_script(campaign: Any, job: Job, arm: Any, duration: float, start_at: int = 0) -> str:
    """waves x n concurrent fetches; with start_at, wave w starts at start_at +
    (w - 1) x timeout on every host, so leaves cannot run their waves apart."""
    url, curl = _curl(campaign, job, arm)
    waves, timeout, size = job.params["waves"], job.params["timeout"], job.params["bytes"]
    wave = (f"ws=$(( {start_at} + (w - 1) * {timeout} )); while [ $(date +%s) -lt $ws ]; do sleep 0.1; done; "
            if start_at else "")
    return (ab.NOW_SH + f"for w in $(seq 1 {waves}); do {wave}for i in $(seq 1 {job.n}); do ( t0=$(now); "
            f"r=$({curl} --max-time {timeout} {url}obj-{size} 2>/dev/null); t1=$(now); {_ROW} \"w${{w}}r$i\" \"$r\" ) & "
            f"done; wait; done")


def probe_script(campaign: Any, job: Job, arm: Any, duration: float, start_at: int = 0) -> str:
    """A small fetch every interval until the window closes or the runner
    drops the stop file after the phase's load finished."""
    url, curl = _curl(campaign, job, arm)
    stop = campaign.stop_file(job.client, arm)
    # On the phase's barrier like its load, so the window it measures is the
    # window the load runs in.
    return (ab.NOW_SH + _barrier(start_at) + f"end=$(( $(date +%s) + {int(duration)} )); i=0; "
            f"while [ $(date +%s) -lt $end ] && [ ! -e {stop} ]; do i=$((i+1)); "
            f"t0=$(now); r=$({curl} --max-time {ab.PROBE_TIMEOUT} {url}obj-{job.params['bytes']} 2>/dev/null); t1=$(now); "
            f"{_ROW} \"p$i\" \"$r\"; sleep {job.params['interval']}; done; rm -f {stop}")


generator("bulk", Generator(bulk_script, lambda out, job, d: ab.job_rows(out, "bulk", job.n, 1, d),
                            lambda job: int(job.params.get("expected", 0))))
generator("requests", Generator(requests_script,
                                lambda out, job, d: ab.job_rows(out, "requests", job.n, job.params["waves"], d),
                                lambda job: int(job.params["bytes"])))
generator("probe", Generator(probe_script, lambda out, job, d: ab.job_rows(out, "probe", 1, 1, d),
                             lambda job: int(job.params["bytes"])))


# --- built-in workloads --------------------------------------------------------------

class _Download(Workload):
    analyzer = "ab_analyze"
    probe = False

    def flows(self, cfg: Any) -> int:
        return cfg.flows

    def positive(self, cfg: Any) -> List[Tuple[str, float]]:
        return [("flows", self.flows(cfg)), ("object_bytes", cfg.object_bytes),
                ("probe_bytes", cfg.probe_bytes), ("probe_interval", cfg.probe_interval)]

    def objects(self, cfg: Any) -> List[Tuple[str, int]]:
        objs = [(f"obj-{cfg.object_bytes}", cfg.object_bytes)]
        if self.probe:
            objs.append((f"obj-{cfg.probe_bytes}", cfg.probe_bytes))
        return objs

    def phases(self, cfg: Any, path: str) -> List[Phase]:
        client = cfg.clients[path]
        jobs = [Job(client, "bulk", self.flows(cfg), params={"expected": cfg.object_bytes})]
        if self.probe:
            jobs.append(Job(client, "probe", 1, load=False,
                            params={"bytes": cfg.probe_bytes, "interval": cfg.probe_interval}))
        return [Phase("main", cfg.duration, jobs)]


@register
class Bulk(_Download):
    """Four concurrent downloads (section 15.3)."""
    name = "bulk"


@register
class Single(_Download):
    """One flow (section 15.3)."""
    name = "single"

    def flows(self, cfg: Any) -> int:
        return 1


@register
class Interactive(_Download):
    """The bulk load plus a small-object probe (section 15.3)."""
    name = "interactive"
    probe = True


@dataclass
class DCParams:
    loaded: List[str]
    control: str
    bulk_flows_per_leaf: int
    request_bytes: int
    requests_per_leaf: int
    waves: int
    request_timeout: int
    control_interval: float
    # Solo calibration of each loaded leaf: "every" block, the "first" block
    # only (the minimal profile), or "false".
    calibrate: str = "every"
    # "waves": request waves of downloads after the sustained phase (the full
    # gate); "rpc": the model's W4 at minimal size before it -- `rpc_requests`
    # requests one at a time, `rpc_spacing` s apart, ASR (355 KB up, 200 B
    # down) and TTS (300 B up, 300 KB down) alternating, the first on a cold
    # app connection (design section 15.3, trial DC).
    request_mode: str = "waves"
    rpc_requests: int = 10
    rpc_spacing: float = 2.0
    rpc_port: int = 23485


def _calibrate(v: str) -> str:
    v = v.strip().lower()
    if v in ("first", "every"):
        return v
    return "every" if ab._bool(v) else "false"


ASR_UP, ASR_DOWN, TTS_UP, TTS_DOWN = 355_000, 200, 300, 300_000   # W4's requests
CLIENT_PY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "validation", "wlclient.py")
SERVER_PY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "validation", "wlserver.py")


def rpc_script(campaign: Any, job: Job, arm: Any, duration: float, start_at: int = 0) -> str:
    """W4 at minimal size through the leaf's client: wlclient rpc, on the
    phase's shared start. The barrier is passed to the client rather than
    waited for in the shell: the interpreter starts at once and waits for
    the barrier itself after initialising, so its start-up time is not added
    to every wave on one leaf host and not on another."""
    dc = campaign.cfg.dc
    W = campaign.w(job.client.host)
    at = f"--start-at {start_at} " if start_at else ""
    return (f"python3 {W}/wlclient.py rpc {at}--socks 127.0.0.1:{job.client.socks_port_base + arm.index} "
            f"--target 127.0.0.1:{dc.rpc_port} --count {dc.rpc_requests} --spacing {dc.rpc_spacing:g} "
            f"--timeout {dc.request_timeout} --asr-up {ASR_UP} --asr-down {ASR_DOWN} --tts-up {TTS_UP} "
            f"--tts-down {TTS_DOWN}")


def _rpc_rows(out: str, job: Job, duration: float) -> List[Tuple[str, ...]]:
    rows = []
    for line in out.splitlines():
        p = line.split("\t")
        if len(p) == 7 and p[0] != "J":
            t0, t1, flow, code, size, secs, ex = p
            rows.append((flow, code, size, secs, ex, t0, t1))
    if not rows:
        rows.append(("w1r1", "000", "0", f"{duration:.3f}", "-1", "", ""))   # the job died: a failure
    return rows


generator("dc-rpc", Generator(rpc_script, _rpc_rows, lambda job: 0))


@register
class DC(Workload):
    """The stage 9 gate (section 11.3): loaded leaves and an uncongested
    control sibling, all at once, plus solo calibration. Judged by dc_gate.py."""
    name = "dc"
    section = "dc"
    analyzer = "dc_gate"

    def configure(self, cfg: Any, cp: configparser.ConfigParser) -> None:
        if not cp.has_section("dc"):
            raise ab.ConfigError("workload = dc needs a [dc] section")
        d = cp["dc"]
        dc = DCParams(loaded=ab._words(d.get("loaded", "")), control=d.get("control", ""),
                      bulk_flows_per_leaf=d.getint("bulk_flows_per_leaf", 2), request_bytes=d.getint("request_bytes", 307200),
                      requests_per_leaf=d.getint("requests_per_leaf", 8), waves=d.getint("waves", 3),
                      request_timeout=d.getint("request_timeout", 30), control_interval=d.getfloat("control_interval", 1.0),
                      calibrate=_calibrate(d.get("calibrate", "true")), request_mode=d.get("request_mode", "waves"),
                      rpc_requests=d.getint("rpc_requests", 10), rpc_spacing=d.getfloat("rpc_spacing", 2.0),
                      rpc_port=d.getint("rpc_port", 23485))
        if dc.request_mode not in ("waves", "rpc"):
            raise ab.ConfigError("[dc] request_mode must be waves or rpc")
        if len(dc.loaded) < 2:
            raise ab.ConfigError("[dc] loaded needs at least two leaves")
        for leaf in dc.loaded + [dc.control]:
            if leaf not in cfg.clients:
                raise ab.ConfigError(f"[dc] leaf {leaf!r} is not a [client:...] section")
        if dc.control in dc.loaded:
            raise ab.ConfigError("[dc] control must not be one of the loaded leaves")
        hosts = [cfg.clients[x].host for x in dc.loaded + [dc.control]]
        if len(set(hosts)) != len(hosts):
            # A leaf is an endpoint pair; two clients on one host share one
            # source address, are one leaf, and would run two temporary
            # clients on one host at once.
            raise ab.ConfigError("[dc] every leaf must be on its own host")
        cfg.dc = dc

    def positive(self, cfg: Any) -> List[Tuple[str, float]]:
        dc = cfg.dc
        return [("object_bytes", cfg.object_bytes), ("[dc] request_timeout", dc.request_timeout), ("[dc] waves", dc.waves),
                ("[dc] requests_per_leaf", dc.requests_per_leaf), ("[dc] bulk_flows_per_leaf", dc.bulk_flows_per_leaf),
                ("[dc] request_bytes", dc.request_bytes), ("[dc] control_interval", dc.control_interval)]

    def owned_flags(self) -> Tuple[str, ...]:
        return ("path-profile",)

    def gateway_flags(self, cfg: Any) -> List[str]:
        return ["--path-profile", "dc-long-haul"]

    def client_flags(self, cfg: Any) -> List[str]:
        return ["--path-profile", "dc-long-haul"]

    def paths(self, cfg: Any) -> List[str]:
        return ["dc"]      # every leaf runs at once: a single path

    def clients_for(self, cfg: Any, path: str) -> List[Any]:
        return [cfg.clients[x] for x in cfg.dc.loaded + [cfg.dc.control]]

    def objects(self, cfg: Any) -> List[Tuple[str, int]]:
        return [(f"obj-{cfg.object_bytes}", cfg.object_bytes), (f"obj-{cfg.dc.request_bytes}", cfg.dc.request_bytes)]

    def endpoints(self, cfg: Any) -> List[Endpoint]:
        if cfg.dc.request_mode != "rpc":
            return []
        p = cfg.dc.rpc_port
        return [Endpoint("wlserver", f"python3 {{W}}/wlserver.py --http {p} --term {p + 1} --dir {{W}}",
                         tcp_ports=[(p, "rpc http"), (p + 1, "terminal")], files=[SERVER_PY])]

    def client_files(self, cfg: Any) -> List[str]:
        return [CLIENT_PY] if cfg.dc.request_mode == "rpc" else []

    def phases(self, cfg: Any, path: str) -> List[Phase]:
        dc = cfg.dc
        loaded = [cfg.clients[x] for x in dc.loaded]
        control = cfg.clients[dc.control]
        probe = {"bytes": dc.request_bytes, "interval": dc.control_interval}
        req = {"waves": dc.waves, "timeout": dc.request_timeout, "bytes": dc.request_bytes}
        if dc.request_mode == "rpc":
            # W4 first (one request at a time, all loaded leaves together),
            # then the sustained phase.
            rpc_window = dc.rpc_requests * dc.rpc_spacing
            # The job's cap is the serial worst case (every request taking its
            # whole timeout), so a candidate that fails them all is measured,
            # not cut off as a harness failure; `expected` is the usual time.
            worst = dc.rpc_requests * (dc.request_timeout + dc.rpc_spacing) + 10
            return [Phase("requests", worst,
                          [Job(c, "dc-rpc", 1) for c in loaded] + [Job(control, "probe", 1, load=False, params=probe)],
                          barrier=True, expected=rpc_window + 2),
                    Phase("sustained", cfg.duration,
                          [Job(c, "bulk", dc.bulk_flows_per_leaf, params={"expected": cfg.object_bytes}) for c in loaded]
                          + [Job(control, "probe", 1, load=False, params=probe)], barrier=True)]
        return [Phase("sustained", cfg.duration,
                      [Job(c, "bulk", dc.bulk_flows_per_leaf, params={"expected": cfg.object_bytes}) for c in loaded]
                      + [Job(control, "probe", 1, load=False, params=probe)], barrier=True),
                Phase("requests", dc.waves * dc.request_timeout,
                      [Job(c, "requests", dc.requests_per_leaf, params=req) for c in loaded]
                      + [Job(control, "probe", 1, load=False, params=probe)], barrier=True)]

    def extra_block_seconds(self, cfg: Any, path: str) -> float:
        return float(len(cfg.dc.loaded) * (cfg.duration + 15)) if cfg.dc.calibrate == "every" else 0.0

    def extra_once_seconds(self, cfg: Any, path: str) -> float:
        return float(len(cfg.dc.loaded) * (cfg.duration + 15)) if cfg.dc.calibrate == "first" else 0.0

    def after_block(self, campaign: Any, block: int, path: str, tagp: str, writer: Any) -> None:
        """Each loaded leaf alone on the main arm: the common ancestor binds
        only if the leaves together deliver clearly less than the sum of what
        each delivers alone (dc_gate.ini)."""
        cfg = campaign.cfg
        if cfg.dc.calibrate == "false" or (cfg.dc.calibrate == "first" and block != 1):
            return
        arm = cfg.main
        for i, name in enumerate(cfg.dc.loaded):
            leaf = cfg.clients[name]
            slot = ab.Slot(block, path, 100 + i, arm.name, 0, False)
            if leaf.small:
                campaign.start_client(leaf, arm, f"client-{tagp}-s{slot.slot}")
            campaign.scrape(arm, f"{tagp}-s{slot.slot}-{arm.name}-pre")
            campaign.scrape_client(leaf, arm, f"{tagp}-s{slot.slot}-{arm.name}-pre")
            campaign.run_phase(slot, arm, Phase("solo", cfg.duration,
                                                [Job(leaf, "bulk", cfg.dc.bulk_flows_per_leaf,
                                                     params={"expected": cfg.object_bytes})]), writer)
            campaign.scrape(arm, f"{tagp}-s{slot.slot}-{arm.name}-post")
            campaign.scrape_client(leaf, arm, f"{tagp}-s{slot.slot}-{arm.name}-post")
            if leaf.small:
                campaign.stop_client(leaf, arm)
            campaign.pause(cfg.gap)

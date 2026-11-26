#!/usr/bin/env python3
"""Analyze a campaign written by ab_campaign.py against the section 15.4 gate.

    ./scripts/ab_analyze.py CAMPAIGN_DIR                 # gate + secondaries
    ./scripts/ab_analyze.py CAMPAIGN_DIR --pilot         # sample size from a pilot

Per path and arm it reports goodput, completion, server loss, queue p90
(srtt - minRTT from the gateway's lane trace), stalls per trial, wire bytes
per useful byte, reissued bytes per source byte, flow-ACK transit p50/p90
(seq-matched when the build logs per-ACK events, otherwise count-matched from
the lane trace's acksout/acksin), client CPU p90, and interactive p99. Then,
per candidate arm, the paired per-block contrast candidate - 0.90 x main with
one-sided t and bootstrap lower bounds, and the pre-registered secondary
bounds from scripts/validation/gates.ini. The output is a markdown table meant
for a pull-request comment; it is also written to CAMPAIGN_DIR/summary.md.
"""
from __future__ import annotations

import argparse
import collections
import configparser
import csv
import datetime as dt
import glob
import gzip
import hashlib
import json
import math
import os
import re
import sys
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ab_stats as st  # noqa: E402
import ab_workloads  # noqa: E402

GATES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "validation", "gates.ini")
CLK_TCK = 100  # ticks per second in the runner's CPU samples
PROBE_TIMEOUT = 10.0  # ab_campaign.PROBE_TIMEOUT: an unfinished probe counts this long


# --- inputs -------------------------------------------------------------------

def ts(s: str) -> float:
    s = s.strip()
    s = s.rstrip("Z")
    s = re.sub(r"\.(\d+)", lambda m: "." + (m.group(1) + "000000")[:6], s, count=1)
    d = dt.datetime.fromisoformat(s)
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.timestamp()


def load_gates(path: str = GATES) -> dict:
    cp = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=(";", "#"))
    if not cp.read(path):
        raise SystemExit(f"cannot read {path}")
    p, w = cp["primary"], cp["power"]
    sec = {}
    if cp.has_section("secondary"):
        for k, v in cp["secondary"].items():
            better, margin = v.split()
            if better not in ("higher", "lower"):
                raise SystemExit(f"gates: {k}: direction must be higher or lower")
            sec[k] = (better, float(margin))
    workloads: Dict[str, Dict[str, Tuple[str, float]]] = {}
    for section in cp.sections():
        if section.startswith("workload:"):
            rules = {}
            for k, v in cp[section].items():
                parts = v.split()
                kinds = list(RULES) + list(HARD) + list(VS_MAIN)
                if parts and parts[0] in ("atmost", "atleast") and len(parts) in (2, 3):
                    pass
                elif len(parts) != 2 or parts[0] not in kinds:
                    raise SystemExit(f"gates: [{section}] {k}: want one of {', '.join(kinds)} and a number "
                                     "(atmost/atleast take an optional absolute slack)")
                try:
                    rules[k] = (parts[0], tuple(float(x) for x in parts[1:]))
                except ValueError:
                    raise SystemExit(f"gates: [{section}] {k}: {v!r} is not a number")
            workloads[section.split(":", 1)[1]] = rules
    with open(path, "rb") as f:
        digest = hashlib.sha256(f.read()).hexdigest()
    return {"ratio": p.getfloat("ratio"), "alpha": p.getfloat("alpha"), "workloads": workloads, "sha256": digest,
            "resamples": p.getint("bootstrap_resamples"), "seed": p.getint("bootstrap_seed"),
            "max_zero_main_fraction": p.getfloat("max_zero_main_fraction"),
            "secondary_min_coverage": p.getfloat("secondary_min_coverage", fallback=1.0),
            "harness": ({"control_plane_void_max": cp["harness"].getint("control_plane_void_max")}
                        if cp.has_section("harness") else None),
            "contention": ({k: cp["contention"].getfloat(k) for k in ("busy_threshold", "production_share_max",
                                                                       "steal_max_fraction")}
                           if cp.has_section("contention") else None),
            "pilot_blocks": w.getint("pilot_blocks"), "power": w.getfloat("power"),
            "true_ratio": w.getfloat("true_ratio"), "secondary": sec}


# Per-workload rules ([workload:<name>] in gates.ini): each is a per-block
# contrast d(cand, main) that must be positive at the one-sided (1 - alpha)
# level by both the t and the bootstrap lower bound.
# v is (multiplier,) or (multiplier, absolute slack) for atleast/atmost, as
# in WORKLOADS.md 3.0's "m x main + a"; (margin,) for higher/lower.
RULES: Dict[str, Callable[[float, float, Tuple[float, ...]], float]] = {
    "atleast": lambda c, m, v: c - (v[0] * m - (v[1] if len(v) > 1 else 0.0)),  # throughput: cand >= m x main - a
    "atmost": lambda c, m, v: (v[0] * m + (v[1] if len(v) > 1 else 0.0)) - c,   # latency: cand <= m x main + a
    "higher": lambda c, m, v: c - (m - v[0]),     # a rate near 1: cand >= main - v
    "lower": lambda c, m, v: (m + v[0]) - c,      # a rate near 0, or a latency near 0: cand <= main + v
}
# Hard assertions on the candidate alone, in every judged block ("not
# statistical", WORKLOADS.md): floor v (cand >= v), ceiling v (cand <= v).
HARD: Dict[str, Callable[[float, Tuple[float, ...]], bool]] = {
    "floor": lambda c, v: c >= v[0],
    "ceiling": lambda c, v: c <= v[0],
}


# Deterministic comparisons with main, not statistical (D's "delivered count
# at 16 min >= main's"): nofewer v (cand >= main - v).
VS_MAIN: Dict[str, Callable[[float, float, Tuple[float, ...]], bool]] = {
    "nofewer": lambda c, m, v: c >= m - v[0],
}


def rule_contrasts(kind: str, value, cand: Sequence[float], main: Sequence[float]) -> List[float]:
    v = value if isinstance(value, tuple) else (float(value),)
    return [RULES[kind](c, m, v) for c, m in zip(cand, main)]


def opener(p: str):
    return gzip.open(p, "rt", errors="replace") if p.endswith(".gz") else open(p, errors="replace")


def iter_json(paths: Iterable[str], markers: Tuple[str, ...]) -> Iterable[dict]:
    """JSON records from log files, parsing only lines containing a marker."""
    for p in paths:
        try:
            with opener(p) as f:
                for line in f:
                    if not any(m in line for m in markers):
                        continue
                    try:
                        yield json.loads(line)
                    except ValueError:
                        continue
        except (OSError, EOFError):
            continue


def metrics(campaign: str, name: str) -> Dict[str, float]:
    out: Dict[str, float] = {}
    try:
        with open(os.path.join(campaign, "metrics", f"{name}.txt")) as f:
            for line in f:
                k, _, v = line.strip().rpartition(" ")
                try:
                    out[k] = float(v)
                except ValueError:
                    pass
    except FileNotFoundError:
        pass
    return out


def useful(row: dict) -> int:
    """Bytes of the requested object: an HTTP 200 body, whole or partial.
    An error page is bytes on the wire, not useful bytes."""
    return int(row["bytes"] or 0) if row.get("http") == "200" else 0


# Least overlap, in seconds, that counts as "at the same time" between rows
# timed on different hosts: it absorbs NTP-level clock error and the few
# milliseconds the timestamps bracket around curl. Less is ambiguous, and an
# ambiguous sample is unmeasured, never assumed concurrent.
OVERLAP_MIN = 0.1


def tcp_evidence(rec: dict) -> bool:
    """A gateway flow record (remote flow complete / ended with error) shows
    TCP when its opening carrier was TCP or any lane that carried bytes was
    (an AUTO rescue can hand a flow to TCP without a client fallback)."""
    if rec.get("transport") != "quic":
        return True
    for lane in (rec.get("lane_bytes") or {}).values():
        if isinstance(lane, dict) and lane.get("Kind") not in (None, "quic") and \
                (lane.get("Sent", 0) or lane.get("Received", 0)):
            return True
    return False


def span(r: dict) -> Optional[Tuple[float, float]]:
    """A row's (start, end) on its host's clock, in milliseconds as the runner
    records them; None when a row has no sub-second times, since whole
    seconds cannot show that two requests overlapped."""
    a, b = r.get("t_start") or "", r.get("t_end") or ""
    if "." not in a or "." not in b:
        return None
    try:
        return float(a), float(b)
    except ValueError:
        return None


def overlap(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    return min(a[1], b[1]) - max(a[0], b[0])


def under_load(probes: List[dict], load: List[dict]) -> List[dict]:
    """Probes that demonstrably ran while some load transfer did. Clock
    uncertainty (OVERLAP_MIN, for NTP-level error between hosts and the
    milliseconds the timestamps bracket around curl) is taken off each load
    interval's ends, and a probe counts if it overlaps what remains: a 20 ms
    probe in the middle of a download counts, one at its edge does not. A
    probe whose overlap cannot be shown (untimed, in a gap between waves,
    marginal) is left out, and the measure becomes unmeasured, not idle."""
    spans = [(a + OVERLAP_MIN, b - OVERLAP_MIN) for a, b in (sp for sp in (span(r) for r in load) if sp is not None)
             if b - a > 2 * OVERLAP_MIN]
    out = []
    for r in probes:
        sp = span(r)
        if sp is not None and any(sp[1] > a and sp[0] < b for a, b in spans):
            out.append(r)
    return out


def flow_rows(rows: List[dict]) -> Dict[str, List[dict]]:
    """Bulk rows grouped by flow: b<i>o<k> is flow b<i>'s k-th object (a
    runner that kept the flow backlogged); a bare b<i> is a one-object flow."""
    out: Dict[str, List[dict]] = collections.OrderedDict()
    for r in rows:
        out.setdefault(r["flow"].split("o", 1)[0], []).append(r)
    for v in out.values():
        v.sort(key=lambda r: int(r["flow"].split("o", 1)[1]) if "o" in r["flow"] else 0)
    return out


def flow_ok(rows: List[dict]) -> bool:
    """Every object complete, except that a looped flow's last object may be
    cut by the window (HTTP 200, curl timeout)."""
    if not rows:
        return False
    looped = "o" in rows[0]["flow"]
    *done, last = rows
    if any(r["complete"] != "1" for r in done):
        return False
    return last["complete"] == "1" or (looped and last.get("http") == "200" and last.get("curl_exit") == "28")


def capped_by_client(rows: List[dict], duration: float) -> int:
    """capped_flows per client: flow names repeat across clients (b1o1 on
    every leaf), so they are grouped one client at a time."""
    clients = sorted({r.get("client", "") for r in rows})
    return sum(capped_flows([r for r in rows if r.get("client", "") == c], duration) for c in clients)


def capped_flows(rows: List[dict], duration: float) -> int:
    """Bulk flows that ran out of bytes before the window closed: the last
    object finished (curl exit 0) and the flow was active for clearly less
    than the window. Its bytes are then capped at the objects' size, so a
    fast and a slow arm can deliver the same total. The A/B and DC analyses
    both void a campaign with any capped flow."""
    n = 0
    slack = max(2.0, 0.05 * duration)
    for rs in flow_rows(rows).values():
        last = rs[-1]
        # Only a flow whose last object arrived whole ran out of bytes; an
        # HTTP error or a failed transfer is a failure of the arm, not a cap.
        if last.get("curl_exit") != "0" or last.get("complete") != "1":
            continue
        spans = [span(r) for r in rs]
        active = (spans[-1][1] - spans[0][0]) if all(spans) else sum(float(r["seconds"] or 0) for r in rs)
        if active < duration - slack:
            n += 1
    return n


class Trial:
    def __init__(self, block: int, path: str, slot: int, arm: str, role: str, arm_trial: int, cold: bool,
                 trial: str = ""):
        self.block, self.path, self.slot, self.arm, self.role = block, path, slot, arm, role
        self.arm_trial, self.cold = arm_trial, cold
        # The trial type whose block this trial is in ("" when every trial
        # runs every phase).
        self.trial = trial
        # The gateway's flow completion records in this trial's window.
        self.flow_records: List[dict] = []
        self.start = math.inf
        self.end = -math.inf
        self.rows: List[dict] = []
        self.records: List[dict] = []
        self.m: Dict[str, float] = {}

    @property
    def tag(self) -> str:
        return f"b{self.block:03d}-{self.path}" + (f"-{self.trial}" if self.trial else "")

    @property
    def unit(self) -> Tuple[int, str, str]:
        return self.block, self.path, self.trial

    def bulk(self) -> List[dict]:
        return [r for r in self.rows if r["kind"] == "bulk"]

    def useful_bytes(self) -> int:
        return sum(useful(r) for r in self.bulk())


def read_campaign(campaign: str) -> Tuple[dict, Dict[Tuple[int, str, int], Trial]]:
    with open(os.path.join(campaign, "plan.json")) as f:
        plan = json.load(f)
    trials: Dict[Tuple[int, str, int], Trial] = collections.OrderedDict()
    tp = os.path.join(campaign, "trials.tsv")
    if not os.path.exists(tp):
        # A campaign that ended before its first trial (set-up failed): no
        # trials is missing evidence, which completeness reports.
        return plan, trials
    with open(tp) as f:
        for r in csv.DictReader(f, delimiter="\t"):
            key = (int(r["block"]), r["path"], int(r["slot"]))
            t = trials.get(key)
            if t is None:
                t = trials[key] = Trial(key[0], key[1], key[2], r["arm"], r["role"], int(r["arm_trial"]), r["cold"] == "1",
                                        r.get("trial") or "")
            t.rows.append(r)
            t.start = min(t.start, ts(r["start_utc"]))
            t.end = max(t.end, ts(r["end_utc"]))
    # Generators' raw records (measures.jsonl), each tagged with its phase.
    mp = os.path.join(campaign, "measures.jsonl")
    if os.path.exists(mp):
        with open(mp) as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except ValueError:
                    continue
                t = trials.get((rec.get("_block"), rec.get("_path"), rec.get("_slot")))
                if t is not None:
                    t.records.append(rec)
    return plan, trials


def host_cpu(campaign: str, t: "Trial", host: str, busy_threshold: float = 0.90) -> Optional[dict]:
    """A trial's host CPU over its window, from the runner's samples
    (host_cpu): "mean" busy share, "hot" the fraction of sample intervals at
    or above busy_threshold, "production" production's share of all CPU
    time, "steal" the share stolen by the hypervisor. None when no two
    samples fall in the window."""
    p = os.path.join(campaign, "hostcpu", f"{t.tag}-s{t.slot}-{t.arm}-{host}.tsv")
    try:
        lines = open(p).read().split("\n")
    except OSError:
        return None
    rows = []
    for line in lines:
        f = line.split()
        if len(f) != 12:
            continue
        try:
            when = float(f[0]) / (1000.0 if float(f[0]) > 1e11 else 1.0)
            ticks = [float(x) for x in f[1:9]]
        except ValueError:
            continue
        prod = float(f[10]) if f[10] != "nan" else float("nan")
        rows.append((when, ticks, prod))
    rows = [r for r in rows if t.start - 1 <= r[0] <= t.end + 1]
    if len(rows) < 2:
        return None
    intervals, total, prod_ticks, steal = [], 0.0, 0.0, 0.0
    for (_, a_, pa), (_, b_, pb) in zip(rows, rows[1:]):
        d = [y - x for x, y in zip(a_, b_)]
        tot = sum(d)
        if tot <= 0:
            continue
        intervals.append(1.0 - (d[3] + d[4]) / tot)       # all but idle and iowait
        total += tot
        steal += d[7]
        prod_ticks += pb - pa
    if not intervals:
        return None
    return {"mean": sum(intervals) / len(intervals),
            "hot": sum(1 for x in intervals if x >= busy_threshold) / len(intervals),
            "production": prod_ticks / total if total else float("nan"),
            "steal": steal / total if total else float("nan")}


def contention_void(campaign: str, plan: dict, trials: Dict[Tuple[int, str, int], "Trial"],
                    bounds: Optional[dict]) -> Tuple[Dict[Tuple[int, str, str], str], Dict[Tuple[str, str], dict]]:
    """Units (block, path, trial type) void under contention, with why, and
    each trial's host CPU by (trial key, host). A unit is void when, in any
    of its trials on any loaded host, production took more than its allowed
    share of the host's CPU, the hypervisor stole more than its allowed
    share (a neighbour contending, which hits the arms unpredictably), or
    the CPU record or production's CPU is missing. A host busy with the test
    itself is not contention of production: it is reported (an arm may be
    CPU-bound), not voided."""
    void: Dict[Tuple[int, str, str], str] = {}
    cpu: Dict[Tuple[str, str], dict] = {}
    hosts = (plan.get("hostcpu_hosts") or {})
    if not plan.get("config", {}).get("host_cpu") or not bounds:
        return void, cpu
    for key, t in trials.items():
        if key[2] >= 100:
            continue
        for host in hosts.get(t.path, []):
            c = host_cpu(campaign, t, host, bounds["busy_threshold"])
            cpu[(t.tag + f"-s{t.slot}", host)] = c
            if c is None:
                void.setdefault(t.unit, f"no CPU record of {host} in slot {t.slot}")
                continue
            if not c["production"] == c["production"]:
                void.setdefault(t.unit, f"production CPU on {host} unreadable in slot {t.slot}")
            elif c["production"] > bounds["production_share_max"]:
                void.setdefault(t.unit, f"production took {c['production']:.0%} of {host}'s CPU in slot {t.slot}")
            if c["steal"] > bounds["steal_max_fraction"]:
                void.setdefault(t.unit, f"{c['steal']:.0%} of {host}'s CPU stolen by the hypervisor in slot {t.slot}")
    return void, cpu


def fetch_failed(campaign: str) -> List[str]:
    """Remote log paths the runner could not fetch (status.json)."""
    try:
        with open(os.path.join(campaign, "status.json")) as f:
            return list(json.load(f).get("fetch_failures") or [])
    except (OSError, ValueError):
        return []


# Section 15.4 secondaries with no measurement in any build yet.
NOT_YET_MEASURED = ("credit_delay_p95_s", "false_promotion_rate")
LOG_MEASURES = ("queue_p90_ms", "reissued_per_source", "transit_p50_s", "transit_p90_s", "cpu_p90")


# The runner's exits that are the control plane, not the arm: the host
# unreachable through every ssh retry, and the runner's own kill when it tore
# down (ab_campaign.CONTROL_PLANE). A job past its window, lost or silent on a
# reachable host is the arm's failure, recorded in its rows, never void.
CONTROL_PLANE_EXITS = (255, -4)


def control_plane(job_failures: Optional[Sequence[dict]]) -> List[dict]:
    return [j for j in job_failures or [] if j.get("exit") in CONTROL_PLANE_EXITS]


def completeness(campaign: str, plan: dict, trials: Dict[Tuple[int, str, int], Trial],
                 interim: bool = False, units_run: Optional[Sequence[Sequence[Any]]] = None,
                 failures: Optional[dict] = None, control_plane_max: Optional[int] = None,
                 voids: Optional[Dict[Tuple[int, str, str], str]] = None
                 ) -> Tuple[List[str], Dict[Tuple[int, str, int], Trial]]:
    """Problems that forbid a verdict, and the trials of complete blocks only.

    A verdict needs a campaign that finished (status.json from the runner)
    with production unchanged, and every planned slot present. A block with
    a missing slot is dropped, so an interrupted block cannot pair two
    candidate trials with one main trial; the problems list says so and the
    verdict becomes INCOMPLETE.
    """
    problems: List[str] = []
    failed_jobs: List[dict] = []
    last_block = None
    # The (block, path, trial type) units that ran: a type stopped by the
    # sequential rule planned no more blocks, and at a look the units run so
    # far are the plan.
    ran: Optional[set] = {tuple(u) for u in units_run} if units_run is not None else None
    try:
        if interim:
            # A look during the campaign: no status yet; the blocks run so
            # far are the plan.
            raise FileNotFoundError
        with open(os.path.join(campaign, "status.json")) as f:
            status = json.load(f)
    except (OSError, ValueError):
        if interim:
            last_block = max((b for b, _, s_ in trials if s_ < 100), default=0)
            # The runner's failures so far: a look applies the same evidence
            # rules as the final analysis.
            failed_jobs = control_plane((failures or {}).get("job_failures"))
            if (failures or {}).get("fetch_failures"):
                problems.append(f"{len(failures['fetch_failures'])} log file(s) could not be fetched")
        else:
            problems.append("no status.json: the campaign did not finish")
    else:
        if plan.get("sequential") and status.get("units_run") is not None:
            ran = {tuple(u) for u in status["units_run"]}
        elif plan.get("sequential") and status.get("blocks_run"):
            # A sequential campaign that stopped early planned no more.
            last_block = int(status["blocks_run"])
        if status.get("status") != "complete":
            problems.append(f"the campaign {status.get('status')} (exit {status.get('exit')})")
        if status.get("production") != "unchanged":
            problems.append("production changed during the campaign")
        if status.get("fetch_failures"):
            problems.append(f"{len(status['fetch_failures'])} log file(s) could not be fetched; the log-derived "
                            "measures of the affected trials are withheld")
        failed_jobs = control_plane(status.get("job_failures"))
    planned: Dict[Tuple[int, str, str], set] = collections.defaultdict(set)
    for sl in plan.get("schedule", []):
        u = (sl["block"], sl["path"], sl.get("trial") or "")
        if (last_block is None or sl["block"] <= last_block) and (ran is None or u in ran):
            planned[u].add(sl["slot"])
    have: Dict[Tuple[int, str, str], set] = collections.defaultdict(set)
    for (b, p, slot), t in trials.items():
        if slot < 100:
            have[t.unit].add(slot)
    bad = sorted(k for k, slots in planned.items() if not slots <= have.get(k, set()))
    # A block in which the runner lost a job to the control plane has harness
    # evidence, not the arm's: left out like a missing trial.
    broken = sorted({(j["block"], j["path"], j.get("trial") or "") for j in failed_jobs})
    if broken and control_plane_max is not None and voids is not None:
        # A control-plane failure (the runner lost a job: its host unreachable
        # through every retry, or the runner killing it tearing down) voids
        # its whole block, both arms: not evidence, though it
        # counted toward the cap. Up to control_plane_max per trial type;
        # more, and the type has no verdict.
        per: Dict[Tuple[str, str], List[Tuple[int, str, str]]] = collections.defaultdict(list)
        for u in broken:
            per[(u[1], u[2])].append(u)
            why = next(j for j in failed_jobs if (j["block"], j["path"], j.get("trial") or "") == u)
            voids.setdefault(u, f"control plane: job {why.get('kind')} on "
                                f"{why.get('client')} (slot {why.get('slot')}) exited {why.get('exit')}")
        for (p_, ty), us in sorted(per.items()):
            if len(us) > control_plane_max:
                problems.append(f"{len(us)} block(s) of {ty or 'the workload'} on {p_} lost a job to the control plane, "
                                f"more than the {control_plane_max} allowed")
        bad = sorted(set(bad) - set(broken))
    elif broken:
        problems.append(f"{len(broken)} block(s) had a job that did not run to its end "
                        f"(first: block {broken[0][0]} on {broken[0][1]}); they are left out")
        bad = sorted(set(bad) | set(broken))
    if bad:
        problems.append(f"{len(bad)} of {len(planned)} planned blocks are missing trials "
                        f"(first: block {bad[0][0]} on {bad[0][1]}); they are left out")
    gone = set(bad) | (set(broken) if voids is not None and control_plane_max is not None else set())
    keep = {k: t for k, t in trials.items() if t.unit not in gone and (k[2] >= 100 or t.unit in planned or not planned)}
    return problems, keep


# --- per-trial measures ---------------------------------------------------------

def server_logs(campaign: str, arm: str, tag: str) -> List[str]:
    return sorted(glob.glob(os.path.join(campaign, "logs", f"gw-{arm}", f"server-{tag}.jsonl*")))


def client_logs(campaign: str, client: str, arm: str, tag: str, slot: Optional[int] = None) -> List[str]:
    pat = f"client-{tag}-s{slot}.jsonl*" if slot is not None else f"client-{tag}.jsonl*"
    files = sorted(glob.glob(os.path.join(campaign, "logs", f"cl-{client}-{arm}", pat)))
    if not files and slot is not None:
        files = sorted(glob.glob(os.path.join(campaign, "logs", f"cl-{client}-{arm}", f"client-{tag}.jsonl*")))
    return files


def in_window(rec: dict, t: Trial, before: float = 1.0, after: float = 2.0) -> Optional[float]:
    try:
        x = ts(rec["time"])
    except (KeyError, ValueError):
        return None
    return x if t.start - before <= x <= t.end + after else None


def queue_and_reissue(t: Trial, recs: List[dict]) -> None:
    seen = set()
    qd: List[float] = []
    reiss: Dict[object, float] = {}
    source: Dict[object, float] = {}
    for r in recs:
        if r.get("type") != "lane_metrics":
            continue
        x = in_window(r, t)
        if x is None:
            continue
        # One QUIC connection is traced once per flow riding it: dedupe by its
        # own counters before taking a queue-delay sample.
        ck = (round(x, 1), r.get("pktsent"), r.get("sent"))
        if ck not in seen and r.get("minrtt", 0) > 0 and r.get("srtt", 0) > 0 and r.get("inflight", 0) > 0:
            seen.add(ck)
            qd.append(r["srtt"] - r["minrtt"])
        fid = r.get("flow_id")
        reiss[fid] = max(reiss.get(fid, 0.0), float(r.get("breissued", 0) or 0))
        source[fid] = max(source.get(fid, 0.0), float(r.get("source", 0) or 0))
    t.m["queue_p90_ms"] = st.quantile(qd, 0.9)
    s = sum(source.values())
    t.m["reissued_per_source"] = sum(reiss.values()) / s if s else float("nan")


def transit_exact(sent: List[dict], recv: List[dict]) -> List[float]:
    """Seq-matched flow-ACK transit (client write -> gateway receipt), each
    flow's 2nd percentile removed as its clock-offset-plus-propagation base."""
    byseq: Dict[Tuple[object, object], List[float]] = collections.defaultdict(list)
    for r in sorted(recv, key=lambda r: r["_t"]):
        byseq[(r.get("flow_id"), r.get("seq"))].append(r["_t"])
    perflow: Dict[object, List[float]] = collections.defaultdict(list)
    for s in sorted(sent, key=lambda r: r["_t"]):
        k = (s.get("flow_id"), s.get("seq"))
        if byseq.get(k):
            perflow[s.get("flow_id")].append(byseq[k].pop(0) - s["_t"])
    out: List[float] = []
    for lat in perflow.values():
        if len(lat) < 5:
            continue
        base = st.quantile(lat, 0.02)
        out += [x - base for x in lat]
    return out


def transit_counted(client: List[dict], server: List[dict]) -> List[float]:
    """Count-matched transit from lane-trace acksout/acksin: the n-th ACK the
    client wrote is taken to be the n-th the gateway read on that flow."""
    cl: Dict[object, List[float]] = collections.defaultdict(list)
    sv: Dict[object, List[float]] = collections.defaultdict(list)
    for r in sorted(client, key=lambda r: r["_t"]):
        cl[r.get("flow_id")] += [r["_t"]] * int(r.get("acksout", 0) or 0)
    for r in sorted(server, key=lambda r: r["_t"]):
        sv[r.get("flow_id")] += [r["_t"]] * int(r.get("acksin", 0) or 0)
    out: List[float] = []
    for fid, c in cl.items():
        s = sv.get(fid, [])
        n = min(len(c), len(s))
        if n < 20:
            continue
        lat = [s[i] - c[i] for i in range(n)]
        base = st.quantile(lat, 0.02)
        out += [x - base for x in lat]
    return out


def cpu_p90(files: List[str], t: Trial) -> float:
    fr: List[float] = []
    for p in files:
        prev = None
        try:
            with open(p) as f:
                for line in f:
                    parts = line.split()
                    if len(parts) != 2:
                        continue
                    x, ticks = float(parts[0]), float(parts[1])
                    if prev and t.start - 1 <= x <= t.end + 1 and x > prev[0]:
                        fr.append((ticks - prev[1]) / CLK_TCK / (x - prev[0]))
                    prev = (x, ticks)
        except OSError:
            continue
    return st.quantile(fr, 0.9)


# Effective configuration per arm and endpoint, from each process's
# "runtime configuration" record (design section 14: every test arm publishes
# its effective configuration per endpoint).
EFFECTIVE: Dict[Tuple[str, str], set] = {}


def effective(r: dict) -> str:
    keys = ("bundle", "congestion", "cc_api", "sched", "mem", "coded_retry", "path_profile", "transport", "wire_versions")
    return " ".join(f"{k}={r[k]}" for k in keys if k in r and r[k] not in ("", None))


def measure(campaign: str, plan: dict, trials: Dict[Tuple[int, str, int], Trial]) -> str:
    """Fill each trial's measures; returns which transit method was used."""
    method = set()
    failed = fetch_failed(campaign)
    cache: Dict[Tuple[str, str], List[dict]] = {}
    current = None
    for t in trials.values():
        # Trials are in block order: a block's gateway records are never
        # needed again once the next block starts.
        if t.tag != current:
            cache.clear()
            current = t.tag
        pre, post = metrics(campaign, f"{t.tag}-s{t.slot}-{t.arm}-pre"), metrics(campaign, f"{t.tag}-s{t.slot}-{t.arm}-post")
        d = lambda k: post.get(k, float("nan")) - pre.get(k, float("nan"))
        bulk = t.bulk()
        useful = t.useful_bytes()
        wall = max((float(r["wall"]) for r in bulk), default=float("nan"))
        t.m["useful_bytes"] = useful
        t.m["goodput_mbps"] = useful * 8 / wall / 1e6 if wall and wall == wall and wall > 0 else float("nan")
        flows = flow_rows(bulk)
        t.m["flows"] = len(flows)
        t.m["ok"] = sum(1 for rs in flows.values() if flow_ok(rs))
        t.m["completion"] = t.m["ok"] / len(flows) if flows else float("nan")
        t.m["capped"] = capped_by_client(bulk, float(plan.get("config", {}).get("duration") or 0))
        sent = d("queqiao_quic_packets_sent")
        t.m["loss"] = d("queqiao_quic_loss_observed_packets_total") / sent if sent and sent == sent else float("nan")
        t.m["stalls_per_trial"] = d("queqiao_flow_stalls_detected_total")
        wire = d("queqiao_quic_bytes_sent")
        # TCP fallback is counted on the client, not the gateway.
        cpre = [metrics(campaign, f"{t.tag}-s{t.slot}-{t.arm}-pre-client-{c}") for c in [t.path]]
        cpost = [metrics(campaign, f"{t.tag}-s{t.slot}-{t.arm}-post-client-{c}") for c in [t.path]]
        t.m["tcp_fallbacks"] = sum(b.get("queqiao_fallbacks_total", float("nan")) - a.get("queqiao_fallbacks_total", float("nan"))
                                   for a, b in zip(cpre, cpost))
        # QUIC bytes only: a trial that moved data over TCP fallback has no
        # comparable wire count, so it is not measured rather than flattered.
        t.m["wire_per_useful"] = (wire / useful if useful and t.m["tcp_fallbacks"] == 0 else float("nan"))
        # A probe that did not finish counts at its timeout: dropping it
        # would let an arm improve its p99 by failing its slowest requests.
        probes = [float(r["seconds"]) if r["complete"] == "1" else PROBE_TIMEOUT
                  for r in under_load([r for r in t.rows if r["kind"] == "probe"], bulk)]
        t.m["interactive_p99_s"] = st.quantile(probes, 0.99) if probes else float("nan")

        key = (t.arm, t.tag)
        if key not in cache:
            recs = list(iter_json(server_logs(campaign, t.arm, t.tag),
                                  ('"lane_metrics"', '"flow ack received"', '"runtime configuration"',
                                   '"remote flow complete"', '"remote flow ended with error"')))
            for r in recs:
                if r.get("msg") == "runtime configuration":
                    EFFECTIVE.setdefault((t.arm, "gateway"), set()).add(effective(r))
            for r in recs:
                try:
                    r["_t"] = ts(r["time"])
                except (KeyError, ValueError):
                    r["_t"] = float("nan")
            cache[key] = recs
        srecs = cache[key]
        queue_and_reissue(t, srecs)

        clients = [t.path] if t.path != "dc" else []
        ex: List[float] = []
        cpus: List[str] = []
        for c in clients:
            small = plan.get("clients", {}).get(c, {}).get("small", False)
            files = client_logs(campaign, c, t.arm, t.tag, t.slot if small else None)
            crecs = list(iter_json(files, ('"lane_metrics"', '"flow ack sent"', '"runtime configuration"')))
            for r in crecs:
                if r.get("msg") == "runtime configuration":
                    EFFECTIVE.setdefault((t.arm, "client"), set()).add(effective(r))
                    t.m["client_transport"] = r.get("transport", "")
            for r in crecs:
                try:
                    r["_t"] = ts(r["time"])
                except (KeyError, ValueError):
                    r["_t"] = float("nan")
            crecs = [r for r in crecs if t.start - 1 <= r["_t"] <= t.end + 2]
            sent_ev = [r for r in crecs if r.get("msg") == "flow ack sent"]
            sw = [r for r in srecs if t.start - 1 <= r["_t"] <= t.end + 2]
            recv_ev = [r for r in sw if r.get("msg") == "flow ack received"]
            if sent_ev and recv_ev:
                ex += transit_exact(sent_ev, recv_ev)
                method.add("seq-matched")
            else:
                ex += transit_counted([r for r in crecs if r.get("type") == "lane_metrics"],
                                      [r for r in sw if r.get("type") == "lane_metrics"])
                method.add("count-matched")
            base = os.path.join(campaign, "logs", f"cl-{c}-{t.arm}")
            cpus += glob.glob(os.path.join(base, f"client-{t.tag}-s{t.slot}.cpu" if small else f"client-{t.tag}.cpu"))
        # Gateway QUIC bytes say nothing about a client that dialled TCP
        # directly (--transport tcp increments no fallback counter).
        # QUIC-only wire cost needs evidence that the trial was QUIC-only:
        # the gateway's flow records, none showing a TCP carrier or lane.
        flows_rec = [r for r in srecs if r.get("msg") in ("remote flow complete", "remote flow ended with error")
                     and t.start - 1 <= r["_t"] <= t.end + 5]
        t.flow_records = flows_rec
        t.m["tcp_flows"] = float(sum(1 for r in flows_rec if tcp_evidence(r)))
        if t.m.get("client_transport") not in ("quic", "auto") or not flows_rec or t.m["tcp_flows"]:
            t.m["wire_per_useful"] = float("nan")
        t.m["_transit"] = ex
        t.m["transit_p50_s"] = st.quantile(ex, 0.5)
        t.m["transit_p90_s"] = st.quantile(ex, 0.9)
        t.m["cpu_p90"] = cpu_p90(cpus, t)
        # A trial whose logs were not all fetched has partial telemetry: a
        # finite number from a subset is not a measurement of the trial.
        if any(f"-{t.tag}" in x and (f"/gw-{t.arm}/" in x or f"-{t.arm}/" in x) for x in failed):
            for k in LOG_MEASURES:
                t.m[k] = float("nan")
            t.m["_transit"] = []
    return "/".join(sorted(method)) or "none"


# --- per-block view -------------------------------------------------------------

def blocks(trials: Dict[Tuple[int, str, int], Trial], path: str, arm: str) -> Dict[int, List[Trial]]:
    out: Dict[int, List[Trial]] = collections.defaultdict(list)
    for t in trials.values():
        if t.path == path and t.arm == arm:
            out[t.block].append(t)
    return out


def block_outcome(ts_: List[Trial]) -> float:
    """Useful bytes delivered within the fixed duration, summed over the arm's
    trials in the block and all their flows (section 15.3)."""
    return float(sum(t.m["useful_bytes"] for t in ts_))


def block_metric(ts_: List[Trial], key: str) -> float:
    """The arm's mean over its trials in the block; NaN unless every trial
    has the measure, so a block cannot be judged on half its evidence."""
    vals = [t.m.get(key, float("nan")) for t in ts_]
    if not vals or any(v is None or v != v for v in vals):
        return float("nan")
    return sum(vals) / len(vals)


def paired(trials, path: str, main: str, cand: str, f) -> Tuple[List[float], List[float], List[int]]:
    mb, cb = blocks(trials, path, main), blocks(trials, path, cand)
    common = sorted(set(mb) & set(cb))
    return [f(cb[b]) for b in common], [f(mb[b]) for b in common], common


# --- reporting --------------------------------------------------------------------

def fmt(x: float, spec: str = ".3g") -> str:
    return "n/a" if x is None or x != x else format(x, spec)


SECONDARY_LABELS = {
    "completion": "flow completion",
    "stalls_per_trial": "stalls / trial",
    "reissued_per_source": "reissued / source bytes",
    "wire_per_useful": "wire bytes / useful byte",
    "queue_p90_ms": "queue p90, ms",
    "interactive_p99_s": "interactive p99 under bulk, s",
    "cpu_p90": "client CPU p90, cores",
    "credit_delay_p95_s": "sender-side credit delay p95, s",
    "false_promotion_rate": "shadow false-promotion rate",
}


def sequential_of(plan: dict) -> "Optional[st.Sequential]":
    s = plan.get("sequential")
    return st.Sequential(**s) if s else None


def decision_verdict(dec: str, interim: bool) -> str:
    return {"pass": "pass", "fail": "**FAIL**"}.get(dec, "undecided" if interim else "**FAIL**")


def report(campaign: str, gates: dict, interim: bool = False,
           units_run: Optional[Sequence[Sequence[Any]]] = None, failures: Optional[dict] = None) -> Tuple[str, dict]:
    plan, trials = read_campaign(campaign)
    if plan.get("workload") == "dc":
        raise SystemExit("this is a DC gate campaign: evaluate it with scripts/dc_gate.py")
    EFFECTIVE.clear()
    sq = sequential_of(plan)
    cp_voids: Dict[Tuple[int, str, str], str] = {}
    all_rows = dict(trials)
    problems, trials = completeness(campaign, plan, trials, interim=interim, units_run=units_run, failures=failures,
                                    control_plane_max=(gates.get("harness") or {}).get("control_plane_void_max"),
                                    voids=cp_voids)
    # Contention: units void under host CPU contention are not evidence (they
    # still counted toward the cap when they ran).
    void_units, cpu_of = contention_void(campaign, plan, trials, gates.get("contention"))
    all_trials = dict(trials)
    for u, why in cp_voids.items():
        void_units.setdefault(u, why)
    if void_units:
        trials = {k: t for k, t in trials.items() if t.unit not in void_units}
    frozen = plan.get("gates_sha256")
    if frozen and gates.get("sha256") and frozen != gates["sha256"]:
        # The rules (and the stopping looks) the campaign ran under are not
        # the ones given now: no verdict under changed gates.
        problems.append("gates.ini differs from the one frozen in the campaign's plan (sha256 "
                        f"{frozen[:12]}); judge it with that file")
    # A sequential campaign that stopped early (on another gate's verdict)
    # leaves its undecided gates undecided, not failed.
    stopped_early = False
    try:
        with open(os.path.join(campaign, "status.json")) as f:
            stopped_early = bool(json.load(f).get("stopped"))
    except (OSError, ValueError):
        pass
    undecided_ok = interim or stopped_early
    method = measure(campaign, plan, trials)
    wl = ab_workloads.get(plan.get("workload", "bulk"))
    for t in trials.values():
        t.m.update(wl.extract(t, plan))
    rules = dict(gates.get("workloads", {}).get(wl.name, {}))
    # The generic secondaries that apply to a rule-judged workload's phases,
    # as rules of those phases with their [secondary] bounds.
    for ph, ks in getattr(wl, "generic", {}).items():
        for k in ks:
            if k in gates["secondary"]:
                better, margin = gates["secondary"][k]
                rules.setdefault(f"{ph}.{k}".lower(), (better, (margin,)))
    # The arm's per-block measures, as the workload defines them (pooled or
    # averaged), for the rules.
    block_cache: Dict[Tuple[str, int, str], Dict[str, float]] = {}

    def block_measures(path: str, block: int, arm: str) -> Dict[str, float]:
        k = (path, block, arm)
        if k not in block_cache:
            ts_ = [t for t in trials.values() if t.path == path and t.block == block and t.arm == arm]
            block_cache[k] = wl.block_extract(ts_, plan) if ts_ else {}
        return block_cache[k]
    def ran(path: str, block: int, arm: str, key: str) -> bool:
        """Did the arm run the phase of measure `key` in this block?"""
        ph = key.split(".", 1)[0] if "." in key else None
        for t in trials.values():
            if t.path == path and t.block == block and t.arm == arm:
                if ph is None or t.trial == ph or any(r["phase"] == ph for r in t.rows):
                    return True
        return False
    tail_cache: Dict[Tuple[str, int, str], Dict[str, Tuple[float, List[float]]]] = {}

    def block_tails(path: str, block: int, arm: str) -> Dict[str, Tuple[float, List[float]]]:
        k = (path, block, arm)
        if k not in tail_cache:
            ts_ = [t for t in trials.values() if t.path == path and t.block == block and t.arm == arm]
            tail_cache[k] = wl.block_tails(ts_, plan) if ts_ else {}
        return tail_cache[k]
    main = plan["main"]
    arms = [main] + [a for a in plan["arms"] if a != main]
    paths = plan["paths"]
    # The interactive probe runs only in the interactive workload; elsewhere
    # that secondary is not applicable rather than unmeasured.
    # The [secondary] bounds go with the byte outcome; a workload without one
    # is judged by its own rules only.
    secondary = {k: v for k, v in gates["secondary"].items()
                 if wl.primary and not (k == "interactive_p99_s" and plan.get("workload") != "interactive")}
    out: List[str] = []
    verdicts: dict = {}
    def sha(a: str) -> str:
        """What the arm ran: its resolved endpoint commits, both when they differ."""
        e = plan["arms"][a]
        g = e.get("gateway_commit") or e.get("gateway_ref") or e.get("commit") or e.get("ref") or ""
        c = e.get("client_commit") or e.get("client_ref") or e.get("commit") or e.get("ref") or ""
        if g != c:
            return f"gateway {g[:12]}, client {c[:12]}"
        return g[:12]
    out.append(f"## Live A/B: {os.path.basename(os.path.normpath(campaign))}")
    out.append("")
    out.append("Arms: " + "; ".join(f"`{a}` ({plan['arms'][a]['role']}, {sha(a) or 'binary'}"
                                     f"{', ' + ' '.join(plan['arms'][a]['flags']) if plan['arms'][a]['flags'] else ''})"
                                     for a in arms))
    out.append(f"Gate: per block `candidate - {gates['ratio']:.2f} x main` in useful bytes; pass when the one-sided "
               f"{1 - gates['alpha']:.0%} t and bootstrap ({gates['resamples']} resamples) lower bounds are both above zero "
               f"(DESIGN-PATH-AUTHORITY.md section 15.4). Flow-ACK transit: {method}.")
    if problems:
        out += ["", "**No verdict: the campaign is incomplete.** " + "; ".join(problems) + ". "
                "The numbers below are for diagnosis only."]
    if void_units:
        out += ["", f"**{len(void_units)} block(s) void** (under contention or lost to the control plane: not "
                "evidence; they counted toward the cap): " + "; ".join(f"block {b} on {p}{' ' + ty if ty else ''}: {why}"
                                                                     for (b, p, ty), why in sorted(void_units.items()))]
    # Workload failures, per block and arm: transfers the job itself recorded
    # as failed (curl exit other than 0, and 28 at a bulk window's end; a
    # generator's failed session). They are the arm's evidence, never voided.
    wf: Dict[Tuple[int, str, str], Dict[str, int]] = collections.defaultdict(lambda: collections.Counter())
    for t in all_rows.values():
        for r in t.rows:
            ex = str(r.get("curl_exit") or "")
            if ex not in ("", "0") and not (ex == "28" and r.get("kind") in ("bulk", "wl-bgdown", "wl-bgup")):
                wf[t.unit][t.arm] += 1
    try:
        with open(os.path.join(campaign, "status.json")) as f:
            jobs_failed = json.load(f).get("workload_failures") or []
    except (OSError, ValueError):
        jobs_failed = []
    if jobs_failed:
        # Jobs the runner saw fail on a reachable host (past their window,
        # lost, silent): their rows and sessions count as the arm's failures.
        out += ["", "Failed jobs (the arm's evidence, not void): " + "; ".join(
            f"block {j['block']} on {j['path']}{' ' + j['trial'] if j.get('trial') else ''}: {j['kind']} on "
            f"{j['client']} ({j['arm']}) {j['reason']}" for j in jobs_failed)]
    if wf:
        out += ["", "Workload failures (recorded by the jobs; evidence against the arm): " + "; ".join(
            f"block {b} on {p}{' ' + ty if ty else ''}: " + ", ".join(f"{arm} {n}" for arm, n in sorted(c.items()))
            for (b, p, ty), c in sorted(wf.items()))]
    if EFFECTIVE:
        out.append("")
        out.append("Effective configuration, from each endpoint's runtime configuration record:")
        for a in arms:
            for end in ("gateway", "client"):
                vals = sorted(EFFECTIVE.get((a, end), set()))
                if vals:
                    warn = " (**differs between processes**)" if len(vals) > 1 else ""
                    out.append(f"- `{a}` {end}: `{'` / `'.join(vals)}`{warn}")
    for path in paths:
        pts = {a: [t for t in trials.values() if t.path == path and t.arm == a] for a in arms}
        if not any(pts.values()):
            # No trial on this path at all: no verdict, not a missing row.
            for cand in arms[1:]:
                verdicts[(path, cand, "primary")] = "INCOMPLETE"
                verdicts[(path, cand, "overall")] = "INCOMPLETE"
            out += ["", f"### {path}: no trials, so no verdict"]
            continue
        nb = len({t.block for t in trials.values() if t.path == path})
        out += ["", f"### {path}: {nb} blocks", "", "| metric | " + " | ".join(arms) + " |", "|---|" + "---:|" * len(arms)]
        med = lambda a, k: st.median([t.m.get(k, float("nan")) for t in pts[a]])
        row = lambda name, cells: out.append(f"| {name} | " + " | ".join(cells) + " |")
        row("useful MB per block (mean)", [fmt(st.mean([block_outcome(v) for v in blocks(trials, path, a).values()]) / 1e6, ".1f") for a in arms])
        row("goodput, Mbit/s (median trial)", [fmt(med(a, "goodput_mbps"), ".1f") for a in arms])
        row("completion (flows ok / total)", [f"{sum(t.m['ok'] for t in pts[a])}/{sum(t.m['flows'] for t in pts[a])}" for a in arms])
        row("server loss (median)", [fmt(med(a, "loss"), ".3f") for a in arms])
        row("queue p90 srtt-minRTT, ms (median)", [fmt(med(a, "queue_p90_ms"), ".0f") for a in arms])
        row("stalls / trial (median)", [fmt(med(a, "stalls_per_trial"), ".1f") for a in arms])
        row("gateway QUIC wire bytes / useful byte (median)", [fmt(med(a, "wire_per_useful"), ".2f") for a in arms])
        row("TCP fallbacks (total)", [fmt(sum(t.m.get("tcp_fallbacks", 0) for t in pts[a] if t.m.get("tcp_fallbacks") == t.m.get("tcp_fallbacks")), ".0f") for a in arms])
        row("reissued / source bytes (median)", [fmt(med(a, "reissued_per_source"), ".1%") for a in arms])
        pooled = {a: [x for t in pts[a] for x in t.m.get("_transit", [])] for a in arms}
        row("flow-ACK transit p50 / p90, s (pooled)", [f"{fmt(st.quantile(pooled[a], .5), '.3f')} / {fmt(st.quantile(pooled[a], .9), '.3f')}" for a in arms])
        row("client CPU p90, cores (median)", [fmt(med(a, "cpu_p90"), ".2f") for a in arms])
        # Host CPU per arm over all its trials, void ones included, so an
        # arm that burns more CPU shows.
        cpu_bound: List[str] = []
        for host in (plan.get("hostcpu_hosts") or {}).get(path, []):
            def host_stat(arm_, k, host=host):
                xs = [cpu_of.get((t.tag + f"-s{t.slot}", host)) for t in all_trials.values()
                      if t.path == path and t.arm == arm_]
                xs = [x[k] for x in xs if x and x[k] == x[k]]
                return st.mean(xs)
            label = "gateway" if host == plan.get("gateway_host") else "client"
            thr = (gates.get("contention") or {}).get("busy_threshold", 0.90)
            row(f"{label} host CPU busy, mean", [fmt(host_stat(a, "mean"), ".0%") for a in arms])
            row(f"{label} host share of the window at or above {thr:.0%} CPU, mean",
                [fmt(host_stat(a, "hot"), ".0%") for a in arms])
            row(f"{label} host production CPU share, mean", [fmt(host_stat(a, "production"), ".1%") for a in arms])
            row(f"{label} host CPU stolen, mean", [fmt(host_stat(a, "steal"), ".1%") for a in arms])
            for a_ in arms:
                ts_ = [cpu_of.get((t.tag + f"-s{t.slot}", host)) for t in all_trials.values()
                       if t.path == path and t.arm == a_]
                ts_ = [x for x in ts_ if x]
                bound = sum(1 for x in ts_ if x["hot"] > 0.5)
                if ts_ and bound > len(ts_) / 2:
                    cpu_bound.append(f"`{a_}` on the {label} host ({bound} of {len(ts_)} trials at or above "
                                     f"{thr:.0%} CPU for most of their window)")
        if any(t.m.get("interactive_p99_s") == t.m.get("interactive_p99_s") for a in arms for t in pts[a]):
            row("interactive p99, s (median)", [fmt(med(a, "interactive_p99_s"), ".3f") for a in arms])
        if cpu_bound:
            out += ["", "**CPU-bound:** " + "; ".join(cpu_bound) + ". Its bulk numbers may be limited by the "
                    "host's CPU rather than by the network."]

        judged: Dict[str, int] = {}
        if wl.primary:
            out += ["", f"**Primary gate (section 15.4), {path}**", "",
                    "| candidate | blocks | ratio of means | mean contrast, MB | t lower bound, MB | bootstrap lower bound, MB | main zero blocks | verdict |",
                    "|---|---:|---:|---:|---:|---:|---:|---|"]
        for cand in arms[1:] if not wl.primary else ():
            _, _, common = paired(trials, path, main, cand, block_outcome)
            judged[cand] = len(common)
            verdicts[(path, cand, "primary")] = "INCOMPLETE" if problems else "n/a"
        for cand in arms[1:] if wl.primary else ():
            c, m, common = paired(trials, path, main, cand, block_outcome)
            g = st.gate(c, m, ratio=gates["ratio"], alpha=gates["alpha"], resamples=gates["resamples"],
                        seed=gates["seed"], max_zero_main_fraction=gates["max_zero_main_fraction"])
            verdict = "VOID" if g.void else ("**PASS**" if g.passed else "**FAIL**")
            if sq and not g.void:
                # Sequential: the look's nominal level (alpha spending) decides.
                d_ = st.contrasts(c, m, gates["ratio"])
                dec = sq.decide(d_, gates["resamples"], gates["seed"])
                verdict = {"pass": "**PASS**", "fail": "**FAIL**"}.get(dec, "undecided" if undecided_ok else "**FAIL**")
                g.passed = verdict == "**PASS**"
                # Report the bounds of the look that decided, at its alpha.
                a_ = sq.alpha_at(len(d_))
                g.t_lower = st.t_lower_bound(d_, a_)
                g.boot_lower = st.bootstrap_lower_bound(d_, a_, gates["resamples"], gates["seed"])
                g.reason = f"sequential look at {len(d_)} blocks, alpha {a_:g}" if not g.passed else "pass"
            capped = sum(int(t.m.get("capped", 0)) for t in trials.values()
                         if t.path == path and t.arm in (main, cand) and t.block in set(common))
            if capped:
                # Byte-capped outcomes make a fast and a slow arm look alike.
                verdict = "VOID"
                g.reason = f"void: {capped} bulk flow(s) ran out of bytes before the window closed; raise object_bytes"
                g.passed = False
            if problems:
                # No verdict from an incomplete campaign, whatever the numbers say.
                verdict = "INCOMPLETE"
            judged[cand] = g.n
            verdicts[(path, cand, "primary")] = verdict
            out.append(f"| {cand} | {g.n} | {fmt(g.ratio_of_means, '.3f')} | {fmt(g.mean_contrast / 1e6, '.2f')} | "
                       f"{fmt(g.t_lower / 1e6, '.2f')} | {fmt(g.boot_lower / 1e6, '.2f')} | {fmt(g.zero_main_fraction, '.0%')} | "
                       f"{verdict}{'' if g.passed else ' (' + g.reason + ')'} |")
        if secondary:
            out += ["", f"**Secondary bounds (pre-registered in gates.ini), {path}**", "",
                    "| metric | candidate | blocks | mean (cand - main) | one-sided bound | margin | verdict |",
                    "|---|---|---:|---:|---:|---:|---|"]
            for key, (better, margin) in secondary.items():
                for cand in arms[1:]:
                    c, m, common = paired(trials, path, main, cand, lambda ts_, k=key: block_metric(ts_, k))
                    upper = better == "lower"
                    mdiff, bound, n = st.paired_secondary(c, m, alpha=gates["alpha"], resamples=gates["resamples"],
                                                          seed=gates["seed"], upper=upper)
                    need = math.ceil(gates["secondary_min_coverage"] * judged.get(cand, 0) - 1e-9)
                    if bound != bound or n < max(need, 2):
                        verdict = "n/a"
                    elif upper:
                        verdict = "pass" if bound <= margin else "**FAIL**"
                    else:
                        verdict = "pass" if bound >= -margin else "**FAIL**"
                    verdicts[(path, cand, key)] = verdict
                    out.append(f"| {SECONDARY_LABELS.get(key, key)} | {cand} | {n} | {fmt(mdiff, '.4g')} | "
                               f"{'upper' if upper else 'lower'} {fmt(bound, '.4g')} | {'+' if upper else '-'}{margin:g} | {verdict} |")
        # Expand the rules against the measures this path reported: "*.m"
        # applies to measure m of every phase. A reported measure without a
        # rule (and not report-only) leaves the workload INCOMPLETE.
        blocks_here = sorted({t.block for t in trials.values() if t.path == path})
        keys = sorted({k for b in blocks_here for a_ in arms for k in block_measures(path, b, a_)})
        judged_rules: Dict[str, Tuple[str, Tuple[float, ...]]] = {}
        unruled: List[str] = []
        for k in keys:
            metric = k.split(".", 1)[-1]
            # gates.ini keys are lowercased by configparser; match likewise.
            rule = rules.get(k.lower()) or rules.get(f"*.{metric.lower()}")
            if rule:
                judged_rules[k] = rule
            elif metric not in wl.report_only:
                unruled.append(k)
        # And every configured rule must find its measure: in each phase that
        # ran on this path for a wildcard, by name otherwise. A measure the
        # extractor never produced is unmeasured, not absent from the verdict.
        phases_here = sorted({r["phase"] for t in trials.values() if t.path == path for r in t.rows})
        for rk, rule in rules.items():
            wanted = [f"{ph}.{rk[2:]}" for ph in phases_here] if rk.startswith("*.") else [rk]
            have = {k.lower() for k in judged_rules}
            for k in wanted:
                if k.lower() not in have and (rk.startswith("*.") or k.split(".", 1)[0] in phases_here):
                    judged_rules[k] = rule
        if keys:
            out += ["", f"**{wl.name} measures, per block (mean over blocks), {path}**", "",
                    "| measure | " + " | ".join(arms) + " |", "|---|" + "---:|" * len(arms)]
            for k in keys:
                cells = [fmt(st.mean([block_measures(path, b, a_).get(k, float("nan")) for b in blocks_here]), ".4g")
                         for a_ in arms]
                tag = "" if k in judged_rules else (" (report only)" if k not in unruled else " (**no rule**)")
                out.append(f"| {k}{tag} | " + " | ".join(cells) + " |")
        if judged_rules:
            out += ["", f"**{wl.name} rules (pre-registered in gates.ini [workload:{wl.name}]), {path}**", "",
                    "| measure | rule | candidate | blocks | mean contrast | t lower bound | bootstrap lower bound | verdict |",
                    "|---|---|---|---:|---:|---:|---:|---|"]
            for key, (kind, values) in judged_rules.items():
                for cand in arms[1:]:
                    # Every block in which this measure's trial type (its phase)
                    # ran in both arms -- a type stopped early has fewer -- the
                    # measure present or not: a block missing it in either arm
                    # is unmeasured, not left out of the coverage count.
                    common = [b for b in blocks_here if ran(path, b, cand, key) and ran(path, b, main, key)]
                    c = [block_measures(path, b, cand).get(key, float("nan")) for b in common]
                    m = [block_measures(path, b, main).get(key, float("nan")) for b in common]
                    # Coverage against the blocks this measure's type ran (its
                    # evidence present or not), not every block on the path.
                    need = math.ceil(gates["secondary_min_coverage"] *
                                     (judged.get(cand, len(common)) if wl.primary else len(common)) - 1e-9)
                    measured = [(x, y) for x, y in zip(c, m) if x == x and (y == y or kind in HARD)]
                    if key in getattr(wl, "primaries", ()) and kind in RULES and m:
                        # A primary outcome: void, not judged, when too many
                        # of main's blocks delivered nothing (section 15.4).
                        zero = sum(1 for y in m if y == y and y == 0) / len(m)
                        if zero > gates["max_zero_main_fraction"]:
                            verdicts[(path, cand, f"rule:{key}")] = "VOID"
                            out.append(f"| {key} | {kind} {' '.join(f'{v:g}' for v in values)} | {cand} | {len(m)} | "
                                       f"n/a | n/a | n/a | VOID ({zero:.0%} of main's blocks delivered nothing) |")
                            continue
                    if wl.sample_based:
                        verdict, mean_d, tl, bl, n_ = sample_rule(wl, trials, path, cand, main, key, kind, values, gates)
                        verdicts[(path, cand, f"rule:{key}")] = verdict
                        out.append(f"| {key} | {kind} {' '.join(f'{v:g}' for v in values)} | {cand} | {n_} samples | "
                                   f"{fmt(mean_d, '.4g')} | n/a | {fmt(bl, '.4g')} | {verdict} |")
                        continue
                    dec = None
                    # Only blocks whose measure is valid in both arms and that
                    # have samples on both sides go into a pooled tail.
                    valid = [b for b, x, y in zip(common, c, m) if x == x and y == y]
                    tails = [(block_tails(path, b, cand).get(key), block_tails(path, b, main).get(key)) for b in valid]
                    tails = [(c_, m_) for c_, m_ in tails if c_ and m_ and c_[1] and m_[1]]
                    has_tail = any(block_tails(path, b, cand).get(key) for b in common)
                    if kind in RULES and has_tail and (not tails or
                            min(min(len(c_[1]), len(m_[1])) for c_, m_ in tails) < tail_min(tails[0][0][0])):
                        # A tail percentile with too few samples per block is
                        # computed over all blocks' pooled samples and tested
                        # by resampling blocks (section 15.4).
                        a_ = sq.alpha_at(len(tails)) if sq else gates["alpha"]
                        q = tails[0][0][0] if tails else 0.95
                        mean_d, lo_, hi_ = st.pooled_quantile_bounds(
                            [c_[1] for c_, _ in tails], [m_[1] for _, m_ in tails], q,
                            lambda c__, m__: RULES[kind](c__, m__, values), a_, gates["resamples"], gates["seed"])
                        if lo_ > 0:
                            dec = "pass"
                        elif hi_ < 0 or (sq and len(tails) >= sq.cap) or not sq:
                            dec = "fail"
                        else:
                            dec = "continue"
                        n_pooled = sum(len(c_[1]) for c_, _ in tails)
                        verdict = "n/a" if len(tails) < max(need, 2) else decision_verdict(dec, undecided_ok)
                        verdicts[(path, cand, f"rule:{key}")] = verdict
                        out.append(f"| {key} | {kind} {' '.join(f'{v:g}' for v in values)} | {cand} | {len(tails)} "
                                   f"(pooled, {n_pooled} samples) | {fmt(mean_d, '.4g')} | n/a | {fmt(lo_, '.4g')} | {verdict} |")
                        continue
                    if kind in HARD:
                        ok = bool(measured) and all(HARD[kind](x, values) for x, _ in measured)
                        mean_d, tl, bl = float("nan"), float("nan"), float("nan")
                    elif kind in VS_MAIN:
                        ok = bool(measured) and all(y == y and VS_MAIN[kind](x, y, values) for x, y in measured)
                        mean_d, tl, bl = float("nan"), float("nan"), float("nan")
                    else:
                        d = rule_contrasts(kind, values, [x for x, _ in measured], [y for _, y in measured])
                        mean_d, tl, bl, ok = st.lower_bounds(d, gates["alpha"], gates["resamples"], gates["seed"])
                        if sq:
                            dec = sq.decide(d, gates["resamples"], gates["seed"])
                            a_ = sq.alpha_at(len(d))
                            mean_d, tl, bl, _ = st.lower_bounds(d, a_, gates["resamples"], gates["seed"])
                    verdict = "n/a" if len(measured) < max(need, 2) else (
                        decision_verdict(dec, undecided_ok) if dec is not None else ("pass" if ok else "**FAIL**"))
                    verdicts[(path, cand, f"rule:{key}")] = verdict
                    out.append(f"| {key} | {kind} {' '.join(f'{v:g}' for v in values)} | {cand} | {len(measured)} | "
                               f"{fmt(mean_d, '.4g')} | {fmt(tl, '.4g')} | {fmt(bl, '.4g')} | {verdict} |")
        for k in unruled:
            for cand in arms[1:]:
                verdicts[(path, cand, f"rule:{k}")] = "n/a"
        unmeasurable = [k for k in gates["secondary"] if getattr(wl, "generic", None)
                        and k not in {x for ks in wl.generic.values() for x in ks} and k in NOT_YET_MEASURED]
        if unmeasurable:
            out += ["", "Generic secondaries the design requires but no build measures yet (stage 0 "
                    "instrumentation), listed and not gated: " + ", ".join(f"`{k}`" for k in unmeasurable) + "."]
        if unruled:
            out += ["", f"Measures without a rule in gates.ini [workload:{wl.name}] (the verdict stays INCOMPLETE): "
                    + ", ".join(f"`{k}`" for k in unruled)]
        rule_keys = list(judged_rules) + unruled
        for cand in arms[1:]:
            prim = verdicts[(path, cand, "primary")]
            sv = [verdicts[(path, cand, k)] for k in secondary] + [verdicts[(path, cand, f"rule:{k}")] for k in rule_keys]
            fails, missing = sv.count("**FAIL**"), sv.count("n/a") + sv.count("undecided")
            if "VOID" in sv and prim != "INCOMPLETE":
                prim = "VOID"
            prim_ok = prim == "**PASS**" or (prim == "n/a" and not wl.primary)
            if prim == "undecided":
                prim, prim_ok = "INCOMPLETE", False
            overall = ("INCOMPLETE" if problems or prim == "INCOMPLETE" else
                       "VOID" if prim == "VOID" else
                       "FAIL" if not prim_ok or fails else
                       "INCOMPLETE" if missing or not sv and not wl.primary else "PASS")
            verdicts[(path, cand, "overall")] = overall
            out += ["", f"**{cand} on {path}: {overall}** (primary {prim.strip('*').lower()}; secondaries "
                    f"{len(sv) - fails - missing} pass, {fails} fail, {missing} not measured"
                    + (": a required secondary that was not measured leaves the gate incomplete, not passed" if missing else "")
                    + ")"]
    write_per_trial(campaign, trials)
    return "\n".join(out) + "\n", verdicts


def tail_min(q: float) -> int:
    """Samples per arm a block needs for a tail percentile to be gated per
    block (section 15.4): 20 for a p95, 100 for a p99."""
    return 100 if q >= 0.99 else 20 if q >= 0.95 else 2


def sample_rule(wl, trials, path: str, cand: str, main: str, key: str, kind: str, values, gates: dict):
    """A sample-based workload's rule (idle-wake): the arms' pooled samples
    compared by an unpaired bootstrap of the measure's statistic; a hard
    rule applies to the candidate's statistic. Returns (verdict, observed
    contrast, nan, bootstrap lower bound, candidate samples)."""
    metric = key.split(".", 1)[-1]
    spec = getattr(wl, "SAMPLES", {}).get(metric)
    nan = float("nan")
    if not spec:
        return "n/a", nan, nan, nan, 0
    src, stat = spec
    ct = [t for t in trials.values() if t.path == path and t.arm == cand]
    mt = [t for t in trials.values() if t.path == path and t.arm == main]
    check = getattr(wl, "samples_complete", None)
    if check and not (check(ct) and check(mt)):
        return "n/a", nan, nan, nan, 0
    cs = wl.samples(ct).get(src, [])
    ms = wl.samples(mt).get(src, [])
    statf = (lambda xs: _sample_stat(xs, stat))
    if kind in VS_MAIN:
        if not cs or not ms:
            return "n/a", nan, nan, nan, len(cs)
        ok = VS_MAIN[kind](statf(cs), statf(ms), values)
        return ("pass" if ok else "**FAIL**"), statf(cs) - statf(ms), nan, nan, len(cs)
    if kind in HARD:
        if not cs:
            return "n/a", nan, nan, nan, 0
        return ("pass" if HARD[kind](statf(cs), values) else "**FAIL**"), statf(cs), nan, nan, len(cs)
    if len(cs) < 2 or len(ms) < 2:
        return "n/a", nan, nan, nan, len(cs)
    import random as _r
    rng = _r.Random(gates["seed"])
    ds = []
    for _ in range(gates["resamples"]):
        c_ = [cs[rng.randrange(len(cs))] for _ in cs]
        m_ = [ms[rng.randrange(len(ms))] for _ in ms]
        ds.append(RULES[kind](statf(c_), statf(m_), values))
    ds.sort()
    lb = ds[int(math.floor(gates["alpha"] * len(ds)))]
    observed = RULES[kind](statf(cs), statf(ms), values)
    return ("pass" if lb > 0 else "**FAIL**"), observed, nan, lb, len(cs)


def _sample_stat(xs, stat) -> float:
    if not xs:
        return float("nan")
    if stat == "mean":
        return sum(xs) / len(xs)
    if stat == "sum":
        return float(sum(xs))
    return st.quantile(xs, stat)


def interim_decisions(campaign: str, gates: dict, units_run: Optional[Sequence[Sequence[Any]]] = None,
                      failures: Optional[dict] = None) -> Dict[Tuple[str, str], List[str]]:
    """The runner's look: every gate decision on the units run so far, as
    'pass', 'fail' or 'continue' (undecided, unmeasured or void), by (path,
    cell): a rule's cell is its phase (the trial type that measures it), the
    primary gate's is ""."""
    with open(os.path.join(campaign, "plan.json")) as f:
        plan = json.load(f)
    wl = ab_workloads.get(plan.get("workload", "bulk"))
    _, v = report(campaign, gates, interim=True, units_run=units_run, failures=failures)
    out: Dict[Tuple[str, str], List[str]] = collections.defaultdict(list)
    # A look on evidence the final analysis would reject (a fetch failure, a
    # missing unit) decides nothing: it waits for another block.
    incomplete = {p_ for (p_, _, k), x in v.items() if k == "primary" and x == "INCOMPLETE"}
    for (path, _, key), verdict in v.items():
        if path in incomplete:
            verdict = "INCOMPLETE"
        # Only the gates that apply: a workload without a byte outcome has no
        # primary gate to wait for.
        if (key == "primary" and wl.primary) or key.startswith("rule:"):
            cell = key[5:].split(".", 1)[0] if key.startswith("rule:") and "." in key else ""
            out[(path, cell)].append({"**PASS**": "pass", "pass": "pass", "**FAIL**": "fail"}.get(verdict, "continue"))
    return dict(out)


def write_per_trial(campaign: str, trials) -> None:
    cols = ["block", "path", "slot", "arm", "arm_trial", "cold", "useful_bytes", "goodput_mbps", "completion", "loss",
            "queue_p90_ms", "stalls_per_trial", "wire_per_useful", "reissued_per_source", "transit_p50_s",
            "transit_p90_s", "cpu_p90", "interactive_p99_s"]
    with open(os.path.join(campaign, "per_trial.tsv"), "w") as f:
        f.write("\t".join(cols) + "\n")
        for t in trials.values():
            base = [t.block, t.path, t.slot, t.arm, t.arm_trial, int(t.cold)]
            f.write("\t".join(str(x) for x in base + [t.m.get(c, "") for c in cols[6:]]) + "\n")


def pilot_report(campaign: str, gates: dict) -> Tuple[str, dict]:
    """Section 15.4 sample size: the contrast's variance and E[main] from the
    pilot, power for the alternative "true ratio 1"."""
    plan, trials = read_campaign(campaign)
    problems, trials = completeness(campaign, plan, trials)
    # A DC campaign's solo calibration slots are not part of the A/B, and its
    # bulk rows are the sustained phase that dc_gate.py tests.
    trials = {k: t for k, t in trials.items() if k[2] < 100}
    duration = float(plan.get("config", {}).get("duration") or 0)
    for t in trials.values():
        t.m["useful_bytes"] = t.useful_bytes()
        t.m["capped"] = capped_by_client([r for r in t.bulk() if r["phase"] in ("main", "sustained")], duration)
    main = plan["main"]
    arms = [a for a in plan["arms"] if a != main]
    out = [f"## Pilot: {os.path.basename(os.path.normpath(campaign))}", "",
           f"Blocks for {gates['power']:.0%} power of the one-sided {1 - gates['alpha']:.0%} t-test of "
           f"`candidate - {gates['ratio']:.2f} x main > 0` when the true ratio is {gates['true_ratio']:g} "
           f"(expected contrast {gates['true_ratio'] - gates['ratio']:.2f} x E[main]). Freeze the largest number "
           "before the campaign, and do not reuse the pilot's blocks.", "",
           "| path | candidate | pilot blocks | E[main], MB | sd(contrast), MB | blocks needed |", "|---|---|---:|---:|---:|---:|"]
    if problems:
        out[2:2] = ["**The pilot is incomplete:** " + "; ".join(problems) + ". No block count is recommended "
                    "until a complete pilot exists.", ""]
    res = {}
    for path in plan["paths"]:
        for cand in arms:
            c, m, common = paired(trials, path, main, cand, block_outcome)
            if len(common) < 2:
                continue
            capped = sum(int(t.m["capped"]) for t in trials.values()
                         if t.path == path and t.arm in (main, cand) and t.block in set(common))
            zero = sum(1 for x in m if x == 0) / len(m)
            if zero > gates["max_zero_main_fraction"]:
                # The primary gate would void this data; do not size from it.
                res[(path, cand)] = None
                out.append(f"| {path} | {cand} | {len(common)} | n/a | n/a | VOID: {zero:.0%} of main's blocks "
                           "delivered nothing; rerun the pilot |")
                continue
            if capped:
                res[(path, cand)] = None
                out.append(f"| {path} | {cand} | {len(common)} | n/a | n/a | VOID: {capped} bulk flow(s) ran out of "
                           "bytes before the window closed; raise object_bytes and rerun the pilot |")
                continue
            d = st.contrasts(c, m, gates["ratio"])
            sd, em = st.stdev(d), st.mean(m)
            try:
                n = st.blocks_for_power(sd, em, ratio=gates["ratio"], alpha=gates["alpha"], power=gates["power"],
                                        true_ratio=gates["true_ratio"])
            except ValueError:
                n = None
            except OverflowError:
                n = "more than 100000: the paths' variance is too large for this gate"
            note = ""
            if problems or len(common) < gates["pilot_blocks"]:
                # The variance is shown for diagnosis; the number to freeze is
                # withheld until the pilot is complete and long enough.
                why = "incomplete pilot" if problems else f"fewer than {gates['pilot_blocks']} blocks"
                note = f" (withheld: {why})"
                n = None
            res[(path, cand)] = n
            out.append(f"| {path} | {cand} | {len(common)} | {fmt(em / 1e6, '.1f')} | {fmt(sd / 1e6, '.2f')} | "
                       f"{n if n is not None else 'n/a'}{note} |")
    return "\n".join(out) + "\n", res


# What the stage-10 default flip needs a PASS for: workload -> its required
# cells (empty: every cell any campaign ran). `minimal` is the default
# profile (about 2.5-3 hours of live time per path); `full` is the opt-in
# matrix. DC is judged once, on its own path.
FLIP_PROFILES = {
    "minimal": {"minimal": ("bulk", "mixed-67", "mixed-all"), "idle-wake": ("wake",), "dc": ("dc-gate",)},
    "full": {w_: () for w_ in ("bulk", "single", "interactive", "llm", "term", "mixed", "dc")},
}
FLIP_WORKLOADS = tuple(FLIP_PROFILES["minimal"])


def cell_verdicts(verdicts: dict, path: str, cand: str) -> Dict[str, str]:
    """Per-cell verdicts from a report's verdict map: a rule-judged workload's
    cells are its phases (every rule of a phase must pass); a bulk workload
    has one cell, its overall verdict."""
    overall = verdicts.get((path, cand, "overall"), "INCOMPLETE")
    cells: Dict[str, List[str]] = collections.defaultdict(list)
    for (p_, c_, key), v in verdicts.items():
        if p_ == path and c_ == cand and key.startswith("rule:"):
            cells[key[5:].split(".", 1)[0]].append(v)
    if not cells:
        return {"main": overall}
    # The campaign itself without a verdict (incomplete) leaves every cell
    # INCOMPLETE; otherwise an unmeasured rule does, then any failure fails.
    incomplete = verdicts.get((path, cand, "primary")) == "INCOMPLETE"
    out = {}
    for cell, vs in cells.items():
        # A cell passes only when every one of its rules explicitly passed:
        # undecided (a sequential campaign stopped on another contrast) or
        # unmeasured is INCOMPLETE, a void primary voids the cell.
        out[cell] = ("INCOMPLETE" if incomplete else
                     "VOID" if "VOID" in vs else
                     "INCOMPLETE" if "n/a" in vs else
                     "FAIL" if "**FAIL**" in vs else
                     "PASS" if all(v in ("pass", "**PASS**") for v in vs) else "INCOMPLETE")
    return out


def build_identity(e: dict) -> Tuple[str, object]:
    """What an arm ran: its resolved commits when it was built from refs,
    else its prebuilt binaries' SHA-256 by role (gateway, client) and
    platform, as the manifest recorded them."""
    g = e.get("gateway_commit") or e.get("commit") or ""
    c = e.get("client_commit") or e.get("commit") or ""
    if g or c:
        return "commits", (g, c)
    digests: Dict[Tuple[str, str], str] = {}
    for end, b in (e.get("binaries") or {}).items():
        role = "gateway" if end == "gateway" else "client"
        if isinstance(b, dict) and b.get("sha256"):
            digests[(role, b.get("platform", ""))] = b["sha256"]
    return "binaries", digests


def arm_config(e: dict) -> Tuple[Tuple[str, ...], ...]:
    """The arm's configured flags: the same build with another flag bundle is
    another configuration, not the same candidate."""
    return tuple(tuple(e.get(k) or ()) for k in ("flags", "gateway_flags", "client_flags"))


def endpoint_switches(plan: dict, path: str) -> Tuple[Tuple[str, ...], ...]:
    """The [gateway] flags and the flags of the path's own client (every
    leaf's, by name, for the DC gate) that the arm's endpoints ran with,
    without the per-run address ones: two campaigns on the same commits but
    other behaviour switches on that path are not the same bundle."""
    def keep(flags):
        out, skip = [], False
        for f in flags or ():
            if skip:
                skip = False
                continue
            if f.split("=", 1)[0] in ("--local-address",):
                skip = "=" not in f
                continue
            out.append(f)
        return tuple(out)
    clients = plan.get("clients") or {}
    if path in clients:
        mine: Tuple[Tuple[str, ...], ...] = (keep(clients[path].get("flags")),)
    else:
        mine = tuple((name,) + keep(c.get("flags")) for name, c in sorted(clients.items()))
    return (keep(plan.get("gateway_flags")),) + mine


def same_build(a: Tuple[str, object], b: Tuple[str, object]) -> bool:
    if a[0] != b[0]:
        return False
    if a[0] == "commits":
        return a[1] == b[1]
    # Binaries: every role and platform both campaigns used must match, and
    # there must be at least one to compare.
    common = set(a[1]) & set(b[1])
    return bool(common) and all(a[1][k] == b[1][k] for k in common)


def merge_build(a: Tuple[str, object], b: Tuple[str, object]) -> Tuple[str, object]:
    """After a successful comparison, remember every role and platform seen,
    so a later campaign is checked against all of them, whatever the order."""
    if a[0] == "binaries":
        return a[0], {**a[1], **b[1]}
    return a


def finished_at(campaign: str) -> str:
    """When the campaign finished (status.json), for ordering; "" if unknown."""
    try:
        with open(os.path.join(campaign, "status.json")) as f:
            return str(json.load(f).get("finished_utc") or "")
    except (OSError, ValueError):
        return ""


def flip_report(campaigns: Sequence[str], gates: dict, profile: str = "minimal",
                carry: Sequence[str] = ()) -> Tuple[str, bool]:
    """The stage-10 default flip over a set of campaigns (design section 15.3:
    each trial type runs once per path, at its gate, and a pass carries
    forward to the flip; a trial is rerun on the final bundle only when a
    later stage changes what it exercises).

    The final bundle of a candidate arm is what it ran in the newest campaign
    (build and flags). A cell measured on the final bundle counts as is. A
    cell measured only on an earlier build counts only when it is named in
    `carry` (the operator's statement that no later stage changed what it
    exercises), and is reported as carried forward. Full coverage: every
    candidate on every path in every required cell. Then each counted cell
    must PASS."""
    required = FLIP_PROFILES[profile] if FLIP_WORKLOADS == tuple(FLIP_PROFILES["minimal"]) else \
        {w_: () for w_ in FLIP_WORKLOADS}
    carry = {c.strip().lower() for c in carry if c.strip()}
    # (workload, path, candidate, cell, verdict, identity, order, campaign)
    rows: List[Tuple[str, str, str, str, str, tuple, tuple, str]] = []
    seen = set()
    problems: List[str] = []
    roles: Dict[str, str] = {}
    paths_all: set = set()
    candidates: set = set()
    # One final build per candidate arm, across every path: its build and arm
    # flags in the newest campaign -> (identity, order, campaign).
    final: Dict[str, Tuple[tuple, tuple, str]] = {}
    for n_, campaign in enumerate(campaigns):
        base = os.path.basename(os.path.normpath(campaign))
        with open(os.path.join(campaign, "plan.json")) as f:
            plan = json.load(f)
        name = plan.get("workload", "bulk")
        if name not in required:
            # An optional campaign may be supplied for its report; it neither
            # covers nor constrains the flip's matrix.
            continue
        seen.add(name)
        order = (finished_at(campaign), n_)
        ident: Dict[Tuple[str, str], tuple] = {}
        plan_paths = ["dc"] if name == "dc" else list(plan["paths"])
        for arm, e in plan["arms"].items():
            role = "main" if arm == plan["main"] else "candidate"
            if roles.setdefault(arm, role) != role:
                problems.append(f"arm `{arm}` is {role} in {base} but {roles[arm]} elsewhere")
            b = build_identity(e)
            build = (b[0], tuple(sorted(b[1].items())) if isinstance(b[1], dict) else b[1], arm_config(e))
            for p_ in plan_paths:
                # (build, arm flags, this path's endpoint switches)
                ident[(arm, p_)] = build + (endpoint_switches(plan, p_),)
            if role == "candidate":
                candidates.add(arm)
                if arm not in final or order > final[arm][1]:
                    final[arm] = (build, order, base)
        if name == "dc":
            import dc_gate
            _, v = dc_gate.evaluate(campaign, dc_gate.load_bounds())
            for cand, verdict in v.items():
                rows.append((name, "dc", cand, "dc-gate", verdict, ident.get((cand, "dc"), ()), order, base))
            continue
        paths_all |= set(plan["paths"])
        _, v = report(campaign, gates)
        for path in plan["paths"]:
            for cand in [a for a in plan["arms"] if a != plan["main"]]:
                for cell, verdict in sorted(cell_verdicts(v, path, cand).items()):
                    rows.append((name, path, cand, cell, verdict, ident[(cand, path)], order, base))

    def same(a: tuple, b: tuple) -> bool:
        """Same build (binaries: every role and platform both used) and arm flags."""
        if not a or not b or a[2] != b[2]:
            return False
        ba = (a[0], dict(a[1]) if a[0] == "binaries" else a[1])
        bb = (b[0], dict(b[1]) if b[0] == "binaries" else b[1])
        return same_build(ba, bb)
    rows = [r for r in rows if r[0] in required]
    # A path's final endpoint switches: those of the newest campaign on that
    # path that ran the candidate's final build.
    final_sw: Dict[Tuple[str, str], tuple] = {}
    for r in sorted(rows, key=lambda r: r[6]):
        fin = final.get(r[2])
        if fin and same(r[5], fin[0]):
            final_sw[(r[2], r[1])] = r[5][3]
    # Per (workload, path, candidate, cell): the newest row on the final
    # bundle (the final build, with the path's final switches), else the
    # newest earlier one if it may carry forward.
    chosen: Dict[Tuple[str, str, str, str], Tuple[str, str]] = {}
    stale: List[str] = []
    final_rows: List[tuple] = []
    for key in sorted({r[:4] for r in rows}):
        rs = sorted((r for r in rows if r[:4] == key), key=lambda r: r[6])
        fin = final.get(key[2])
        on_final = [r for r in rs if fin and same(r[5], fin[0]) and r[5][3] == final_sw.get((key[2], key[1]))]
        if on_final:
            chosen[key] = (on_final[-1][4], "")
            final_rows.append(on_final[-1])
        elif key[3].lower() in carry:
            chosen[key] = (rs[-1][4], f"carried forward from {rs[-1][7]}")
        else:
            stale.append(f"{key[0]} {key[3]} on {key[1]} for {key[2]} ran on an earlier build or other flags "
                         f"({rs[-1][7]}; the final bundle is {fin[2] if fin else '?'}): rerun it on the final bundle, "
                         f"or carry it forward with --carry {key[3]} if no later stage changed what it exercises")
    # One SHA-256 per (role, platform) of each candidate's prebuilt binaries
    # across every campaign counted on the final bundle: two campaigns that
    # each match the newest one on a shared digest may still disagree on a
    # platform the newest did not use.
    accepted: Dict[Tuple[str, Tuple[str, str]], Tuple[str, str]] = {}
    for r in sorted(final_rows, key=lambda r: r[6]):
        if r[5] and r[5][0] == "binaries":
            for rp, digest in r[5][1]:
                got = accepted.setdefault((r[2], rp), (digest, r[7]))
                if got[0] != digest:
                    problems.append(f"arm `{r[2]}` ran {rp[0]} {rp[1]} binary {got[0][:12]} in {got[1]} but "
                                    f"{digest[:12]} in {r[7]}")
    problems[:] = list(dict.fromkeys(problems))
    # Coverage before verdicts: a combination with no row is missing, not
    # passed by the rows that happen to exist.
    cells_of: Dict[str, set] = collections.defaultdict(set)
    for r in rows:
        cells_of[r[0]].add(r[3])
    missing: List[str] = [f"{w_} (no campaign)" for w_ in required if w_ not in seen]
    for w_ in sorted(set(required)):
        for p_ in (["dc"] if w_ == "dc" else sorted(paths_all)):
            for c_ in sorted(candidates):
                for cell in sorted(set(required[w_]) or cells_of.get(w_) or {"(any)"}):
                    if not any(r[:4] == (w_, p_, c_, cell) for r in rows):
                        missing.append(f"{w_} {cell} on {p_} for {c_}")
    ok = bool(chosen) and not missing and not stale and not problems and all(v_ == "PASS" for v_, _ in chosen.values())
    out = ["## Stage-10 default flip", "",
           "Every path x trial type of every candidate must PASS on the final bundle, or carry its pass forward "
           "(DESIGN-PATH-AUTHORITY.md sections 14 and 15.3).", "",
           "| workload | path | candidate | cell | verdict | note |", "|---|---|---|---|---|---|"]
    out += [f"| {w_} | {p_} | {c_} | {cell} | {v_ if v_ == 'PASS' else '**' + v_ + '**'} | {note} |"
            for (w_, p_, c_, cell), (v_, note) in sorted(chosen.items())]
    if final:
        out += ["", "Final bundle: " + "; ".join(f"`{a_}` as run in {fin[2]}" for a_, fin in sorted(final.items())) + "."]
    if missing:
        out += ["", "Missing: " + ", ".join(f"`{m}`" for m in missing) + "."]
    if stale:
        out += ["", "Not on the final bundle: " + "; ".join(stale) + "."]
    if problems:
        out += ["", "Inconsistent: " + "; ".join(problems) + "."]
    out += ["", f"**Flip: {'PASS' if ok else 'NOT PASSED'}.**"]
    return "\n".join(out) + "\n", ok


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("campaign", nargs="+")
    ap.add_argument("--gates", default=GATES)
    ap.add_argument("--pilot", action="store_true", help="estimate the block count from this (pilot) campaign")
    ap.add_argument("--flip", action="store_true",
                    help="judge the stage-10 flip over these campaigns: every path x workload x cell must PASS")
    ap.add_argument("--profile", choices=sorted(FLIP_PROFILES), default="minimal",
                    help="which workloads the flip requires (default: minimal)")
    ap.add_argument("--carry", default="",
                    help="with --flip: comma-separated cells (bulk, mixed-67, dc-gate, ...) whose pass on an earlier "
                         "build carries forward because no later stage changed what they exercise")
    args = ap.parse_args(argv)
    gates = load_gates(args.gates)
    if args.flip:
        text, ok = flip_report(args.campaign, gates, args.profile, args.carry.split(","))
        sys.stdout.write(text)
        return 0 if ok else 1
    if len(args.campaign) != 1:
        ap.error("one campaign at a time, except with --flip")
    args.campaign = args.campaign[0]
    if args.pilot:
        text, _ = pilot_report(args.campaign, gates)
        name = "pilot.md"
    else:
        text, _ = report(args.campaign, gates)
        name = "summary.md"
    sys.stdout.write(text)
    with open(os.path.join(args.campaign, name), "w") as f:
        f.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())

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
import json
import math
import os
import re
import sys
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

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
                kinds = list(RULES) + list(HARD)
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
    return {"ratio": p.getfloat("ratio"), "alpha": p.getfloat("alpha"), "workloads": workloads,
            "resamples": p.getint("bootstrap_resamples"), "seed": p.getint("bootstrap_seed"),
            "max_zero_main_fraction": p.getfloat("max_zero_main_fraction"),
            "secondary_min_coverage": p.getfloat("secondary_min_coverage", fallback=1.0),
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
    def __init__(self, block: int, path: str, slot: int, arm: str, role: str, arm_trial: int, cold: bool):
        self.block, self.path, self.slot, self.arm, self.role = block, path, slot, arm, role
        self.arm_trial, self.cold = arm_trial, cold
        self.start = math.inf
        self.end = -math.inf
        self.rows: List[dict] = []
        self.records: List[dict] = []
        self.m: Dict[str, float] = {}

    @property
    def tag(self) -> str:
        return f"b{self.block:03d}-{self.path}"

    def bulk(self) -> List[dict]:
        return [r for r in self.rows if r["kind"] == "bulk"]

    def useful_bytes(self) -> int:
        return sum(useful(r) for r in self.bulk())


def read_campaign(campaign: str) -> Tuple[dict, Dict[Tuple[int, str, int], Trial]]:
    with open(os.path.join(campaign, "plan.json")) as f:
        plan = json.load(f)
    trials: Dict[Tuple[int, str, int], Trial] = collections.OrderedDict()
    with open(os.path.join(campaign, "trials.tsv")) as f:
        for r in csv.DictReader(f, delimiter="\t"):
            key = (int(r["block"]), r["path"], int(r["slot"]))
            t = trials.get(key)
            if t is None:
                t = trials[key] = Trial(key[0], key[1], key[2], r["arm"], r["role"], int(r["arm_trial"]), r["cold"] == "1")
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


def fetch_failed(campaign: str) -> List[str]:
    """Remote log paths the runner could not fetch (status.json)."""
    try:
        with open(os.path.join(campaign, "status.json")) as f:
            return list(json.load(f).get("fetch_failures") or [])
    except (OSError, ValueError):
        return []


LOG_MEASURES = ("queue_p90_ms", "reissued_per_source", "transit_p50_s", "transit_p90_s", "cpu_p90")


def completeness(campaign: str, plan: dict, trials: Dict[Tuple[int, str, int], Trial]) -> Tuple[List[str], Dict[Tuple[int, str, int], Trial]]:
    """Problems that forbid a verdict, and the trials of complete blocks only.

    A verdict needs a campaign that finished (status.json from the runner)
    with production unchanged, and every planned slot present. A block with
    a missing slot is dropped, so an interrupted block cannot pair two
    candidate trials with one main trial; the problems list says so and the
    verdict becomes INCOMPLETE.
    """
    problems: List[str] = []
    failed_jobs: List[dict] = []
    try:
        with open(os.path.join(campaign, "status.json")) as f:
            status = json.load(f)
    except (OSError, ValueError):
        problems.append("no status.json: the campaign did not finish")
    else:
        if status.get("status") != "complete":
            problems.append(f"the campaign {status.get('status')} (exit {status.get('exit')})")
        if status.get("production") != "unchanged":
            problems.append("production changed during the campaign")
        if status.get("fetch_failures"):
            problems.append(f"{len(status['fetch_failures'])} log file(s) could not be fetched; the log-derived "
                            "measures of the affected trials are withheld")
        failed_jobs = status.get("job_failures") or []
    planned: Dict[Tuple[int, str], set] = collections.defaultdict(set)
    for sl in plan.get("schedule", []):
        planned[(sl["block"], sl["path"])].add(sl["slot"])
    have: Dict[Tuple[int, str], set] = collections.defaultdict(set)
    for (b, p, slot) in trials:
        if slot < 100:
            have[(b, p)].add(slot)
    bad = sorted(k for k, slots in planned.items() if not slots <= have.get(k, set()))
    # A block in which a generator did not run to its end has harness
    # evidence, not the arm's: left out like a missing trial.
    broken = sorted({(j["block"], j["path"]) for j in failed_jobs})
    if broken:
        problems.append(f"{len(broken)} block(s) had a job that did not run to its end "
                        f"(first: block {broken[0][0]} on {broken[0][1]}); they are left out")
        bad = sorted(set(bad) | set(broken))
    if bad:
        problems.append(f"{len(bad)} of {len(planned)} planned blocks are missing trials "
                        f"(first: block {bad[0][0]} on {bad[0][1]}); they are left out")
    keep = {k: t for k, t in trials.items() if (k[0], k[1]) not in set(bad)}
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


def report(campaign: str, gates: dict) -> Tuple[str, dict]:
    plan, trials = read_campaign(campaign)
    if plan.get("workload") == "dc":
        raise SystemExit("this is a DC gate campaign: evaluate it with scripts/dc_gate.py")
    EFFECTIVE.clear()
    problems, trials = completeness(campaign, plan, trials)
    method = measure(campaign, plan, trials)
    wl = ab_workloads.get(plan.get("workload", "bulk"))
    for t in trials.values():
        t.m.update(wl.extract(t, plan))
    rules = gates.get("workloads", {}).get(wl.name, {})
    # The arm's per-block measures, as the workload defines them (pooled or
    # averaged), for the rules.
    block_cache: Dict[Tuple[str, int, str], Dict[str, float]] = {}

    def block_measures(path: str, block: int, arm: str) -> Dict[str, float]:
        k = (path, block, arm)
        if k not in block_cache:
            ts_ = [t for t in trials.values() if t.path == path and t.block == block and t.arm == arm]
            block_cache[k] = wl.block_extract(ts_, plan) if ts_ else {}
        return block_cache[k]
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
        if any(t.m.get("interactive_p99_s") == t.m.get("interactive_p99_s") for a in arms for t in pts[a]):
            row("interactive p99, s (median)", [fmt(med(a, "interactive_p99_s"), ".3f") for a in arms])

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
                    common = [b for b in blocks_here if block_measures(path, b, cand) and block_measures(path, b, main)]
                    c = [block_measures(path, b, cand).get(key, float("nan")) for b in common]
                    m = [block_measures(path, b, main).get(key, float("nan")) for b in common]
                    need = math.ceil(gates["secondary_min_coverage"] * judged.get(cand, len(common)) - 1e-9)
                    measured = [(x, y) for x, y in zip(c, m) if x == x and (y == y or kind in HARD)]
                    if kind in HARD:
                        ok = bool(measured) and all(HARD[kind](x, values) for x, _ in measured)
                        mean_d, tl, bl = float("nan"), float("nan"), float("nan")
                    else:
                        d = rule_contrasts(kind, values, [x for x, _ in measured], [y for _, y in measured])
                        mean_d, tl, bl, ok = st.lower_bounds(d, gates["alpha"], gates["resamples"], gates["seed"])
                    verdict = "n/a" if len(measured) < max(need, 2) else ("pass" if ok else "**FAIL**")
                    verdicts[(path, cand, f"rule:{key}")] = verdict
                    out.append(f"| {key} | {kind} {' '.join(f'{v:g}' for v in values)} | {cand} | {len(measured)} | "
                               f"{fmt(mean_d, '.4g')} | {fmt(tl, '.4g')} | {fmt(bl, '.4g')} | {verdict} |")
        for k in unruled:
            for cand in arms[1:]:
                verdicts[(path, cand, f"rule:{k}")] = "n/a"
        if unruled:
            out += ["", f"Measures without a rule in gates.ini [workload:{wl.name}] (the verdict stays INCOMPLETE): "
                    + ", ".join(f"`{k}`" for k in unruled)]
        rule_keys = list(judged_rules) + unruled
        for cand in arms[1:]:
            prim = verdicts[(path, cand, "primary")]
            sv = [verdicts[(path, cand, k)] for k in secondary] + [verdicts[(path, cand, f"rule:{k}")] for k in rule_keys]
            fails, missing = sv.count("**FAIL**"), sv.count("n/a")
            prim_ok = prim == "**PASS**" or (prim == "n/a" and not wl.primary)
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


# The workloads the stage-10 default flip needs a PASS for, on every path
# (WORKLOADS.md 6.1; Phase B adds the rest).
FLIP_WORKLOADS = ("bulk", "single", "interactive", "llm", "term", "mixed")


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
        out[cell] = ("INCOMPLETE" if incomplete or "n/a" in vs else
                     "FAIL" if "**FAIL**" in vs else "PASS")
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


def flip_report(campaigns: Sequence[str], gates: dict) -> Tuple[str, bool]:
    """The stage-10 default flip over a set of campaigns. One release: each
    arm the same build, in the same role, everywhere. Full coverage: every
    candidate on every path in every required workload. Then every cell of
    every one of those must PASS."""
    rows: List[Tuple[str, str, str, str, str]] = []
    seen = set()
    mismatch: List[str] = []
    builds: Dict[str, Tuple[str, object]] = {}
    configs: Dict[str, Tuple[Tuple[str, ...], ...]] = {}
    roles: Dict[str, str] = {}
    paths_all: set = set()
    candidates: set = set()
    for campaign in campaigns:
        base = os.path.basename(os.path.normpath(campaign))
        with open(os.path.join(campaign, "plan.json")) as f:
            plan = json.load(f)
        name = plan.get("workload", "bulk")
        seen.add(name)
        for arm, e in plan["arms"].items():
            role = "main" if arm == plan["main"] else "candidate"
            if roles.setdefault(arm, role) != role:
                mismatch.append(f"arm `{arm}` is {role} in {base} but {roles[arm]} elsewhere")
            b = build_identity(e)
            if arm in builds:
                if not same_build(builds[arm], b):
                    mismatch.append(f"arm `{arm}` is a different build in {base}")
                else:
                    builds[arm] = merge_build(builds[arm], b)
            else:
                builds[arm] = b
            cfg_ = arm_config(e)
            if configs.setdefault(arm, cfg_) != cfg_:
                mismatch.append(f"arm `{arm}` has different flags in {base}")
            if role == "candidate":
                candidates.add(arm)
        if name == "dc":
            import dc_gate
            _, v = dc_gate.evaluate(campaign, dc_gate.load_bounds())
            for cand, verdict in v.items():
                rows.append((name, "dc", cand, "dc-gate", verdict))
            continue
        paths_all |= set(plan["paths"])
        _, v = report(campaign, gates)
        for path in plan["paths"]:
            for cand in [a for a in plan["arms"] if a != plan["main"]]:
                for cell, verdict in sorted(cell_verdicts(v, path, cand).items()):
                    rows.append((name, path, cand, cell, verdict))
    if len({a for a, r in roles.items() if r == "main"}) > 1:
        mismatch.append("the campaigns do not share one reference arm: "
                        + ", ".join(sorted(a for a, r in roles.items() if r == "main")))
    # Coverage before verdicts: a combination with no row is missing, not
    # passed by the rows that happen to exist.
    # Per cell: a workload's required cells are every cell any campaign ran
    # for it, and each must be there on every path for every candidate.
    have = {(w_, p_, c_, cell) for w_, p_, c_, cell, _ in rows}
    cells_of: Dict[str, set] = collections.defaultdict(set)
    for w_, _, _, cell, _ in rows:
        cells_of[w_].add(cell)
    missing: List[str] = [f"{w_} (no campaign)" for w_ in FLIP_WORKLOADS if w_ not in seen]
    for w_ in sorted((set(FLIP_WORKLOADS) | seen) - {"dc"}):
        for p_ in sorted(paths_all):
            for c_ in sorted(candidates):
                for cell in sorted(cells_of.get(w_) or {"(any)"}):
                    if (w_, p_, c_, cell) not in have:
                        missing.append(f"{w_} {cell} on {p_} for {c_}")
    ok = bool(rows) and not missing and not mismatch and all(r[4] == "PASS" for r in rows)
    out = ["## Stage-10 default flip", "",
           "Every path x workload x cell of every candidate must PASS (DESIGN-PATH-AUTHORITY.md section 14, stage 10; "
           "WORKLOADS.md 6).", "", "| workload | path | candidate | cell | verdict |", "|---|---|---|---|---|"]
    out += [f"| {w_} | {p_} | {c_} | {cell} | {v_ if v_ == 'PASS' else '**' + v_ + '**'} |" for w_, p_, c_, cell, v_ in rows]
    if missing:
        out += ["", "Missing: " + ", ".join(f"`{m}`" for m in missing) + "."]
    if mismatch:
        out += ["", "Not one release: " + "; ".join(mismatch) + "."]
    out += ["", f"**Flip: {'PASS' if ok else 'NOT PASSED'}.**"]
    return "\n".join(out) + "\n", ok


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("campaign", nargs="+")
    ap.add_argument("--gates", default=GATES)
    ap.add_argument("--pilot", action="store_true", help="estimate the block count from this (pilot) campaign")
    ap.add_argument("--flip", action="store_true",
                    help="judge the stage-10 flip over these campaigns: every path x workload x cell must PASS")
    args = ap.parse_args(argv)
    gates = load_gates(args.gates)
    if args.flip:
        text, ok = flip_report(args.campaign, gates)
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

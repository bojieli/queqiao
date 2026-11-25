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
from typing import Dict, Iterable, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ab_stats as st  # noqa: E402

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
    return {"ratio": p.getfloat("ratio"), "alpha": p.getfloat("alpha"),
            "resamples": p.getint("bootstrap_resamples"), "seed": p.getint("bootstrap_seed"),
            "max_zero_main_fraction": p.getfloat("max_zero_main_fraction"),
            "pilot_blocks": w.getint("pilot_blocks"), "power": w.getfloat("power"),
            "true_ratio": w.getfloat("true_ratio"), "secondary": sec}


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


class Trial:
    def __init__(self, block: int, path: str, slot: int, arm: str, role: str, arm_trial: int, cold: bool):
        self.block, self.path, self.slot, self.arm, self.role = block, path, slot, arm, role
        self.arm_trial, self.cold = arm_trial, cold
        self.start = math.inf
        self.end = -math.inf
        self.rows: List[dict] = []
        self.m: Dict[str, float] = {}

    @property
    def tag(self) -> str:
        return f"b{self.block:03d}-{self.path}"

    def bulk(self) -> List[dict]:
        return [r for r in self.rows if r["kind"] == "bulk"]

    def useful_bytes(self) -> int:
        return sum(int(r["bytes"] or 0) for r in self.bulk())


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
    return plan, trials


def completeness(campaign: str, plan: dict, trials: Dict[Tuple[int, str, int], Trial]) -> Tuple[List[str], Dict[Tuple[int, str, int], Trial]]:
    """Problems that forbid a verdict, and the trials of complete blocks only.

    A verdict needs a campaign that finished (status.json from the runner)
    with production unchanged, and every planned slot present. A block with
    a missing slot is dropped, so an interrupted block cannot pair two
    candidate trials with one main trial; the problems list says so and the
    verdict becomes INCOMPLETE.
    """
    problems: List[str] = []
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
    planned: Dict[Tuple[int, str], set] = collections.defaultdict(set)
    for sl in plan.get("schedule", []):
        planned[(sl["block"], sl["path"])].add(sl["slot"])
    have: Dict[Tuple[int, str], set] = collections.defaultdict(set)
    for (b, p, slot) in trials:
        if slot < 100:
            have[(b, p)].add(slot)
    bad = sorted(k for k, slots in planned.items() if not slots <= have.get(k, set()))
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
        t.m["flows"] = len(bulk)
        t.m["ok"] = sum(int(r["complete"]) for r in bulk)
        t.m["completion"] = t.m["ok"] / len(bulk) if bulk else float("nan")
        sent = d("queqiao_quic_packets_sent")
        t.m["loss"] = d("queqiao_quic_loss_observed_packets_total") / sent if sent and sent == sent else float("nan")
        t.m["stalls_per_trial"] = d("queqiao_flow_stalls_detected_total")
        wire = d("queqiao_quic_bytes_sent")
        t.m["wire_per_useful"] = wire / useful if useful else float("nan")
        # A probe that did not finish counts at its timeout: dropping it
        # would let an arm improve its p99 by failing its slowest requests.
        probes = [float(r["seconds"]) if r["complete"] == "1" else PROBE_TIMEOUT for r in t.rows if r["kind"] == "probe"]
        t.m["interactive_p99_s"] = st.quantile(probes, 0.99) if probes else float("nan")

        key = (t.arm, t.tag)
        if key not in cache:
            recs = list(iter_json(server_logs(campaign, t.arm, t.tag),
                                  ('"lane_metrics"', '"flow ack received"', '"runtime configuration"')))
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
        t.m["_transit"] = ex
        t.m["transit_p50_s"] = st.quantile(ex, 0.5)
        t.m["transit_p90_s"] = st.quantile(ex, 0.9)
        t.m["cpu_p90"] = cpu_p90(cpus, t)
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
    return st.mean([t.m.get(key, float("nan")) for t in ts_])


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
}


def report(campaign: str, gates: dict) -> Tuple[str, dict]:
    plan, trials = read_campaign(campaign)
    if plan.get("workload") == "dc":
        raise SystemExit("this is a DC gate campaign: evaluate it with scripts/dc_gate.py")
    EFFECTIVE.clear()
    problems, trials = completeness(campaign, plan, trials)
    method = measure(campaign, plan, trials)
    main = plan["main"]
    arms = [main] + [a for a in plan["arms"] if a != main]
    paths = plan["paths"]
    # The interactive probe runs only in the interactive workload; elsewhere
    # that secondary is not applicable rather than unmeasured.
    secondary = {k: v for k, v in gates["secondary"].items()
                 if not (k == "interactive_p99_s" and plan.get("workload") != "interactive")}
    out: List[str] = []
    verdicts: dict = {}
    def sha(a: str) -> str:
        e = plan["arms"][a]
        if e.get("commit") or e.get("ref"):
            return (e.get("commit") or e.get("ref"))[:12]
        if e.get("gateway_ref"):
            return f"gateway {(e.get('gateway_commit') or e['gateway_ref'])[:12]}, client {(e.get('client_commit') or e['client_ref'])[:12]}"
        return ""
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
        row("wire bytes / useful byte (median)", [fmt(med(a, "wire_per_useful"), ".2f") for a in arms])
        row("reissued / source bytes (median)", [fmt(med(a, "reissued_per_source"), ".1%") for a in arms])
        pooled = {a: [x for t in pts[a] for x in t.m.get("_transit", [])] for a in arms}
        row("flow-ACK transit p50 / p90, s (pooled)", [f"{fmt(st.quantile(pooled[a], .5), '.3f')} / {fmt(st.quantile(pooled[a], .9), '.3f')}" for a in arms])
        row("client CPU p90, cores (median)", [fmt(med(a, "cpu_p90"), ".2f") for a in arms])
        if any(t.m.get("interactive_p99_s") == t.m.get("interactive_p99_s") for a in arms for t in pts[a]):
            row("interactive p99, s (median)", [fmt(med(a, "interactive_p99_s"), ".3f") for a in arms])

        out += ["", f"**Primary gate (section 15.4), {path}**", "",
                "| candidate | blocks | ratio of means | mean contrast, MB | t lower bound, MB | bootstrap lower bound, MB | main zero blocks | verdict |",
                "|---|---:|---:|---:|---:|---:|---:|---|"]
        for cand in arms[1:]:
            c, m, common = paired(trials, path, main, cand, block_outcome)
            g = st.gate(c, m, ratio=gates["ratio"], alpha=gates["alpha"], resamples=gates["resamples"],
                        seed=gates["seed"], max_zero_main_fraction=gates["max_zero_main_fraction"])
            verdict = "VOID" if g.void else ("**PASS**" if g.passed else "**FAIL**")
            if problems and verdict == "**PASS**":
                verdict = "INCOMPLETE"
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
                    if bound != bound:
                        verdict = "n/a"
                    elif upper:
                        verdict = "pass" if bound <= margin else "**FAIL**"
                    else:
                        verdict = "pass" if bound >= -margin else "**FAIL**"
                    verdicts[(path, cand, key)] = verdict
                    out.append(f"| {SECONDARY_LABELS.get(key, key)} | {cand} | {n} | {fmt(mdiff, '.4g')} | "
                               f"{'upper' if upper else 'lower'} {fmt(bound, '.4g')} | {'+' if upper else '-'}{margin:g} | {verdict} |")
        for cand in arms[1:]:
            prim = verdicts[(path, cand, "primary")]
            sv = [verdicts[(path, cand, k)] for k in secondary]
            fails, missing = sv.count("**FAIL**"), sv.count("n/a")
            overall = ("VOID" if prim == "VOID" else
                       "INCOMPLETE" if prim == "INCOMPLETE" else
                       "FAIL" if prim != "**PASS**" or fails else
                       "INCOMPLETE" if missing else "PASS")
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
    for t in trials.values():
        t.m["useful_bytes"] = t.useful_bytes()
    main = plan["main"]
    arms = [a for a in plan["arms"] if a != main]
    out = [f"## Pilot: {os.path.basename(os.path.normpath(campaign))}", "",
           f"Blocks for {gates['power']:.0%} power of the one-sided {1 - gates['alpha']:.0%} t-test of "
           f"`candidate - {gates['ratio']:.2f} x main > 0` when the true ratio is {gates['true_ratio']:g} "
           f"(expected contrast {gates['true_ratio'] - gates['ratio']:.2f} x E[main]). Freeze the largest number "
           "before the campaign, and do not reuse the pilot's blocks.", "",
           "| path | candidate | pilot blocks | E[main], MB | sd(contrast), MB | blocks needed |", "|---|---|---:|---:|---:|---:|"]
    if problems:
        out[2:2] = ["**The pilot is incomplete:** " + "; ".join(problems) + ".", ""]
    res = {}
    for path in plan["paths"]:
        for cand in arms:
            c, m, common = paired(trials, path, main, cand, block_outcome)
            if len(common) < 2:
                continue
            d = st.contrasts(c, m, gates["ratio"])
            sd, em = st.stdev(d), st.mean(m)
            try:
                n = st.blocks_for_power(sd, em, ratio=gates["ratio"], alpha=gates["alpha"], power=gates["power"],
                                        true_ratio=gates["true_ratio"])
            except ValueError:
                n = None
            res[(path, cand)] = n
            note = "" if len(common) >= gates["pilot_blocks"] else f" (pilot has fewer than {gates['pilot_blocks']} blocks)"
            out.append(f"| {path} | {cand} | {len(common)} | {fmt(em / 1e6, '.1f')} | {fmt(sd / 1e6, '.2f')} | "
                       f"{n if n is not None else 'n/a'}{note} |")
    return "\n".join(out) + "\n", res


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("campaign")
    ap.add_argument("--gates", default=GATES)
    ap.add_argument("--pilot", action="store_true", help="estimate the block count from this (pilot) campaign")
    args = ap.parse_args(argv)
    gates = load_gates(args.gates)
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

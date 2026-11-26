#!/usr/bin/env python3
"""Evaluate a stage 9 DC gate campaign (DESIGN-PATH-AUTHORITY.md section 11.3).

    ./scripts/dc_gate.py CAMPAIGN_DIR

Reads a campaign that ab_campaign.py ran with `workload = dc` and the frozen
bounds in scripts/validation/dc_gate.ini. Per trial it computes, over the
loaded leaves running at once: aggregate sustained useful bytes, the worst
leaf's sustained bytes, Jain's fairness index across leaves, the completion
spread (slowest leaf's median request completion over the fastest leaf's),
request latency p50 and p99, and the control leaf's request p50. Each
criterion is a paired per-block contrast against the legacy (main) arm with
one-sided t and bootstrap lower bounds. The in-block calibration establishes
whether the common ancestor actually bound; if it did not, the campaign is
void. Prints a markdown table and writes CAMPAIGN_DIR/dc_gate.md.
"""
from __future__ import annotations

import argparse
import configparser
import glob
import hashlib
import os
import sys
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ab_stats as st  # noqa: E402
from ab_analyze import (OVERLAP_MIN, PROBE_TIMEOUT, Trial, capped_flows, completeness, flow_rows,  # noqa: E402
                        fmt, iter_json, metrics, read_campaign, span, tcp_evidence, ts, under_load, useful)

BOUNDS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "validation", "dc_gate.ini")


def load_bounds(path: str = BOUNDS) -> dict:
    cp = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=(";", "#"))
    if not cp.read(path):
        raise SystemExit(f"cannot read {path}")
    s, v, g = cp["statistics"], cp["validity"], cp["gate"]
    out = {k: g.getfloat(k) for k in g}
    out.update(alpha=s.getfloat("alpha"), resamples=s.getint("bootstrap_resamples"), seed=s.getint("bootstrap_seed"),
               max_zero_main_fraction=s.getfloat("max_zero_main_fraction"), binding_max=v.getfloat("binding_max"),
               min_legacy_completion=v.getfloat("min_legacy_completion"))
    with open(path, "rb") as f:
        out["sha256"] = hashlib.sha256(f.read()).hexdigest()
    return out


def leaf_rate(rows: List[dict]) -> float:
    """Bytes per second while the leaf was loaded: its bytes over the longest
    of its flows (a backlogged flow's objects back to back). A rate, not a
    byte total, so that objects finishing inside the window cannot make a
    shared bottleneck look unshared."""
    secs = max((sum(float(r["seconds"] or 0) for r in rs) for rs in flow_rows(rows).values()), default=0.0)
    return sum(useful(r) for r in rows) / secs if secs > 0 else float("nan")


def merge(intervals: List[Tuple[float, float]], join: float = 0.5) -> List[Tuple[float, float]]:
    """Union of intervals; ones less than `join` seconds apart (a flow's
    back-to-back objects) are joined."""
    out: List[List[float]] = []
    for a, b in sorted(intervals):
        if out and a <= out[-1][1] + join:
            out[-1][1] = max(out[-1][1], b)
        else:
            out.append([a, b])
    return [(a, b) for a, b in out]


def intersect(x: List[Tuple[float, float]], y: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
    out = []
    for a, b in x:
        for c, d in y:
            lo, hi = max(a, c), min(b, d)
            if hi > lo:
                out.append((lo, hi))
    return merge(out, 0.0)


def active(rows: List[dict]) -> List[Tuple[float, float]]:
    """When these rows were actually transferring: the union of their
    millisecond intervals, each shrunk by the clock uncertainty."""
    return merge([(a + OVERLAP_MIN, b - OVERLAP_MIN) for a, b in (x for x in (span(r) for r in rows) if x)
                  if b - a > 2 * OVERLAP_MIN])


def shared_window(rows: List[dict], leaves: List[str], duration: float, share: float = 0.9) -> bool:
    """Every leaf was transferring at the same time for at least `share` of
    the window: the intersection, across leaves, of each leaf's actual
    transfer intervals (not its first-to-last span, which would count a
    leaf idle in the middle as competing)."""
    if duration <= 0:
        return True
    common = None
    for leaf in leaves:
        iv = active([r for r in rows if r["client"] == leaf])
        if not iv:
            return False
        common = iv if common is None else intersect(common, iv)
    return sum(b - a for a, b in (common or [])) >= share * duration - 2 * OVERLAP_MIN


def dc_measures(t: Trial, loaded: List[str], control: str, request_timeout: float, duration: float = 0.0) -> Dict[str, float]:
    m: Dict[str, float] = {}
    sus = [r for r in t.rows if r["phase"] == "sustained" and r["kind"] == "bulk"]
    m["sim_rate"] = sum(leaf_rate([r for r in sus if r["client"] == leaf]) for leaf in loaded)
    # The same capping rule as the A/B analysis, leaf by leaf (flow names
    # repeat across leaves).
    m["capped"] = sum(capped_flows([r for r in sus if r["client"] == leaf], duration) for leaf in loaded)
    per_leaf = {leaf: float(sum(useful(r) for r in sus if r["client"] == leaf)) for leaf in loaded}
    m["aggregate_bytes"] = sum(per_leaf.values())
    m["worst_leaf_bytes"] = min(per_leaf.values()) if per_leaf else float("nan")
    m["jain"] = st.jain(list(per_leaf.values()))
    # The leaves' sustained transfers must have shared the window: a leaf
    # whose ssh session missed the barrier runs late and alone, and its
    # bytes are not service it got while competing.
    if not shared_window(sus, loaded, duration):
        for k in ("sim_rate", "aggregate_bytes", "worst_leaf_bytes", "jain"):
            m[k] = float("nan")
    # Request waves of downloads, or W4's one-at-a-time requests (dc-rpc,
    # request i of every leaf being wave i).
    req = [r for r in t.rows if r["phase"] == "requests" and r["kind"] in ("requests", "dc-rpc")]
    # An unfinished request is censored at its timeout rather than dropped:
    # dropping it would reward the arm that failed it.
    done = lambda r: float(r["seconds"]) if r["complete"] == "1" else float(request_timeout)
    # Each request wave must actually have been concurrent across the loaded
    # leaves, or the spread and latency do not describe contention.
    waves = sorted({r["flow"].split("r", 1)[0] for r in req})
    overlap = bool(waves)
    for w in waves:
        # Demonstrable overlap, on millisecond times: the intersection across
        # leaves of each leaf's actual request intervals (not its first-to-
        # last envelope, which would fill an idle gap) lasts OVERLAP_MIN.
        common = None
        for leaf in loaded:
            sp = [x for x in (span(r) for r in req if r["client"] == leaf and r["flow"].split("r", 1)[0] == w) if x]
            if not sp:
                common = []
                break
            iv = merge(sp, 0.0)
            common = iv if common is None else intersect(common, iv)
        if not common or sum(b - a for a, b in common) < OVERLAP_MIN - 1e-6:
            overlap = False
    m["waves_overlap"] = float(overlap)
    leaf_med = [st.median([done(r) for r in req if r["client"] == leaf]) for leaf in loaded]
    leaf_med = [x for x in leaf_med if x == x]
    m["spread"] = max(leaf_med) / min(leaf_med) if leaf_med and min(leaf_med) > 0 else float("nan")
    lat = [done(r) for r in req]
    # The requests themselves, for a p99 pooled over blocks when a block has
    # fewer than 100 per arm (design section 15.4).
    m["_lat"] = lat if overlap else []
    m["_ctl"] = ctl_samples = []
    m["latency_p50"] = st.quantile(lat, 0.5)
    m["latency_p99"] = st.quantile(lat, 0.99)
    if not overlap:
        m["spread"] = m["latency_p50"] = m["latency_p99"] = float("nan")
    # Only control requests that overlapped the loaded leaves' phase count.
    probes = []
    for ph in ("sustained", "requests"):
        load = [r for r in t.rows if r["phase"] == ph and r["client"] in loaded and r["kind"] != "probe"]
        probes += under_load([r for r in t.rows if r["client"] == control and r["kind"] == "probe" and r["phase"] == ph], load)
    ctl = [float(r["seconds"]) if r["complete"] == "1" else PROBE_TIMEOUT for r in probes]
    # The control leaf must have been observed under both phases' load; a
    # probe job that produced nothing leaves the trial unmeasured.
    phases = {r["phase"] for r in probes}
    m["control_p50"] = st.quantile(ctl, 0.5) if {"sustained", "requests"} <= phases else float("nan")
    if m["control_p50"] == m["control_p50"]:
        ctl_samples.extend(ctl)
    m["requests_ok"] = sum(1 for r in req if r["complete"] == "1")
    m["requests"] = len(req)
    # Completion: loaded leaves' requests and the control leaf's, together.
    # Latency substitutes a timeout for a failure, so it cannot show an arm
    # that fails everything; completion does.
    m["completion"] = ((m["requests_ok"] + sum(1 for r in probes if r["complete"] == "1")) / (len(req) + len(probes))
                       if req or probes else float("nan"))
    return m


# Percentile measures computed over an arm's pooled samples in a block:
# measure -> (sample list in Trial.m, quantile).
POOLED = {"latency_p50": ("_lat", 0.5), "latency_p99": ("_lat", 0.99), "control_p50": ("_ctl", 0.5)}


def pooled_block_stat(trials: List[Trial], key: str) -> float:
    """The arm's block value of a percentile measure: the quantile of its
    trials' samples pooled, NaN unless every trial measured it."""
    src, q = POOLED[key]
    if not trials or any(t.m.get(key) != t.m.get(key) for t in trials):
        return float("nan")
    return st.quantile([x for t in trials for x in t.m.get(src, [])], q)


def carrier_evidence(campaign: str, plan: dict, ab: List[Trial], solo: List[Trial], loaded: List[str],
                     control: str) -> Tuple[List[str], List[str]]:
    """Did each trial run on QUIC through the leaves the gate is about?

    From the gateway's flow records (path_egress / path_key, written under
    the campaign trace) and each client's fallback counter, per block:
    every solo calibration trial shows exactly one leaf key, the loaded
    leaves' keys are distinct (two hosts behind one NAT are one leaf), every
    simultaneous trial shows exactly those keys plus the control leaf's, all
    flows are QUIC under one common egress, and no client fell back to TCP.
    Returns (missing evidence, which withholds the verdict; contrary
    evidence, which voids the campaign)."""
    missing: List[str] = []
    wrong: List[str] = []
    records: Dict[Tuple[str, int], List[dict]] = {}

    def recs(arm: str, block: int) -> List[dict]:
        k = (arm, block)
        if k not in records:
            files = glob.glob(os.path.join(campaign, "logs", f"gw-{arm}", f"server-b{block:03d}-dc.jsonl*"))
            out = []
            for r in iter_json(files, ('"remote flow complete"', '"remote flow ended with error"')):
                try:
                    r["_t"] = ts(r["time"])
                except (KeyError, ValueError):
                    continue
                out.append(r)
            records[k] = out
        return records[k]

    def in_trial(t: Trial) -> List[dict]:
        return [r for r in recs(t.arm, t.block) if t.start - 1 <= r["_t"] <= t.end + 5]

    def fallbacks(t: Trial, leaf: str) -> float:
        name = f"{t.tag}-s{t.slot}-{t.arm}"
        a, b = metrics(campaign, f"{name}-pre-client-{leaf}"), metrics(campaign, f"{name}-post-client-{leaf}")
        return b.get("queqiao_fallbacks_total", float("nan")) - a.get("queqiao_fallbacks_total", float("nan"))

    def check(t: Trial, leaves: List[str], what: str) -> Optional[set]:
        rs = in_trial(t)
        if not rs:
            missing.append(f"{what}: no gateway record of its flows")
            return None
        # The carrier first: a TCP record is positive evidence against the
        # gate (and carries no leaf key by design).
        if any(tcp_evidence(r) for r in rs):
            wrong.append(f"{what}: some flows ran over TCP (an opening carrier or a lane that carried bytes), "
                         "not only QUIC")
            return None
        if any("path_key" not in r for r in rs):
            missing.append(f"{what}: no gateway record of its carrier and leaf (needs QUEQIAO_LANE_TRACE=1 and a build "
                           "that logs path_key)")
            return None
        for leaf in leaves:
            fb = fallbacks(t, leaf)
            if fb != fb:
                missing.append(f"{what}: no fallback counter from client {leaf}")
            elif fb > 0:
                wrong.append(f"{what}: client {leaf} fell back to TCP")
        egress = {r.get("path_egress") for r in rs}
        if not all(egress):
            # resolvePath gives an empty egress when the route lookup failed:
            # the common ancestor is then unknown, not verified.
            missing.append(f"{what}: a flow record has no egress, so the common ancestor is unknown")
            return None
        if len(egress) != 1:
            wrong.append(f"{what}: flows under {len(egress)} egresses, not one common ancestor")
        return {r["path_key"] for r in rs}

    egress_seen = set()
    # calibrate = first: block 1's solo runs name the leaves for every block;
    # each block's simultaneous trials are still checked against them.
    once = str(plan["dc"].get("calibrate")) == "first"
    first_keys: Dict[str, str] = {}
    for b in sorted({t.block for t in ab}):
        keys: Dict[str, str] = {}
        if once and b != 1 and not any(t.block == b for t in solo):
            keys = dict(first_keys)
        for t in [t for t in solo if t.block == b]:
            leaf = next((r["client"] for r in t.rows), "")
            got = check(t, [leaf], f"block {b} solo {leaf}")
            if got is None:
                continue
            if len(got) != 1:
                wrong.append(f"block {b} solo {leaf}: {len(got)} leaf keys at the gateway, expected 1")
            else:
                keys[leaf] = next(iter(got))
        egress_seen |= {r.get("path_egress") for t in solo if t.block == b for r in in_trial(t)}
        if b == 1:
            first_keys = dict(keys)
        if set(keys) != set(loaded):
            continue  # calibration problems are reported by the calibration check
        if len(set(keys.values())) != len(loaded):
            wrong.append(f"block {b}: the loaded leaves are not distinct at the gateway ({sorted(keys.values())}); "
                         "hosts behind one address are one leaf")
            continue
        want = set(keys.values())
        for t in [t for t in ab if t.block == b]:
            got = check(t, loaded + [control], f"block {b} slot {t.slot} ({t.arm})")
            if got is None:
                continue
            egress_seen |= {r.get("path_egress") for r in in_trial(t)}
            extra = got - want
            if not want <= got or len(extra) != 1:
                wrong.append(f"block {b} slot {t.slot} ({t.arm}): gateway saw leaves {sorted(got)}, expected the loaded "
                             f"leaves {sorted(want)} and one distinct control leaf")
    if len(egress_seen) > 1:
        wrong.append(f"leaves were under {len(egress_seen)} egresses, not one common ancestor")
    return sorted(set(missing)), sorted(set(wrong))


# The candidate's bundle: dc-long-haul runs the legacy bundle or this one and
# nothing in between (DESIGN-PATH-AUTHORITY.md section 11.3; the daemon's
# validator refuses the rest). classifier=phased, mem=owned and
# gateway-admission=owned may ride along; a client also needs topology=single.
DC_BUNDLE = ("cc-api=v2", "sched=class", "congestion=path", "coded-retry=sticky", "recovery=carrier")


def bundle_evidence(campaign: str, plan: dict, loaded: List[str], control: str) -> Tuple[List[str], List[str]]:
    """Each endpoint's bundle, from the runtime configuration it logged at
    every start: the main arm must have run the legacy bundle (no cc-api=v2)
    and every candidate the new-architecture bundle, or the campaign compared
    something else. Returns (missing, wrong)."""
    missing: List[str] = []
    wrong: List[str] = []
    for arm in plan["arms"]:
        candidate = arm != plan["main"]
        for who, d, need in [("gateway", f"gw-{arm}", DC_BUNDLE)] + \
                [(f"client {leaf}", f"cl-{leaf}-{arm}", DC_BUNDLE + ("topology=single",)) for leaf in loaded + [control]]:
            files = glob.glob(os.path.join(campaign, "logs", d, "*.jsonl*"))
            seen = {rec.get("bundle") for rec in iter_json(files, ('"runtime configuration"',))
                    if rec.get("msg") == "runtime configuration"} - {None}
            if not seen:
                missing.append(f"{who}/{arm}: no bundle recorded")
            for b in sorted(seen):
                parts = set(b.split(","))
                if candidate and not set(need) <= parts:
                    wrong.append(f"{who}/{arm}: ran bundle {b!r}, not dc-long-haul's new-architecture bundle "
                                 f"({','.join(need)})")
                elif not candidate and "cc-api=v2" in parts:
                    wrong.append(f"{who}/{arm}: the legacy arm ran bundle {b!r}")
    return missing, wrong


def evaluate(campaign: str, bounds: dict) -> Tuple[str, dict]:
    plan, trials = read_campaign(campaign)
    if plan.get("workload") != "dc" or not plan.get("dc"):
        raise SystemExit(f"{campaign} is not a dc campaign")
    problems, trials = completeness(campaign, plan, trials)
    frozen = plan.get("dc_bounds_sha256")
    if frozen and bounds.get("sha256") != frozen:
        # The bounds were frozen before the first block; another file (or a
        # changed one) does not judge this campaign.
        problems = problems + [f"the DC bounds differ from the ones frozen in the campaign's plan (sha256 {frozen[:12]}); "
                               "judge it with that dc_gate.ini"]
    dc = plan["dc"]
    loaded, control = dc["loaded"], dc["control"]
    main = plan["main"]
    cands = [a for a in plan["arms"] if a != main]
    ab = [t for t in trials.values() if t.slot < 100]
    solo = [t for t in trials.values() if t.slot >= 100]
    duration = float(plan.get("config", {}).get("duration") or 0)
    for t in ab:
        t.m = dc_measures(t, loaded, control, dc["request_timeout"], duration)
    evidence_missing, evidence_wrong = carrier_evidence(campaign, plan, ab, solo, loaded, control)
    bundle_missing, bundle_wrong = bundle_evidence(campaign, plan, loaded, control)
    evidence_missing, evidence_wrong = evidence_missing + bundle_missing, bundle_wrong + evidence_wrong

    def per_block(arm: str, key: str, agg) -> Dict[int, float]:
        out: Dict[int, List[float]] = {}
        trials_of: Dict[int, List[Trial]] = {}
        for t in ab:
            if t.arm == arm:
                out.setdefault(t.block, []).append(t.m[key])
                trials_of.setdefault(t.block, []).append(t)
        if key in POOLED:
            # A percentile of the arm's block is taken over the block's pooled
            # samples (design section 15.4), not averaged over its trials.
            return {b: pooled_block_stat(ts_, key) for b, ts_ in trials_of.items()}
        return {b: agg(v) for b, v in out.items()}

    # Strict: a block with an unmeasured trial is unmeasured, not averaged
    # over whatever is left.
    strict = lambda f: (lambda v: f(v) if v and all(x == x for x in v) else float("nan"))
    total = strict(lambda v: float(sum(v)))
    avg = strict(lambda v: sum(v) / len(v))

    # Validity: did the shared ancestor bind? Legacy's simultaneous rate per
    # block against the sum of the leaves' solo rates in the same block.
    # Every judged block needs its own calibration: a usable solo rate for
    # every loaded leaf and a measured simultaneous rate.
    binding: List[float] = []
    uncalibrated: List[int] = []
    # calibrate = first (the minimal profile): block 1's solo rates stand for
    # every block.
    once = str(dc.get("calibrate")) == "first"
    for b in sorted({t.block for t in ab}):
        rates = {}
        for t in solo:
            if t.block == (1 if once else b):
                leaf = next((r["client"] for r in t.rows), None)
                bulk = [r for r in t.rows if r["kind"] == "bulk"]
                # A usable calibration ran backlogged for the window: a short
                # burst that then failed would inflate the solo rate.
                sustained = shared_window(bulk, [leaf], duration) and capped_flows(bulk, duration) == 0
                rates[leaf] = leaf_rate(bulk) if sustained else float("nan")
        sim = [t.m["sim_rate"] for t in ab if t.block == b and t.arm == main]
        ok = (set(loaded) <= set(rates) and all(rates[x] == rates[x] and rates[x] > 0 for x in loaded)
              and sim and all(x == x for x in sim))
        if not ok:
            uncalibrated.append(b)
            continue
        binding.append(st.mean(sim) / sum(rates[x] for x in loaded))
    if uncalibrated:
        evidence_missing = evidence_missing + [f"{len(uncalibrated)} block(s) lack a usable solo calibration of every "
                                               f"loaded leaf (first: block {uncalibrated[0]})"]
    binding_mean = st.mean(binding)
    # VOID only for a measured ratio over the bound; no ratio at all (no
    # usable calibration, or calibrate = false) is missing evidence.
    bound_ok = not (binding_mean == binding_mean) or binding_mean <= bounds["binding_max"]
    if not binding:
        evidence_missing = evidence_missing + ["no usable solo calibration: the ancestor's binding was not measured "
                                               "(set [dc] calibrate = true)"]
    # The sustained outcome is bytes within the window; if a flow ran out of
    # bytes first, the arms are capped at the object size and look alike.
    capped = sum(int(t.m["capped"]) for t in ab)

    # Every endpoint of every arm must have run dc-long-haul, checked one
    # endpoint at a time. Clients log their path profile; the gateway does
    # not, so its evidence is its running command line, recorded by the
    # runner at every start.
    for arm in plan["arms"]:
        try:
            with open(os.path.join(campaign, "effective", f"gw-{arm}.args")) as f:
                gw = [line for line in f if line.strip()]
        except OSError:
            gw = []
        if not gw or not all("--path-profile dc-long-haul" in line for line in gw):
            problems = problems + [f"gateway {arm}: no evidence it ran --path-profile dc-long-haul"]
        for leaf in loaded + [control]:
            files = glob.glob(os.path.join(campaign, "logs", f"cl-{leaf}-{arm}", "*.jsonl*"))
            seen = {rec.get("path_profile", "") for rec in iter_json(files, ('"runtime configuration"',))
                    if rec.get("msg") == "runtime configuration"}
            if seen != {"dc-long-haul"}:
                problems = problems + [f"client {leaf}/{arm}: ran path profile(s) {sorted(seen) or 'unrecorded'}, "
                                       "not only dc-long-haul"]
    a, r, s = bounds["alpha"], bounds["resamples"], bounds["seed"]
    # Section 15.4's void rule, on the sustained outcome: a campaign in which
    # too many legacy blocks delivered nothing is rerun, not judged.
    legacy_sus = per_block(main, "aggregate_bytes", total)
    zero_frac = (sum(1 for v in legacy_sus.values() if v == 0) / len(legacy_sus)) if legacy_sus else float("nan")
    zero_void = not (zero_frac <= bounds["max_zero_main_fraction"])
    legacy_completion = st.mean([t.m["completion"] for t in ab if t.arm == main])
    workload_broken = not (legacy_completion >= bounds["min_legacy_completion"])
    # Order: what the traffic actually was (carrier, leaves), whether it was
    # backlogged, then whether the workload worked and bound the ancestor;
    # a later reason is meaningless when an earlier one holds.
    void_reason = ("; ".join(evidence_wrong) if evidence_wrong else
                   f"{capped} sustained flow(s) ran out of bytes before the window closed; raise object_bytes"
                   if capped else
                   "more than the pre-registered fraction of legacy blocks delivered nothing" if zero_void else
                   f"the legacy arm completed only {legacy_completion:.0%} of requests; the workload does not work"
                   if workload_broken else
                   "the shared ancestor did not bind; add load" if not bound_ok else "")
    criteria = [
        # name, key, per-block aggregation, contrast builder (cand, legacy) -> d
        ("sustained goodput (aggregate bytes)", "aggregate_bytes", total,
         lambda c, l: c - bounds["goodput_ratio"] * l, f"cand - {bounds['goodput_ratio']:g} x legacy"),
        ("request completion", "completion", avg,
         lambda c, l: c - (l - bounds["completion_margin"]), f"cand - (legacy - {bounds['completion_margin']:g})"),
        ("request latency p50", "latency_p50", avg,
         lambda c, l: bounds["latency_max_ratio"] * l - c, f"{bounds['latency_max_ratio']:g} x legacy - cand"),
        ("request latency p99", "latency_p99", avg,
         lambda c, l: bounds["latency_max_ratio"] * l - c, f"{bounds['latency_max_ratio']:g} x legacy - cand"),
        ("worst leaf's service (bytes)", "worst_leaf_bytes", total,
         lambda c, l: c - bounds["worst_leaf_ratio"] * l, f"cand - {bounds['worst_leaf_ratio']:g} x legacy"),
        ("completion spread", "spread", avg,
         lambda c, l: bounds["spread_max_ratio"] * l - c, f"{bounds['spread_max_ratio']:g} x legacy - cand"),
        ("completion spread ceiling", "spread", avg,
         lambda c, l: bounds["spread_ceiling"] - c, f"{bounds['spread_ceiling']:g} - cand"),
        ("cross-leaf fairness (Jain)", "jain", avg,
         lambda c, l: c - (l - bounds["jain_margin"]), f"cand - (legacy - {bounds['jain_margin']:g})"),
        ("control leaf request p50", "control_p50", avg,
         lambda c, l: bounds["control_max_ratio"] * l - c, f"{bounds['control_max_ratio']:g} x legacy - cand"),
    ]
    name = os.path.basename(os.path.normpath(campaign))
    out = [f"## DC gate (stage 9): {name}", "",
           "Bounds frozen in `scripts/validation/dc_gate.ini` (DESIGN-PATH-AUTHORITY.md section 11.3). Each row is a "
           f"paired per-block contrast that must be positive: one-sided {1 - a:.0%} t and bootstrap lower bounds both above zero.",
           "", f"Loaded leaves: {', '.join(loaded)}; control leaf: {control}.",
           f"Ancestor binding (legacy simultaneous rate / sum of solo rates, mean over {len(binding)} blocks): "
           f"{fmt(binding_mean, '.2f')} (must be <= {bounds['binding_max']:g})"
           + ("" if bound_ok else " -- **the shared ancestor did not bind: campaign VOID**"), ""]
    if problems:
        out += ["**No verdict: the campaign is incomplete.** " + "; ".join(problems) + ".", ""]
    arms = [main] + cands
    out += ["| per block, mean | " + " | ".join(arms) + " |", "|---|" + "---:|" * len(arms)]
    for label, key, agg, scale, spec in (("aggregate sustained MB", "aggregate_bytes", total, 1e-6, ".1f"),
                                         ("worst leaf MB", "worst_leaf_bytes", total, 1e-6, ".1f"),
                                         ("Jain index", "jain", avg, 1, ".3f"),
                                         ("completion spread", "spread", avg, 1, ".2f"),
                                         ("request p50, s", "latency_p50", avg, 1, ".3f"),
                                         ("request p99, s", "latency_p99", avg, 1, ".3f"),
                                         ("control p50, s", "control_p50", avg, 1, ".3f")):
        out.append(f"| {label} | " + " | ".join(fmt(st.mean(list(per_block(x, key, agg).values())) * scale, spec) for x in arms) + " |")
    out.append("| requests completed | " + " | ".join(
        f"{sum(int(t.m['requests_ok']) for t in ab if t.arm == x)}/{sum(int(t.m['requests']) for t in ab if t.arm == x)}" for x in arms) + " |")
    verdicts: dict = {}
    for cand in cands:
        out += ["", f"**{cand} against {main}**", "",
                "| criterion | contrast | blocks | mean | t lower bound | bootstrap lower bound | verdict |",
                "|---|---|---:|---:|---:|---:|---|"]
        all_pass, unmeasured = True, []
        for label, key, agg, build, desc in criteria:
            cb, lb = per_block(cand, key, agg), per_block(main, key, agg)
            common = sorted(set(cb) & set(lb))
            d = [build(cb[b], lb[b]) for b in common if cb[b] == cb[b] and lb[b] == lb[b]]
            mean_d, tl, bl, ok = st.lower_bounds(d, a, int(r), int(s))
            if key == "latency_p99" and len(d) == len(common) and len(d) >= 2:
                samples = lambda arm, b: [x for t in ab if t.arm == arm and t.block == b for x in t.m["_lat"]]
                cs_, ms_ = [samples(cand, b) for b in common], [samples(main, b) for b in common]
                if min(len(x) for x in cs_ + ms_) < 100:
                    # Too few requests per arm in a block for a per-block p99:
                    # the pooled p99, tested by resampling blocks.
                    mean_d, lo_, _ = st.pooled_quantile_bounds(cs_, ms_, 0.99, build, a, int(r), int(s))
                    tl, bl, ok = float("nan"), lo_, lo_ > 0
                    label = "request latency p99 (pooled over blocks)"
            if len(d) < len(common) or len(d) < 2:
                # Every judged block must carry the criterion's evidence.
                unmeasured.append(f"{label}: {len(d)} of {len(common)} blocks measured")
                cell = "n/a"
            else:
                all_pass &= ok
                cell = "pass" if ok else "**FAIL**"
            out.append(f"| {label} | `{desc}` | {len(d)} | {fmt(mean_d, '.4g')} | {fmt(tl, '.4g')} | {fmt(bl, '.4g')} | {cell} |")
        # An unfinished campaign has no verdict; a finished one that did not
        # test the regime is void; then every criterion must be measured.
        # An unfinished campaign has no verdict. Positive evidence that the
        # traffic was not what the gate is about voids it even where other
        # evidence is missing. Then every criterion needs its evidence.
        why = problems + ([] if void_reason else evidence_missing + unmeasured)
        verdict = ("INCOMPLETE" if problems else
                   "VOID" if void_reason else
                   "INCOMPLETE" if evidence_missing or unmeasured else
                   "PASS" if all_pass else "FAIL")
        verdicts[cand] = verdict
        out += ["", f"**Verdict for {cand}: {verdict}.** "
                + {"PASS": "The profile may move to the new architecture and the legacy bundle may be deleted (section 11.3).",
                   "FAIL": "The legacy bundle stays for dc-long-haul; joint limiting waits for the tree-arbiter design (Q2).",
                   "VOID": f"Rerun: {void_reason}.",
                   "INCOMPLETE": "No verdict: " + "; ".join(why) + "."}[verdict]]
    return "\n".join(out) + "\n", verdicts


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("campaign")
    ap.add_argument("--bounds", default=BOUNDS)
    args = ap.parse_args(argv)
    text, verdicts = evaluate(args.campaign, load_bounds(args.bounds))
    sys.stdout.write(text)
    with open(os.path.join(args.campaign, "dc_gate.md"), "w") as f:
        f.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())

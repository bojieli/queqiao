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
import os
import sys
from typing import Dict, List, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ab_stats as st  # noqa: E402
from ab_analyze import (PROBE_TIMEOUT, Trial, completeness, fmt, iter_json, read_campaign,  # noqa: E402
                        span, under_load, useful)

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
    return out


def leaf_rate(rows: List[dict]) -> float:
    """Bytes per second while the leaf was loaded: its bytes over the longest
    of its flows. A rate, not a byte total, so that objects finishing inside
    the window cannot make a shared bottleneck look unshared."""
    secs = max((float(r["seconds"]) for r in rows), default=0.0)
    return sum(useful(r) for r in rows) / secs if secs > 0 else float("nan")


def dc_measures(t: Trial, loaded: List[str], control: str, request_timeout: float) -> Dict[str, float]:
    m: Dict[str, float] = {}
    sus = [r for r in t.rows if r["phase"] == "sustained" and r["kind"] == "bulk"]
    m["sim_rate"] = sum(leaf_rate([r for r in sus if r["client"] == leaf]) for leaf in loaded)
    m["sus_flows"] = len(sus)
    m["sus_finished"] = sum(1 for r in sus if r["complete"] == "1")
    per_leaf = {leaf: float(sum(useful(r) for r in sus if r["client"] == leaf)) for leaf in loaded}
    m["aggregate_bytes"] = sum(per_leaf.values())
    m["worst_leaf_bytes"] = min(per_leaf.values()) if per_leaf else float("nan")
    m["jain"] = st.jain(list(per_leaf.values()))
    req = [r for r in t.rows if r["phase"] == "requests" and r["kind"] == "requests"]
    # An unfinished request is censored at its timeout rather than dropped:
    # dropping it would reward the arm that failed it.
    done = lambda r: float(r["seconds"]) if r["complete"] == "1" else float(request_timeout)
    # Each request wave must actually have been concurrent across the loaded
    # leaves, or the spread and latency do not describe contention.
    waves = sorted({r["flow"].split("r", 1)[0] for r in req})
    overlap = bool(waves)
    for w in waves:
        iv = []
        for leaf in loaded:
            sp = [x for x in (span(r) for r in req if r["client"] == leaf and r["flow"].split("r", 1)[0] == w) if x]
            if not sp:
                overlap = False
                break
            iv.append((min(a for a, _ in sp), max(b for _, b in sp)))
        if overlap and not max(a for a, _ in iv) <= min(b for _, b in iv) + 1.0:
            overlap = False
    m["waves_overlap"] = float(overlap)
    leaf_med = [st.median([done(r) for r in req if r["client"] == leaf]) for leaf in loaded]
    leaf_med = [x for x in leaf_med if x == x]
    m["spread"] = max(leaf_med) / min(leaf_med) if leaf_med and min(leaf_med) > 0 else float("nan")
    lat = [done(r) for r in req]
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
    m["requests_ok"] = sum(1 for r in req if r["complete"] == "1")
    m["requests"] = len(req)
    # Completion: loaded leaves' requests and the control leaf's, together.
    # Latency substitutes a timeout for a failure, so it cannot show an arm
    # that fails everything; completion does.
    m["completion"] = ((m["requests_ok"] + sum(1 for r in probes if r["complete"] == "1")) / (len(req) + len(probes))
                       if req or probes else float("nan"))
    return m


def evaluate(campaign: str, bounds: dict) -> Tuple[str, dict]:
    plan, trials = read_campaign(campaign)
    if plan.get("workload") != "dc" or not plan.get("dc"):
        raise SystemExit(f"{campaign} is not a dc campaign")
    problems, trials = completeness(campaign, plan, trials)
    dc = plan["dc"]
    loaded, control = dc["loaded"], dc["control"]
    main = plan["main"]
    cands = [a for a in plan["arms"] if a != main]
    ab = [t for t in trials.values() if t.slot < 100]
    solo = [t for t in trials.values() if t.slot >= 100]
    for t in ab:
        t.m = dc_measures(t, loaded, control, dc["request_timeout"])

    def per_block(arm: str, key: str, agg) -> Dict[int, float]:
        out: Dict[int, List[float]] = {}
        for t in ab:
            if t.arm == arm:
                out.setdefault(t.block, []).append(t.m[key])
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
    for b in sorted({t.block for t in ab}):
        rates = {}
        for t in solo:
            if t.block == b:
                leaf = next((r["client"] for r in t.rows), None)
                rates[leaf] = leaf_rate([r for r in t.rows if r["kind"] == "bulk"])
        sim = [t.m["sim_rate"] for t in ab if t.block == b and t.arm == main]
        ok = (set(loaded) <= set(rates) and all(rates[x] == rates[x] and rates[x] > 0 for x in loaded)
              and sim and all(x == x for x in sim))
        if not ok:
            uncalibrated.append(b)
            continue
        binding.append(st.mean(sim) / sum(rates[x] for x in loaded))
    if uncalibrated:
        problems = problems + [f"{len(uncalibrated)} block(s) lack a usable solo calibration of every loaded leaf "
                               f"(first: block {uncalibrated[0]})"]
    binding_mean = st.mean(binding)
    bound_ok = binding_mean == binding_mean and binding_mean <= bounds["binding_max"]
    # The sustained outcome is bytes within the window; if the objects ran
    # out first, both arms are capped at the object size and look alike.
    flows = sum(t.m["sus_flows"] for t in ab)
    finished = sum(t.m["sus_finished"] for t in ab) / flows if flows else float("nan")
    backlogged = finished == finished and finished <= 0.10

    # Every endpoint of every arm must have run dc-long-haul, checked one
    # endpoint at a time. Clients log their path profile; the gateway does
    # not, so its evidence is its running command line, recorded by the
    # runner at every start.
    import glob
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
    void_reason = ("more than the pre-registered fraction of legacy blocks delivered nothing" if zero_void else
                   f"the legacy arm completed only {legacy_completion:.0%} of requests; the workload does not work"
                   if workload_broken else
                   "the shared ancestor did not bind; add load" if not bound_ok else
                   f"{finished:.0%} of sustained flows finished their object inside the window; raise object_bytes"
                   if not backlogged else "")
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
        why = problems + ([] if void_reason else unmeasured)
        verdict = ("INCOMPLETE" if problems else
                   "VOID" if void_reason else
                   "INCOMPLETE" if unmeasured else
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

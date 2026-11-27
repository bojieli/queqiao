"""Reliability under slow, lossy and broken networks (docs/VALIDATING-A-BUNDLE.md,
"Impaired networks"): the mixed foreground under the frozen scenarios of
ab_impair, applied on the path's client host ([impair]).

Trial types, each one scenario, both (all) arms under the identical
impairment, one trial per arm per block in random order:

* `slow-lossy` (90 s): 5 Mbit/s, +250 ms RTT, 20 ms jitter, bursty loss ~3 %.
* `blackhole` (120 s): 20 Mbit/s, +150 ms RTT, 100 % drop for 8 s at 30 s and
  for 25 s at 70 s; a long upload beside the foreground, and probes every
  0.25 s that time the recovery.
* `policed` (90 s): a 4 Mbit/s token-bucket policer, +150 ms RTT.
* `idle-wake-lossy` (D): the minimal plan's idle-wake session (wakes at 1, 5
  and 16 minutes) beside the blocks, on its own ports under 2 % loss and
  +200 ms RTT; judged as the campaign in `<campaign>/idle-wake` by
  [workload:idle-wake].

Each trial: the LLM swarm (4 x 10 turns), W6 keystrokes without bursts, and 30
short downloads of 1 MiB across the window.

Gated on reliability, each pre-registered and one-sided ([workload:impaired]
in gates.ini, plus the [secondary] bounds of flow completion and stalls):
flow completion, stalls per trial, recovery after a blackhole, LLM call
failures and TTFT p99, keystroke echo p99, and silent deaths (none). Goodput
is reported, not gated.
"""
from __future__ import annotations

import os
import re
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple

import ab_campaign as ab
import ab_impair
import ab_stats as st
import ab_workloads as w


def _sibling(mod: str, file: str):
    m = sys.modules.get(mod)
    if m is None:                       # loaded before its sibling (sorted order): import it
        import importlib.util as _u
        spec = _u.spec_from_file_location(mod, os.path.join(os.path.dirname(__file__), file))
        m = _u.module_from_spec(spec)
        sys.modules[mod] = m
        spec.loader.exec_module(m)
    return m


pa = _sibling("ab_workload_phase_a", "phase_a.py")
mn = _sibling("ab_workload_minimal", "minimal.py")

TRIAL_SECONDS = {"slow-lossy": 120, "blackhole": 120, "policed": 120,
                 # DIAGNOSTIC ONLY (branch diag/credit-base, never merged):
                 # blackhole's base path and workload without its outages.
                 "blackhole-base": 120}
TRIAL_TYPES = tuple(TRIAL_SECONDS)
WAKE_SCENARIO = "idle-wake-lossy"
LEAD = 5                    # the foreground starts this long after the download
BULK_OBJECT = 1 << 20       # each short download's object
FETCHES = 30                # short downloads per trial, sequential, one started every window/30 (or after the last)
FETCH_TIMEOUT = 30          # a download not done in this long failed (a 25 s outage fits inside)
# The LLM swarm: AGENTS concurrent loops of TURNS turns, short responses, so
# a trial has AGENTS x TURNS calls: enough opportunities for the pooled
# call-failure rate over a 4-to-6-block campaign (gates.ini `events`).
# Per-trial offsets, drawn from the frozen seed independently per block, slot
# and arm, so an LLM turn boundary does not fall at the same point of an
# outage in every trial: the LLM swarm starts LEAD + U(0, LLM_JITTER) s in,
# the outages at their nominal times + U(0, OUTAGE_JITTER[k]) s (the last
# outage still ends at least 20 s before the trial does).
LLM_JITTER = 6.0
OUTAGE_JITTER = (10.0, 5.0)
# A call is clear of the outages when it neither starts nor runs within an
# outage or the CLEAR_AFTER seconds after it ends.
CLEAR_AFTER = 5.0
# Background transfers in flight across the outages (blackhole): a download
# and an upload, each object after object, a failed object followed by the
# next one (each failure recorded), with a socket timeout per scenario well
# above its longest scripted outage (25 s), so a transfer fails only when
# the tunnel does not resume, not because the outage outlasted the client.
BG_DOWN_OBJECT = 4 << 20
BG_UP_OBJECT = 2 << 20
BG_IO_TIMEOUT = {"blackhole": 60.0}
AGENTS = 4
TURNS = 10
DELTAS = 100
PROBE_BYTES = 1024
PROBE_EVERY = 0.25          # seconds between probes, each on its own (open loop)
PROBE_TIMEOUT = 10
# A failure that is a client-side timeout: the tunnel neither delivered nor
# ended the flow (a silent death).
SILENT_RE = re.compile(r"time(d)?\s*out|timeout", re.I)


# --- generators ----------------------------------------------------------------------

def probe_script(campaign, job, arm, duration, start_at=0):
    """A small fetch every PROBE_EVERY seconds, each in its own process
    (open loop: a stuck probe does not delay the next), until the window
    closes or the stop file appears."""
    url, curl = w._curl(campaign, job, arm)
    curl = curl.replace("%{exitcode}'", "%{exitcode}\\t%{time_starttransfer}'")
    stop = campaign.stop_file(job.client, arm)
    # Each probe: a row, and a record with its first byte (for the time to
    # first byte of a connection opened after an outage).
    return (ab.NOW_SH + f"end=$(( $(date +%s) + {int(duration)} )); i=0; "
            f"while [ $(date +%s) -lt $end ] && [ ! -e {stop} ]; do i=$((i+1)); "
            f"( t0=$(now); r=$({curl} --max-time {PROBE_TIMEOUT} {url}obj-{PROBE_BYTES} 2>/dev/null); t1=$(now); "
            f"{w._ROW} \"p$i\" \"$r\"; IFS=$'\\t' read -r h_ s_ x_ e_ fb_ <<< \"$r\"; "
            f"printf 'J\\t{{\"kind\":\"probe\",\"t0\":%s,\"ttfb\":%s,\"http\":\"%s\",\"exit\":\"%s\"}}\\n' "
            f"\"$t0\" \"${{fb_:-0}}\" \"$h_\" \"$e_\" ) & sleep {PROBE_EVERY}; done; wait")


def fetch_script(campaign, job, arm, duration, start_at=0):
    """FETCHES downloads of one BULK_OBJECT each, one after the other, the
    k-th starting at k x window/FETCHES (or when the one before ends), each
    allowed FETCH_TIMEOUT s or what is left of the window. A row per
    download, and a record saying whether the window's end cut it (a cut
    download was not a fair attempt). The job lasts the window: it is the
    trial's load."""
    url, curl = w._curl(campaign, job, arm)
    d = float(duration)
    return (ab.NOW_SH + f"s0=$(now); end=$(awk -v s=$s0 'BEGIN{{printf \"%.3f\", s + {d}}}'); "
            f"for k in $(seq 1 {FETCHES}); do "
            f"sleep $(awk -v s=$s0 -v n=$(now) -v k=$k 'BEGIN{{x = s + (k - 1) * {d / FETCHES} - n; if (x < 0) x = 0; printf \"%.3f\", x}}'); "
            f"rem=$(awk -v e=$end -v n=$(now) 'BEGIN{{r = e - n; printf \"%.3f\", r}}'); "
            f"awk -v r=$rem 'BEGIN{{exit !(r < 0.5)}}' && break; "
            f"mt=$(awk -v r=$rem 'BEGIN{{if (r < {FETCH_TIMEOUT}) printf \"%.3f\", r; else print {FETCH_TIMEOUT}}}'); "
            f"cut=$(awk -v r=$rem 'BEGIN{{print (r < {FETCH_TIMEOUT}) ? 1 : 0}}'); "
            f"t0=$(now); r=$({curl} --max-time $mt {url}obj-{BULK_OBJECT} 2>/dev/null); t1=$(now); "
            f"{w._ROW} \"w1r$k\" \"$r\"; "
            f"printf 'J\\t{{\"kind\":\"fetch\",\"k\":%s,\"window_cut\":%s}}\\n' $k $cut; done; "
            f"sleep $(awk -v e=$end -v n=$(now) 'BEGIN{{x = e - n; if (x < 0) x = 0; printf \"%.3f\", x}}')")


def fetch_rows(out, job, duration):
    """The downloads that started: a row each (no padding: one the window
    never reached was not attempted)."""
    rows = []
    for line in out.splitlines():
        p_ = line.split("\t")
        if len(p_) >= 7 and p_[0] != "J" and ab.FLOW_RE.match(p_[2]):
            t0, t1, flow, code, size, secs, ex = p_[:7]
            rows.append((flow, code if code.isdigit() else "000", size if size.isdigit() else "0", secs or "0",
                         ex if ex.lstrip("-").isdigit() else "-1", t0, t1))
    return rows


def outage_script(campaign, job, arm, duration, start_at=0):
    return campaign.impair.outage_script(job.params["scenario"], job.params.get("outages"))


w.generator("imp-probe", w.Generator(probe_script, lambda out, job, d: ab.job_rows(out, "probe", 1, 1, d),
                                     lambda job: PROBE_BYTES, w.json_records))
w.generator("imp-fetch", w.Generator(fetch_script, fetch_rows, lambda job: BULK_OBJECT, w.json_records))
w.generator("impair-outages", w.Generator(outage_script, lambda out, job, d: [], lambda job: 0, w.json_records,
                                          "impair", harness=True))


# --- measures --------------------------------------------------------------------------

def recoveries(outages: Sequence[Sequence[float]], probes: Sequence[dict], trial_end: float) -> List[float]:
    """Per outage: seconds from its end to the first probe that completed
    after it (HTTP 200, curl exit 0). None completed before the next outage
    (or the trial's end): censored there."""
    ok = sorted(float(r["t_end"]) for r in probes
                if r.get("http") == "200" and r.get("curl_exit") == "0" and r.get("t_end"))
    out = []
    for k, (_, end) in enumerate(outages):
        nxt = outages[k + 1][0] if k + 1 < len(outages) else trial_end
        first = next((t for t in ok if t > end), None)
        out.append(min(first, nxt) - end if first is not None else nxt - end)
    return out


def trial_end(outages, recs) -> float:
    """The trial's end on the job host's clock, bounded from above by the
    last probe's start plus its timeout, and at least the last outage's end
    plus 25 s (the trial's tail after it)."""
    starts = [float(r["t0"]) for r in recs if r.get("kind") == "probe" and r.get("t0") is not None]
    return max([outages[-1][1] + 25.0] + ([max(starts) + PROBE_TIMEOUT] if starts else []))


def reopen_ttfb(outages, recs, end) -> List[float]:
    """Per outage: seconds from its end to the first byte of the first probe
    (a new connection) opened at or after it that got one (HTTP 200);
    censored at the next outage or the trial's end."""
    probes = [r for r in recs if r.get("kind") == "probe" and r.get("http") == "200"]
    out = []
    for k, (_, e) in enumerate(outages):
        nxt = outages[k + 1][0] if k + 1 < len(outages) else end
        fb = [float(r["t0"]) + float(r.get("ttfb") or 0) for r in probes if float(r["t0"]) >= e]
        out.append(min([min(fb), nxt]) - e if fb else nxt - e)
    return out


def inflight_ttfb(outages, recs, end) -> List[float]:
    """Per outage: seconds from its end until every flow in flight across it
    (the background download and upload, by their progress timelines) moved
    a byte again -- the worst flow; censored at the next outage or the
    trial's end."""
    flows = [r["samples"] for r in recs if r.get("kind") == "bgprog" and r.get("samples")]
    out = []
    for k, (s, e) in enumerate(outages):
        nxt = outages[k + 1][0] if k + 1 < len(outages) else end
        worst = 0.0
        for smp in flows:
            smp = [(float(a), float(b)) for a, b in smp]
            if not smp or smp[0][0] > s or smp[-1][0] < e:
                continue                     # not in flight across this outage
            at_end = max((b for a, b in smp if a <= e), default=0.0)
            moved = next((a for a, b in smp if a > e and b > at_end), None)
            worst = max(worst, (min(moved, nxt) if moved is not None else nxt) - e)
        out.append(worst if flows else float("nan"))
    return out


def _outages(trial, ph):
    ev = next((r for r in getattr(trial, "records", []) if r.get("_phase") == ph and r.get("kind") == "impair"
               and not r.get("fail")), None)
    return (ev or {}).get("outages") or []


def clear_calls(trials, ph) -> List[dict]:
    """Completed LLM calls clear of every outage: neither starting nor
    running within an outage or the CLEAR_AFTER s after it."""
    out = []
    for t in trials:
        outs = _outages(t, ph)
        for c in getattr(t, "records", []):
            if c.get("_phase") != ph or c.get("kind") != "llm" or c.get("fail") or c.get("ttft") is None:
                continue
            a, b = float(c["t0"]), float(c.get("t1") or c["t0"])
            if not any(a < e + CLEAR_AFTER and b > s for s, e in outs):
                out.append(c)
    return out


def after_restore(outages, recs, end) -> List[float]:
    """Per outage: of the LLM calls running across it (started before its
    end, not finished before its start), the latest first token after the
    restore, in seconds from the restore; a call that got no token is
    censored at its end (or the next outage, or the trial's end). An outage
    no call ran across: 0."""
    calls = [c for c in recs if c.get("kind") == "llm" and c.get("t0") is not None]
    out = []
    for k, (s, e) in enumerate(outages):
        nxt = outages[k + 1][0] if k + 1 < len(outages) else end
        worst = 0.0
        for c in calls:
            a, b = float(c["t0"]), float(c.get("t1") or c["t0"])
            if not (a < e and b > s):
                continue
            first = (a + float(c.get("connect") or 0) + float(c["ttft"])) if c.get("ttft") is not None else None
            if first is not None and first <= s:
                continue                      # its first token came before the outage
            t_ = first if first is not None and not c.get("fail") else min(b, nxt)
            worst = max(worst, max(0.0, min(t_, nxt) - e))
        out.append(worst)
    return out


def window_end(trial, ph) -> Optional[float]:
    """The trial window's end on the client host's clock: the first short
    download starts with the window, which lasts TRIAL_SECONDS[ph]."""
    starts = [float(r["t_start"]) for r in trial.rows
              if r["phase"] == ph and r["kind"] == "imp-fetch" and r.get("flow") == "w1r1" and r.get("t_start")]
    return min(starts) + TRIAL_SECONDS[ph] if starts else None


def echo_samples(trial, ph) -> List[float]:
    """Keystroke echo times, a broken session included: a keystroke the
    session did not echo (wlclient lists them last, `unanswered` of them,
    censored at the session's end) is censored instead at the trial
    window's end -- its echo time is from when it was sent to then -- so a
    session that broke counts against its arm rather than leaving the
    measure unmeasured. Adopted before imp3's first look, outcome-blind."""
    out: List[float] = []
    end = window_end(trial, ph)
    for s in getattr(trial, "records", []):
        if s.get("_phase") != ph or s.get("kind") != "term":
            continue
        keys = s.get("keys") or []
        n_open = int(s.get("unanswered") or 0)
        answered = keys[:len(keys) - n_open] if n_open else keys
        out += [float(e) for _, e in answered]
        if n_open and end is not None and s.get("t0") is not None:
            # Key send times are seconds from the session's start (t0).
            out += [max(float(e), end - float(s["t0"]) - float(sent)) for sent, e in keys[len(keys) - n_open:]]
        elif n_open:
            out += [float(e) for _, e in keys[len(keys) - n_open:]]
    return out


def resume_by_kind(outages, recs, end, direction) -> List[Optional[float]]:
    """Per outage: seconds from its end until the background flow of this
    direction ("down", "up") moved a byte again, if that flow was alive when
    the outage began (its timeline started before it and had not ended);
    None when it was not. One that never moved again before the next outage
    or the trial's end is censored there."""
    flows = [[(float(a), float(b)) for a, b in r["samples"]] for r in recs
             if r.get("kind") == "bgprog" and r.get("dir") == direction and r.get("samples")]
    out: List[Optional[float]] = []
    for k, (s, e) in enumerate(outages):
        nxt = outages[k + 1][0] if k + 1 < len(outages) else end
        worst: Optional[float] = None
        for smp in flows:
            if smp[0][0] > s or smp[-1][0] < s:
                continue                         # not alive when the outage began
            at_end = max((b for a, b in smp if a <= e), default=0.0)
            moved = next((a for a, b in smp if a > e and b > at_end), None)
            x = (min(moved, nxt) if moved is not None else nxt) - e
            worst = x if worst is None else max(worst, x)
        out.append(worst)
    return out


def never_resumed(outages, recs, end) -> int:
    """Background flows alive at an outage's start that never moved a byte
    again before the next outage or the trial's end (each a failure)."""
    n = 0
    for direction in ("down", "up"):
        for k, x in enumerate(resume_by_kind(outages, recs, end, direction)):
            nxt = outages[k + 1][0] if k + 1 < len(outages) else end
            if x is not None and x >= nxt - outages[k][1] - 1e-9:
                n += 1
    return n


def bg_transfers(trial, ph) -> Tuple[int, int]:
    """(failed, attempted) background transfers in a trial: every object a
    background download or upload attempted, the one the window's end cut
    left out; a failure is a send or receive error or socket timeout, or a
    non-200 answer. Each flow that never resumed after an outage adds a
    failure (and an attempt)."""
    rows = [r for r in trial.rows if r["phase"] == ph and r["kind"] in ("wl-bgdown", "wl-bgup")]
    tried = [r for r in rows if r.get("curl_exit") != "28"]
    failed = sum(1 for r in tried if r.get("curl_exit") != "0" or r.get("http") != "200")
    ev = next((r for r in getattr(trial, "records", []) if r.get("_phase") == ph and r.get("kind") == "impair"
               and not r.get("fail")), None)
    stuck = 0
    if ev and ev.get("outages"):
        trecs = [r for r in getattr(trial, "records", []) if r.get("_phase") == ph]
        stuck = never_resumed(ev["outages"], trecs, trial_end(ev["outages"], trecs))
    return failed + stuck, len(tried) + stuck


def fetch_attempts(trials, ph) -> Tuple[int, int]:
    """(failed, attempted) short downloads over the trials, pooled: a download
    the window's end cut was not a fair attempt and is left out."""
    failed = tried_n = 0
    for t in trials:
        rows = [r for r in t.rows if r["phase"] == ph and r["kind"] == "imp-fetch"]
        cut = {f"w1r{r.get('k')}" for r in getattr(t, "records", [])
               if r.get("_phase") == ph and r.get("kind") == "fetch" and r.get("window_cut")}
        tried = [r for r in rows if not (r["flow"] in cut and r.get("curl_exit") == "28")]
        tried_n += len(tried)
        failed += sum(1 for r in tried if r.get("complete") != "1")
    return failed, tried_n


def fetch_completion(trials, ph) -> float:
    """Completed over attempted short downloads, per trial, averaged: a
    download the window's end cut (curl's timeout while its limit was the
    window's remainder) was not a fair attempt and is left out."""
    per = []
    for t in trials:
        rows = [r for r in t.rows if r["phase"] == ph and r["kind"] == "imp-fetch"]
        cut = {f"w1r{r.get('k')}" for r in getattr(t, "records", [])
               if r.get("_phase") == ph and r.get("kind") == "fetch" and r.get("window_cut")}
        tried = [r for r in rows if not (r["flow"] in cut and r.get("curl_exit") == "28")]
        if not tried:
            return float("nan")
        per.append(sum(1 for r in tried if r.get("complete") == "1") / len(tried))
    return sum(per) / len(per) if per else float("nan")


def silent_deaths(recs: Sequence[dict]) -> int:
    """LLM calls and keystroke sessions that failed by the client's own
    timeout: the tunnel neither delivered nor ended them."""
    return sum(1 for r in recs if r.get("kind") in ("llm", "term") and r.get("fail")
               and not str(r["fail"]).startswith("not run") and SILENT_RE.search(str(r["fail"])))


@w.register
class Impaired(mn.Minimal):
    """Trial types slow-lossy, blackhole and policed, each a block of its own
    under its scenario; D (idle-wake-lossy) beside them."""
    name = "impaired"
    impairs = True
    # Wildcard rules that do not apply to a type: blackhole judges TTFT as
    # ttft_clear_p99_s and ttft_after_restore_s instead (#239).
    rule_exempt = {"blackhole": ("ttft_net_p99_s",)}
    trials_per_arm = 1
    primaries: Tuple[str, ...] = ()
    # Flow completion is the trial's own (completed / attempted short
    # downloads), not the generic single-flow one.
    generic = {t: ("stalls_per_trial",) for t in TRIAL_TYPES}
    report_only = ("completion", "goodput_bps", "ttft_net_p50_s", "ttft_all_p99_s", "echo_p50_s", "recover_8s_s", "recover_25s_s",
                   "reopen_ttfb_8s_s", "reopen_ttfb_25s_s", "inflight_ttfb_8s_s", "inflight_ttfb_25s_s",
                   "inflight_ttfb_s")
    # Measures compared only in the blocks where both arms have them (a flow
    # kind alive at an outage's start in both): the coverage rule asks for
    # two such blocks, not every block.
    paired_only = ("resume_download_s", "resume_upload_s")

    def generics(self, plan):
        """At stage 10 (`[workload:impaired] stage = 10`) sender-side credit
        delay p95 is also a rule of every trial type ([secondary] bound)."""
        if mn.campaign_stage(plan) < 10:
            return dict(self.generic)
        return {ph: ks + ("credit_delay_p95_s",) for ph, ks in self.generic.items()}

    def report_only_for(self, plan):
        return self.report_only + (("false_promotion_rate",) if mn.campaign_stage(plan) >= 10 else ())

    def reported(self, plan):
        # The shadow false-promotion rate is reported at stage 10 (it needs
        # --carrier-shadow on every endpoint), never gated here.
        if mn.campaign_stage(plan) < 10:
            return {}
        return {ph: ("false_promotion_rate",) for ph in self.generic}

    def read(self, cfg, p):
        cfg.scenario_udp_port = int(p.get("udp_port", "23487"))
        cfg.scenario_wake_port = int(p.get("wake_port", "23488"))
        trials = pa._words(p.get("trials", " ".join(TRIAL_TYPES)))
        bad = [t for t in trials if t not in TRIAL_TYPES]
        if bad or not trials or len(set(trials)) != len(trials):
            raise ab.ConfigError(f"[workload:impaired] trials: want some of {', '.join(TRIAL_TYPES)}, each once")
        cfg.minimal_trials = [t for t in TRIAL_TYPES if t in trials]
        stage = p.get("stage", "7").strip()
        if stage not in ("7", "10"):
            raise ab.ConfigError("[workload:impaired] stage: 7 or 10 (10 adds credit delay to the rules)")
        cfg.minimal_stage = int(stage)
        iw = p.get("idle_wake", "true").strip().lower()
        if iw not in ("true", "false"):
            raise ab.ConfigError("[workload:impaired] idle_wake: true or false")
        cfg.minimal_wake = iw == "true"
        if len(cfg.clients) != 1:
            raise ab.ConfigError("[workload:impaired] one path per campaign: the impairment is on its client host")
        if cfg.minimal_wake:
            small = [c.name for c in cfg.clients.values() if c.small]
            if small and not ab._bool(p.get("idle_wake_on_small", "false")):
                raise ab.ConfigError(f"[workload:impaired] D runs idle client instances beside the blocks' one on small "
                                     f"client(s) {', '.join(small)}: set idle_wake_on_small = true, or idle_wake = false")
        cfg.minimal = {"upload": mn.UPLOAD}

    def trial_types(self, cfg):
        return list(cfg.minimal_trials)

    def trial_seconds(self, cfg, path, trial):
        return float(TRIAL_SECONDS[trial])

    def impairment(self, cfg, trial):
        return trial if trial in TRIAL_SECONDS else ""

    def run_first(self, cfg):
        return []

    def beside(self, cfg):
        b = super().beside(cfg)
        if b is not None:
            b.impair = WAKE_SCENARIO
        return b

    def plan_notes(self, cfg):
        notes = [f"impairment: {t}: {ab_impair.SCENARIOS[t].summary}" for t in cfg.minimal_trials]
        if cfg.minimal_wake:
            notes.append(f"impairment: D ({WAKE_SCENARIO}, its own ports, {mn.wake_session_seconds() / 60:.0f} min "
                         f"beside the blocks): {ab_impair.SCENARIOS[WAKE_SCENARIO].summary}")
        return notes

    def objects(self, cfg):
        return [(f"obj-{BULK_OBJECT}", BULK_OBJECT), (f"obj-{PROBE_BYTES}", PROBE_BYTES),
                (f"obj-{BG_DOWN_OBJECT}", BG_DOWN_OBJECT), (f"obj-{cfg.object_bytes}", cfg.object_bytes)]

    def llm_swarm(self) -> dict:
        # W5's call shape with short responses (100 deltas, 25 ms apart):
        # AGENTS loops of TURNS turns at once.
        return {"sequence": "swarm", "upload": mn.UPLOAD, "calls": 1, "turns": TURNS, "agents": AGENTS,
                "think_ms": 500, "deltas": DELTAS, "cadence_ms": 25, "timeout": 30, "delay": LEAD}

    def phases(self, cfg, path):
        c = cfg.clients[path]
        out = []
        for t, secs in TRIAL_SECONDS.items():
            # The load: FETCHES short downloads across the window (flow
            # completion, goodput); the foreground under it.
            jobs = [w.Job(c, "imp-fetch", 1),
                    w.Job(c, "wl-llm", 1, load=False, params=self.llm_swarm()),
                    w.Job(c, "wl-term", 1, load=False, params=self.term(secs - LEAD - 5, None))]
            if t in ("blackhole", "blackhole-base"):
                # Flows in flight across the outages (a download and a long
                # upload, each with a progress timeline), probes for the
                # recovery and for new connections, and the outages.
                bg = {"io_timeout": BG_IO_TIMEOUT["blackhole"], "continue_after_failure": True}
                jobs += [w.Job(c, "wl-bgdown", 1, load=False, params=dict(bg, object_bytes=BG_DOWN_OBJECT)),
                         w.Job(c, "wl-bgup", 1, load=False, params=dict(bg, object_bytes=BG_UP_OBJECT)),
                         w.Job(c, "imp-probe", 1, load=False)]
                if t == "blackhole":
                    jobs.append(w.Job(c, "impair-outages", 1, params={"scenario": t}))
            out.append(w.Phase(t, secs, jobs))
        return out

    def per_trial(self, cfg, slot, arm, phase):
        import dataclasses
        import random
        rng = random.Random(f"{cfg.seed}/{slot.block}/{slot.path}/{slot.trial}/{slot.slot}/{arm.name}")
        jobs = []
        for j in phase.jobs:
            params = dict(j.params)
            if j.kind == "wl-llm":
                params["delay"] = round(LEAD + rng.uniform(0, LLM_JITTER), 3)
            if j.kind == "impair-outages":
                nominal = ab_impair.SCENARIOS[params["scenario"]].outages
                params["outages"] = [(round(at + rng.uniform(0, jit), 3), dur)
                                     for (at, dur), jit in zip(nominal, OUTAGE_JITTER)]
            jobs.append(dataclasses.replace(j, params=params))
        return dataclasses.replace(phase, jobs=jobs)

    def has_evidence(self, ph, recs):
        kinds = {r.get("kind") for r in recs}
        need = {"llm", "term"} | ({"impair"} if ph == "blackhole" else set())
        if ph == "blackhole" and not any(r.get("kind") == "impair" and not r.get("fail") and r.get("outages")
                                         for r in recs):
            return False
        return need <= kinds

    def under_background(self, ph, recs, rows, trials):
        return True

    def block_tails(self, trials, plan):
        # The generic secondaries' pooled evidence (credit delay histograms,
        # promotion counts), then this workload's own tails.
        out: Dict[str, Tuple[Optional[float], List[float]]] = dict(mn.Minimal.block_tails(self, trials, plan))
        for ph in TRIAL_TYPES:
            pt = [t for t in trials if getattr(t, "trial", "") == ph]
            if not pt:
                continue
            recs = [r for t in pt for r in getattr(t, "records", []) if r.get("_phase") == ph]
            ok = [c for c in recs if c.get("kind") == "llm" and not c.get("fail")]
            ttft = [c["ttft"] - c["T"] for c in ok if c.get("ttft") is not None]
            if ph == "blackhole":
                # Gated on the calls clear of the outages; the rest are timed
                # from the restore (ttft_after_restore_s).
                out[f"{ph}.ttft_all_p99_s"] = (0.99, ttft)
                out[f"{ph}.ttft_clear_p99_s"] = (0.99, [c["ttft"] - c["T"] for c in clear_calls(pt, ph)])
            else:
                out[f"{ph}.ttft_net_p99_s"] = (0.99, ttft)
            out[f"{ph}.echo_p99_s"] = (0.99, [x for t in pt for x in echo_samples(t, ph)])
        return out

    def block_events(self, trials, plan) -> Dict[str, Tuple[int, int]]:
        """(events, opportunities) per trial type of an arm's block, for the
        pooled-rate rules: failed LLM calls over calls, and silent deaths over
        LLM calls and keystroke sessions."""
        out: Dict[str, Tuple[int, int]] = {}
        for ph in TRIAL_TYPES:
            pt = [t for t in trials if getattr(t, "trial", "") == ph]
            if not pt:
                continue
            recs = [r for t in pt for r in getattr(t, "records", []) if r.get("_phase") == ph]
            llm = [r for r in recs if r.get("kind") == "llm"]
            sessions = [r for r in recs if r.get("kind") in ("llm", "term")]
            out[f"{ph}.call_failures"] = (sum(1 for r in llm if r.get("fail")), len(llm))
            out[f"{ph}.silent_rate"] = (silent_deaths(recs), len(sessions))
            out[f"{ph}.fetch_failures"] = fetch_attempts(pt, ph)
            if ph == "blackhole":
                bg = [bg_transfers(t, ph) for t in pt]
                out[f"{ph}.bg_transfer_failures"] = (sum(f for f, _ in bg), sum(n for _, n in bg))
        return out

    def phase_measures(self, ph, recs, rows, trials):
        nan = float("nan")
        llm = [r for r in recs if r.get("kind") == "llm"]
        lm = pa.llm_metrics(llm)
        ok = [c for c in llm if not c.get("fail")]
        ttft = [c["ttft"] - c["T"] for c in ok if c.get("ttft") is not None]
        echo = [x for t in trials for x in echo_samples(t, ph)]
        secs = TRIAL_SECONDS.get(ph, 0)
        got = sum(int(r["bytes"] or 0) for r in rows if r["kind"] == "imp-fetch" and r.get("http") == "200")
        sessions = [r for r in recs if r.get("kind") in ("llm", "term")]
        m: Dict[str, float] = {
            "completion": fetch_completion(trials, ph),
            # Judged as a pooled event rate (gates.ini `events`); completion
            # itself is reported.
            "fetch_failures": (lambda f, n: f / n if n else nan)(*fetch_attempts(trials, ph)),
            "call_failures": lm.get("call_failures", nan),
            "silent_rate": silent_deaths(recs) / len(sessions) if sessions else nan,
            "ttft_net_p50_s": st.quantile(ttft, 0.5),
            ("ttft_all_p99_s" if ph == "blackhole" else "ttft_net_p99_s"): st.quantile(ttft, 0.99),
            "echo_p50_s": st.quantile(echo, 0.5), "echo_p99_s": st.quantile(echo, 0.99),
            "silent_deaths": silent_deaths(recs) / len(trials) if trials else nan,
            "goodput_bps": got * 8 / (secs * len(trials)) if trials and secs else nan,
        }
        if ph == "blackhole":
            per8, per25, worst = [], [], []
            for t in trials:
                ev = next((r for r in getattr(t, "records", []) if r.get("_phase") == ph and r.get("kind") == "impair"
                           and not r.get("fail")), None)
                probes = [r for r in t.rows if r["phase"] == ph and r["kind"] == "imp-probe"]
                if not ev or not ev.get("outages") or not probes:
                    worst.append(nan)
                    continue
                outs = ev["outages"]
                # The trial's end on the same clock: the probes' last start
                # plus their timeout bounds it from above.
                end = max(float(r["t_start"] or 0) for r in probes) + PROBE_TIMEOUT
                rec = recoveries(outs, probes, end)
                worst.append(max(rec))
                per8.append(rec[0])
                if len(rec) > 1:
                    per25.append(rec[1])
            m["recover_s"] = sum(worst) / len(worst) if worst and all(x == x for x in worst) else nan
            m["recover_8s_s"] = sum(per8) / len(per8) if per8 else nan
            m["recover_25s_s"] = sum(per25) / len(per25) if per25 else nan
            # Calls clear of the outages, and calls caught by one: the time
            # from its restore to their first token (the worse outage of a
            # trial, averaged over trials).
            m["ttft_clear_p99_s"] = st.quantile([c["ttft"] - c["T"] for c in clear_calls(trials, ph)], 0.99)
            per_t = []
            for t in trials:
                ev = next((r for r in getattr(t, "records", []) if r.get("_phase") == ph and
                           r.get("kind") == "impair" and not r.get("fail")), None)
                trecs = [r for r in getattr(t, "records", []) if r.get("_phase") == ph]
                per_t.append(max(after_restore(ev["outages"], trecs, trial_end(ev["outages"], trecs)))
                             if ev and ev.get("outages") else nan)
            m["ttft_after_restore_s"] = sum(per_t) / len(per_t) if per_t and all(x == x for x in per_t) else nan
            # Per outage, from the path's restore: the first byte of a
            # connection opened after it, and of a flow in flight across it.
            for name, fn in (("reopen_ttfb", reopen_ttfb), ("inflight_ttfb", inflight_ttfb)):
                per = []
                for t in trials:
                    ev = next((r for r in getattr(t, "records", []) if r.get("_phase") == ph and
                               r.get("kind") == "impair" and not r.get("fail")), None)
                    trecs = [r for r in getattr(t, "records", []) if r.get("_phase") == ph]
                    per.append(fn(ev["outages"], trecs, trial_end(ev["outages"], trecs)) if ev and ev.get("outages")
                               else [nan, nan])
                worst_ = [max(x) for x in per]
                m[f"{name}_s"] = sum(worst_) / len(worst_) if worst_ and all(x == x for x in worst_) else nan
                for k, label in ((0, "8s"), (1, "25s")):
                    xs = [x[k] for x in per if len(x) > k]
                    m[f"{name}_{label}_s"] = sum(xs) / len(xs) if xs else nan
            # Resume per flow kind, the worse outage: a block's value only
            # when that flow was alive at an outage's start (the analyzer
            # compares a kind only in blocks where both arms have it).
            for direction, key in (("down", "resume_download_s"), ("up", "resume_upload_s")):
                per_k = []
                for t in trials:
                    ev = next((r for r in getattr(t, "records", []) if r.get("_phase") == ph and
                               r.get("kind") == "impair" and not r.get("fail")), None)
                    trecs = [r for r in getattr(t, "records", []) if r.get("_phase") == ph]
                    xs = ([x for x in resume_by_kind(ev["outages"], trecs, trial_end(ev["outages"], trecs), direction)
                           if x is not None] if ev and ev.get("outages") else [])
                    per_k.append(max(xs) if xs else nan)
                m[key] = sum(per_k) / len(per_k) if per_k and all(x == x for x in per_k) else nan
            bg = [bg_transfers(t, ph) for t in trials]
            m["bg_transfer_failures"] = (sum(f for f, _ in bg) / sum(n for _, n in bg)
                                         if sum(n for _, n in bg) else nan)
        return m

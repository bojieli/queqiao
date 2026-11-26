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

Each trial: the W5 LLM loop (5 turns), W6 keystrokes without bursts, and one
bulk download of 4 MiB objects for the whole window.

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

TRIAL_SECONDS = {"slow-lossy": 90, "blackhole": 120, "policed": 90}
TRIAL_TYPES = tuple(TRIAL_SECONDS)
WAKE_SCENARIO = "idle-wake-lossy"
LEAD = 5                    # the foreground starts this long after the download
BULK_OBJECT = 4 << 20       # one download, object after object (3.4 s each at 10 Mbit/s)
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
    stop = campaign.stop_file(job.client, arm)
    return (ab.NOW_SH + f"end=$(( $(date +%s) + {int(duration)} )); i=0; "
            f"while [ $(date +%s) -lt $end ] && [ ! -e {stop} ]; do i=$((i+1)); "
            f"( t0=$(now); r=$({curl} --max-time {PROBE_TIMEOUT} {url}obj-{PROBE_BYTES} 2>/dev/null); t1=$(now); "
            f"{w._ROW} \"p$i\" \"$r\" ) & sleep {PROBE_EVERY}; done; wait")


def outage_script(campaign, job, arm, duration, start_at=0):
    return campaign.impair.outage_script(job.params["scenario"])


w.generator("imp-probe", w.Generator(probe_script, lambda out, job, d: ab.job_rows(out, "probe", 1, 1, d),
                                     lambda job: PROBE_BYTES))
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
    trials_per_arm = 1
    primaries: Tuple[str, ...] = ()
    generic = {t: ("completion", "stalls_per_trial") for t in TRIAL_TYPES}
    report_only = ("goodput_bps", "ttft_net_p50_s", "echo_p50_s", "recover_8s_s", "recover_25s_s")

    def generics(self, plan):
        return dict(self.generic)

    def report_only_for(self, plan):
        return self.report_only

    def reported(self, plan):
        return {}

    def read(self, cfg, p):
        cfg.scenario_udp_port = int(p.get("udp_port", "23487"))
        cfg.scenario_wake_port = int(p.get("wake_port", "23488"))
        trials = pa._words(p.get("trials", " ".join(TRIAL_TYPES)))
        bad = [t for t in trials if t not in TRIAL_TYPES]
        if bad or not trials or len(set(trials)) != len(trials):
            raise ab.ConfigError(f"[workload:impaired] trials: want some of {', '.join(TRIAL_TYPES)}, each once")
        cfg.minimal_trials = [t for t in TRIAL_TYPES if t in trials]
        cfg.minimal_stage = 7
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
                (f"obj-{cfg.object_bytes}", cfg.object_bytes)]

    def phases(self, cfg, path):
        c = cfg.clients[path]
        out = []
        for t, secs in TRIAL_SECONDS.items():
            jobs = [w.Job(c, "bulk", 1, params={"expected": BULK_OBJECT, "object_bytes": BULK_OBJECT}),
                    w.Job(c, "wl-llm", 1, load=False, params=self.llm("loop", LEAD)),
                    w.Job(c, "wl-term", 1, load=False, params=self.term(secs - LEAD - 5, None))]
            if t == "blackhole":
                jobs += [w.Job(c, "wl-bgup", 1, load=False, params={"object_bytes": cfg.scenario_upload_object}),
                         w.Job(c, "imp-probe", 1, load=False),
                         w.Job(c, "impair-outages", 1, params={"scenario": t})]
            out.append(w.Phase(t, secs, jobs))
        return out

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
        out: Dict[str, Tuple[Optional[float], List[float]]] = {}
        for ph in TRIAL_TYPES:
            pt = [t for t in trials if getattr(t, "trial", "") == ph]
            if not pt:
                continue
            recs = [r for t in pt for r in getattr(t, "records", []) if r.get("_phase") == ph]
            ok = [c for c in recs if c.get("kind") == "llm" and not c.get("fail")]
            out[f"{ph}.ttft_net_p99_s"] = (0.99, [c["ttft"] - c["T"] for c in ok if c.get("ttft") is not None])
            out[f"{ph}.echo_p99_s"] = (0.99, pa.term_samples([r for r in recs if r.get("kind") == "term"])["a"])
        return out

    def phase_measures(self, ph, recs, rows, trials):
        nan = float("nan")
        llm = [r for r in recs if r.get("kind") == "llm"]
        lm = pa.llm_metrics(llm)
        ok = [c for c in llm if not c.get("fail")]
        ttft = [c["ttft"] - c["T"] for c in ok if c.get("ttft") is not None]
        echo = pa.term_samples([r for r in recs if r.get("kind") == "term"])["a"]
        secs = TRIAL_SECONDS.get(ph, 0)
        got = sum(int(r["bytes"] or 0) for r in rows if r["kind"] == "bulk" and r.get("http") == "200")
        m: Dict[str, float] = {
            "call_failures": lm.get("call_failures", nan),
            "ttft_net_p50_s": st.quantile(ttft, 0.5), "ttft_net_p99_s": st.quantile(ttft, 0.99),
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
        return m

"""The model's minimal live plan (DESIGN-PATH-AUTHORITY.md section 15.3;
docs/WORKLOAD-MODEL.md; docs/VALIDATING-A-BUNDLE.md, "The minimal profile"):
four trial types and the DC gate, about 2.5-3 hours of live time per path.

* `minimal`: trial types, each a block of its own (one phase per trial),
  round-robin at block granularity, each stopped on its own:
  - `bulk` (A): four downloads, 60 s. Gates stages 6 and 7.
  - `mixed-67` (B): a 5-turn LLM loop (100 KB prompt plus 8 KB per turn,
    2 s apart) and keystrokes with one 300 KB burst at 40 s, under two
    downloads, 90 s. Gates stages 6 and 7.
  - `mixed-all` (C): a video call for the whole trial, one h2 page on cold
    connections at 20 s, one LLM call at 50 s, keystrokes without bursts,
    one download and one upload, 120 s. Gates stage 10.
* `idle-wake` (D): 50 connections per arm after a 200 KB sync, each woken
  once at 1, 5 or 16 minutes (17/17/16), push and send alternating, then 5
  warm follow-ups. Its arms run at once beside the blocks, on their own
  instances, during C's first two blocks; judged as the campaign in
  `<campaign>/idle-wake`. Gates stage 10.

Every setting is the plan's and is fixed here. The metrics are computed from
wlclient's raw records; the rules are in gates.ini [workload:minimal] and
[workload:idle-wake], provisional.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Sequence, Tuple

import ab_campaign as ab
import ab_stats as st
import ab_workloads as w
import generic_metrics as gm

pa = __import__("sys").modules.get("ab_workload_phase_a")
if pa is None:                                   # loaded alone (tests): import its sibling
    import importlib.util as _u
    _spec = _u.spec_from_file_location("ab_workload_phase_a", os.path.join(os.path.dirname(__file__), "phase_a.py"))
    pa = _u.module_from_spec(_spec)
    __import__("sys").modules["ab_workload_phase_a"] = pa
    _spec.loader.exec_module(pa)

LATE = 0.150          # a call packet later than this past the path's best is as good as lost (W7)
AUDIO_GAP = 3         # consecutive bad audio packets (60 ms) make an audible gap
FREEZE_FRAMES = 6     # consecutive bad video frames (200 ms) make a freeze
WAKE_WITHIN = 5.0     # a wake push is delivered if it arrives within this of its time (W9)


# --- call --------------------------------------------------------------------------

def _lateness(xs: Sequence[float]) -> List[float]:
    m = min(xs) if xs else 0.0
    return [(x - m) / 1000.0 for x in xs]


def _runs(bad: Sequence[bool], length: int) -> int:
    """Runs of at least `length` consecutive True values."""
    n = run = 0
    for b in bad:
        if b:
            run += 1
            if run == length:
                n += 1
        else:
            run = 0
    return n


def _streams(c: dict, d: str) -> Tuple[List[list], List[list], int]:
    """A call's audio and video packets in one direction, and the index of
    that direction's transit: the client's sends up, the far side's own
    sends down."""
    if d == "up":
        return c.get("audio") or [], c.get("video") or [], 2
    return c.get("down_audio") or [], c.get("down_video") or [], 3


def call_samples(calls: Sequence[dict]) -> Dict[str, List[float]]:
    """Per-direction lateness samples (seconds) of the calls that started."""
    out: Dict[str, List[float]] = {"up": [], "down": []}
    for c in calls:
        if c.get("fail"):
            continue
        for d in ("up", "down"):
            a, v, idx = _streams(c, d)
            out[d] += _lateness([p[idx] for p in a + v if len(p) > idx and p[idx] is not None])
    return out


def call_metrics(calls: Sequence[dict]) -> Dict[str, float]:
    """W7 metrics per direction over a set of call records.

    Each direction is its own open-loop stream: the client's uplink, and the
    far side's downlink sent on its own clock. Each packet carries its
    transit in its direction (None when it never arrived: up from the
    reflector's report of its arrivals, down from the client's receipts of
    the reflector's sends). Lateness is on that direction's clock pair
    (clock-free); a packet is bad when missing or more than 150 ms late. Per
    direction: audio late loss, frame loss (a frame with any bad packet),
    freezes (runs of 6 bad frames) and audio gaps (runs of 3 bad audio
    packets) per minute, lateness p99, and late loss over all packets."""
    nan = float("nan")
    if not calls:
        return {}
    ok = [c for c in calls if not c.get("fail")]
    if not ok:
        # Every call failed to start: that is measured, as failures; the
        # quality measures have nothing to describe.
        return {"sessions_failed": 1.0}
    minutes = sum(float(c.get("seconds") or 0) for c in ok) / 60.0
    out: Dict[str, float] = {"sessions_failed": (len(calls) - len(ok)) / len(calls)}
    for d in ("up", "down"):
        lat_all: List[float] = []
        pkts = bad_pkts = audio = audio_bad = frames = frames_bad = freezes = gaps = 0
        for c in ok:
            a_pk, v_pk, idx = _streams(c, d)
            pk = a_pk + v_pk
            lat = _lateness([p[idx] for p in pk if len(p) > idx and p[idx] is not None])
            lat_all += lat
            lat_iter = iter(lat)
            bad: Dict[int, bool] = {}
            for p in pk:
                v = p[idx] if len(p) > idx else None
                bad[id(p)] = v is None or next(lat_iter) > LATE
            pkts += len(bad)
            bad_pkts += sum(bad.values())
            a_bad = [bad[id(p)] for p in a_pk]
            audio += len(a_bad)
            audio_bad += sum(a_bad)
            gaps += _runs(a_bad, AUDIO_GAP)
            by_frame: Dict[int, bool] = {}
            for p in v_pk:
                by_frame[p[0]] = by_frame.get(p[0], False) or bad[id(p)]
            fb = [by_frame[f] for f in sorted(by_frame)]
            frames += len(fb)
            frames_bad += sum(fb)
            freezes += _runs(fb, FREEZE_FRAMES)
        out.update({
            f"audio_late_loss_{d}": audio_bad / audio if audio else nan,
            f"frame_loss_{d}": frames_bad / frames if frames else nan,
            f"freezes_per_min_{d}": freezes / minutes if minutes else nan,
            f"lateness_p99_{d}_s": st.quantile(lat_all, 0.99),
            f"late_loss_{d}": bad_pkts / pkts if pkts else nan,
            f"audio_gaps_per_min_{d}": gaps / minutes if minutes else nan,
        })
    return out


# --- browsing ----------------------------------------------------------------------

PAGE_OBJECTS = 41     # the HTML and its 40 objects (W3)


def page_times(pages: Sequence[dict]) -> List[float]:
    """Page load times: from the first CONNECT (the page's clock zero) to the
    last object's last byte, failed objects counted at the time they failed."""
    out = []
    for p in pages:
        objs = p.get("objects") or []
        if not p.get("fail") and objs:
            out.append(max(o[4] for o in objs))
    return out


def browse_metrics(pages: Sequence[dict]) -> Dict[str, float]:
    """W3 metrics over a set of page loads: page load time p50 and p95 (first
    CONNECT to the last object's last byte), the HTML's first byte (from the
    first CONNECT), and the fraction of objects that failed. A page whose
    first connection never opened counts as fully failed."""
    nan = float("nan")
    if not pages:
        return {}
    html, failed, total = [], 0, 0
    for p in pages:
        objs = p.get("objects") or []
        if p.get("fail") or not objs:
            failed += PAGE_OBJECTS
            total += PAGE_OBJECTS
            continue
        total += max(len(objs), PAGE_OBJECTS)
        failed += sum(1 for o in objs if o[5]) + max(0, PAGE_OBJECTS - len(objs))
        h = next((o for o in objs if o[0] == 0), None)
        if h and h[3] is not None:
            html.append(h[3])
    plt = page_times(pages)
    return {"plt_p50_s": st.quantile(plt, 0.5), "plt_p95_s": st.quantile(plt, 0.95),
            "html_first_byte_p50_s": st.quantile(html, 0.5),
            "objects_failed": failed / total if total else nan}


# --- idle wake ----------------------------------------------------------------------

WAKE_TIMES = (60, 300, 960)       # 1, 5 and 16 minutes (W9a, trial D)
WAKE_CONNS = 50                   # 17, 17 and 16 connections
WAKE_SYNC = 200_000               # the login-history burst after connecting
WAKE_FOLLOW = 5                   # warm follow-ups after the wake, 1 s apart
WAKE_TIMEOUT = 30.0               # the client's own wait for a send's answer


def wake_plan(conns: int = WAKE_CONNS) -> List[Tuple[int, str]]:
    """(wake time, direction) per connection: the times in turn (17/17/16 of
    50), push and send alternating within each time."""
    out = []
    for i in range(conns):
        at = WAKE_TIMES[i % len(WAKE_TIMES)]
        out.append((at, "push" if (i // len(WAKE_TIMES)) % 2 == 0 else "send"))
    return out


def wake_samples(conns: Sequence[dict]) -> Dict[str, List[float]]:
    """Per-wake samples from a session's connections (one wake each).

    `delivered_<t>`: 1 for a wake at t seconds that arrived within 5 s (a
    push received, or a send answered), else 0. `push_penalty` /
    `send_penalty`: the wake's delay over the best of its own 5 warm
    follow-ups (a push's one-way delta, clock-free; a send's round trip).
    `silent`: per wake, 1 if it was undelivered and its application saw no
    EOF or reset by 5 s after the wake (design: a flow the tunnel ended must reach the
    application within 5 s). `death_<mode>`: undelivered wakes by how they
    ended (eof, rst, timeout: a send unanswered for 30 s, silent: a push
    never seen). `idle_eof`: seconds from the end of the sync to a close
    seen while idle."""
    out: Dict[str, List[float]] = {f"delivered_{t}": [] for t in WAKE_TIMES}
    for k in ("push_penalty", "send_penalty", "silent", "death_eof", "death_rst", "death_timeout",
              "death_silent", "idle_eof", "sync_seconds"):
        out[k] = []
    for c in conns:
        at = float(c.get("at") or 0)
        mode = c.get("mode")
        end_at = c.get("end_at")
        # Wake times count from the end of the sync (acknowledged by the
        # client), not from the connection: the idle interval is the plan's.
        base = c.get("sync_done")
        ok = False
        if not c.get("fail") and base is not None:
            base = float(base)
            if mode == "push":
                pushes = {p[0]: p for p in c.get("pushes") or []}
                wp = pushes.get(0)
                ok = wp is not None and wp[2] <= base + at + WAKE_WITHIN
                if wp is not None:
                    warm = [pushes[j][2] - pushes[j][1] / 1000.0 for j in range(1, WAKE_FOLLOW + 1) if j in pushes]
                    if warm:
                        out["push_penalty"].append((wp[2] - wp[1] / 1000.0) - min(warm))
            else:
                sends = {s[0]: s for s in c.get("sends") or []}
                ws = sends.get(0)
                ok = ws is not None and ws[2] <= WAKE_WITHIN
                if ws is not None:
                    warm = [sends[j][2] for j in range(1, WAKE_FOLLOW + 1) if j in sends]
                    if warm:
                        out["send_penalty"].append(ws[2] - min(warm))
        base = float(c.get("sync_done") or 0.0)
        out.setdefault(f"delivered_{int(at)}", []).append(1.0 if ok else 0.0)
        if ok:
            out["silent"].append(0.0)
        else:
            told = end_at is not None and float(end_at) <= base + at + WAKE_WITHIN
            out["silent"].append(0.0 if told else 1.0)
            end = (c.get("end") or "").lower()
            mode_ = ("eof" if "eof" in end else "rst" if end else
                     "timeout" if mode == "send" else "silent") if not c.get("fail") else "rst"
            for m in ("eof", "rst", "timeout", "silent"):
                out[f"death_{m}"].append(1.0 if m == mode_ else 0.0)
        if end_at is not None and c.get("sync_done") is not None and float(end_at) < base + at:
            out["idle_eof"].append(float(end_at) - base)
        if c.get("sync_seconds") is not None:
            out["sync_seconds"].append(float(c["sync_seconds"]))
    return out


# --- generators ----------------------------------------------------------------------

def _scenario_port(cfg, which):
    return {"http": cfg.scenario_http_port, "term": cfg.scenario_term_port, "udp": cfg.scenario_udp_port,
            "wake": cfg.scenario_wake_port}[which]


def call_script(campaign, job, arm, duration, start_at=0):
    p = job.params
    return pa._client(campaign, job, arm, "call", _scenario_port(campaign.cfg, "udp"),
                      f"--duration {p['seconds']} --delay {p.get('delay', 0)} "
                      f"--report-target 127.0.0.1:{_scenario_port(campaign.cfg, 'http')}")


def browse_script(campaign, job, arm, duration, start_at=0):
    p = job.params
    return pa._client(campaign, job, arm, "browse", _scenario_port(campaign.cfg, "http"),
                      f"--delay {p.get('delay', 0)} --timeout {p.get('timeout', 30)} --seed {p.get('seed', 1)} "
                      f"--mode {p.get('mode', 'h2')}")


def wake_script(campaign, job, arm, duration, start_at=0):
    p = job.params
    ready = f" --ready-file {p['ready_file']}" if p.get("ready_file") else ""
    return pa._client(campaign, job, arm, "wake", _scenario_port(campaign.cfg, "wake"),
                      f"--conns {p['conns']} --sync {p['sync']} --wake-times {','.join(str(t) for t in WAKE_TIMES)} "
                      f"--follow {p['follow']} --duration {int(duration)}{ready}")


for _name, _fn, _kind in (("wl-call", call_script, "call"), ("wl-browse", browse_script, "browse"),
                          ("wl-wake", wake_script, None)):
    w.generator(_name, w.Generator(_fn, pa.parse_rows, lambda job: 0, w.json_records, _kind))


# --- workloads --------------------------------------------------------------------

# The trial table of the model's Minimal live plan (DESIGN-PATH-AUTHORITY.md
# section 15.3): every setting is fixed there, so none is configurable here.
TRIAL_TYPES = ("bulk", "mixed-67", "mixed-all")
# Generic secondaries of section 15.4 that gate stage 10 and are reported,
# not gated, at stages 6 and 7.
STAGE10_GENERIC = ("credit_delay_p95_s", "false_promotion_rate")


def pooled_generic(ms: Sequence[dict], key: str) -> float:
    """Credit delay p95 or the false-promotion rate over several trials'
    measures, from their pooled histograms and promotion counts."""
    if key == "credit_delay_p95_s":
        return gm.quantile(0.95, gm.pool([m.get("_credit_hist") for m in ms]))
    return gm.rate(gm.pooled_promotions([m.get("_promotions") for m in ms]))


def campaign_stage(plan: dict) -> int:
    """The stage a minimal campaign gates (`[workload:minimal] stage`). A
    plan from before the setting has its default: 10 with trial type C,
    else 7."""
    c = plan.get("config") or {}
    if c.get("minimal_stage"):
        return int(c["minimal_stage"])
    return 10 if "mixed-all" in (c.get("minimal_trials") or ()) else 7
TRIAL_SECONDS = {"bulk": 60, "mixed-67": 90, "mixed-all": 120}
LEAD = 5              # backgrounds start this long before the foreground (model 3.0)
UPLOAD = 100_000      # U, the LLM prompt
DC_TRIAL_SECONDS = 60  # the DC gate at minimal size, its own campaign (stage 9)
# Tail percentiles: their quantile and pooled samples, for the per-block or
# pooled test of section 15.4.
# The foreground whose span the backgrounds must cover, and over which their
# rate is taken: every measured workload, the page load included.
FOREGROUND = ("llm", "term", "call", "browse")


def wake_session_seconds() -> float:
    """D's session: the last wake, its follow-ups, the send timeout, and slack."""
    return max(WAKE_TIMES) + WAKE_FOLLOW + WAKE_TIMEOUT + 10


def wake_params() -> dict:
    return {"conns": WAKE_CONNS, "sync": WAKE_SYNC, "follow": WAKE_FOLLOW}


@w.register
class Minimal(pa._Scenario):
    """Trial types A (bulk), B (mixed-67) and C (mixed-all), each a block of
    its own with the endpoints restarted before it, round-robin; D beside
    C's first two blocks."""
    name = "minimal"
    report_only = (pa.LLM_REPORT_ONLY + pa.TERM_REPORT_ONLY +
                   ("html_first_byte_p50_s", "sessions_failed", "call_sessions_failed", "call_late_loss_up",
                    "call_late_loss_down", "call_audio_gaps_per_min_up", "call_audio_gaps_per_min_down") +
                   STAGE10_GENERIC)

    # The primaries: only bulk flows' bytes (section 15.3).
    primaries = ("bulk.goodput_bps", "mixed-67.bg_down_rate_bps", "mixed-all.bg_down_rate_bps",
                 "mixed-all.bg_up_rate_bps")
    # Generic secondaries where defined (section 15.4): flow completion and
    # wire bytes per useful byte need A's bulk flows; the others apply to
    # every trial. Credit delay and the shadow false-promotion rate gate
    # every trial type of a stage-10 campaign and are reported, not gated,
    # at stages 6 and 7 (see `generics`).
    generic = {"bulk": ("completion", "stalls_per_trial", "reissued_per_source", "wire_per_useful", "queue_p90_ms",
                        "cpu_p90"),
               "mixed-67": ("stalls_per_trial", "reissued_per_source", "queue_p90_ms", "cpu_p90"),
               "mixed-all": ("stalls_per_trial", "reissued_per_source", "queue_p90_ms", "cpu_p90")}

    def generics(self, plan) -> Dict[str, Tuple[str, ...]]:
        """The generic secondaries each trial type is judged on in this
        campaign: at stage 10, `generic` plus STAGE10_GENERIC."""
        if campaign_stage(plan) < 10:
            return dict(self.generic)
        return {ph: ks + STAGE10_GENERIC for ph, ks in self.generic.items()}

    def report_only_for(self, plan) -> Tuple[str, ...]:
        """The measures shown without a rule. Stage 10's generic
        secondaries are report-only below stage 10 only: at stage 10 one
        without a rule in gates.ini leaves the cell INCOMPLETE."""
        if campaign_stage(plan) < 10:
            return self.report_only
        return tuple(k for k in self.report_only if k not in STAGE10_GENERIC)

    def reported(self, plan) -> Dict[str, Tuple[str, ...]]:
        """The generic secondaries each trial type reports without gating."""
        if campaign_stage(plan) < 10:
            return {ph: STAGE10_GENERIC for ph in self.generic}
        return {}

    def block_extract(self, trials, plan):
        out = super().block_extract(trials, plan)
        measured: Dict[str, Tuple[str, ...]] = {}
        for src in (self.generics(plan), self.reported(plan)):
            for ph, ks in src.items():
                measured[ph] = measured.get(ph, ()) + ks
        for ph, ks in measured.items():
            pt = [t for t in trials if getattr(t, "trial", "") == ph]
            if not pt:
                continue
            for k in ks:
                if k in STAGE10_GENERIC:
                    # Pooled over the block's trials, never an average of
                    # per-trial percentiles or rates.
                    out[f"{ph}.{k}"] = pooled_generic([getattr(t, "m", {}) for t in pt], k)
                    continue
                vals = [getattr(t, "m", {}).get(k, float("nan")) for t in pt]
                out[f"{ph}.{k}"] = (sum(vals) / len(vals) if all(v is not None and v == v for v in vals)
                                    else float("nan"))
        return out

    def read(self, cfg, p):
        cfg.scenario_udp_port = int(p.get("udp_port", "23487"))
        cfg.scenario_wake_port = int(p.get("wake_port", "23488"))
        for k in ("bulk_seconds", "mixed67_seconds", "mixedall_seconds", "upload_kb"):
            if k in p:
                raise ab.ConfigError(f"[workload:minimal] {k}: the trial settings are fixed by the model's minimal "
                                     "live plan (design section 15.3); remove it")
        trials = pa._words(p.get("trials", " ".join(TRIAL_TYPES)))
        bad = [t for t in trials if t not in TRIAL_TYPES]
        if bad or not trials or len(set(trials)) != len(trials):
            raise ab.ConfigError(f"[workload:minimal] trials: want some of {', '.join(TRIAL_TYPES)}, each once")
        cfg.minimal_trials = [t for t in TRIAL_TYPES if t in trials]
        # The stage this campaign gates. Stage 10 gates credit delay and the
        # shadow false-promotion rate (the latter needs --carrier-shadow on
        # every endpoint of both arms); stages 6 and 7 report them.
        stage = p.get("stage", "auto").strip().lower()
        if stage == "auto":
            cfg.minimal_stage = 10 if "mixed-all" in cfg.minimal_trials else 7
        elif stage in ("6", "7", "10"):
            cfg.minimal_stage = int(stage)
        else:
            raise ab.ConfigError("[workload:minimal] stage: auto, 6, 7 or 10")
        if cfg.minimal_stage < 10 and "mixed-all" in cfg.minimal_trials:
            # C gates stage 10 only, with stage 10's generic secondaries.
            raise ab.ConfigError("[workload:minimal] stage: trial type mixed-all (C) gates stage 10; "
                                 "use stage = 10 or auto")
        iw = p.get("idle_wake", "auto").strip().lower()
        if iw not in ("auto", "true", "false"):
            raise ab.ConfigError("[workload:minimal] idle_wake: auto, true or false")
        # D runs beside C at stage 10 (and beside A and B for classifier=v2).
        cfg.minimal_wake = iw == "true" or (iw == "auto" and "mixed-all" in cfg.minimal_trials)
        if cfg.minimal_wake and len(cfg.clients) > 1:
            # Blocks load one path at a time, so on a second path D's wakes
            # could fall while that path is idle: run D per path.
            raise ab.ConfigError("[workload:minimal] D (idle_wake) needs one path per campaign: blocks load one path "
                                 "at a time, so another path's wakes would not be under load. Run one campaign per "
                                 "path, or set idle_wake = false")
        if cfg.minimal_wake:
            # D adds an idle client instance per arm on each path's client
            # host, beside the blocks' one: a small host must allow it.
            small = [c.name for c in cfg.clients.values() if c.small and c.name in cfg.clients]
            if small and not ab._bool(p.get("idle_wake_on_small", "false")):
                raise ab.ConfigError(f"[workload:minimal] D (idle_wake) runs two idle client instances beside the "
                                     f"blocks' one on small client(s) {', '.join(small)}: set idle_wake_on_small = true "
                                     "to allow it, or idle_wake = false and run D as its own campaign")
        cfg.minimal = {"upload": UPLOAD}

    def trial_types(self, cfg):
        return list(cfg.minimal_trials)

    def trial_seconds(self, cfg, path, trial):
        return float(TRIAL_SECONDS[trial])

    def run_first(self, cfg):
        # D's 17 minutes are covered by C's first two blocks, back to back.
        if cfg.minimal_wake and "mixed-all" in cfg.minimal_trials:
            return [(1, "mixed-all"), (2, "mixed-all")]
        return []

    def beside(self, cfg):
        if not getattr(cfg, "minimal_wake", False):
            return None
        return w.Beside("idle-wake", "idle-wake",
                        w.Phase("wake", wake_session_seconds() + 60, [w.Job(None, "wl-wake", 1, params=wake_params())],
                                expected=wake_session_seconds()))

    def plan_notes(self, cfg):
        dc = 4 * DC_TRIAL_SECONDS + ab.BLOCK_OVERHEAD
        every = sum(4 * s + ab.BLOCK_OVERHEAD for s in TRIAL_SECONDS.values()) + dc
        notes = [f"DC (stage 9) is its own campaign (dc-minimal.example.ini): about {dc / 60:.0f} min a block; "
                 f"one block each of A, B, C and DC is about {every / 60:.0f} min a path (as measured; the design "
                 f"budgets about 26), "
                 f"{6 * every / 3600:.1f} h at 6 blocks, {12 * every / 3600:.1f} h at the 12-block cap"]
        if cfg.minimal_wake:
            notes.append(f"D (idle-wake) runs beside the blocks on its own instances: one {wake_session_seconds() / 60:.0f}-minute "
                         "session per arm per path, its syncs before the first trial"
                         + (", during C's first two blocks" if "mixed-all" in cfg.minimal_trials else "")
                         + f"; judged as the campaign in <out>/idle-wake")
        return notes

    def positive(self, cfg):
        return [("object_bytes", cfg.object_bytes)]

    def endpoints(self, cfg):
        tls = "" if cfg.scenario_tls else " --no-tls"
        wake = f" --wake {cfg.scenario_wake_port}" if cfg.minimal_wake else ""
        tcp = [(cfg.scenario_http_port, "http"), (cfg.scenario_term_port, "terminal")]
        if cfg.minimal_wake:
            tcp.append((cfg.scenario_wake_port, "wake"))
        return [w.Endpoint("wlserver", f"python3 {{W}}/wlserver.py --http {cfg.scenario_http_port} "
                                       f"--term {cfg.scenario_term_port} --udp {cfg.scenario_udp_port}{wake} --dir {{W}}{tls}",
                           tcp_ports=tcp, udp_ports=[(cfg.scenario_udp_port, "call reflector")], files=[pa.SERVER])]

    def llm(self, seq: str, delay: float) -> dict:
        # W5: 100 KB prompt (8 KB more per turn in the loop, 2 s apart), 500 ms
        # think, 400 deltas 25 ms apart, 30 s client timeout.
        return {"sequence": seq, "upload": UPLOAD, "calls": 1, "turns": 5, "agents": 1,
                "think_ms": 500, "deltas": 400, "cadence_ms": 25, "timeout": 30, "delay": delay}

    def term(self, seconds: float, burst_at: Optional[float]) -> dict:
        # W6: keystrokes from the foreground's start; B: one 300 KB burst at
        # 40 s; C: none.
        return {"seconds": seconds, "delay": LEAD, "burst_at": burst_at or 0, "burst_every": 10_000,
                "burst_bytes": 300_000, "canary": burst_at is None}

    def phases(self, cfg, path):
        c = cfg.clients[path]
        b, m67, mall = TRIAL_SECONDS["bulk"], TRIAL_SECONDS["mixed-67"], TRIAL_SECONDS["mixed-all"]
        return [
            # A: four downloads, 60 s.
            w.Phase("bulk", b, [w.Job(c, "bulk", 4, params={"expected": cfg.object_bytes})]),
            # B: two downloads from 5 s before the foreground; the LLM loop
            # and the keystrokes for 90 s. The window is a cap for the jobs'
            # timeouts; `expected` is what the trial normally takes.
            w.Phase("mixed-67", m67 + 60, expected=m67 + LEAD, jobs=[
                w.Job(c, "wl-bgdown", 2, load=False),
                w.Job(c, "wl-llm", 1, params=self.llm("loop", LEAD)),
                w.Job(c, "wl-term", 1, params=self.term(m67, 40)),
            ]),
            # C: one download and one upload from 5 s before; the call for the
            # whole 120 s, one h2 page on cold connections at 20 s, one LLM
            # call at 50 s, keystrokes without bursts.
            w.Phase("mixed-all", mall + 60, expected=mall + LEAD, jobs=[
                w.Job(c, "wl-bgdown", 1, load=False),
                w.Job(c, "wl-bgup", 1, load=False, params={"object_bytes": cfg.scenario_upload_object}),
                w.Job(c, "wl-call", 1, params={"seconds": mall, "delay": LEAD}),
                w.Job(c, "wl-term", 1, params=self.term(mall, None)),
                w.Job(c, "wl-browse", 1, params={"delay": LEAD + 20, "seed": 7, "mode": "h2"}),
                w.Job(c, "wl-llm", 1, params=self.llm("single", LEAD + 50)),
            ]),
        ]

    def has_evidence(self, ph, recs):
        # Present, failed or not: a failed session is a measured failure.
        kinds = {r.get("kind") for r in recs}
        need = {"bulk": set(), "mixed-67": {"llm", "term"}, "mixed-all": {"llm", "term", "call", "browse"}}.get(ph, set())
        return need <= kinds

    def under_background(self, ph, recs, rows, trials):
        for t in trials:
            trecs = [x for x in getattr(t, "records", []) if x.get("_phase") == ph and x.get("kind") in FOREGROUND]
            span = pa.fg_span(trecs)
            precs = [x for x in getattr(t, "records", []) if x.get("_phase") == ph]
            down, up = pa.progress_flows(precs, "down"), pa.progress_flows(precs, "up")
            flows = 2 if ph == "mixed-67" else 1
            if span is None or not pa.progress_covered(span, down, flows) or \
                    (ph == "mixed-all" and not pa.progress_covered(span, up, 1)):
                return False
        return True

    def block_tails(self, trials, plan):
        out: Dict[str, Tuple[Optional[float], List[float]]] = {}
        # The generic secondaries' raw evidence, for pooling over blocks:
        # credit delay as samples (a p95), the promotions as 0/1 events (a
        # proportion, q None). Absent when any of the type's trials lacks it.
        for ph in self.generic:
            ms = [getattr(t, "m", {}) for t in trials if getattr(t, "trial", "") == ph]
            if not ms:
                continue
            h = gm.pool([m.get("_credit_hist") for m in ms])
            if h is not None:
                out[f"{ph}.credit_delay_p95_s"] = (0.95, gm.Tail(h))
            ev = gm.outcomes(gm.pooled_promotions([m.get("_promotions") for m in ms]))
            if ev is not None:
                out[f"{ph}.false_promotion_rate"] = (None, ev)
        for ph in ("mixed-67", "mixed-all"):
            pt = [t for t in trials if getattr(t, "trial", "") == ph]
            if not pt:
                continue
            recs = [r for t in pt for r in getattr(t, "records", []) if r.get("_phase") == ph]
            for k, v in pa.llm_samples([r for r in recs if r.get("kind") == "llm"]).items():
                out[f"{ph}.{k}"] = (pa.LLM_TAILS[k], v)
            ts_ = pa.term_samples([r for r in recs if r.get("kind") == "term"])
            if ph == "mixed-67":
                out[f"{ph}.echo_p99_a_s"] = (0.99, ts_["a"])
                out[f"{ph}.echo_p99_c_s"] = (0.99, ts_["c"])
            else:
                out[f"{ph}.echo_p99_s"] = (0.99, ts_["a"])
                cs = call_samples([r for r in recs if r.get("kind") == "call"])
                out[f"{ph}.call_lateness_p99_up_s"] = (0.99, cs["up"])
                out[f"{ph}.call_lateness_p99_down_s"] = (0.99, cs["down"])
                out[f"{ph}.plt_p95_s"] = (0.95, page_times([r for r in recs if r.get("kind") == "browse"]))
        return out

    def phase_measures(self, ph, recs, rows, trials):
        nan = float("nan")
        if ph == "bulk":
            got = secs = 0.0
            for t in trials:
                b = [r for r in t.rows if r["phase"] == "bulk" and r["kind"] == "bulk"]
                got += sum(int(r["bytes"] or 0) for r in b if r.get("http") == "200")
                # Over the trial's fixed window, not the job's lifetime: an
                # arm whose flows died early must not score a higher rate.
                secs += TRIAL_SECONDS["bulk"] if b else 0.0
            return {"goodput_bps": got / secs if secs else nan}
        m: Dict[str, float] = {}
        m.update(pa.llm_metrics([r for r in recs if r.get("kind") == "llm"]))
        m["class_interactive"] = pa.class_interactive(trials, UPLOAD, ph)
        term = pa.term_metrics([r for r in recs if r.get("kind") == "term"])
        if ph == "mixed-67":
            m.update({k: v for k, v in term.items() if k in ("echo_p99_a_s", "echo_p99_c_s", "latch_ratio", "over1s_a",
                                                             "over1s_c", "sessions_failed")})
        else:
            # C's keystrokes are judged on echo time alone (no bursts).
            m["echo_p99_s"] = term.get("echo_p99_a_s", nan)
            m["over1s"] = term.get("over1s_a", nan)
            m["sessions_failed"] = term.get("sessions_failed", nan)
            # The call's measures are prefixed: the LLM call reports its own
            # freezes and failures in the same trial.
            call = call_metrics([r for r in recs if r.get("kind") == "call"])
            m.update({f"call_{k}": v for k, v in call.items()})
            m.update(browse_metrics([r for r in recs if r.get("kind") == "browse"]))
        # The primaries: the bulk flows' bytes over the foreground's window.
        # Read off each flow's progress timeline at the window's ends: bytes
        # moved in the lead or after the foreground are not credited to it.
        for direction, key in (("down", "bg_down_rate_bps"), ("up", "bg_up_rate_bps")):
            got = secs = 0.0
            for t in trials:
                precs = [x for x in getattr(t, "records", []) if x.get("_phase") == ph]
                span = pa.fg_span([x for x in precs if x.get("kind") in FOREGROUND])
                prog = pa.progress_flows(precs, direction)
                if span and prog:
                    g, w_ = pa.progress_in_window(prog, span)
                    got, secs = got + g, secs + w_
            if secs:
                m[key] = got / secs
        if not self.under_background(ph, recs, rows, trials):
            return {k: nan for k in m}
        return m


@w.register
class IdleWake(pa._Scenario):
    """D: one paired session per path, judged over its wakes, not over
    blocks. Run beside the minimal campaign's blocks (its `idle-wake`
    subdirectory), or as a campaign of its own."""
    name = "idle-wake"
    trials_per_arm = 1
    sample_based = True
    report_only = ("push_penalty_p95_s", "push_penalty_max_s", "send_penalty_p95_s", "send_penalty_max_s",
                   "deaths_eof", "deaths_rst", "deaths_timeout", "deaths_silent", "idle_eofs", "idle_eof_p50_s",
                   "sync_p50_s", "sync_max_s")

    def configure(self, cfg, cp):
        # D's arms run at once, as paired sessions beside the blocks (design
        # section 15.3); a campaign of D alone would run them one after the
        # other.
        raise ab.ConfigError("D (idle-wake) runs beside the minimal campaign's blocks: use workload = minimal "
                             "with [workload:minimal] idle_wake = true (the default with mixed-all), and judge "
                             "<campaign>/idle-wake")

    def phases(self, cfg, path):
        return [w.Phase("wake", wake_session_seconds() + 60, expected=wake_session_seconds(),
                        jobs=[w.Job(cfg.clients[path], "wl-wake", 1, params=wake_params())])]

    def has_evidence(self, ph, recs):
        return any(r.get("kind") == "wake" for r in recs)

    def samples_complete(self, trials: Sequence[Any]) -> bool:
        """Every trial has one record per planned connection (0..of-1), each
        with the planned wake time and direction: missing or malformed
        records must not leave a few connections to stand for 50."""
        for t in trials:
            recs = [r for r in getattr(t, "records", []) if r.get("kind") == "wake"]
            if not recs:
                return False
            of = recs[0].get("of")
            ids = [r.get("conn") for r in recs]
            if not isinstance(of, int) or sorted(ids) != list(range(of)):
                return False
            plan = wake_plan(of)
            if any((r.get("at"), r.get("mode")) != plan[r["conn"]] for r in recs):
                return False
        return True

    # Sample-based judging: the analyzer compares the arms' pooled samples.
    SAMPLES = {"delivered_1m": ("delivered_60", "mean"), "delivered_5m": ("delivered_300", "mean"),
               "delivered_16m": ("delivered_960", "sum"), "silent_deaths": ("silent", "sum"),
               "push_penalty_p50_s": ("push_penalty", 0.5), "send_penalty_p50_s": ("send_penalty", 0.5),
               "push_penalty_p95_s": ("push_penalty", 0.95), "push_penalty_max_s": ("push_penalty", "max"),
               "send_penalty_p95_s": ("send_penalty", 0.95), "send_penalty_max_s": ("send_penalty", "max"),
               "deaths_eof": ("death_eof", "sum"), "deaths_rst": ("death_rst", "sum"),
               "deaths_timeout": ("death_timeout", "sum"), "deaths_silent": ("death_silent", "sum"),
               "idle_eofs": ("idle_eof", "count"), "idle_eof_p50_s": ("idle_eof", 0.5),
               "sync_p50_s": ("sync_seconds", 0.5), "sync_max_s": ("sync_seconds", "max")}

    def samples(self, trials: Sequence[Any]) -> Dict[str, List[float]]:
        recs = [r for t in trials for r in getattr(t, "records", []) if r.get("kind") == "wake"]
        return wake_samples(recs)

    def phase_measures(self, ph, recs, rows, trials):
        s = wake_samples([r for r in recs if r.get("kind") == "wake"])
        return {key: sample_stat(s[src], stat) for key, (src, stat) in self.SAMPLES.items()}


def sample_stat(xs: Sequence[float], stat) -> float:
    if stat == "count":
        return float(len(xs))
    if stat == "sum":
        return float(sum(xs))
    if not xs:
        return float("nan")
    if stat == "mean":
        return sum(xs) / len(xs)
    if stat == "max":
        return max(xs)
    return st.quantile(xs, stat)

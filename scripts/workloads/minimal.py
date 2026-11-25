"""The minimal live profile: four trial types that share blocks, sized so a
path's gates fit in about 2.5-3 hours (docs/VALIDATING-A-BUNDLE.md,
"The minimal profile").

* `minimal`, one trial = three phases, each judged as its own cell:
  - `bulk` (A): four downloads, 60 s. Gates stages 6 and 7.
  - `mixed-67` (B): a 5-turn LLM loop (100 KB prompt) and a terminal session
    with one 300 KB burst, both under two bulk downloads, about 90 s.
    Gates stages 6 and 7.
  - `mixed-all` (C): a video call (audio and about 3 Mbit/s of video each
    way, open loop), one browsing burst, one LLM call, terminal keystrokes,
    one bulk download and one bulk upload, all at once, 120 s. Gates
    stage 10.
* `idle-wake` (D): 50 idle connections woken by the far side at 1, 5 and
  16 minutes, and by the client at 3 and 10 minutes; one session per arm.
  Gates stage 10.

The metrics are computed here from wlclient's raw records; the rules are in
gates.ini [workload:minimal] and [workload:idle-wake], provisional.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Sequence

import ab_stats as st
import ab_workloads as w

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


def call_metrics(calls: Sequence[dict]) -> Dict[str, float]:
    """W7 metrics over a set of call records. Per direction, from the
    reflector's stamps: lateness on that direction's own clock pair
    (clock-free), its p99, and late loss = packets lost or later than 150 ms
    past the best, over packets sent. Losses are split by the reflector's
    count: packets it never received were lost up; packets it received that
    never came back were lost down. Frame loss, freezes and audio gaps are
    judged round trip: a packet is good only if it came back and was on time
    both ways."""
    nan = float("nan")
    ok = [c for c in calls if not c.get("fail")]
    if not calls:
        return {}
    if not ok:
        # Every call failed to start: that is measured, as failures; the
        # quality measures have nothing to describe.
        return {"call_failures": 1.0}
    up_lat: List[float] = []
    down_lat: List[float] = []
    sent = reflected = lost_up = lost_down = late_up = late_down = 0
    audio_sent = audio_bad = 0
    frames = frames_bad = gaps = freezes = 0
    minutes = 0.0
    for c in ok:
        pk = c["audio"] + c["video"]
        back = [p for p in pk if len(p) == 4]
        sent += len(pk)
        received_up = min(len(pk), int(c.get("count") or len(back)))
        reflected += received_up
        lost_up += len(pk) - received_up
        lost_down += max(0, received_up - len(back))
        lu = _lateness([p[2] for p in back])
        ld = _lateness([p[3] for p in back])
        up_lat += lu
        down_lat += ld
        late_up += sum(1 for x in lu if x > LATE)
        late_down += sum(1 for x in ld if x > LATE)
        good = {}
        for p, a, b in zip(back, lu, ld):
            good[id(p)] = a <= LATE and b <= LATE

        def ok_(p):
            return len(p) == 4 and good.get(id(p), False)
        a_bad = [not ok_(p) for p in c["audio"]]
        audio_sent += len(a_bad)
        audio_bad += sum(a_bad)
        gaps += _runs(a_bad, AUDIO_GAP)
        by_frame: Dict[int, bool] = {}
        for p in c["video"]:
            by_frame[p[0]] = by_frame.get(p[0], True) and ok_(p)
        frame_bad = [not by_frame[f] for f in sorted(by_frame)]
        frames += len(frame_bad)
        frames_bad += sum(frame_bad)
        freezes += _runs(frame_bad, FREEZE_FRAMES)
        minutes += float(c.get("seconds") or 0) / 60.0
    return {
        "lateness_p99_up_s": st.quantile(up_lat, 0.99), "lateness_p99_down_s": st.quantile(down_lat, 0.99),
        "late_loss_up": (lost_up + late_up) / sent if sent else nan,
        # Downlink: over the packets the reflector sent back, the only ones
        # that could arrive.
        "late_loss_down": (lost_down + late_down) / reflected if reflected else nan,
        "audio_late_loss": audio_bad / audio_sent if audio_sent else nan,
        "frame_loss": frames_bad / frames if frames else nan,
        "audio_gaps_per_min": gaps / minutes if minutes else nan,
        "freezes_per_min": freezes / minutes if minutes else nan,
        "call_failures": (len(calls) - len(ok)) / len(calls),
    }


# --- browsing ----------------------------------------------------------------------

def browse_metrics(pages: Sequence[dict]) -> Dict[str, float]:
    """W3 metrics over a set of page loads: page load time (first request to
    the last object's last byte, failed objects included at the time they
    failed), the HTML's first byte, and the fraction of objects that failed.
    A page whose connection never opened counts as fully failed."""
    nan = float("nan")
    if not pages:
        return {}
    plt, html, failed, total = [], [], 0, 0
    for p in pages:
        objs = p.get("objects") or []
        if p.get("fail") or not objs:
            failed += 41
            total += 41
            continue
        total += len(objs)
        failed += sum(1 for o in objs if o[5]) + max(0, 41 - len(objs))
        plt.append(max(o[4] for o in objs) - min(o[2] for o in objs))
        h = next((o for o in objs if o[0] == 0), None)
        if h and h[3] is not None:
            html.append(h[3] - h[2])
    return {"plt_p50_s": st.quantile(plt, 0.5), "html_first_byte_p50_s": st.quantile(html, 0.5),
            "objects_failed": failed / total if total else nan}


# --- idle wake ----------------------------------------------------------------------

def wake_samples(conns: Sequence[dict]) -> Dict[str, List[float]]:
    """Per-wake samples from a session's connections.

    `delivered`: 1 for a wake push that arrived within 5 s of its time, 0 for
    one that did not (or whose connection had died). `push_penalty`: the
    wake push's delay over the best of its own follow-ups (clock-free).
    `send_penalty`: the round trip of a client send after idle over the best
    of its two warm sends. `silent_deaths`: 1 per connection that lost a
    wake with no EOF or error ever seen locally."""
    out: Dict[str, List[float]] = {"delivered": [], "push_penalty": [], "send_penalty": [], "silent_deaths": []}
    for c in conns:
        wakes = c.get("wakes") or []
        follow = int(c.get("follow") or 0)
        group = follow + 1
        pushes = {p[0]: p for p in c.get("pushes") or []}
        missed = False
        for gi, at in enumerate(wakes):
            base = gi * group
            wp = pushes.get(base)
            ok = wp is not None and not c.get("fail") and wp[2] <= at + WAKE_WITHIN
            out["delivered"].append(1.0 if ok else 0.0)
            missed |= not ok
            if wp is not None:
                warm = [pushes[base + j][2] - pushes[base + j][1] / 1000.0 for j in range(1, group) if base + j in pushes]
                if warm:
                    out["push_penalty"].append((wp[2] - wp[1] / 1000.0) - min(warm))
        # Sends are numbered 3k+1 (after idle), 3k+2 and 3k+3 (warm): group by
        # scheduled id, so a lost acknowledgement cannot shift the trios.
        by_id = {s[0]: s for s in c.get("sends") or []}
        trios = max((n - 1) // 3 for n in by_id) + 1 if by_id else 0
        for k in range(trios):
            cold, warm = by_id.get(3 * k + 1), [by_id[i] for i in (3 * k + 2, 3 * k + 3) if i in by_id]
            if cold and warm:
                out["send_penalty"].append(cold[2] - min(x[2] for x in warm))
        out["silent_deaths"].append(1.0 if missed and not c.get("end") and not c.get("fail") else 0.0)
    return out


# --- generators ----------------------------------------------------------------------

def _scenario_port(cfg, which):
    return {"http": cfg.scenario_http_port, "term": cfg.scenario_term_port, "udp": cfg.scenario_udp_port,
            "wake": cfg.scenario_wake_port}[which]


def call_script(campaign, job, arm, duration, start_at=0):
    p = job.params
    return pa._client(campaign, job, arm, "call", _scenario_port(campaign.cfg, "udp"),
                      f"--duration {p['seconds']} --delay {p.get('delay', 0)} --no-tls")


def browse_script(campaign, job, arm, duration, start_at=0):
    p = job.params
    return pa._client(campaign, job, arm, "browse", _scenario_port(campaign.cfg, "http"),
                      f"--delay {p.get('delay', 0)} --timeout {p.get('timeout', 30)} --seed {p.get('seed', 1)}")


def wake_script(campaign, job, arm, duration, start_at=0):
    p = job.params
    return pa._client(campaign, job, arm, "wake", _scenario_port(campaign.cfg, "wake"),
                      f"--conns {p['conns']} --sync {p['sync']} --wakes {p['wakes']} --sends {p['sends']} "
                      f"--follow {p['follow']} --duration {int(duration)}")


for _name, _fn in (("wl-call", call_script), ("wl-browse", browse_script), ("wl-wake", wake_script)):
    w.generator(_name, w.Generator(_fn, pa.parse_rows, lambda job: 0, w.json_records))


# --- workloads --------------------------------------------------------------------

@w.register
class Minimal(pa._Scenario):
    """Trial types A (bulk), B (mixed-67) and C (mixed-all), one trial each
    per arm per block, judged per cell."""
    name = "minimal"
    report_only = pa.LLM_REPORT_ONLY + pa.TERM_REPORT_ONLY + ("html_first_byte_p50_s",)

    def read(self, cfg, p):
        cfg.scenario_udp_port = int(p.get("udp_port", "23487"))
        cfg.scenario_wake_port = int(p.get("wake_port", "23488"))
        cfg.minimal = {"bulk_seconds": int(p.get("bulk_seconds", "60")),
                       "mixed67_seconds": int(p.get("mixed67_seconds", "90")),
                       "mixedall_seconds": int(p.get("mixedall_seconds", "120")),
                       "upload_kb": int(p.get("upload_kb", "100"))}

    def positive(self, cfg):
        return super().positive(cfg) + [(k, v) for k, v in cfg.minimal.items()]

    def endpoints(self, cfg):
        tls = "" if cfg.scenario_tls else " --no-tls"
        return [w.Endpoint("wlserver", f"python3 {{W}}/wlserver.py --http {cfg.scenario_http_port} "
                                       f"--term {cfg.scenario_term_port} --udp {cfg.scenario_udp_port} --dir {{W}}{tls}",
                           tcp_ports=[(cfg.scenario_http_port, "http"), (cfg.scenario_term_port, "terminal")],
                           udp_ports=[(cfg.scenario_udp_port, "call reflector")], files=[pa.SERVER])]

    def llm(self, cfg, seq: str, delay: float) -> dict:
        return {"sequence": seq, "upload": cfg.minimal["upload_kb"] * 1000, "calls": 1, "turns": 5, "agents": 1,
                "think_ms": 500, "deltas": 400, "cadence_ms": 25, "timeout": 30, "delay": delay}

    def term(self, seconds: float, delay: float, burst_at: float) -> dict:
        return {"seconds": seconds, "delay": delay, "burst_at": burst_at, "burst_every": 10_000,
                "burst_bytes": 300_000, "canary": False}

    def phases(self, cfg, path):
        c = cfg.clients[path]
        m = cfg.minimal
        return [
            w.Phase("bulk", m["bulk_seconds"], [w.Job(c, "bulk", 4, params={"expected": cfg.object_bytes})]),
            # The windows below are caps for the jobs' timeouts; `expected`
            # is what a phase normally takes, for the wall-time estimate.
            w.Phase("mixed-67", m["mixed67_seconds"] + 60, expected=m["mixed67_seconds"] + 5, jobs=[
                w.Job(c, "wl-bgdown", 2, load=False),
                w.Job(c, "wl-llm", 1, params=self.llm(cfg, "loop", 5)),
                w.Job(c, "wl-term", 1, params=self.term(m["mixed67_seconds"], 5, 40)),
            ]),
            w.Phase("mixed-all", m["mixedall_seconds"] + 60, expected=m["mixedall_seconds"] + 5, jobs=[
                w.Job(c, "wl-bgdown", 1, load=False),
                w.Job(c, "wl-bgup", 1, load=False, params={"object_bytes": cfg.scenario_upload_object}),
                w.Job(c, "wl-call", 1, params={"seconds": m["mixedall_seconds"], "delay": 5}),
                w.Job(c, "wl-term", 1, params=dict(self.term(m["mixedall_seconds"], 5, 0), canary=True)),
                w.Job(c, "wl-browse", 1, params={"delay": 25, "seed": 7}),
                w.Job(c, "wl-llm", 1, params=self.llm(cfg, "single", 45)),
            ]),
        ]

    def has_evidence(self, ph, recs):
        # Present, failed or not: a failed session is a measured failure.
        kinds = {r.get("kind") for r in recs}
        need = {"bulk": set(), "mixed-67": {"llm", "term"}, "mixed-all": {"llm", "term", "call", "browse"}}.get(ph, set())
        return need <= kinds

    def under_background(self, ph, recs, rows, trials):
        for t in trials:
            trecs = [x for x in getattr(t, "records", []) if x.get("_phase") == ph and x.get("kind") in ("llm", "term", "call")]
            span = pa.fg_span(trecs)
            bg = [r for r in t.rows if r["phase"] == ph and r["kind"] in ("wl-bgdown", "wl-bgup")]
            down = [r for r in bg if r["kind"] == "wl-bgdown"]
            up = [r for r in bg if r["kind"] == "wl-bgup"]
            flows = 2 if ph == "mixed-67" else 1
            if span is None or not pa.covered_each(span, down, flows) or (ph == "mixed-all" and not pa.covered_each(span, up, 1)):
                return False
        return True

    def phase_measures(self, ph, recs, rows, trials):
        nan = float("nan")
        if ph == "bulk":
            got = secs = 0.0
            for t in trials:
                b = [r for r in t.rows if r["phase"] == "bulk" and r["kind"] == "bulk"]
                got += sum(int(r["bytes"] or 0) for r in b if r.get("http") == "200")
                walls = [float(r["wall"]) for r in b if r.get("wall")]
                secs += max(walls) if walls else 0.0
            return {"goodput_bps": got / secs if secs else nan}
        m: Dict[str, float] = {}
        m.update(pa.llm_metrics([r for r in recs if r.get("kind") == "llm"]))
        term = pa.term_metrics([r for r in recs if r.get("kind") == "term"])
        if ph == "mixed-67":
            m.update({k: v for k, v in term.items() if k in ("echo_p99_a_s", "echo_p99_c_s", "latch_ratio", "over1s_a",
                                                             "over1s_c", "sessions_failed")})
        else:
            m["echo_p99_s"] = term.get("echo_p99_a_s", nan)
            # The call's measures are prefixed: the LLM call reports its own
            # freezes and failures in the same trial.
            call = call_metrics([r for r in recs if r.get("kind") == "call"])
            call["sessions_failed"] = call.pop("call_failures", nan)
            m.update({f"call_{k}": v for k, v in call.items()})
            m.update(browse_metrics([r for r in recs if r.get("kind") == "browse"]))
        for kind, key in (("wl-bgdown", "bg_down_rate_bps"), ("wl-bgup", "bg_up_rate_bps")):
            got = secs = 0.0
            unknown = False
            for t in trials:
                trows = [r for r in t.rows if r["phase"] == ph and r["kind"] == kind]
                unknown |= any(r.get("curl_exit") == pa.SINK_UNKNOWN for r in trows)
                span = pa.fg_span([x for x in getattr(t, "records", []) if x.get("_phase") == ph
                                   and x.get("kind") in ("llm", "term", "call")])
                if span and trows:
                    g, w_ = pa.rate_in_window(trows, span)
                    got, secs = got + g, secs + w_
            if secs and not unknown:
                m[key] = got / secs
        m.pop("class_interactive", None)
        if not self.under_background(ph, recs, rows, trials):
            return {k: nan for k in m}
        return m


@w.register
class IdleWake(pa._Scenario):
    """D: one session per arm; judged over its wakes, not over blocks."""
    name = "idle-wake"
    trials_per_arm = 1
    sample_based = True
    report_only = ()

    def read(self, cfg, p):
        cfg.scenario_wake_port = int(p.get("wake_port", "23488"))
        cfg.scenario_udp_port = int(p.get("udp_port", "23487"))
        cfg.wake = {"conns": int(p.get("conns", "50")), "sync": int(p.get("sync_bytes", "200000")),
                    "wakes": p.get("wakes", "60,300,960"), "sends": p.get("sends", "180,600"),
                    "follow": int(p.get("follow", "5"))}

    def positive(self, cfg):
        return [("conns", cfg.wake["conns"])]

    def objects(self, cfg):
        return []

    def endpoints(self, cfg):
        tls = "" if cfg.scenario_tls else " --no-tls"
        return [w.Endpoint("wlserver", f"python3 {{W}}/wlserver.py --http {cfg.scenario_http_port} "
                                       f"--term {cfg.scenario_term_port} --wake {cfg.scenario_wake_port} --dir {{W}}{tls}",
                           tcp_ports=[(cfg.scenario_http_port, "http"), (cfg.scenario_term_port, "terminal"),
                                      (cfg.scenario_wake_port, "wake")], files=[pa.SERVER])]

    def session_seconds(self, cfg) -> float:
        times = [float(x) for x in (cfg.wake["wakes"] + "," + cfg.wake["sends"]).split(",") if x]
        return max(times) + cfg.wake["follow"] + 20

    def phases(self, cfg, path):
        return [w.Phase("wake", self.session_seconds(cfg) + 60, expected=self.session_seconds(cfg),
                        jobs=[w.Job(cfg.clients[path], "wl-wake", 1, params=dict(cfg.wake))])]

    def has_evidence(self, ph, recs):
        return any(r.get("kind") == "wake" for r in recs)

    def samples_complete(self, trials: Sequence[Any]) -> bool:
        """Every trial has one record per planned connection (0..of-1), each
        with the same wake schedule: missing or malformed records must not
        leave a few connections to stand for 50."""
        for t in trials:
            recs = [r for r in getattr(t, "records", []) if r.get("kind") == "wake"]
            if not recs:
                return False
            of = recs[0].get("of")
            ids = [r.get("conn") for r in recs]
            if not isinstance(of, int) or sorted(ids) != list(range(of)):
                return False
            if any(r.get("wakes") != recs[0].get("wakes") for r in recs):
                return False
        return True

    # Sample-based judging: the analyzer compares the arms' pooled samples.
    SAMPLES = {"delivered": ("delivered", "mean"), "push_penalty_p50_s": ("push_penalty", 0.5),
               "push_penalty_p99_s": ("push_penalty", 0.99), "send_penalty_p50_s": ("send_penalty", 0.5),
               "send_penalty_p99_s": ("send_penalty", 0.99), "silent_deaths": ("silent_deaths", "sum")}

    def samples(self, trials: Sequence[Any]) -> Dict[str, List[float]]:
        recs = [r for t in trials for r in getattr(t, "records", []) if r.get("kind") == "wake"]
        return wake_samples(recs)

    def phase_measures(self, ph, recs, rows, trials):
        s = wake_samples([r for r in recs if r.get("kind") == "wake"])
        out = {}
        for key, (src, stat) in self.SAMPLES.items():
            out[key] = sample_stat(s[src], stat)
        return out


def sample_stat(xs: Sequence[float], stat) -> float:
    if not xs:
        return float("nan")
    if stat == "mean":
        return sum(xs) / len(xs)
    if stat == "sum":
        return float(sum(xs))
    return st.quantile(xs, stat)

"""Phase A scenario workloads (WORKLOADS.md 6.1): W5 LLM streaming calls, W6
terminal keystrokes with output bursts, and the mixed scenarios M1 and M2.

The far side is scripts/validation/wlserver.py on the gateway host's
loopback; the client side is scripts/validation/wlclient.py on each client
host. Every metric is computed here, from the raw timings wlclient records,
so the definitions can be revised and re-run on old campaigns. The rules
that judge them are in scripts/validation/gates.ini, [workload:llm],
[workload:term] and [workload:mixed], all provisional until a pilot.

Workloads and their cells (phases), each judged separately:

* `llm`: <sequence>-<U>k-<background> for every sequence (single, loop,
  swarm), prompt size U (KB) and background (none, down4, up1) configured.
* `term`: term-<background>.
* `mixed`: m1-<U>k (W5-single under four bulk downloads) and m2-<U>k
  (W5-single under a bulk upload), each with W6 phase-A keystrokes as a
  canary.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Sequence, Tuple

import ab_campaign as ab
import ab_stats as st
import ab_workloads as w

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVER = os.path.join(HERE, "validation", "wlserver.py")
CLIENT = os.path.join(HERE, "validation", "wlclient.py")

STALL = 0.5          # an inter-delta gap this long is a visible hesitation (WORKLOADS.md W5)
FREEZE = 2.0         # and this long a freeze
STALL_EXCESS = 0.25  # gaps count toward the stall-time ratio beyond this
SLOW_KEY = 1.0       # a keystroke echo slower than this is "over 1 s" (W6)
AFTER_BURST = 2.0    # keystrokes this soon after a burst are left out of phase C (W6)
COVERAGE = 0.9       # a background must cover this much of the foreground's span


# --- metrics (pure functions over wlclient's records) ---------------------------------

def lateness(deltas: Sequence[Sequence[float]]) -> List[float]:
    """Clock-free lateness of one call's deltas (WORKLOADS.md 3.0): with
    delta_i = r_i - s_i (client receive time minus server emission time, on
    two unsynchronised clocks), L_i = delta_i - min_j delta_j, in seconds.
    `deltas` holds [s_i in server ms, r_i in client seconds]."""
    d = [r - s / 1000.0 for s, r in deltas]
    if not d:
        return []
    m = min(d)
    return [x - m for x in d]


def gaps(deltas: Sequence[Sequence[float]]) -> List[float]:
    """Inter-delta gaps g_i = r_i - r_{i-1} at the client, in seconds."""
    r = [x[1] for x in deltas]
    return [b - a for a, b in zip(r, r[1:])]


def llm_metrics(calls: Sequence[dict]) -> Dict[str, float]:
    """W5 metrics over a set of calls (a block's, pooled across the arm's
    trials): TTFT_net quantiles, lateness quantiles, gap tail, stalls and
    freezes per streamed minute, stall-time ratio, completion excess, upload
    time R, connect time, and the call failure fraction."""
    nan = float("nan")
    if not calls:
        return {}
    ok = [c for c in calls if not c.get("fail")]
    ttft_net = [c["ttft"] - c["T"] for c in ok if c.get("ttft") is not None]
    late: List[float] = []
    g: List[float] = []
    streamed = 0.0
    excess = []
    for c in ok:
        late += lateness(c["deltas"])
        g += gaps(c["deltas"])
        if len(c["deltas"]) > 1:
            streamed += c["deltas"][-1][1] - c["deltas"][0][1]
        excess.append(c["done"] - (c["T"] + c["N"] * c["c"]))
    minutes = streamed / 60.0
    q = st.quantile
    return {
        "ttft_net_p50_s": q(ttft_net, 0.5), "ttft_net_p95_s": q(ttft_net, 0.95),
        "lateness_p50_s": q(late, 0.5), "lateness_p95_s": q(late, 0.95), "lateness_p99_s": q(late, 0.99),
        "lateness_max_s": max(late) if late else nan,
        "gap_p99_s": q(g, 0.99), "gap_max_s": max(g) if g else nan,
        "stalls_per_min": sum(1 for x in g if x >= STALL) / minutes if minutes > 0 else nan,
        "freezes_per_min": sum(1 for x in g if x >= FREEZE) / minutes if minutes > 0 else nan,
        "stall_time_ratio": sum(max(0.0, x - STALL_EXCESS) for x in g) / streamed if streamed > 0 else nan,
        "completion_excess_p50_s": q(excess, 0.5),
        "upload_r_p50_s": q([c["R"] for c in ok if c.get("R") is not None], 0.5),
        "connect_p50_s": q([c["connect"] for c in calls if c.get("connect") is not None], 0.5),
        "call_failures": (len(calls) - len(ok)) / len(calls),
    }


def term_metrics(sessions: Sequence[dict]) -> Dict[str, float]:
    """W6 metrics over a set of sessions: echo time quantiles and the
    fraction over 1 s for phase A (before the first burst) and phase C
    (after it, leaving out keystrokes within 2 s after any burst), and the
    latch ratio p99(C) / p99(A). A canary session has phase A only."""
    nan = float("nan")
    a: List[float] = []
    c: List[float] = []
    unanswered = 0
    for s in sessions:
        if s.get("fail"):
            continue
        bursts = s.get("bursts") or []
        first = bursts[0] if bursts else float("inf")
        unanswered += int(s.get("unanswered") or 0)
        for sent, echo in s.get("keys") or []:
            if sent < first:
                a.append(echo)
            elif not any(b <= sent < b + AFTER_BURST for b in bursts):
                c.append(echo)
    q = st.quantile
    out = {"echo_p50_a_s": q(a, 0.5), "echo_p95_a_s": q(a, 0.95), "echo_p99_a_s": q(a, 0.99),
           "over1s_a": sum(1 for x in a if x > SLOW_KEY) / len(a) if a else nan,
           "sessions_failed": sum(1 for s in sessions if s.get("fail")) / len(sessions) if sessions else nan}
    if any(s.get("bursts") for s in sessions):
        p99a, p99c = q(a, 0.99), q(c, 0.99)
        out.update({"echo_p50_c_s": q(c, 0.5), "echo_p95_c_s": q(c, 0.95), "echo_p99_c_s": p99c,
                    "over1s_c": sum(1 for x in c if x > SLOW_KEY) / len(c) if c else nan,
                    "latch_ratio": p99c / p99a if p99a and p99a == p99a and p99c == p99c else nan})
    return out


def covered(fg: Tuple[float, float], bg_rows: Sequence[dict]) -> bool:
    """The background was running for at least COVERAGE of the
    foreground's span (its rows' wall-clock intervals, merged)."""
    spans = sorted((float(r["t_start"]), float(r["t_end"])) for r in bg_rows if r.get("t_start") and r.get("t_end"))
    if not spans or fg[1] <= fg[0]:
        return False
    merged: List[List[float]] = []
    for a, b in spans:
        if merged and a <= merged[-1][1] + 1.0:   # back-to-back objects of one flow
            merged[-1][1] = max(merged[-1][1], b)
        else:
            merged.append([a, b])
    inside = sum(max(0.0, min(b, fg[1]) - max(a, fg[0])) for a, b in merged)
    return inside >= COVERAGE * (fg[1] - fg[0])


def fg_span(records: Sequence[dict]) -> Optional[Tuple[float, float]]:
    """The foreground's wall-clock span: each record's t0 to its real end
    (t1), connection setup and failed calls included."""
    ends = []
    for r in records:
        t0 = r.get("t0")
        if t0 is None:
            continue
        if r.get("t1") is not None:
            ends.append((t0, float(r["t1"])))
            continue
        dur = r.get("done") or (r["keys"][-1][0] + r["keys"][-1][1] if r.get("keys") else 0)
        ends.append((t0, t0 + float(dur or 0)))
    if not ends:
        return None
    return min(a for a, _ in ends), max(b for _, b in ends)


def covered_each(fg: Tuple[float, float], bg_rows: Sequence[dict], flows: int) -> bool:
    """Every one of the background's `flows` flows was transferring across
    the foreground: per flow, its rows that moved bytes must cover it. One
    surviving download of four is not a four-download background."""
    by_flow: Dict[str, List[dict]] = {}
    for r in bg_rows:
        if int(r.get("bytes") or 0) > 0:
            by_flow.setdefault(r["flow"].split("o", 1)[0], []).append(r)
    return len(by_flow) >= flows and all(covered(fg, rs) for rs in by_flow.values())


def useful_bg(rows: Sequence[dict]) -> float:
    return float(sum(int(r["bytes"] or 0) for r in rows if r.get("http") == "200"))


SINK_UNKNOWN = "99"   # wlclient: a cut upload whose received bytes could not be learned


def rate_in_window(rows: Sequence[dict], window: Tuple[float, float]) -> Tuple[float, float]:
    """(useful bytes, seconds) the background delivered within `window`:
    each transfer's bytes prorated over its own interval and clipped to the
    window. The window is the foreground's span, so two arms whose
    foregrounds take different times are compared as rates, not as totals
    over different durations."""
    a, b = window
    got = 0.0
    for r in rows:
        if r.get("http") != "200" or not r.get("t_start") or not r.get("t_end"):
            continue
        s, e = float(r["t_start"]), float(r["t_end"])
        n = float(r["bytes"] or 0)
        if e <= s:
            continue
        got += n * max(0.0, min(e, b) - max(s, a)) / (e - s)
    return got, max(0.0, b - a)


def prompt_bytes(recs: Sequence[dict]) -> int:
    """The smallest prompt the phase's LLM calls uploaded (U)."""
    us = [int(r["U"]) for r in recs if r.get("kind") == "llm" and r.get("U")]
    return min(us) if us else 1 << 62


def class_interactive(trials: Sequence[Any], upload: int, phase: str) -> float:
    """Class correctness of the LLM's response (W5) in one phase: the
    fraction of DATA payload the gateway sent on the LLM's flows with the
    INTERACTIVE class byte, from its flow completion records (per-flow class
    telemetry). Only records that completed within the phase's LLM calls
    (from their first start to 5 s after their last end, on the trial's
    wall clock) count, so another phase of the same trial cannot supply or
    mask them. Within that window the LLM's flows are the ones that carried
    a prompt upward (at least U bytes from the client, less than 4 MB) and a
    streamed response down. NaN when a trial has no such record or no class
    telemetry."""
    inter = total = 0
    for t in trials:
        calls = [r for r in getattr(t, "records", []) or [] if r.get("_phase") == phase and r.get("kind") == "llm"
                 and r.get("t0") is not None]
        if not calls:
            return float("nan")
        lo = min(float(r["t0"]) for r in calls) - 1.0
        hi = max(float(r.get("t1") or r["t0"]) for r in calls) + 5.0
        # The prompt up (at least U, under 4 MB) and the streamed response
        # down (at least 40 KB: 400 deltas of about 180 B): a cut background
        # upload carries the first but gets only a short reply.
        recs = [r for r in getattr(t, "flow_records", []) or []
                if lo <= float(r.get("_t", float("nan"))) <= hi
                and upload <= int(r.get("bytes_from_client") or 0) < 4_000_000
                and 40_000 <= int(r.get("bytes_to_client") or 0) < 4_000_000]
        if not recs:
            return float("nan")
        for r in recs:
            keys = [f"data_bytes_{c}_{s}" for c in ("new", "interactive", "bulk") for s in ("coded", "stream")]
            if not all(k in r for k in keys):
                return float("nan")
            inter += int(r["data_bytes_interactive_coded"]) + int(r["data_bytes_interactive_stream"])
            total += sum(int(r[k]) for k in keys)
    return inter / total if total else float("nan")



# --- generators ----------------------------------------------------------------------

def _client(campaign: Any, job: w.Job, arm: Any, cmd: str, target_port: int, extra: str) -> str:
    W = campaign.w(job.client.host)
    socks = job.client.socks_port_base + arm.index
    stop = f" --stop-file {campaign.stop_file(job.client, arm)}" if not job.load else ""
    tls = " --no-tls" if not campaign.cfg.scenario_tls else ""
    return (f"python3 {W}/wlclient.py {cmd} --socks 127.0.0.1:{socks} --target 127.0.0.1:{target_port}"
            f"{tls}{stop} {extra}")


def llm_script(campaign, job, arm, duration, start_at=0):
    p = job.params
    return _client(campaign, job, arm, "llm", campaign.cfg.scenario_http_port,
                   f"--sequence {p['sequence']} --upload {p['upload']} --calls {p['calls']} --turns {p['turns']} "
                   f"--agents {p['agents']} --think-ms {p['think_ms']} --deltas {p['deltas']} "
                   f"--cadence-ms {p['cadence_ms']} --timeout {p['timeout']} --delay {p['delay']} "
                   f"--duration {int(duration)} --seed {p.get('seed', 1)}")


def term_script(campaign, job, arm, duration, start_at=0):
    p = job.params
    canary = " --canary" if p.get("canary") else ""
    return _client(campaign, job, arm, "term", campaign.cfg.scenario_term_port,
                   f"--duration {p['seconds']} --delay {p['delay']} --burst-at {p['burst_at']} "
                   f"--burst-every {p['burst_every']} --burst-bytes {p['burst_bytes']}{canary}")


def bgdown_script(campaign, job, arm, duration, start_at=0):
    cfg = campaign.cfg
    return _client(campaign, job, arm, "bgdown", cfg.gateway.origin_port,
                   f"--flows {job.n} --path /obj-{cfg.object_bytes} --duration {int(duration)} --no-tls")


def bgup_script(campaign, job, arm, duration, start_at=0):
    return _client(campaign, job, arm, "bgup", campaign.cfg.scenario_http_port,
                   f"--flows {job.n} --object-bytes {job.params.get('object_bytes', 8 << 20)} --duration {int(duration)}")


def parse_rows(out: str, job: w.Job, duration: float) -> List[Tuple[str, ...]]:
    """wlclient's rows: t_start t_end flow http bytes seconds exit."""
    rows = []
    for line in out.splitlines():
        p = line.split("\t")
        if len(p) != 7 or p[0] == "J":
            continue
        t0, t1, flow, code, size, secs, ex = p
        rows.append((flow, code, size, secs, ex, t0, t1))
    if not rows and job.load:
        # Nothing at all: the job died. It still counts, as a failure.
        rows.append(("x0", "000", "0", f"{duration:.3f}", "-1", "", ""))
    return rows


for name, fn in (("wl-llm", llm_script), ("wl-term", term_script), ("wl-bgdown", bgdown_script), ("wl-bgup", bgup_script)):
    w.generator(name, w.Generator(fn, parse_rows, lambda job: 0, w.json_records))


# --- workloads --------------------------------------------------------------------

def _words(v: str) -> List[str]:
    return v.replace(",", " ").split()


LLM_REPORT_ONLY = ("lateness_p50_s", "lateness_p95_s", "lateness_max_s", "gap_p99_s", "gap_max_s",
                   "stall_time_ratio", "completion_excess_p50_s", "upload_r_p50_s", "connect_p50_s")
TERM_REPORT_ONLY = ("echo_p50_a_s", "echo_p95_a_s", "echo_p50_c_s", "echo_p95_c_s")


class _Scenario(w.Workload):
    """Shared: the wlserver endpoint, wlclient on every client host, a
    background of four downloads or one upload, and per-block pooling."""
    primary = False
    default_backgrounds = "none down4 up1"

    def configure(self, cfg, cp):
        p = self.params(cp)
        cfg.scenario_tls = ab._bool(p.get("tls", "true"))
        cfg.scenario_http_port = int(p.get("server_port", "23485"))
        cfg.scenario_term_port = int(p.get("term_port", "23486"))
        cfg.scenario_backgrounds = _words(p.get("backgrounds", self.default_backgrounds))
        for b in cfg.scenario_backgrounds:
            if b not in ("none", "down4", "up1"):
                raise ab.ConfigError(f"[workload:{self.name}] background {b!r} is not none, down4 or up1")
        cfg.scenario_phase_seconds = int(p.get("phase_seconds", "180"))
        cfg.scenario_upload_object = int(p.get("upload_object_bytes", str(8 << 20)))
        self.read(cfg, p)

    def read(self, cfg, p: Dict[str, str]) -> None:
        pass

    def positive(self, cfg):
        return [("object_bytes", cfg.object_bytes), ("phase_seconds", cfg.scenario_phase_seconds)]

    def objects(self, cfg):
        return [(f"obj-{cfg.object_bytes}", cfg.object_bytes)]

    def endpoints(self, cfg):
        tls = "" if cfg.scenario_tls else " --no-tls"
        return [w.Endpoint("wlserver", f"python3 {{W}}/wlserver.py --http {cfg.scenario_http_port} "
                                       f"--term {cfg.scenario_term_port} --dir {{W}}{tls}",
                           tcp_ports=[(cfg.scenario_http_port, "http"), (cfg.scenario_term_port, "terminal")],
                           files=[SERVER])]

    def client_files(self, cfg):
        return [CLIENT]

    def background(self, cfg, client, bg: str) -> List[w.Job]:
        if bg == "down4":
            return [w.Job(client, "wl-bgdown", 4, load=False)]
        if bg == "up1":
            return [w.Job(client, "wl-bgup", 1, load=False, params={"object_bytes": cfg.scenario_upload_object})]
        return []

    # Pooling: the per-block statistic of an arm pools its trials' records
    # (WORKLOADS.md 3.0), rather than averaging per-trial quantiles.
    def block_extract(self, trials: Sequence[Any], plan: dict) -> Dict[str, float]:
        out: Dict[str, float] = {}
        phases = sorted({r["phase"] for t in trials for r in t.rows} |
                        {t.trial for t in trials if getattr(t, "trial", "")})
        for ph in phases:
            # Only the trials that ran this phase: with trial types (one phase
            # each) an arm's block holds trials of other types too.
            # A trial of another type is told by its type, not by its rows, so
            # a trial whose jobs wrote nothing still counts as missing evidence.
            pt = [t for t in trials if not getattr(t, "trial", "") or t.trial == ph]
            recs = [rec for t in pt for rec in getattr(t, "records", []) if rec.get("_phase") == ph]
            rows = [r for t in pt for r in t.rows if r["phase"] == ph]
            m = self.phase_measures(ph, recs, rows, pt)
            # Before pooling: every trial must carry this phase's evidence, or
            # one trial's records would stand in for the block.
            if not all(self.has_evidence(ph, [x for x in getattr(t, "records", []) if x.get("_phase") == ph])
                       for t in pt):
                m = {k: float("nan") for k in m}
            for k, v in m.items():
                out[f"{ph}.{k}"] = v
        return out

    def extract(self, trial, plan):
        return self.block_extract([trial], plan)

    def has_evidence(self, ph: str, recs: List[dict]) -> bool:
        """The raw records one trial must have for phase ph."""
        return True

    def under_background(self, ph: str, recs: List[dict], rows: List[dict], trials: Sequence[Any]) -> bool:
        """For a phase with a background: did it run across the foreground
        in every trial? Unmeasured otherwise, never judged idle."""
        if not ph.endswith(("down4", "up1")) and not ph.startswith(("m1", "m2")):
            return True
        for t in trials:
            trecs = [x for x in getattr(t, "records", []) if x.get("_phase") == ph and x.get("kind") in ("llm", "term")
                     and not x.get("canary")]
            span = fg_span(trecs)
            bg = [r for r in t.rows if r["phase"] == ph and r["kind"] in ("wl-bgdown", "wl-bgup")]
            flows = 4 if ph.endswith("down4") or ph.startswith("m1") else 1
            if span is None or not covered_each(span, bg, flows):
                return False
        return True


@w.register
class LLM(_Scenario):
    """W5: LLM streaming calls."""
    name = "llm"
    report_only = LLM_REPORT_ONLY

    def read(self, cfg, p):
        cfg.llm = {
            "sequences": _words(p.get("sequences", "single loop")),
            "uploads_kb": [int(x) for x in _words(p.get("uploads_kb", "100 400"))],
            "calls": int(p.get("calls", "5")), "turns": int(p.get("turns", "5")), "agents": int(p.get("agents", "8")),
            "think_ms": float(p.get("think_ms", "500")), "deltas": int(p.get("deltas", "400")),
            "cadence_ms": float(p.get("cadence_ms", "25")), "timeout": float(p.get("timeout", "30")),
            "delay": float(p.get("background_lead", "5")),
        }
        for s in cfg.llm["sequences"]:
            if s not in ("single", "loop", "swarm"):
                raise ab.ConfigError(f"[workload:llm] sequence {s!r} is not single, loop or swarm")

    def call_params(self, cfg, seq: str, kb: int, bg: str) -> dict:
        p = dict(cfg.llm, sequence=seq, upload=kb * 1000)
        p["delay"] = p["delay"] if bg != "none" else 0
        return p

    def phases(self, cfg, path):
        client = cfg.clients[path]
        out = []
        for seq in cfg.llm["sequences"]:
            for kb in cfg.llm["uploads_kb"]:
                for bg in cfg.scenario_backgrounds:
                    jobs = self.background(cfg, client, bg) + \
                        [w.Job(client, "wl-llm", 1, params=self.call_params(cfg, seq, kb, bg))]
                    out.append(w.Phase(f"{seq}-{kb}k-{bg}", cfg.scenario_phase_seconds, jobs))
        return out

    def has_evidence(self, ph, recs):
        return any(r.get("kind") == "llm" for r in recs)

    def phase_measures(self, ph, recs, rows, trials):
        m = llm_metrics([r for r in recs if r.get("kind") == "llm"])
        if not self.under_background(ph, recs, rows, trials):
            return {k: float("nan") for k in m}
        # From the gateway's per-flow class telemetry (the model's R3).
        m["class_interactive"] = class_interactive(trials, prompt_bytes(recs), ph)
        return m


@w.register
class Term(_Scenario):
    """W6: terminal keystrokes with 300 KB output bursts."""
    name = "term"
    report_only = TERM_REPORT_ONLY

    def read(self, cfg, p):
        cfg.term = {"seconds": float(p.get("seconds", "120")), "burst_at": float(p.get("burst_at", "40")),
                    "burst_every": float(p.get("burst_every", "20")), "burst_bytes": int(p.get("burst_bytes", "300000")),
                    "delay": float(p.get("background_lead", "5"))}

    def phases(self, cfg, path):
        client = cfg.clients[path]
        out = []
        for bg in cfg.scenario_backgrounds:
            params = dict(cfg.term, delay=cfg.term["delay"] if bg != "none" else 0)
            out.append(w.Phase(f"term-{bg}", max(cfg.scenario_phase_seconds, cfg.term["seconds"] + 30),
                               self.background(cfg, client, bg) + [w.Job(client, "wl-term", 1, params=params)]))
        return out

    def has_evidence(self, ph, recs):
        return any(r.get("kind") == "term" and not r.get("canary") for r in recs)

    def phase_measures(self, ph, recs, rows, trials):
        m = term_metrics([r for r in recs if r.get("kind") == "term"])
        if not self.under_background(ph, recs, rows, trials):
            return {k: float("nan") for k in m}
        return m


@w.register
class Mixed(_Scenario):
    """M1 (W5-single under four bulk downloads) and M2 (W5-single under a
    bulk upload), each with W6 phase-A keystrokes as a canary."""
    name = "mixed"
    report_only = LLM_REPORT_ONLY

    def read(self, cfg, p):
        LLM.read(self, cfg, p)
        cfg.mixed = {"scenarios": _words(p.get("scenarios", "m1 m2"))}
        for s in cfg.mixed["scenarios"]:
            if s not in ("m1", "m2"):
                raise ab.ConfigError(f"[workload:mixed] scenario {s!r} is not m1 or m2 (Phase A)")

    def phases(self, cfg, path):
        client = cfg.clients[path]
        out = []
        for sc in cfg.mixed["scenarios"]:
            bg = "down4" if sc == "m1" else "up1"
            for kb in cfg.llm["uploads_kb"]:
                call = dict(cfg.llm, sequence="single", upload=kb * 1000)
                canary = {"seconds": cfg.scenario_phase_seconds, "delay": 0, "burst_at": 0, "burst_every": 0,
                          "burst_bytes": 0, "canary": True}
                out.append(w.Phase(f"{sc}-{kb}k", cfg.scenario_phase_seconds,
                                   self.background(cfg, client, bg)
                                   + [w.Job(client, "wl-term", 1, load=False, params=canary),
                                      w.Job(client, "wl-llm", 1, params=call)]))
        return out

    def has_evidence(self, ph, recs):
        # A canary counts only if it measured something: a session that
        # failed to connect, or returned no echo, is not evidence.
        return any(r.get("kind") == "llm" for r in recs) and \
            any(r.get("kind") == "term" and r.get("canary") and not r.get("fail") and r.get("keys") for r in recs)

    def phase_measures(self, ph, recs, rows, trials):
        m = llm_metrics([r for r in recs if r.get("kind") == "llm"])
        canary = term_metrics([r for r in recs if r.get("kind") == "term" and r.get("canary")])
        m["canary_echo_p99_s"] = canary.get("echo_p99_a_s", float("nan"))
        # The background's own rate over each trial's foreground window,
        # pooled: bytes in the windows over the windows' length.
        got = secs = 0.0
        unknown = False
        for t in trials:
            trows = [r for r in t.rows if r["phase"] == ph and r["kind"] in ("wl-bgdown", "wl-bgup")]
            unknown |= any(r.get("curl_exit") == SINK_UNKNOWN for r in trows)
            span = fg_span([x for x in getattr(t, "records", []) if x.get("_phase") == ph and x.get("kind") == "llm"])
            if span:
                g, w_ = rate_in_window(trows, span)
                got, secs = got + g, secs + w_
        m["bg_rate_bps"] = got / secs if secs > 0 and not unknown else float("nan")
        if not self.under_background(ph, recs, rows, trials):
            return {k: float("nan") for k in m}
        m["class_interactive"] = class_interactive(trials, prompt_bytes(recs), ph)
        return m

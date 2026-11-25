#!/usr/bin/env python3
"""Summarize carrier-shadow records from client and gateway logs.

Endpoints started with `--carrier-shadow` record, beside today's recovery, the
carrier state the path-authority design (docs/DESIGN-PATH-AUTHORITY.md §10)
would assign to every QUIC connection and the remedy it would take at every
flow stall and lane failure. This reads their JSON-lines logs and prints what
a measurement campaign needs:

1. Threshold calibration (§10.2, §15, open question Q4). For every n, how many
   no-progress episodes reached n probe timeouts and how many of those were
   acknowledged again afterwards. recovered / (recovered + dead) is the
   conditional false-promotion rate of k_p = n; the same ratio bounds the
   wasted standby dials of k_s = n.
2. Stall comparison. Each watchdog stall with the carrier's state at that
   moment, whether QUIC was still acknowledging, the design's remedy against
   today's, and what the carrier did next: a stall on a carrier that later
   reached REPLACE or DEAD is a carrier failure the design detects later (the
   delay is reported); one whose carrier never left ACTIVE is a rescue the
   design would not have made.
3. Recovery deadline (round-4 P0-1). Per flow, both endpoints' first failure
   observation, the gateway's replacement-grace deadline, the client's rescue
   start and the JOIN's arrival at the gateway. From those, the detection skew
   and JOIN transit distributions and the latest handoff start a client can
   afford, measured from its own failure observation.

    ./scripts/carrier_shadow_summary.py --client client.jsonl [...] \\
        --gateway gateway.jsonl [...] [--clock-offset-ms MS] [--json]

Both clocks must be synchronized (NTP) for part 3; --clock-offset-ms is the
gateway clock minus the client clock and is subtracted from gateway times.
Files of one endpoint are read in the order given; carrier identifiers are
per process and are keyed by the logger's pid when present.
"""

from __future__ import annotations

import argparse
import datetime
import json
import math
import re
import sys
from collections import Counter, defaultdict
from typing import Any, Iterable, Iterator

BUCKETS = 16
DEFAULT_GRACE_S = 45.0

STATE = "carrier shadow state"
EPISODE = "carrier shadow episode"
ENDED = "carrier shadow carrier ended"
BOUND = "carrier shadow lane bound"
STALL = "carrier shadow stall"
LANE_FAILED = "carrier shadow lane failed"
OUTAGE = "carrier shadow outage"
OUTAGE_ENDED = "carrier shadow outage ended"
RESCUE = "carrier shadow rescue started"
JOIN = "carrier shadow join arrived"
ATTEMPT = "carrier shadow join attempt"

RFC3339 = re.compile(
    r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})\Z", re.IGNORECASE
)


def parse_instant(value: Any) -> float | None:
    """Parse an RFC 3339 log timestamp to Unix seconds, or None."""
    if not isinstance(value, str):
        return None
    match = RFC3339.match(value.strip())
    if match is None:
        return None
    base, fraction, zone = match.groups()
    zone = "+00:00" if zone.upper() == "Z" else zone
    try:
        whole = datetime.datetime.fromisoformat(f"{base}{zone}").timestamp()
    except (ValueError, OverflowError):
        return None
    return whole + (float(f"0.{fraction}") if fraction else 0.0)


def read_records(stream: Iterable[str], skipped: Counter) -> Iterator[dict[str, Any]]:
    for line in stream:
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except ValueError:
            skipped["not json"] += 1
            continue
        if isinstance(record, dict):
            yield record
        else:
            skipped["not an object"] += 1


def seconds(value: Any) -> float | None:
    """slog writes a time.Duration as integer nanoseconds."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return value / 1e9


def quantiles(values: list[float]) -> dict[str, float] | None:
    if not values:
        return None
    ordered = sorted(values)

    def q(p: float) -> float:
        # Nearest-rank, so a reported quantile is always an observed value.
        rank = max(1, math.ceil(p * len(ordered)))
        return ordered[rank - 1]

    return {"n": len(ordered), "min": ordered[0], "p10": q(0.10), "p50": q(0.50), "p90": q(0.90), "p99": q(0.99), "max": ordered[-1]}


def wilson_upper(successes: int, total: int, z: float = 1.96) -> float | None:
    """Upper bound of the Wilson score interval for a proportion."""
    if total == 0:
        return None
    p = successes / total
    centre = p + z * z / (2 * total)
    spread = z * math.sqrt(p * (1 - p) / total + z * z / (4 * total * total))
    return min(1.0, (centre + spread) / (1 + z * z / total))


class Endpoint:
    """Every carrier-shadow record one endpoint wrote, in file order."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.records: list[dict[str, Any]] = []
        self.skipped: Counter = Counter()
        # The latest timestamp of any record, shadow or not: where the
        # capture really ends, for deciding what is censored.
        self.capture_end = 0.0

    def load(self, paths: list[str], offset_s: float = 0.0) -> None:
        for path in paths:
            with open(path, encoding="utf-8", errors="replace") as stream:
                for record in read_records(stream, self.skipped):
                    stamp = parse_instant(record.get("time"))
                    if stamp is not None:
                        self.capture_end = max(self.capture_end, stamp - offset_s)
                    msg = record.get("msg")
                    if not isinstance(msg, str) or not msg.startswith("carrier shadow"):
                        continue
                    at = parse_instant(record.get("time"))
                    if at is None:
                        self.skipped["no time"] += 1
                        continue
                    # Prefer the time the observation was made over the
                    # time it was logged: a hook can log after the sampler.
                    observed = record.get("observed_unix_ms")
                    if isinstance(observed, (int, float)) and not isinstance(observed, bool):
                        at = observed / 1000.0
                    record["_t"] = at - offset_s
                    record["_process"] = record.get("pid", path)
                    self.records.append(record)

    def of(self, msg: str) -> list[dict[str, Any]]:
        return [r for r in self.records if r.get("msg") == msg]


def carrier_key(record: dict[str, Any]) -> tuple[Any, Any]:
    return (record["_process"], record.get("carrier_id"))


def flow_key(record: dict[str, Any]) -> tuple[Any, Any]:
    return (record.get("session_id"), record.get("flow_id"))


def calibration(endpoint: Endpoint) -> dict[str, Any]:
    """Episode histogram: closing summaries, plus episode lines for carriers
    still open when the log ends (those count only n >= 2, which is all an
    episode line is written for)."""
    reached = [0] * BUCKETS
    recovered = [0] * BUCKETS
    dead = [0] * BUCKETS
    summarized = set()
    for record in endpoint.of(ENDED):
        summarized.add(carrier_key(record))
        for name, total in (("pto_reached", reached), ("pto_recovered", recovered), ("pto_dead", dead)):
            values = record.get(name)
            if isinstance(values, list):
                for i, value in enumerate(values[:BUCKETS]):
                    if isinstance(value, int):
                        total[i] += value
    open_episodes = 0
    for record in endpoint.of(EPISODE):
        if carrier_key(record) in summarized:
            continue
        top = record.get("max_pto")
        if not isinstance(top, int) or top < 1:
            continue
        open_episodes += 1
        # Episode lines exist only for two or more expiries, so they say
        # nothing about n = 1: that bucket comes from complete summaries.
        for i in range(1, min(top, BUCKETS)):
            reached[i] += 1
            if record.get("outcome") == "recovered":
                recovered[i] += 1
            elif record.get("outcome") == "dead":
                dead[i] += 1
    rows = []
    for i in range(BUCKETS):
        decided = recovered[i] + dead[i]
        rows.append({
            "n": f"{i + 1}+" if i == BUCKETS - 1 else str(i + 1),
            "reached": reached[i], "recovered": recovered[i], "dead": dead[i],
            "censored": reached[i] - decided,
            "false_rate": recovered[i] / decided if decided else None,
            "false_rate_upper95": wilson_upper(recovered[i], decided),
        })
    transitions = Counter(r.get("to") for r in endpoint.of(STATE))
    false_replacements = sum(1 for r in endpoint.of(STATE) if r.get("false_replacement") is True)
    thresholds = Counter((r.get("k_s"), r.get("k_p")) for r in endpoint.of(STATE))
    return {
        "carriers_ended": len(summarized), "open_carrier_episodes": open_episodes, "rows": rows,
        "transitions": dict(transitions), "false_replacements": false_replacements,
        "thresholds": [{"k_s": k[0], "k_p": k[1], "records": n} for k, n in thresholds.items()],
    }


def stall_comparison(endpoint: Endpoint) -> dict[str, Any]:
    transitions: dict[tuple, list[dict[str, Any]]] = defaultdict(list)
    for record in endpoint.of(STATE):
        transitions[carrier_key(record)].append(record)
    # Hooks and the sampler can log out of order; walk by observation time
    # (stable, so file order breaks ties).
    for values in transitions.values():
        values.sort(key=lambda r: r["_t"])
    by_kind: Counter = Counter()
    later_failure_delay: list[float] = []
    outcomes: Counter = Counter()
    for stall in endpoint.of(STALL):
        by_kind[(stall.get("carrier_state"), stall.get("design_remedy"), stall.get("legacy_remedy"), stall.get("transport_acked"))] += 1
        if stall.get("carrier_state") == "untracked":
            outcomes["untracked carrier"] += 1
            continue
        if stall.get("carrier_state") in ("replace", "dead"):
            outcomes["carrier already failed at the stall"] += 1
            continue
        epoch = stall.get("transport_epoch")
        if not isinstance(epoch, int):
            outcomes["unknown (no transport epoch)"] += 1
            continue
        # Only transitions of the same no-progress episode -- the carrier's
        # progress epoch unchanged since the stall -- are its consequence.
        # Progress that changes no state is not logged, so a later failure
        # in a newer epoch is a different episode.
        following = [t for t in transitions.get(carrier_key(stall), []) if t["_t"] >= stall["_t"]]
        same = []
        for t in following:
            if t.get("transport_epoch") != epoch:
                break
            same.append(t)
        failure = next((t for t in same if t.get("to") in ("replace", "dead")), None)
        newer = len(same) < len(following)
        if failure is not None:
            outcomes[f"carrier reached {failure.get('to')} after the stall"] += 1
            later_failure_delay.append(failure["_t"] - stall["_t"])
        elif stall.get("carrier_state") == "closed" or any(t.get("to") == "closed" for t in same):
            outcomes["carrier closed by an endpoint (censored)"] += 1
        elif newer:
            # Progress in a newer epoch: the carrier was live.
            outcomes["carrier stayed live: design would not rescue"] += 1
        else:
            outcomes["unresolved: no later evidence about the carrier"] += 1
    lane_failures = Counter((r.get("carrier_state"), r.get("design_remedy")) for r in endpoint.of(LANE_FAILED))
    return {
        "stalls": sum(by_kind.values()),
        "by_kind": [
            {"carrier_state": k[0], "design_remedy": k[1], "legacy_remedy": k[2], "transport_acked": k[3], "count": n}
            for k, n in sorted(by_kind.items(), key=lambda item: -item[1])
        ],
        "outcomes": dict(outcomes),
        "design_detection_delay_s": quantiles(later_failure_delay),
        "lane_failures": [{"carrier_state": k[0], "design_remedy": k[1], "count": n} for k, n in lane_failures.items()],
    }


def failure_observations(endpoint: Endpoint) -> dict[tuple, list[tuple[float, str]]]:
    """Each flow's failure observations on this endpoint, in time order."""
    out: dict[tuple, list[tuple[float, str]]] = defaultdict(list)
    for msg, kind in ((STALL, "stall"), (LANE_FAILED, "lane_failed"), (OUTAGE, "outage")):
        for record in endpoint.of(msg):
            out[flow_key(record)].append((record["_t"], kind))
    for values in out.values():
        values.sort()
    return out


def recovery_deadline(client: Endpoint, gateway: Endpoint, grace_s: float, offset_s: float = 0.0) -> dict[str, Any]:
    """Pair each gateway outage with the client's observation of the same
    failure and with the JOIN that ended it.

    An outage's window runs from the end of the flow's previous gateway outage
    (or a grace before its start) to its own end: the outage-ended record, or
    the next outage's start. The client's observation is its earliest failure
    observation inside that window, the JOIN the first arrival inside it, and
    the rescue the client's latest rescue start between its observation and
    that arrival. A pair that cannot be formed is left out, not guessed.
    """
    client_obs = failure_observations(client)
    gateway_obs = failure_observations(gateway)
    # A JOIN's transit runs from the client starting that attempt to the
    # gateway parsing it, matched by lane ID; the transport is kept, since a
    # TCP handoff and a QUIC JOIN have different budgets.
    attempts: dict[tuple, tuple[float, str]] = {}
    for record in client.of(ATTEMPT):
        key = (*flow_key(record), record.get("lane_id"))
        attempts.setdefault(key, (record["_t"], str(record.get("transport"))))
    joins: dict[tuple, list[dict[str, Any]]] = defaultdict(list)
    for record in gateway.of(JOIN):
        joins[flow_key(record)].append(record)
    ended: dict[tuple, list[float]] = defaultdict(list)
    for record in gateway.of(OUTAGE_ENDED):
        ended[flow_key(record)].append(record["_t"])
    outages: dict[tuple, list[dict[str, Any]]] = defaultdict(list)
    for record in gateway.of(OUTAGE):
        outages[flow_key(record)].append(record)
    capture_end = gateway.capture_end
    graces: list[float] = []

    skew: list[float] = []
    observation_skew: list[float] = []
    transit: list[float] = []
    transit_by: dict[str, list[float]] = defaultdict(list)
    arrival_margin: list[float] = []
    flows = []
    late = expired = unfinished = 0
    for key, records in outages.items():
        records.sort(key=lambda r: r["_t"])
        previous_end = None
        for index, outage in enumerate(records):
            start = outage["_t"]
            grace = seconds(outage.get("grace")) or grace_s
            graces.append(grace)
            deadline = start + grace
            recorded = outage.get("deadline_unix_ms")
            if isinstance(recorded, (int, float)) and not isinstance(recorded, bool):
                # The gateway's own deadline, on the same clock and offset as
                # every other gateway timestamp.
                deadline = recorded / 1000.0 - offset_s
                start = deadline - grace
            next_start = records[index + 1]["_t"] if index + 1 < len(records) else math.inf
            end = next((t for t in sorted(ended.get(key, [])) if start <= t < next_start), None)
            # Without an end record the window runs past the deadline, so a
            # JOIN that arrived late is found and counted late.
            window_end = end if end is not None else min(next_start, capture_end)
            window_start = max(previous_end if previous_end is not None else -math.inf, start - grace)
            previous_end = end if end is not None else min(next_start, deadline)
            row: dict[str, Any] = {"session_id": key[0], "flow_id": key[1], "gateway_outage_start": start, "gateway_deadline": deadline}
            # The client observation nearest the gateway's grace start is the
            # most plausible view of the same failure; an earlier, resolved
            # stall in the window is not.
            candidates = [(t, kind) for t, kind in client_obs.get(key, []) if window_start <= t <= window_end]
            observed = min(candidates, key=lambda c: abs(c[0] - start)) if candidates else None
            if observed is not None:
                # Positive: the gateway started its grace after the client
                # first saw the failure.
                row["skew_s"] = start - observed[0]
                row["client_observation"] = observed[1]
                skew.append(row["skew_s"])
                first_gateway = next((t for t, _ in gateway_obs.get(key, []) if window_start <= t <= window_end), None)
                if first_gateway is not None:
                    observation_skew.append(first_gateway - observed[0])
            join = next((j for j in sorted(joins.get(key, []), key=lambda j: j["_t"]) if start <= j["_t"] <= window_end), None)
            if join is not None:
                arrival = join["_t"]
                row["join_arrival_margin_s"] = deadline - arrival
                row["join_refused_session_ended"] = join.get("session_ended") is True
                arrival_margin.append(row["join_arrival_margin_s"])
                if arrival > deadline:
                    late += 1
            elif end is None and next_start == math.inf and capture_end < deadline:
                # The log stops inside the grace: right-censored, not failed.
                unfinished += 1
                row["unfinished"] = True
            elif end is None:
                expired += 1
                row["expired_without_join"] = True
            # Every JOIN of this outage that matches an attempt is a transit
            # sample, not only the first: J budgets for the slow handoffs.
            # A slower sibling of the winning JOIN can arrive after the
            # outage ended; it still measures J, up to the next outage.
            for j in joins.get(key, []):
                if not start <= j["_t"] < min(next_start, capture_end + 1):
                    continue
                attempt = attempts.get((*key, j.get("lane_id")))
                if attempt is not None and attempt[0] <= j["_t"]:
                    transit.append(j["_t"] - attempt[0])
                    transit_by[attempt[1]].append(j["_t"] - attempt[0])
                    if join is not None and j is join:
                        row["join_transit_s"] = j["_t"] - attempt[0]
                        row["join_transport"] = attempt[1]
            flows.append(row)

    result: dict[str, Any] = {
        "grace_s": grace_s, "gateway_outages": len(flows), "joins_after_deadline": late,
        "outages_without_join": expired, "outages_unfinished_at_capture_end": unfinished,
        "skew_s": quantiles(skew), "first_observation_skew_s": quantiles(observation_skew),
        "join_transit_s": quantiles(transit), "join_arrival_margin_s": quantiles(arrival_margin),
        "join_transit_by_transport_s": {k: quantiles(v) for k, v in sorted(transit_by.items())},
        "flows": flows,
    }
    # §10.3 step 4: the JOIN must reach the gateway by t_c + G - S, where S
    # is how much later the client observes than the gateway, so a handoff
    # must start by t_c + G - S - J. A gateway that starts its grace later
    # than the client is not counted as extra time: S is never negative.
    # Skew is not bounded by anything the client sees, so with few samples
    # the answer is best effort, which is what the design says.
    if skew and transit:
        few = len(skew) < 100 or len(transit) < 100
        low_skew = quantiles(skew)["min" if few else "p10"]
        high_transit = quantiles(transit)["max" if few else "p99"]
        result["S_s"] = max(0.0, -low_skew)
        result["J_s"] = high_transit
        # The grace the gateways actually ran; the smallest if they differ.
        grace = min(graces) if graces else grace_s
        result["grace_s"] = grace
        result["graces_seen_s"] = sorted(set(graces))
        result["latest_handoff_start_s"] = grace - result["S_s"] - result["J_s"]
        result["latest_handoff_basis"] = "min skew, max transit" if few else "p10 skew, p99 transit"
    return result


def format_q(q: dict[str, float] | None, unit: str = "s") -> str:
    if q is None:
        return "no samples"
    return f"n={q['n']} min={q['min']:.3f}{unit} p50={q['p50']:.3f}{unit} p90={q['p90']:.3f}{unit} max={q['max']:.3f}{unit}"


def render(report: dict[str, Any], out) -> None:
    for name in ("client", "gateway"):
        if name not in report["calibration"]:
            continue
        cal = report["calibration"][name]
        print(f"== {name}: probe-timeout episodes (k_s/k_p calibration)", file=out)
        print(f"   carriers ended {cal['carriers_ended']}, episodes from still-open carriers {cal['open_carrier_episodes']}", file=out)
        print(f"   transitions {cal['transitions']}, false replacements {cal['false_replacements']}", file=out)
        print("   n   reached  recovered  dead  censored  P(recover|n)  upper95", file=out)
        for row in cal["rows"]:
            rate = "-" if row["false_rate"] is None else f"{row['false_rate']:.3f}"
            upper = "-" if row["false_rate_upper95"] is None else f"{row['false_rate_upper95']:.3f}"
            print(f"   {row['n']:<3} {row['reached']:>8} {row['recovered']:>10} {row['dead']:>5} {row['censored']:>9} {rate:>13} {upper:>8}", file=out)
        stalls = report["stalls"][name]
        print(f"== {name}: watchdog stalls against the design ({stalls['stalls']})", file=out)
        for row in stalls["by_kind"]:
            print(f"   {row['count']:>6}  carrier={row['carrier_state']} acked={row['transport_acked']} design={row['design_remedy']} today={row['legacy_remedy']}", file=out)
        for outcome, n in sorted(stalls["outcomes"].items()):
            print(f"   {n:>6}  {outcome}", file=out)
        print(f"   design detection delay after the stall: {format_q(stalls['design_detection_delay_s'])}", file=out)
        for row in stalls["lane_failures"]:
            print(f"   lane failure: {row['count']:>6}  carrier={row['carrier_state']} design={row['design_remedy']}", file=out)
    deadline = report.get("recovery_deadline")
    if deadline is not None:
        print("== recovery deadline (client and gateway failure observations)", file=out)
        print(f"   gateway outages {deadline['gateway_outages']}, JOINs after the deadline {deadline['joins_after_deadline']}, outages with no JOIN {deadline['outages_without_join']}, unfinished at capture end {deadline['outages_unfinished_at_capture_end']}", file=out)
        print(f"   skew (gateway grace start - client first observation): {format_q(deadline['skew_s'])}", file=out)
        print(f"   first-observation skew (gateway - client): {format_q(deadline['first_observation_skew_s'])}", file=out)
        print(f"   JOIN transit (client attempt start -> gateway arrival, same lane): {format_q(deadline['join_transit_s'])}", file=out)
        for transport, q in deadline["join_transit_by_transport_s"].items():
            print(f"   JOIN transit over {transport}: {format_q(q)}", file=out)
        print(f"   JOIN arrival margin before the gateway deadline: {format_q(deadline['join_arrival_margin_s'])}", file=out)
        if "latest_handoff_start_s" in deadline:
            print(f"   measured S (client observes later than the gateway) {deadline['S_s']:.3f}s, J (handoff start to JOIN parsed) {deadline['J_s']:.3f}s", file=out)
            print(f"   latest handoff start after the client's own observation, G - S - J: {deadline['latest_handoff_start_s']:.3f}s ({deadline['latest_handoff_basis']}, G {deadline['grace_s']}s)", file=out)
        else:
            print("   latest handoff start: not computable (needs paired client and gateway outage records)", file=out)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--client", nargs="*", default=[], help="client JSON-lines logs, oldest first")
    parser.add_argument("--gateway", nargs="*", default=[], help="gateway JSON-lines logs, oldest first")
    parser.add_argument("--clock-offset-ms", type=float, default=0.0, help="gateway clock minus client clock")
    parser.add_argument("--grace-s", type=float, default=DEFAULT_GRACE_S, help="gateway replacement grace when a record omits it")
    parser.add_argument("--json", action="store_true", help="print the report as JSON")
    args = parser.parse_args(argv)
    if not args.client and not args.gateway:
        parser.error("give --client and/or --gateway logs")
    client, gateway = Endpoint("client"), Endpoint("gateway")
    client.load(args.client)
    gateway.load(args.gateway, args.clock_offset_ms / 1000.0)
    report: dict[str, Any] = {"calibration": {}, "stalls": {}}
    for endpoint, paths in ((client, args.client), (gateway, args.gateway)):
        if paths:
            report["calibration"][endpoint.name] = calibration(endpoint)
            report["stalls"][endpoint.name] = stall_comparison(endpoint)
    if args.client and args.gateway:
        report["recovery_deadline"] = recovery_deadline(client, gateway, args.grace_s, args.clock_offset_ms / 1000.0)
    report["skipped"] = {"client": dict(client.skipped), "gateway": dict(gateway.skipped)}
    if args.json:
        json.dump(report, sys.stdout, indent=2, sort_keys=True)
        print()
    else:
        render(report, sys.stdout)
    return 0


if __name__ == "__main__":
    sys.exit(main())

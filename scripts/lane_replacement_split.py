#!/usr/bin/env python3
"""Split client lane-replacement-timeout failures by what recovery did.

Issue #53 asks why most `lane replacement timeout` failures never have a
replacement lane admitted: a client pool that never rebuilt, or a path that
would not carry a handshake. Only the client's flow records can answer it,
because only the client opens replacements. This reads client JSON-lines logs
(and optionally gateway logs for context) and prints the split.

    ./scripts/lane_replacement_split.py client.jsonl [client.jsonl.1 ...] \
        [--gateway server.jsonl ...] [--json]

Gateway records carry `lane_replacement_attempts`/`_failures` too, always zero:
the gateway never opens a replacement. They are never used for the split.
"""

from __future__ import annotations

import argparse
import datetime
import json
import re
import sys
from collections import Counter, defaultdict
from typing import Any, Iterable, Iterator, TextIO


CLIENT_FAILURE = "local flow ended with error"
CLIENT_COMPLETE = "local flow complete"
GATEWAY_FAILURE = "remote flow ended with error"
JOIN_REFUSED = "lane join refused"

TIMEOUT = "lane replacement timeout"
ABANDONED = "no replacement lane will be attempted"
RESUME_REFUSED = "peer does not hold this session"

# Buckets for `lane replacement timeout` with lanes_joined == 1, in report order.
NO_ATTEMPT = "no zero-lane replacement attempted (attempts == 0)"
EVERY_ATTEMPT_FAILED = "every attempt failed (attempts == failures)"
OUTCOME_UNRECORDED = "attempt outcome unrecorded (attempts > failures)"
OLD_CLIENT = "old client (fields missing)"
INCONSISTENT = "inconsistent (failures > attempts)"
BUCKETS = (NO_ATTEMPT, EVERY_ATTEMPT_FAILED, OUTCOME_UNRECORDED, OLD_CLIENT, INCONSISTENT)

# The categories the client writes as lane_replacement_last_failure.
FAILURE_KINDS = ("timeout", "peer_rejected", "peer_capacity", "canceled", "other")

# Go's RFC 3339 output: variable fractional digits (up to nanoseconds, which
# datetime cannot hold) and either Z or a numeric offset.
RFC3339 = re.compile(
    r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})\Z", re.IGNORECASE
)


def parse_instant(value: Any) -> datetime.datetime | None:
    """Parse a log timestamp to an aware UTC instant, or None."""
    if not isinstance(value, str):
        return None
    match = RFC3339.match(value.strip())
    if match is None:
        return None
    base, fraction, zone = match.groups()
    micros = (fraction or "")[:6].ljust(6, "0")
    zone = "+00:00" if zone.upper() == "Z" else zone
    try:
        instant = datetime.datetime.fromisoformat(f"{base}.{micros}{zone}")
    except ValueError:
        return None
    return instant.astimezone(datetime.timezone.utc)


LAST_LANE = re.compile(r"last lane failed \((.*)\): ")


def read_records(stream: Iterable[str], skipped: Counter) -> Iterator[dict[str, Any]]:
    """Yield JSON objects, counting lines that are not one."""
    for line in stream:
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except ValueError:
            skipped["not json"] += 1
            continue
        if not isinstance(record, dict):
            skipped["not an object"] += 1
            continue
        yield record


def counter(record: dict[str, Any], name: str) -> int | None:
    """Return a non-negative integer field, or None if absent or malformed."""
    value = record.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def last_lane_death(error: str) -> str:
    """Classify how the flow's last lane died, from the wrapped flow error."""
    match = LAST_LANE.search(error)
    if match is None:
        return "unrecorded"
    cause = match.group(1)
    if "no recent network activity" in cause:
        return "no recent network activity"
    if cause == "EOF" or cause.endswith(": EOF") or "unexpected EOF" in cause:
        return "EOF"
    return "other"


def timeout_bucket(record: dict[str, Any]) -> tuple[str, str | None]:
    """Bucket one lanes_joined == 1 timeout; second value is the sub-split."""
    attempts = counter(record, "lane_replacement_attempts")
    failures = counter(record, "lane_replacement_failures")
    if attempts is None or failures is None:
        return OLD_CLIENT, None
    # The invariant comes first: attempts=0 with failures=1 is a counter bug,
    # not a flow that tried nothing.
    if failures > attempts:
        return INCONSISTENT, None
    if attempts == 0:
        return NO_ATTEMPT, None
    if attempts == failures:
        kind = record.get("lane_replacement_last_failure")
        if kind not in FAILURE_KINDS:
            kind = "unrecorded" if kind is None else "other"
        return EVERY_ATTEMPT_FAILED, kind
    return OUTCOME_UNRECORDED, None


def iso(instant: datetime.datetime) -> str:
    return instant.isoformat(timespec="microseconds").replace("+00:00", "Z")


def flow_key(record: dict[str, Any]) -> tuple[str, str] | None:
    session, flow = record.get("session_id"), record.get("flow_id")
    if session is None or flow is None:
        return None
    return str(session), str(flow)


class Split:
    def __init__(self) -> None:
        self.skipped: Counter = Counter()
        self.client_complete = 0
        self.client_failed = 0
        self.ignored_server_records = 0
        self.first: datetime.datetime | None = None
        self.last: datetime.datetime | None = None
        self.buckets: Counter = Counter()
        self.failure_kinds: Counter = Counter()
        self.deaths: dict[str, Counter] = defaultdict(Counter)
        self.timeouts_joined: Counter = Counter()  # lanes_joined value for every timeout
        self.other_terminal: Counter = Counter()
        self.members: dict[str, list[tuple[str, str]]] = defaultdict(list)
        self.gateway_failures: dict[tuple[str, str], str] = {}
        self.gateway_records = 0
        self.refusals: dict[str, Counter] = defaultdict(Counter)

    def add_client(self, record: dict[str, Any]) -> None:
        msg = record.get("msg")
        if msg not in (CLIENT_FAILURE, CLIENT_COMPLETE):
            return
        if record.get("role") == "server":
            # A gateway log passed as a client log would make every flow look
            # like "no zero-lane replacement attempted". Refuse to count it.
            self.ignored_server_records += 1
            return
        instant = parse_instant(record.get("time"))
        if instant is not None:
            if self.first is None or instant < self.first:
                self.first = instant
            if self.last is None or instant > self.last:
                self.last = instant
        if msg == CLIENT_COMPLETE:
            self.client_complete += 1
            return
        self.client_failed += 1
        error = record.get("error")
        error = error if isinstance(error, str) else ""
        death = last_lane_death(error)
        key = flow_key(record)

        def note(bucket: str) -> None:
            self.deaths[bucket][death] += 1
            if key is not None:
                self.members[bucket].append(key)

        if TIMEOUT in error:
            joined = counter(record, "lanes_joined")
            self.timeouts_joined["missing" if joined is None else ("1" if joined == 1 else ">1" if joined > 1 else "0")] += 1
            if joined is not None and joined != 1:
                return
            # A client too old to write lanes_joined cannot be split at all; it
            # is counted with the clients missing the attempt counters rather
            # than silently dropped.
            bucket, kind = (OLD_CLIENT, None) if joined is None else timeout_bucket(record)
            self.buckets[bucket] += 1
            if kind is not None:
                self.failure_kinds[kind] += 1
            note(bucket)
            return
        for terminal in (ABANDONED, RESUME_REFUSED):
            if terminal in error:
                self.other_terminal[terminal] += 1
                note(terminal)
                return

    def add_gateway(self, record: dict[str, Any]) -> None:
        msg = record.get("msg")
        if msg == GATEWAY_FAILURE:
            self.gateway_records += 1
            key = flow_key(record)
            if key is not None:
                error = record.get("error")
                self.gateway_failures[key] = last_lane_death(error if isinstance(error, str) else "")
        elif msg == JOIN_REFUSED:
            reason = str(record.get("reason", "unknown"))
            suppressed = counter(record, "suppressed") or 0
            self.refusals[reason]["records"] += 1
            # A record stands for itself plus the refusals suppressed since the
            # previous one of its reason. A storm's tail after the last record
            # is not counted; `total` is per process and resets on restart.
            self.refusals[reason]["refusals"] += 1 + suppressed

    def gateway_join(self) -> dict[str, Counter]:
        joined: dict[str, Counter] = {}
        for bucket, keys in self.members.items():
            tally: Counter = Counter()
            for key in keys:
                death = self.gateway_failures.get(key)
                tally["no gateway failure record" if death is None else f"gateway failed, last lane: {death}"] += 1
            joined[bucket] = tally
        return joined

    def report(self, with_gateway: bool) -> dict[str, Any]:
        result: dict[str, Any] = {
            "window": ({"first": iso(self.first), "last": iso(self.last)}
                       if self.first is not None and self.last is not None else None),
            "client_flows": {"complete": self.client_complete, "failed": self.client_failed},
            "lane_replacement_timeout_by_lanes_joined": dict(self.timeouts_joined),
            "buckets": {b: self.buckets[b] for b in BUCKETS},
            "every_attempt_failed_by_last_failure": dict(self.failure_kinds),
            "other_terminal_errors": {t: self.other_terminal[t] for t in (ABANDONED, RESUME_REFUSED)},
            "last_lane_death": {b: dict(c) for b, c in self.deaths.items()},
        }
        if self.ignored_server_records:
            result["ignored_server_records"] = self.ignored_server_records
        if self.skipped:
            result["skipped_lines"] = dict(self.skipped)
        if with_gateway:
            result["gateway"] = {
                "failure_records": self.gateway_records,
                "joined_by_session_and_flow": {b: dict(c) for b, c in self.gateway_join().items()},
                "lane_join_refused": {r: dict(c) for r, c in sorted(self.refusals.items())},
            }
        return result


def open_input(path: str) -> TextIO:
    if path == "-":
        return sys.stdin
    return open(path, encoding="utf-8", errors="replace")


def analyse(client_paths: list[str], gateway_paths: list[str]) -> Split:
    split = Split()
    for path in client_paths:
        with open_input(path) as stream:
            for record in read_records(stream, split.skipped):
                split.add_client(record)
    for path in gateway_paths:
        with open_input(path) as stream:
            for record in read_records(stream, split.skipped):
                split.add_gateway(record)
    return split


def percent(part: int, whole: int) -> str:
    return f"{100 * part / whole:5.1f}%" if whole else "    -"


def format_text(report: dict[str, Any]) -> str:
    out: list[str] = []
    window = report["window"]
    if window:
        out.append(f"window          {window['first']} -> {window['last']}")
    flows = report["client_flows"]
    out.append(f"client flows    {flows['complete']} complete, {flows['failed']} failed")
    joined = report["lane_replacement_timeout_by_lanes_joined"]
    out.append("lane replacement timeout by lanes_joined: "
               + (", ".join(f"{k}={v}" for k, v in sorted(joined.items())) or "none"))
    if report.get("ignored_server_records"):
        out.append(f"WARNING: ignored {report['ignored_server_records']} gateway (role=server) flow records "
                   "passed as client input")
    if report.get("skipped_lines"):
        out.append("skipped lines   " + ", ".join(f"{k}={v}" for k, v in report["skipped_lines"].items()))

    buckets = report["buckets"]
    total = sum(buckets.values())
    out.append("")
    out.append(f"lane replacement timeout, lanes_joined == 1 (or unrecorded): {total}")
    deaths = report["last_lane_death"]
    for bucket in BUCKETS:
        n = buckets[bucket]
        out.append(f"  {bucket:<46} {n:6d} {percent(n, total)}")
        if bucket == EVERY_ATTEMPT_FAILED:
            kinds = report["every_attempt_failed_by_last_failure"]
            for kind in (*FAILURE_KINDS, "unrecorded"):
                if kinds.get(kind):
                    out.append(f"      last_failure={kind:<30} {kinds[kind]:6d} {percent(kinds[kind], n)}")
            if n:
                out.append("      only last_failure=timeout suggests the path did not carry the handshake;")
                out.append("      peer_rejected/peer_capacity prove the peer answered it")
        if bucket in deaths:
            out.append("      last lane: " + ", ".join(f"{k}={v}" for k, v in sorted(deaths[bucket].items())))
    out.append("")
    out.append("other terminal errors (any lanes_joined):")
    for terminal, n in report["other_terminal_errors"].items():
        out.append(f"  {terminal:<46} {n:6d}")
        if terminal in deaths:
            out.append("      last lane: " + ", ".join(f"{k}={v}" for k, v in sorted(deaths[terminal].items())))

    gateway = report.get("gateway")
    if gateway is not None:
        out.append("")
        out.append(f"gateway `remote flow ended with error` records: {gateway['failure_records']}")
        for bucket, tally in gateway["joined_by_session_and_flow"].items():
            out.append(f"  {bucket}: " + ", ".join(f"{k}={v}" for k, v in sorted(tally.items())))
        out.append("gateway lane_join_refused (records / refusals incl. suppressed):")
        if not gateway["lane_join_refused"]:
            out.append("  none")
        for reason, tally in gateway["lane_join_refused"].items():
            out.append(f"  {reason:<30} {tally['records']:6d} / {tally['refusals']}")
    return "\n".join(out) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("client", nargs="+", help="client JSON-lines log files ('-' for stdin)")
    parser.add_argument("--gateway", action="append", default=[], metavar="FILE",
                        help="gateway JSON-lines log file, for the flow join and refusal tally (repeatable)")
    parser.add_argument("--json", action="store_true", help="print the report as JSON")
    args = parser.parse_args(argv)
    if args.client.count("-") + args.gateway.count("-") > 1:
        parser.error("stdin can be read only once")
    split = analyse(args.client, args.gateway)
    report = split.report(with_gateway=bool(args.gateway))
    if args.json:
        json.dump(report, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
    else:
        sys.stdout.write(format_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

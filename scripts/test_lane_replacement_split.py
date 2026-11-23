import contextlib
import importlib.util
import io
import json
import pathlib
import tempfile
import unittest


SCRIPT = pathlib.Path(__file__).with_name("lane_replacement_split.py")
SPEC = importlib.util.spec_from_file_location("lane_replacement_split", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

SESSION = "0a" * 16


def client_failure(flow_id, error, **fields):
    record = {
        "time": f"2026-11-20T10:00:{flow_id:02d}Z", "level": "WARN",
        "msg": "local flow ended with error", "service": "queqiaod", "role": "client",
        "session_id": SESSION, "flow_id": flow_id, "error": error,
        "lane_replacement_waits": 1, "lane_replacement_timeouts": 1,
        "lane_replacement_wait": 45_000_000_000, "lanes_joined": 1,
        "lane_replacement_attempts": 0, "lane_replacement_failures": 0,
    }
    record.update(fields)
    return {k: v for k, v in record.items() if v is not None}


SILENT = "last lane failed (timeout: no recent network activity): lane replacement timeout"
EOF_TIMEOUT = "last lane failed (EOF): lane replacement timeout"

CLIENT = [
    # Pool never rebuilt: one silent, one EOF.
    client_failure(1, SILENT),
    client_failure(2, EOF_TIMEOUT),
    # Every attempt returned an error.
    client_failure(3, SILENT, lane_replacement_attempts=2, lane_replacement_failures=2,
                   lane_replacement_last_failure="timeout"),
    client_failure(4, EOF_TIMEOUT, lane_replacement_attempts=1, lane_replacement_failures=1,
                   lane_replacement_last_failure="peer_rejected"),
    client_failure(5, EOF_TIMEOUT, lane_replacement_attempts=3, lane_replacement_failures=3,
                   lane_replacement_last_failure="peer_capacity"),
    client_failure(6, SILENT, lane_replacement_attempts=1, lane_replacement_failures=1,
                   lane_replacement_last_failure="canceled"),
    client_failure(7, SILENT, lane_replacement_attempts=1, lane_replacement_failures=1,
                   lane_replacement_last_failure="something new"),
    # A dial still outstanding when the record was written.
    client_failure(8, EOF_TIMEOUT, lane_replacement_attempts=2, lane_replacement_failures=1,
                   lane_replacement_last_failure="timeout"),
    # Older clients: no attempt counters; or not even lanes_joined.
    client_failure(9, SILENT, lane_replacement_attempts=None, lane_replacement_failures=None),
    client_failure(10, EOF_TIMEOUT, lanes_joined=None, lane_replacement_attempts=None,
                   lane_replacement_failures=None),
    # A replacement was admitted: not part of the split.
    client_failure(11, SILENT, lanes_joined=2, lane_replacement_attempts=1, lane_replacement_failures=0),
    # The other two terminal errors.
    client_failure(12, "last lane failed (EOF): no replacement lane will be attempted"),
    client_failure(13, "peer does not hold this session"),
    # Unrelated failure and an ordinary completion: controls.
    client_failure(14, "destination refused"),
    {"time": "2026-11-20T10:00:15Z", "msg": "local flow complete", "role": "client",
     "session_id": SESSION, "flow_id": 15, "lanes_joined": 1},
    # Other log lines are ignored.
    {"time": "2026-11-20T10:00:16Z", "msg": "lane recovery unavailable", "flow_id": 3, "attempt": 1},
]

GATEWAY = [
    {"msg": "remote flow ended with error", "role": "server", "session_id": SESSION, "flow_id": 1,
     "error": "last lane failed (timeout: no recent network activity): lane replacement timeout",
     "lane_replacement_attempts": 0, "lane_replacement_failures": 0},
    {"msg": "remote flow ended with error", "role": "server", "session_id": SESSION, "flow_id": 4,
     "error": "last lane failed (EOF): lane replacement timeout",
     "lane_replacement_attempts": 0, "lane_replacement_failures": 0},
    {"msg": "lane join refused", "role": "server", "reason": "unknown_session", "flow_id": 4,
     "suppressed": 0, "total": 1},
    {"msg": "lane join refused", "role": "server", "reason": "unknown_session", "flow_id": 9,
     "suppressed": 3, "total": 5},
    {"msg": "lane join refused", "role": "server", "reason": "lane_unavailable", "flow_id": 5,
     "suppressed": 0, "total": 1},
]


def write_jsonl(directory, name, records, extra_lines=()):
    path = pathlib.Path(directory) / name
    lines = [json.dumps(r) for r in records] + list(extra_lines)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(path)


class LaneReplacementSplitTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.client = write_jsonl(self.tmp.name, "client.jsonl", CLIENT, ["not json", "[1, 2]"])
        self.gateway = write_jsonl(self.tmp.name, "server.jsonl", GATEWAY)

    def report(self, gateway=True):
        split = MODULE.analyse([self.client], [self.gateway] if gateway else [])
        return split.report(with_gateway=gateway)

    def test_buckets(self):
        report = self.report()
        self.assertEqual(report["buckets"], {
            MODULE.NO_ATTEMPT: 2,
            MODULE.HANDSHAKE_NOT_CARRIED: 5,
            MODULE.DIAL_OUTSTANDING: 1,
            MODULE.OLD_CLIENT: 2,
            MODULE.INCONSISTENT: 0,
        })
        self.assertEqual(report["handshake_not_carried_by_last_failure"], {
            "timeout": 1, "peer_rejected": 1, "peer_capacity": 1, "canceled": 1, "other": 1,
        })
        self.assertEqual(report["lane_replacement_timeout_by_lanes_joined"], {"1": 9, ">1": 1, "missing": 1})
        self.assertEqual(report["client_flows"], {"complete": 1, "failed": 14})
        self.assertEqual(report["skipped_lines"], {"not json": 1, "not an object": 1})

    def test_missing_counters_are_not_read_as_zero(self):
        # An old client must never land in "no zero-lane replacement attempted".
        bucket, _ = MODULE.timeout_bucket({"lanes_joined": 1})
        self.assertEqual(bucket, MODULE.OLD_CLIENT)
        bucket, _ = MODULE.timeout_bucket({"lane_replacement_attempts": 0, "lane_replacement_failures": True})
        self.assertEqual(bucket, MODULE.OLD_CLIENT)

    def test_failures_above_attempts_is_flagged(self):
        bucket, _ = MODULE.timeout_bucket({"lane_replacement_attempts": 1, "lane_replacement_failures": 2})
        self.assertEqual(bucket, MODULE.INCONSISTENT)
        # Checked before the zero-attempt bucket, which it would otherwise hide in.
        bucket, _ = MODULE.timeout_bucket({"lane_replacement_attempts": 0, "lane_replacement_failures": 1})
        self.assertEqual(bucket, MODULE.INCONSISTENT)

    def test_window_uses_instants_not_strings(self):
        split = MODULE.Split()
        for flow_id, stamp in enumerate([
            "2026-11-20T10:00:00.5Z",              # 10:00:00.5 UTC
            "2026-11-20T18:00:00.123456789+08:00",  # 10:00:00.123 UTC, earliest
            "2026-11-20T05:00:01-05:00",           # 10:00:01 UTC, latest
            "not a time",
        ]):
            split.add_client(client_failure(flow_id, SILENT, time=stamp))
        self.assertEqual(split.report(False)["window"], {
            "first": "2026-11-20T10:00:00.123456Z", "last": "2026-11-20T10:00:01.000000Z",
        })
        self.assertIsNone(MODULE.Split().report(False)["window"])

    def test_other_terminal_errors_counted_separately(self):
        report = self.report()
        self.assertEqual(report["other_terminal_errors"], {
            MODULE.ABANDONED: 1, MODULE.RESUME_REFUSED: 1,
        })
        self.assertEqual(report["last_lane_death"][MODULE.ABANDONED], {"EOF": 1})
        self.assertEqual(report["last_lane_death"][MODULE.RESUME_REFUSED], {"unrecorded": 1})

    def test_last_lane_death_per_bucket(self):
        deaths = self.report()["last_lane_death"]
        self.assertEqual(deaths[MODULE.NO_ATTEMPT], {"EOF": 1, "no recent network activity": 1})
        self.assertEqual(deaths[MODULE.HANDSHAKE_NOT_CARRIED], {"EOF": 2, "no recent network activity": 3})
        self.assertEqual(MODULE.last_lane_death("last lane failed (read tcp: unexpected EOF): x"), "EOF")
        self.assertEqual(MODULE.last_lane_death("last lane failed (connection reset): x"), "other")

    def test_gateway_join_and_refusals(self):
        gateway = self.report()["gateway"]
        self.assertEqual(gateway["failure_records"], 2)
        joined = gateway["joined_by_session_and_flow"]
        self.assertEqual(joined[MODULE.NO_ATTEMPT], {
            "gateway failed, last lane: no recent network activity": 1, "no gateway failure record": 1,
        })
        self.assertEqual(joined[MODULE.HANDSHAKE_NOT_CARRIED], {
            "gateway failed, last lane: EOF": 1, "no gateway failure record": 4,
        })
        self.assertEqual(gateway["lane_join_refused"], {
            "lane_unavailable": {"records": 1, "refusals": 1},
            "unknown_session": {"records": 2, "refusals": 5},
        })

    def test_gateway_records_passed_as_client_input_are_refused(self):
        split = MODULE.analyse([self.gateway], [])
        # The gateway's failure message differs, so nothing is counted, and a
        # client-named record written by a server is set aside.
        self.assertEqual(sum(split.report(False)["buckets"].values()), 0)
        mislabelled = dict(CLIENT[0], role="server")
        split = MODULE.Split()
        split.add_client(mislabelled)
        self.assertEqual(split.ignored_server_records, 1)
        self.assertEqual(sum(split.buckets.values()), 0)

    def test_cli_text_and_json(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(MODULE.main([self.client, "--gateway", self.gateway]), 0)
        text = out.getvalue()
        self.assertIn("no zero-lane replacement attempted (attempts == 0)", text)
        self.assertIn("last_failure=peer_rejected", text)
        self.assertIn("unknown_session", text)

        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(MODULE.main([self.client, "--json"]), 0)
        report = json.loads(out.getvalue())
        self.assertNotIn("gateway", report)
        self.assertEqual(report["buckets"][MODULE.NO_ATTEMPT], 2)


if __name__ == "__main__":
    unittest.main()

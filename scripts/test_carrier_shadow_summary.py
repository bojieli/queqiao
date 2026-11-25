import contextlib
import importlib.util
import io
import json
import pathlib
import tempfile
import unittest


SCRIPT = pathlib.Path(__file__).with_name("carrier_shadow_summary.py")
SPEC = importlib.util.spec_from_file_location("carrier_shadow_summary", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

SESSION = "0b" * 16


def at(seconds):
    whole = int(seconds)
    frac = round((seconds - whole) * 1e6)
    return f"2026-11-25T10:{whole // 60:02d}:{whole % 60:02d}.{frac:06d}Z"


def rec(t, msg, **fields):
    record = {"time": at(t), "level": "INFO", "msg": msg, "pid": 7}
    record.update(fields)
    return record


def zeros():
    return [0] * 8


CLIENT = [
    {"time": at(0), "msg": "runtime configuration"},
    rec(1, "carrier shadow lane bound", endpoint="client", session_id=SESSION, flow_id=1, lane_id=0, carrier_id=1),
    rec(1, "carrier shadow lane bound", endpoint="client", session_id=SESSION, flow_id=2, lane_id=0, carrier_id=2),
    # Flow 1: stall on a carrier that later fails -- design acts 1.5 s later.
    rec(2.0, "carrier shadow stall", endpoint="client", session_id=SESSION, flow_id=1, carrier_id=1, carrier_state="active", transport_epoch=40,
        transport_acked=False, design_remedy="wait", legacy_remedy="demote_and_request_rescue"),
    rec(2.0, "carrier shadow rescue started", endpoint="client", session_id=SESSION, flow_id=1, healthy_lanes=1),
    rec(2.0, "carrier shadow join attempt", endpoint="client", session_id=SESSION, flow_id=1, lane_id=2, transport="quic"),
    # A different attempt of the same flow whose JOIN never arrived.
    rec(8.0, "carrier shadow join attempt", endpoint="client", session_id=SESSION, flow_id=1, lane_id=3, transport="tcp"),
    rec(2.2, "carrier shadow state", endpoint="client", carrier_id=1, to="suspect", transport_epoch=40, **{"from": "active"}, k_s=2, k_p=4),
    rec(3.5, "carrier shadow state", endpoint="client", carrier_id=1, to="replace", transport_epoch=40, **{"from": "suspect"}, k_s=2, k_p=4),
    rec(20.0, "carrier shadow state", endpoint="client", carrier_id=1, to="dead", transport_epoch=40, **{"from": "replace"}, k_s=2, k_p=4),
    rec(20.0, "carrier shadow carrier ended", endpoint="client", carrier_id=1, state="dead",
        pto_reached=[3, 2, 1, 1, 1, 0, 0, 0], pto_recovered=[2, 1, 0, 0, 0, 0, 0, 0], pto_dead=[1, 1, 1, 1, 1, 0, 0, 0]),
    # Flow 2: stall on a live carrier.
    rec(5.0, "carrier shadow stall", endpoint="client", session_id=SESSION, flow_id=2, carrier_id=2, carrier_state="active", transport_epoch=9,
        transport_acked=True, design_remedy="wait", legacy_remedy="demote_and_request_rescue"),
    # Carrier 2 progresses (unlogged), then fails hours later in a newer
    # epoch: not the consequence of the stall above.
    rec(3000.0, "carrier shadow state", endpoint="client", carrier_id=2, to="suspect", transport_epoch=900, **{"from": "active"}),
    rec(3001.0, "carrier shadow state", endpoint="client", carrier_id=2, to="replace", transport_epoch=900, **{"from": "suspect"}),
    # Flow 1 fails again much later; the client sees it 1 s before the gateway.
    rec(2000.0, "carrier shadow lane failed", endpoint="client", session_id=SESSION, flow_id=1, carrier_state="active", design_remedy="rebind_stream"),
    rec(2000.5, "carrier shadow rescue started", endpoint="client", session_id=SESSION, flow_id=1, healthy_lanes=0),
    rec(2000.5, "carrier shadow join attempt", endpoint="client", session_id=SESSION, flow_id=1, lane_id=5, transport="tcp"),
    # A still-open carrier's episode line.
    rec(6.0, "carrier shadow episode", endpoint="client", carrier_id=2, max_pto=2, outcome="recovered"),
]

GATEWAY = [
    # The gateway loses flow 1's last lane 4 s after the client first saw it;
    # the client's rescue JOIN arrives 2.5 s after its rescue started.
    rec(6.0, "carrier shadow outage", endpoint="gateway", session_id=SESSION, flow_id=1, grace=45_000_000_000),
    rec(4.5, "carrier shadow join arrived", endpoint="gateway", session_id=SESSION, flow_id=1, lane_id=1, in_outage=False),
    rec(8.5, "carrier shadow join arrived", endpoint="gateway", session_id=SESSION, flow_id=1, lane_id=2, in_outage=True),
    rec(8.6, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=1),
    rec(2001.0, "carrier shadow outage", endpoint="gateway", session_id=SESSION, flow_id=1, grace=45_000_000_000),
    rec(2002.0, "carrier shadow join arrived", endpoint="gateway", session_id=SESSION, flow_id=1, lane_id=5, in_outage=True),
    rec(2002.1, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=1),
    # Flow 3's outage is still running when the capture stops.
    rec(2010.0, "carrier shadow outage", endpoint="gateway", session_id=SESSION, flow_id=3, grace=45_000_000_000),
]


class CarrierShadowSummaryTest(unittest.TestCase):
    def run_script(self, *extra):
        with tempfile.TemporaryDirectory() as directory:
            paths = {}
            for name, records in (("client", CLIENT), ("gateway", GATEWAY)):
                path = pathlib.Path(directory) / f"{name}.jsonl"
                path.write_text("\n".join(json.dumps(r) for r in records) + "\nnot json\n")
                paths[name] = str(path)
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                MODULE.main(["--client", paths["client"], "--gateway", paths["gateway"], *extra])
            return out.getvalue()

    def test_calibration_rates(self):
        report = json.loads(self.run_script("--json"))
        rows = report["calibration"]["client"]["rows"]
        # n = 1 comes from complete summaries only; the open carrier's
        # two-expiry episode line counts from n = 2.
        self.assertEqual(rows[0]["reached"], 3)
        self.assertEqual(rows[0]["recovered"], 2)
        self.assertEqual(rows[1]["reached"], 3)
        self.assertAlmostEqual(rows[1]["false_rate"], 2 / 3)
        self.assertEqual(rows[3]["false_rate"], 0.0)
        self.assertIsNone(rows[5]["false_rate"])
        self.assertGreater(rows[3]["false_rate_upper95"], 0.0)
        self.assertEqual(report["skipped"]["client"], {"not json": 1})

    def test_stall_outcomes(self):
        report = json.loads(self.run_script("--json"))
        stalls = report["stalls"]["client"]
        self.assertEqual(stalls["stalls"], 2)
        self.assertEqual(stalls["outcomes"]["carrier reached replace after the stall"], 1)
        self.assertEqual(stalls["outcomes"]["carrier stayed live: design would not rescue"], 1)
        self.assertEqual(stalls["design_detection_delay_s"]["n"], 1)
        self.assertAlmostEqual(stalls["design_detection_delay_s"]["max"], 1.5, places=3)

    def test_recovery_deadline(self):
        report = json.loads(self.run_script("--json"))
        deadline = report["recovery_deadline"]
        self.assertEqual(deadline["gateway_outages"], 3)
        # Each outage is paired with its own observation and JOIN: skews 4 s
        # and 1 s, transits 6.5 s and 1.5 s -- not the first failure of the
        # flow against the second outage.
        self.assertEqual(deadline["skew_s"]["n"], 2)
        self.assertAlmostEqual(deadline["skew_s"]["min"], 1.0, places=3)
        self.assertAlmostEqual(deadline["skew_s"]["max"], 4.0, places=3)
        self.assertAlmostEqual(deadline["join_transit_s"]["max"], 6.5, places=3)
        self.assertAlmostEqual(deadline["join_transit_s"]["min"], 1.5, places=3)
        self.assertAlmostEqual(deadline["join_arrival_margin_s"]["min"], 42.5, places=3)
        self.assertEqual(deadline["joins_after_deadline"], 0)
        self.assertEqual(deadline["outages_without_join"], 0)
        self.assertEqual(deadline["outages_unfinished_at_capture_end"], 1)
        # G - S - J with S = 0 (the gateway was never earlier) and J = 6.5.
        self.assertEqual(deadline["S_s"], 0.0)
        self.assertAlmostEqual(deadline["J_s"], 6.5, places=3)
        self.assertAlmostEqual(deadline["latest_handoff_start_s"], 45 - 6.5, places=3)
        self.assertAlmostEqual(deadline["join_transit_by_transport_s"]["tcp"]["max"], 1.5, places=3)
        self.assertAlmostEqual(deadline["join_transit_by_transport_s"]["quic"]["max"], 6.5, places=3)

    def test_clock_offset_moves_gateway_times(self):
        report = json.loads(self.run_script("--json", "--clock-offset-ms", "1000"))
        self.assertAlmostEqual(report["recovery_deadline"]["skew_s"]["min"], 0.0, places=3)

    def test_text_report(self):
        text = self.run_script()
        self.assertIn("latest handoff start after the client's own observation, G - S - J", text)
        self.assertIn("carrier=active acked=True design=wait today=demote_and_request_rescue", text)


class CarrierShadowCensoringTest(unittest.TestCase):
    def test_unresolved_stall_and_measured_grace(self):
        client = [
            rec(1.0, "carrier shadow stall", endpoint="client", session_id=SESSION, flow_id=9, carrier_id=4,
                carrier_state="active", transport_epoch=3, transport_acked=False, design_remedy="wait",
                legacy_remedy="demote_and_request_rescue"),
            rec(1.5, "carrier shadow join attempt", endpoint="client", session_id=SESSION, flow_id=9, lane_id=1, transport="quic"),
        ]
        gateway = [
            rec(2.0, "carrier shadow outage", endpoint="gateway", session_id=SESSION, flow_id=9, grace=10_000_000_000),
            rec(3.0, "carrier shadow join arrived", endpoint="gateway", session_id=SESSION, flow_id=9, lane_id=1),
            rec(3.1, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=9),
            # Ordinary logging continues long after.
            {"time": at(300), "msg": "performance snapshot"},
        ]
        with tempfile.TemporaryDirectory() as directory:
            paths = []
            for name, records in (("c", client), ("g", gateway)):
                path = pathlib.Path(directory) / f"{name}.jsonl"
                path.write_text("\n".join(json.dumps(r) for r in records) + "\n")
                paths.append(str(path))
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                MODULE.main(["--client", paths[0], "--gateway", paths[1], "--json"])
        report = json.loads(out.getvalue())
        self.assertEqual(report["stalls"]["client"]["outcomes"], {"unresolved: no later evidence about the carrier": 1})
        deadline = report["recovery_deadline"]
        self.assertEqual(deadline["grace_s"], 10.0)
        self.assertAlmostEqual(deadline["latest_handoff_start_s"], 10.0 - 0.0 - 1.5, places=3)


class LateJoinTest(unittest.TestCase):
    def test_a_join_after_the_deadline_is_late_not_missing(self):
        client = [rec(1.0, "carrier shadow join attempt", endpoint="client", session_id=SESSION, flow_id=4, lane_id=7, transport="tcp")]
        gateway = [
            rec(2.0, "carrier shadow outage", endpoint="gateway", session_id=SESSION, flow_id=4, grace=10_000_000_000),
            rec(12.5, "carrier shadow join arrived", endpoint="gateway", session_id=SESSION, flow_id=4, lane_id=7, in_outage=False, session_ended=True),
            {"time": at(100), "msg": "performance snapshot"},
        ]
        with tempfile.TemporaryDirectory() as directory:
            paths = []
            for name, records in (("c", client), ("g", gateway)):
                path = pathlib.Path(directory) / f"{name}.jsonl"
                path.write_text("\n".join(json.dumps(r) for r in records) + "\n")
                paths.append(str(path))
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                MODULE.main(["--client", paths[0], "--gateway", paths[1], "--json"])
        deadline = json.loads(out.getvalue())["recovery_deadline"]
        self.assertEqual(deadline["joins_after_deadline"], 1)
        self.assertEqual(deadline["outages_without_join"], 0)
        self.assertTrue(deadline["flows"][0]["join_refused_session_ended"])
        self.assertAlmostEqual(deadline["join_transit_s"]["max"], 11.5, places=3)


def summarize(client, gateway):
    with tempfile.TemporaryDirectory() as directory:
        paths = []
        for name, records in (("c", client), ("g", gateway)):
            path = pathlib.Path(directory) / f"{name}.jsonl"
            path.write_text("\n".join(json.dumps(r) for r in records) + "\n")
            paths.append(str(path))
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            MODULE.main(["--client", paths[0], "--gateway", paths[1], "--json"])
    return json.loads(out.getvalue())


def stall(t, flow, carrier, epoch, **fields):
    record = dict(endpoint="client", session_id=SESSION, flow_id=flow, carrier_id=carrier, carrier_state="active",
                  transport_epoch=epoch, transport_acked=False, design_remedy="wait", legacy_remedy="demote_and_request_rescue")
    record.update(fields)
    return rec(t, "carrier shadow stall", **record)


class CalibrationFollowUpTest(unittest.TestCase):
    def test_skew_runs_from_the_first_observation_of_the_matched_incident(self):
        client = [
            # An earlier incident, resolved: the next stall is in a newer epoch.
            stall(0.0, 1, 1, 4),
            # The incident the gateway outage matches: first seen at 1.0.
            stall(1.0, 1, 1, 5),
            stall(3.0, 1, 1, 5),
        ]
        gateway = [
            rec(4.0, "carrier shadow outage", endpoint="gateway", session_id=SESSION, flow_id=1, grace=45_000_000_000),
            rec(4.5, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=1),
        ]
        deadline = summarize(client, gateway)["recovery_deadline"]
        self.assertAlmostEqual(deadline["skew_s"]["max"], 3.0, places=3)
        self.assertEqual(deadline["skew_s"]["n"], 1)

    def test_a_carrier_recovery_between_observations_starts_a_new_incident(self):
        client = [
            stall(0.0, 1, 1, 5),
            rec(0.5, "carrier shadow state", endpoint="client", carrier_id=1, to="active", transport_epoch=6, **{"from": "suspect"}),
            rec(2.0, "carrier shadow lane failed", endpoint="client", session_id=SESSION, flow_id=1, carrier_id=1, carrier_state="active"),
        ]
        gateway = [rec(3.0, "carrier shadow outage", endpoint="gateway", session_id=SESSION, flow_id=1, grace=45_000_000_000)]
        deadline = summarize(client, gateway)["recovery_deadline"]
        self.assertAlmostEqual(deadline["skew_s"]["max"], 1.0, places=3)

    def test_joins_started_after_recovery_are_not_transit(self):
        client = [
            stall(3.9, 2, 1, 5),
            rec(4.2, "carrier shadow join attempt", endpoint="client", session_id=SESSION, flow_id=2, lane_id=8, transport="quic"),
            # Started after the outage ended: an ordinary JOIN.
            rec(6.0, "carrier shadow join attempt", endpoint="client", session_id=SESSION, flow_id=2, lane_id=9, transport="quic"),
        ]
        gateway = [
            rec(4.0, "carrier shadow outage", endpoint="gateway", session_id=SESSION, flow_id=2, grace=45_000_000_000),
            rec(4.8, "carrier shadow join arrived", endpoint="gateway", session_id=SESSION, flow_id=2, lane_id=8, in_outage=True),
            rec(4.9, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=2),
            rec(6.5, "carrier shadow join arrived", endpoint="gateway", session_id=SESSION, flow_id=2, lane_id=9, in_outage=False),
        ]
        deadline = summarize(client, gateway)["recovery_deadline"]
        self.assertEqual(deadline["join_transit_s"]["n"], 1)
        self.assertAlmostEqual(deadline["join_transit_s"]["max"], 0.6, places=3)
        self.assertEqual(deadline["joins_started_after_recovery"], 1)

    def test_an_episode_open_at_capture_end_is_censored(self):
        client = [
            rec(1.0, "carrier shadow state", endpoint="client", carrier_id=3, to="suspect", transport_epoch=7, pto_since_progress=2, **{"from": "active"}),
            stall(2.0, 5, 3, 7, carrier_state="suspect", pto_since_progress=3),
            # A carrier whose episode ended in progress is not open.
            rec(1.0, "carrier shadow state", endpoint="client", carrier_id=6, to="suspect", transport_epoch=1, pto_since_progress=2, **{"from": "active"}),
            rec(1.5, "carrier shadow state", endpoint="client", carrier_id=6, to="active", transport_epoch=2, **{"from": "suspect"}),
        ]
        report = summarize(client, [rec(9.0, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=99)])
        cal = report["calibration"]["client"]
        self.assertEqual(cal["open_episodes_censored"], 1)
        rows = cal["rows"]
        for n in range(3):
            self.assertEqual(rows[n]["reached"], 1)
            self.assertEqual(rows[n]["censored"], 1)
            self.assertIsNone(rows[n]["false_rate"], "a censored episode is not in the denominator")
        self.assertEqual(rows[3]["reached"], 0)

    def test_a_later_stall_in_a_newer_epoch_is_progress(self):
        client = [stall(10.0, 7, 4, 1), stall(20.0, 7, 4, 2)]
        report = summarize(client, [rec(30.0, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=99)])
        outcomes = report["stalls"]["client"]["outcomes"]
        self.assertEqual(outcomes["carrier stayed live: design would not rescue"], 1)
        self.assertEqual(outcomes["unresolved: no later evidence about the carrier"], 1)

    def test_rescue_requests_are_counted_against_rescue_rounds(self):
        def request(t):
            return rec(t, "carrier shadow rescue requested", endpoint="client", session_id=SESSION, flow_id=7)

        def attempt(t, lane, **fields):
            return rec(t, "carrier shadow join attempt", endpoint="client", session_id=SESSION, flow_id=7, lane_id=lane, transport="quic", **fields)

        client = [
            stall(10.0, 7, 4, 1),
            request(10.0),
            rec(10.0, "carrier shadow rescue started", endpoint="client", session_id=SESSION, flow_id=7, healthy_lanes=1, rescue_round=41),
            attempt(10.5, 1, rescue_round=41),
            attempt(11.0, 2, rescue_round=41),
            # The same stall episode asks again; the manager skips it.
            request(15.0),
            # A much later ordinary JOIN belongs to no round.
            attempt(500.0, 3),
        ]
        gateway = [stall(12.0, 7, 9, 1, endpoint="gateway", legacy_remedy="demote"),
                   rec(30.0, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=99)]
        report = summarize(client, gateway)
        requests = report["stalls"]["client"]["rescue_requests"]
        self.assertEqual(requests, {"requests": 2, "requests_followed_by_a_rescue_round": 1, "requests_without_a_rescue_round": 1,
                                    "join_attempts_in_those_rounds": 2})
        self.assertIsNone(report["stalls"]["gateway"]["rescue_requests"], "a gateway stall requests nothing")
        self.assertEqual(report["stalls"]["client"]["by_kind"][0]["legacy_remedy"], "demote_and_request_rescue")

    def test_an_episode_line_closes_an_open_episode(self):
        client = [
            stall(1.0, 5, 3, 7, pto_since_progress=1),
            rec(1.2, "carrier shadow episode", endpoint="client", carrier_id=3, max_pto=2, outcome="recovered", transport_epoch=8),
        ]
        report = summarize(client, [rec(9.0, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=99)])
        self.assertEqual(report["calibration"]["client"]["open_episodes_censored"], 0)

    def test_a_newer_zero_count_observation_closes_an_open_episode(self):
        client = [stall(1.0, 5, 3, 7, pto_since_progress=1), stall(2.0, 5, 3, 8, pto_since_progress=0)]
        report = summarize(client, [rec(9.0, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=99)])
        self.assertEqual(report["calibration"]["client"]["open_episodes_censored"], 0)

    def test_incident_boundaries(self):
        gateway = [rec(100.0, "carrier shadow outage", endpoint="gateway", session_id=SESSION, flow_id=1, grace=45_000_000_000)]
        # An unrelated carrier's recovery does not split the incident on
        # carrier 1, and an incident that began before the window keeps its
        # start: skew 60 s.
        client = [
            stall(40.0, 1, 1, 5),
            rec(70.0, "carrier shadow state", endpoint="client", carrier_id=2, to="active", transport_epoch=3, **{"from": "suspect"}),
            stall(90.0, 1, 1, 5),
        ]
        self.assertAlmostEqual(summarize(client, gateway)["recovery_deadline"]["skew_s"]["max"], 60.0, places=3)
        # The client's own outage ending (a replacement lane came) ends the
        # incident even though the old carrier never recovered.
        client = [
            stall(40.0, 1, 1, 5),
            rec(70.0, "carrier shadow outage ended", endpoint="client", session_id=SESSION, flow_id=1),
            stall(90.0, 1, 2, 1),
        ]
        self.assertAlmostEqual(summarize(client, gateway)["recovery_deadline"]["skew_s"]["max"], 10.0, places=3)

if __name__ == "__main__":
    unittest.main()

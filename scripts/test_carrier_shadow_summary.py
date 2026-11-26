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

    def test_a_resolved_incident_does_not_match_a_later_outage(self):
        client = [
            stall(9.0, 1, 1, 5),
            rec(9.5, "carrier shadow state", endpoint="client", carrier_id=1, to="active", transport_epoch=6, **{"from": "suspect"}),
            stall(12.0, 1, 1, 6),
        ]
        gateway = [rec(10.0, "carrier shadow outage", endpoint="gateway", session_id=SESSION, flow_id=1, grace=45_000_000_000),
                   rec(13.0, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=1)]
        self.assertAlmostEqual(summarize(client, gateway)["recovery_deadline"]["skew_s"]["max"], -2.0, places=3)

    def test_a_recovered_episode_line_is_progress_after_a_stall(self):
        client = [
            stall(10.0, 7, 4, 1, pto_since_progress=1),
            rec(10.4, "carrier shadow episode", endpoint="client", carrier_id=4, max_pto=3, outcome="recovered", transport_epoch=2),
        ]
        report = summarize(client, [rec(30.0, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=99)])
        self.assertEqual(report["stalls"]["client"]["outcomes"], {"carrier stayed live: design would not rescue": 1})
        self.assertEqual(report["calibration"]["client"]["open_episodes_censored"], 0)

    def test_a_lane_failure_is_evidence_of_an_open_episode(self):
        client = [rec(3.0, "carrier shadow lane failed", endpoint="client", session_id=SESSION, flow_id=5, carrier_id=8,
                      carrier_state="active", pto_since_progress=1, transport_epoch=4)]
        report = summarize(client, [rec(9.0, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=99)])
        self.assertEqual(report["calibration"]["client"]["open_episodes_censored"], 1)

    def test_records_of_one_sample_keep_their_order(self):
        # One sample ends an episode and finds a new one SUSPECT: same time,
        # emitted in that order.
        client = [
            rec(5.0, "carrier shadow episode", endpoint="client", carrier_id=8, max_pto=2, outcome="recovered", transport_epoch=4, observed_unix_ms=5000),
            rec(5.0, "carrier shadow state", endpoint="client", carrier_id=8, to="suspect", transport_epoch=4, pto_since_progress=2,
                observed_unix_ms=5000, **{"from": "active"}),
        ]
        report = summarize(client, [rec(9.0, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=99)])
        self.assertEqual(report["calibration"]["client"]["open_episodes_censored"], 1)

    def test_round_four_review(self):
        # A recovery after the last client observation ends that incident.
        client = [stall(9.0, 1, 1, 5),
                  rec(9.5, "carrier shadow state", endpoint="client", carrier_id=1, to="active", transport_epoch=6, **{"from": "suspect"})]
        gateway = [rec(10.0, "carrier shadow outage", endpoint="gateway", session_id=SESSION, flow_id=1, grace=45_000_000_000),
                   rec(13.0, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=1)]
        self.assertIsNone(summarize(client, gateway)["recovery_deadline"]["skew_s"])
        # A terminal or unwatched lane observation is no open episode, and a
        # newer-epoch lane failure is progress after a stall.
        client = [
            rec(3.0, "carrier shadow lane failed", endpoint="client", session_id=SESSION, flow_id=5, carrier_id=0,
                carrier_state="dead", pto_since_progress=4, transport_epoch=0),
            stall(4.0, 6, 9, 2, pto_since_progress=1),
            rec(5.0, "carrier shadow lane failed", endpoint="client", session_id=SESSION, flow_id=6, carrier_id=9,
                carrier_state="dead", pto_since_progress=3, transport_epoch=2),
            stall(6.0, 7, 4, 1),
            rec(7.0, "carrier shadow lane failed", endpoint="client", session_id=SESSION, flow_id=7, carrier_id=4,
                carrier_state="active", pto_since_progress=0, transport_epoch=2),
        ]
        report = summarize(client, [rec(30.0, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=99)])
        self.assertEqual(report["calibration"]["client"]["open_episodes_censored"], 0)
        self.assertEqual(report["stalls"]["client"]["outcomes"].get("carrier stayed live: design would not rescue"), 1)
        # A round the manager started to replace a lost lane answers no request.
        client = [
            rec(1.0, "carrier shadow rescue requested", endpoint="client", session_id=SESSION, flow_id=7),
            rec(1.1, "carrier shadow rescue started", endpoint="client", session_id=SESSION, flow_id=7, healthy_lanes=0, rescue_round=3, trigger="stall"),
            rec(2.0, "carrier shadow rescue requested", endpoint="client", session_id=SESSION, flow_id=7),
            rec(2.1, "carrier shadow rescue started", endpoint="client", session_id=SESSION, flow_id=7, healthy_lanes=0, rescue_round=4, trigger="outage"),
        ]
        requests = summarize(client, [rec(30.0, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=99)])["stalls"]["client"]["rescue_requests"]
        self.assertEqual((requests["requests_followed_by_a_rescue_round"], requests["requests_without_a_rescue_round"]), (1, 1))

    def test_round_five_review(self):
        gateway = [rec(100.0, "carrier shadow outage", endpoint="gateway", session_id=SESSION, flow_id=1, grace=45_000_000_000),
                   rec(120.0, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=1)]
        # A new incident first seen on carrier 2, then on carrier 1 whose
        # epoch moved since the previous incident: one incident.
        client = [
            stall(10.0, 1, 1, 5),
            rec(20.0, "carrier shadow outage ended", endpoint="client", session_id=SESSION, flow_id=1),
            stall(90.0, 1, 2, 1),
            stall(95.0, 1, 1, 9),
        ]
        self.assertAlmostEqual(summarize(client, gateway)["recovery_deadline"]["skew_s"]["max"], 10.0, places=3)
        # A zero-lane outage on the client is not ended by its former
        # carrier recovering for another flow.
        client = [
            rec(80.0, "carrier shadow lane failed", endpoint="client", session_id=SESSION, flow_id=1, carrier_id=1, carrier_state="active", transport_epoch=5),
            rec(80.1, "carrier shadow outage", endpoint="client", session_id=SESSION, flow_id=1, grace=45_000_000_000),
            rec(85.0, "carrier shadow state", endpoint="client", carrier_id=1, to="active", transport_epoch=6, **{"from": "suspect"}),
        ]
        self.assertAlmostEqual(summarize(client, gateway)["recovery_deadline"]["skew_s"]["max"], 20.0, places=3)
        # A JOIN started within the outage end's millisecond is this outage's.
        client = [rec(4.9002, "carrier shadow join attempt", endpoint="client", session_id=SESSION, flow_id=2, lane_id=8, transport="quic")]
        gateway = [
            rec(4.0, "carrier shadow outage", endpoint="gateway", session_id=SESSION, flow_id=2, grace=45_000_000_000),
            rec(4.9008, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=2,
                observed_unix_ms=int(MODULE.parse_instant(at(4.9008)) * 1000)),
            rec(5.2, "carrier shadow join arrived", endpoint="gateway", session_id=SESSION, flow_id=2, lane_id=8, in_outage=False),
        ]
        deadline = summarize(client, gateway)["recovery_deadline"]
        self.assertEqual(deadline["joins_started_after_recovery"], 0)
        self.assertEqual(deadline["join_transit_s"]["n"], 1)

class CountingRecord(dict):
    """A record that counts how often its transport epoch is read."""

    reads = 0

    def get(self, key, default=None):
        if key == "transport_epoch":
            CountingRecord.reads += 1
        return super().get(key, default)


class SummaryFollowUpTest(unittest.TestCase):
    def test_progress_seen_by_another_flow_ends_an_incident(self):
        client = [
            stall(10.0, 1, 1, 5),
            # Another flow on the same pooled carrier sees it in a newer
            # epoch: flow 1's incident is over.
            stall(20.0, 2, 1, 6),
        ]
        gateway = [rec(50.0, "carrier shadow outage", endpoint="gateway", session_id=SESSION, flow_id=1, grace=45_000_000_000),
                   rec(60.0, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=1)]
        self.assertIsNone(summarize(client, gateway)["recovery_deadline"]["skew_s"])

    def test_an_outage_that_ends_in_its_own_millisecond_is_over(self):
        start = int(MODULE.parse_instant(at(10.0)) * 1000)
        client = [
            # The end can be logged before its start: both are asynchronous.
            rec(10.0, "carrier shadow outage ended", endpoint="client", session_id=SESSION, flow_id=1, outage_id=7, observed_unix_ms=start),
            rec(10.0, "carrier shadow outage", endpoint="client", session_id=SESSION, flow_id=1, outage_id=7, observed_unix_ms=start,
                grace=45_000_000_000),
        ]
        gateway = [rec(30.0, "carrier shadow outage", endpoint="gateway", session_id=SESSION, flow_id=1, grace=45_000_000_000),
                   rec(40.0, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=1)]
        self.assertIsNone(summarize(client, gateway)["recovery_deadline"]["skew_s"])

    def test_stall_comparison_reads_each_epoch_a_bounded_number_of_times(self):
        endpoint = MODULE.Endpoint("client")
        n = 2000
        for i in range(n):
            record = CountingRecord(stall(float(i), i, 1, 5))
            record.update(_t=float(i), _process=7, _seq=i)
            endpoint.records.append(record)
        CountingRecord.reads = 0
        report = MODULE.stall_comparison(endpoint)
        self.assertEqual(report["stalls"], n)
        self.assertLess(CountingRecord.reads, 10 * n, "each stall rescanned the carrier's later history")

    def test_review_round(self):
        # Gateway: an outage that ends in the millisecond the next starts
        # keeps its own end.
        t0 = int(MODULE.parse_instant(at(10.0)) * 1000)
        gateway = [
            rec(5.0, "carrier shadow outage", endpoint="gateway", session_id=SESSION, flow_id=1, outage_id=1, grace=45_000_000_000),
            rec(10.0, "carrier shadow outage", endpoint="gateway", session_id=SESSION, flow_id=1, outage_id=2, grace=45_000_000_000, observed_unix_ms=t0),
            rec(10.0, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=1, outage_id=1, observed_unix_ms=t0),
            rec(12.0, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=1, outage_id=2),
        ]
        deadline = summarize([stall(1.0, 9, 9, 1)], gateway)["recovery_deadline"]
        self.assertEqual(deadline["outages_without_join"], 0, "an outage lost its own end to the next one")
        gateway_far = [rec(100.0, "carrier shadow outage", endpoint="gateway", session_id=SESSION, flow_id=1, grace=45_000_000_000),
                       rec(110.0, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=1)]
        # A, then B, then A in a newer epoch: the flow's own comparison
        # still sees A's progress.
        client = [stall(70.0, 1, 1, 5), stall(75.0, 1, 2, 1), stall(90.0, 1, 1, 6)]
        self.assertAlmostEqual(summarize(client, gateway_far)["recovery_deadline"]["skew_s"]["max"], 10.0, places=3)
        # Another flow's newer epoch in the same millisecond is progress.
        t1 = int(MODULE.parse_instant(at(80.0)) * 1000)
        client = [stall(80.0, 1, 1, 5, observed_unix_ms=t1), stall(80.0, 2, 1, 6, observed_unix_ms=t1)]
        self.assertIsNone(summarize(client, gateway_far)["recovery_deadline"]["skew_s"])
        # An identified end whose outage start is not in the capture still
        # ends the flow's incident.
        client = [rec(70.0, "carrier shadow lane failed", endpoint="client", session_id=SESSION, flow_id=1, carrier_id=1, carrier_state="active"),
                  rec(72.0, "carrier shadow outage ended", endpoint="client", session_id=SESSION, flow_id=1, outage_id=5)]
        self.assertIsNone(summarize(client, gateway_far)["recovery_deadline"]["skew_s"])


class CountingList(list):
    """A list that counts element reads, so a test can bound list scans."""

    reads = 0

    def __getitem__(self, index):
        CountingList.reads += 1
        return super().__getitem__(index)

    def __iter__(self):
        for i in range(len(self)):
            yield self[i]


def counted(value):
    """Wrap every list reachable from value in a CountingList."""
    if isinstance(value, list):
        return CountingList(counted(v) for v in value)
    if isinstance(value, dict):
        return {k: counted(v) for k, v in value.items()}
    return value


class AttributionEdgeCaseTest(unittest.TestCase):
    """The four attribution edge cases of #196."""

    GATEWAY_FAR = [rec(100.0, "carrier shadow outage", endpoint="gateway", session_id=SESSION, flow_id=1, grace=45_000_000_000),
                   rec(110.0, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=1)]

    def test_tied_outage_starts_are_ordered_by_outage_id(self):
        t0 = int(MODULE.parse_instant(at(10.0)) * 1000)
        # Gateway: two starts in one millisecond, the newer (id 2) logged
        # first and still open at capture end; the older one ends.
        gateway = [
            rec(10.0, "carrier shadow outage", endpoint="gateway", session_id=SESSION, flow_id=1, outage_id=2,
                grace=45_000_000_000, observed_unix_ms=t0),
            rec(10.0, "carrier shadow outage", endpoint="gateway", session_id=SESSION, flow_id=1, outage_id=1,
                grace=45_000_000_000, observed_unix_ms=t0),
            rec(10.0, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=1, outage_id=1,
                observed_unix_ms=t0),
            rec(12.0, "unrelated", endpoint="gateway"),
        ]
        deadline = summarize([stall(1.0, 9, 9, 1)], gateway)["recovery_deadline"]
        self.assertEqual(deadline["outages_without_join"], 0, "the open outage was closed by its older sibling")
        self.assertEqual(deadline["outages_unfinished_at_capture_end"], 1)
        # Client: the same order; the newer outage stays open, so the
        # client's incident is not over when the gateway's outage starts.
        client = [
            rec(70.0, "carrier shadow outage", endpoint="client", session_id=SESSION, flow_id=1, outage_id=2,
                grace=45_000_000_000, observed_unix_ms=int(MODULE.parse_instant(at(70.0)) * 1000)),
            rec(70.0, "carrier shadow outage", endpoint="client", session_id=SESSION, flow_id=1, outage_id=1,
                grace=45_000_000_000, observed_unix_ms=int(MODULE.parse_instant(at(70.0)) * 1000)),
            rec(70.0, "carrier shadow outage ended", endpoint="client", session_id=SESSION, flow_id=1, outage_id=1,
                observed_unix_ms=int(MODULE.parse_instant(at(70.0)) * 1000)),
        ]
        skew = summarize(client, self.GATEWAY_FAR)["recovery_deadline"]["skew_s"]
        self.assertIsNotNone(skew, "the open outage's incident was closed by its older sibling's end")
        self.assertAlmostEqual(skew["max"], 30.0, places=3)

    def test_a_state_record_in_a_newer_epoch_ends_an_incident(self):
        client = [
            stall(70.0, 1, 1, 5),
            # The carrier made progress, then became suspect again: epoch 6.
            rec(75.0, "carrier shadow state", endpoint="client", carrier_id=1, to="suspect", transport_epoch=6, **{"from": "active"}),
        ]
        self.assertIsNone(summarize(client, self.GATEWAY_FAR)["recovery_deadline"]["skew_s"])

    def test_an_unmatched_end_does_not_close_an_outage_with_another_id(self):
        # Gateway: outage 2 has no end; the end of outage 1, whose start the
        # capture lost, is not outage 2's.
        gateway = [
            rec(10.0, "carrier shadow outage", endpoint="gateway", session_id=SESSION, flow_id=1, outage_id=2, grace=45_000_000_000),
            rec(12.0, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=1, outage_id=1),
        ]
        deadline = summarize([stall(1.0, 9, 9, 1)], gateway)["recovery_deadline"]
        self.assertEqual(deadline["outages_unfinished_at_capture_end"], 1, "a foreign end closed the outage")
        # An end without an identifier is still flow-level evidence.
        gateway[1] = rec(12.0, "carrier shadow outage ended", endpoint="gateway", session_id=SESSION, flow_id=1)
        deadline = summarize([stall(1.0, 9, 9, 1)], gateway)["recovery_deadline"]
        self.assertEqual(deadline["outages_unfinished_at_capture_end"], 0)
        # Client: the same; outage 2 is open when the gateway's starts.
        client = [
            rec(70.0, "carrier shadow outage", endpoint="client", session_id=SESSION, flow_id=1, outage_id=2, grace=45_000_000_000),
            rec(72.0, "carrier shadow outage ended", endpoint="client", session_id=SESSION, flow_id=1, outage_id=1),
        ]
        skew = summarize(client, self.GATEWAY_FAR)["recovery_deadline"]["skew_s"]
        self.assertIsNotNone(skew, "a foreign end closed the client's incident")
        self.assertAlmostEqual(skew["max"], 30.0, places=3)
        client[1] = rec(72.0, "carrier shadow outage ended", endpoint="client", session_id=SESSION, flow_id=1)
        self.assertIsNone(summarize(client, self.GATEWAY_FAR)["recovery_deadline"]["skew_s"])

    def test_an_epoch_reset_keeps_progress_lookups_logarithmic(self):
        n = 4000
        endpoint = MODULE.Endpoint("client")
        # A reused process/carrier key: one high epoch, then n stalls in a
        # lower one.
        records = [stall(0.0, 0, 1, 100)] + [stall(1.0, i, 1, 1) for i in range(1, n + 1)]
        for i, record in enumerate(records):
            record.update(_t=float(i), _process=7, _seq=i)
            endpoint.records.append(record)
        original = MODULE.CarrierProgress

        class Counted(original):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                for name, value in vars(self).items():
                    setattr(self, name, counted(value))

        MODULE.CarrierProgress = Counted
        try:
            CountingList.reads = 0
            report = MODULE.stall_comparison(endpoint)
        finally:
            MODULE.CarrierProgress = original
        self.assertEqual(report["stalls"], n + 1)
        self.assertLess(CountingList.reads, 100 * n, "each progress lookup scanned the carrier's later history")


if __name__ == "__main__":
    unittest.main()

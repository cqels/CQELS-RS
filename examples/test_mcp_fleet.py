import contextlib
import hashlib
import io
import json
from pathlib import Path
import queue
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

import mcp_fleet as fleet


class FleetControls(unittest.TestCase):
    def test_session_cleanup_error_preserves_forced_shutdown_evidence(self):
        rpc = fleet.Rpc.__new__(fleet.Rpc)
        rpc.process = Mock(stdin=io.StringIO(), stdout=io.StringIO(), stderr=io.StringIO(), returncode=-15)
        rpc.process.wait.side_effect = [fleet.subprocess.TimeoutExpired("server", 10), -15]
        rpc.readers = []
        rpc.directory = Mock()
        rpc.directory.cleanup.side_effect = PermissionError("scanner retains a file")
        rpc.close(timeout=10)
        self.assertTrue(rpc.forced_shutdown)
        self.assertEqual(rpc.process.returncode, -15)
        self.assertEqual(rpc.process.wait.call_args_list[0].kwargs["timeout"], 10)
        rpc.process.terminate.assert_called_once()
        rpc.directory.cleanup.assert_called_once()

    def test_blank_lines_are_ignored_but_non_json_is_reported(self):
        rpc = fleet.Rpc.__new__(fleet.Rpc)
        rpc.process = Mock(stdout=io.StringIO('\n  \n{"id":1}\nnot JSON\n'))
        rpc.messages, rpc.contamination = queue.Queue(), []
        rpc._stdout()
        self.assertEqual(rpc.messages.get_nowait(), {"id": 1})
        self.assertEqual(rpc.contamination, ["not JSON"])

    def test_java_startup_signal_survives_log_eviction(self):
        rpc = fleet.Rpc.__new__(fleet.Rpc)
        rpc.stderr, rpc.java_started, rpc.timeout = [], threading.Event(), 1
        rpc.process = Mock(stderr=io.StringIO("CQELS MCP server is running.\n" + "later log\n" * 250))
        rpc._stderr()
        self.assertNotIn("CQELS MCP server is running.", rpc.stderr)
        rpc.wait_for_java_launcher()
        rpc.process.poll.assert_not_called()

    def test_negotiated_protocol_must_match(self):
        rpc = fleet.Rpc.__new__(fleet.Rpc)
        rpc.request = Mock(return_value={"protocolVersion": "different"})
        rpc.send = Mock()
        with self.assertRaisesRegex(AssertionError, "negotiated protocol"):
            rpc.initialize()
        rpc.send.assert_not_called()

    def test_cleanup_handles_already_closed_pipes(self):
        rpc = fleet.Rpc.__new__(fleet.Rpc)
        rpc.process, rpc.directory, rpc.readers = Mock(), Mock(), []
        rpc.process.stdin.close.side_effect = BrokenPipeError()
        rpc.process.stdout.close.side_effect = OSError()
        rpc.process.stderr.close.side_effect = OSError()
        rpc.close()
        rpc.directory.cleanup.assert_called_once()

    def test_java_digest_is_verified(self):
        with tempfile.TemporaryDirectory() as directory:
            jar = Path(directory) / "server.jar"
            jar.write_bytes(b"reference")
            reference = {"sha256": hashlib.sha256(b"reference").hexdigest()}
            fleet.verify_java_jar(jar, reference)
            jar.write_bytes(b"wrong jar")
            with self.assertRaisesRegex(ValueError, "SHA-256"):
                fleet.verify_java_jar(jar, reference)

    def test_java_report_uses_reference_version(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            here = root / "examples"
            (here / "fleet").mkdir(parents=True)
            (root / "mcp-server").mkdir()
            jar = root / "server.jar"
            jar.write_bytes(b"jar")
            pin = {"version": "rust-version", "java_reference": {
                "version": "java-version", "sha256": hashlib.sha256(b"jar").hexdigest()}}
            (root / "RELEASE.json").write_text(json.dumps(pin), encoding="utf-8")
            (root / "mcp-server/contract.json").write_text('{}', encoding="utf-8")
            (here / "fleet/expectations.json").write_text('{"rdfs":{"java":[]}}', encoding="utf-8")
            report = root / "report.json"
            rpc = Mock(contamination=[])
            rpc.initialize.return_value = {"serverInfo": {"version": "java-version"}}
            with patch.object(fleet, "HERE", here), patch.object(fleet, "Rpc", return_value=rpc), \
                 patch.object(fleet, "surface", return_value={}), patch.object(fleet, "run_scenario", return_value=[]), \
                 patch("sys.argv", ["fleet", "--java-jar", str(jar), "--report", str(report)]), \
                 contextlib.redirect_stdout(io.StringIO()):
                fleet.main()
            self.assertEqual(json.loads(report.read_text())["version"], "java-version")

    def test_rejected_ingestion_cannot_pass_zero_row_probe(self):
        rpc = Mock()
        rpc.tool_result.return_value = {"structuredContent": {"accepted": 0}}
        with self.assertRaisesRegex(AssertionError, "not accepted"):
            fleet.run_scenario(rpc, "aggregation")
        rpc.drain.assert_not_called()

    def test_zero_row_probe_records_successful_ingestion(self):
        rpc = Mock()
        rpc.tool_result.return_value = {"structuredContent": {"accepted": 1, "durable": False}}
        rpc.drain.return_value = []
        controls = {}
        self.assertEqual(fleet.run_scenario(rpc, "aggregation", controls), [])
        self.assertEqual(len(controls["push_acks"]), 3)
        for call in rpc.tool_result.call_args_list:
            self.assertTrue(call.kwargs["events"][0]["nquads"].startswith('VERSION "1.2-messages"'))

    def test_missing_static_seed_cannot_pass_zero_row_probe(self):
        rpc = Mock()
        rpc.tool.return_value = []
        with self.assertRaisesRegex(AssertionError, "static seed readback"):
            fleet.run_scenario(rpc, "static-join")
        rpc.tool_result.assert_not_called()

    def test_cep_checks_exact_subjects_for_both_encodings(self):
        for events in [f"[{{subject={fleet.EX}event/1, timestamp=1000}}, {{subject={fleet.EX}event/2, timestamp=2000}}]",
                       [{"subject": fleet.EX + "event/1"}, {"subject": fleet.EX + "event/2"}]]:
            rpc = Mock()
            rpc.tool_result.return_value = {"structuredContent": {"accepted": 1}}
            rpc.drain.return_value = [{"start": 1000, "end": 2000, "events": events}]
            self.assertEqual(fleet.run_scenario(rpc, "cep"), [{"start": 1000, "end": 2000}])
        rpc.drain.return_value = [{"start": 1000, "end": 2000, "events":
            f"[{{subject={fleet.EX}event/10}}, {{subject={fleet.EX}event/2}}]"}]
        with self.assertRaisesRegex(AssertionError, "unexpected CEP"):
            fleet.run_scenario(rpc, "cep")

    def test_reversed_control_uses_the_same_query(self):
        queries = []
        for scenario in ["cep", "cep-reversed"]:
            rpc = Mock()
            rpc.tool_result.return_value = {"structuredContent": {"accepted": 1}}
            rpc.drain.return_value = []
            fleet.run_scenario(rpc, scenario)
            queries.append(next(c.kwargs["query"] for c in rpc.tool.call_args_list if c.args[0] == "register_stream_query"))
        self.assertEqual(queries[0], queries[1])

    def test_numeric_normalization_is_symmetric(self):
        expected = [{"n": n, "avgSpeed": n, "peak": n} for n in (10, 2)]
        actual = [{k: str(v) for k, v in row.items()} for row in reversed(expected)]
        self.assertEqual(fleet.canonical_rows(actual, "aggregation"), fleet.canonical_rows(expected, "aggregation"))

    def capture_fake_report(self, observed, expected, fail_during_push=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            here = root / "examples"
            (here / "fleet").mkdir(parents=True)
            (root / "mcp-server").mkdir()
            server = root / "server"
            server.write_bytes(b"fake")
            (root / "RELEASE.json").write_text('{"version":"test"}', encoding="utf-8")
            (root / "mcp-server/contract.json").write_text('{}', encoding="utf-8")
            (here / "fleet/expectations.json").write_text(json.dumps({"low-battery": {"rust": expected}}), encoding="utf-8")
            report = root / "report.json"
            rpc = Mock(contamination=[])
            rpc.initialize.return_value = {"serverInfo": {"version": "test"}}
            def scenario(rpc, name, controls):
                controls["push_acks"] = [{"accepted": 1}]
                if fail_during_push:
                    raise AssertionError("later push failed")
                return observed
            with patch.object(fleet, "HERE", here), patch.object(fleet, "Rpc", return_value=rpc), \
                 patch.object(fleet, "surface", return_value={}), patch.object(fleet, "run_scenario", side_effect=scenario), \
                 patch("sys.argv", ["fleet", "--server", str(server), "--report", str(report)]), \
                 contextlib.redirect_stdout(io.StringIO()):
                try:
                    fleet.main()
                except AssertionError:
                    pass
            return json.loads(report.read_text(encoding="utf-8"))["scenarios"]["low-battery"]

    def test_report_preserves_raw_numeric_strings(self):
        entry = self.capture_fake_report([{"soc": "18.5"}], [{"soc": 18.5}])
        self.assertEqual(entry["status"], "pass")
        self.assertEqual(entry["rows"], [{"soc": "18.5"}])
        self.assertEqual(entry["normalized"], [{"soc": 18.5}])

    def test_mismatch_report_preserves_observations_and_controls(self):
        entry = self.capture_fake_report([{"soc": "12"}], [{"soc": 18.5}])
        self.assertEqual(entry["status"], "fail")
        self.assertEqual(entry["rows"], [{"soc": "12"}])
        self.assertEqual(entry["controls"]["push_acks"], [{"accepted": 1}])

    def test_partial_failure_report_preserves_controls(self):
        entry = self.capture_fake_report([], [], fail_during_push=True)
        self.assertEqual(entry["status"], "fail")
        self.assertIn("later push failed", entry["error"])
        self.assertEqual(entry["controls"]["push_acks"], [{"accepted": 1}])


if __name__ == "__main__":
    unittest.main()

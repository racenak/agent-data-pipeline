from memory.signature import extract_error_signature
from memory.formatter import format_past_incidents
from memory.schema import Incident


class TestExtractErrorSignature:
    def test_extracts_error_line(self):
        logs = "INFO: starting\nERROR: connection refused\nINFO: done"
        sig = extract_error_signature(logs)
        assert sig is not None
        assert "connection refused" in sig

    def test_normalizes_timestamps(self):
        logs = "2026-07-22T10:30:45.123Z ERROR: timeout"
        sig = extract_error_signature(logs)
        assert "2026" not in sig
        assert "<timestamp>" in sig

    def test_normalizes_numeric_ids(self):
        logs = "ERROR: user 12345678 not found"
        sig = extract_error_signature(logs)
        assert "12345678" not in sig
        assert "<id>" in sig

    def test_returns_none_for_no_errors(self):
        logs = "INFO: all good\nDEBUG: processing"
        assert extract_error_signature(logs) is None

    def test_uses_first_error_line(self):
        logs = "ERROR: first error\nERROR: second error"
        sig = extract_error_signature(logs)
        assert "first error" in sig

    def test_case_insensitive(self):
        logs = "Exception: something broke"
        sig = extract_error_signature(logs)
        assert sig is not None

    def test_empty_logs(self):
        assert extract_error_signature("") is None


class TestFormatPastIncidents:
    def test_empty_incidents(self):
        result = format_past_incidents([])
        assert "No past incidents" in result

    def test_formats_single_incident(self):
        inc = Incident(
            pipeline_name="etl",
            error_signature="error: timeout",
            error_summary="Connection timed out",
            root_cause="Network issue",
            severity="CRITICAL",
            suggested_actions=["Check network", "Retry"],
        )
        result = format_past_incidents([inc])
        assert "etl" in result
        assert "Connection timed out" in result
        assert "CRITICAL" in result

    def test_shows_resolved_status(self):
        inc = Incident(
            pipeline_name="etl",
            error_signature="error: timeout",
            resolved=True,
            resolution_notes="Restarted service",
        )
        result = format_past_incidents([inc])
        assert "RESOLVED" in result

    def test_shows_unresolved_status(self):
        inc = Incident(
            pipeline_name="etl",
            error_signature="error: timeout",
            resolved=False,
        )
        result = format_past_incidents([inc])
        assert "UNRESOLVED" in result


class TestIncidentSchema:
    def test_required_fields(self):
        inc = Incident(pipeline_name="test", error_signature="err")
        assert inc.pipeline_name == "test"
        assert inc.error_signature == "err"
        assert inc.resolved is False
        assert inc.suggested_actions == []

    def test_all_fields(self):
        inc = Incident(
            pipeline_name="test",
            task_id="task-1",
            error_signature="err",
            error_summary="summary",
            root_cause="cause",
            severity="CRITICAL",
            suggested_actions=["fix1", "fix2"],
            resolved=True,
            resolution_notes="done",
        )
        assert inc.task_id == "task-1"
        assert inc.severity == "CRITICAL"
        assert len(inc.suggested_actions) == 2

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from graphs.monitoring import route_after_analysis, AgentState


class TestRouteAfterAnalysis:
    def _state(self, **overrides):
        base = {
            "raw_logs": "",
            "job_metadata": {},
            "processed_logs": "",
            "analysis_result": {},
            "retry_count": 0,
            "missing_info_reason": "",
            "messages": [],
            "past_incidents": "",
        }
        base.update(overrides)
        return base

    def test_routes_to_tools_when_tool_calls_present(self):
        class FakeMsg:
            tool_calls = [{"name": "grafana"}]

        state = self._state(messages=[FakeMsg()])
        assert route_after_analysis(state) == "tools"

    def test_routes_to_fetch_when_not_enough_info(self):
        state = self._state(
            analysis_result={"is_enough_info": False},
            retry_count=0,
        )
        assert route_after_analysis(state) == "fetch_more"

    def test_routes_to_fetch_until_max_retries(self):
        state = self._state(
            analysis_result={"is_enough_info": False},
            retry_count=2,
        )
        assert route_after_analysis(state) == "slack"

    def test_routes_to_slack_when_enough_info(self):
        state = self._state(
            analysis_result={"is_enough_info": True},
        )
        assert route_after_analysis(state) == "slack"

    def test_routes_to_slack_by_default(self):
        state = self._state()
        assert route_after_analysis(state) == "slack"

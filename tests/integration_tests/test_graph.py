import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from graphs.monitoring import build_graph

pytestmark = pytest.mark.anyio


async def test_graph_builds():
    graph = await build_graph()
    assert graph is not None
    assert hasattr(graph, "nodes")
    assert "filter_log" in graph.nodes
    assert "analyze_log" in graph.nodes
    assert "send_slack" in graph.nodes


async def test_graph_has_all_nodes():
    graph = await build_graph()
    expected = {
        "__start__",
        "filter_log",
        "lookup_memory",
        "analyze_log",
        "save_incident",
        "fetch_more_log",
        "send_slack",
        "tools",
    }
    assert set(graph.nodes) == expected

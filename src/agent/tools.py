from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta

import httpx
from langchain_core.tools import tool

PREFECT_API = "http://prefect-server:4200/api"


@tool
def check_prefect_failures(since_hours: int = 24) -> str:
    """Query Prefect server for failed flow runs in the last N hours."""
    now = datetime.now(timezone.utc)
    since = (now - timedelta(hours=since_hours)).isoformat()

    try:
        resp = httpx.post(
            f"{PREFECT_API}/flow_runs/filter",
            json={
                "flows": None,
                "flow_runs": {
                    "start_time_after": since,
                    "status": ["Failed", "Crashed", "TimedOut"],
                },
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            return f"No failed flows in the last {since_hours}h."

        lines = [f"Found {len(data)} failed flow run(s):"]
        for run in data:
            lines.append(
                f"  - {run.get('name', '?')} [{run.get('status', '?')}] "
                f"at {run.get('start_time', run.get('expected_start_time', '?'))}"
            )
        return "\n".join(lines)
    except Exception as e:
        return f"Error querying Prefect: {e}"


@tool
def check_clickhouse(query: str) -> str:
    """Run a read-only SQL query on ClickHouse (pipeline database)."""
    try:
        import clickhouse_connect

        client = clickhouse_connect.get_client(
            host="clickhouse",
            port=8123,
            username="clickhouse",
            password="clickhouse",
            database="pipeline",
        )
        result = client.query(query)
        if not result.result_rows:
            return "Query returned no results."
        columns = result.column_names
        rows = [dict(zip(columns, row)) for row in result.result_rows]
        return json.dumps(rows, indent=2, default=str)
    except Exception as e:
        return f"Error querying ClickHouse: {e}"


tools = [check_prefect_failures, check_clickhouse]

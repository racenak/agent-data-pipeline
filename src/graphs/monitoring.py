from __future__ import annotations

import json
import os
from typing import Any, Dict, Literal

from dotenv import load_dotenv
import httpx

load_dotenv()
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_openrouter import ChatOpenRouter
from langgraph.graph import END, StateGraph
from langgraph.prebuilt import ToolNode, tools_condition
from typing_extensions import TypedDict

from memory import Incident, IncidentStore, extract_error_signature, format_past_incidents

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+asyncpg://agent:agent@localhost:5433/agent_memory",
)

MAX_RETRIES = 2


class AgentState(TypedDict):
    raw_logs: str
    job_metadata: Dict[str, Any]
    processed_logs: str
    analysis_result: Dict[str, Any]
    retry_count: int
    missing_info_reason: str
    messages: list
    past_incidents: str


def make_filter_log_node():
    def filter_log_node(state: AgentState) -> Dict[str, Any]:
        lines = state["raw_logs"].split("\n")
        error_indices = [
            i for i, line in enumerate(lines)
            if "error" in line.lower() or "exception" in line.lower()
        ]
        if error_indices:
            start = max(0, error_indices[0] - 15)
            end = min(len(lines), error_indices[-1] + 35)
            processed = "\n".join(lines[start:end])
        else:
            processed = "\n".join(lines[-100:])
        return {"processed_logs": processed, "retry_count": 0}
    return filter_log_node


def make_lookup_memory_node(store: IncidentStore):
    async def lookup_memory_node(state: AgentState) -> Dict[str, Any]:
        pipeline_name = state["job_metadata"].get("pipeline_name", "")
        signature = extract_error_signature(state["raw_logs"])

        if not signature or not pipeline_name:
            return {"past_incidents": ""}

        try:
            incidents = await store.find_similar(
                error_signature=signature,
                pipeline_name=pipeline_name,
                limit=3,
            )
            past = format_past_incidents(incidents)
        except Exception:
            past = ""

        return {"past_incidents": past}
    return lookup_memory_node


def make_analyze_log_node(llm):
    def analyze_log_node(state: AgentState) -> Dict[str, Any]:
        past = state.get("past_incidents", "")
        system_prompt = (
            "You are an expert Data Platform SRE Agent.\n"
            "Analyze the provided application/data logs and metadata to identify the root cause.\n"
            "\n"
            f"{past}\n"
            "\n"
            "Use the past incidents above to check if this is a recurring issue. "
            "If a similar incident was resolved before, reference the previous resolution.\n"
            "\n"
            "You MUST return a JSON object with EXACTLY the following fields:\n"
            "1. 'is_enough_info': true/false (Set false ONLY if logs are cut off and you absolutely need lines BEFORE or AFTER to conclude).\n"
            "2. 'missing_info_reason': String (If is_enough_info is false, specify what you need, else '').\n"
            "3. 'error_summary': One sentence summarizing the error.\n"
            "4. 'root_cause': Detailed diagnosis (e.g., OOM, Auth failure, Schema drift).\n"
            "5. 'suggested_actions': List of string actions to resolve this.\n"
            "6. 'severity': 'CRITICAL' or 'WARNING'.\n"
            "Output ONLY valid JSON, no markdown fences. "
            "Use available tools (e.g. grafana) to query metrics or dashboards if needed."
        )

        user_content = (
            f"Job Metadata: {json.dumps(state['job_metadata'])}\n\n"
            f"Log Snippet:\n{state['processed_logs']}"
        )

        response = llm.invoke(
            [SystemMessage(content=system_prompt), HumanMessage(content=user_content)]
        )

        result = json.loads(response.content)
        return {
            "analysis_result": result,
            "missing_info_reason": result.get("missing_info_reason", ""),
            "messages": [HumanMessage(content=user_content), response],
        }

    return analyze_log_node


def make_fetch_more_log_node():
    def fetch_more_log_node(state: AgentState) -> Dict[str, Any]:
        extended_logs = (
            f"[EXTENDED LOG PREPENDED]\n"
            f"... (Older log data restored) ...\n"
            f"{state['raw_logs']}\n"
            f"[EXTENDED LOG APPENDED]"
        )
        return {
            "processed_logs": extended_logs,
            "retry_count": state["retry_count"] + 1,
        }
    return fetch_more_log_node


def make_save_incident_node(store: IncidentStore):
    async def save_incident_node(state: AgentState) -> Dict[str, Any]:
        analysis = state["analysis_result"]
        meta = state["job_metadata"]
        signature = extract_error_signature(state["raw_logs"])

        if not signature:
            return {}

        incident = Incident(
            pipeline_name=meta.get("pipeline_name", "unknown"),
            task_id=meta.get("task_id"),
            error_signature=signature,
            error_summary=analysis.get("error_summary"),
            root_cause=analysis.get("root_cause"),
            severity=analysis.get("severity"),
            suggested_actions=analysis.get("suggested_actions", []),
        )

        try:
            await store.save(incident)
        except Exception:
            pass

        return {}
    return save_incident_node


def make_send_slack_node():
    def send_slack_node(state: AgentState) -> Dict[str, Any]:
        analysis = state["analysis_result"]
        meta = state["job_metadata"]
        color = "#FF0000" if analysis.get("severity") == "CRITICAL" else "#FF9900"

        slack_payload = {
            "attachments": [
                {
                    "color": color,
                    "blocks": [
                        {
                            "type": "header",
                            "text": {
                                "type": "plain_text",
                                "text": f"AI SRE Alert: {meta.get('pipeline_name', 'unknown')}",
                            },
                        },
                        {
                            "type": "section",
                            "fields": [
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Task ID:* `{meta.get('task_id', '?')}`",
                                },
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Severity:* `{analysis.get('severity', '?')}`",
                                },
                            ],
                        },
                        {"type": "divider"},
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": (
                                    f"*Summary:* {analysis.get('error_summary', '')}\n\n"
                                    f"*Root Cause:*\n{analysis.get('root_cause', '')}"
                                ),
                            },
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": "*Suggested Fixes:*\n"
                                + "\n".join(
                                    f"• {a}" for a in analysis.get("suggested_actions", [])
                                ),
                            },
                        },
                    ],
                }
            ]
        }

        if SLACK_WEBHOOK_URL:
            httpx.post(SLACK_WEBHOOK_URL, json=slack_payload, timeout=10)
        return {}

    return send_slack_node


def route_after_analysis(state: AgentState) -> Literal["fetch_more", "tools", "slack"]:
    if state.get("messages"):
        last_msg = state["messages"][-1]
        if hasattr(last_msg, "tool_calls") and last_msg.tool_calls:
            return "tools"
    analysis = state.get("analysis_result", {})
    if not analysis.get("is_enough_info", True) and state.get("retry_count", 0) < MAX_RETRIES:
        return "fetch_more"
    return "slack"


async def build_graph():
    tools = []
    try:
        client = MultiServerMCPClient(
            {
                "grafana": {
                    "transport": "streamable_http",
                    "url": os.environ.get("GRAFANA_MCP_URL", "http://localhost:8000/mcp"),
                }
            }
        )
        tools = await client.get_tools()
    except Exception:
        pass

    llm = ChatOpenRouter(model=os.environ.get("LLM_MODEL", "openai/gpt-oss-20b:free"), temperature=0.1)
    llm = llm.bind_tools(tools)

    store = IncidentStore(dsn=DATABASE_URL)
    try:
        await store.init()
    except Exception:
        pass

    workflow = StateGraph(AgentState)

    workflow.add_node("filter_log", make_filter_log_node())
    workflow.add_node("lookup_memory", make_lookup_memory_node(store))
    workflow.add_node("analyze_log", make_analyze_log_node(llm))
    workflow.add_node("save_incident", make_save_incident_node(store))
    workflow.add_node("fetch_more_log", make_fetch_more_log_node())
    workflow.add_node("send_slack", make_send_slack_node())
    workflow.add_node("tools", ToolNode(tools))

    workflow.set_entry_point("filter_log")
    workflow.add_edge("filter_log", "lookup_memory")
    workflow.add_edge("lookup_memory", "analyze_log")

    workflow.add_conditional_edges(
        "analyze_log",
        route_after_analysis,
        {
            "tools": "tools",
            "fetch_more": "fetch_more_log",
            "slack": "save_incident",
        },
    )

    workflow.add_edge("save_incident", "send_slack")
    workflow.add_edge("tools", "analyze_log")
    workflow.add_edge("fetch_more_log", "analyze_log")
    workflow.add_edge("send_slack", END)

    return workflow.compile()

# agent-data-pipeline

An AI-assisted batch data pipeline that combines **Prefect** for orchestration
with a **LangGraph** monitoring agent (an "AI SRE") that analyzes pipeline logs,
queries observability tools, and posts alerts to Slack.

## Overview

The project has two cooperating parts:

1. **ETL flows (Prefect)** — `flow/etl.py` extracts user data from a public API,
   transforms and cleans it, then loads it into **ClickHouse**. After the load,
   the flow invokes the monitoring agent over HTTP for automated analysis.
2. **Monitoring agent (LangGraph)** — `src/graphs/monitoring.py` is a stateful
   agent graph (`monitoring`) that:
   - filters raw logs down to the relevant error window,
   - asks an LLM (via OpenRouter) to diagnose root cause and severity,
   - can call **Grafana** MCP tools to pull metrics/dashboards when more context
     is needed,
   - fetches more log context if the analysis is inconclusive (up to 2 retries),
   - posts a formatted alert to **Slack**.

## Architecture

```
                        ┌─────────────────────────────┐
                        │     Prefect (orchestration) │
                        │  - extract / transform / load│
                        │  - ClickHouse load           │
                        └──────────────┬──────────────┘
                                       │ HTTP /runs/wait
                                       ▼
                        ┌─────────────────────────────┐
                        │  LangGraph "monitoring" agent│
                        │  filter_log → analyze_log    │
                        │       ↘ tools (Grafana MCP)  │
                        │       ↘ fetch_more_log       │
                        │            → send_slack      │
                        └─────────────────────────────┘

Observability stack (docker-compose):
  Prefect server · Postgres · Redis · Prefect worker
  Prometheus · Loki · Promtail · Grafana · Grafana MCP · ClickHouse
```

## Data Pipeline

The data pipeline is a Prefect flow defined in `flow/etl.py`
(`main_etl_flow`, deployed as **"User ETL Pipeline Blueprint"**). It runs the
classic extract → transform → load pattern, then triggers the monitoring agent:

1. **Extract** (`extract_users`) — fetches raw user records from the public
   [JSONPlaceholder](https://jsonplaceholder.typicode.com/users) API. Retries up
   to 3 times with a 5s delay on failure.
2. **Transform** (`transform_users`) — cleans each record, lowercases
   `username`/`email`, and flattens the nested `company` name into a flat
   dictionary.
3. **Load** (`load_users`) — connects to **ClickHouse** via `clickhouse-connect`
   and upserts into a `ReplacingMergeTree` `users` table (`id`, `name`,
   `username`, `email`, `company`).
4. **Agent monitor** (`run_agent_monitor`) — POSTs the job metadata to the
   LangGraph server (`/runs/wait`) for automated log analysis. On `CRITICAL`
   severity it logs an error; failures are caught and logged as a warning so the
   pipeline still completes.

A second trivial flow, `flow/hello.py` (`hello_flow`), is included as a
deployment example.

## Agent Workflow

The monitoring agent is a LangGraph state graph built in
`src/graphs/monitoring.py` (`build_graph`) and served as the `monitoring` graph
via `langgraph.json`. Its `AgentState` carries the raw/processed logs, job
metadata, analysis result, retry counter, and chat messages. The node flow:

```
filter_log → analyze_log ─┬─(needs tools)─▶ tools ──────────┐
                          ├─(needs more logs)─▶ fetch_more_log ┘
                          └─(done)─▶ send_slack ─▶ END
```

- **`filter_log`** — trims the raw logs to a window around the first/last
  `error`/`exception` lines (or the last 100 lines if none).
- **`analyze_log`** — sends a system-prompted SRE request (with metadata + log
  snippet) to an OpenRouter LLM (`deepseek/deepseek-v4-flash`). The model must
  return strict JSON: `is_enough_info`, `missing_info_reason`, `error_summary`,
  `root_cause`, `suggested_actions`, and `severity` (`CRITICAL`/`WARNING`). The
  LLM is bound to Grafana MCP tools (streamable HTTP at `:8000`) for pulling
  metrics/dashboards.
- **`tools`** — executes any Grafana tool calls, then loops back to
  `analyze_log`.
- **`fetch_more_log`** — extends the log context and increments `retry_count`,
  looping back to `analyze_log` (capped at `MAX_RETRIES = 2`).
- **`send_slack`** — posts a color-coded (red for CRITICAL, orange for WARNING)
  Slack attachment summarizing the analysis; posts only if `SLACK_WEBHOOK_URL`
  is set.

Routing after `analyze_log` (`route_after_analysis`) decides whether to call
tools, fetch more logs, or send the alert. The agent can be exercised
interactively via `src/main.py`.

## Repository layout

| Path | Purpose |
| --- | --- |
| `flow/etl.py` | Main Prefect ETL flow (extract → transform → load → agent monitor) |
| `flow/hello.py` | Minimal example flow |
| `src/graphs/monitoring.py` | LangGraph monitoring / SRE agent |
| `src/main.py` | Interactive CLI to chat with the agent |
| `langgraph.json` | LangGraph deployment config (graph: `monitoring`) |
| `prefect.yaml` | Prefect deployment definitions (`etl`, `hello`) |
| `docker-compose.yaml` | Composes Prefect + observability stacks |
| `configs/monitoring/` | Prometheus, Loki/Promtail, Grafana datasources & dashboards |
| `scripts/` | `setup.sh` (env bootstrap) and `run_pipeline.sh` |
| `tests/` | Unit and integration tests |

## Prerequisites

- Python `>=3.11`
- [uv](https://github.com/astral-sh/uv) (for dependency management)
- Docker & Docker Compose
- An [OpenRouter](https://openrouter.ai) API key
- A Slack incoming webhook URL (optional, for alerts)

## Setup

```bash
# 1. Create a virtual environment and install dependencies
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# 2. Configure environment variables
cp .env.example .env   # then edit: OPENROUTER_API_KEY, SLACK_WEBHOOK_URL, ...
```

`docker-compose.yaml` pulls in two additional files:

- `docker-prefect.yaml` — Prefect server, Postgres, Redis, a Prefect worker
  (`local-pool`), and a Prometheus exporter.
- `observation.yaml` — Prometheus, Loki, Promtail, Grafana, the Grafana MCP
  server (exposed on `:8000`), and ClickHouse.

## Running locally

### 1. Start the infrastructure

```bash
docker compose up -d
```

This brings up Prefect (UI on `http://localhost:4200`), Grafana
(`http://localhost:3000`, admin/admin), Prometheus (`:9090`), ClickHouse
(`:8123`), and the Grafana MCP server (`:8000`).

### 2. Start the LangGraph agent server

```bash
langgraph dev   # serves the "monitoring" graph on http://127.0.0.1:2024
```

The ETL flow calls this server at `AGENT_API_URL` (default
`http://127.0.0.1:2024`).

### 3. Run the ETL flow

```bash
python flow/etl.py            # run directly
# or via Prefect after deploying:
prefect deploy -f prefect.yaml
prefect worker start --pool local-pool
```

### 4. Chat with the agent interactively

```bash
python -m src.main
```

Type a question at the `>` prompt; enter `exit` to quit.

## Configuration

Key environment variables (see `.env`):

| Variable | Default | Description |
| --- | --- | --- |
| `OPENROUTER_API_KEY` | — | Required for LLM access |
| `SLACK_WEBHOOK_URL` | `""` | Slack incoming webhook for alerts (disabled if empty) |
| `AGENT_API_URL` | `http://127.0.0.1:2024` | LangGraph server URL used by the ETL flow |
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse host for the load step |
| `GRAFANA_SERVICE_ACCOUNT_TOKEN` | — | Token for the Grafana MCP server |

Monitoring retries in the agent are capped at `MAX_RETRIES = 2`
(`src/graphs/monitoring.py`).

## Monitoring & alerting

The agent emits a Slack attachment per analysis with severity (`CRITICAL` /
`WARNING`), error summary, root cause, and suggested fixes. Grafana dashboards
for Prefect flow runs and platform metrics are provisioned under
`configs/monitoring/grafana/dashboards/`.

## Testing

```bash
pytest
```

Lint and type checks (configured in `pyproject.toml`):

```bash
ruff check .
mypy .
```

## CI

`.github/workflows/build-worker.yaml` builds and pushes the Prefect worker image
(`Dockerfile.prefect-worker`) to GitHub Container Registry on pushes to the
`data-pipeline` branch.

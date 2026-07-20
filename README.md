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

The data pipeline is an **agent-free** Prefect flow defined in `flow/etl.py`
(`main_etl_flow`, deployed as **"User ETL Pipeline Blueprint"**). It runs the
following stages, failing fast with a Slack alert on any validation error:

```
extract → load_raw_to_seaweedfs → schema_validation (GE)
                                     ├─ FAIL → send_slack(CRITICAL) → end
                                     └─ PASS → transform → load_staging_clickhouse
                                               → data_quality_validation (GE)
                                                 ├─ FAIL → send_slack(CRITICAL) → end
                                                 └─ PASS → merge_to_mart → send_slack(INFO)
```

1. **Extract** (`extract_users`) — fetches raw user records from the public
   [JSONPlaceholder](https://jsonplaceholder.typicode.com/users) API. Retries up
   to 3 times with a 5s delay on failure.
2. **Load raw** (`load_raw_to_seaweedfs`) — persists the raw payload as an S3
   object in **SeaweedFS** (`s3://<bucket>/users/<run_id>.json`) via `boto3`
   for lineage/audit. The S3 gateway listens on `:8333` (see `docker-data.yaml`).
3. **Schema validation** (`schema_validation`) — a **Great Expectations** suite
   on the raw data checking required columns exist, `id` is integer-typed, and
   `email` matches a basic regex. On failure a `CRITICAL` Slack alert is sent
   and the flow stops.
4. **Transform** (`transform_users`) — cleans each record, lowercases
   `username`/`email`, and flattens the nested `company` name into a flat
   dictionary.
5. **Load staging** (`load_staging_clickhouse`) — connects to **ClickHouse** via
   `clickhouse-connect` and inserts into the `users_staging` `ReplacingMergeTree`
   table.
6. **Data quality validation** (`data_quality_validation`) — a second **Great
   Expectations** suite on the cleaned data: non-null `email`, unique `id`,
   `username` lowercase, valid `email`. On failure a `CRITICAL` Slack alert is
   sent and the flow stops.
7. **Merge to mart** (`merge_to_mart`) — `INSERT INTO users_mart SELECT ... FROM
   users_staging`, deduplicated via `ReplacingMergeTree` (ORDER BY `id`).
8. **Slack** (`send_slack`) — posts a color-coded alert (red CRITICAL / green
   INFO). No-op when `SLACK_WEBHOOK_URL` is unset.

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
| `flow/etl.py` | Agent-free Prefect ETL flow: extract → SeaweedFS raw → GE schema → transform → ClickHouse staging → GE data quality → mart → Slack |
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
- `docker-data.yaml` — SeaweedFS (S3 gateway on `:8333`) and ClickHouse, used by
  the data pipeline for raw object storage and the mart.

## Running locally

### 1. Start the infrastructure

For the **data pipeline** (no agent required):

```bash
docker compose -f docker-data.yaml up -d
```

This brings up **SeaweedFS** (S3 gateway on `:8333`, Filer UI on `:8888`) and
**ClickHouse** (`:8123`). For the full stack including the monitoring agent:

```bash
docker compose up -d   # docker-prefect.yaml + observation.yaml
```

That brings up Prefect (UI on `http://localhost:4200`), Grafana
(`http://localhost:3000`, admin/admin), Prometheus (`:9090`), ClickHouse
(`:8123`), and the Grafana MCP server (`:8000`).

### 2. Start the LangGraph agent server (optional, agent only)

```bash
langgraph dev   # serves the "monitoring" graph on http://127.0.0.1:2024
```

Required only if you run the LangGraph monitoring agent; the data pipeline does
not call it.

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
| `SLACK_WEBHOOK_URL` | `""` | Slack incoming webhook for pipeline alerts (disabled if empty) |
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse host for staging + mart steps |
| `SEAWEEDFS_ENDPOINT` | `http://localhost:8333` | SeaweedFS S3 gateway URL |
| `SEAWEEDFS_ACCESS_KEY` | `""` | S3 access key for SeaweedFS |
| `SEAWEEDFS_SECRET_KEY` | `""` | S3 secret key for SeaweedFS |
| `SEAWEEDFS_BUCKET` | `raw` | S3 bucket for raw payloads |
| `OPENROUTER_API_KEY` | — | Required only for the LangGraph monitoring agent |
| `GRAFANA_SERVICE_ACCOUNT_TOKEN` | — | Token for the Grafana MCP server (agent only) |

The data pipeline itself does **not** depend on the agent or OpenRouter.

The LangGraph agent retries are capped at `MAX_RETRIES = 2`
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

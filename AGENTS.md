# Data Pipeline Agent

## Project Overview
Automated batch data pipeline orchestration using Prefect, ClickHouse, and AI agents via LangGraph.

## Architecture
- **Prefect** — workflow orchestration (flows, tasks, deployments)
- **ClickHouse** — analytics database for pipeline data
- **LangGraph** — AI agent framework (agent-api serves the compiled graph at POST /runs)
- **Monitoring** — LangSmith for LLM/tool observability; Prometheus + Loki + Grafana for infra/pipeline monitoring

## Key Directories
- `src/agent/` — LangGraph agent graph (compiled `StateGraph` exported as `graph`)
- `src/agent/tools.py` — agent tools (check_prefect_failures, check_clickhouse)
- `flow/` — Prefect flow definitions
- `configs/` — YAML configs, monitoring configs

## How to Run
- Start stack: `podman compose up -d`
- Build agent image: `podman build -t agent-langgraph:latest -f Dockerfile.agent .`
- Invoke agent: `curl -X POST http://localhost:8000/runs -d '{"query":"..."}'`

## Coding Conventions
- Python 3.12+, async where possible
- Prefect flows in flow/, agent logic in src/agent/
- Agent tools are plain Python functions with docstrings (LLM reads them)

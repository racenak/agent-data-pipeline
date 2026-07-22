#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SCRIPT_DIR"

# 1. Create virtual environment with uv
if [ ! -d .venv ]; then
    uv venv
    echo "Created virtual environment"
else
    echo "Virtual environment already exists"
fi

source .venv/bin/activate

# 2. Install all dependencies (agent + ETL + dev)
uv pip install -e ".[dev]"

# 3. Create .env if missing
if [ ! -f .env ]; then
    cat > .env << 'EOF'
# OpenRouter API key — https://openrouter.ai/keys
OPENROUTER_API_KEY=

# Default LLM model to use
LLM_MODEL=deepseek/deepseek-v4-flash

LANGSMITH_API_KEY=
LANGSMITH_TRACING=true
LANGSMITH_ENDPOINT=https://apac.api.smith.langchain.com
LANGSMITH_PROJECT="agent-data-pipeline"

GRAFANA_SERVICE_ACCOUNT_TOKEN=

SLACK_WEBHOOK_URL=
EOF
    echo "Created .env — fill in your API keys"
fi

echo "Setup complete"

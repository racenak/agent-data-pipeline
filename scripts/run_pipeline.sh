#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SCRIPT_DIR"

source .venv/bin/activate

python -m src.data_pipeline_agent.main "$@"

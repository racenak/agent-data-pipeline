#!/usr/bin/env bash
set -euo pipefail

python -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt

if [ ! -f .env ]; then
    cp .env.example .env
    echo "Created .env from .env.example — edit it to add your OPENROUTER_API_KEY"
fi

mkdir -p data/raw data/processed data/output
echo "Setup complete"

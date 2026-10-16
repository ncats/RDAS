#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cp "$PROJECT_ROOT/.env.newdb" "$PROJECT_ROOT/.env"

source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate rdas

cd "$SCRIPT_DIR"
python main.py

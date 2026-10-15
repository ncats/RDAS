#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate rdas

python "$PROJECT_ROOT/Z_GARD_Update/2_pipelines/pipeline_1_create_indexes_in_memgraph.py"

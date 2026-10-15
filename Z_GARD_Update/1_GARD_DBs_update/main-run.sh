#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate rdas

cp .env.newdb .env
python main.py

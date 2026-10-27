#!/usr/bin/env bash
set -euo pipefail

CASE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$CASE_ROOT"
CASE_PYTHON="${CASE_PYTHON:-python}"
INPUT="examples/synthetic"
OUTPUT="${1:-data/processed/synthetic_demo}"
mkdir -p "$OUTPUT"

"$CASE_PYTHON" step3_generate_csv_files/1_generate_nodes_csv.py \
  --gard "$INPUT/gard_node_GARD_0006233.json" \
  --grants "$INPUT/grants_GARD_0006233.json" \
  --articles "$INPUT/articles_GARD_0006233.json" \
  --trials "$INPUT/clinical_trials_GARD_0006233.json" \
  --authors "$INPUT/authors_GARD_0006233.json" \
  --output-dir "$OUTPUT"
"$CASE_PYTHON" step3_generate_csv_files/2_generate_relationships.py \
  --gard-json "$INPUT/gard_node_GARD_0006233.json" \
  --articles-json "$INPUT/articles_GARD_0006233.json" \
  --grants-json "$INPUT/grants_GARD_0006233.json" \
  --trials-json "$INPUT/clinical_trials_GARD_0006233.json" \
  --author-assoc-json "$INPUT/author_associate_relations_GARD_0006233.json" \
  --author-collab-json "$INPUT/author_collaborations_GARD_0006233.json" \
  --output-dir "$OUTPUT"
"$CASE_PYTHON" step2_clustering/0_generate_author_information.py \
  --authors "$INPUT/authors_GARD_0006233.json" \
  --associations "$INPUT/author_associate_relations_GARD_0006233.json" \
  --articles "$INPUT/articles_GARD_0006233.json" \
  --trials "$INPUT/clinical_trials_GARD_0006233.json" \
  --grants "$INPUT/grants_GARD_0006233.json" \
  --gard-id GARD:0006233 --output "$OUTPUT/author_information.json"

"$CASE_PYTHON" - "$OUTPUT" <<'PY'
import csv, json, pathlib, sys
out = pathlib.Path(sys.argv[1])
def count(name):
    with (out / name).open(newline='') as handle:
        return sum(1 for _ in csv.DictReader(handle))
assert count('gard_node_GARD_0006233.csv') == 1
assert count('authors_GARD_0006233.csv') == 2
assert count('author_article_relationships.csv') == 2
assert count('author_collaborations_relationships.csv') == 1
assert len(json.loads((out / 'author_information.json').read_text())) == 2
print('Synthetic smoke test passed')
PY

#!/usr/bin/env bash
set -euo pipefail

CASE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$CASE_ROOT"

for name in MYSQL_HOST MYSQL_DATABASE MYSQL_USER MYSQL_PASSWORD EXPERTISE_MODEL_PATH; do
  if [[ -z "${!name:-}" ]]; then
    echo "Missing required environment variable: $name" >&2
    exit 2
  fi
done

CASE_PYTHON="${CASE_PYTHON:-python}"
GARD_ID="${GARD_ID:-GARD:0006233}"
if [[ ! "$GARD_ID" =~ ^GARD:[0-9]+$ ]]; then
  echo "GARD_ID must have the form GARD:0006233" >&2
  exit 2
fi
GARD_SAFE="${GARD_ID//:/_}"
STEP1="data/processed/step1"
STEP2="data/processed/step2"
CSV_DIR="data/processed/csv_files"
mkdir -p "$STEP1" "$STEP2" "$CSV_DIR"

"$CASE_PYTHON" step1_data_extraction/1_extract_gard_node.py \
  --gard_id "$GARD_ID" --output "$STEP1/gard_node_${GARD_SAFE}.json"
"$CASE_PYTHON" step1_data_extraction/2_extract_clinical_trial.py \
  --gard_json "$STEP1/gard_node_${GARD_SAFE}.json" \
  --output "$STEP1/clinical_trials_${GARD_SAFE}.json"
"$CASE_PYTHON" step1_data_extraction/3_extract_grant.py \
  --gard_json "$STEP1/gard_node_${GARD_SAFE}.json" \
  --output "$STEP1/grants_${GARD_SAFE}.json"
"$CASE_PYTHON" step1_data_extraction/4_extract_articles.py \
  --gard_json "$STEP1/gard_node_${GARD_SAFE}.json" \
  --output "$STEP1/articles_${GARD_SAFE}.json"
"$CASE_PYTHON" step1_data_extraction/5_extract_authors.py \
  --articles_json "$STEP1/articles_${GARD_SAFE}.json" \
  --trials_json "$STEP1/clinical_trials_${GARD_SAFE}.json" \
  --grants_json "$STEP1/grants_${GARD_SAFE}.json" \
  --authors_output "$STEP1/authors_${GARD_SAFE}.json" \
  --assoc_rel_output "$STEP1/author_associate_relations_${GARD_SAFE}.json" \
  --collab_output "$STEP1/author_collaborations_${GARD_SAFE}.json"

"$CASE_PYTHON" step2_clustering/0_generate_author_information.py \
  --authors "$STEP1/authors_${GARD_SAFE}.json" \
  --associations "$STEP1/author_associate_relations_${GARD_SAFE}.json" \
  --articles "$STEP1/articles_${GARD_SAFE}.json" \
  --trials "$STEP1/clinical_trials_${GARD_SAFE}.json" \
  --grants "$STEP1/grants_${GARD_SAFE}.json" \
  --output "$STEP2/author_information.json" --gard-id "$GARD_ID"
"$CASE_PYTHON" step2_clustering/1_generate_embeddings.py \
  --input "$STEP2/author_information.json" \
  --output "$STEP2/embeddings" \
  --processes "${EMBEDDING_PROCESSES:-1}" \
  --model "${EMBEDDING_MODEL:-sentence-transformer}" \
  --l2-norm --umap --umap-components 50
"$CASE_PYTHON" step2_clustering/2_generate_clustering.py \
  --input "$STEP2/embeddings/gard_embeddings_summary.csv" \
  --output "$STEP2/clustering"
"$CASE_PYTHON" step2_clustering/3_generate_expertise_node.py \
  --input_dir "$STEP2/clustering" --output_dir "$STEP2/expertise" \
  --step1_file "$STEP2/author_information.json" \
  --model_path "$EXPERTISE_MODEL_PATH" \
  --tensor_parallel_size "${TENSOR_PARALLEL_SIZE:-1}"
cp "$STEP2/expertise/cluster_node.csv" "$CSV_DIR/cluster_node.csv"
cp "$STEP2/expertise/expertise_node.csv" "$CSV_DIR/expertise_node.csv"

"$CASE_PYTHON" step3_generate_csv_files/1_generate_nodes_csv.py \
  --gard "$STEP1/gard_node_${GARD_SAFE}.json" \
  --grants "$STEP1/grants_${GARD_SAFE}.json" \
  --articles "$STEP1/articles_${GARD_SAFE}.json" \
  --trials "$STEP1/clinical_trials_${GARD_SAFE}.json" \
  --authors "$STEP1/authors_${GARD_SAFE}.json" --output-dir "$CSV_DIR"
"$CASE_PYTHON" step3_generate_csv_files/2_generate_relationships.py \
  --gard-json "$STEP1/gard_node_${GARD_SAFE}.json" \
  --articles-json "$STEP1/articles_${GARD_SAFE}.json" \
  --grants-json "$STEP1/grants_${GARD_SAFE}.json" \
  --trials-json "$STEP1/clinical_trials_${GARD_SAFE}.json" \
  --author-assoc-json "$STEP1/author_associate_relations_${GARD_SAFE}.json" \
  --author-collab-json "$STEP1/author_collaborations_${GARD_SAFE}.json" \
  --output-dir "$CSV_DIR"
"$CASE_PYTHON" step3_generate_csv_files/8_generate_keyword_expertise.py \
  --expertise-nodes "$STEP2/expertise/expertise_node.csv" \
  --expertise-relationships "$STEP2/expertise/expertise_relationships.csv" \
  --output-dir "$CSV_DIR"
"$CASE_PYTHON" step3_generate_csv_files/3_generate_clustering_relationships.py \
  --expertise-rel "$STEP2/expertise/expertise_relationships.csv" \
  --expertise-rel-keyword "$CSV_DIR/expertise_relationships_keyword.csv" \
  --output-dir "$CSV_DIR"
if [[ -n "${USCITIES_CSV:-}" ]]; then
  "$CASE_PYTHON" step3_generate_csv_files/5_generate_affiliation_csv.py \
    --authors-csv "$CSV_DIR/authors_${GARD_SAFE}.csv" \
    --uscities-csv "$USCITIES_CSV" --output-dir "$CSV_DIR"
fi
"$CASE_PYTHON" step3_generate_csv_files/4_sanity_check_counts.py \
  --csv-dir "$CSV_DIR" \
  --gard-csv "gard_node_${GARD_SAFE}.csv" \
  --articles-csv "articles_${GARD_SAFE}.csv" \
  --grants-csv "grants_${GARD_SAFE}.csv" \
  --trials-csv "clinical_trials_${GARD_SAFE}.csv" \
  --authors-csv "authors_${GARD_SAFE}.csv"

echo "Pipeline CSV outputs: $CASE_ROOT/$CSV_DIR"

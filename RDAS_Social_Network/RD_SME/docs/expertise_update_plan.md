# Expertise Node Update Plan (keyword-level)

## Goal
Post-process existing step2 outputs to create keyword-level Expertise nodes
deduplicated per-GARD. Generate new author_expertise and cluster_expertise
relationships that point at keyword nodes, while keeping the original files
unchanged for rollback.

## Current procedure (step1-3)
- Step1 data extraction
  - run_1_extract_gard_node.sh -> gard_node_GARD_0006233.json
  - run_2_extract_clinical_trial.sh -> clinical_trials_GARD_0006233.json
  - run_3_extract_grant.sh -> grants_GARD_0006233.json
  - run_4_extract_articles.sh -> articles_GARD_0006233.json
  - run_5_extract_authors.sh -> authors_GARD_0006233.json,
    author_associate_relations_GARD_0006233.json, author_collaborations_GARD_0006233.json
- Step2 clustering + expertise
  - run_0_generate_author_information.sh -> author_information.json
  - run_1_generate_embeddings.sh -> embeddings/gard_embeddings_summary.csv
  - run_2_generate_clustering.sh -> clustering/clustered_*.csv
  - run_3_generate_expertise_node.sh -> expertise/expertise_node.csv,
    expertise/expertise_relationships.csv, expertise/cluster_node.csv
- Step3 CSV generation for graph import
  - run_1_generate_nodes_csv.sh -> csv_files/*_GARD_0006233.csv nodes
  - run_2_generate_relationships.sh -> csv_files/*_relationships.csv
  - run_3_generate_clustering_relationships.sh -> cluster_author,
    cluster_expertise, author_expertise relationships
  - run_4_sanity_check_counts.sh -> counts/consistency checks
  - run_5_generate_affiliation_csv.sh -> affiliation_node.csv

## Proposed data model changes (new files only)
1) expertise_node_keyword.csv (keyword-level nodes; new file)
   - Columns:
     - expertise_id (stable, unique hash)
     - expertise_keyword (original text)
     - expertise_keyword_norm (canonicalized; dedupe key)
     - gard_id (dedupe scope is per-GARD)

2) expertise_relationships_keyword.csv (keyword-level edges; new file)
   - Columns:
     - author_id
     - cluster_id
     - gard_id
     - cluster_label
     - expertise_id
     - expertise_rank (1..3)
     - relationship_type (CLUSTER_EXPERTISE or INDIVIDUAL_EXPERTISE)

3) output relationships for Memgraph import (new files)
   - cluster_expertise_relationships_keyword.csv: cluster_id, expertise_id,
     relationship, expertise_rank (optional)
   - author_expertise_relationships_keyword.csv: author_id, expertise_id,
     relationship, expertise_rank (optional)
   - cluster_author_relationships.csv unchanged (still from original rels)

## Implementation plan
1) Add a new post-processing script (Step2 or Step3 folder)
   - Inputs:
     - `step2_clustering/expertise/expertise_node.csv` (LLM output; per-entity)
     - `step2_clustering/expertise/expertise_relationships.csv` (entity->author/cluster)
   - Parse the `expertise` JSON into ordered keywords (expertise_1..3).
   - Canonicalize keyword (lowercase, trim, collapse whitespace, strip punctuation).
   - Build expertise_id as hash over `gard_id + "|" + expertise_keyword_norm`.
   - Emit new files:
     - `expertise_node_keyword.csv`
     - `expertise_relationships_keyword.csv`
   - Keep originals intact.

2) Update `step3_generate_csv_files/3_generate_clustering_relationships.py`
   - Add new optional input path for `expertise_relationships_keyword.csv`.
   - If provided, write:
     - `cluster_expertise_relationships_keyword.csv`
     - `author_expertise_relationships_keyword.csv`
   - Preserve original outputs when the keyword file is not supplied.

3) Update sanity checks (optional but recommended)
   - Extend `step3_generate_csv_files/4_sanity_check_counts.py` to:
     - validate uniqueness of expertise_id in expertise_node_keyword.csv
     - validate referential integrity between keyword relationship CSVs
       and expertise_node_keyword.csv

4) Regenerate keyword outputs
   - Run the new post-processing script.
   - Run clustering relationship generator with the keyword input.

## Decisions confirmed
- Dedup scope: per-GARD only.
- Canonicalization: normalize case, punctuation, and whitespace.
- expertise_id format: hash.
- Graphing scripts: no changes for now.
- Output files: write new keyword files and keep originals unchanged.

## Record
- Added `step3_generate_csv_files/8_generate_keyword_expertise.py` and `step3_generate_csv_files/run_8_generate_keyword_expertise.sh` to produce keyword-level expertise nodes/relationships.
- Updated `step3_generate_csv_files/3_generate_clustering_relationships.py` and `step3_generate_csv_files/run_3_generate_clustering_relationships.sh` to emit keyword-based relationship CSVs.
- Added `step3_generate_csv_files/9_import_expertise.py` and `step3_generate_csv_files/run_9_import_expertise.sh` to import keyword Expertise nodes and create `RESEARCHER_HAS_EXPERTISE` and `CLUSTER_HAS_EXPERTISE` relationships (with optional `expertise_rank`).

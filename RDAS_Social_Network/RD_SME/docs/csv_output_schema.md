# Step3 CSV outputs: nodes and relationships

This document describes the CSV outputs produced in `step3_generate_csv_files/`.
It focuses on node CSVs, relationship CSVs, and clustering relationship CSVs.
All CSVs are written to the configured `--output-dir` (default: `csv_files/`).

## Node CSVs (1_generate_nodes_csv.py)

The node CSVs are direct conversions of step1 JSON outputs. Columns match
the JSON keys. Any list/dict fields are serialized as JSON strings so each
CSV cell is scalar.

Typical outputs and columns:
- `gard_node_<GARDID>.csv`
  - GardId, GardName, Synonyms, Classification_Level, Disorder_Type
- `grants_<GARDID>.csv`
  - application_id, title, abstract, terms, year, gard_id
- `articles_<GARDID>.csv`
  - pubmed_id, doi, gard_id, title, abstract_Text, affiliation,
    first_publication_date, publication_year, cited_by_count, is_open_access,
    in_EPMC, in_PMC, is_EPI, pub_type
- `clinical_trials_<GARDID>.csv`
  - nctid, briefTitle, officialTitle, briefSummary, intervention
- `authors_<GARDID>.csv`
  - author_id, first_name, last_name, affiliation, orc_id

Notes:
- `Synonyms`, `intervention`, `affiliation`, and `orc_id` are stored as JSON
  strings in the CSV because they are lists in the source JSON.
- Output filenames are derived from the input JSON basename, replacing
  `.json` with `.csv`.

## Relationship CSVs (2_generate_relationships.py)

These relationships connect GARD, authors, and the associated artifacts.
Each relationship CSV includes a `relationship` column with a fixed value.

Outputs and columns:
- `gard_article_relationships.csv`
  - gard_id, pubmed_id, relationship (`GARD_HAS_ARTICLE`)
- `gard_grant_relationships.csv`
  - gard_id, application_id, relationship (`GARD_HAS_GRANT`)
- `gard_trial_relationships.csv`
  - gard_id, nctid, relationship (`GARD_HAS_CLINICAL_TRIAL`)
- `author_article_relationships.csv`
  - author_id, pubmed_id, relationship (`AUTHORED_ARTICLE`)
- `author_grant_relationships.csv`
  - author_id, application_id, relationship (`ASSOCIATED_WITH_GRANT`)
- `author_trial_relationships.csv`
  - author_id, nctid, relationship (`ASSOCIATED_WITH_TRIAL`)
- `author_collaborations_relationships.csv`
  - author_id_1, author_id_2, relationship (`COLLABORATES_WITH`)

## Clustering relationship CSVs (3_generate_clustering_relationships.py)

These files are derived from `step2_clustering/expertise/expertise_relationships.csv`
and represent cluster/expertise membership.

Outputs and columns:
- `cluster_author_relationships.csv`
  - cluster_id, author_id, relationship (`AUTHOR_BELONGS_TO_CLUSTER`)
- `cluster_expertise_relationships.csv`
  - cluster_id, entity_id, relationship (`CLUSTER_HAS_EXPERTISE`)
- `author_expertise_relationships.csv`
  - author_id, entity_id, relationship (`AUTHOR_HAS_EXPERTISE`)

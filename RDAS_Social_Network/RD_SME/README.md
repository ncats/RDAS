# Rare Disease SME Collaborative Network (RD-SME) for Specialist and Research Collaboration Recommendation, a Case Study of Cystic Fibrosis

This repository is the case-study code for RD-SME. It extracts
GARD-linked publications, trials, grants, and researchers; builds researcher
embeddings; identifies groups with HDBSCAN; generates group and individual
expertise; and prepares CSVs for a graph database.

The pipeline treats HDBSCAN label `-1` as an **individual researcher**. Those
researchers do not belong to a cluster; expertise is generated for each one
when source abstracts are available.

## Contents

| Directory | Purpose |
| --- | --- |
| `step1_data_extraction/` | Extract GARD, trial, grant, article, and author JSON from SQL. |
| `step2_clustering/` | Aggregate author text, make embeddings, cluster, and generate expertise. |
| `step2_experiment_clustering/` | Compare HDBSCAN and K-means on a sampled embedding set. |
| `step3_generate_csv_files/` | Create graph node and relationship CSVs and optional graph imports. |
| `graph_for_paper/` | Paper figure scripts for approved input data. |
| `examples/synthetic/` | Invented records for the smoke test; no source records. |
| `docs/` | Data schemas, source access, and reproducibility notes. |

## Run the synthetic smoke test

From this repository's root, use Python 3.10 or later:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-smoke.txt
bash scripts/run_synthetic_demo.sh
```

The demo verifies JSON-to-CSV conversion, graph relationships, and author
background aggregation. It does not contact a database or download a model.

Before committing public files, run `python scripts/check_release.py`. This
checks staged paths and text for obvious private material; it does not replace
the data-rights review described below.

## Run on approved source data

Install the pinned reference dependencies with
`python -m pip install -r requirements-pipeline.txt`.
Expertise generation additionally needs `requirements-llm.txt`, a compatible
GPU environment, and local access to the approved model snapshot. Copy
`.env.example` to `.env`, supply credentials locally, and export the settings
into your shell before running `bash scripts/run_pipeline.sh`. The runner uses
environment variables for credentials and writes outputs under
`data/processed/`, which Git ignores.

```bash
cp .env.example .env
chmod 600 .env
# Edit .env locally, then:
set -a
source .env
set +a
bash scripts/run_pipeline.sh
```

The runner stops before graph import. The included graph import scripts assume
base `Researcher` and `Cluster` nodes have already been loaded. The original
base graph loader is outside this case-study folder and is not included here.
See [reproducibility notes](docs/reproducibility.md) for the stage order,
models, method-comparison command, and figure commands.

## Data and release scope

No row-level research data, researcher embeddings, account-specific job
wrappers, completed paper figures, or third-party map datasets are included.
The aggregate clustering comparison table is retained. Figure scripts
require approved input data. See
[data access](docs/data_access.md) and the
[source schema](docs/data_extraction_schema.md).

This candidate has no license or paper citation metadata yet. The project
owner must select approved terms and final citation details before publication.

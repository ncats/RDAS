# RDAS_RDOMICS

RDAS_RDOMICS is a rare-disease omics data extraction and graph-construction pipeline centered on GEO studies. The project takes disease-driven queries, gathers matching GEO accessions, extracts experiment and sample metadata, normalizes selected fields, and reshapes the results into graph-ready node and relationship tables that can be imported into Neo4j or Memgraph.

The codebase supports two connected workflows. The first is the main RD-OMICS build pipeline, which moves from GEO search through metadata extraction, graph table generation, graph import, and experiment-property normalization. The second is a downstream sample-characteristics harmonization workflow that cleans noisy sample labels, applies rule-based and LLM-assisted grouping, and writes harmonized sample properties back onto graph `Sample` nodes.

At a high level, the repository does the following:

- searches GEO with rare-disease terms and records matched GSE studies
- extracts study, platform, project, publication, and sample metadata from GEO/NCBI pages
- converts extracted tables into graph node files and relationship mapping files
- imports graph-ready exports into Neo4j or Memgraph
- normalizes experiment assay fields for cleaner graph properties
- harmonizes sample-characteristics labels into curated categories and subcategories

## Repository Layout

- `scripts/`: core pipeline scripts plus sample-characteristics harmonization utilities
- `scripts/config/paths.yaml`: tracked default config with repo-relative paths
- `scripts/data/`: small source inputs and templates that the pipeline reads; large generated graph outputs are intentionally not versioned here
- `paper_materials/`: figures and supplementary files for the paper
- `SCHEMA.md`: graph labels, identifiers, and relationship types

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Optional LLM/HPC workflow:

```bash
pip install -r requirements-llm.txt
```

The LLM harmonization scripts under `scripts/sample_characteristics_harmonization/` assume a GPU-capable environment and local access to a vLLM-compatible model.

## Configuration

The default config lives at `scripts/config/paths.yaml` and now uses repo-relative paths. You can point the pipeline at a different config file with `--config` or by setting `RDAS_RDOMICS_CONFIG`.

For the optional LLM steps, set `settings.llm_model_path` in the config or export `RDAS_LLM_MODEL_PATH`.
For NCBI Entrez calls, set `ENTREZ_EMAIL` in your environment instead of editing source files.

Example:

```bash
python -m scripts.main --step3-download-gse-number --step4-extract-to-table
python -m scripts.main --step5-generate-node-mappings --step6-import-to-neo4j
python -m scripts.main --step7-experiment-normalization
python -m scripts.main --step7-1-import-normalized-experiment-properties
```

## Pipeline Order

1. Core extraction and graph-building pipeline: `step1` through `step7.1` in `scripts/`
2. Sample-characteristics harmonization: the scripts under `scripts/sample_characteristics_harmonization/`


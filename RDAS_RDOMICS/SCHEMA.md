# RD-OMICS Graph Schema

This repository builds a graph-oriented export with six node types and six relationship types. The definitions below are inferred from `scripts/step6_import_to_neo4j.py`.

## Node Labels

| Node file | Graph label | Primary identifier |
| --- | --- | --- |
| `publication_node.csv` | `Publication` | `Pubmed_id` |
| `project_node.csv` | `Project` | `Project_id` |
| `experiment_node.csv` | `Experiment` | `Experiment_id` |
| `sample_node.csv` | `Sample` | `Sample_id` |
| `platform_node.csv` | `Platform` | `Platform_id` |
| `condition_node.csv` | `Condition` | `GardId` |

## Relationship Types

The importer derives relationship type names from mapping filenames by uppercasing the basename without `_mapping`.

| Mapping file | Start node | End node | Relationship type |
| --- | --- | --- | --- |
| `sample_platform_mapping.csv` | `Sample` | `Platform` | `SAMPLE_PLATFORM` |
| `condition_project_mapping.csv` | `Condition` | `Project` | `CONDITION_PROJECT` |
| `publication_project_mapping.csv` | `Publication` | `Project` | `PUBLICATION_PROJECT` |
| `project_experiment_mapping.csv` | `Project` | `Experiment` | `PROJECT_EXPERIMENT` |
| `experiment_platform_mapping.csv` | `Experiment` | `Platform` | `EXPERIMENT_PLATFORM` |
| `sample_experiment_mapping.csv` | `Sample` | `Experiment` | `SAMPLE_EXPERIMENT` |

## Pipeline Notes

- `scripts/step5_generate_node_mappings.py` creates the node CSVs and relationship mapping CSVs.
- `scripts/step6_import_to_neo4j.py` converts those CSVs to JSON and imports them into Memgraph/Neo4j.
- `scripts/step7_experiment_normalization.py` adds normalized assay fields to `Experiment`.
- `scripts/sample_characteristics_harmonization/7_import_to_sample_node.py` adds harmonized sample property fields to `Sample`.

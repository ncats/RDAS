# RDAS Memgraph

This repository builds and maintains the RDAS Memgraph database and the recurring
alert pipelines that keep the graph aligned with source/staging data.

The codebase has two main workflows:

- Initial database construction from GARD, clinical trial, publication, grant,
  follow-up, person, and maintenance modules.
- Alert pipeline updates under `Z_Alert/`, including discovery, MySQL staging,
  Memgraph graph updates, email alerts, person regrouping, and wrap-up tasks.


## Project Layout

```text
A_GARD/              GARD disease node and relationship initializers.
B_clinical_trial/    Clinical trial import, enrichment, and graph loaders.
C_publication/       Publication, PubMed, OMIM, PubTator, and article loaders.
D_grant/             Grant, project, patent, annotation, and relationship loaders.
E_followup/          Follow-up update scripts for publication and expertise data.
F_person/            Person extraction, grouping, and graph update scripts.
G_update/            Graph maintenance and data correction scripts.
Z_Alert/             Recurring alert pipeline runners, tasks, email code, and docs.
baseclass/           Shared MySQL and Memgraph connection helper.
utils/               Shared file, logging, HTTP, NLP, batching, and utility code.
dump.py              Memgraph export utility for CYPHERL and JSON dumps.
MODEL_INSTALLATION.md Model and local data setup notes.
requirements.txt     Python package requirements.
```

For the alert pipeline step diagram, see `Z_Alert/pipeline.MD`.

## Requirements

- Python environment with the packages from `requirements.txt`.
- MySQL access for staging/source tables.
- Memgraph access for graph reads and writes.
- A local `.env` file with database credentials and API settings.
- NLP/model assets listed in `MODEL_INSTALLATION.md` when running tasks that use
  spaCy, sciSpaCy, NLTK, Hugging Face, or the local organization-name extractor.

Install Python dependencies:

```bash
pip install -r requirements.txt
```

Install model/data assets:

```bash
cat MODEL_INSTALLATION.md
```

## Environment

The project loads environment variables from `.env`. The `.env` file is ignored
by Git and should not be committed.

Common variables used by the tracked code include:

```text
NCBI_KEY
PUBMED_ESEARCH_API
EURO_PEPMC_SERVICE_URL
OMIM_API_KEY
OMIM_ENTRY_API
UMLS_API_KEY
UMLS_SEARCH_API
UMLS_CUI_API_TEMPLATE
NHS_PREDICT_API
EPI_CLASSIFY_API
EPI_EXTRACT_API
CLINICAL_TRIAL_STUDIES_API
RXNAV_RXCUI_API
RXNAV_ALL_PROPERTIES_API_TEMPLATE
ROR_ORGANIZATIONS_API
HUGGINGFACE_TOKEN
ORG_NAME_EXTRACT_MODEL
ORG_NAME_EXTRACT_BASE_URL
ORG_NAME_EXTRACT_TIMEOUT_SECONDS
ORG_NAME_EXTRACT_MAX_LENGTH
```

## Database Connections

`baseclass/conn.py` centralizes database connections:

- `DBConnection().mysql_conn()` connects to MySQL using `MYSQL_*` variables.
- `DBConnection().memgraph_conn()` connects to Memgraph using `MEMGRAPH_*`
  variables.

Most scripts load `.env` before creating database connections.

## Initial Database Build

The root initializer coordinates the main graph build:

```bash
python init_all_db_nodes.py
```

Before running a production build, inspect the active initializer list in
`init_all_db_nodes.py`. The file may be temporarily narrowed for testing during
development.

Domain-specific initializers are also available:

```bash
python A_GARD/init_GARD_all.py
python B_clinical_trial/init_ClinicalTrail_all.py
python C_publication/init_Publication_all.py
python D_grant/init_Grant_all.py
```

These scripts prompt before running so you can confirm the `.env` and database
state.

## Alert Pipeline

Run the main recurring alert pipeline:

```bash
python Z_Alert/main.py
```

The active high-level order is:

1. Find new clinical trial and publication updates.
2. Run clinical trial MySQL updates.
3. Run publication MySQL updates.
4. Memgraph index initialization step, currently disabled in `main.py`.
5. Run clinical trial graph updates.
6. Run publication graph updates.
7. Run follow-up graph/statistics updates.
8. Send alert emails.
9. Regroup people.
10. Run graph maintenance.
11. Run pipeline wrap-up tasks.

Run the grant alert pipeline:

```bash
python Z_Alert/main_grant.py
```

Run one main alert step directly:

```bash
python Z_Alert/steps_of_main/main-step-1.py
python Z_Alert/steps_of_main/main-step-11.py
```

## Memgraph Dump Utility

`dump.py` exports the Memgraph database. Generated dump files are written under
`memgraph_dumps/`, which is ignored by Git.

Print CLI help:

```bash
python dump.py --help
```

Dump the whole database as CYPHERL:

```bash
python dump.py cypherl
```

Dump the whole database as JSON:

```bash
python dump.py json
```

Dump each node label into a separate JSON file:

```bash
python dump.py labels
```

Dump one node label into a JSON file:

```bash
python dump.py label GARD
```

Use a custom output directory or batch size:

```bash
python dump.py --output-dir /tmp/memgraph_dumps json
python dump.py --batch-size 10000 labels
```

Dump files include the current date in the file name.

## Testing

Run the tracked tests:

```bash
pytest
```

Some tests and maintenance flows depend on a local organization-name extraction
model server. Configure the `ORG_NAME_EXTRACT_*` and `MODEL_START_COMMAND`
variables before running those checks.

## Generated Files

The repository ignores generated and local-only files such as:

- `.env*`
- `*.log`
- `*.json`
- `*.cypher`
- `memgraph_dumps/`
- `**/__pycache__/`
- `**/.pytest_cache/`
- local data folders and screenshots

Keep these files local unless a specific artifact is intentionally unignored and
tracked.

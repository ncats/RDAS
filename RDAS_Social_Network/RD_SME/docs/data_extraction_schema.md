# Step1 data extraction: SQL sources and output schema

This document describes what is extracted from the SQL database in
`step1_data_extraction/` and how each output field is derived or renamed.
It also documents the derived `author_information.json` produced in
`step2_clustering/0_generate_author_information.py`.

All outputs are JSON unless noted otherwise.

## GARD node (1_extract_gard_node.py)

Output file: `gard_node_<GARDID>.json` (single JSON object)

Source table: `gard`

Field mapping:
| SQL table.column | Output field | Notes |
| --- | --- | --- |
| gard.GardID | GardId | Input GARD id (e.g., GARD:0006233) |
| gard.Label | GardName | First non-null Label with at least 3 letters; fallback to first Label |
| gard.Label | Synonyms | Remaining Labels, unique (case-insensitive), order-preserving |
| gard.Classification_Level | Classification_Level | First non-null value across rows |
| gard.Disorder_Type | Disorder_Type | First non-null value across rows |

## Clinical trials (2_extract_clinical_trial.py)

Output file: `clinical_trials_<GARDID>.json` (JSON array)

Source tables:
- `clinical_trail_intervention_drug` (note the table name uses "trail")
- `clinical_trial`

Field mapping:
| SQL table.column | Output field | Notes |
| --- | --- | --- |
| clinical_trail_intervention_drug.gardId | (filter) | Only rows where gardId == target GARD id |
| clinical_trail_intervention_drug.nctid | nctid | Distinct NCTIDs for the GARD id |
| clinical_trail_intervention_drug.intervention | intervention | List of unique interventions per nctid |
| clinical_trial.studies | briefTitle | From JSON: protocolSection.descriptionModule.briefTitle (fallback: identificationModule.briefTitle) |
| clinical_trial.studies | officialTitle | From JSON: protocolSection.descriptionModule.officialTitle (fallback: identificationModule.officialTitle) |
| clinical_trial.studies | briefSummary | From JSON: protocolSection.descriptionModule.briefSummary |

Output record format:
```
{
  "nctid": "...",
  "briefTitle": "...",
  "officialTitle": "...",
  "briefSummary": "...",
  "intervention": ["...", "..."]
}
```

## Grants (3_extract_grant.py)

Output file: `grants_<GARDID>.json` (JSON array)

Source tables:
- `grant_gard_project_relation`
- `grant_project`
- `grant_abstract`

Field mapping:
| SQL table.column | Output field | Notes |
| --- | --- | --- |
| grant_gard_project_relation.gard_id | (filter) | Only rows where gard_id == target GARD id |
| grant_gard_project_relation.application_id | application_id | Distinct application IDs for the GARD id |
| grant_project.PROJECT_TITLE | title | Matched by APPLICATION_ID |
| grant_project.PROJECT_TERMS | terms | Matched by APPLICATION_ID |
| grant_abstract.ABSTRACT_TEXT | abstract | Matched by APPLICATION_ID |
| grant_abstract.YEAR | year | Latest YEAR if multiple rows per APPLICATION_ID |
| (input GARD id) | gard_id | Added to every output record |

Output record format:
```
{
  "application_id": "...",
  "title": "...",
  "abstract": "...",
  "terms": "...",
  "year": 2023,
  "gard_id": "GARD:0006233"
}
```

## Articles (4_extract_articles.py)

Output file: `articles_<GARDID>.json` (JSON array)

Source tables:
- `publication_gard_pubmed`
- `publication_article`

Field mapping:
| SQL table.column | Output field | Notes |
| --- | --- | --- |
| publication_gard_pubmed.gard_id | (filter) | Only rows where gard_id == target GARD id |
| publication_gard_pubmed.pubmed_ids | pubmed_id | Split comma-separated list into distinct pubmed_id values |
| publication_article.pubmed_id | pubmed_id | Matched by pubmed_id |
| publication_article.doi | doi |  |
| publication_article.title | title |  |
| publication_article.abstract_text | abstract_Text | Output uses capital "T" in abstract_Text |
| publication_article.affiliation | affiliation |  |
| publication_article.first_publication_date | first_publication_date |  |
| publication_article.publication_year | publication_year |  |
| publication_article.cited_by_count | cited_by_count |  |
| publication_article.is_open_access | is_open_access |  |
| publication_article.in_EPMC | in_EPMC | Output key preserves capitalization |
| publication_article.in_PMC | in_PMC | Output key preserves capitalization |
| publication_article.is_EPI | is_EPI | Output key preserves capitalization |
| publication_article.pub_type | pub_type |  |
| (input GARD id) | gard_id | Added to every output record |

Output record format:
```
{
  "pubmed_id": "...",
  "doi": "...",
  "gard_id": "GARD:0006233",
  "title": "...",
  "abstract_Text": "...",
  "affiliation": "...",
  "first_publication_date": "...",
  "publication_year": 2021,
  "cited_by_count": 12,
  "is_open_access": 1,
  "in_EPMC": 1,
  "in_PMC": 0,
  "is_EPI": 0,
  "pub_type": "..."
}
```

## Authors and relations (5_extract_authors.py)

Output files:
- `authors_<GARDID>.json` (JSON array)
- `author_associate_relations_<GARDID>.json` (JSON array)
- `author_collaborations_<GARDID>.json` (JSON array)

Source table:
- `person_of_all_sources`

Input associate IDs:
- From `articles_<GARDID>.json`: `pubmed_id`
- From `clinical_trials_<GARDID>.json`: `nctid`
- From `grants_<GARDID>.json`: `application_id`

Field mapping for `authors_<GARDID>.json`:
| SQL table.column | Output field | Notes |
| --- | --- | --- |
| person_of_all_sources.first_name | first_name | Author identity uses (first_name, last_name) after trimming |
| person_of_all_sources.last_name | last_name | Author identity uses (first_name, last_name) after trimming |
| person_of_all_sources.affiliation | affiliation | Stored as list of unique, normalized affiliations per author |
| person_of_all_sources.orc_id | orc_id | Stored as list of unique ORCID values per author |
| (generated) | author_id | Generated as AUTH000001, AUTH000002, ... |

Output record format (authors):
```
{
  "author_id": "AUTH000001",
  "first_name": "...",
  "last_name": "...",
  "affiliation": ["..."],
  "orc_id": ["..."]
}
```

Field mapping for `author_associate_relations_<GARDID>.json`:
| Source | Output field | Notes |
| --- | --- | --- |
| person_of_all_sources.associate_id | associate_id | pubmed_id, nctid, or application_id |
| (generated) | author_id | From the authors output |

Output record format (author-associate relations):
```
{
  "associate_id": "...",
  "author_id": "AUTH000001"
}
```

Field mapping for `author_collaborations_<GARDID>.json`:
| Source | Output field | Notes |
| --- | --- | --- |
| (derived) | author_id_1 | Unique author pairs co-occurring on the same associate_id |
| (derived) | author_id_2 | Unique author pairs co-occurring on the same associate_id |

Output record format (collaborations):
```
{
  "author_id_1": "AUTH000001",
  "author_id_2": "AUTH000045"
}
```

## Author information aggregation (step2_clustering/0_generate_author_information.py)

Output file: `step2_clustering/author_information.json` (JSON array)

This file is derived from step1 outputs and therefore indirectly from SQL
tables listed above. It aggregates each author’s background from their
associated articles, clinical trials, and grants.

Output fields and original SQL sources:
| Output field | Derived from | Original SQL tables |
| --- | --- | --- |
| author_id | `authors_<GARDID>.json` | person_of_all_sources |
| first_name | `authors_<GARDID>.json` | person_of_all_sources.first_name |
| last_name | `authors_<GARDID>.json` | person_of_all_sources.last_name |
| author_name | first_name + " " + last_name | person_of_all_sources |
| full_abstract[].type | Association match | publication_article, clinical_trial, grant_project/grant_abstract |
| full_abstract[].abstract_id | associate_id | person_of_all_sources.associate_id |
| full_abstract[].title | Article: title; Trial: briefTitle; Grant: title | publication_article.title, clinical_trial.studies, grant_project.PROJECT_TITLE |
| full_abstract[].abstract | Article: abstract_Text; Trial: briefSummary; Grant: abstract | publication_article.abstract_text, clinical_trial.studies, grant_abstract.ABSTRACT_TEXT |
| GardId | Input parameter | gard.GardID |

Output record format:
```
{
  "author_id": "AUTH000001",
  "first_name": "...",
  "last_name": "...",
  "author_name": "First Last",
  "full_abstract": [
    {"type": "Article", "abstract_id": "123456", "title": "...", "abstract": "..."},
    {"type": "ClinicalTrial", "abstract_id": "NCT000000", "title": "...", "abstract": "..."},
    {"type": "Grant", "abstract_id": "987654", "title": "...", "abstract": "..."}
  ],
  "GardId": ["GARD:0006233"]
}
```

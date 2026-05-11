# RDAS Alert Pipeline Workflow

This document describes the current alert workflow from the Python code in
`alert/`. It uses `alert/main.py` as the source of truth for execution order and
ignores the older `alert/*.txt` notes.

## Entry Point

Main file:

```text
alert/main.py
```

Primary runner:

```text
AlertPipelineRunner
```

The runner owns the high-level order. Individual task classes extend
`PipelineBase`, open the MySQL and/or Memgraph connections they need, process
their data, and close their own resources.

The current `__main__` block runs all seven top-level steps through
`run_step_with_timing()`, then closes the runner logger in `finally`.

## Runtime Pattern

Most task classes follow this shape:

1. `__init__()` opens MySQL and/or Memgraph through `PipelineBase`.
2. `find_new_data(gard_node)` is used by discovery tasks that process one GARD node.
3. `process_new_data()` is used by import, enrichment, graph, follow-up, person, and wrap-up tasks.
4. `close()` closes the task's database handles and logger handlers.

`AlertPipelineRunner._run_pipeline_task()` logs task start/end timing, calls
`process_new_data()`, and closes the task if it still owns an open handle.

## Important Current Settings

`AlertPipelineRunner.TEST_LAST_UPDATE_DATE` is currently:

```python
date(2026, 5, 1)
```

That value overrides each GARD node's real update date during discovery. Set it
to `None` when the pipeline should use the real GARD update date.

`AlertSender` currently sends user alert emails to a testing address in code:

```python
mail_to = 'tongan.zhao@nih.gov'
```

Review that before production use.

## Logs

Runner log:

```text
logs/alert-AlertPipelineRunner.log
```

Pipeline task logs:

```text
~/rdas-memgraph-alert-logs/alert-<TaskClass>.log
```

`PipelineBase` expands `~` with `os.path.expanduser()` before creating the task
log directory.

## Incremental Flags

Common current-run flags:

```text
is_new = 1
```

The row belongs to the current alert run and should be processed.

```text
is_new = 0
```

The row has already been consumed by the pipeline wrap-up step.

```text
alert_sent = 0 / 1
```

Clinical trial alert rows use `alert_sent` to avoid sending the same trial alert
again.

Common incremental tables:

```text
clinical_trial
clinical_trial_unique
publication_article
publication_omim
publication_gard_omim_mapping
person_of_all_sources
organization_location
```

## Top-Level Execution Order

The current `alert/main.py` `__main__` block runs:

1. `run_find_new_clinical_trial_and_publication_updates()`
2. `run_mysql_database_updates()`
3. `run_memgraph_database_updates()`
4. `run_pipeline_followup_updates()`
5. `send_alert_emails()`
6. `run_regroup_the_person()`
7. `run_pipeline_wrapup()`

## Step 1: Find New Clinical Trial And Publication Updates

Method:

```text
run_find_new_clinical_trial_and_publication_updates()
```

Tasks:

1. `GardNodeNamesTask`
2. `NewClinicalTrialDiscoveryTask`
3. `NewPublicationDiscoveryTask`

Flow:

1. Load GARD nodes and filtered searchable names.
2. Resolve the discovery start date for each GARD node.
3. Search ClinicalTrials.gov for matching updated trials.
4. Search PubMed/Europe PMC for matching new publications.
5. Write current-run rows directly into the active incremental tables.

## Step 2: Run MySQL Database Updates

Method:

```text
run_mysql_database_updates()
```

This method runs clinical trial MySQL tasks first, then publication MySQL tasks.

### Step 2A: Clinical Trial MySQL Updates

Method:

```text
run_clinical_trial_mysql_updates()
```

Tasks, in order:

1. `NewClinicalTrialImportTask`
   - Builds `clinical_trial_unique` rows for new clinical trials.
2. `ClinicalTrialDrugInterventionMappingTask`
   - Extracts interventions and maps drugs to RxNorm.
3. `ClinicalTrialPublicationMappingTask`
   - Extracts clinical trial publication references and NCT/PMID mappings.
4. `ClinicalTrialPmidArticleImportTask`
   - Imports PubMed articles referenced by new clinical trials if missing.
5. `NewClinicalTrialAnnotationTask`
   - Runs biomedical entity annotation for new clinical trials.

### Step 2B: Publication MySQL Updates

Method:

```text
run_publication_mysql_updates()
```

Tasks, in order:

1. `PublicationEpiNhsClassificationTask`
   - Classifies new articles as epidemiology and/or natural history studies.
2. `GardOmimPublicationMappingTask`
   - Builds GARD/OMIM publication mappings for new publication data.
3. `PublicationOminDataRetrievalTask`
   - Retrieves OMIM entry JSON for new OMIM mappings.
4. `NewPublicationPubtatorRetrievalTask`
   - Retrieves and parses PubTator data for new articles.
5. `NewPublicationChemicalSubstanceTask`
   - Extracts chemical substance rows from new publication articles.
6. `PublicationFalsePositiveFilterTask`
   - Marks likely false-positive GARD/publication mappings.
7. `NewOmimPublicationArticleImportTask`
   - Imports PubMed articles referenced by new OMIM entries.

## Step 3: Run Memgraph Database Updates

Method:

```text
run_memgraph_database_updates()
```

This method runs clinical trial graph tasks first, then publication graph tasks.

### Step 3A: Clinical Trial Graph Updates

Method:

```text
run_clinical_trial_graph_updates()
```

Tasks, in order:

1. `NewClinicalTrialGraphTask`
   - Creates new `ClinicalTrial` nodes.
2. `NewClinicalTrialGardRelationshipTask`
   - Creates clinical trial to GARD relationships.
3. `NewClinicalTrialConditionGraphTask`
   - Creates `Condition` nodes and trial/GARD condition links.
4. `NewClinicalTrialInterventionGraphTask`
   - Creates `Intervention` nodes and trial intervention links.
5. `NewClinicalTrialDrugGraphTask`
   - Creates `Drug` nodes and intervention drug links.
6. `NewClinicalTrialParticipantGraphTask`
   - Creates participant nodes for new clinical trials.
7. `NewClinicalTrialPrimaryOutcomeGraphTask`
   - Creates primary outcome nodes and links them to trials.
8. `NewClinicalTrialStudyDesignGraphTask`
   - Creates study design nodes and links them to trials.
9. `NewClinicalTrialIndividualPatientDataGraphTask`
   - Creates individual patient data nodes and links them to trials.
10. `NewClinicalTrialAnnotationGraphTask`
    - Creates annotation nodes from UMLS rows and links them to trials.

### Step 3B: Publication Graph Updates

Method:

```text
run_publication_graph_updates()
```

Tasks, in order:

1. `NewPublicationArticleGraphTask`
   - Creates `Article` nodes from `publication_article.is_new = 1`.
2. `NewPublicationArticleNodeAttrsUpdateTask`
   - Updates Article full-text URLs, issue, and volume from `source_json`.
3. `NewPublicationEpidemiologyGraphTask`
   - Creates `EpidemiologyAnnotation` nodes and Article links.
4. `NewPublicationKeywordGraphTask`
   - Creates `Keyword` nodes and Article keyword links.
5. `NewPublicationJournalGraphTask`
   - Creates `Journal` nodes and `published_in` links.
6. `NewPublicationMeshTermGraphTask`
   - Creates `MeshTerm` nodes and Article MeSH links.
7. `GardPublicationEpiNhsCountUpdateTask`
   - Increments GARD EPI/NHS article counters for new articles.
8. `NewPublicationGardArticleRelationshipTask`
   - Creates GARD to Article `mentioned_in` relationships.
9. `NewPublicationPubtatorGraphTask`
   - Creates PubTator annotation nodes and Article annotation links.
10. `NewPublicationSubstanceGraphTask`
    - Creates Substance nodes and Article substance links.
11. `NewPublicationOmimRefGraphTask`
    - Creates OMIM reference nodes and Article OMIM reference links.

## Step 4: Run Pipeline Follow-Up Updates

Method:

```text
run_pipeline_followup_updates()
```

Tasks, in order:

1. `GardRelationshipCountRefreshTask`
   - Refreshes relationship count properties on GARD nodes.
2. `ArticleGeneReviewFlagUpdateTask`
   - Marks Article nodes that correspond to GeneReviews PMIDs.
3. `OrganizationLocationRorLookupTask`
   - Finds Organization nodes needing ROR/location enrichment and writes
     `organization_location` rows with `is_new = 1`.
4. `OrganizationLocationGraphSyncTask`
   - Applies new organization location rows back into Memgraph.

## Step 5: Send Alert Emails

Method:

```text
send_alert_emails()
```

Task:

```text
AlertSender
```

Flow:

1. Load Firebase authenticated users and Firestore GARD subscriptions.
2. For each subscribed GARD ID, count new clinical trials and publications.
3. Build an email payload containing only subscriptions with new data.
4. Send the user alert email.
5. Send an operational summary email to configured admin recipients.

Primary data sources:

```text
Firebase Auth
Firestore subscriptions
clinical_trial
publication_article
publication_gard_searchterm_pubmed_mapping
```

## Step 6: Regroup The Person

Method:

```text
run_regroup_the_person()
```

Tasks, in order:

1. `NewPublicationPersonTask`
   - Extracts publication authors from `publication_article.is_new = 1`.
2. `NewClinicalTrialPersonTask`
   - Extracts contacts and officials from `clinical_trial_unique.is_new = 1`.
3. `NewGrantPersonTask`
   - Placeholder task; currently logs that it is empty.
4. `NewPersonGroupingTask`
   - Regroups affected person rows by last name and updates `rdas_group_id`.
5. `NewPersonAgentGraphTask`
   - Creates Agent nodes and links them to Article, ClinicalTrial, Project, and
     Organization nodes.
6. `PersonPipelineWrapUpTask`
   - Resets `person_of_all_sources.is_new = 0`.

## Step 7: Pipeline Wrap-Up

Method:

```text
run_pipeline_wrapup()
```

Tasks:

1. `ClinicalTrialPipelineWrapUpTask`
   - Resets clinical trial related `is_new` flags after processing.
2. `PublicationPipelineWrapUpTask`
   - Resets publication related `is_new` flags after processing.

## Dependency Chain

The intended data dependency chain is:

```text
discover new data
  -> enrich MySQL rows
  -> update Memgraph nodes and relationships
  -> refresh derived graph metadata
  -> send alert emails
  -> extract/regroup people
  -> clear is_new flags
```

Wrap-up tasks should run only after every downstream consumer has finished using
the current `is_new = 1` rows.

## Directory Map

```text
alert/main.py
```

High-level runner and step order.

```text
alert/pipelines/pipeline_base.py
```

Base class for alert tasks.

```text
alert/pipelines/pipeline_1_gard/
```

GARD node and name discovery.

```text
alert/pipelines/pipeline_2_clinical_trial/
```

Clinical trial discovery, MySQL enrichment, graph updates, and wrap-up.

```text
alert/pipelines/pipeline_3_publication/
```

Publication discovery, MySQL enrichment, graph updates, and wrap-up.

```text
alert/pipelines/pipeline_4_grant/
```

Reserved grant pipeline folder.

```text
alert/pipelines/pipeline_5_followup/
```

Graph statistics, GeneReviews, and organization/location follow-up updates.

```text
alert/pipelines/pipeline_6_person/
```

Person extraction, grouping, Agent graph sync, and person wrap-up.

```text
alert/firebase/
```

Firebase Auth and Firestore subscription access.

```text
alert/emails/
```

Email template rendering and SMTP delivery.

```text
alert/alert_sender.py
```

Subscription matching and email orchestration.

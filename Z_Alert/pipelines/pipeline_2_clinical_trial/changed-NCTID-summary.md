# Changed-NCTID Update Summary

Generated: 2026-08-17

## Goal

Support this alert-pipeline case:

1. A ClinicalTrials.gov NCTID already exists in RDAS.
2. The fetched study JSON for that NCTID has changed.
3. The pipeline should refresh downstream MySQL and Memgraph data for that NCTID.
4. Existing unchanged related data, especially existing PMIDs and people, should not be duplicated or incorrectly marked as new.
5. Stale graph relationships should be removed only when they are no longer present in the latest NCTID JSON.

## Main Data Rule

`clinical_trial_unique` is the canonical comparison source because it has one row per NCTID.

`clinical_trial` can contain multiple rows for the same NCTID because the same trial can match more than one GARD disease term.

When an existing NCTID changes:

- `clinical_trial.studies` is refreshed for matching rows.
- `clinical_trial_unique.studies` is refreshed for the canonical NCTID row.
- `brief_title` and `brief_summary` are cleared so the next step can recalculate them.
- `is_new = 1` is set so downstream tasks consume the changed NCTID.
- `clinical_trial.alert_sent` is reset to `'0'` so a real content change can be included in alert emails again.

When an existing NCTID is unchanged:

- no downstream staging flags are changed.
- no graph refresh is triggered for that NCTID.

## Changed-NCTID Pipeline Flow

1. `task_clinical_trial_1.py` searches ClinicalTrials.gov and fetches the full study JSON for each matching NCTID.
2. `ClinicalTrialStudyChangeHandler` compares the fetched study JSON with `clinical_trial_unique.studies`.
3. Brand-new NCTIDs are inserted into `clinical_trial`.
4. Changed existing NCTIDs are updated in both `clinical_trial` and `clinical_trial_unique`, then marked `is_new = 1`.
5. Clinical-trial MySQL tasks refresh derived rows only for current changed/new NCTIDs.
6. Clinical-trial graph tasks read `clinical_trial_unique.is_new = 1` and refresh graph nodes/relationships.
7. Person tasks insert only genuinely new people for changed NCTIDs and preserve existing people.
8. Organization maintenance updates source tracking for changed responsible organizations.
9. Wrap-up tasks reset `is_new` flags after all downstream tasks have consumed them.

## Discovery and Study Change Handling

### `Z_Alert/pipelines/pipeline_2_clinical_trial/clinical_trial_study_change_handler.py`

Added `ClinicalTrialStudyChangeHandler`.

It handles:

- loading the stored canonical study JSON from `clinical_trial_unique`.
- normalizing JSON before comparison so whitespace and key order do not create false changes.
- returning a difference list for changed JSON.
- inserting brand-new NCTIDs into `clinical_trial`.
- updating changed existing NCTIDs in both `clinical_trial` and `clinical_trial_unique`.
- setting `is_new = 1` for changed studies.
- resetting `clinical_trial.alert_sent = '0'` for changed studies so alert emails can include them again.

### `Z_Alert/pipelines/pipeline_2_clinical_trial/task_clinical_trial_1.py`

Updated discovery to use `ClinicalTrialStudyChangeHandler`.

Instead of blindly inserting every fetched study, it now:

- fetches each matching NCTID's full study JSON.
- asks the helper whether the NCTID is new, unchanged, or changed.
- logs differences when the stored and fetched JSON do not match.

## Clinical-Trial MySQL Derived Data

### `Z_Alert/pipelines/pipeline_2_clinical_trial/task_clinical_trial_3.py`

Changed RxNorm intervention mapping behavior for changed NCTIDs.

It now:

- resets old current `clinical_trial_intervention_drug.is_new = 1` rows for changed clinical trials.
- keeps old rows as history.
- inserts current RxNorm mappings with `is_new = 1`.

### `Z_Alert/pipelines/pipeline_2_clinical_trial/task_clinical_trial_4.py`

Changed NCTID to PMID mapping behavior.

It now:

- resets old current `clinical_trial_nctid_pmids_mapping.is_new = 1` rows for changed NCTIDs.
- inserts only brand-new NCTID/PMID pairs with `is_new = 1`.
- leaves existing NCTID/PMID pairs unchanged.

### `Z_Alert/pipelines/pipeline_2_clinical_trial/task_clinical_trial_5.py`

Changed clinical-trial PMID article import behavior.

It now:

- imports only PMIDs that are missing from `publication_article`.
- inserts those missing PMIDs as new publication article rows.
- leaves already-existing publication articles unchanged and does not mark them new.

### `Z_Alert/pipelines/pipeline_2_clinical_trial/task_clinical_trial_6.py`

Changed clinical-trial annotation refresh behavior.

It now:

- clears old current `clinical_trial_annotation.is_new = 1` rows for the changed NCTID batch.
- inserts refreshed annotations with `is_new = 1`.
- clears stale current annotation flags even when a changed study no longer has usable description text.

## Clinical-Trial Graph Refresh

### `Z_Alert/pipelines/pipeline_2_clinical_trial/task_clinical_trial_graph_1.py`

Updated ClinicalTrial graph node loading to use `MERGE` by `nctId`.

Changed NCTIDs now refresh the existing ClinicalTrial node instead of trying to create a duplicate node.

### `Z_Alert/pipelines/pipeline_2_clinical_trial/task_clinical_trial_graph_2.py`

No changed-NCTID modification was made here.

Reason: there are no requested changes in the ClinicalTrial to GARD relationship path.

### `Z_Alert/pipelines/pipeline_2_clinical_trial/task_clinical_trial_graph_3.py`

Updated Condition graph refresh.

It now:

- creates or reuses current Condition nodes.
- connects the changed ClinicalTrial to current conditions.
- deletes stale `ClinicalTrial -> Condition` relationships for the changed NCTID when old conditions disappear.

### `Z_Alert/pipelines/pipeline_2_clinical_trial/task_clinical_trial_graph_4.py`

Updated Intervention graph refresh.

It now:

- creates or refreshes current Intervention nodes.
- connects the changed ClinicalTrial to current interventions.
- deletes stale `ClinicalTrial -> Intervention` relationships for the changed NCTID when old interventions disappear.

### `Z_Alert/pipelines/pipeline_2_clinical_trial/task_clinical_trial_graph_5.py`

Updated Drug graph loading.

It now:

- uses only current `clinical_trial_intervention_drug.is_new = 1` mappings.
- refreshes Drug node properties by RxNorm ID.
- avoids graph-loading stale historical RxNorm mappings for changed NCTIDs.

### `Z_Alert/pipelines/pipeline_2_clinical_trial/task_clinical_trial_graph_6.py`

Updated Participant graph refresh.

Changed NCTIDs now update the existing Participant node for that NCTID instead of creating duplicate participant data.

### `Z_Alert/pipelines/pipeline_2_clinical_trial/task_clinical_trial_graph_7.py`

Updated PrimaryOutcome graph refresh.

It now:

- creates or refreshes current PrimaryOutcome nodes.
- connects the changed ClinicalTrial to current outcomes.
- deletes stale `ClinicalTrial -> PrimaryOutcome` relationships when old outcomes disappear.

### `Z_Alert/pipelines/pipeline_2_clinical_trial/task_clinical_trial_graph_8.py`

Updated StudyDesign graph refresh.

Changed NCTIDs now upsert the StudyDesign node by NCTID so the latest design values replace stale values.

### `Z_Alert/pipelines/pipeline_2_clinical_trial/task_clinical_trial_graph_9.py`

Updated IndividualPatientData graph refresh.

It now:

- upserts IPD data by NCTID.
- removes older linked IPD nodes that do not match the latest changed NCTID data.

### `Z_Alert/pipelines/pipeline_2_clinical_trial/task_clinical_trial_graph_10.py`

Updated Annotation graph refresh.

It now:

- loads only current `clinical_trial_annotation.is_new = 1` rows joined to changed `clinical_trial_unique` rows.
- creates or refreshes Annotation nodes.
- deletes stale `ClinicalTrial -> Annotation` relationships for changed NCTIDs.
- clears all old annotation links when the changed study no longer has current annotations.

### `Z_Alert/pipelines/pipeline_2_clinical_trial/task_clinical_trial_graph_11.py`

Updated Organization and Location graph refresh.

It now:

- creates or refreshes the responsible Organization when present in latest study JSON.
- preserves existing ROR enrichment fields on Organization nodes.
- creates all valid current Location nodes from `contactsLocationsModule.locations`.
- supports multiple Location entries for one changed NCTID.
- supports organization-only studies.
- supports location-only studies.
- deletes stale `ClinicalTrial -> Organization` relationships when the responsible organization changes or disappears.
- deletes stale `ClinicalTrial -> Location` relationships when old locations disappear from the latest NCTID JSON.
- clears old organization/location relationships when the changed study now has neither organization nor locations.

## Person Handling

### `Z_Alert/pipelines/pipeline_6_person/task_person_2_clinical_trial.py`

Updated clinical-trial person extraction.

For changed NCTIDs, it now inserts only people that do not already exist in `person_of_all_sources`.

The identity used to decide whether a person already exists is:

- `associate_id`, the NCTID
- `source`, always `ClinicalTrial`
- `first_name`
- `last_name`

Existing people are left unchanged. Only new people are inserted with `is_new = 1`.

### `Z_Alert/pipelines/pipeline_6_person/task_person_5_graph.py`

Updated Agent graph cleanup for changed clinical trials.

Because `task_person_2_clinical_trial.py` intentionally does not re-mark existing people as new, this graph task now:

- reads changed NCTIDs from `clinical_trial_unique.is_new = 1`.
- rebuilds current person identities from the latest `clinical_trial_unique.studies` JSON.
- finds matching `person_of_all_sources` rows even when those rows have `is_new = 0`.
- refreshes current Agent relationships before cleanup.
- removes stale `ClinicalTrial -> Agent` investigator/contact relationships only when those Agent keys are no longer present in the latest study JSON.
- skips stale cleanup if current person identities cannot be matched back to `person_of_all_sources`, preventing accidental relationship deletion.

## Organization Source Tracking

### `Z_Alert/pipelines/pipeline_7_graph_maintenance/task_pipeline_maintenance_3.py`

Updated organization source tracking for changed clinical trials.

It now:

- reads changed rows from `clinical_trial_unique.is_new = 1`.
- extracts the latest responsible organization from `protocolSection.identificationModule.organization.fullName`.
- inserts or updates the current `organization_location_source` row.
- deletes stale `organization_location_source` rows for the same NCTID when the responsible organization changes or disappears.
- skips cleanup for invalid or untrusted study JSON so source mappings are not deleted based on bad input.

## Alerting and Wrap-Up

### `Z_Alert/pipelines/pipeline_2_clinical_trial/clinical_trial_study_change_handler.py`

Changed existing clinical-trial rows now reset:

```sql
alert_sent = '0'
```

This matters because `AlertSender` requires both:

```sql
ct.is_new = 1
COALESCE(ct.alert_sent, '0') = '0'
```

Without resetting `alert_sent`, a changed NCTID could be marked `is_new = 1` but still be skipped by alert email logic.

### `Z_Alert/pipelines/pipeline_2_clinical_trial/task_clinical_trial_pipeline_wrapup.py`

Confirmed final clinical-trial wrap-up resets `is_new` for:

- `clinical_trial`
- `clinical_trial_unique`
- `clinical_trial_intervention_drug`
- `clinical_trial_nctid_pmids_mapping`
- `clinical_trial_annotation`

This keeps changed-NCTID processing retryable until wrap-up, then prevents the same changed rows from replaying forever.

## Test and Validation Work

### `test/clinical_trial_study_change_handler_test.py`

Added a test helper for NCTID:

```text
NCT06487273
```

It prints:

- `is_same=True/False`
- JSON object differences between stored and fetched study data

### Final focused checks

Checked the Location and Person changed-NCTID paths with fake study JSON:

- a changed study with two locations produced two distinct Location chunks.
- organization-only study JSON produced an organization chunk with zero locations.
- location-only study JSON produced location chunks without an organization.
- empty study JSON produced no organization/location chunk, which triggers stale relationship cleanup.
- clinical-trial person extraction produced three people from responsible party, central contact, and overall official.
- person identities from `task_person_2_clinical_trial.py` matched the identities rebuilt by `task_person_5_graph.py` for cleanup.

Also ran:

```bash
conda run -n rdas python -m py_compile \
  Z_Alert/pipelines/pipeline_2_clinical_trial/task_clinical_trial_graph_11.py \
  Z_Alert/pipelines/pipeline_7_graph_maintenance/task_pipeline_maintenance_3.py \
  Z_Alert/pipelines/pipeline_6_person/task_person_2_clinical_trial.py \
  Z_Alert/pipelines/pipeline_6_person/task_person_5_graph.py \
  Z_Alert/pipelines/pipeline_2_clinical_trial/clinical_trial_study_change_handler.py
```

and:

```bash
git diff --check
```

## Key Safeguards

- Unchanged NCTIDs are ignored.
- Changed NCTIDs refresh existing graph nodes instead of creating duplicates.
- Stale relationships are removed only for changed NCTIDs.
- Shared graph nodes, such as Organization, Location, Drug, Condition, and Agent, are not deleted just because one trial changed.
- Existing publication articles are not re-marked as new for changed clinical-trial PMIDs.
- Existing clinical-trial people are not reinserted or re-marked as new.
- Existing Agent links are preserved when the person is still present in the latest changed NCTID JSON.
- `task_clinical_trial_graph_2.py` was intentionally not changed.

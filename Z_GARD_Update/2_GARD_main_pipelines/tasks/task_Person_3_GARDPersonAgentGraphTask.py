"""Create final Agents and reconcile clinical-trial people from current studies.

Invocation: _run_person_grouping_and_graph_updates() calls this after grouping.
The runner clears pending person flags only after graph and final organization
processing both succeed, so interrupted final work remains eligible for retry.

Illustrative grouped person and current trial identities:
    person = {"id": 42, "first_name": "Jane", "last_name": "Smith", "rdas_group_id": "Smith_12"}
    trial = {"nctid": "NCT01234567", "current_people": ["Jane Smith"]}

Pending person pages create/reuse Agents via the inherited payload builders and
MERGE queries. A separate bounded pass over ALL imported trials reconstructs
current investigator/contact keep lists from each latest study JSON; it does not
rely on trial.is_new, which per-node wrap-up already cleared. Matching person
rows are fetched regardless of their flags, allowing earlier completed identities
to remain linked. Graph cleanup begins only after this trial's people were found,
grouped, and written successfully. Invalid JSON or a missing usable group ID
stops before cleanup; a valid empty people list removes stale edges intentionally.
These sample identities do not describe a live clinical trial."""

import json
from collections import defaultdict

from pipelines.pipeline_6_person.task_person_5_graph import NewPersonAgentGraphTask


class GARDPersonAgentGraphTask(NewPersonAgentGraphTask):
    """Build pending Agents, then reconcile all imported trials using bounded reads."""

    __module__ = NewPersonAgentGraphTask.__module__
    TRIAL_BATCH_SIZE = 100

    def process_new_data(self) -> None:

        '''
        Source is_new flags have already been cleared per GARD node. The
        durable person flags select pending Article/Project/Agent work, while
        a separate complete pass over clinical_trial_unique supplies the final
        trial relationship worklist. Each trial's keep lists come only from
        its latest study JSON, including people extracted in earlier runs.

        Every SQL result is limited before buffering. Agent payloads are
        bounded by BATCH_SIZE and relationship keep lists live for one trial,
        so the accumulated build is never buffered as one complete result.
        The shared MERGE statements make retries safe without clearing flags
        here. Exceptions propagate to the runner before final person wrap-up.
        '''
        '''
        Follow the primary-key range for person pages. The existing
        (is_new, last_name) index can otherwise sort the whole pending backlog
        by id again for each page, defeating the purpose of keyset pagination.
        '''
        person_query = f'''
            SELECT id, associate_id, associate_type, source, first_name,
                   last_name, affiliation, orcid, email, rdas_group_id, PI_id, role
            FROM {self.PERSON_TABLE} FORCE INDEX (PRIMARY)
            WHERE is_new = 1 AND id > %s
            ORDER BY id
            LIMIT %s
        '''
        identity_query = self.FETCH_PERSON_BY_CLINICAL_TRIAL_IDENTITY_QUERY + " AND id > %s ORDER BY id LIMIT %s"
        trial_query = '''
            SELECT id, nctid, studies
            FROM clinical_trial_unique
            WHERE id > %s AND nctid IS NOT NULL AND nctid <> ''
            ORDER BY id
            LIMIT %s
        '''
        fetch_cursor = None
        identity_cursor = None
        total_people = 0
        total_trials = 0
        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            identity_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            last_person_id = 0
            while True:
                fetch_cursor.execute(person_query, (last_person_id, self.BATCH_SIZE))
                rows = fetch_cursor.fetchall()
                if not rows:
                    break

                last_person_id = rows[-1]["id"]
                self._validate_grouped_people(rows)
                chunks = self.create_agent_chunks(rows)
                if chunks:
                    self.memgraph.execute(self.BATCH_CREATE, {"chunks": chunks})
                total_people += len(rows)
                self.logger.info(f"Final Agent person rows processed={total_people}; last id={last_person_id}.")

            last_trial_id = 0
            while True:
                fetch_cursor.execute(trial_query, (last_trial_id, self.TRIAL_BATCH_SIZE))
                trials = fetch_cursor.fetchall()
                if not trials:
                    break

                last_trial_id = trials[-1]["id"]
                for trial in trials:
                    nctid = trial["nctid"]
                    raw_study = trial["studies"]
                    '''
                    Missing or malformed JSON is not an empty contact list.
                    Fail before cleanup rather than interpreting unreadable
                    source data as permission to delete every Agent edge.
                    '''
                    study = json.loads(raw_study) if isinstance(raw_study, str) else raw_study
                    if not isinstance(study, dict) or not isinstance(study.get("protocolSection"), dict):
                        raise ValueError(f"Cannot reconcile Agent relationships for {nctid}: invalid study JSON.")

                    identities = dict.fromkeys(self.create_current_clinical_trial_person_identities(nctid, study))
                    current_keys = {"has_investigator": defaultdict(set), "has_contact": defaultdict(set)}
                    for identity in identities:
                        last_identity_id = 0
                        matched = False
                        while True:
                            identity_cursor.execute(identity_query, (*identity, last_identity_id, self.BATCH_SIZE))
                            rows = identity_cursor.fetchall()
                            if not rows:
                                break

                            matched = True
                            last_identity_id = rows[-1]["id"]
                            self._validate_grouped_people(rows)
                            chunks = self.create_agent_chunks(rows)
                            if chunks:
                                self.memgraph.execute(self.BATCH_CREATE, {"chunks": chunks})
                                self.collect_current_clinical_trial_agent_keys(chunks, current_keys)

                        if not matched:
                            raise RuntimeError(f"Cannot reconcile {nctid}: a current person identity has not been extracted.")

                    '''
                    Only this trial is cleaned after all its current people
                    were found and their graph writes succeeded. A valid study
                    with no people intentionally has empty keep lists, removing
                    stale relationships without deleting shared Agent nodes.
                    '''
                    investigators = self.create_clinical_trial_agent_cleanup_chunks({nctid}, current_keys["has_investigator"])
                    contacts = self.create_clinical_trial_agent_cleanup_chunks({nctid}, current_keys["has_contact"])
                    self.memgraph.execute(self.BATCH_DELETE_STALE_CLINICAL_TRIAL_INVESTIGATORS, {"chunks": investigators})
                    self.memgraph.execute(self.BATCH_DELETE_STALE_CLINICAL_TRIAL_CONTACTS, {"chunks": contacts})
                    total_trials += 1

                self.logger.info(f"Final Agent trials reconciled={total_trials}; last id={last_trial_id}.")

            self.logger.info(f"Final Agent processing finished: pending person rows={total_people}; trials={total_trials}.")
        finally:
            if identity_cursor is not None:
                identity_cursor.close()
            if fetch_cursor is not None:
                fetch_cursor.close()
            self.close()


    def _validate_grouped_people(self, rows) -> None:

        '''
        Preserve pending flags if a person usable by the shared Agent builder
        still has no group ID. Names intentionally rejected by that builder
        retain its existing handling; they must not block the entire build.
        '''
        for row in rows:
            if row.get("first_name") and row.get("last_name") and self.normalize_last_name(row["last_name"]) and not row.get("rdas_group_id"):
                raise RuntimeError(f"Person row {row['id']} has no group ID; final person flags must remain pending.")

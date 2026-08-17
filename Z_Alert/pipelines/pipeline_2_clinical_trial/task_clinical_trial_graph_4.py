import os
import sys
import json
from typing import Any, Dict, List

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "../..")),
    os.path.abspath(os.path.join(_dir, "../../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _clean, _make_hash_key

"""
Create or refresh Intervention nodes and ClinicalTrial/Intervention mappings for new or changed clinical trials.
"""
# Reference: B_clinical_trial/initializer/intervention.py


class NewClinicalTrialInterventionGraphTask(PipelineBase):
    """
    Create or refresh Intervention nodes and link them to new or changed
    ClinicalTrial nodes.

    ClinicalTrials.gov stores interventions under armsInterventionsModule. This
    task turns each intervention into a stable graph node and connects it to the
    trial that declared it.
    """

    BATCH_SIZE = 200

    '''
    Intervention nodes are keyed by a hash of name, type, and description. A
    plain SET after MERGE refreshes the node when the same intervention is
    reprocessed. The cleanup removes stale has_intervention relationships for
    each changed NCT ID when the latest study JSON no longer contains a previous
    intervention.
    '''
    BATCH_CREATE = '''
        UNWIND $chunks AS chunk
        MATCH (x: ClinicalTrial {nctId: chunk.nctId})
        MERGE (y: Intervention {_composite_key: chunk._composite_key})
        SET
            y.interventionName = chunk.name,
            y.interventionType = chunk.type,
            y.interventionDescription = chunk.description,
            y._intervention_name_key = chunk._intervention_name_key
        MERGE (x)-[:has_intervention]->(y)

        WITH collect(DISTINCT chunk.nctId) AS nctids
        UNWIND nctids AS current_nctid
        MATCH (x: ClinicalTrial {nctId: current_nctid})
        OPTIONAL MATCH (x)-[old_rel:has_intervention]->(old:Intervention)
        WHERE old_rel IS NOT NULL
        AND (
            old._composite_key IS NULL
            OR NOT old._composite_key IN [
                chunk IN $chunks
                WHERE chunk.nctId = current_nctid
                | chunk._composite_key
            ]
        )
        DELETE old_rel
    '''

    '''
    If a changed study has no current valid interventions, no intervention chunk
    exists for BATCH_CREATE. This query still clears stale intervention
    relationships for those reprocessed NCT IDs.
    '''
    BATCH_DELETE_STALE_FOR_EMPTY_INTERVENTIONS = '''
        UNWIND $nctids AS nctid
        MATCH (x: ClinicalTrial {nctId: nctid})-[old_rel:has_intervention]->(:Intervention)
        DELETE old_rel
    '''

    FETCH_NEW_CLINICAL_QUERY = '''
        SELECT id, nctid, studies
        FROM clinical_trial_unique
        WHERE nctid IS NOT NULL
        AND is_new = 1
    '''

    def __init__(self):

        """Initialize MySQL and Memgraph connections for intervention graph loading."""

        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:

        raise NotImplementedError("NewClinicalTrialInterventionGraphTask does not implement find_new_data().")


    # implement
    def process_new_data(self) -> None:

        """Fetch new trial JSON and write intervention graph mappings in batches."""

        count = 0
        batch_num = 0
        empty_intervention_nctids = []
        fetch_cursor = None

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(self.FETCH_NEW_CLINICAL_QUERY)

            while True:
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    self.logger.info("No more rows to fetch.")
                    break

                batch_num += 1
                self.logger.info(f'--- batch# = {batch_num} ---')

                chunks = []

                for row in rows:
                    nctid = row.get('nctid')
                    if not nctid:
                        continue

                    try:
                        study = json.loads(row.get('studies') or '{}')
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Invalid JSON for nctId {nctid}: {e}")
                        continue

                    '''
                    One clinical trial may produce multiple intervention
                    relationship chunks.
                    '''
                    intervention_chunks = self._create_intervention_chunks(nctid, study)

                    if not intervention_chunks:
                        empty_intervention_nctids.append(nctid)
                        continue

                    chunks.extend(intervention_chunks)

                if chunks:
                    self.memgraph.execute(self.BATCH_CREATE, {"chunks": chunks})

                    count += len(chunks)
                    self.logger.info(f'Upserted {len(chunks)} intervention mappings in memgraph. Total = {count}')
                else:
                    self.logger.info('No valid interventions to upsert into memgraph.')

            if empty_intervention_nctids:
                self.memgraph.execute(self.BATCH_DELETE_STALE_FOR_EMPTY_INTERVENTIONS, {"nctids": empty_intervention_nctids})
                self.logger.info(f'Removed stale intervention mappings for {len(empty_intervention_nctids)} trials with no current valid interventions.')

        except Exception as e:
            self.logger.error(f"Error executing intervention graph task: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def _create_intervention_chunks(self, nctid: str, study: Dict[str, Any]) -> List[Dict[str, str]]:

        """Convert one study payload into Cypher chunks for intervention nodes."""

        chunks = []
        interventions = self._extract_interventions(study)

        for intervention in interventions:
            if not isinstance(intervention, dict):
                continue

            name = _clean(intervention.get('name', ''))
            intervention_type = _clean(intervention.get('type', ''))
            description = _clean(intervention.get('description', ''))

            if not any([name, intervention_type, description]):
                continue

            '''
            The graph uses hashes for stable matching keys while preserving the
            original readable values as properties.
            '''
            composite_key = f'{name}_{intervention_type}_{description}'

            chunks.append({
                "nctId": nctid,
                "name": name,
                "type": intervention_type,
                "description": description,
                "_composite_key": _make_hash_key(composite_key),
                "_intervention_name_key": _make_hash_key(name)
            })

        return chunks


    def _extract_interventions(self, study: Dict[str, Any]) -> List[Dict[str, Any]]:

        """Read intervention records from protocolSection.armsInterventionsModule."""

        if not isinstance(study, dict):
            return []

        protocol = study.get('protocolSection', {})
        if not isinstance(protocol, dict):
            return []

        intervention_module = protocol.get('armsInterventionsModule', {})
        if not isinstance(intervention_module, dict):
            return []

        interventions = intervention_module.get('interventions', [])
        return interventions if isinstance(interventions, list) else []

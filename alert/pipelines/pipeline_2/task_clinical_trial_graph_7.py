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
from utils.tools import _clean


"""
Create PrimaryOutcome nodes and ClinicalTrial/PrimaryOutcome mappings for new clinical trials.
"""
# Reference: B_clinical_trial/initializer/outcome.py


class ClinicalTrialGraphTask_7(PipelineBase):

    BATCH_SIZE = 200

    BATCH_CREATE = '''
        UNWIND $chunks AS chunk
        MATCH (x: ClinicalTrial {nctId: chunk.nctId})
        CREATE (y: PrimaryOutcome)
        SET
            y.primaryOutcomeMeasure = chunk.measure,
            y.primaryOutcomeTimeFrame = chunk.timeFrame,
            y.primaryOutcomeDescription = chunk.description

        MERGE (x)-[:has_outcome]->(y)
    '''

    FETCH_NEW_CLINICAL_QUERY = '''
        SELECT id, nctid, studies
        FROM clinical_trial_unique
        WHERE nctid IS NOT NULL
        AND is_new = 1
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        raise NotImplementedError("ClinicalTrialGraphTask_7 does not implement find_new_data().")


    # implement
    def process_new_data(self) -> None:

        count = 0
        batch_num = 0
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
                    except (json.JSONDecodeError, TypeError) as e:
                        self.logger.error(f"Invalid JSON for nctId {nctid}: {e}")
                        continue

                    chunks.extend(self._create_primary_outcome_chunks(nctid, study))

                if chunks:
                    #self.memgraph.execute(self.BATCH_CREATE, {"chunks": chunks})

                    count += len(chunks)
                    self.logger.info(f'Created {len(chunks)} primary outcome mappings in memgraph. Total = {count}')
                else:
                    self.logger.info('No valid primary outcomes to insert into memgraph.')

        except Exception as e:
            self.logger.error(f"Error executing primary outcome graph task: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def _create_primary_outcome_chunks(self, nctid: str, study: Dict[str, Any]) -> List[Dict[str, str]]:

        chunks = []
        primary_outcomes = self._extract_primary_outcomes(study)

        for outcome in primary_outcomes:
            if not isinstance(outcome, dict):
                continue

            chunks.append({
                "nctId": nctid,
                "measure": _clean(outcome.get('measure', '')),
                "timeFrame": _clean(outcome.get('timeFrame', '')),
                "description": _clean(outcome.get('description', ''))
            })

        return chunks


    def _extract_primary_outcomes(self, study: Dict[str, Any]) -> List[Dict[str, Any]]:

        if not isinstance(study, dict):
            return []

        protocol = study.get('protocolSection', {})
        if not isinstance(protocol, dict):
            return []

        outcomes_module = protocol.get('outcomesModule', {})
        if not isinstance(outcomes_module, dict):
            return []

        primary_outcomes = outcomes_module.get('primaryOutcomes', [])
        return primary_outcomes if isinstance(primary_outcomes, list) else []

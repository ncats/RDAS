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
Create PrimaryOutcome nodes and ClinicalTrial/PrimaryOutcome mappings for new clinical trials.
"""
# Reference: B_clinical_trial/initializer/outcome.py


class NewClinicalTrialPrimaryOutcomeGraphTask(PipelineBase):
    """
    Create PrimaryOutcome nodes for newly imported clinical trials.

    ClinicalTrials.gov stores primary outcomes under outcomesModule. This task
    extracts those records and links each outcome back to its ClinicalTrial.
    """

    BATCH_SIZE = 200

    # PrimaryOutcome nodes are keyed by a hashed composite key so reruns can
    # reuse outcomes without scanning long text fields.
    BATCH_CREATE = '''
        UNWIND $chunks AS chunk
        MATCH (x: ClinicalTrial {nctId: chunk.nctId})
        MERGE (y: PrimaryOutcome {_composite_key: chunk._composite_key})
        ON CREATE SET
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
        """Initialize MySQL and Memgraph connections for outcome graph loading."""

        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        raise NotImplementedError("NewClinicalTrialPrimaryOutcomeGraphTask does not implement find_new_data().")


    # implement
    def process_new_data(self) -> None:
        """Fetch new clinical trial JSON and write primary outcome graph chunks."""

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

                    # One clinical trial may include multiple primary outcomes.
                    chunks.extend(self._create_primary_outcome_chunks(nctid, study))

                if chunks:
                    self.memgraph.execute(self.BATCH_CREATE, {"chunks": chunks})

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
        """Convert primary outcome records into Cypher chunk dictionaries."""

        chunks = []
        primary_outcomes = self._extract_primary_outcomes(study)

        for outcome in primary_outcomes:
            if not isinstance(outcome, dict):
                continue

            measure = _clean(outcome.get('measure', ''))
            time_frame = _clean(outcome.get('timeFrame', ''))
            description = _clean(outcome.get('description', ''))
            composite_key = _make_hash_key(f"{measure}|{time_frame}|{description}")

            chunks.append({
                "nctId": nctid,
                "_composite_key": composite_key,
                "measure": measure,
                "timeFrame": time_frame,
                "description": description
            })

        return chunks


    def _extract_primary_outcomes(self, study: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Read primaryOutcomes from protocolSection.outcomesModule."""

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

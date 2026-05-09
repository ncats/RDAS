import os
import sys
import json
from typing import Any, Dict

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "../..")),
    os.path.abspath(os.path.join(_dir, "../../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _clean

"""
Create Participant nodes and ClinicalTrial/Participant mappings for new clinical trials.
"""
# Reference: B_clinical_trial/initializer/participant.py


class NewClinicalTrialParticipantGraphTask(PipelineBase):
    """
    Create Participant nodes for newly imported clinical trials.

    Participant data comes from the ClinicalTrials.gov eligibility and design
    modules. This task extracts those fields and links each Participant node to
    its ClinicalTrial.
    """

    BATCH_SIZE = 200

    # Participant nodes are keyed per trial so rerunning the alert task reuses
    # the existing participant-info node instead of creating a duplicate.
    BATCH_CREATE = '''
        UNWIND $chunks AS chunk
        MATCH (x: ClinicalTrial {nctId: chunk.nctId})
        MERGE (y: Participant {nctId: chunk.nctId})
        ON CREATE SET
            y.eligibilityCriteria = chunk.eligibilityCriteria,
            y.healthyVolunteers = chunk.healthyVolunteers,
            y.stdAges = chunk.stdAges,
            y.minimumAge = chunk.minimumAge,
            y.maximumAge = chunk.maximumAge,
            y.enrollmentCount = chunk.enrollmentCount,
            y.enrollmentType = chunk.enrollmentType

        MERGE (x)-[:has_participant_info]->(y)
    '''

    FETCH_NEW_CLINICAL_QUERY = '''
        SELECT id, nctid, studies
        FROM clinical_trial_unique
        WHERE nctid IS NOT NULL
        AND is_new = 1
    '''

    def __init__(self):
        """Initialize MySQL and Memgraph connections for participant graph loading."""

        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        raise NotImplementedError("NewClinicalTrialParticipantGraphTask does not implement find_new_data().")


    # implement
    def process_new_data(self) -> None:
        """Fetch new clinical trials and write participant-info graph chunks."""

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

                    # One trial produces at most one participant-info node.
                    participant_chunk = self._create_participant_chunk(nctid, study)
                    if participant_chunk:
                        chunks.append(participant_chunk)

                if chunks:
                    self.memgraph.execute(self.BATCH_CREATE, {"chunks": chunks})

                    count += len(chunks)
                    self.logger.info(f'Created {len(chunks)} participant mappings in memgraph. Total = {count}')
                else:
                    self.logger.info('No valid participants to insert into memgraph.')

        except Exception as e:
            self.logger.error(f"Error executing participant graph task: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def _create_participant_chunk(self, nctid: str, study: Dict[str, Any]) -> Dict[str, Any]:
        """Extract eligibility and enrollment fields from one study payload."""

        if not isinstance(study, dict):
            return {}

        protocol = study.get('protocolSection', {})
        if not isinstance(protocol, dict):
            return {}

        design_module = protocol.get('designModule', {})
        if not isinstance(design_module, dict):
            design_module = {}

        eligibility_module = protocol.get('eligibilityModule', {})
        if not isinstance(eligibility_module, dict):
            eligibility_module = {}

        if not (eligibility_module or design_module):
            return {}

        # Enrollment lives in designModule, while age/volunteer/criteria fields
        # live in eligibilityModule.
        enrollment_info = design_module.get('enrollmentInfo', {})
        if not isinstance(enrollment_info, dict):
            enrollment_info = {}

        std_ages = eligibility_module.get('stdAges', [])
        if not isinstance(std_ages, list):
            std_ages = []

        # The returned keys match the Cypher chunk properties used by BATCH_CREATE.
        return {
            "nctId": nctid,
            "eligibilityCriteria": _clean(eligibility_module.get('eligibilityCriteria', '')),
            "healthyVolunteers": _clean(eligibility_module.get('healthyVolunteers', '')),
            "stdAges": std_ages,
            "minimumAge": _clean(eligibility_module.get('minimumAge', '')),
            "maximumAge": _clean(eligibility_module.get('maximumAge', '')),
            "enrollmentCount": _clean(enrollment_info.get('count', '')),
            "enrollmentType": _clean(enrollment_info.get('type', ''))
        }

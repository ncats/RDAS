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
Create IndividualPatientData nodes and ClinicalTrial/IndividualPatientData mappings for new clinical trials.
"""
# Reference: B_clinical_trial/initializer/patient_data.py


class NewClinicalTrialIndividualPatientDataGraphTask(PipelineBase):
    """
    Create IndividualPatientData nodes for newly imported clinical trials.

    ClinicalTrials.gov stores individual patient data sharing statements in
    ipdSharingStatementModule. This task extracts that statement and links it
    back to the trial.
    """

    BATCH_SIZE = 200

    # IPD sharing nodes are keyed per trial so rerunning the task reuses the
    # existing IndividualPatientData node for that NCT ID.
    BATCH_CREATE = '''
        UNWIND $chunks AS chunk
        MATCH (x: ClinicalTrial {nctId: chunk.nctId})
        MERGE (y:IndividualPatientData {nctId: chunk.nctId})
        ON CREATE SET
            y.ipdSharing = chunk.IPDSharing,
            y.ipdSharingInfoType = chunk.IPDSharingInfoType,
            y.ipdSharingTimeFrame = chunk.IPDSharingTimeFrame,
            y.ipdSharingDescription = chunk.IPDSharingDescription,
            y.ipdSharingAccessCriteria = chunk.IPDSharingAccessCriteria

        MERGE (x)-[:has_individual_patient_data]->(y)
    '''

    FETCH_NEW_CLINICAL_QUERY = '''
        SELECT id, nctid, studies
        FROM clinical_trial_unique
        WHERE nctid IS NOT NULL
        AND is_new = 1
    '''

    def __init__(self):
        """Initialize MySQL and Memgraph connections for IPD graph loading."""

        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        raise NotImplementedError("NewClinicalTrialIndividualPatientDataGraphTask does not implement find_new_data().")


    # implement
    def process_new_data(self) -> None:
        """Fetch new trial JSON and write individual-patient-data graph chunks."""

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

                    # One clinical trial produces at most one IPD sharing node.
                    patient_data_chunk = self._create_patient_data_chunk(nctid, study)
                    if patient_data_chunk:
                        chunks.append(patient_data_chunk)

                if chunks:
                    self.memgraph.execute(self.BATCH_CREATE, {"chunks": chunks})

                    count += len(chunks)
                    self.logger.info(f'Created {len(chunks)} individual patient data mappings in memgraph. Total = {count}')
                else:
                    self.logger.info('No valid individual patient data to insert into memgraph.')

        except Exception as e:
            self.logger.error(f"Error executing individual patient data graph task: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def _create_patient_data_chunk(self, nctid: str, study: Dict[str, Any]) -> Dict[str, Any]:
        """Extract IPD sharing fields from one study payload."""

        if not isinstance(study, dict):
            return {}

        protocol = study.get('protocolSection', {})
        if not isinstance(protocol, dict):
            return {}

        ipd_module = protocol.get('ipdSharingStatementModule', {})
        if not isinstance(ipd_module, dict) or not ipd_module:
            return {}

        info_types = ipd_module.get('infoTypes', [])
        if not isinstance(info_types, list):
            info_types = []

        # The returned keys match the Cypher chunk properties used by BATCH_CREATE.
        return {
            "nctId": nctid,
            "IPDSharing": _clean(ipd_module.get('ipdSharing', '')),
            "IPDSharingDescription": _clean(ipd_module.get('description', '')),
            "IPDSharingInfoType": info_types,
            "IPDSharingTimeFrame": _clean(ipd_module.get('timeFrame', '')),
            "IPDSharingAccessCriteria": _clean(ipd_module.get('accessCriteria', ''))
        }

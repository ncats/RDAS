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
Create StudyDesign nodes and ClinicalTrial/StudyDesign mappings for new clinical trials.
"""
# Reference: B_clinical_trial/initializer/study_design.py


class NewClinicalTrialStudyDesignGraphTask(PipelineBase):
    """
    Create StudyDesign nodes for newly imported clinical trials.

    Study design data is split across ClinicalTrials.gov design, description,
    and status modules. This task gathers those fields into one StudyDesign
    node and links it to the trial.
    """

    BATCH_SIZE = 200

    # The bulk initializer creates one StudyDesign node per trial. Since this
    # node does not have a persisted key property, CREATE must be followed by
    # SET, not ON CREATE SET.
    BATCH_CREATE = '''
        UNWIND $chunks AS chunk
        MATCH (x: ClinicalTrial {nctId: chunk.nctId})
        CREATE (y:StudyDesign)
        SET
            y.designAllocation = chunk.allocation,
            y.designInterventionModel = chunk.interventionModel,
            y.designInterventionModelDescription = chunk.interventionModelDescription,
            y.designMasking = chunk.masking,
            y.designObservationalModel = chunk.observationalModel,
            y.designPrimaryPurpose = chunk.primaryPurpose,
            y.designTimePerspective = chunk.timePerspective,
            y.detailedDescription = chunk.description,
            y.hasExpandedAccess = chunk.hasExpandedAccess,
            y.studyType = chunk.studyType

        MERGE (x)-[:has_study_design]->(y)
    '''

    FETCH_NEW_CLINICAL_QUERY = '''
        SELECT id, nctid, studies
        FROM clinical_trial_unique
        WHERE nctid IS NOT NULL
        AND is_new = 1
    '''

    def __init__(self):
        """Initialize MySQL and Memgraph connections for study-design graph loading."""

        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        raise NotImplementedError("NewClinicalTrialStudyDesignGraphTask does not implement find_new_data().")


    # implement
    def process_new_data(self) -> None:
        """Fetch new trial JSON and write study-design graph chunks."""

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

                    # One clinical trial produces at most one StudyDesign chunk.
                    study_design_chunk = self._create_study_design_chunk(nctid, study)
                    if study_design_chunk:
                        chunks.append(study_design_chunk)

                if chunks:
                    self.memgraph.execute(self.BATCH_CREATE, {"chunks": chunks})

                    count += len(chunks)
                    self.logger.info(f'Created {len(chunks)} study design mappings in memgraph. Total = {count}')
                else:
                    self.logger.info('No valid study designs to insert into memgraph.')

        except Exception as e:
            self.logger.error(f"Error executing study design graph task: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def _create_study_design_chunk(self, nctid: str, study: Dict[str, Any]) -> Dict[str, Any]:
        """Extract design, description, and expanded-access fields from a study."""

        if not isinstance(study, dict):
            return {}

        protocol = study.get('protocolSection', {})
        if not isinstance(protocol, dict):
            return {}

        design_module = protocol.get('designModule', {})
        if not isinstance(design_module, dict):
            design_module = {}

        desc_module = protocol.get('descriptionModule', {})
        if not isinstance(desc_module, dict):
            desc_module = {}

        status_module = protocol.get('statusModule', {})
        if not isinstance(status_module, dict):
            status_module = {}

        design_info = design_module.get('designInfo', {})
        if not isinstance(design_info, dict):
            design_info = {}

        masking_info = design_info.get('maskingInfo', {})
        if not isinstance(masking_info, dict):
            masking_info = {}

        expanded_access_info = status_module.get('expandedAccessInfo', {})
        if not isinstance(expanded_access_info, dict):
            expanded_access_info = {}

        # Skip trials with no design-related details to avoid empty graph nodes.
        if not (design_info or masking_info or expanded_access_info):
            return {}

        # The returned keys match the Cypher chunk properties used by BATCH_CREATE.
        return {
            "nctId": nctid,
            "studyType": design_module.get('studyType', ''),
            "observationalModel": _clean(design_info.get('observationalModel', '')),
            "interventionModel": _clean(design_info.get('interventionModel', '')),
            "interventionModelDescription": _clean(design_info.get('interventionModelDescription', '')),
            "timePerspective": _clean(design_info.get('timePerspective', '')),
            "allocation": _clean(design_info.get('allocation', '')),
            "primaryPurpose": _clean(design_info.get('primaryPurpose', '')),
            "masking": _clean(masking_info.get('masking', '')),
            "description": _clean(desc_module.get('detailedDescription', '')),
            "hasExpandedAccess": _clean(expanded_access_info.get('hasExpandedAccess', ''))
        }

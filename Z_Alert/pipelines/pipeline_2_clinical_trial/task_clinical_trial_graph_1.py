import os
import sys
import json
from typing import Dict, List, Any, Optional

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "../..")),
    os.path.abspath(os.path.join(_dir, "../../..")),
])

from utils.tools import _clean, _safe_get
from pipelines.pipeline_base import PipelineBase

"""
Insert NEW Clinical Trial nodes
"""
# Reference: Z_Alert/pipelines/pipeline_2_clinical_trial/task_clinical_trial_2.py
# Reference: B_clinical_trial/initializer/clinicaltrial.py

class NewClinicalTrialGraphTask(PipelineBase):
    """
    Create ClinicalTrial nodes in Memgraph for newly imported trials.

    The task reads staged clinical_trial_unique rows, converts each stored
    ClinicalTrials.gov study JSON into graph-ready properties, and creates the
    node only when its nctId does not already exist.
    """

    def __init__(self):
        """Initialize both MySQL and Memgraph connections for graph loading."""

        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        raise NotImplementedError("NewClinicalTrialGraphTask does not implement find_new_data().")


    # implement
    def process_new_data(self) -> None:
        """Fetch new clinical trials from MySQL and submit ClinicalTrial nodes in batches."""

        ''' create the node only when nctId does not exist; if it already exists, do nothing. '''
        # MERGE is keyed by nctId. ON CREATE SET intentionally avoids updating
        # existing ClinicalTrial nodes during this incremental graph step.
        batch_create = '''
            UNWIND $chunks AS props
            MERGE (n: ClinicalTrial {nctId: props.nctId})
            ON CREATE SET n = props
        ''' 
        
        # clinical_trial_unique has one row per NCT ID; is_new limits the graph
        # load to records discovered in the current alert run.
        fetch_new_clinical_query = '''
            SELECT id, nctid, studies 
            FROM clinical_trial_unique
            WHERE nctid IS NOT NULL AND is_new = 1
        '''
    
        count = 0
        batch_num = 0
        batch_size = 100

        fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
        fetch_cursor.execute(fetch_new_clinical_query)

        while True:
            rows = fetch_cursor.fetchmany(batch_size)

            if not rows:
                self.logger.info(f"No more rows to fetch.")
                break

            batch_num += 1
            self.logger.info(f'--- batch# = {batch_num} ---')
        

            chunks = []

            for row in rows:
                nctid = row['nctid']
                full_study = json.loads(row['studies'])

                # Convert the full ClinicalTrials.gov payload into the compact
                # property dictionary used by the ClinicalTrial node.
                clinicalTrailObj = self._create_ClinicalTrial_node(nctid, full_study)

                if clinicalTrailObj is None:
                    continue    

                chunks.append(clinicalTrailObj)


            if len(chunks) > 0:
                try:
                    self.memgraph.execute(batch_create, {"chunks": chunks})

                    count += len(chunks)
                    self.logger.info(f'Inserted {len(chunks)} nodes into memgraph. Total = {count}')

                except Exception as e:
                    self.logger.error(f"Error executing batch create: {e}") 
            else:
                self.logger.info('No new nodes to insert into memgraph.')
 
        ''' Explicitly close all db connections. '''
        self.close()



    def _create_ClinicalTrial_node(self, nctid: str, study: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Create ClinicalTrial node properties from one study JSON payload.

        Args:
            nctid: NCT identifier for the clinical trial
            study: Study data dictionary from API response            
        Returns:
            A node containing clinical trial properties or None if essential data is missing
        """
        # Stop early when the NCT ID or study payload is unusable; downstream
        # Cypher expects a valid nctId property for MERGE.
        if not isinstance(nctid, str) or not nctid:
            return None

        if not isinstance(study, dict) or not study:
            return None

        protocol = study.get('protocolSection', {})
        if not isinstance(protocol, dict) or not protocol:
            return None

        # ClinicalTrials.gov modules are optional. These helpers keep nested
        # access consistent and prevent non-dict values from leaking into the node.
        def _module(key: str) -> Dict[str, Any]:
            value = protocol.get(key, {})
            return value if isinstance(value, dict) else {}

        def _clean_get(data: Dict[str, Any], *keys: str) -> str:
            return _clean(_safe_get(data, *keys))

        identification = _module('identificationModule')
        status = _module('statusModule')
        description = _module('descriptionModule')
        design = _module('designModule')
        ipd = _module('ipdSharingStatementModule')
        contact = _module('contactsLocationsModule')

        if not any([status, description, design, ipd, contact]):
            return None

        phases = design.get('phases', [])
        phase_value = ','.join(str(phase) for phase in phases) if isinstance(phases, list) and phases else 'NA'

        node = {
            "nctId": nctid,
            "studyType": _clean_get(design, 'studyType'),
            "briefTitle": _clean_get(identification, 'briefTitle'),
            "briefSummary": _clean_get(description, 'briefSummary'),
            "officialTitle": _clean_get(identification, 'officialTitle'),

            # Completion date information
            "completionDate": _clean_get(status, 'completionDateStruct', 'date'),
            "completionDateType": _clean_get(status, 'completionDateStruct', 'type'),

            # Last known status information
            "lastKnownStatus": _clean_get(status, 'lastKnownStatus'),

            # Last update information
            "lastUpdatePostDate": _clean_get(status, 'lastUpdatePostDateStruct', 'date'),
            "lastUpdatePostDateType": _clean_get(status, 'lastUpdatePostDateStruct', 'type'),
            "lastUpdateSubmitDate": _clean_get(status, 'lastUpdateSubmitDate'),

            # Status and start date
            "overallStatus": _clean_get(status, 'overallStatus'),
            "startDate": _clean_get(status, 'startDateStruct', 'date'),
            "startDateType": _clean_get(status, 'startDateStruct', 'type'),

            # Design information
            "phase": _clean(phase_value),
            "patientRegistry": design.get('patientRegistry') if isinstance(design.get('patientRegistry'), bool) else False,

            # Primary completion date information
            "primaryCompletionDate": _clean_get(status, 'primaryCompletionDateStruct', 'date'),
            "primaryCompletionDateType": _clean_get(status, 'primaryCompletionDateStruct', 'type'),

            # Results posting information
            "resultsFirstPostDate": _clean_get(status, 'resultsFirstPostDateStruct', 'date'),
            "resultsFirstPostDateType": _clean_get(status, 'resultsFirstPostDateStruct', 'type'),
            "resultsFirstPostedQCCommentsDate": _clean_get(status, 'resultsFirstSubmitQcDate'),

            # RDAS metadata
            "lastUpdatedRDAS": self.formatted_today,
            "dateCreatedRDAS": self.formatted_today
        }
        
        return node

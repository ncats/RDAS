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
Insert NEW Clinical Trail nodes
"""
# Reference: alert/pipelines/pipeline_2/task_clinical_trial_2.py
# Reference: B_clinical_trial/initializer/clinicaltrial.py

class ClinicalTrialGraphTask_1(PipelineBase):

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        raise NotImplementedError("ClinicalTrialGraphTask_1 does not implement find_new_data().")


    # implement
    def process_new_data(self) -> None:

        ''' create the node only when nctId does not exist; if it already exists, do nothing. '''
        batch_create = '''
            UNWIND $chunks AS props
            MERGE (n: ClinicalTrial {nctId: props.nctId})
            ON CREATE SET n = props
        ''' 
        
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

                clinicalTrailObj = self._create_ClinicalTrial_node(nctid, full_study)

                if clinicalTrailObj is None:
                    continue    

                chunks.append(clinicalTrailObj)


            if len(chunks) > 0:
                try:
                    #self.memgraph.execute(batch_create, {"chunks": chunks}) 

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
        Create a clinical trial node from study data with comprehensive validation and error handling.
        Args:
            nctid: NCT identifier for the clinical trial
            study: Study data dictionary from API response            
        Returns:
            A node containing clinical trial properties or None if essential data is missing
        """
        if not isinstance(nctid, str) or not nctid:
            return None

        if not isinstance(study, dict) or not study:
            return None

        protocol = study.get('protocolSection', {})
        if not isinstance(protocol, dict) or not protocol:
            return None

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

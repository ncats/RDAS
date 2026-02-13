
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import time
from typing import Dict, List, Any, Optional
from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import _id_range_generator, _curr_timestamp, _date_string, _safe_get, _clean

# Create ClinicalTrail nodes
class ClinicalTrialInitializer(InitBase):

    def __init__(self): 

        super().__init__('clinical_trial_unique', 'ClinicalTrail') 

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/2-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)
         
        self.create_indexes('GARD', ['gardId'])

        # create index
        self.create_indexes('ClinicalTrial', ['nctId'])


    # Override the abstract method
    def init_nodes(self):   

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)


    # Override
    def populate_nodes(self, min_id, max_id, step=1, batch_size = 600):         
       
        # MERGE on nctId: MERGE (n:ClinicalTrial {nctId: props.nctId}) - Finds or creates a ClinicalTrial node with the matching nctId
        # SET n = props: Updates all properties from the props object, whether the node is newly created or already exists
        
        # batch create nodes
        batch_create = '''
            UNWIND $chunks AS props
            MERGE (n: ClinicalTrial {nctId: props.nctId})
            ON CREATE SET n = props
        ''' 
       
        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

        total  = 0
        for start_id, end_id in id_ranges: 

            # 1. Get distinct Clinical Trial nodes (simple)  
            query = f'''
                SELECT id, nctid, studies 
                FROM {self.table_name}
                WHERE nctid IS NOT NULL
                AND (id BETWEEN {start_id} AND {end_id}) AND (processed IS NULL OR processed != '{self.processed_flag}')
            ''' 

            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall() 

            chunks = []
            for row in rows:
                total += 1

                nctid = row['nctid'] 
                full_study = json.loads(row['studies'])

                clinicalTrailObj = self._create_ClinicalTrial_node(nctid, full_study)

                if clinicalTrailObj is None:
                    continue

                chunks.append(clinicalTrailObj)
 

            if chunks:
                try:
                    self.memgraph.execute(batch_create, {"chunks": chunks}) 
                except Exception as e:
                    self.appender.log_stdout(f"Error executing batch create: {e}")
                    raise
            
            self.update_processed_flag(start_id, end_id, self.processed_flag)
            
            self.appender.append_and_print(f'{_curr_timestamp()} [total: {total}], Id range: [{start_id} - {end_id}], #ClinicalTrial = {len(chunks)}')

        self.close_mysql_conn()  

        ''' 
        # Create indexes here
        start_time = time.time()
        self.appender.log_stdout(f'\n{"="*50} {_curr_timestamp()} \'CREATE INDEX ON :ClinicalTrial(nctId)\' after uploaing the data {"="*50}\n')

        self.create_indexes('ClinicalTrial', ['nctId'])

        self.appender.log_stdout(f'\n{"*"*30} {_curr_timestamp()} The index on ClinicalTrial.nctId has been created {"*"*30}\n')
        '''

        # done        
        self.appender.log_stdout(f'\n{_curr_timestamp()} {"="*50} Done! Total = {total} {"="*50}\n\n')
        self.appender.close() 
        
 

    def _create_ClinicalTrial_node(self, nctid: str, study: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Create a clinical trial node from study data with comprehensive validation and error handling.        
        Args:
            nctid: NCT identifier for the clinical trial
            study: Study data dictionary from API response            
        Returns:
            A node containing clinical trial properties or None if essential data is missing
        """
        # Validate input
        if not nctid or not isinstance(nctid, str):
            return None
        
        if not study or not isinstance(study, dict):
            return None
        
        # Extract protocol section
        protocol = study.get('protocolSection', {})
        if not isinstance(protocol, dict):
            return None
        
        # Extract modules with single dictionary lookups
        identification = protocol.get('identificationModule', {})
        status = protocol.get('statusModule', {})
        description = protocol.get('descriptionModule', {})
        design = protocol.get('designModule', {})
        ipd = protocol.get('ipdSharingStatementModule', {})
        contact = protocol.get('contactsLocationsModule', {})
        
        # Validate that at least one module has data
        if not any([status, description, design, ipd, contact]):
            return None
        
        # Process phase information
        phases = design.get('phases', [])
        if isinstance(phases, list) and phases:
            phase_value = ','.join(str(p) for p in phases)
        else:
            phase_value = 'NA'
        
        # Build object with safe navigation
        node = {

            "nctId": nctid,
            "studyType": _clean(_safe_get(design, 'studyType')),
            "briefTitle": _clean(_safe_get(identification, 'briefTitle')),
            "briefSummary": _clean(_safe_get(description, 'briefSummary')),
            "officialTitle": _clean(_safe_get(identification, 'officialTitle')),

             # Completion date information
            "completionDate": _clean(_safe_get(status, 'completionDateStruct', 'date')),
            "completionDateType": _clean(_safe_get(status, 'completionDateStruct', 'type')), 
            
            # Last known status information
            "lastKnownStatus": _clean(_safe_get(status, 'lastKnownStatus')),  

            # Last update information
            "lastUpdatePostDate": _clean(_safe_get(status, 'lastUpdatePostDateStruct', 'date')),
            "lastUpdatePostDateType": _clean(_safe_get(status, 'lastUpdatePostDateStruct', 'type')),
            "lastUpdateSubmitDate": _clean(_safe_get(status, 'lastUpdateSubmitDate')),
            
            # Status and start date
            "overallStatus": _clean(_safe_get(status, 'overallStatus')),
            "startDate": _clean(_safe_get(status, 'startDateStruct', 'date')),
            "startDateType": _clean(_safe_get(status, 'startDateStruct', 'type')),
            
            # Design information
            "phase": _clean(phase_value),
            "patientRegistry": design.get('patientRegistry', False) if isinstance(design.get('patientRegistry'), bool) else False,

            # Primary completion date information
            "primaryCompletionDate": _clean(_safe_get(status, 'primaryCompletionDateStruct', 'date')),
            "primaryCompletionDateType": _clean(_safe_get(status, 'primaryCompletionDateStruct', 'type')),

            # Results posting information
            "resultsFirstPostDate": _clean(_safe_get(status, 'resultsFirstPostDateStruct', 'date')),
            "resultsFirstPostDateType": _clean(_safe_get(status, 'resultsFirstPostDateStruct', 'type')),
            "resultsFirstPostedQCCommentsDate": _clean(_safe_get(status, 'resultsFirstSubmitQcDate')),
            
            # RDAS metadata
            "lastUpdatedRDAS": self.formatted_today,
            "dateCreatedRDAS": self.formatted_today
        }
        
        return node
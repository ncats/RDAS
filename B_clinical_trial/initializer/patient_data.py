import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import  _id_range_generator, _clean, _curr_timestamp, _date_string

# Create IndividualPatientData nodes
class IndividualPatientDataInitializer(InitBase):


    def __init__(self): 

        super().__init__('clinical_trial_unique', 'IndividualPatientData')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/2-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)   


    # Override the abstract method
    def init_nodes(self):   

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)


    # Override
    def populate_nodes(self, min_id, max_id, step=1, batch_size=300):

        batch_create = '''
            UNWIND $chunks AS chunk
            MATCH (x: ClinicalTrial {nctId: chunk.nctId}) 
            CREATE (y:IndividualPatientData)
            SET
                y.ipdSharing = chunk.IPDSharing,
                y.ipdSharingInfoType = chunk.IPDSharingInfoType,
                y.ipdSharingTimeFrame = chunk.IPDSharingTimeFrame,
                y.ipdSharingDescription = chunk.IPDSharingDescription,
                y.ipdSharingAccessCriteria = chunk.IPDSharingAccessCriteria

            MERGE (x)-[:has_individual_patient_data]->(y)
        '''                  
        
        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

        total  = 0
        for start_id, end_id in id_ranges:

            query = f'''
                SELECT id, nctid, studies  
                FROM {self.table_name}
                WHERE nctid IS NOT NULL 
                AND id BETWEEN {start_id} AND {end_id}
            '''

            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()
            '''
                "ipdSharingStatementModule": {
                    "ipdSharing": "YES",
                    "description": "A major focus of this work is sharing and dissemination of the image acquisition and analysis methods we develop as well as standard operating procedures for 129Xe MRI.",
                    "infoTypes": [
                        "STUDY_PROTOCOL",
                        "SAP",
                        "ICF",
                        "CSR",
                        "ANALYTIC_CODE"
                    ],
                    "timeFrame": "In all instances we will adhere to the NIH Sharing Policies and Related Guidance on NIH-Funded Research Resources for Recipients of NIH Grants and Contracts on Obtaining (https://grants.nih.gov/policy/sharing.htm) and Disseminating Biomedical Research Re-sources (issued December 1999). However, we intend to greatly exceed these requirements, making as much of our work freely available to the broader research community either before, or immediately after publication of manuscripts, as well as through PubMedCentral. While we will provide relevant protocols upon request at any time, we further intend to pursue several proactive data sharing mechanisms.",
                    "accessCriteria": "We are committed to making de-identified datasets and image analysis available to qualified investigators. When required scientifically, data including identifiers will be shared under an agreement that provides for: (1) a commitment to using data only for research purposes and not to identify any individual participant; (2) a commitment to securing the data appropriately; and (3) a commitment to destroying or returning the data after analyses are completed."
                }
            '''
            chunks = [] 

            for row in rows:
                total += 1
                nctid = row['nctid'] 
                study = json.loads(row['studies'])
  
                ipd_module = study.get('protocolSection', {}).get('ipdSharingStatementModule', {})

                if not ipd_module:
                    continue

                chunks.append(
                    {   
                        "nctId": nctid,
                        "IPDSharing": _clean(ipd_module.get('ipdSharing', '')),
                        "IPDSharingDescription": _clean(ipd_module.get('description', '')),
                        "IPDSharingInfoType": ipd_module.get('infoTypes', []),
                        "IPDSharingTimeFrame": _clean(ipd_module.get('timeFrame', '')),
                        "IPDSharingAccessCriteria": _clean(ipd_module.get('accessCriteria', ''))
                    }
                )
                
            if chunks:               
                try:
                    self.memgraph.execute(batch_create, {"chunks": chunks}) 
                except  Exception as e:
                    self.appender.append_and_print(f"Error executing batch create: {e}")
                    raise

            self.update_processed_flag(start_id, end_id, self.processed_flag)

            self.appender.log_stdout(f'{_curr_timestamp()} [total: {total}], Id range: [{start_id} - {end_id}], #IndividualPatientData = {len(chunks)}')


        self.close_mysql_conn()   
        
        self.appender.log_stdout(f'\n{_curr_timestamp()} {"="*50} Done! Total = {total} {"="*50}\n\n')
        self.appender.close()
            
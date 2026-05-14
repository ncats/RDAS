import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import  _id_range_generator, _clean, _curr_timestamp, _date_string, _make_hash_key
 
# Create Organization nodes 
#
# Also see: rdas-memgraph/B_clinical_trial/initializer/organization_location.py
#
''' The Organization node has relationship with CoreProject '''
class FundingIcInitializer(InitBase): 


    def __init__(self): 

        super().__init__('grant_gard_project_relation_unique_application_id', 'Organization')
        
        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/2-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)

        self.create_indexes('Organization', ['name', '_idx_key'])   
     
        
    # Override the abstract method
    def init_nodes(self):    

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)


    # Overwrite
    def populate_nodes(self, min_id, max_id, step=1, batch_size=300):

        batch_create = '''
            UNWIND $chunks AS chunk
            MATCH (cp: CoreProject {coreProjectNumber: chunk.coreProjectNumber}) 

            MERGE (org: Organization {_idx_key: chunk._idx_key})
            ON CREATE SET 
                org.name = chunk.name,
                org.displayName = '',
                org.ror_id = '',
                org.website = '',
                org.types = []

            MERGE (cp)-[:has_funding_organization]->(org)            
        '''


        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

        total  = 0
        for start_id, end_id in id_ranges:

            query = f'''
                SELECT  p.core_project_num, p.application_id, p.full_project_num, p.IC_NAME
                FROM  {self.table_name} gpr  

                LEFT JOIN grant_project p
                ON gpr.application_id=p.application_id 

                WHERE (gpr.id BETWEEN {start_id} AND {end_id}) 
                AND (gpr.processed IS NULL OR gpr.processed != '{self.processed_flag}')
            '''

            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()

            chunks = [] 
            for row in rows:

                coreProjectNumber = row['core_project_num'] or row['full_project_num']

                if not coreProjectNumber:
                    continue

                ic_name = row['IC_NAME']
                if not ic_name:
                    continue

                _idx_key = _make_hash_key(ic_name)
                
                chunks.append({   
                    "coreProjectNumber": coreProjectNumber,
                    "name": ic_name,
                    "_idx_key": _idx_key
                })
        
            if chunks:             
                try:
                    self.memgraph.execute(batch_create, {"chunks": chunks}) 
                    total += len(chunks)

                except Exception as e:
                    self.appender.log_stdout(f"Error executing batch create: {e}")
                    raise

            self.update_processed_flag(start_id, end_id, self.processed_flag)

            self.appender.log_stdout(f'{_curr_timestamp()} [total: {total}], Id range: [{start_id} - {end_id}], #Organizations = {len(chunks)}')


        self.close_mysql_conn()   

        self.appender.log_stdout(f'\n{_curr_timestamp()} {"="*50} Done! Total = {total} {"="*50}\n\n')
        self.appender.close()




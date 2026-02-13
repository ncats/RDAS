import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import _id_range_generator, _curr_timestamp, _date_string

"""
    This initializer can be run only after ClinicalTrail nodes are ready.
"""

class CoreProjectClinicalTrialRelationInitializer(InitBase):
    

    def __init__(self): 

        super().__init__('grant_clinical_study', 'Core Project - Clinical Trial Relation')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/4-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)
  
        self.create_indexes('ClinicalTrial', ['nctId'])
        self.create_indexes('CoreProject', ['coreProjectNumber']) 


 
    # Override the abstract method
    def init_nodes(self):   

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)


    def populate_nodes(self, min_id, max_id, step=1, batch_size = 200):         

        # Create CoreProject to ClinicalTrail relation
        batch_create = '''
            UNWIND $chunks AS chunk 
            MATCH(ct: ClinicalTrial {nctId: chunk.nctId})
            MATCH(cp:CoreProject {coreProjectNumber: chunk.coreProjectNumber})
            MERGE (cp)-[:studied]->(ct)
        '''  

        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)
        
        total = 0
        for start_id, end_id in id_ranges:

            query = f'''
                SELECT  gcs.core_project_num, gcs.nctid
                FROM {self.table_name} gcs
                WHERE (gcs.id BETWEEN {start_id} AND {end_id}) 
                AND (gcs.processed IS NULL OR gcs.processed != '{self.processed_flag}')
            '''
            
            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()
 
            chunks = []
            for row in rows:

                total += 1

                ''' Both core_project_num and nctid are not null '''
                #row = _set_value_for_none(row)
                
                chunks.append({          
                    "nctId": row['nctid'],   
                    "coreProjectNumber": row['core_project_num']
                })

            try:
                self.memgraph.execute(batch_create, {"chunks": chunks}) 
            except Exception as e:  
                self.appender.log_stdout(f'Exception while insert: {e}')


            self.update_processed_flag(start_id, end_id, self.processed_flag)

            self.appender.log_stdout(f'{_curr_timestamp()}\t[flag={self.processed_flag}], [total: {total}], [Id range: [{start_id} - {end_id}], #relations = {len(chunks)}')

        self.close_mysql_conn 

        self.appender.append_and_print(f'{_curr_timestamp()} {"="*50} Done Total = {total} {"="*50}\n\n')
        self.appender.close()

        

            
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import _id_range_generator, _val, _curr_timestamp, _date_string, _set_value_for_none

'''

    Deprecated, see core_project_clinical_trail_relation.py
    
'''

# 1. Create ClinicalStudy nodes
class ClinicalStudyInitializer(InitBase):


    def __init__(self): 

        super().__init__('grant_gard_project_relation', 'ClinicalStudy')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/4-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)
 
        self.create_indexes('ClinicalStudy', ['nctId']) 
        self.create_indexes('CoreProject', ['coreProjectNumber']) 



     # Override the abstract method
    def init_nodes(self):   

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)


    def populate_nodes(self, min_id, max_id, step=1, batch_size = 200):         

        # Create ClinicalStudy nodes and map to CoreProject
        batch_create = '''
            UNWIND $chunks AS chunk
            MERGE (cs:ClinicalStudy {nctId: chunk.clinicalStudy.nctId}) 
            ON CREATE SET 
                cs.nctId = chunk.clinicalStudy.nctId,
                cs.status = chunk.clinicalStudy.status,
                cs.title = chunk.clinicalStudy.title

            WITH cs, chunk
            MATCH(cp:CoreProject) WHERE cp.coreProjectNumber = chunk.coreProjectNumber
            MERGE (cp)-[:has_clinical_trial]->(cs)
        '''  

        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)
        
        total = 0
        for start_id, end_id in id_ranges:

            # The gpr.core_project_num value may be empty string '', See: init_9_GARD_and_Project_relationship.py
            query = f'''
                SELECT
                    gpr.id, gpr.gard_id, gpr.application_id,
                    cs.core_project_num, cs.nctid, cs.study_status, cs.study
                FROM {self.table_name} gpr     

                JOIN grant_clinical_study cs
                ON gpr.core_project_num=cs.core_project_num

                WHERE (gpr.id BETWEEN {start_id} AND {end_id}) 
                AND (gpr.core_project_num IS NOT NULL AND LENGTH(TRIM(gpr.core_project_num)) > 0 )
                AND (gpr.processed IS NULL OR gpr.processed != '{self.processed_flag}')
            '''
            
            self.dict_cursor.execute(query)

            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()
 
            chunks = []
            for row in rows:

                total += 1
                row = _set_value_for_none(row)
                
                chunks.append({
                    #"applicationId": row['application_id'],                   
                    "coreProjectNumber": row['core_project_num'], 

                    "clinicalStudy":{
                        "nctId": row['nctid'],
                        "status": row['study_status'],
                        "title": row['study']
                    }
                })

            try:
                self.memgraph.execute(batch_create, {"chunks": chunks}) 
            except Exception as e:  
                self.appender.log_stdout(f'Exception while insert: {e}')


            self.update_processed_flag(start_id, end_id, self.processed_flag)

            self.appender.log_stdout(f'{_curr_timestamp()} [total: {total}], [Id range: [{start_id} - {end_id}], #ClinicalStudy = {len(chunks)}')

        self.close_mysql_conn 

        self.appender.append_and_print(f'{_curr_timestamp()} {"="*50} Done Total = {total} {"="*50}\n\n')
        self.appender.close()

        

            

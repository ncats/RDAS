import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import _id_range_generator, _val, _curr_timestamp, _date_string, _arr

'''
    There is also a rdas-memgraph/2_clinical_trial/initializer/annotation.py
'''
# 1. Create Annotation nodes
class GrantAnnotationInitializer(InitBase):

    def __init__(self): 

        super().__init__('grant_project_annotation', 'Annotation')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/4-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)

        # Create indexes
        self.create_indexes('Annotation', ['umlsCui']) 
        self.create_indexes('Project', ['applicationId']) 

        
    # Override the abstract method
    def init_nodes(self):   

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)

  
    # Override
    def populate_nodes(self, min_id, max_id, step=1, batch_size = 300):         

        # CREATE and MERGE
        batch_create = '''
            UNWIND $chunks AS chunk
            
            MERGE (a: Annotation {umlsCui: chunk.umlsCui}) 
            ON CREATE SET
                a.umlsCui = chunk.umlsCui,
                a.umlsConcept = chunk.umlsConcept,
                a.semanticTypes = chunk.semanticTypes,
                a.semanticTypesNames = chunk.semanticTypesNames

            WITH a, chunk
            MATCH (p: Project {applicationId: chunk.applicationId})
            MERGE (p)-[:has_annotation]->(a)                
        '''  

        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)
        
        total = 0
        for start_id, end_id in id_ranges:
           
            query = f'''
                SELECT   
                    pa.application_id, pa.umls_cui, pa.umls_concept,  pa.semantic_types, pa.semantic_type_names
                FROM  {self.table_name} pa 
                WHERE 
                    (pa.id BETWEEN {start_id} AND {end_id}) 
                AND (pa.processed IS NULL OR pa.processed != '{self.processed_flag}')
            '''

            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()
 
            chunks = []
            for row in rows:
                total += 1 

                chunks.append({
                    "applicationId": row['application_id'],
                    "umlsCui": _val(row['umls_cui']),
                    "umlsConcept": _val(row['umls_concept']),
                    "semanticTypes": _arr(row['semantic_types']),
                    "semanticTypesNames": _arr(row['semantic_type_names'])                    
                })

            try:
                self.memgraph.execute(batch_create, {"chunks": chunks}) 
            except Exception as e:  
                self.appender.log_stdout(f'Exception while insert: {e}')

            self.update_processed_flag(start_id, end_id, self.processed_flag) 

            self.appender.log_stdout(f'{_curr_timestamp()}\t[flag={self.processed_flag}], [total: {total}], [Id range: [{start_id} - {end_id}], #Annotation = {len(chunks)}')

        self.close_mysql_conn()   
        
        self.appender.append_and_print(f'{_curr_timestamp()} {"="*50} Done! Total = {total} {"="*50}\n\n')
        self.appender.close()





            

            

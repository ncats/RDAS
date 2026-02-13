import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import  _id_range_generator, _val, _arr, _curr_timestamp, _date_string


'''
    There is also a rdas-memgraph/4_grant/initializer/annotation.py
'''
# 1. Create clinical trial Annotation
class ClinicalTrialAnnotationInitializer(InitBase):


    def __init__(self): 

        super().__init__('clinical_trial_annotation', 'Annotation')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/3-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)

        #
        self.create_indexes('Annotation', ['umlsCui']) 
  
  
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
                a.umlsConcept = chunk.umlsConcept,
                a.semanticTypes = chunk.semanticTypes,
                a.semanticTypeNames = chunk.semanticTypeNames

            WITH a, chunk
            MATCH (ct: ClinicalTrial {nctId: chunk.nctId})
            MERGE (ct)-[:annotated]->(a)                
        '''  

        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)
        
        total = 0
        for start_id, end_id in id_ranges:
           
            query = f'''
                SELECT   
                    cta.nctid, cta.umls_cui, cta.umls_concept,  cta.semantic_types, cta.semantic_type_names
                FROM  {self.table_name} cta 
                WHERE 
                    (cta.id BETWEEN {start_id} AND {end_id}) 
                AND (cta.processed IS NULL OR cta.processed != '{self.processed_flag}')
            '''

            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()
 
            chunks = []

            for row in rows:
                total += 1

                nctid = row['nctid']
                umls_cui = _val(row['umls_cui'])
                umls_concept = _val(row['umls_concept'])
                semantic_types = _arr(row['semantic_types'])
                semantic_type_names = _arr(row['semantic_type_names'])
                 

                chunks.append({
                    "nctId": nctid,
                    "umlsCui": umls_cui,
                    "umlsConcept": umls_concept,
                    "semanticTypes": semantic_types,
                    "semanticTypeNames": semantic_type_names                    
                })

            if chunks:
                try:
                    self.memgraph.execute(batch_create, {"chunks": chunks}) 
                except Exception as e:  
                    self.appender.log_stdout(f'Exception while insert: {e}')

            self.update_processed_flag(start_id, end_id, self.processed_flag) 

            self.appender.log_stdout(f'{_curr_timestamp()}\t[flag={self.processed_flag}], [total: {total}], [Id range: [{start_id} - {end_id}], #Annotation = {len(chunks)}')
 

        self.close_mysql_conn()    
        
        self.appender.log_stdout(f'\n{_curr_timestamp()} {"="*50} Done! Total = {total} {"="*50}\n\n')
        self.appender.close()





            

            
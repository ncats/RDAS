import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
  
from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import _id_range_generator, _val, _curr_timestamp, _date_string

# Criteria
'''
    grant_publication links to grant_linktable via pmid.
    grant_linktable links to grant_gard_project_relation via project_number and core_project_num.
    grant_gard_project_relation links to grant_gard_project_relation_unique_application_id via application_id.
'''
# Clean the data by
'''
UPDATE rdas_db.grant_gard_project_relation SET core_project_num = NULL WHERE TRIM(core_project_num) = '';
'''
 
# 1. Create Grant CoreProject -> Article relationships
''' !!! Run this initializer after creating all the Article nodes !!! '''
class CoreProjectToArticleRelationInitializer(InitBase):

    def __init__(self): 
        
        super().__init__('grant_gard_project_relation_unique_core_project_num', 'CoreProject -> Article relationshp')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/4-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)
 
        self.create_indexes('CoreProject', ['coreProjectNumber'])  
        self.create_indexes('Article', ['pubmedId']) 


    # Override the abstract method  
    def init_nodes(self):   

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)


    # Override  
    def populate_nodes(self, min_id, max_id, step=1, batch_size = 50):         

        batch_create = '''
            UNWIND $chunks AS chunk

            // Try to find an existing Article node
            MATCH (p:Article {pubmedId: chunk.pubmedId})

            // Always ensure the CoreProject node exists (The MERGE will create it if it doesn't exist)
            MERGE (cp:CoreProject {coreProjectNumber: chunk.coreProjectNumber})

            // Only create relationship if Article exists
            MERGE (cp)-[:published]->(p)
            '''

         
        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)
        
        total = 0
        for start_id, end_id in id_ranges: 

            query = f'''
                SELECT DISTINCT gl.pmid AS PMID, gl.project_number AS CORE_PROJECT_NUM

                FROM {self.table_name} ucpn 

                LEFT JOIN rdas_db.grant_linktable gl 
                    ON gl.project_number=ucpn.core_project_num
                
                WHERE gl.pmid IS NOT NULL 
                AND (ucpn.id BETWEEN {start_id} AND {end_id})
                AND (ucpn.processed is null OR ucpn.processed != \'{self.processed_flag}\') 
            '''
            #print(query) 

            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()
 
            chunks = []
            for row in rows:
                total += 1     

                chunks.append({
                    "pubmedId": row['PMID'],
                    "coreProjectNumber": row['CORE_PROJECT_NUM']
                })
                
            try:
                self.memgraph.execute(batch_create, {"chunks": chunks}) 
            except Exception as e:  
                self.appender.log_stdout(f'Exception while insert: {e}')

            self.update_processed_flag(start_id, end_id, self.processed_flag)
            
            self.appender.log_stdout(f'{_curr_timestamp()}\t[flag={self.processed_flag}], [total: {total}], [Id range: [{start_id} - {end_id}], #CoreProject -> Article = {len(chunks)}')

        self.close_mysql_conn()   

        self.appender.append_and_print(f'{_curr_timestamp()} {"="*50} Done! Total = {total} {"="*50}\n\n')
        self.appender.close()


 
            
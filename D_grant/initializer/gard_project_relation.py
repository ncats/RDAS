import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
 
from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import _id_range_generator, _curr_timestamp, _date_string, _set_value_for_none


# 1. Create GARD and Project relationship
class GardProjectReleationInitializer(InitBase):


    def __init__(self): 

        super().__init__('grant_gard_project_relation', 'GARD-Project relationship')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/4-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)
 
        # Create indexes if indexes are not exist
        self.create_indexes('GARD', ['gardId']) 
        self.create_indexes('Project', ['applicationId']) 
       


    # Override the abstract method
    def init_nodes(self):   

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)


    # Override
    def populate_nodes(self, min_id, max_id, step=1, batch_size = 200):     

        batch_merge = '''
            UNWIND $chunks AS chunk 
            MATCH(p:Project) WHERE p.applicationId = chunk.applicationId
            MATCH (g:GARD) WHERE g.gardId = chunk.gardId 
            MERGE (g)-[:researched_by {confidenceScore: chunk.confidenceScore, semanticSimilarity: chunk.semanticSimilarity, sourceType: chunk.sourceType}]->(p) 
        '''      

        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)
        
        total = 0
        for start_id, end_id in id_ranges:

            query = f'''
                SELECT  
                id, gard_id, application_id, gard_name, source_type, confidence_score, semantic_similarity 
                FROM  {self.table_name}  
                WHERE (id BETWEEN {start_id} AND {end_id}) AND (processed IS NULL OR processed != '{self.processed_flag}')
            '''

            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()

            chunks = []
            for row in rows:

                total += 1 
                row = _set_value_for_none(row)
                
                chunks.append({
                    "gardId":  row['gard_id'],
                    "applicationId":  row['application_id'],
                    "gard_name":  row['gard_name'],
                    "sourceType":  row['source_type'],
                    "confidenceScore":  str(row['confidence_score']),
                    "semanticSimilarity":  str(row['semantic_similarity'])
                })
             

            if len(chunks) > 0:
                try:
                    self.memgraph.execute(batch_merge, {"chunks": chunks}) 
                except Exception as e:  
                    self.appender.log_stdout(f'Exception while insert: {e}')
            else:
               self.appender.log_stdout(f'{start_id} - {end_id} has no rows')  


            self.update_processed_flag(start_id, end_id, self.processed_flag)

            self.appender.log_stdout(f'{_curr_timestamp()}\t[flag={self.processed_flag}], [total: {total}], [Id range: [{start_id} - {end_id}], #GARD-Project relationships = {len(chunks)}')

        self.close_mysql_conn() 
         
        self.appender.log_stdout(f'\n{"="*50} {_curr_timestamp()} Done Total = {total} {"="*50}\n\n')
        self.appender.close()
        


            


            




            
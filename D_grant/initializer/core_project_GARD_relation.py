import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
  
from baseclass.init_base import InitBase 
from utils.file_appender import FileAppender
from utils.tools import _date_string

''' Create Disease-[has_mention_under]->CoreProject relationship '''
class CoreProjectToGARDRelationInitializer(InitBase):

    def __init__(self): 
        
        super().__init__('grant_gard_project_relation', 'CoreProject -> GARD relationshp')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/4-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)


    # Override the abstract method
    def init_nodes(self):   
        self.populate_nodes(None, None)


    # Override  
    def populate_nodes(self, min_id, max_id, step=1, batch_size = 200):         

         # Create GARD to CoreProject relation
        batch_create = '''
            UNWIND $chunks AS chunk 
            MATCH(disease:GARD {gardId: chunk.gardId}) 
            MATCH(cp:CoreProject {coreProjectNumber: chunk.coreProjectNumber})
            MERGE (disease)-[:has_mention_under]->(cp)
        '''  

        fetch_query = f''' SELECT DISTINCT gard_id, core_project_num 
                FROM {self.table_name} 
                WHERE core_project_num is not null 
            '''
        
        fetch_cursor = None 

        count = 0
        batch_num = 0

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(fetch_query)

            while True:

                chunks = []

                rows = fetch_cursor.fetchmany(batch_size)

                if not rows:
                    self.appender.log_stdout("No more rows to fetch.")
                    break
                
                batch_num += 1
                self.appender.log_stdout(f'--- batch# = {batch_num} ---')
               
                for row in rows:
                    gard_id = row['gard_id']
                    core_project_num = row['core_project_num']

                    chunks.append({"gardId": gard_id, "coreProjectNumber": core_project_num})

                if chunks:
                    self.memgraph.execute(batch_create, {"chunks": chunks})

                    count += len(chunks)
                    self.appender.log_stdout(f'Created {len(chunks)} relations in memgraph. Total = {count}')
                else:
                    self.appender.log_stdout('No valid relations to insert into memgraph.')

        except Exception as e:
            self.appender.log_stdout(f'{e}')

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            




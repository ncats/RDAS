import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
   
from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import _id_range_generator, _format_dollars, _val, _curr_timestamp, _date_string, _set_value_for_none

# 1. Create CoreProject nodes
class CoreProjectInitializer(InitBase):


    def __init__(self): 

        super().__init__('grant_gard_project_relation_unique_application_id', 'CoreProject')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/4-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)
 
        self.create_indexes('CoreProject', ['coreProjectNumber','applicationId']) 


     # Override the abstract method
    def init_nodes(self):   

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)


    def populate_nodes(self, min_id, max_id, step=1, batch_size = 200):         

        # Create CoreProject and map to Project
        batch_create = '''
            UNWIND $chunks AS chunk
            MERGE (cp: CoreProject {coreProjectNumber: chunk.coreProjectNumber}) 
            ON CREATE SET 
                cp.coreProjectNumber = chunk.coreProjectNumber,
                cp.totalCost = chunk.totalCost

            WITH cp, chunk
            MATCH(p: Project {applicationId: chunk.applicationId})
            MERGE (cp)-[:has_subproject]->(p)
        '''  

        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)
        
        total = 0
        for start_id, end_id in id_ranges:

            query = f'''
                SELECT  
                    p.application_id, p.core_project_num, p.full_project_num, p.TOTAL_COST
                FROM  {self.table_name} gpru  

                LEFT JOIN grant_project p
                ON gpru.application_id=p.application_id

                WHERE (gpru.id BETWEEN {start_id} AND {end_id}) 
                    AND (gpru.processed IS NULL OR gpru.processed != '{self.processed_flag}')
                    AND p.core_project_num IS NOT NULL                 
            '''

            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()
 
            chunks = []
            for row in rows:

                total += 1
                row = _set_value_for_none(row)
                
                core_project_num = row['core_project_num'] or row['full_project_num']

                if not core_project_num:
                    continue

                total_cost = row['TOTAL_COST']

                chunks.append({
                    "applicationId":  row['application_id'],                   
                    "coreProjectNumber":  core_project_num,
                    "totalCost": _format_dollars(total_cost) if total_cost not in (None, '') and int(total_cost) > 0  else ''
                })
              
            self.memgraph.execute(batch_create, {"chunks": chunks})   

            self.update_processed_flag(start_id, end_id, self.processed_flag) 

            self.appender.log_stdout(f'{_curr_timestamp()} {_curr_timestamp()}\t[flag={self.processed_flag}], [total: {total}], [Id range: [{start_id} - {end_id}], #CoreProject = {len(chunks)}')

        self.close_mysql_conn()        

        self.appender.log_stdout(f'\n\n{_curr_timestamp()} {"="*50} Done Total = {total} {"="*50}\n\n')
        self.appender.close()
        


            


            
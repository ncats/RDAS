import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import _id_range_generator, _val, _curr_timestamp, _date_string, _make_hash_key

'''
01-09-2026
Deprecated - the Agent nodes will be used as Person/People nodes
'''
# 1. Create Agent nodes
class AgentInitializer(InitBase):


    def __init__(self): 

        super().__init__('grant_gard_project_relation_unique_application_id', 'Agent')
        
        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/4-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)

        self.create_indexes('Agent', ['name', '_idx_key']) 
        self.create_indexes('CoreProject', ['coreProjectNumber']) 

 
    # Override the abstract method
    def init_nodes(self):   

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)
    

    # Override
    def populate_nodes(self, min_id, max_id, step=1, batch_size = 200):         

        # CREATE and MERGE
        batch_create = '''
            UNWIND $chunks AS chunk

            MERGE (a: Agent {_idx_key: chunk._idx_key}) 
            ON CREATE SET                
                a.name = chunk.name, 
                a.year = chunk.year,
                a.fundingICs = chunk.fundingICs

            WITH a, chunk
            MATCH (p: Project {applicationId: chunk.applicationId})
            MERGE (p)-[:has_funding_organization]->(a)                
        '''  

        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)
        
        total = 0
        for start_id, end_id in id_ranges:

            query = f'''
                SELECT  p.core_project_num, p.application_id, p.FUNDING_ICS, p.ORG_NAME, p.FY 
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
                total += 1

                application_id =  row['application_id']
                if not application_id:
                    continue

                org_name = row['ORG_NAME']
                _idx_key = _make_hash_key(org_name)

                core_project_num =  _val(row['core_project_num'])

                funding_ics = []
                #NEI:4500000\NIDDK:2000000\
                entries = _val(row['FUNDING_ICS']).split('\\')
                for entry in entries:
                    if entry:
                        parts = entry.split(':')
                        if parts:
                            funding_ics.append(parts[0])

                chunks.append({
                    "applicationId": application_id,
                    "coreProjectNumber":  core_project_num,  
                    "name": org_name,                   
                    "fundingICs": funding_ics,
                    "year": _val(row['FY']),
                     "_idx_key": _idx_key
                })

            try:
                self.memgraph.execute(batch_create, {"chunks": chunks}) 
            except Exception as e:  
                self.appender.log_stdout(f'Exception while insert: {e}')


            # update
            self.update_processed_flag(start_id, end_id, self.processed_flag) 

            self.appender.log_stdout(f'{_curr_timestamp()}\t[flag={self.processed_flag}], [total: {total}], [Id range: [{start_id} - {end_id}], #Agent = {len(chunks)}')


        self.close_mysql_conn()  

        self.appender.log_stdout(f'{_curr_timestamp()} {"="*50} Done! Total = {total} {"="*50}\n\n')
        self.appender.close()

        




            

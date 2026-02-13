import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import _id_range_generator, _val, _curr_timestamp, _date_string


'''
    DROP TABLE IF EXISTS grant_gard_project_relation_unique_core_project_num;

    CREATE TABLE IF NOT EXISTS grant_gard_project_relation_unique_core_project_num (
        id INT(11) AUTO_INCREMENT PRIMARY KEY,
        core_project_num VARCHAR(150) NOT NULL,
        processed VARCHAR(45) DEFAULT NULL,
        created DATETIME DEFAULT current_timestamp(),
        UNIQUE (core_project_num)
    ) AS
    SELECT DISTINCT core_project_num
    FROM grant_gard_project_relation
    WHERE core_project_num is not null and LENGTH(TRIM(core_project_num)) > 0;
'''

# 1. Create Patent nodes
class PatentInitializer(InitBase):


    def __init__(self): 

        super().__init__('grant_gard_project_relation_unique_core_project_num', 'Patent')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/4-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)
 
        # Create index later
        self.create_indexes('Patent', ['patentId']) 



    # Override the abstract method
    def init_nodes(self):   

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)


    # Override
    def populate_nodes(self, min_id, max_id, step=1, batch_size = 200):         

        # Create Patent and map to CoreProject
        batch_create = '''
            UNWIND $chunks AS chunk
            MERGE (p: Patent {patentId: chunk.patent.id}) 
            ON CREATE SET 
                p.patentId = chunk.patent.id,
                p.title = chunk.patent.title,
                p.orgName = chunk.patent.orgName

            WITH p, chunk 
            MATCH(cp: CoreProject {coreProjectNumber: chunk.coreProjectNumber}) 

            MERGE (cp)-[:patented]->(p)
        '''  

        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)
        
        total = 0
        for start_id, end_id in id_ranges:

            query = f'''
                SELECT
                    ucpn.core_project_num,
                    p.patent_id,
                    p.patent_title,
                    p.patent_org_name
                FROM {self.table_name} ucpn

                LEFT JOIN grant_patent p
                ON ucpn.core_project_num = p.project_id 

                WHERE  (ucpn.id BETWEEN {start_id} AND {end_id})  
                AND (ucpn.processed IS NULL OR ucpn.processed != '{self.processed_flag}')
                AND p.PATENT_ID is not null
            '''
            # ??? One PATENT_ID may have multiple PROJECT_IDs ???
            # SELECT * FROM rdas_db.grant_patent where patent_id=10550405;
            # SELECT * FROM rdas_db.grant_patent where patent_id=10946042;

            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()
 
            chunks = []
            for row in rows:
                total += 1 
                
                core_project_num = _val(row['core_project_num'])
                if not core_project_num:
                    continue

                chunks.append({             
                    "coreProjectNumber":  core_project_num,  
                    "patent":{
                        "id":  _val(row['patent_id']),
                        "title":  _val(row['patent_title']),
                        "orgName":  _val(row['patent_org_name'])
                    }
                })
  
            try:
                self.memgraph.execute(batch_create, {"chunks": chunks}) 
            except Exception as e:  
                self.appender.log_stdout(f'Exception while insert: {e}')

            self.update_processed_flag(start_id, end_id, self.processed_flag)

            self.appender.log_stdout(f'{_curr_timestamp()}\t[flag={self.processed_flag}], [total: {total}], [Id range: [{start_id} - {end_id}], #Patents = {len(chunks)}')


        self.close_mysql_conn()  
        
        self.appender.log_stdout(f'\n{_curr_timestamp()} {"="*50} Done! Total = {total} {"="*50}\n\n')
        self.appender.close() 
        


            




            
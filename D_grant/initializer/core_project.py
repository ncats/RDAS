import os
import sys
from decimal import Decimal, InvalidOperation
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
   
from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import _id_range_generator, _val, _curr_timestamp, _date_string, _set_value_for_none

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
                cp.coreProjectNumber = chunk.coreProjectNumber

            SET cp.totalCost = chunk.totalCost

            WITH cp, chunk
            MATCH(p: Project {applicationId: chunk.applicationId})
            MERGE (cp)-[:has_subproject]->(p)
        '''  

        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)
        
        total = 0
        for start_id, end_id in id_ranges:

            query = f'''
                SELECT  
                    p.application_id,
                    p.core_project_num,
                    cost.total_cost_1,
                    cost.total_cost_2
                FROM  {self.table_name} gpru  

                LEFT JOIN grant_project p
                ON gpru.application_id=p.application_id

                LEFT JOIN (
                    SELECT
                        p2.core_project_num,
                        SUM(p2.TOTAL_COST) AS total_cost_1,
                        SUM(p2.DIRECT_COST_AMT + p2.INDIRECT_COST_AMT) AS total_cost_2
                    FROM grant_project AS p2
                    WHERE p2.core_project_num IS NOT NULL
                    GROUP BY p2.core_project_num
                ) cost
                ON p.core_project_num = cost.core_project_num

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
                
                core_project_num = row['core_project_num']

                if not core_project_num:
                    continue

                total_cost = row['total_cost_2']

                if total_cost is None:
                    total_cost = row['total_cost_1']

                chunks.append({
                    "applicationId":  row['application_id'],                   
                    "coreProjectNumber":  core_project_num,
                    "totalCost": self._to_number_or_blank(total_cost)
                })
              
            self.memgraph.execute(batch_create, {"chunks": chunks})   

            self.update_processed_flag(start_id, end_id, self.processed_flag) 

            self.appender.log_stdout(f'{_curr_timestamp()} {_curr_timestamp()}\t[flag={self.processed_flag}], [total: {total}], [Id range: [{start_id} - {end_id}], #CoreProject = {len(chunks)}')

        self.close_mysql_conn()        

        self.appender.log_stdout(f'\n\n{_curr_timestamp()} {"="*50} Done Total = {total} {"="*50}\n\n')
        self.appender.close()


    def _to_number_or_blank(self, value):
        """
        Convert the selected MySQL total cost to a Python number.
        If MySQL has no cost value, keep CoreProject.totalCost as an empty string.
        """

        if value is None:
            return ''

        if isinstance(value, str) and value.strip() == '':
            return ''

        if isinstance(value, int) and not isinstance(value, bool):
            return value

        if isinstance(value, float):
            return int(value) if value.is_integer() else value

        if isinstance(value, Decimal):
            return int(value) if value == value.to_integral_value() else float(value)

        try:
            number = Decimal(str(value).strip().replace(',', ''))
        except (InvalidOperation, ValueError):
            return ''

        return int(number) if number == number.to_integral_value() else float(number)
        


            


            

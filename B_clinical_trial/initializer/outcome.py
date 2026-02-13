import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import  _id_range_generator, _clean, _curr_timestamp, _date_string

# Create PrimaryOutcome nodes
class PrimaryOutcomeInitializer(InitBase):


    def __init__(self): 

        super().__init__('clinical_trial_unique', 'PrimaryOutcome')
        
        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/2-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)


    # Override the abstract method
    def init_nodes(self):   

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)


    # Override
    def populate_nodes(self, min_id, max_id, step=1, batch_size=300):

        batch_create = '''
            UNWIND $chunks AS chunk
            MATCH (x: ClinicalTrial {nctId: chunk.nctId}) 
            CREATE (y: PrimaryOutcome)
            SET 
                y.primaryOutcomeMeasure = chunk.measure,
                y.primaryOutcomeTimeFrame = chunk.timeFrame,
                y.primaryOutcomeDescription = chunk.description

            MERGE (x)-[:has_outcome]->(y) 
        '''               
                
        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

        total  = 0
        for start_id, end_id in id_ranges:

            query = f'''
                SELECT id, nctid, studies  
                FROM {self.table_name}
                WHERE nctid IS NOT NULL 
                AND id BETWEEN {start_id} AND {end_id}
            '''

            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()

            chunks = [] 

            for row in rows:

                total += 1
                nctid = row['nctid'] 
                study = json.loads(row['studies'])

                outcomes_module = study.get('protocolSection', dict()).get('outcomesModule', {})    
                primaryOutcomes = outcomes_module.get('primaryOutcomes', [])

                if not primaryOutcomes:
                    continue
                
                '''
                    "primaryOutcomes": [
                        {
                            "measure": "Number of Subjects With Grade 3 Solicited Local Symptoms After Dose 1, Dose 2 and Across Doses",
                            "description": "Solicited local symptoms assessed were pain, redness and swelling. Grade 3 pain = pain that prevented normal activity. Grade 3 redness/swelling = redness/swelling spreading beyond 20 millimeters (mm) of injection site.",
                            "timeFrame": "From Day 0 to Day 6"
                        },
                        {
                            "measure": "Number of Subjects With Grade 3 Solicited Local Symptoms After Dose 2, Dose 3 and Across Doses.",
                            "description": "Solicited local symptoms were only collected after Dose 2 of EPI vaccination. Solicited local symptoms assessed were pain, redness and swelling. Grade 3 pain = pain that prevented normal activity. Grade 3 redness/swelling = redness/swelling spreading beyond 20 millimeters (mm) of injection site.",
                            "timeFrame": "From Day 0 to Day 6"
                        }
                    ]
                '''
                for outcome in primaryOutcomes:

                    chunks.append(
                        {   
                            "nctId": nctid,
                            "measure": _clean(outcome.get('measure','')),
                            "timeFrame":  _clean(outcome.get('timeFrame','')),
                            "description":  _clean( outcome.get('description','')) 
                        }
                    )
            

            if chunks: 
            
                try:
                    self.memgraph.execute(batch_create, {"chunks": chunks}) 
                except Exception as e:
                    self.appender.log_stdout(f"Error executing batch create: {e}")
                    raise
            
            self.update_processed_flag(start_id, end_id, self.processed_flag)
            
            self.appender.log_stdout(f'{_curr_timestamp()} [total: {total}], Id range: [{start_id} - {end_id}], #PrimaryOutcome = {len(chunks)}')


        self.close_mysql_conn()  
 
        self.appender.log_stdout(f'\n{_curr_timestamp()} {"="*50} Done! Total = {total} {"="*50}\n\n')
        self.appender.close()
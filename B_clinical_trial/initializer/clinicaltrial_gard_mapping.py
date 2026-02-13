import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
 
import time
from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import  _id_range_generator, _curr_timestamp, _date_string, _time_hms

class ClinicalTrialToGARDMappingInitializer(InitBase):


    def __init__(self): 

        super().__init__('clinical_trial', 'ClinicalTrail-GARD-Mapping')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/2-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)

        self.create_indexes('GARD', ['GardId']) 

    
    # Override the abstract method
    def init_nodes(self):   

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)


    # Override
    def populate_nodes(self, min_id, max_id, step=1, batch_size=300):
         
        batch_create = '''
            UNWIND $chunks AS chunk
            MATCH (x: GARD {gardId: chunk.gardId})
            MATCH (y: ClinicalTrial {nctId: chunk.nctId})
            MERGE (x)<-[:mapped_to_gard {matchedTermRDAS: chunk.disease}]-(y)
        '''
        start = time.time()

        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

        _count  = 0
        for start_id, end_id in id_ranges:
            query = f'''
                SELECT id, gardid, disease, nctid
                FROM {self.table_name} 
                WHERE nctid IS NOT NULL
                AND id BETWEEN {start_id} AND {end_id}
            ''' 

            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()

            chunks = []
            for row in rows:
                _count += 1

                gard_id = row['gardid']
                disease = row['disease']
                nctid = row['nctid']  

                chunks.append({"nctId": nctid, "gardId": gard_id, "disease": disease})

            # Execute batch creation if we have valid data
            if chunks:
                try:
                    self.memgraph.execute(batch_create, {"chunks": chunks})   
                except Exception as e:
                    self.appender.append_and_print(f'ClinicalTrialGARDMapping:: Error executing batch create for ID range {start_id}-{end_id}: {e}')
                    raise
            
            self.update_processed_flag(start_id, end_id, self.processed_flag)
            self.appender.log_stdout(f'{_curr_timestamp()} [total: {_count}], Id range: [{start_id} - {end_id}], #mappings = {len(chunks)}')
            
        self.close_mysql_conn()    
 
        end = time.time()
        elapsed_time = end - start
        hours, minutes, seconds = _time_hms(elapsed_time)

        self.appender.log_stdout(f'\n\n{"="*50} All done! Total time elapsed: {hours} hours, {minutes} minutes, {seconds} seconds {"="*50}\n\n') 
        self.appender.close() 
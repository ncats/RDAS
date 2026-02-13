import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import re
import json
from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import  _id_range_generator, _curr_timestamp, _date_string, _clean, _make_hash_key

# Create Intervention nodes
class InterventionInitializer(InitBase):


    def __init__(self): 

        super().__init__('clinical_trial_unique', 'Intervention')

        class_name = type(self).__name__ 
        self.log_file = f'{self.log_dir}/2-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)

        # Memgraph doesn't support composite index
        self.create_indexes('Intervention', ['interventionName', 'interventionType', '_composite_key', '_intervention_name_key']) 
        

     # Override the abstract method
    def init_nodes(self):   

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)


    # Override
    def populate_nodes(self, min_id, max_id, step=1, batch_size=200):
        
        batch_create = '''
            UNWIND $chunks AS chunk
            MATCH (x: ClinicalTrial {nctId: chunk.nctId}) 
            MERGE (y: Intervention {_composite_key: chunk._composite_key})
            ON CREATE SET 
                y.interventionName = chunk.name, 
                y.interventionType = chunk.type, 
                y.interventionDescription = chunk.description,
                y._intervention_name_key = chunk._intervention_name_key
            MERGE (x)-[:has_intervention]->(y)
        '''           
        
        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

        total  = 0
        for start_id, end_id in id_ranges:

            query = f'''
                SELECT id, nctid, studies  FROM {self.table_name}
                WHERE nctid IS NOT NULL AND id BETWEEN {start_id} AND {end_id}
            '''

            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()

            chunks = [] 

            for row in rows:

                nctid = row['nctid'] 
                study = json.loads(row['studies'])
  
                intervention_module = study.get('protocolSection', {}).get('armsInterventionsModule', {})
                interventions = intervention_module.get('interventions', [])

                if not interventions:
                    continue

                for intervention in interventions: 

                    name = _clean(intervention.get('name','')) 
                    type = _clean(intervention.get('type','')) 
                    description = _clean(intervention.get('description','')) 

                    composite_key = f'{name}_{type}_{description}'
                    _composite_key =  _make_hash_key(composite_key)      
                    _intervention_name_key = _make_hash_key(name)

                    chunks.append(
                        {   
                            "nctId": nctid,
                            "name": name,
                            "type": type,
                            "description": description,
                            "_composite_key": _composite_key,
                            "_intervention_name_key": _intervention_name_key
                        }
                    )
                
            # Execute batch creation if we have valid data
            if chunks:            
                try:
                    self.memgraph.execute(batch_create, {"chunks": chunks}) 
                    total += len(chunks)

                except Exception as e:
                    self.appender.log_stdout(f"Error executing batch create for ID range {start_id}-{end_id}: {e}")
                    raise

            self.update_processed_flag(start_id, end_id, self.processed_flag)

            self.appender.append_and_print(f'{_curr_timestamp()} [total: {total}], Id range: [{start_id} - {end_id}], #Intervention = {len(chunks)}')
            
        self.close_mysql_conn()   

        self.appender.log_stdout(f'{_curr_timestamp()} {"="*50} Done! Total = {total} {"="*50}\n\n')
        self.appender.close()
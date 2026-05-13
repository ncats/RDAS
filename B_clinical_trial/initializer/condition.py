import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import time
from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import  _id_range_generator, _gard_text_normalize, _is_english, _is_under_char_threshold, _curr_timestamp, _date_string

# Create Condition nodes
class ConditionInitializer(InitBase):


    def __init__(self): 

        super().__init__('clinical_trial_unique','Condition & Mapping')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/2-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)

        self.create_indexes('Condition', ['condition']) 

    
    # Override the abstract method
    def init_nodes(self):   

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)


    # Override
    def populate_nodes(self, min_id, max_id, step=3, batch_size=200):
        
        batch_create = '''
            UNWIND $chunks AS chunk
            MATCH (ct: ClinicalTrial {nctId: chunk.nctId})
            
            WITH ct, chunk.condition_gard_mappings AS condition_gard_mappings
            UNWIND condition_gard_mappings AS mapping
            
            MERGE (con: Condition {condition: mapping.condition})
            MERGE (ct)-[:has_investigated_condition]->(con)
            
            WITH con, mapping.gardid_list AS gardid_list
            WHERE gardid_list IS NOT NULL AND size(gardid_list) > 0
            
            UNWIND gardid_list AS gardId
            MATCH (g: GARD {gardId: gardId})
            MERGE (con)-[:has_mapped_condition]->(g)
        '''
        
        # O(1) performance
        gard_id_names_dict = self._get_GARD_names_syns()

        # Pre-build reverse lookup index for O(1) performance
        term_to_gard_ids = {}

        for gardid, terms_list in gard_id_names_dict.items():
            for term in terms_list:
                if term not in term_to_gard_ids:
                    term_to_gard_ids[term] = []

                term_to_gard_ids[term].append(gardid)
                

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
            skipped_no_conditions = 0

            for row in rows:
                total += 1

                try:
                    nctid = row.get('nctid')
                    if not nctid:
                        continue

                    # Parse study JSON with error handling
                    study = json.loads(row['studies'])
                    
                    # Safely extract conditions
                    protocol = study.get('protocolSection', {})
                    conditions_module = protocol.get('conditionsModule', {})
                    conditions = conditions_module.get('conditions', [])
                    
                    if not conditions or not isinstance(conditions, list):
                        skipped_no_conditions += 1
                        continue
                    
                    #Map conditions to GARD IDs using pre-built index
                    condition_gard_mappings = []

                    for condition in conditions:
                        if not condition:
                            continue
                        
                        cond_normalized = _gard_text_normalize(condition)
                        gardid_list = term_to_gard_ids.get(cond_normalized, [])
                        
                        condition_gard_mappings.append({
                            "condition": cond_normalized,
                            "gardid_list": gardid_list
                        })
                    
                    if condition_gard_mappings:
                        chunks.append({
                            "nctId": nctid,
                            "condition_gard_mappings": condition_gard_mappings
                        })

                except json.JSONDecodeError as e:
                    self.appender.log_stdout(f"Invalid JSON for nctId {row.get('nctid')}: {e}")
                    continue
                except Exception as e:
                    self.appender.log_stdout(f"Error processing row {row.get('id')}: {e}")
                    continue
                

            # Execute batch creation if we have valid data
            if chunks:
                try:
                    self.memgraph.execute(batch_create, {"chunks": chunks})
                    self.appender.log_stdout(f"[Processed {total} rows] Created {len(chunks)} mappings, skipped {skipped_no_conditions} without conditions (ID range: {start_id}-{end_id})")
                
                except Exception as e:
                    self.appender.log_stdout(f"Error executing batch create for ID range {start_id}-{end_id}: {e}")
                    raise
            else:
                self.appender.log_stdout(f"No valid condition mappings found in ID range {start_id}-{end_id}")

            self.update_processed_flag(start_id, end_id, self.processed_flag)

        self.close_mysql_conn()  

        self.appender.log_stdout(f'\n{_curr_timestamp()} {"="*50} Done! Total = {total} {"="*50}\n\n')
        self.appender.close() 

            

    def _get_GARD_names_syns(self):

        temp = dict()
        response = self.memgraph.execute_and_fetch('MATCH (x:GARD) RETURN x.gardId AS gardId, x.gardName AS gardName, x.synonyms AS synonyms')

        for res in response:

            gardid = res['gardId']
            gardname = res['gardName']
            gardsyns = res['synonyms']

            gardsyns_eng = [syn for syn in gardsyns if _is_english(syn)]
            gardsyns_char_threshold = [syn for syn in gardsyns if _is_under_char_threshold(syn)]
            
            filtered_syns = [x for x in gardsyns if not x in gardsyns_eng]
            filtered_syns = [x for x in filtered_syns if not x in gardsyns_char_threshold]

            termlist = [gardname] + filtered_syns

            termlist = [_gard_text_normalize(term) for term in termlist]

            temp[gardid] = termlist

        return temp

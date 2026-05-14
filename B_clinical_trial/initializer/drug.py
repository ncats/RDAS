import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import time
from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import  _curr_time_diff, _curr_timestamp, _date_string, _make_hash_key
 
# Create Drug nodes
class DrugInitializer(InitBase): 


    def __init__(self): 

        super().__init__('clinical_trial_intervention_drug', 'Drug')

        self.no_column_named_processed = True
        
        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/2-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)

        self.create_indexes('Drug', ['rxnormID']) 
        self.create_indexes('Intervention', ['_intervention_name_key']) 

        self.key_map = { 
            'ATC': 'atc',
            'AVAILABLE_STRENGTH': 'availableStrength',   
            'DRUGBANK': 'drugBank',   
            'MMSL_CODE': 'mmslCode',  
            'PRESCRIBABLE': 'prescribable',
            'QUANTITY': 'quantity',
            'RxCUI': 'RxCUI',
            'RXNAV_HUMAN_DRUG': 'rxnavHumanDrug',            
            'RXNAV_VET_DRUG': 'rxnormVetDrug',
            'RxNormID': 'rxnormID',
            'RxNormName': 'rxnormName',
            'RxNormSynonym': 'rxNormSynonym',             
            'SNOMEDCT': 'snomedCt',            
            'SPL_SET_ID': 'splSetId',
            'STRENGTH': 'strength',
            'TTY': 'tty',
            'UNII_CODE': 'unii',
            'USP': 'usp',
            'VUID': 'vuid'
        }
    

    def transform_json_object(self, json_obj, key_map):
        """
        Transform JSON object by filtering keys, renaming them, and adding missing keys.
        
        Args:
            json_obj: Original JSON object
            key_map: Dictionary mapping old keys to new keys
        
        Returns:
            Transformed JSON object with filtered, renamed, and complete keys
        """
        transformed = {}
        
        # Add existing keys with new names
        for old_key, value in json_obj.items():

            if old_key in key_map:
                new_key = key_map[old_key]
                transformed[new_key] = value
        
        # Add missing keys with empty list
        for old_key, new_key in key_map.items():
            if new_key not in transformed:
                transformed[new_key] = []
        
        return transformed
    
        
    # Override the abstract method
    def init_nodes(self):    

        self.populate_nodes(0, 0) 


    # Overwrite
    def populate_nodes(self, min_id=0, max_id=0, step=0, batch_size=200):
        
        batch_create = '''
            UNWIND $chunks AS chunk

            MERGE (x: Drug {rxnormID: chunk.rxnormID}) 
            ON CREATE SET 
                x = chunk.props

            WITH x, chunk
            OPTIONAL MATCH (y: Intervention {_intervention_name_key: chunk._intervention_name_key}) 

            WITH x, y, chunk
            WHERE y IS NOT NULL
            MERGE (y)-[:has_rxnorm_mapping {with_spacy: chunk.wspacy}]->(x) 
        ''' 
    
          
        # 1. Create optimized index with property_key FIRST
        '''
        CREATE INDEX idx_drug_optimized 
            ON clinical_trial_intervention_drug(
                property_key,
                RxNormID, 
                intervention,
                wspacy
            );
        '''
        # 2. Create index for filter
        ''' 
        CREATE INDEX idx_drug_group_filter 
            ON clinical_trial_intervention_drug(
                RxNormID,
                intervention,
                wspacy,
                property_key
            );
        ''' 

        query = f'''
            SELECT 
                RxNormID, intervention, wspacy, 
                GROUP_CONCAT(
                    DISTINCT CONCAT('"', property_key, '":', property_val) 
                    ORDER BY property_key, property_val SEPARATOR ','
                ) AS props
            FROM {self.table_name} 
            GROUP BY 
                RxNormID, intervention, wspacy
        '''
    
        start1 = time.time()
        print(f'{query}\nloading data......\n') 

        # Increase GROUP_CONCAT limit, which has a maximum length in MySQL.
        #   SHOW VARIABLES LIKE 'group_concat_max_len';
        self.dict_cursor.execute("SET SESSION group_concat_max_len = 10000000")  # 10MB

        self.dict_cursor.execute(query)    

        hour, minute, second = _curr_time_diff(start1)       
        self.appender.log_stdout(f'Time elapsed: {hour} hours, {minute} minutes, {second} seconds')

        total = 0 
        while True:
            try:            
                # Fetch rows by the fetch batch_size
                rows = self.dict_cursor.fetchmany(batch_size)

                if not rows:
                    self.appender.log_stdout(f'\n--- All finished, no more data ---')
                    break
 
                chunks = [] 
                for row in rows:

                    props = row['props'] 
                    wspacy = row['wspacy']
                    RxNormID = row['RxNormID']
                    intervention_name = row['intervention']
                    normalized_wspacy = "true" if wspacy == 1 else "false"

                    try:
                        obj = json.loads('{'+props+'}')
                        transformed_obj = self.transform_json_object(obj, self.key_map)
                        print(f'RxNormID = {RxNormID}, obj.RxNormID = {obj.get("RxNormID")}, transformed_obj.rxnormID = {transformed_obj.get("rxnormID")}')
                    except Exception as e:
                        self.appender.log_stdout(f'\n--- Error: {e} ---')
                        self.appender.log_stdout(f'RxNormID = {RxNormID}') 
                        self.appender.log_stdout(f'props = {props}')  
                        self.appender.log_stdout(f'obj = {obj}') 
    
                    _intervention_name_key = _make_hash_key(intervention_name)

                    chunks.append({
                        "rxnormID": RxNormID,
                        "props": transformed_obj,
                        "wspacy": normalized_wspacy,
                        "_intervention_name_key": _intervention_name_key
                    }) 
                
                if chunks:
                    try:
                        self.memgraph.execute(batch_create, {"chunks": chunks}) 
                        total += len(chunks)
                        
                    except Exception as e:
                        self.appender.log_stdout(f"Error executing batch create: {e}")
                        raise

                self.appender.log_stdout(f'{_curr_timestamp()}  {total} (Drug - interventionName) rows processed')

            except Exception as err:
                self.appender.log_stdout(f"Error: {err}") 

        self.close_mysql_conn()  
 
        hour, minute, second = _curr_time_diff(start1)       
        self.appender.log_stdout(f'\n{"="*50} {_curr_timestamp()} Done! Total = {total} {"="*50}\n\n')
        self.appender.log_stdout(f'Total time: {hour} hours, {minute} minutes, {second} seconds')

        self.appender.close()

 

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
 
import hashlib
from baseclass.init_base import InitBase
#from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import _curr_timestamp, _date_string, _clean, _make_hash_key
 
# Create Substance nodes
class SubstanceInitializer(InitBase):


    def __init__(self): 
        
        super().__init__('publication_substance', 'Substance')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/3-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)
  
        self.create_indexes('Substance', ['registryNumber','name', '_composite_key'])


    # Override the abstract method
    def init_nodes(self):   

        #min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        #print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(0, 0)


    # Override
    def populate_nodes(self, min_id, max_id, step=1, batch_size = 100): 

        # SHOW INDEX INFO; 
        batch_create = '''                
            UNWIND $substances AS subs
            MERGE (s: Substance {_composite_key: subs._composite_key})  
            ON CREATE SET
                s.name = subs.substanceName,
                s.registryNumber = subs.registryNumber,

            WITH s, subs
            UNWIND subs.pubmed_id_list AS pubmedId
            MATCH (a: Article {pubmedId: pubmedId})
            MERGE (a)-[:has_substance]->(s) 
        '''
           
        batch_create = '''                
            UNWIND $substances AS subs
            MERGE (s: Substance {_composite_key: subs._composite_key})  
            ON CREATE SET
                s.name = subs.substanceName,
                s.registryNumber = subs.registryNumber
            ON MATCH SET
                s.name = CASE 
                    WHEN (subs.substanceName IS NOT NULL AND subs.substanceName <> '' AND 
                          (s.name IS NULL OR s.name = '')) 
                    THEN subs.substanceName 
                    ELSE s.name 
                END,
                s.registryNumber = CASE 
                    WHEN (subs.registryNumber IS NOT NULL AND subs.registryNumber <> '' AND 
                          (s.registryNumber IS NULL OR s.registryNumber = '')) 
                    THEN subs.registryNumber 
                    ELSE s.registryNumber 
                END

            WITH s, subs
            UNWIND subs.pubmed_id_list AS pubmedId
            MATCH (a: Article {pubmedId: pubmedId})
            MERGE (a)-[:has_substance]->(s) 
        '''

        fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
        data_cursor = self.mysql.cursor(dictionary=True, buffered=True)  # Add buffered=True

        # query
        query = f'''
            SELECT DISTINCT hash_id FROM {self.table_name} 
            WHERE (processed IS NULL OR processed != %s)
            ORDER BY hash_id
        ''' 
        fetch_cursor.execute(query, (self.processed_flag,)) 
        
        _count = 0
                
        # batch fetch
        while True:

            rows = fetch_cursor.fetchmany(batch_size)
            if not rows:
                break

            batch_hash_ids = [row['hash_id'] for row in rows] 
            _count += len(batch_hash_ids)

            # 1. Create a single query for the batch of batch_hash_ids
            sub_query = f'''
                SELECT pubmed_id, substance_name, registry_number, hash_id 
                FROM {self.table_name} 
                WHERE hash_id IN {tuple(batch_hash_ids)}
            '''

            data_cursor.execute(sub_query)
            rows = data_cursor.fetchall()

            # 2. Group rows by hash_id
            rows_by_hash_id = {}
            substance_list = []

            for row in rows:
                hash_id = row['hash_id']
                rows_by_hash_id.setdefault(hash_id, []).append(row)

            # 3. Process each hash_id in the batch
            for hash_id in batch_hash_ids:

                pubmed_id_list = []
                substance_name = None
                registry_number = None

                # 4. Process rows for ONE hash_id
                for row in rows_by_hash_id.get(hash_id, []):

                    pubmed_id_list.append(row['pubmed_id'])
                    
                    # Assign substance_name and registry_number only if not already set
                    substance_name = substance_name or row['substance_name']
                    registry_number = registry_number or (row['registry_number'] if row['registry_number'] != '0' else None)

                # 5. After processing for ONE hash_id, create substance object for this hash_id
                substance = {
                    "pubmed_id_list": pubmed_id_list,
                    "substanceName": substance_name or '',
                    "registryNumber": registry_number or ''
                }

                # 6. Append substance to the list if one of the two fields is non-empty
                if substance_name or registry_number:

                    # Create composite key
                    if registry_number:
                        composite_str = registry_number.lower()
                    else:
                        composite_str = _clean(substance_name).lower() 

                    composite_str = "".join(composite_str.split())  # Replace multiple whitespaces
                    _composite_key = _make_hash_key(composite_str)

                    substance['_composite_key'] = _composite_key

                    # Append substance to the list
                    substance_list.append(substance)
                    
                    # print out every substance 
                    self.appender.log_stdout( f'hash_id={hash_id}, registry_number={registry_number}, substance_name={substance_name}, #pubmed_id={len(pubmed_id_list)}')

            # 7.  
            if substance_list:
                try:         
                    self.memgraph.execute(batch_create, {"substances": substance_list})               
                    self.appender.log_stdout( f'\n=== {_curr_timestamp()} [Total: {_count}], [flag: {self.processed_flag}], #substances={len(substance_list)} ===\n')

                except Exception as e:
                    self.appender.append_and_print(f'Error: {e}')
                    raise             

            # 8.
            # Batch update processed flag by executemany
            query = f"UPDATE {self.table_name} SET processed = %s WHERE hash_id = %s"

            self.update_cursor.executemany(query, [(self.processed_flag, hid) for hid in batch_hash_ids])
            self.mysql.commit()

        self.close_mysql_conn() 
                   
        self.appender.log_stdout(f'\n{"="*50}{_curr_timestamp()} Done! Total = {_count} {"="*50}\n')
        self.appender.close()
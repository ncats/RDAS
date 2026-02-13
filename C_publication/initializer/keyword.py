import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import _id_range_generator, _curr_timestamp, _date_string
 
# 1. Create Keyword nodes
class KeywordInitializer(InitBase):


    def __init__(self): 

        super().__init__('publication_article', 'Keyword')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/3-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)

        self.create_indexes('Keyword', ['keyword'])

        # Create index on pubmedId if it doesn't exist
        self.create_indexes('Article', ['pubmedId']) 

 
    # Override the abstract method
    def init_nodes(self):   

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)


    # Override
    def populate_nodes(self, min_id, max_id, step=1, batch_size = 1000): 
  
        batch_create = ''' 
            UNWIND $chunks  AS chunk 
            MATCH (a: Article {pubmedId: chunk.pubmedId})

            WITH a, chunk
            UNWIND chunk.keywords AS kw
            MERGE (k: Keyword {keyword: kw}) 
            MERGE (a)- [r: has_keyword] -> (k)
        '''
        
        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

        _count  = 0
        for start_id, end_id in id_ranges:
            
            # With nornal index
            query = f'''
                    SELECT  id, pubmed_id, source_json
                    FROM  {self.table_name}
                    WHERE (id BETWEEN {start_id} AND {end_id}) 
                    AND (processed IS NULL or processed !=\'{self.processed_flag}\')
                '''
            
            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()

            chunks = [] 
            for row in rows:
                
                _count += 1
                pubmed_id = int(row['pubmed_id'])
                source_obj = json.loads(row['source_json'])

                # Skip if keywordList is missing or empty
                keyword_list = source_obj.get('keywordList') or {}
                
                if not keyword_list:
                    continue

                try:
                    keywords = [kw.lower() for kw in keyword_list.get('keyword', []) if kw is not None]
                    # Only append if keywords exist
                    if keywords:
                        chunks.append({"pubmedId": pubmed_id, "keywords": keywords})
                except Exception as e:
                    self.appender.log_stdout(f"Error processing row {pubmed_id}: {e}")

            if chunks:
                try:
                    self.memgraph.execute(batch_create, {"chunks": chunks})  
                except Exception as e:
                    self.appender.append_and_print(f'Exception while insert: {e}')
                    raise  

            self.update_processed_flag(start_id, end_id, self.processed_flag) 
  
            self.appender.append_and_print(f'{_curr_timestamp()} [total: {_count}], [flag: {self.processed_flag}], Id range {start_id} - {end_id},  #chunks.size = {len(chunks)}')
            
        self.close_mysql_conn()   
        
        self.appender.log_stdout(f'\n{"="*50}{_curr_timestamp()} Done! Total = {_count} {"="*50}\n')
        self.appender.close()
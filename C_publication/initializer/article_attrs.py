import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import _id_range_generator, _curr_timestamp, _set_value_for_none, _date_string
 

# 1. Add fullTextUrl list as attribute to the Article node (FullTextUrlInitializer depreciated)
# 2. Add issue & volume as attributes to the Article (issue & volume are originally from 3_publication/initializer/journal.py)

class ArticleExtraAttributesInitializer(InitBase):


    def __init__(self): 

        super().__init__('publication_article', 'ExtraAttributesInitializer')
        
        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/3-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)

        # Create index on pubmedId if it doesn't exist
        self.create_indexes('Article', ['pubmedId']) 


     # Override the abstract method
    def init_nodes(self):   

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)


    # Override
    def populate_nodes(self, min_id, max_id, step=1, batch_size = 200): 
  
        batch_create = '''                
            UNWIND $chunks AS chunk
            MATCH (a: Article {pubmedId: chunk.pubmedId})  
            SET a.fullTextUrls = chunk.fullTextUrls,
                a.issue = chunk.issue,
                a.volume = chunk.volume
        '''
        
        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

        _count  = 0
        for start_id, end_id in id_ranges:   
                        
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

                # 1.
                fullTextUrlListObj = source_obj.get('fullTextUrlList') or {}

                # 2.
                journalInfo = source_obj.get('journalInfo') or {}
                '''
                 {
                    "issue": "1",
                    "volume": "1",
                    "journalIssueId": 599194,
                    "dateOfPublication": "1978 Dec",
                    "monthOfPublication": 12,
                    "yearOfPublication": 1978,
                    "printPublicationDate": "1978-12-01",
                    "journal": {
                        "title": "Immunopharmacology",
                        "medlineAbbreviation": "Immunopharmacology",
                        "issn": "0162-3109",
                        "isoabbreviation": "Immunopharmacology",
                        "nlmid": "7902474"
                    }
                }
                '''

                if not fullTextUrlListObj and not journalInfo:
                    continue

                try:
                    full_text_urls = fullTextUrlListObj.get("fullTextUrl", []) or []
                    fullTextUrls = [u.get("url") for u in full_text_urls if u.get("url")]

                    issue = journalInfo.get("issue", "")
                    volume = journalInfo.get("volume", "")
                    
                    chunks.append({
                        "pubmedId": pubmed_id, 
                        "fullTextUrls": fullTextUrls,
                        "issue": issue,
                        "volume": volume
                    })

                except Exception as e:
                    self.appender.log_stdout(f"Error processing row with pubmed_id: {pubmed_id}: {e}")

            if chunks: 
                try:
                    self.memgraph.execute(batch_create, {"chunks": chunks}) 
                except Exception as e:  
                    self.appender.log_stdout(f'Exception while insert: {e}')
                    raise             

            self.update_processed_flag(start_id, end_id, self.processed_flag) 

            total_urls = sum(map(lambda item: len(item["fullTextUrls"]), chunks))
            self.appender.log_stdout(f'{_curr_timestamp()} [total: {_count}], [flag: {self.processed_flag}], Id range: [{start_id} - {end_id}], #fullTextUrls = {total_urls}')            
            
        self.close_mysql_conn() 
        
        self.appender.log_stdout(f'\n{"="*50}{_curr_timestamp()} Done! Total = {_count} {"="*50}\n')
        self.appender.close()
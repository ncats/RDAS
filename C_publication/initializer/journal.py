import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import _id_range_generator, _curr_timestamp, _date_string 
 

# Create Journal nodes
class JournalInitializer(InitBase):


    def __init__(self): 

        super().__init__('publication_article', 'Journal')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/3-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)

        self.create_indexes('Journal', ['issn','essn','nlmid','title'])
 

    # Override the abstract method
    def init_nodes(self):   

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)

    
    def populate_nodes(self, min_id, max_id, step=1, batch_size = 100): 

        # The CALL block with UNION is the standard Memgraph pattern for conditional execution.
        # This checks if either issn or essn has a valid value to use as an identifier.
        batch_create = '''
            WITH $chunks AS chunk
            WHERE chunk IS NOT NULL AND size(chunk) > 0

            UNWIND chunk AS item
            MATCH (a: Article {pubmedId: item.pubmedId})

            CALL {
                WITH item, a
                WHERE item.journal.issn = 'N/A' AND item.journal.essn = 'N/A'
                MERGE (j: Journal {title: item.journal.title})
                ON CREATE SET 
                    j.issn = item.journal.issn,
                    j.essn = item.journal.essn,
                    j.nlmid = item.journal.nlmid
                MERGE (a)-[:published_in]->(j)
                RETURN j
                UNION
                WITH item, a
                WHERE item.journal.issn <> 'N/A' OR item.journal.essn <> 'N/A'
                MERGE (j: Journal {issn: item.journal.issn, essn: item.journal.essn})
                ON CREATE SET 
                    j.title = item.journal.title,
                    j.nlmid = item.journal.nlmid
                MERGE (a)-[:published_in]->(j)
                RETURN j
            }
        '''
  
        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

        _count = 0
        for start_id, end_id in id_ranges:

            query = f'''
                SELECT  id, pubmed_id, source_json
                FROM  {self.table_name}
                WHERE (id BETWEEN {start_id} AND {end_id}) 
                AND (processed is null OR processed != \'{self.processed_flag}\')
            ''' 
             
            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()

            chunks = []            
            for row in rows: 
                _count += 1
                pubmed_id = row['pubmed_id'] 
                source_json = row['source_json']

                try:
                    source_obj = json.loads(source_json)
                except json.JSONDecodeError as e:
                    self.appender.log_stdout(f"Error decoding JSON for PubMed ID {pubmed_id}: {e}") 
                    continue
                
                '''
                  "journalInfo": {
                        "issue": "10",
                        "volume": "12",
                        "journalIssueId": 241918,
                        "dateOfPublication": "1979 Oct",
                        "monthOfPublication": 10,
                        "yearOfPublication": 1979,
                        "printPublicationDate": "1979-10-01",
                        "journal": {
                            "title": "Anales espanoles de pediatria",
                            "medlineAbbreviation": "An Esp Pediatr",
                            "isoabbreviation": "An Esp Pediatr",
                            "nlmid": "0420463",
                            "issn": "0302-4342",
                            "essn": "1577-2799"
                        }
                    }
                '''  
                journal = source_obj.get('journalInfo', {}).get('journal')
                
                if not journal:
                    continue
                
                chunks.append({
                    "pubmedId": pubmed_id, 
                    "journal": {
                        "title": journal.get('title', 'N/A'),
                        "issn": journal.get('issn', 'N/A'),
                        "essn": journal.get('essn', 'N/A'),
                        "nlmid": journal.get('nlmid', 'N/A'),
                    }
                })

            if chunks:
                try:
                     self.memgraph.execute(batch_create, {"chunks": chunks})
                except Exception as e:
                    self.appender.log_stdout(f'Exception while insert: {e}') 
                    raise

            self.update_processed_flag(start_id, end_id, self.processed_flag)

            self.appender.log_stdout(f'{_curr_timestamp()} [total: {_count}], [flag: {self.processed_flag}], Id range: {start_id} - {end_id}, #chunks = {len(chunks)}')
 
        self.close_mysql_conn()  
       
        self.appender.log_stdout(f'\n{"="*50}{_curr_timestamp()} Done! Total = {_count} {"="*50}\n')
        self.appender.close()
  
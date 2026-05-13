import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import _id_range_generator, _curr_timestamp, _date_string
from typing import Dict, Any, Optional
 

# Create MeshTerm nodes
class MeshTermInitializer(InitBase):


    def __init__(self): 

        super().__init__('publication_article', 'MeshTerm')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/3-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)

        self.create_indexes('MeshTerm', ['meshTerm']) 
  

    # Override the abstract method
    def init_nodes(self):   

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)


    # Override
    def populate_nodes(self, min_id, max_id, step=3, batch_size = 200): 

        batch_create =   '''
            UNWIND $chunks AS chunk            
            MATCH (p: Article {pubmedId: chunk.pubmedId}) 

            UNWIND chunk.meshTerms AS meshTerm
            MERGE (m: MeshTerm {meshTerm: meshTerm})
            MERGE (m)-[r: has_mesh_term]->(p)
        '''         
  
        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

        _count = 0
        for start_id, end_id in id_ranges:

            query = f'''
                SELECT  pubmed_id, source_json
                FROM  {self.table_name}
                WHERE (id BETWEEN {start_id} AND {end_id}) 
                AND (processed IS NULL OR processed != \'{self.processed_flag}\') 
            '''
            
            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()

            chunks = []
            for row in rows:

                _count += 1
                pubmed_id = row['pubmed_id'] 
                source_json = row['source_json']
                
                mesh_terms = self.get_mesh_terms_list(pubmed_id, source_json) 

                if not mesh_terms:
                    continue

                chunks.append({"pubmedId": pubmed_id, "meshTerms": mesh_terms})
               
            if chunks:
                try:
                    self.memgraph.execute(batch_create, {"chunks": chunks}) 
                except Exception as e:
                    self.appender.append_and_print(f'Exception while insert: {e}')
                    raise 

            self.update_processed_flag(start_id, end_id, self.processed_flag)
            self.appender.log_stdout(f'{_curr_timestamp()} [total: {_count}], [flag: {self.processed_flag}], Id range: {start_id} - {end_id}, #chunks = {len(chunks)}')
 
        self.close_mysql_conn() 

        self.appender.log_stdout(f'\n{"="*50}{_curr_timestamp()} Done! Total = {_count} {"="*50}\n')
        self.appender.close()  
        
 

    def get_mesh_terms_list(self, pubmed_id: int, source_json: str) -> None:
 
        try:
            source_obj: Dict[str, Any] = json.loads(source_json)

        except json.JSONDecodeError as e:
            self.appender.log_stdout(f"Error decoding JSON for PubMed ID {pubmed_id}:\n{e}")
            return []

        '''
        "meshHeadingList": {
            "meshHeading": [
                {
                    "majorTopic_YN": "N",
                    "descriptorName": "Humans"
                },
                {
                    "majorTopic_YN": "N",
                    "descriptorName": "Polyarteritis Nodosa",
                    "meshQualifierList": {
                        "meshQualifier": [
                            {
                                "abbreviation": "IM",
                                "qualifierName": "immunology",
                                "majorTopic_YN": "N"
                            }
                        ]
                    }
                },
                {
                    "majorTopic_YN": "N",
                    "descriptorName": "Lymphatic Diseases",
                    "meshQualifierList": {
                        "meshQualifier": [
                            {
                                "abbreviation": "IM",
                                "qualifierName": "immunology",
                                "majorTopic_YN": "Y"
                            }
                        ]
                    }
                },
                {
                    "majorTopic_YN": "N",
                    "descriptorName": "Child, Preschool"
                }
            ]
        }   
        '''
        # 
        mesh_heading_list = source_obj.get('meshHeadingList', {}).get('meshHeading', [])

        if not mesh_heading_list:
            return []
 

        mesh_terms_list = [] 
        
        for heading in mesh_heading_list: 
            descriptor_name: str = heading.get('descriptorName', '')

            if descriptor_name:
                mesh_terms_list.append(descriptor_name)

        return mesh_terms_list

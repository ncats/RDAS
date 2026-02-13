import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import hashlib
from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import _id_range_generator, _curr_timestamp, _date_string, _make_hash_key


#This is typically used to quickly generate column lists for INSERT or SELECT statements, especially useful when you want to copy the exact column structure without typing them manually.
#For example: 'id','pubmed_id','infons_identifier','infons_type','infons_text','relation_type','processed','created'
"""
    SELECT CONCAT('\'',  GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION SEPARATOR '\',\''), '\'') 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'rdas_db' 
    AND TABLE_NAME = 'publication_pubtator_parsed';
"""

#This SQL script retrieves all column names from a specific table and concatenates them into a single comma-separated string.
"""
    SELECT CONCAT( GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION SEPARATOR ',')) 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'rdas_db' 
    AND TABLE_NAME = 'publication_pubtator_parsed';
"""

# Create PubtatorAnnotation nodes
class PubtatorInitializer(InitBase):
  
  
    def __init__(self): 

        super().__init__('publication_pubtator_parsed', 'PubtatorAnnotation')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/3-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)
        
        self.create_indexes('PubtatorAnnotation', ['annotationIdentifier','annotationType', 'annotation', '_composite_key'])


    # Override the abstract method
    def init_nodes(self):   

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)


    # Override
    def populate_nodes(self, min_id, max_id, step=1, batch_size = 1000): 

        # 'relation_type' used for relation name
        batch_create = '''
            WITH $chunks AS chunks 
            WHERE chunks IS NOT NULL AND size(chunks) > 0

            UNWIND chunks AS chunk
            MATCH (a: Article {pubmedId: chunk.pubmedId})

            WITH a, chunk
            WHERE chunk.pubtators IS NOT NULL AND size(chunk.pubtators) > 0

            UNWIND chunk.pubtators AS pt
            
            MERGE (p: PubtatorAnnotation {_composite_key: pt._composite_key})
            ON CREATE SET
                p.annotation = pt.annotation,
                p.annotationType = pt.annotationType,
                p.annotationIdentifier = pt.annotationIdentifier,
                p.dateCreatedByRDAS = pt.dateCreatedByRDAS,
                p.lastUpdatedByRDAS = pt.lastUpdatedByRDAS
                
            MERGE (a)-[r:has_pubtator_annotation {type: pt.relation_type}]->(p)
        '''

        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

        _count = 0
        for start_id, end_id in id_ranges:

            query = f'''
                    SELECT  id, pubmed_id, infons_identifier, infons_type, infons_text, relation_type
                    FROM  {self.table_name}
                    WHERE (id BETWEEN {start_id} AND {end_id}) 
                    AND (processed IS NULL or processed !=\'{self.processed_flag}\')
                '''
            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()

            pubmedId_ann_dict = {}
            for row in rows: 

                _count += 1
                pubmed_id = row['pubmed_id']
                
                pubmedId_ann_dict[pubmed_id] = pubmedId_ann_dict.get(pubmed_id, [])

                infons_identifier = row['infons_identifier']                
                infons_identifier = '' if (not infons_identifier or infons_identifier == '-') else infons_identifier

                # relation_type as list
                relation_type = json.loads(row['relation_type']) if row['relation_type'] else []

                pubmedId_ann_dict[pubmed_id].append({
                    "annotation": row['infons_text'],
                    "annotationType": row['infons_type'],
                    "relation_type": relation_type,
                    "annotationIdentifier": infons_identifier,
                    #"lastUpdatedByRDAS": self.formatted_today,
                    #"dateCreatedByRDAS": self.formatted_today
                })
 
            # Merge annotations if the annotationIdentifier, annotationType and relation_type are same.
            pubmedId_ann_dict = self.merge_annotations(pubmedId_ann_dict)

            # Convert to list of dicts for Cypher
            chunks = [{"pubmedId": k, "pubtators": v} for k, v in pubmedId_ann_dict.items()]

            if chunks:
                try:
                    self.memgraph.execute(batch_create, {"chunks": chunks})   
                except Exception as e:
                    self.appender.append_and_print(f'Error: {e}')

            self.update_processed_flag(start_id, end_id, self.processed_flag)
 
            self.appender.log_stdout(f'{_curr_timestamp()} {_count}],[flag: {self.processed_flag}]  Id range: [{start_id} - {end_id}] #PubtatorAnnotation = {len(chunks)}')
            
        self.close_mysql_conn() 
        
        self.appender.log_stdout(f'\n{"="*50}{_curr_timestamp()} Done! Total = {_count} {"="*50}\n')
        self.appender.close() 
 


    #Verifiy the 'annotation' list -----------------------------------------------------------------------------------------
    '''
        SELECT p.*
        FROM rdas_db.publication_pubtator_parsed p
        INNER JOIN (
            SELECT pubmed_id, infons_identifier
            FROM rdas_db.publication_pubtator_parsed
            GROUP BY pubmed_id, infons_identifier
            HAVING COUNT(*) >= 3
        ) grouped
        ON p.pubmed_id = grouped.pubmed_id 
        AND p.infons_identifier = grouped.infons_identifier
        ORDER BY p.pubmed_id, p.infons_identifier, p.infons_type, p.infons_text, p.relation_type
        LIMIT 0, 50;
    '''
    '''
        MATCH (x: PubtatorAnnotation) where x.annotationIdentifier='' return x;
    '''
    def merge_annotations(self, val_dict):
        '''
        pubmed_id, infons_identifier, infons_type, infons_text, relation_type,
        22,  MESH:C006646, Chemical, BA 1, ["abstract", "title"]
        22,  MESH:C006646, Chemical, N-(2-cyanoethylene)-urea, ["abstract", "title"]
        22,  MESH:D002296, Disease, carcinosarcoma, ["abstract", "title"] 
        22,  MESH:D007069, Chemical, IF, ["abstract", "content"]
        22,  MESH:D007069, Chemical, ifosfamide, ["abstract", "content"]
        22,  MESH:D007069, Disease, tumor, ["title"]
        '''
        # result_dict example
        '''
        result_dict = {
            "22": [
                {
                    "annotationIdentifier": "MESH:C006646",
                    "annotation": ["BA 1"," N-(2-cyanoethylene)-urea"], (### list of annotations)
                    "annotationType": "Chemical",
                    "relation_type": ["abstract", "title"]                  
                },{   
                    "annotationIdentifier": "MESH:D002296",
                    "annotation": ["carcinosarcoma"],
                    "annotationType": "Disease",
                    "relation_type": ["abstract", "title"]
                },{
                    "annotationIdentifier": "MESH:D007069",
                    "annotation": ["IF", "ifosfamide"],   (### list of annotations)
                    "annotationType": "Chemical",
                    "relation_type": ["abstract", "content"]  
                },{
                    "annotationIdentifier": "MESH:D007069",
                    "annotation": ["tumor"],
                    "annotationType": "Disease",
                    "relation_type": ["title"]      
                }
            ]
        }
        '''

        result_dict = {}

        for pubmed_id, items in val_dict.items():
            temp_obj = {}
            
            for item in items:
                # Convert relation_type list to a tuple for hashing
                relation_type_tuple = tuple(sorted(item['relation_type']))
                
                # Create composite key from annotationIdentifier, annotationType, and relation_type
                compose_key = (item['annotationIdentifier'], item['annotationType'], relation_type_tuple)
                
                if compose_key not in temp_obj:
                    temp_obj[compose_key] = set()
                
                temp_obj[compose_key].add(item['annotation'])

            
            result_dict[pubmed_id] = []
            
            for compose_key, annotations in temp_obj.items():
                annotationIdentifier, annotationType, relation_type_tuple = compose_key
                
                # Create composite key from annotationIdentifier + annotations + relation_type
                annotations_tuple = tuple(sorted(annotations))
                
                # Create string representation and replace whitespaces with underscores
                composite_key_str = f"{annotationIdentifier}_{'_'.join(sorted(annotations))}_{'_'.join(relation_type_tuple)}"
                composite_key_str = "_".join(composite_key_str.split())  # Replace multiple whitespaces with single underscore
                
                # Hash the composite key
                _composite_key = _make_hash_key(composite_key_str)
                
                result_dict[pubmed_id].append({
                    "annotationIdentifier": annotationIdentifier,
                    "annotation": list(annotations),
                    "annotationType": annotationType,
                    "relation_type": list(relation_type_tuple),
                    "_composite_key": _composite_key,  # Hashed composite key
                    "lastUpdatedByRDAS": self.formatted_today,
                    "dateCreatedByRDAS": self.formatted_today
                })
        
        return result_dict
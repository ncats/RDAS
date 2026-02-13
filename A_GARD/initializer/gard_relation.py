import os
import sys
import csv
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
 
from utils.file_appender import FileAppender
from utils.tools import  _date_string, read_csv_as_dict, _val

from baseclass.init_base import InitBase 

"""
    1. GARD to GARD relationship
    2. See the previous version: 1_GARD/init_4_GARD_step_4.py
"""

class GARDRelationInitializer(InitBase):


    def __init__(self): 

        super().__init__(None, '')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/1-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)

        # 1_GARD/data/GARD_classification.csv
        self.DATA_DIR = os.path.join(os.path.join(os.path.dirname(__file__), '..'), 'data')
        self.data_file_path = f'{self.DATA_DIR}/GARD_classification.csv' 

        self.create_indexes('GARD', ['gardId'])


    def init_nodes(self, min_id=0, max_id=0, step=0, batch_size=200):    

        #1. Batch delete all the subclass_of relationships
        """
            MATCH (g1:GARD)-[r:subclass_of]->(g2:GARD)
            WITH r LIMIT 10000
            DELETE r
            RETURN count(r) as deleted_relationships
        """

        #2. --- Test ---

        #2.1 Find parents by gardId
        """
        MATCH (g:GARD {gardId: 'GARD:0000023'})-[rel:subclass_of]->(parent:GARD)
        RETURN g, rel, parent
        """

        #2.2 Find children by gardId
        """
        MATCH (g:GARD {gardId: 'GARD:0000023'})<-[r:subclass_of]-(child:GARD)
        RETURN g, r,child
        """

        query = '''
            UNWIND $chunks AS chunk 
             
            WITH chunk
            WHERE chunk.hasParent = true
            MATCH (x:GARD {gardId: chunk.parent})
            MATCH (y:GARD {gardId: chunk.current})
            MERGE (x)<-[:subclass_of]-(y)

            WITH chunk
            WHERE chunk.hasChild = true
            MATCH (p:GARD {gardId: chunk.current})
            MATCH (c:GARD {gardId: chunk.child})
            MERGE (c)-[:subclass_of]->(p)
        '''

        chunks = self.get_data_from_csv_files()

        # Batch upload by batch_size
        for start_idx in range(0, len(chunks), batch_size):

            end_idx = start_idx + batch_size
            batch_chunks = chunks[start_idx:end_idx]

            try:
                self.memgraph.execute(query, {'chunks': batch_chunks})

                self.appender.log_stdout(f'Uploaded chunks from {start_idx} to {end_idx}')
            except Exception as e:
                self.appender.log_stdout(e)

        self.appender.log_stdout('='*30 + ' Done ' + '='*30)



    def get_data_from_csv_files(self):

        rows = read_csv_as_dict(self.data_file_path)

        chunks = []
        for row in rows:
            current = row['GardID']
            parent = _val(row['Parent'])
            child = _val(row['Child'])

            chunks.append({
                'current': current,
                'parent': parent,
                'child': child,
                'hasParent': True if parent else False,
                'hasChild': True if child else False
            })

        self.appender.log_stdout(f'\n=== Total processed GARD reationships chunks: {len(chunks)} ===\n')

        return chunks
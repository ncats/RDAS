import os
import sys
import csv
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
 
from utils.file_appender import FileAppender
from utils.tools import  _date_string, read_csv_as_dict, _split_str, _val

from baseclass.init_base import InitBase
"""
    1. Upload Phenotype nodes
    2. Create has_phenotype relationship from GARD to Phenotype
    3. See the previous version: 1_GARD/init_4_GARD_step_4.py
"""

class PhenotypeInitializer(InitBase):


    def __init__(self): 

        super().__init__(None, '')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/1-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)

        # 1_GARD/data/2025/GARD_Disease_To_Phenotype_reformated-20250225.csv
        self.DATA_DIR = os.path.join(os.path.join(os.path.dirname(__file__), '..'), 'data/2025')
        self.data_file_path = f'{self.DATA_DIR}/GARD_Disease_To_Phenotype_reformated-20250225.csv' 

        self.create_indexes('GARD', ['gardId'])
        self.create_indexes('Phenotype', ['hpoId'])


    def init_nodes(self):    

        batch_size=200
         
        query = '''
            UNWIND $chunks AS chunk 
            MATCH (g:GARD {gardId: chunk.gardId})
            WITH g, chunk
            MERGE (d:Phenotype {hpoId: chunk.hpoId})
            ON CREATE SET d.hpoTerm = chunk.hpoTerm

            MERGE (g)-[r:has_phenotype]->(d)
            ON CREATE SET                 
                r.evidence = chunk.evidence,
                r.references = chunk.references,
                r.hpoTermFrequency = chunk.hpoTermFrequency
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
        self.appender.close()



    def get_data_from_csv_files(self):

        rows = read_csv_as_dict(self.data_file_path)
 
        chunks = []
        row_idx = 0
        for row in rows:  
            row_idx += 1

            try:
                # Be aware of the separator
                refs_str = row['References'].strip('"')
                if refs_str:
                    references = refs_str.split(',')
                else:
                    references = []             
            except Exception as e:
                references = []
                self.appender.log_stdout(f'The row index: {row_idx} has no references.')
        
            chunks.append({
                'gardId': row['GardID'],
                'hpoId': _val(row['HPO_ID']),
                'hpoTerm': _val(row['HPO_Term']),
                'hpoTermFrequency': _val(row['HPOTerm_Frequency']),
                'evidence': _val(row['Evidence']),
                'references': references
            })
        
        self.appender.log_stdout(f'\n=== Total processed Phenotype chunks: {len(chunks)} ===\n')

        return chunks
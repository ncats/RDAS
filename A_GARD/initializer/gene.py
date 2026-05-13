import os
import sys
import csv
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
 
from utils.file_appender import FileAppender
from utils.tools import  _date_string, read_csv_as_dict, _val

from baseclass.init_base import InitBase 

"""
    1. Upload Gene nodes
    2. Create has_associated_gene relationship from GARD to Gene
    3. See the previous version: 1_GARD/init_4_GARD_step_4.py
"""

class GeneInitializer(InitBase):


    def __init__(self): 

        super().__init__(None, '')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/1-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)

        # 1_GARD/data/2025/GARD-Disease-to-Gene-Associations_reformatted_20250224.csv
        self.DATA_DIR = os.path.join(os.path.join(os.path.dirname(__file__), '..'), 'data/2025')
        self.data_file_path = f'{self.DATA_DIR}/GARD-Disease-to-Gene-Associations_reformatted_20250224.csv' 

        self.create_indexes('GARD', ['gardId'])


    def init_nodes(self):    
        
        batch_size=100

        query = '''
            UNWIND $chunks AS chunk 
            MATCH (g:GARD {gardId: chunk.gardId})
            MERGE (d:Gene {geneIdentifier: chunk.geneIdentifier})
            ON CREATE SET
                d.geneSymbol = chunk.geneSymbol,
                d.geneSynonyms = chunk.geneSynonyms,
                d.geneTitle = chunk.geneTitle,
                d.geneType = chunk.geneType,
                d.geneUrl = chunk.geneUrl,
                d.locus = chunk.locus,
                d.locusGroup = chunk.locusGroup,
                d.medGen = chunk.medGen,
                d.mondo = chunk.mondo,
                d.omim = chunk.omim,
                d.orphanet = chunk.orphanet,
                d.umls = chunk.umls
             
            MERGE (g)-[r:has_associated_gene]->(d)
            ON CREATE SET
                r.Reference = chunk.reference
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

        count = 0
        chunks = []
        row_idx = 0
        for row in rows: 
            row_idx += 1
            
            try:
                # Be aware of the separator
                #synonyms = gene['GeneSynonym'].strip('][').split(',')
                synonyms = [syn.strip() for syn in row['GeneSynonym'].strip('][').split(',')]
            except Exception as e:
                synonyms = [] 
                self.appender.log_stdout(f"*** row_idx: {row_idx} *** {row['GardID']}\t{row['GeneSymbol']}\tGeneSynonym is empty") 

            try:
                # Be aware of the separator
                reference = row['Reference'].strip('][').split(';')
            except Exception as e:
                reference = [] 
                self.appender.log_stdout(f"*** row_idx: {row_idx} *** {row['GardID']}\t{row['GeneSymbol']}\t Reference is empty") 

            # Add to chunks
            chunks.append({
                'gardId': row['GardID'],
                'geneIdentifier': row['GeneIdentifier'],      
                'geneSymbol': _val(row['GeneSymbol']),
                'geneSynonyms': synonyms,          
                'geneTitle': _val(row['GeneTitle']),
                'geneType': _val(row['GeneType']),
                'geneUrl': _val(row['gene_url']),
                'locus': _val(row['Locus']),
                'locusGroup': _val(row['locus_group']),
                'medGen': _val(row['Equivalent_MedGen']),
                'mondo': _val(row['Equivalent_MONDO']),
                'omim': _val(row['OMIM']),
                'orphanet': _val(row['Equivalent_Orphanet']),
                'reference': reference,
                'umls': _val(row['Equivalent_UMLS'])
            })

        self.appender.log_stdout(f'\n=== Total processed Gene chunks: {len(chunks)} ===\n')

        return chunks

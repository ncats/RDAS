import os
import sys
import csv
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
 
from utils.file_appender import FileAppender
from utils.tools import  _date_string, _split_str, _val

from baseclass.init_base import InitBase

'''
    1. Merge Xrefs into the GARD nodes
    2. See the previous version: 1_GARD/init_3_GARD_step_3.py

    3. *** Change the self.data_file_path with new data file ***
'''

class XrefInitializer(InitBase):


    def __init__(self): 

        super().__init__(None, '')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/1-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)

        # 1_GARD/data/2025/GARD_Disease_Xref_pivoted-20250214.csv
        self.DATA_DIR = os.path.join(os.path.join(os.path.dirname(__file__), '..'), 'data/2025')
        self.data_file_path = f'{self.DATA_DIR}/GARD_Disease_Xref_pivoted-20250214.csv' 

        self.create_indexes('GARD', ['gardId'])


    def init_nodes(self):

        batch_size=100

        query = '''
            UNWIND $chunks AS chunk
            MERGE (x:GARD {gardId: chunk.gardId})
            SET
                x.diseaseType = chunk.diseaseType,
                x.mondo = chunk.mondo,
                x.orphanet = chunk.orphanet,
                x.omim = chunk.omim,
                x.omimps = chunk.omimps,
                x.medGen = chunk.medGen,
                x.umls = chunk.umls,
                x.mesh = chunk.mesh,
                x.nctid = chunk.nctid,
                x.sctid = chunk.sctid,
                x.doid = chunk.doid,
                x.idc10cm = chunk.idc10cm
        ''' 

        try:
            with open(self.data_file_path, 'r', encoding='utf-8-sig') as csv_file:
                    
                    reader = csv.DictReader(csv_file)                
                    # Print data file field names
                    self.appender.log_stdout(f"CSV field names:{', '.join(reader.fieldnames)}")
                    
                    count = 0
                    chunks = []

                    # Process each row
                    for row in reader:                        
                        try:      
                            gardId = row['GardID_RDIPv2']

                            chunks.append({
                                "gardId": gardId,
                                "diseaseType": _val(row['Type']),
                                "mondo": _val(row['Equivalent_MONDO']),
                                "orphanet": _val(row['Equivalent_Orphanet']),
                                "omim": _val(row['Equivalent_OMIM']),
                                "omimps": _val(row['Equivalent_OMIMPS']),
                                "medGen": _val(row['Equivalent_MedGen']),
                                "umls": _val(row['Equivalent_UMLS']),
                                "mesh": _split_str(row['ExactMatch_MESH']),
                                "nctid": _split_str(row['ExactMatch_NCIT']),
                                "sctid": _split_str(row['ExactMatch_SCTID']),
                                "doid": _split_str(row['ExactMatch_DOID']),
                                "idc10cm": _split_str(row['ExactMatch_ICD10CM'])
                            })

                            count += 1
                            print(f'Count: {count}\t{gardId}')

                            if count % batch_size == 0:
                                self.memgraph.execute(query, {'chunks': chunks})
                                
                                chunks = []
                                self.appender.log_stdout(f'Updated GARD nodes count: {count}')

                        except KeyError as ke:
                            self.appender.log_stdout(f"KeyError in row: {row}")
                            self.appender.log_stdout(f"Missing key: {str(ke)}")
                            continue

                    # The remains of chunks
                    try:
                         self.memgraph.execute(query, {'chunks': chunks})
                         self.appender.log_stdout(f'\nFinal total updated GARD nodes count: {count + len(chunks)}\n')

                    except Exception as e:
                        print(e)

        except FileNotFoundError:
            self.appender.log_stdout(f"Error: File '{self.data_file_path}' not found.") 

        except Exception as e:
            self.appender.log_stdout(f"Error reading file: {str(e)}")


        self.appender.close()
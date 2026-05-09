import os
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
from baseclass.init_base import InitBase
from utils.tools import  _clean, _id_range_generator
from utils.file_appender import FileAppender

'''
    Deprecated:

    Just use reference as the edge name linking the trial node to the publication node

'''
class ReferenceInitializer(InitBase):

    def __init__(self): 

        super().__init__('clinical_trial_unique', 'Reference')

        self.log_file = f'{self.log_dir}/2-ReferenceInitializer.log'
        self.appender = FileAppender(self.log_file)  


    def populate_nodes(self, min_id, max_id, step=3, batch_size=200):

        batch_create = '''
            UNWIND $chunks AS chunk
            MATCH (ct:ClinicalTrial {NCTId: chunk.nctid}) 
            CREATE(r:Reference)
            SET
                r.citation = chunk.citation,
                r.referencePMID = chunk.pmid,
                r.referenceType = chunk.type

            MERGE (ct)<-[:is_about]-(r)
        '''               
        
        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

        total  = 0
        for start_id, end_id in id_ranges:

            query = f'''
                SELECT id, nctid, studies  FROM {self.table_name}
                WHERE nctid IS NOT NULL AND id BETWEEN {start_id} AND {end_id} ORDER BY id
            '''

            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()

            chunks = [] 

            for row in rows:
                total += 1
                nctid = row['nctid'] 
                study = json.loads(row['studies'])
  
                ref_module = study.get('protocolSection', dict()).get('referencesModule', {}) 
                references = ref_module.get('references', [])

                if not references:
                    continue

                for ref in references: 
                    chunks.append(
                        {   
                            "nctid": nctid,
                            "citation": _clean(ref.get('citation','')),
                            "pmid":  _clean(ref.get('pmid','')),
                            "type": _clean(ref.get('type',''))
                        }
                    ) 

            if chunks: 
                try:
                    self.memgraph.execute(batch_create, {"chunks": chunks}) 
                except  Exception as e:
                    self.appender.append_and_print(f"Error executing batch create: {e}")
                    raise

            self.appender.append_and_print(f'ReferenceInitializer:: [total: {total}], Id range: [{start_id} - {end_id}], #Reference = {len(chunks)}')


        self._close_conn()
        self.appender.append_and_print(f'ReferenceInitializer:: Done total = {total}')
        self.appender.close()

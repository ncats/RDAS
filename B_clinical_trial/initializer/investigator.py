import os
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import hashlib
from baseclass.init_base import InitBase
from utils.tools import  _clean, _id_range_generator
'''

    Deprecated
    
'''
'''
    The InvestigatorInitializer must be later than ContactInitializer, since the Investigator need create a relation with Contact
'''
class InvestigatorInitializer(InitBase):

    def __init__(self): 

        super().__init__('clinical_trial_unique','Investigator')
 
        if not self._is_index_field_exists('Contact', 'contactName'):
            self._create_index('Contact', 'contactName')

        self.create_indexes('Investigator', ['id','officialName', 'officialAffiliation', 'officialRole']) 
         


    def populate_nodes(self, min_id, max_id, step=3, batch_size=200):
        
        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

        total  = 0
        for start_id, end_id in id_ranges:

            query = f'''
                SELECT id, nctid, studies  FROM {self.table_name}
                WHERE nctid IS NOT NULL AND id BETWEEN {start_id} AND {end_id} ORDER BY id
            '''

            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()

            batch_chunks = [] 

            for row in rows:
                total += 1
                nctid = row['nctid'] 
                study = json.loads(row['studies'])
  
                contact_module = study.get('protocolSection', {}).get('contactsLocationsModule', {})                 
                officials = contact_module.get('overallOfficials', [])

                if not officials:
                    continue

                for official in officials:
                    name = _clean(official.get('name',''))
                    role = _clean(official.get('role',''))
                    affiliation = _clean(official.get('affiliation',''))

                    combined = (name+'|'+affiliation+'|'+role).encode()
                    id = hashlib.md5(combined).hexdigest()[:16]

                    batch_chunks.append({"nctid": nctid, "name": name, "role": role, "affiliation": affiliation, "id": id})


            batch_create = '''
                UNWIND $batch_chunks AS chunk
                MATCH (x:ClinicalTrial {NCTId: chunk.nctid})
                
                MERGE (y:Investigator {id: chunk.id})
                ON CREATE SET 
                    y.officialName = chunk.name,
                    y.officialAffiliation = chunk.affiliation,
                    y.officialRole = chunk.role
                
                MERGE (x)<-[:investigates]-(y)

                WITH y, chunk
                OPTIONAL MATCH (c:Contact {contactName: chunk.name})

                WITH y, c
                WHERE c IS NOT NULL
                MERGE (c)<-[:has_contact]-(y)
            '''              

            self.memgraph.execute(batch_create, {"batch_chunks": batch_chunks}) 

            print(f'InvestigatorInitializer:: [total: {total}], Id range: [{start_id} - {end_id}], #Investigator = {len(batch_chunks)}')


        self._close_conn()

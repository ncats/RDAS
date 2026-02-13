import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import  _id_range_generator, _curr_timestamp, _date_string, _make_hash_key, _remove_parentheses
 
# Create Organization & Location nodes 
''' 
The Organization and Location nodes only have relationships with ClinicalTrial in this process 
'''

#
# Also see: rdas-memgraph/D_grant/initializer/funding_IC.py
#
class OrganizationLocationInitializer(InitBase): 


    def __init__(self): 

        super().__init__('clinical_trial_unique', 'Organization-Location')
        
        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/2-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)

        self.create_indexes('Organization', ['ror_id','name', '_idx_key', 'ror_id'])   
        self.create_indexes('Location', ['facility', '_idx_key'])   
     
        
    # Override the abstract method
    def init_nodes(self):    

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)


    # Overwrite
    def populate_nodes(self, min_id, max_id, step=1, batch_size=300):

        batch_create = '''
            UNWIND $chunks AS chunk
            MATCH (ct: ClinicalTrial {nctId: chunk.nctId}) 

            MERGE (org: Organization {_idx_key: chunk.org_idx_key})
            ON CREATE SET 
                org.name = chunk.org_name,
                org.displayName = '',
                org.ror_id = '',
                org.website = '',
                org.types = []

            MERGE (ct)-[:associated_with]->(org)  

            WITH chunk, ct
            UNWIND chunk.locations AS location
            MERGE (loc: Location {_idx_key: location.loc_idx_key})
            ON CREATE SET 
                loc.facility = location.facility,
                loc.address = location.address,
                loc.city = location.city,
                loc.state = location.state,
                loc.country = location.country,
                loc.zip = location.zip,
                loc.countryCode = ''
 
            MERGE (ct)-[:has_trial_location]->(loc)
        '''

        # Change relation name from in_location to has_trial_location
        """
        MATCH (a: ClinicalTrial)-[r1:in_location]->(b: Location)
        CREATE (x: ClinicalTrial)-[r2:has_trial_location]->(y: Location)
        SET r2 = properties(r1)
        DELETE r1
        """
        # See JSON data example: nctId = 'NCT00000126'

        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

        total  = 0
        for start_id, end_id in id_ranges:

            query = f'''
                SELECT id, nctid, studies  FROM {self.table_name}
                WHERE nctid IS NOT NULL 
                AND id BETWEEN {start_id} AND {end_id}
                AND (processed IS NULL OR processed != '{self.processed_flag}')
            '''

            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()

            chunks = [] 
            for row in rows:
                
                nctid = row['nctid'] 
                study = json.loads(row['studies'])
                 
                if not study:
                    continue
                
                protocol = study.get('protocolSection', {})
                locations = protocol.get('contactsLocationsModule', {}).get('locations', [])
                organization = protocol.get('identificationModule', {}).get('organization', {}).get('fullName')                

                if not organization and not locations:
                    continue

                ''' Generate the Organization index key '''
                # "National Eye Institute (NEI)" -> "National Eye Institute"
                # "National Heart, Lung, and Blood Institute (NHLBI)" -> "National Heart, Lung, and Blood Institute"
                organization = _remove_parentheses(organization)
                org_idx_key = _make_hash_key(organization)

                locs = []
                for loc in locations:

                    loc_idx_key = None
                    facility = loc.get('facility', None)
                    if facility:
                        loc_idx_key = _make_hash_key(facility)

                    # geoPoint as index key is more precise than the facility
                    geo_point = loc.get('geoPoint', None)
                    if geo_point:
                        loc_idx_key = _make_hash_key(str(geo_point.get('lat')) + '' + str(geo_point.get('lon')))
                    
                    # No facility and geoPoint
                    if not loc_idx_key:
                        continue

                    locs.append(
                        {   
                            "nctId": nctid,
                            "facility": facility,
                            "address": "",
                            "city": loc.get('city', ''),
                            "state": loc.get('state', ''),
                            "country": loc.get('country', ''),
                            "zip": loc.get('zip', ''), 
                            "loc_idx_key": loc_idx_key,
                            "org_idx_key": org_idx_key                            
                        }
                    )

                chunks.append(
                    {
                        "nctId": nctid,
                        "org_idx_key": org_idx_key,
                        "org_name": organization,
                        "locations": locs
                    }
                )
  
            if chunks:             
                try:
                    self.memgraph.execute(batch_create, {"chunks": chunks}) 
                    total += len(chunks)

                except Exception as e:
                    self.appender.log_stdout(f"Error executing batch create: {e}")
                    raise

            self.update_processed_flag(start_id, end_id, self.processed_flag)

            numLocations = sum(len(chunk['locations']) for chunk in chunks)
            self.appender.log_stdout(f'{_curr_timestamp()} [total: {total}], Id range: [{start_id} - {end_id}], #Organizations/#Locations: {len(chunks)}/{numLocations}') 


        self.close_mysql_conn()   

        self.appender.log_stdout(f'\n{_curr_timestamp()} {"="*50} Done! Total = {total} {"="*50}\n\n')
        self.appender.close()





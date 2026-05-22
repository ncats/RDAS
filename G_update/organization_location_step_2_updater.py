import os
import sys

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, '..')),
])
 
from colorama import init, Fore, Style
init()

import json
from utils.file_appender import FileAppender
from baseclass.init_base import InitBase
from utils.tools import _date_string, _clean, _make_hash_key, _curr_timestamp, ask_to_continue


'''
Pipeline Steps:
 Step 1: Run B_clinical_trial/initializer/organization_location.py
 Step 2: Run E_followup/updater/1_organization_location_finder.py to fetch ROR data
 Step 3: Run this script to sync MySQL data back to graph database
'''

class OrganizationLocationUpdater(InitBase):
    """
    Updates Organization nodes in Memgraph with location data from MySQL.
    Syncs ROR data (fetched by organization_location_finder.py) back to graph DB.
    """

    def __init__(self):
        super().__init__('organization_location', 'Organization')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/G-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)


    def organization_in_graph_db_generator(self, batch_size=300):
        """
        Memory-efficient generator that yields organizations needing updates.
        Only fetches organizations with empty ror_id (not yet updated).
        
        Args:
            batch_size: Number of organizations per batch
            
        Yields:
            Tuple of (batch_index, list of org dicts with 'name' and '_idx_key')
        """
        query = """
            MATCH (o:Organization)  
            WHERE o.ror_id = ''
            RETURN o.name AS name, o._idx_key AS _idx_key
            SKIP $skip LIMIT $limit
        """

        skip = 0
        batch_index = 1

        while True:
            results = self.memgraph.execute_and_fetch(query, {"skip": skip, "limit": batch_size})
            batch = [row for row in results]

            if not batch:
                break

            yield batch_index, batch

            skip += batch_size
            batch_index += 1



    def get_organization_from_db(self, _idx_key_list):
        """
        Fetch organization records from MySQL by idx_keys.
        
        Note: 'original_name_in_graph_db_idx_key' in MySQL corresponds to '_idx_key' in Organization nodes.
        
        Args:
            _idx_key_list: List of organization idx_keys to fetch
            
        Returns:
            List of organization dicts from organization_location table
        """
        if not _idx_key_list:
            return []

        placeholders = ",".join(["%s"] * len(_idx_key_list))
        
        query = f"""
            SELECT *
            FROM {self.table_name}
            WHERE original_name_in_graph_db_idx_key IN ({placeholders})
        """

        cursor = self.mysql.cursor(buffered=True, dictionary=True)
        try:
            cursor.execute(query, tuple(_idx_key_list))
            return cursor.fetchall()
        finally:
            cursor.close()
    


    def init_nodes(self):
        """Override abstract method from InitBase."""
        self.update()



    def update_organizations(self, batch_num, batch_idx_keys):
        """
        Update Organization nodes in graph DB with location data from MySQL.
        Creates Location nodes and relationships for organizations with valid data.
        Sets ror_id='N/A' for organizations without location data.
        
        Args:
            batch_num: Current batch number for logging
            batch_idx_keys: List of organization idx_keys to update
            
        Returns:
            Number of organizations updated, or 0 if none found
        """
        # Cypher query handles two cases:
        # 1. has_location=true: Update org props and create/link Location node
        # 2. has_location=false: Set ror_id='N/A' only
        batch_cypher_script = '''
            UNWIND $chunks AS chunk
            
            MERGE (org:Organization {_idx_key: chunk.org_idx_key})
            
            FOREACH (_ IN CASE WHEN chunk.has_location THEN [1] ELSE [] END |
                SET org.displayName = chunk.display_name,
                    org.ror_id = chunk.ror_id,
                    org.types = chunk.types,
                    org.website = chunk.website
                    
                MERGE (loc:Location {_idx_key: chunk.loc_idx_key})
                ON CREATE SET 
                    loc.facility = '',
                    loc.address = '',
                    loc.zip = '',
                    loc.city = chunk.city,
                    loc.state = chunk.state,
                    loc.country = chunk.country,
                    loc.countryCode = chunk.country_code
                    
                MERGE (org)-[:has_location]->(loc)
            )
            
            FOREACH (_ IN CASE WHEN NOT chunk.has_location THEN [1] ELSE [] END |
                SET org.ror_id = 'N/A'
            )
        '''

        # Fetch organization data from MySQL
        orgs = self.get_organization_from_db(batch_idx_keys)

        if not orgs:            
            return 0
                    
        # Build update chunks for Cypher query
        chunks = []
        with_loc_count = 0
        without_loc_count = 0
        
        for org in orgs: 
            display_name = _clean(org.get('org_name', ''))
            org_idx_key = org['original_name_in_graph_db_idx_key']

            # Organization not found in ROR API (no data saved)
            if not display_name:
                without_loc_count += 1
                chunks.append({
                    'org_idx_key': org_idx_key, 
                    'has_location': False
                })
            else:
                # Organization found in ROR - has location data
                lat = float(org['lat']) if org.get('lat') else None
                lng = float(org['lng']) if org.get('lng') else None

                # Parse types from JSON string to list
                types_raw = org.get('types', '[]')
                try:
                    types = json.loads(types_raw) if isinstance(types_raw, str) else types_raw
                except json.JSONDecodeError:
                    types = []
                
                # Generate location key: prefer lat/lng hash, fallback to name hash
                if lat and lng:
                    loc_idx_key = _make_hash_key(f"{lat}{lng}")
                else:
                    loc_idx_key = _make_hash_key(display_name)
                
                chunks.append({
                    'display_name': display_name,
                    'ror_id': _clean(org.get('ror_id', 'N/A')),
                    'types': types,  # A proper Python list
                    'website': _clean(org.get('website', '')),
                    'city': _clean(org.get('city', '')),
                    'country': _clean(org.get('country', '')),
                    'country_code': _clean(org.get('country_code', '')),
                    'state': _clean(org.get('state', '')),
                    'loc_idx_key': loc_idx_key,
                    'org_idx_key': org_idx_key,
                    'has_location': True
                })
                with_loc_count += 1

        # Execute batch update
        if chunks: 
            try:                        
                self.memgraph.execute(batch_cypher_script, {"chunks": chunks})
                self.appender.log_stdout(
                    f'\nBatch {batch_num}: Found in MySQL: {len(orgs)} | '
                    f'With location: {Fore.GREEN}{with_loc_count}{Style.RESET_ALL} | '
                    f'Without location: {Fore.RED}{without_loc_count}{Style.RESET_ALL}'
                )
                return len(chunks)
            except Exception as e:  
                self.appender.log_stdout(
                    f'{Fore.RED}Graph DB update failed in batch {batch_num}: {e}{Style.RESET_ALL}'
                )
                raise


    def update(self):
        """
        Main update loop:
        1. Fetch organizations with empty ror_id from graph DB in batches
        2. Retrieve their location data from MySQL
        3. Update graph DB with ROR data and create Location nodes
        4. Mark organizations without data as ror_id='N/A'
        """
        # Get organizations needing updates from graph DB
        generator = self.organization_in_graph_db_generator(batch_size=200)

        total_updated = 0

        for batch_index, batch in generator:
            if not batch:
                self.appender.log_stdout(
                    f'{Fore.YELLOW}Batch {batch_index}: Empty batch, skipping{Style.RESET_ALL}'
                )
                continue

            # Extract idx_keys from batch
            batch_idx_keys = [org["_idx_key"] for org in batch]
            
            # Update organizations using MySQL data
            count = self.update_organizations(batch_index, batch_idx_keys)

            if not count:
                self.appender.log_stdout(
                    f'{Fore.RED}Batch {batch_index}: No matching records found in '
                    f'organization_location table{Style.RESET_ALL}'
                )
                continue

            total_updated += count
            self.appender.log_stdout(
                f'{"-"*3} Progress: {Fore.BLUE}{total_updated}{Style.RESET_ALL} '
                f'organizations updated so far'
            )

        # Cleanup
        self.close_mysql_conn()

        self.appender.log_stdout(
            f'\n{_curr_timestamp()} {"="*30} Complete! '
            f'Total organizations updated: {total_updated} {"="*30}\n\n'
        )
        self.appender.close()



if __name__ == "__main__":

    # Pre-flight checks
    prompts = [
        'Did you update the .env and clean up the indexes on the Memgraph database?',
        'Did you change the stage value in .env? [ DEV/TEST/PROD ]',
        'Did you comment out the initializers that do not need to be processed again?'
    ]
    
    for prompt in prompts:
        if not ask_to_continue(f'*** {prompt} ***'):
            sys.exit('------Stopped------')

    # Run updater
    updater = OrganizationLocationUpdater()
    updater.update()

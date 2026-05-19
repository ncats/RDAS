import os
import sys

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, '..')),
])

from colorama import init, Fore, Style
init()
  
import re
import json
import time
import requests
from multiprocessing import Pool
import multiprocessing
from baseclass.init_base import InitBase
from utils.file_appender import FileAppender
from utils.tools import _date_string, _time_hms, _time_day_hms, _curr_timestamp

'''
Pipeline Steps:
 Step 1: Run B_clinical_trial/initializer/organization_location.py
 Step 2: Run D_grant/initializer/funding_IC.py
 Step 3: Run this script to fetch ROR data and populate MySQL
 Step 4: Run G_update/organization_location_step_2_updater.py to sync back to graph DB
'''

# STANDALONE FUNCTIONS - Must be at module level for multiprocessing compatibility
# Consider to change to ThreadPoolExecutor, see example: G_update/article_EPI_NHS_updater_multithread.py

def _clean_org_name(text):
    """
    Sanitize organization name by removing emails and unprintable characters.
    
    Args:
        text: Raw organization name string
        
    Returns:
        Cleaned organization name
    """
    if not text:
        return text
    
    # Remove email addresses
    text = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', '', text)
    
    # Remove unprintable characters (keep only printable ASCII + common unicode)
    text = re.sub(r'[^\x20-\x7E\u00A0-\uFFFF]', '', text)
    
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()



def lookup_ror_static(org_name, _idx_key, timeout=20):
    """
    Query the ROR (Research Organization Registry) API for organization details.
    
    Args:
        org_name: Name of the organization to search
        _idx_key: Unique identifier for the organization in graph DB
        timeout: HTTP request timeout in seconds
        
    Returns:
        Tuple of organization details if found, None otherwise:
        (ror_id, original_name, display_name, geonames_id, established, 
         status, types, website, city, country, country_code, state, 
         lat, lng, full_data, _idx_key)
    """
    cleaned_org_name = _clean_org_name(org_name)

    try:
        url = f"https://api.ror.org/v2/organizations?affiliation={cleaned_org_name}"
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        data = r.json()

        # Check if API returned valid results
        if data['number_of_results'] == 0 or not data['items'][0].get('chosen'):
            return None
            
        first_item = data['items'][0]
        org = first_item['organization']

        # Extract core organization data
        ror_id = org['id']
        names = org['names']
        org_display_name = next(
            (name['value'] for name in names if 'ror_display' in name.get('types', [])), 
            None
        )

        # Extract location data
        locations = org.get("locations") or []
        location_details = locations[0].get("geonames_details") if locations else {}
        geonames_id = locations[0].get("geonames_id") if locations else None

        # Extract geographic coordinates
        lat = location_details.get("lat") if location_details else None
        lng = location_details.get("lng") if location_details else None

        # Prepare database fields
        original_name_in_graph_db = org_name[:500]  # Truncate to match DB column width
        geonames_id_int = int(geonames_id) if geonames_id else None
        established = org.get("established")
        status = org.get("status")
        types = json.dumps(org.get("types", []))

        # Extract website (max 500 chars to match DB column)
        website = next(
            (link.get("value", "").strip()[:500] 
             for link in org.get("links", []) 
             if link.get("type") == "website" and link.get("value")), 
            None
        )
        
        # Extract location details
        city = location_details.get("name") if location_details else None
        country = location_details.get("country_name") if location_details else None
        country_code = location_details.get("country_code") if location_details else None
        state = location_details.get("country_subdivision_name") if location_details else None
        lat_float = float(lat) if lat else None
        lng_float = float(lng) if lng else None

        # Store complete API response for reference
        full_data = json.dumps(org)

        return (
            ror_id, original_name_in_graph_db, org_display_name, geonames_id_int, established, 
            status, types, website, city, country, 
            country_code, state, lat_float, lng_float, full_data,
            _idx_key
        )
            
    except requests.RequestException:
        return None
    except (KeyError, ValueError, json.JSONDecodeError):
        return None
    except Exception:
        return None



def lookup_ror_worker(org):
    """
    Multiprocessing worker wrapper for ROR API lookup.
    
    Args:
        org: Dict with 'name' and '_idx_key' fields
        
    Returns:
        Result from lookup_ror_static()
    """
    return lookup_ror_static(org["name"], org["_idx_key"])



class OrganizationLocationFinder(InitBase):
    """
    Fetches organization location data from ROR API and stores in MySQL.
    Updates graph database with 'N/A' status for organizations not found in ROR.
    """

    def __init__(self):
        super().__init__('organization_location', 'OrganizationLocationFinder')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/G-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)



    def init_nodes(self):
        """Override abstract method from InitBase."""
        self.update()



    def organization_generator(self, batch_size=100):
        """
        Memory-efficient generator that yields organizations in batches.
        Only fetches organizations without ROR IDs (ror_id = '').
        
        Args:
            batch_size: Number of organizations per batch
            
        Yields:
            Tuple of (batch_index, list of org dicts)
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
 


    def find_idx_key_not_in_db(self, _idx_key_list):        
        """
        Identify which organization idx_keys are NOT yet in MySQL.
        Avoids reprocessing organizations already in the database.
        
        Args:
            _idx_key_list: List of organization idx_keys to check
            
        Returns:
            List of idx_keys NOT found in organization_location table
        """
        if not _idx_key_list:
            return []

        placeholders = ",".join(["%s"] * len(_idx_key_list))
        
        query = f"""
            SELECT original_name_in_graph_db_idx_key
            FROM {self.table_name}
            WHERE original_name_in_graph_db_idx_key IN ({placeholders})
        """
        
        cursor = self.mysql.cursor(buffered=True, dictionary=True)
        cursor.execute(query, tuple(_idx_key_list))
        result = cursor.fetchall()
        cursor.close()
        
        found_keys = {row['original_name_in_graph_db_idx_key'] for row in result}
        return [key for key in _idx_key_list if key not in found_keys]



    def save_not_found(self, not_found_list):
        """
        Save organizations for which ROR data was not found.
        Inserts placeholder records to prevent reprocessing.
        
        Args:
            not_found_list: List of (org_name, idx_key) tuples
        """
        if not not_found_list:
            return

        sql = f'''
            INSERT INTO {self.table_name} 
            (original_name_in_graph_db, original_name_in_graph_db_idx_key) 
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE 
                original_name_in_graph_db_idx_key = VALUES(original_name_in_graph_db_idx_key)
        ''' 

        cursor = self.mysql.cursor(buffered=True)
        try:           
            cursor.executemany(sql, not_found_list)
            self.mysql.commit()
        except Exception as e:
            self.appender.log_stdout(f'Error saving not-found records at {_curr_timestamp()}')
            self.appender.log_stdout(f'{e}')
            raise
        finally:
            cursor.close()


    def save(self, org_info_tuple_list):
        """
        Bulk insert/update organization location data in MySQL.
        
        Args:
            org_info_tuple_list: List of organization data tuples from ROR API
        """
        if not org_info_tuple_list:
            return

        sql = f'''
            INSERT INTO {self.table_name} 
            ( 
                ror_id, original_name_in_graph_db, org_name, geonames_id, established, 
                status, types, website, city, country, 
                country_code, state, lat, lng, full_data,
                original_name_in_graph_db_idx_key
            ) 
            VALUES ( %s, %s, %s, %s, %s,  %s, %s, %s, %s, %s,  %s, %s, %s, %s, %s,  %s )
            ON DUPLICATE KEY UPDATE
                original_name_in_graph_db_idx_key = VALUES(original_name_in_graph_db_idx_key)
        '''

        cursor = self.mysql.cursor(buffered=True)
        try:
            cursor.executemany(sql, org_info_tuple_list)
            self.mysql.commit()
        except Exception as e:
            self.appender.log_stdout(f'Error saving organization data at {_curr_timestamp()}')
            self.appender.log_stdout(f'{e}')
            raise
        finally:
            cursor.close()


    def update(self):
        """
        Main processing loop:
        1. Fetch organizations from graph DB in batches
        2. Filter out already-processed organizations
        3. Query ROR API in parallel using multiprocessing
        4. Save results to MySQL
        5. Mark not-found organizations with 'N/A' in graph DB
        """
        generator = self.organization_generator(batch_size=100)

        # Cypher query to mark organizations not found in ROR
        cypher_script_of_not_found_update = '''
            UNWIND $chunks AS idx_key
            MATCH (org:Organization {_idx_key: idx_key})
            SET org.ror_id = 'N/A'
        '''

        total_processed = 0

        very_first_sart = time.time()

        for batch_index, batch in generator:
            start = time.time()

            self.appender.log_stdout(
                f"\n{Fore.BLUE}{'='*30} Processing batch {batch_index} "
                f"with {len(batch)} items {'='*30}{Style.RESET_ALL}\n"
            )

            # Get idx_keys for current batch
            batch_idx_keys = [org["_idx_key"] for org in batch]

            # Skip organizations already in MySQL
            idx_keys_to_process = set(self.find_idx_key_not_in_db(batch_idx_keys))
            org_list_to_process = [
                org for org in batch if org["_idx_key"] in idx_keys_to_process
            ]

            already_in_db_count = len(batch_idx_keys) - len(idx_keys_to_process)
            self.appender.log_stdout(
                f"Already in DB: {already_in_db_count} | "
                f"To process: {Fore.BLUE}{len(org_list_to_process)}{Style.RESET_ALL}"
            )
            
            if not org_list_to_process:
                continue

            # --- PARALLEL ROR API LOOKUPS ---
            # Calculate optimal process count for I/O-bound task
            cpu_count = multiprocessing.cpu_count()
            num_processes = min(
                cpu_count * 2,           # 2x CPUs for I/O-bound tasks
                20,                      # Cap to avoid API rate limits
                len(org_list_to_process) # Don't exceed items to process
            )

            self.appender.log_stdout(f"Querying ROR API for {len(org_list_to_process)} organizations using {num_processes} parallel processes...\n")
            
            # Parallel ROR API lookups
            with Pool(processes=num_processes) as pool:
                results = pool.map(lookup_ror_worker, org_list_to_process)
              

            # Separate found vs not-found organizations
            org_info_tuple_list = []
            orgs_not_found = []
            orgs_not_found_idx_keys = []

            # Process results
            for org, org_info_tuple in zip(org_list_to_process, results):
                org_name = org["name"]
                idx_key = org["_idx_key"]

                if org_info_tuple is not None:
                    org_info_tuple_list.append(org_info_tuple)
                    self.appender.log_stdout(f"✓ {org_name}")
                else:    
                    orgs_not_found_idx_keys.append(idx_key)
                    orgs_not_found.append((org_name[:500], idx_key))  # Truncate to DB limit

                    self.appender.log_stdout(f"{Fore.RED}✗ {org_name}{Style.RESET_ALL}")

            # Save not-found records and update graph DB
            if orgs_not_found:

                self.save_not_found(orgs_not_found)
                 
                try:                        
                    self.memgraph_execute_with_retry(cypher_script_of_not_found_update, {"chunks": orgs_not_found_idx_keys})

                except Exception as e:  
                    self.appender.log_stdout(f'{Fore.RED}Graph DB update failed: {e}{Style.RESET_ALL}')
                    raise

            # Save successfully found organizations
            if org_info_tuple_list:
                self.save(org_info_tuple_list)
                
            # Log batch summary
            batch_total = len(org_info_tuple_list) + len(orgs_not_found)
            total_processed += batch_total
            
            self.appender.log_stdout(f'\n--- Batch #: {Fore.BLUE}{batch_index}{Style.RESET_ALL} ---')
            self.appender.log_stdout(f'Found in ROR: {len(org_info_tuple_list)} | '
                                     f'Not found: {Fore.RED}{len(orgs_not_found_idx_keys)}{Style.RESET_ALL} | '
                                     f'Saved {Fore.GREEN} {len(org_info_tuple_list)}{Style.RESET_ALL} + {Fore.RED}{len(orgs_not_found)}{Style.RESET_ALL} | '
                                     f'Total processed so far: {total_processed}'
                                    )

            # Log time elapsed for batch
            end = time.time()
            hour, minute, second = _time_hms(end - start)
            self.appender.log_stdout(f'Time used for batch: {hour}:{minute}:{second}')

            day, hour, minute, second = _time_day_hms(end - very_first_sart)
            self.appender.log_stdout(f'=== Total time elapsed [d:h:m:s]: {day}:{hour}:{minute}:{second} ===\n')
            self.appender.log_stdout(f'\u23F0 {_curr_timestamp()}')

        # Cleanup
        self.close_mysql_conn()

        self.appender.log_stdout(f'{Fore.BLUE}{"="*30} Complete: {total_processed} organizations processed {"="*30}{Style.RESET_ALL}') 
        self.appender.close()

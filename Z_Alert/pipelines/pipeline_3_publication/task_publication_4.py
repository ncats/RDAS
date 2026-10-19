import os
import sys 
import json
import time
from typing import Optional
import requests
_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase

"""
1. Get unique omim_id list from table publication_gard_omim_mapping  which are not in publication_omim'
2. Retrieve OMIM data from API and insert into table publication_omim
"""
# Reference: GardOmimPublicationMappingTask - Z_Alert/pipelines/pipeline_3_publication/task_publication_3.py
# Reference: C_publication/init_6_publication-retrieve-omim.py

class PublicationOminDataRetrievalTask(PipelineBase):
    """
    Retrieve OMIM entry JSON for publication-linked OMIM IDs.

    The previous publication task maps GARD/search terms to OMIM IDs. This task
    finds OMIM IDs that are not yet stored in publication_omim, calls the OMIM
    entry API, and stages the JSON response for downstream publication graph work.
    """


    def __init__(self, gard_id: Optional[str] = None):

        """Initialize MySQL access and OMIM API configuration."""

        super().__init__(init_mysql=True, init_memgraph=False)

        self.gard_id = gard_id
        self.api_key = os.getenv('OMIM_API_KEY')
        self.omim_entry_api = os.getenv('OMIM_ENTRY_API')
        self.omim_rate_limited = False


    # Not implemented
    def find_new_data(self) -> None:
        
        raise NotImplementedError("PublicationOminDataRetrievalTask does not implement find_new_data().")
   

    def get_omim(self, omim_id: int) -> Optional[dict]:

        """Fetch one OMIM entry, backing off when the API returns HTTP 429."""

        params = {
            "mimNumber": omim_id,
            "include": "all",
            "format": "json",
            "apiKey": self.api_key,
        }
        max_attempts = 4

        for attempt in range(max_attempts):
            try:
                response = requests.get(self.omim_entry_api, params=params, timeout=30)

                if response.status_code == 429:
                    if attempt == max_attempts - 1:
                        self.omim_rate_limited = True
                        self.logger.error(f"OMIM API remains rate limited for omim_id={omim_id} after {max_attempts} attempts.")
                        return None

                    # Respect a numeric Retry-After value; otherwise use a
                    # bounded exponential delay before another HTTP request.
                    retry_after = response.headers.get("Retry-After", "")
                    try:
                        delay = min(60.0, max(0.0, float(retry_after)))
                    except ValueError:
                        delay = min(60.0, 2 ** (attempt + 1))
                    self.logger.warning(f"OMIM API rate limited omim_id={omim_id}; retrying in {delay:g} seconds.")
                    time.sleep(delay)
                    continue

                response.raise_for_status()
                return response.json()

            except requests.exceptions.HTTPError:
                self.logger.error(f"OMIM API returned HTTP {response.status_code} for omim_id={omim_id}.")
                return None

            except (requests.exceptions.RequestException, ValueError) as e:
                # Never log the request URL: its query parameters contain the API key.
                self.logger.error(f"OMIM API request failed for omim_id={omim_id}: {type(e).__name__}.")
                return None

        return None
    

    # implement
    def process_new_data(self) -> None:

        """Fetch missing OMIM entries and insert them into publication_omim."""

        if not self.omim_entry_api or not self.api_key:
            self.logger.error("OMIM_ENTRY_API or OMIM_API_KEY is not configured.")
            self.close()
            return
        
        # A GARD-scoped run reads only that disease's mappings. DISTINCT avoids
        # repeated API calls when several mapping rows share an OMIM ID.
        # Check all mappings, including older ones, so failed downloads remain
        # eligible for a later retry after is_new has been reset.
        fetch_query = '''
            SELECT DISTINCT pgom.omim_id
            FROM publication_gard_omim_mapping pgom
            LEFT JOIN publication_omim po 
                ON pgom.omim_id = po.omim_id
            WHERE po.omim_id IS NULL
        '''
        fetch_params = ()
        if self.gard_id is not None:
            fetch_query += ' AND pgom.gard_id = %s'
            fetch_params = (self.gard_id,)
        fetch_query += ' ORDER BY pgom.omim_id'

        fetch_cursor = None
        insert_cursor = None

        # A second GARD node may reference an OMIM ID inserted since the read.
        # The unique omim_id key makes that a harmless no-op, preserving the
        # existing JSON and is_new state instead of replacing either value.
        insert_sql = '''
            INSERT INTO publication_omim (omim_id, entry_json, is_new)
            VALUES (%s, %s, 1)
            ON DUPLICATE KEY UPDATE omim_id = omim_id
        '''

        batch_num = 0
        batch_size = 20

        val_list = []

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(fetch_query, fetch_params)

            insert_cursor = self.mysql.cursor()

            while True:
                if self.omim_rate_limited:
                    self.logger.warning("Stopping OMIM retrieval after repeated HTTP 429 responses; missing IDs remain retryable.")
                    break

                rows = fetch_cursor.fetchmany(batch_size)

                if not rows:
                    self.logger.info("No more missing OMIM IDs to fetch for this run.")
                    break
 
                batch_num += 1
                self.logger.info(f'\n--- batch# = {batch_num} ---')

                val_list = []

                for row in rows:
                    if self.omim_rate_limited:
                        break

                    omim_id = row['omim_id'] 
    
                    entry_json = self.get_omim(omim_id)

                    if entry_json is None:
                        self.logger.error(f"OMIM API failed for omim_id={omim_id}; no publication_omim row inserted, so it remains retryable.")
                        continue

                    if not isinstance(entry_json, dict) or not entry_json.get("omim"):
                        self.logger.error(f"OMIM API returned an unexpected payload for omim_id={omim_id}; no publication_omim row inserted, so it remains retryable.")
                        continue

                    # Error: Failed executing the operation; Python type dict cannot be converted
                    ''' Solution: json.dumps(entry_json) '''
                    # Store the raw OMIM JSON string so later tasks can parse
                    # references, phenotype maps, and other entry sections.
                    val = (omim_id, json.dumps(entry_json))
                    val_list.append(val)
                
                if len(val_list) > 0:
                    try:  
                        insert_cursor.executemany(insert_sql, val_list)
                        self.mysql.commit() 
                        self.logger.info(f'Batch#: {batch_num} - {insert_cursor.rowcount} rows have been inserted into publication_omim table')
                    
                    except Exception as e:
                        self.mysql.rollback()
                        self.logger.error(f'insert_sql error: \n{e}')
                else:
                    self.logger.warning(f"Batch#: {batch_num} - no valid OMIM API responses to insert; skipped OMIM IDs remain retryable.")
                
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            if insert_cursor:
                insert_cursor.close()

            ''' Explicitly close the all the db connections '''
            self.close()

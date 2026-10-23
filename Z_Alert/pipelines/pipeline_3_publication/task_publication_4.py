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
from utils.https_request import HTTPSUtils

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

        """
        Retry one OMIM entry without making an outage fatal to the GARD loop.
        Keep the OMIM-specific rate-limit circuit breaker: repeated HTTP 429
        stops this task's remaining requests, while other pipeline tasks and
        GARD nodes continue. Missing entries are never saved as empty JSON.
        Both retries and terminal failures are warnings; an unavailable entry
        does not defer the GARD node. Never log API-key URLs.
        """
        params = {"mimNumber": omim_id, "include": "all", "format": "json", "apiKey": self.api_key}
        max_attempts = 4
        for attempt in range(1, max_attempts + 1):
            response = None
            retry_after = ""
            rate_limited = False
            retryable = True
            try:
                response = requests.get(self.omim_entry_api, params=params, timeout=30)
                retry_after = response.headers.get("Retry-After", "").strip()
                rate_limited = response.status_code == 429
                if response.status_code >= 400:
                    reason = f"HTTP {response.status_code}"
                    retryable = response.status_code in {408, 425, 429} or 500 <= response.status_code <= 599
                else:
                    entry = response.json()
                    if not isinstance(entry, dict) or not isinstance(entry.get("omim"), dict) or not entry["omim"]:
                        raise ValueError("Missing OMIM entry object.")
                    return entry
            except (requests.RequestException, ValueError) as exc:
                reason = type(exc).__name__
            finally:
                if response is not None:
                    try:
                        response.close()
                    except (requests.RequestException, OSError):
                        self.logger.warning(f"OMIM entry {omim_id} HTTP response cleanup failed; continuing.")

            if not retryable or attempt == max_attempts:
                self.omim_rate_limited = rate_limited
                HTTPSUtils.record_failure(self.logger, f"OMIM entry {omim_id}", f"{reason}; attempts={attempt}/{max_attempts}")
                return None
            delay = min(60, 2 ** attempt)
            if retry_after.isdigit():
                delay = min(60, max(delay, int(retry_after)))
            self.logger.warning(f"OMIM entry {omim_id} attempt {attempt}/{max_attempts} failed ({reason}); retrying in {delay} seconds.")
            time.sleep(delay)

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
                        self.logger.warning(f"OMIM API failed for omim_id={omim_id}; no publication_omim row inserted, so it remains retryable.")
                        continue

                    if not isinstance(entry_json, dict) or not entry_json.get("omim"):
                        self.logger.warning(f"OMIM API returned an unexpected payload for omim_id={omim_id}; no publication_omim row inserted, so it remains retryable.")
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

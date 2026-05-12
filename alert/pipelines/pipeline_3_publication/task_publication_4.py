import os
import sys 
import json
_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.https_request import HTTPSUtils as HttpsUtil

"""
1. Get unique omim_id list from table publication_gard_omim_mapping  which are not in publication_omim'
2. Retrieve OMIM data from API and insert into table publication_omim
"""
# Reference: GardOmimPublicationMappingTask - alert/pipelines/pipeline_3_publication/task_publication_3.py
# Reference: C_publication/init_6_publication-retrieve-omim.py

class PublicationOminDataRetrievalTask(PipelineBase):
    """
    Retrieve OMIM entry JSON for publication-linked OMIM IDs.

    The previous publication task maps GARD/search terms to OMIM IDs. This task
    finds OMIM IDs that are not yet stored in publication_omim, calls the OMIM
    entry API, and stages the JSON response for downstream publication graph work.
    """


    def __init__(self):
        """Initialize MySQL access and OMIM API configuration."""

        super().__init__(init_mysql=True, init_memgraph=False)

        self.api_key = os.getenv('OMIM_API_KEY')
        self.omim_entry_api = os.getenv('OMIM_ENTRY_API')


    # Not implemented
    def find_new_data(self) -> None:
        
        raise NotImplementedError("PublicationOminDataRetrievalTask does not implement find_new_data().")
   

    def get_omim(self, url):
        """Call the OMIM API and return the parsed JSON response."""

        def parse_api_response(response): 
            try: 
                response_json = response.json() 
                return response_json
            except TypeError as e:
                print(f'TypeError: {e}') 
            return None
    
        return HttpsUtil.with_api_retry_GET(url, parse_api_response)
    

    # implement
    def process_new_data(self) -> None:
        """Fetch missing OMIM entries and insert them into publication_omim."""

        if not self.omim_entry_api:
            self.logger.error("OMIM_ENTRY_API is not configured.")
            self.close()
            return
        
        # Select only OMIM IDs found by the GARD/OMIM mapping task that have
        # not already been inserted into publication_omim.
        fetch_query = '''
            SELECT DISTINCT pgom.omim_id
            FROM publication_gard_omim_mapping pgom

            LEFT JOIN publication_omim po 
                ON pgom.omim_id = po.omim_id

            WHERE po.omim_id IS NULL
            AND pgom.is_new = 1
            ORDER BY pgom.omim_id;
        '''

        fetch_cursor = None
        insert_cursor = None

        insert_sql = 'INSERT INTO publication_omim (omim_id, entry_json, is_new) VALUES (%s, %s, 1)'

        batch_num = 0
        batch_size = 20

        val_list = []

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(fetch_query)

            insert_cursor = self.mysql.cursor()

            while True:
                rows = fetch_cursor.fetchmany(batch_size)

                if not rows:
                    self.logger.info(f"No more rows to fetch.\nNo difference of omim_id between table publication_gard_omim_mapping and publication_omim.\n")
                    break
 
                batch_num += 1
                self.logger.info(f'\n--- batch# = {batch_num} ---')

                val_list = []

                for row in rows:

                    omim_id = row['omim_id'] 
    
                    # The API key stays in env; this URL only adds the OMIM ID
                    # and response-shaping parameters needed by the endpoint.
                    url = f'{self.omim_entry_api}?mimNumber={omim_id}&include=all&format=json&apiKey={self.api_key}'

                    entry_json = self.get_omim(url)

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
                        self.logger.info(f'Batch#: {batch_num} - {len(val_list)} rows have been inserted into publication_omim table')
                    
                    except Exception as e:
                        self.logger.error(f'insert_sql error: \n{e}')
                
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            if insert_cursor:
                insert_cursor.close()

            ''' Explicitly close the all the db connections '''
            self.close()

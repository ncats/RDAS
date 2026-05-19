import os
import sys
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterator, List, Sequence
from collections import OrderedDict
import requests
import time
import json
import re

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from dotenv import load_dotenv
load_dotenv()

from pipelines.pipeline_base import PipelineBase
from utils.publication_worker import PublicationWorker

"""
Find and store newly published PubMed articles for updated GARD diseases.

For each GARD node, this pipeline searches PubMed with the node's filtered
disease names between the node's last update date and today. 

It downloads article metadata for PMIDs that are NOT already in publication_article,
stores those article rows in publication_article, and records the
GARD/search-term/PMID relationship(unique)in publication_gard_searchterm_pubmed_mapping.
"""
# Reference: C_publication/init_1_publication_gard_pubmed-id.py
# Reference: C_publication/init_2_1_publication-gard-searchterm-pubmed-mapping.py
# Reference: C_publication/init_3_1_publication-article-by-pubmed-id.py

# The pubmed_id in publication_article should be unique for alert imports.
'''
# check uniqueness of pubmed_id in publication_article

SELECT distinct pubmed_id, count(*) as ct
FROM rdas_db.publication_article
WHERE pubmed_id IS NOT NULL
GROUP BY pubmed_id
HAVING COUNT(*) > 1
limit 10;
'''

class NewPublicationDiscoveryTask(PipelineBase):


    def __init__(self):

        super().__init__(init_mysql=True, init_memgraph=False)

        self.api_key = os.getenv("NCBI_KEY") 
        self.pubmed_esearch_api = os.getenv("PUBMED_ESEARCH_API")
        self.publication_worker = PublicationWorker()


    # Not implemented
    def process_new_data(self) -> None:
        raise NotImplementedError("NewPublicationDiscoveryTask does not implement process_new_data().")
   

    def find_new_data(self, gard_node) -> None:
        
        gard_id = gard_node['gardId']
        names = gard_node['filtered_names']
        last_update_date = gard_node.get("updated")
        today = date.today()

        for name in names:

            pubmed_ids = self.retrieve_pubmed_ids(gard_id, name, last_update_date, today)

            if not pubmed_ids:
                #self.logger.warning(f"No new PubMed IDs found for [{gard_id}: {name}]")
                continue
            
            self.find_new_publications(gard_id, name, pubmed_ids)
    


    # Step 1
    def retrieve_pubmed_ids(self, gard_id, search_term, mindate=None, maxdate=None):
                 
        count = None  
        search_term = search_term.lower() # lower case in database
 
        if not self.pubmed_esearch_api:
            self.logger.error("PUBMED_ESEARCH_API is not configured.")
            return None

        # Exact match uses the PubMed ESearch Title/Abstract syntax.
        search_term_normalized = re.sub(r'\s+', '+', search_term) # replace whitespace with + sign
        term_search_query = f'"{search_term_normalized}"[Title/Abstract:~0]'

        retries = 0
        max_retries=10
    
        url = f"{self.pubmed_esearch_api}?db=pubmed&term={term_search_query}&mindate={mindate}&maxdate={maxdate}&retmode=json&retmax=10000&api_key={self.api_key}"
        #print(url)
        
        while retries < max_retries:
            try:                
                response = requests.get(url)

                if response.status_code >= 400:
                    self.logger.error(f"PubMed request failed: status={response.status_code}, url={url}")
                    break

                try:
                    obj = response.json()
                    result = obj['esearchresult']
                    
                    count = result['count']
                    retmax = result['retmax']
                    retstart = result['retstart']
                    pubmed_ids = result['idlist']
                    querytranslation = result['querytranslation']
                    phrasesignored = result['warninglist']['phrasesignored']
                    quotedphrasesnotfound = result['warninglist']['quotedphrasesnotfound'] 
 
                except KeyError as e:
                    self.logger.error(f"KeyError: {e} - The required key does not exist in the JSON structure.")
                    self.logger.error(f'{url}')
                    self.logger.error(f'\n{result}\n')
                except (TypeError, AttributeError):
                    self.logger.error("The JSON structure is not as expected or 'response' might not be JSON.")
                    self.logger.error(f'{url}')
                
                break  # Exit the loop if successful
            except requests.exceptions.Timeout:
                retries += 1
                time.sleep(1)
            except requests.exceptions.RequestException as e:
                break  # Exit the loop for non-retryable errors
        """
        {
            "header": { "type": "esearch",  "version": "0.3" },
            "esearchresult": {
                "count": "10",
                "retmax": "10",
                "retstart": "0",
                "idlist": [
                    "39634243", "31075093", "23034868", "22821547", "23074680", "19996736", "19048502", "18230893",  "11045586", "8322820"
                ],
                "translationset": [],
                "querytranslation": "\"momo syndrome\"[Title/Abstract:~0] AND 1970/01/01:2025/12/31[Date - Entry]",
                "warninglist": {
                "phrasesignored": [],
                "quotedphrasesnotfound": [],
                "outputmessages": [ "Restrictions achieved. start and count adjusted to 0, 9999" ]
                }
            }
        }
        """

        if count is not None:
           return pubmed_ids
        else:
            return None


    # Step 2
    def find_new_publications(self, gard_id, search_term, pubmed_ids):
  
        check_cursor = self.mysql.cursor()
        insert_article_cursor = self.mysql.cursor(buffered=True)
        insert_gard_searchterm_pubmed_mapping_cursor = self.mysql.cursor(buffered=True)

        insert_new_article_sql = '''
            INSERT INTO publication_article (
                pubmed_id, doi, title, abstract_text, affiliation,
                first_publication_date, publication_year, cited_by_count, is_open_access, in_EPMC,
                in_PMC, has_PDF, pub_type, source_json, is_new)
            SELECT %s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s, 1
            WHERE NOT EXISTS (
                SELECT 1
                FROM publication_article
                WHERE pubmed_id = %s
            )
        '''

        insert_gard_searchterm_pubmed_mapping_sql = '''
            INSERT INTO publication_gard_searchterm_pubmed_mapping (gard_id, search_term, pubmed_id)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE gard_id = VALUES(gard_id), search_term = VALUES(search_term), pubmed_id = VALUES(pubmed_id)
        '''
        
        try:
            # 1.
              
            # 2. check if the pubmed_id is already in publication_article
            placeholders = ",".join(["%s"] * len(pubmed_ids))

            batch_check_exist_query = f'''
                SELECT pubmed_id
                FROM publication_article
                WHERE pubmed_id IN ({placeholders})
            '''

            check_cursor.execute(batch_check_exist_query, list(pubmed_ids))

            existing_pubmed_ids = {
                str(existing_row[0])
                for existing_row in check_cursor.fetchall()
            }

            # 3. 
            for pubmed_id in pubmed_ids: 

                # the pubmed_id is already in publication_article table
                if pubmed_id in existing_pubmed_ids:
                    continue
                 
                # 5. the pubmed_id is NOT in publication_article, download article
                article_val = self.publication_worker.download_by_pmid(pubmed_id)

                if not article_val:
                    url = f"{os.getenv('EURO_PEPMC_SERVICE_URL')}?query=EXT_ID:{pubmed_id}&resultType=core&format=json"

                    self.logger.warning(f"GARD ID: {gard_id}, Search term: {search_term} - Unable to download: {url}")
                    continue
                 
                # 6. save the new article into publication_article table
                insert_article_cursor.execute(insert_new_article_sql, (*article_val, pubmed_id))

                self.mysql.commit()

                pubmedid_gardid_searchitem_for_logging = f'PubMed ID: {pubmed_id}\tGARD ID: {gard_id}\tSearch term: {search_term}'
                self.logger.info(f"1. New publication added to table publication_article :: {pubmedid_gardid_searchitem_for_logging}")

                # 7. save the gard_id, search_term and pubmed_id
                insert_gard_searchterm_pubmed_mapping_cursor.execute(insert_gard_searchterm_pubmed_mapping_sql, (gard_id, search_term, pubmed_id))
                self.mysql.commit()

                self.logger.info(f"2. New mapping added to table publication_gard_searchterm_pubmed_mapping :: {pubmedid_gardid_searchitem_for_logging}")
 
        except Exception as e:
            self.logger.error(e)
            self.mysql.commit()

        finally:
            check_cursor.close()
            insert_article_cursor.close()
            insert_gard_searchterm_pubmed_mapping_cursor.close()
            self.mysql.commit()

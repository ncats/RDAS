import os
import sys
import json
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import requests
import urllib3
import warnings
# Suppress only the InsecureRequestWarning
warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)

import time
from datetime import datetime
from dotenv import load_dotenv
load_dotenv() 

from utils.tools import  _normalize_txt


class PublicationWorker:


    def __init__(self):  

        self.base_url = os.getenv('EURO_PEPMC_SERVICE_URL') 
        self.chars_to_remove = "!@#$%^&*()_+-={}[]|\\:;\"'<>,.?/`~"


    def _check_key_value(self, obj, key):
        result = key in obj and obj[key] == 'Y'
        return result
    

    def get_insert_sql(self, table_name='publication_article'):

        return f'''
            INSERT INTO {table_name} (
                pubmed_id, doi, title, abstract_text, affiliation,
                first_publication_date, publication_year, cited_by_count, is_open_access, in_EPMC, 
                in_PMC, has_PDF, pub_type, source_json)
            VALUES(%s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s)
        '''
        
        
    def download_by_pmid(self,pmid):
        #https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=EXT_ID:5770408&resultType=core&format=json&pageSize=1000    
        url = f"{self.base_url}?query=EXT_ID:{pmid}&resultType=core&format=json"
        
        retries = 0
        max_retries=10
        while retries < max_retries:
            try:                
                response = requests.get(url, verify=False)
                response.raise_for_status()  # Raise an exception for HTTP errors (4xx and 5xx)                                
                try:
                    bigObj = response.json()
                
                    resultList = bigObj['resultList']['result']
                    if len(resultList) <= 0:
                        break

                    for result in resultList:
                        if 'pmid' not in result:
                            continue
                            
                        pubmed_id  = result['pmid']

                        # The query returned more other pubmed_id(s)
                        if str(pubmed_id) != str(pmid):
                            continue

                        source = _normalize_txt(result['source']) if 'source' in result else None
                        doi = _normalize_txt(result['doi']) if 'doi' in result else None
                        title = _normalize_txt(result['title']) if 'title' in result else None
                        abstract_text = _normalize_txt(result['abstractText']) if 'abstractText' in result else None
                        affiliation = _normalize_txt(result['affiliation']) if 'affiliation' in result else None
                        first_publication_date = result['firstPublicationDate'] if 'firstPublicationDate' in result else None
                        publication_year = int(datetime.strptime(result['firstPublicationDate'], '%Y-%m-%d').year) if 'firstPublicationDate' in result else None
                        is_open_access = self._check_key_value(result, 'isOpenAccess')
                        in_EPMC = self._check_key_value(result, 'inEPMC')
                        in_PMC = self._check_key_value(result, 'inPMC')
                        has_PDF = self._check_key_value(result, 'hasPDF')
                        
                        pub_type = json.dumps(result['pubTypeList']['pubType']) if 'pubTypeList' in result else None
                        cited_by_count = int(result['citedByCount']) if 'citedByCount' in result else 0
                        
                        if title:
                            title = title.strip(self.chars_to_remove)

                        if abstract_text:
                            abstract_text = abstract_text.strip(self.chars_to_remove)

                        print(f'pubmed_id = {pubmed_id}\t{publication_year}\t{doi}')

                        '''
                        pubmed_id, doi, title, abstract_text, affiliation,
                        first_publication_date, publication_year, cited_by_count, is_open_access, in_EPMC, 
                        in_PMC, has_PDF,  pub_type, source_json
                        ''' 
                        val = (pubmed_id, doi, title, abstract_text, affiliation, 
                            first_publication_date, publication_year, cited_by_count, is_open_access, in_EPMC,
                            in_PMC, has_PDF, pub_type, json.dumps(result)
                            ) 

                        return val 

                except KeyError as e:
                    print(f'KeyError: {e}\n{url}')
                except TypeError as e:
                    print(f'TypeError: {e}\n{url}')
                except AttributeError as e:
                    print(f'AttributeError: {e}\n{url}')
                
                break  # Exit the loop if successful
            except requests.exceptions.Timeout:
                retries += 1
                time.sleep(1)
            except requests.exceptions.RequestException as e:
                break  # Exit the loop for non-retryable errors

        return None

 
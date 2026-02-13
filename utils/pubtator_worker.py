import os
import sys
import json
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
   
from utils.https_request import HTTPSUtils as HttpsUtil
from colorama import init, Fore, Style
# Initialize colorama for Windows compatibility
init()

class PubtatorWorker:

    def __init__(self):  
        pass
    
    def _get_pubtator_json(self, url):

        def parse_api_response(response):
            try: 
                response_json = response.json() 
                return response_json
            except TypeError as e:
                print(f'TypeError: {e}') 
                
            return None

        return HttpsUtil.with_api_retry_GET(url, parse_api_response)
    

    def download_by_pmid(self,pmid):

        ''' Check pubtator by PMID, https://www.ncbi.nlm.nih.gov/research/pubtator3/ '''

        #url = f'https://www.ncbi.nlm.nih.gov/research/pubtator-api/publications/export/biocjson?pmids={pmid}'
        url = f'https://www.ncbi.nlm.nih.gov/research/pubtator3-api/publications/export/biocjson?pmids={pmid}'

        source_json = self._get_pubtator_json(url)
            
        if not source_json: 
            print(f'{Fore.RED}pmid = {pmid}, Pubtator source_json is None{Style.RESET_ALL}')
        
        #return (pmid, json.dumps(source_json))
        return (pmid, source_json)
            
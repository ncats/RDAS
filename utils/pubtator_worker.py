import os
import sys
import json
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
   
from utils.https_request import HTTPSUtils as HttpsUtil
from utils.applogger import AppLogger

class PubtatorWorker:

    def __init__(self, logger=None):
        self.logger = logger or self._create_logger()


    def _create_logger(self):
        # Keep utility logs in the same home-directory alert log folder used by
        # PipelineBase, and expand "~" before passing the path to FileHandler.
        log_dir = os.path.expanduser('~/rdas-memgraph-alert-log')
        os.makedirs(log_dir, exist_ok=True)
        return AppLogger(
            type(self).__name__,
            f"{log_dir}/alert-{type(self).__name__}.log"
        ).get_logger()
    

    def _get_pubtator_json(self, url):

        def parse_api_response(response):
            try: 
                response_json = response.json() 
                return response_json
            except TypeError as e:
                self.logger.error(f'TypeError while parsing PubTator response: {e}\n{url}')
            except ValueError as e:
                self.logger.error(f'ValueError while parsing PubTator response: {e}\n{url}')
            except AttributeError as e:
                self.logger.error(f'AttributeError while parsing PubTator response: {e}\n{url}')
                
            return None

        try:
            return HttpsUtil.with_api_retry_GET(url, parse_api_response)
        except Exception as e:
            self.logger.error(f'Unexpected error while downloading PubTator data: {e}\n{url}')
            return None
    

    def download_by_pmid(self,pmid):

        ''' Check pubtator by PMID, https://www.ncbi.nlm.nih.gov/research/pubtator3/ '''

        if not pmid:
            self.logger.error("Cannot download PubTator data because pubmed_id is empty.")
            return (pmid, None)

        #url = f'https://www.ncbi.nlm.nih.gov/research/pubtator-api/publications/export/biocjson?pmids={pmid}'
        url = f'https://www.ncbi.nlm.nih.gov/research/pubtator3-api/publications/export/biocjson?pmids={pmid}'

        source_json = self._get_pubtator_json(url)
            
        if not source_json: 
            self.logger.error(f'pubmed_id={pmid}, PubTator source_json is None. url={url}')
        
        #return (pmid, json.dumps(source_json))
        return (pmid, source_json)

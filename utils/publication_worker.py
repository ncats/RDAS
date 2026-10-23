import os
import sys
import json
# Add the project root to the Python path
_dir = os.path.dirname(__file__)
sys.path.append(os.path.abspath(os.path.join(_dir, '..')))

import urllib3
import warnings
# Suppress only the InsecureRequestWarning
warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)

from datetime import datetime
from dotenv import load_dotenv
load_dotenv(os.path.abspath(os.path.join(_dir, '..', '.env')))

from utils.applogger import AppLogger
from utils.https_request import HTTPSUtils
from utils.tools import  _normalize_txt


class PublicationWorker:


    def __init__(self, logger=None):

        self.base_url = os.getenv('EURO_PEPMC_SERVICE_URL') 
        self.chars_to_remove = "!@#$%^&*()_+-={}[]|\\:;\"'<>,.?/`~"
        self.logger = logger or self._create_logger()


    def _create_logger(self):
        '''
        Keep utility logs in the same home-directory alert log folder used by PipelineBase, and expand "~" before passing the path to FileHandler.
        '''
        log_dir = os.path.expanduser(os.getenv("ALERT_LOG_DIR", "logs"))
        os.makedirs(log_dir, exist_ok=True)
        return AppLogger(
            type(self).__name__,
            f"{log_dir}/alert-{type(self).__name__}.log"
        ).get_logger()


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
        
        
    def download_by_pmid(self, pmid):

        """
        Download Europe PMC metadata without stopping the parent pipeline.
        Network/HTTP/JSON failures use bounded retries and return None, so the
        importer skips an unavailable Article instead of inserting empty JSON.
        For example, PMID 9282845 can fail after ten requests while other PMIDs
        in the thread pool still import and the GARD node completes normally.
        Field conversion below preserves the existing insert-ready tuple.
        """
        if not self.base_url:
            self.logger.error("EURO_PEPMC_SERVICE_URL is not configured. Cannot download pubmed_id=%s.", pmid)
            return None

        url = f"{self.base_url}?query=EXT_ID:{pmid}&resultType=core&format=json"
        bigObj = HTTPSUtils.with_api_retry_GET(url, lambda response: response.json(), timeout=30, logger=self.logger, description=f"Europe PMC publication {pmid}", verify=False)
        if bigObj is None:
            return None
        try:
            resultList = bigObj['resultList']['result']
            if len(resultList) <= 0:
                HTTPSUtils.record_failure(self.logger, f"Europe PMC publication {pmid}", "no matching Article metadata returned")
                return None

            has_pubmed_result = False
            for result in resultList:
                if 'pmid' not in result:
                    continue

                pubmed_id  = result['pmid']
                has_pubmed_result = True

                # The query returned more other pubmed_id(s)
                if str(pubmed_id) != str(pmid):
                    self.logger.info(
                        "Skipping Europe PMC result for requested pubmed_id=%s because returned pmid=%s.",
                        pmid,
                        pubmed_id
                    )
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

                self.logger.info(f'pubmed_id = {pubmed_id}\t{publication_year}\t{doi}')

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

            reason = "no returned PMID matched the request" if has_pubmed_result else "response had no PMID field"
            HTTPSUtils.record_failure(self.logger, f"Europe PMC publication {pmid}", reason)
        except (KeyError, TypeError, AttributeError, ValueError) as exc:
            HTTPSUtils.record_failure(self.logger, f"Europe PMC publication {pmid}", f"invalid metadata: {type(exc).__name__}")
        return None

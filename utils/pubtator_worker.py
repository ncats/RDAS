import os
import sys
from typing import Any, Dict, List, Optional
# Add the project root to the Python path
_dir = os.path.dirname(__file__)
sys.path.append(os.path.abspath(os.path.join(_dir, '..')))
   
from dotenv import load_dotenv
from utils.https_request import HTTPSUtils as HttpsUtil
from utils.applogger import AppLogger

load_dotenv(os.path.abspath(os.path.join(_dir, '..', '.env')))

class PubtatorWorker:

    def __init__(self, logger=None):

        self.logger = logger or self._create_logger()
        self.base_url = os.getenv("PUBTATOR_SERVICE_URL")


    def _create_logger(self):

        # Keep utility logs in the same home-directory alert log folder used by
        # PipelineBase, and expand "~" before passing the path to FileHandler.
        log_dir = os.path.expanduser(os.getenv("ALERT_LOG_DIR", "logs"))
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


    def download_by_pmids(self, pmids) -> Dict[str, Optional[Dict[str, Any]]]:

        '''
        Download PubTator JSON for a batch of PMIDs.

        PubTator3 supports comma-separated PMIDs in one request. The API returns
        all documents in one top-level PubTator3 list, but publication_pubtator
        stores one source_json row per PMID. This method splits the batch
        response back into one PubTator-shaped JSON object for each requested
        PMID, preserving the existing database format used by parse_pubtator().
        '''
        pubmed_ids = self._normalize_pubmed_ids(pmids)
        result_by_pmid = {pubmed_id: None for pubmed_id in pubmed_ids}

        if not pubmed_ids:
            self.logger.error("Cannot download PubTator data because the PMID batch is empty.")
            return result_by_pmid

        if not self.base_url:
            self.logger.error("PUBTATOR_SERVICE_URL is not configured. Cannot download PubTator PMID batch.")
            return result_by_pmid

        url = f'{self.base_url}?pmids={",".join(pubmed_ids)}'
        source_json = self._get_pubtator_json(url)

        if not source_json:
            self.logger.error(f'PubTator source_json is None for PMID batch={",".join(pubmed_ids)}. url={url}')
            return result_by_pmid

        return self._split_pubtator_json_by_pmid(source_json, pubmed_ids)


    def download_by_pmid(self,pmid):

        ''' Check PubTator by PMID. '''

        if not pmid:
            self.logger.error("Cannot download PubTator data because pubmed_id is empty.")
            return (pmid, None)

        pubmed_ids = self._normalize_pubmed_ids([pmid])
        source_json = self.download_by_pmids(pubmed_ids).get(pubmed_ids[0]) if pubmed_ids else None
            
        if not source_json: 
            self.logger.error(f'pubmed_id={pmid}, PubTator source_json is None.')
        
        #return (pmid, json.dumps(source_json))
        return (pmid, source_json)


    def _normalize_pubmed_ids(self, pmids) -> List[str]:

        '''
        Convert a PMID iterable into unique non-empty strings while preserving
        the input order. Duplicate PMIDs in one batch would waste URL length and
        can produce duplicate insert attempts later.
        '''
        if isinstance(pmids, (str, int)):
            pmids = [pmids]

        pubmed_ids = []
        seen = set()

        for pmid in pmids:
            if pmid is None:
                continue

            pubmed_id = str(pmid).strip()

            if not pubmed_id or pubmed_id in seen:
                continue

            seen.add(pubmed_id)
            pubmed_ids.append(pubmed_id)

        return pubmed_ids


    def _split_pubtator_json_by_pmid(self, source_json: Dict[str, Any], requested_pmids: List[str]) -> Dict[str, Optional[Dict[str, Any]]]:

        '''
        Split one PubTator3 batch response into the per-PMID JSON structure that
        publication_pubtator.source_json already stores.

        Each saved value keeps the same top-level keys as the batch response,
        but replaces PubTator3 with a one-item list for that PMID. Missing PMIDs
        stay mapped to None so the caller can skip them and retry later.
        '''
        result_by_pmid = {pubmed_id: None for pubmed_id in requested_pmids}

        if not isinstance(source_json, dict):
            self.logger.error(f'Unexpected PubTator response type: {type(source_json).__name__}')
            return result_by_pmid

        documents = source_json.get('PubTator3')

        if not isinstance(documents, list):
            self.logger.error('Unexpected PubTator response: PubTator3 is missing or is not a list.')
            return result_by_pmid

        requested_set = set(requested_pmids)

        for document in documents:
            pubmed_id = self._extract_pubmed_id_from_document(document, requested_set)

            if not pubmed_id:
                if isinstance(document, dict):
                    self.logger.warning(
                        f'Unable to match PubTator document to requested PMIDs. '
                        f'id={document.get("id")}, _id={document.get("_id")}'
                    )
                else:
                    self.logger.warning(f'Unable to match PubTator document type={type(document).__name__} to requested PMIDs.')
                continue

            document_json = dict(source_json)
            document_json['PubTator3'] = [document]
            result_by_pmid[pubmed_id] = document_json

        missing_pmids = [
            pubmed_id
            for pubmed_id, document_json in result_by_pmid.items()
            if document_json is None
        ]

        if missing_pmids:
            self.logger.warning(f'PubTator batch response did not include PMIDs: {", ".join(missing_pmids)}')

        return result_by_pmid


    def _extract_pubmed_id_from_document(self, document: Dict[str, Any], requested_pmids) -> Optional[str]:

        '''
        Return the requested PMID represented by one PubTator3 document.

        PubTator3 commonly returns the PMID in document["id"], but some records
        also expose it in _id as "PMID|PMCID" or in infons.article-id_pmid. Check
        all three locations so one response-shape variation does not make a
        successful API response look like a failed PMID.
        '''
        if not isinstance(document, dict):
            return None

        candidates = []

        document_id = document.get('id')
        if document_id is not None:
            candidates.append(str(document_id))

        composite_id = document.get('_id')
        if composite_id is not None:
            candidates.append(str(composite_id).split('|')[0])

        infons = document.get('infons')
        if isinstance(infons, dict) and infons.get('article-id_pmid') is not None:
            candidates.append(str(infons.get('article-id_pmid')))

        for candidate in candidates:
            candidate = candidate.strip()

            if candidate in requested_pmids:
                return candidate

        return None

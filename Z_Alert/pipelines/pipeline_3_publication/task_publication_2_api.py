import os
import sys
from concurrent.futures import ThreadPoolExecutor
import threading
from typing import Any, Dict, Optional, Sequence, Tuple
import requests
import json
from math import isfinite
from utils.https_request import HTTPSUtils as HttpsUtil
from utils.tools import _resolve_worker_count, _to_float, _to_txt

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from dotenv import load_dotenv
load_dotenv()

from pipelines.pipeline_base import PipelineBase

"""
Update is_EPI and is_NHS for new rows in publication_article.
"""
# Reference: C_publication/init_4_publication-update-EPI-NHS-of-Article-multi.py

'''
These are module-level functions, not class methods. Keeping the HTTP work
outside PublicationEpiNhsClassificationTask lets worker threads call the APIs
without sharing the parent task's MySQL cursor or connection.
'''

def _worker_log_prefix(article_id: Any, pubmed_id: Any) -> str:

    """Return a compact runtime label for API worker log messages."""

    return f'OS.process_id:{os.getpid()}\tThread.id:{threading.get_ident()}\tId:{article_id} - pubmed_id:{pubmed_id}'


def get_nhs_extract(texts: Sequence[str]) -> Optional[bool]:

    """
    Predict whether the supplied publication text is NIH/NHS-related.

    Args:
        texts: Text payload expected by the NHS prediction API.

    Returns:
        True or False when the API returns a usable prediction. None means the
        API call failed or returned an invalid payload, so the caller saves
        is_NHS as NULL while retaining any available EPI results.
    """

    def parse_api_response(response: requests.Response) -> Optional[bool]:
        try:
            nhs_info = response.json()
        except ValueError as e:
            print(f'Invalid NHS prediction JSON response: {e}')
            return None

        if not isinstance(nhs_info, dict):
            print(f'Unexpected NHS prediction response type: {type(nhs_info).__name__}')
            return None

        predictions = nhs_info.get('predictions')
        if predictions is None:
            print('NHS prediction response is missing predictions.')
            return None

        try:
            # A missing decision is unknown, never a negative NHS classification.
            if predictions[0] not in (0, 1):
                raise ValueError("NHS prediction is neither 0 nor 1.")
            return predictions[0] == 1
        except (IndexError, TypeError) as e:
            print(f'Unable to read NHS prediction value: {e}')
            return None

    api_url = os.getenv('NHS_PREDICT_API')
    if not api_url:
        print('NHS_PREDICT_API is not configured.')
        return None

    payload = {'texts': texts}

    return HttpsUtil.with_api_retry(api_url, payload, parse_api_response)


def get_is_epi(text: str) -> Optional[Dict[str, Any]]:

    """
    Predict whether publication text describes epidemiology.

    Args:
        text: Combined publication title and abstract text.

    Returns:
        A dictionary with isEpi and probability keys when the API returns a usable
        prediction. None means the API call failed or returned an invalid payload,
        so the caller saves NULL for the EPI fields and continues with any
        available NHS prediction without repeating the GARD node.
    """
    def parse_api_response(response: requests.Response) -> Optional[Dict[str, Any]]:
        try:
            prediction = response.json()
        except ValueError as e:
            print(f'Invalid EPI classification JSON response: {e}')
            return None

        if not isinstance(prediction, dict):
            print(f'Unexpected EPI classification response type: {type(prediction).__name__}')
            return None

        if 'IsEpi' not in prediction:
            print('EPI classification response is missing IsEpi.')
            return None
        if not isinstance(prediction['IsEpi'], bool):
            raise ValueError("EPI classification is not a boolean decision.")

        return {
            'isEpi': prediction.get('IsEpi'),
            'probability': prediction.get('EPI_PROB')
        }

    api_url = os.getenv('EPI_CLASSIFY_API')
    if not api_url:
        print('EPI_CLASSIFY_API is not configured.')
        return None

    payload = {'text': text}

    return HttpsUtil.with_api_retry(api_url, payload, parse_api_response)


def get_epi_extract(text: str) -> Optional[Dict[str, Any]]:

    """
    Extract epidemiological entities and statistics from publication text.

    Args:
        text: Combined publication title and abstract text.

    Returns:
        A dictionary of extracted epidemiology fields, or None when extraction
        fails. The caller saves a NULL extraction while keeping known decisions.
    """
    def parse_api_response(response: requests.Response) -> Optional[Dict[str, Any]]:
        try:
            epi_extract = response.json()
        except ValueError as e:
            print(f'Exception during get_epi_extract. text: {text}, error: {e}')
            return None

        if not isinstance(epi_extract, dict):
            print(f'Unexpected EPI extraction response type: {type(epi_extract).__name__}')
            return None

        return epi_extract

    api_url = os.getenv('EPI_EXTRACT_API')
    if not api_url:
        print('EPI_EXTRACT_API is not configured.')
        return None

    payload = {'text': text, 'extract_diseases': False}

    return HttpsUtil.with_api_retry(api_url, payload, parse_api_response)


def process_publication_article(obj: Dict[str, Any]) -> Tuple[Optional[bool], Optional[bool], Any, Optional[str], Any]:

    """
    Return known predictions plus None for unavailable HTTP-derived values.
    For example, EPI can succeed while NHS times out: keep (True, None, 0.9,
    extraction_or_None, PMID), rather than discarding the successful EPI value.
    A failed EPI request is unknown, not False, and must not trigger a repeat of
    the whole GARD node. Database writes stay in the parent task.
    """
    article_id, pubmed_id = obj["id"], obj["pubmed_id"]
    text_to_predict = (_to_txt(obj["title"]) + " " + _to_txt(obj["abstract_text"])).strip()
    prefix = _worker_log_prefix(article_id, pubmed_id)
    try:
        prediction = get_is_epi(text_to_predict)
    except requests.RequestException as error:
        print(f"{prefix}\tEPI request unavailable ({type(error).__name__}); saving SQL NULL and continuing.")
        prediction = None
    prediction = prediction if isinstance(prediction, dict) else {}
    is_epi = prediction.get("isEpi")
    if not isinstance(is_epi, bool):
        is_epi = None
    epi_probability = _to_float(prediction.get("probability")) if is_epi is not None else None
    if epi_probability is not None and (not isfinite(epi_probability) or not 0 <= epi_probability <= 1):
        epi_probability = None

    try:
        is_nhs = get_nhs_extract([text_to_predict])
    except requests.RequestException as error:
        print(f"{prefix}\tNHS request unavailable ({type(error).__name__}); saving SQL NULL and continuing.")
        is_nhs = None
    if not isinstance(is_nhs, bool):
        is_nhs = None

    epi_extract = None
    if is_epi:
        try:
            extraction = get_epi_extract(text_to_predict)
        except requests.RequestException as error:
            print(f"{prefix}\tEPI extraction unavailable ({type(error).__name__}); saving SQL NULL and continuing.")
            extraction = None
        if isinstance(extraction, dict):
            epi_extract = json.dumps(extraction)
    print(f"{prefix}\tis_EPI={is_epi}\tepiProbability={epi_probability}\tis_NHS={is_nhs}")
    return (is_epi, is_nhs, epi_probability, epi_extract, pubmed_id)



class PublicationEpiNhsClassificationTask(PipelineBase):

    DEFAULT_WORKER_COUNT = 20
    # Keep the old constant as a compatibility alias for scripts that may import it.
    DEFAULT_PROCESS_COUNT = DEFAULT_WORKER_COUNT

    def __init__(self, fetch_order: str = "ASC", worker_count: Optional[int] = None, process_count: Optional[int] = None):
        super().__init__(init_mysql=True, init_memgraph=False)
        self.fetch_order = fetch_order.upper()
        '''
        These calls are HTTP-bound, so threads are lighter than processes and
        avoid multiprocessing startup/pickle overhead. worker_count is the
        preferred argument name; process_count remains accepted as a legacy
        requested upper bound for older scripts.
        '''
        requested_worker_count = worker_count if worker_count is not None else process_count
        self.worker_count = _resolve_worker_count(
            self.DEFAULT_WORKER_COUNT,
            configured_worker_count=requested_worker_count,
            env_var_name="PUBLICATION_EPI_NHS_API_MAX_WORKERS",
            logger=self.logger
        )
        # Backward-compatible attribute for any external code that still reads it.
        self.process_count = self.worker_count

        if self.fetch_order not in {"ASC", "DESC"}:
            raise ValueError("fetch_order must be ASC or DESC.")


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        self.logger.info("PublicationEpiNhsClassificationTask does not use find_new_data().")


    # implement
    def process_new_data(self) -> None:

        '''
        NULL is a valid unknown outcome after an unavailable HTTP request.
        Advance by primary key so even an all-NULL batch is visited once and
        cannot repeatedly select the same failed API inputs for days.
        '''
        comparison = ">" if self.fetch_order == "ASC" else "<"
        last_id = None
        fetch_is_new_query = f"SELECT id, pubmed_id, title, abstract_text FROM publication_article WHERE is_EPI IS NULL AND is_new = 1 AND (%s IS NULL OR id {comparison} %s) ORDER BY id {self.fetch_order} LIMIT %s"

        update_sql = " UPDATE publication_article SET is_EPI = %s, is_NHS = %s, epi_probability =%s, epi_extract = %s WHERE pubmed_id = %s "

        update_cursor = self.mysql.cursor()

        fetch_cursor = self.mysql.cursor(dictionary=True)
        self.logger.info(f"Fetching new publication_article rows in id {self.fetch_order} order.")

        batch_num = 0
        '''
        Keep the fetch batch aligned with the thread pool size so each worker
        receives at most one API-bound article at a time. The actual worker
        count is resolved at task startup from PUBLICATION_EPI_NHS_API_MAX_WORKERS
        or DEFAULT_WORKER_COUNT, capped to CPU count minus two reserved CPUs.
        '''
        batch_size = self.worker_count

        try:
            with ThreadPoolExecutor(max_workers=self.worker_count) as executor:
                while True:

                    '''
                    Fetch one small batch at a time instead of opening one large
                    cursor over all remaining rows. This lets the ascending and
                    descending standalone scripts run at the same time: after
                    each commit, the next SELECT only sees rows where is_EPI is
                    still NULL.
                    '''
                    fetch_cursor.execute(fetch_is_new_query, (last_id, last_id, batch_size))
                    rows = fetch_cursor.fetchall()

                    batch_num += 1
                    self.logger.info(f'\n--- batch# = {batch_num} ---')

                    if not rows:
                        self.logger.info(f"No more rows to fetch.")
                        break

                    last_id = rows[-1]["id"]
                    obj_list = [{
                        'id': row['id'],
                        'title': row['title'],
                        'abstract_text': row['abstract_text'],
                        'pubmed_id': row['pubmed_id']
                    } for row in rows]

                    try:
                        processed_values = list(executor.map(process_publication_article, obj_list))
                        val_list = [value for value in processed_values if value is not None]
                        skipped_count = len(processed_values) - len(val_list)
                        if skipped_count:
                            self.logger.warning(f"Skipped {skipped_count} publication_article rows in batch#{batch_num} because EPI classification failed; unknown fields stay NULL and processing advances to the next batch.")
                        print(val_list)
                    except Exception as e:
                        self.logger.error(f"Error processing batch#{batch_num}: {e}")
                        continue

                    try:
                        if not val_list:
                            self.logger.warning(f"No publication_article rows updated for batch#{batch_num}; unknown fields remain NULL; continuing with the next batch.")
                            continue

                        update_cursor.executemany(update_sql, val_list)
                        self.mysql.commit()

                    except Exception as e:
                        self.logger.error(f"Error during update: {e}")
                        self.mysql.rollback()
                        continue

        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            if update_cursor:
                update_cursor.close()

            # Explicitly close the all the db connections
            self.close()

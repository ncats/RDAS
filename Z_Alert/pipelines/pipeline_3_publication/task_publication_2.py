import os
import sys
from typing import Any, Dict, Optional, Sequence, Tuple
import requests
import json
from multiprocessing import Pool
from utils.https_request import HTTPSUtils as HttpsUtil
from utils.tools import _to_txt

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
These are module-level functions, not class methods, and keeping them outside PublicationEpiNhsClassificationTask is correct for multiprocessing.
'''

def get_nhs_extract(texts: Sequence[str]) -> bool:
    """
    Predict whether the supplied publication text is NIH/NHS-related.

    Args:
        texts: Text payload expected by the NHS prediction API.

    Returns:
        True when the first prediction is positive; otherwise False.
    """

    def parse_api_response(response: requests.Response) -> bool:
        try:
            nhs_info = response.json()
        except ValueError as e:
            print(f'Invalid NHS prediction JSON response: {e}')
            return False

        if not isinstance(nhs_info, dict):
            print(f'Unexpected NHS prediction response type: {type(nhs_info).__name__}')
            return False

        predictions = nhs_info.get('predictions')
        if not predictions:
            return False

        try:
            return predictions[0] == 1
        except (IndexError, TypeError) as e:
            print(f'Unable to read NHS prediction value: {e}')
            return False

    api_url = os.getenv('NHS_PREDICT_API')
    if not api_url:
        print('NHS_PREDICT_API is not configured.')
        return False

    payload = {'texts': texts}

    return bool(HttpsUtil.with_api_retry(api_url, payload, parse_api_response))


def get_is_epi(text: str) -> Optional[Dict[str, Any]]:
    """
    Predict whether publication text describes epidemiology.

    Args:
        text: Combined publication title and abstract text.

    Returns:
        A dictionary with isEpi and probability keys when the API returns a usable
        prediction. None means the API call failed or returned an invalid payload,
        so the caller should leave the database row unchanged and retry later.
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
        A dictionary of extracted epidemiology fields, or None when extraction fails.
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


def process_publication_article(obj: Dict[str, Any]) -> Optional[Tuple[bool, bool, Any, Optional[str], Any]]:
    """
    Run all publication classifiers/extractors for one article row.

    Args:
        obj: Article data containing id, pubmed_id, title, and abstract_text.

    Returns:
        Tuple matching the publication_article update statement when the EPI
        prediction succeeds: is_epi, is_nhs, epi_probability, epi_extract,
        pubmed_id. None means the EPI prediction failed, and the caller should
        skip the update so is_EPI stays NULL for a later retry.
    """

    article_id = obj['id']
    pubmed_id = obj['pubmed_id']
    title = _to_txt(obj['title'])
    abstract_text = _to_txt(obj['abstract_text'])

    text_to_predict = (title + ' ' + abstract_text).strip()

    epi_prediction = get_is_epi(text_to_predict)
    if epi_prediction is None:
        print(f'OS.process_id:{os.getpid()}\tId:{article_id} - pubmed_id:{pubmed_id}\tEPI classification failed; database row will not be updated.')
        return None

    is_epi = epi_prediction['isEpi']
    epi_probability = epi_prediction['probability']

    is_nhs = get_nhs_extract([text_to_predict])

    print(f'OS.process_id:{os.getpid()}\tId:{article_id} - pubmed_id:{pubmed_id}\tis_EPI={is_epi}\tepiProbability={epi_probability}\tis_NHS={is_nhs}')

    epi_extract = None

    if is_epi:
        epi_extract_json = get_epi_extract(text_to_predict)
        if epi_extract_json:
            epi_extract = json.dumps(epi_extract_json)
            print(f'\t\t{epi_extract}')

    return (is_epi, is_nhs, epi_probability, epi_extract, pubmed_id)



class PublicationEpiNhsClassificationTask(PipelineBase):


    def __init__(self, fetch_order: str = "ASC"):
        super().__init__(init_mysql=True, init_memgraph=False)
        self.fetch_order = fetch_order.upper()

        if self.fetch_order not in {"ASC", "DESC"}:
            raise ValueError("fetch_order must be ASC or DESC.")


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        self.logger.info("PublicationEpiNhsClassificationTask does not use find_new_data().")


    # implement
    def process_new_data(self) -> None:
        
        fetch_is_new_query = f'SELECT id, pubmed_id, title, abstract_text FROM publication_article WHERE is_EPI is null AND is_new = 1 ORDER BY id {self.fetch_order} LIMIT %s'

        update_sql = " UPDATE publication_article SET is_EPI = %s, is_NHS = %s, epi_probability =%s, epi_extract = %s WHERE pubmed_id = %s "

        
        update_cursor = self.mysql.cursor()    

        fetch_cursor = self.mysql.cursor(dictionary=True)
        self.logger.info(f"Fetching new publication_article rows in id {self.fetch_order} order.")

        batch_num = 0
        batch_size = 20

        try: 
            with Pool(processes=batch_size) as active_pool:
                while True:

                    '''
                    Fetch one small batch at a time instead of opening one large
                    cursor over all remaining rows. This lets the ascending and
                    descending standalone scripts run at the same time: after
                    each commit, the next SELECT only sees rows where is_EPI is
                    still NULL.
                    '''
                    fetch_cursor.execute(fetch_is_new_query, (batch_size,))
                    rows = fetch_cursor.fetchall()

                    batch_num += 1
                    self.logger.info(f'\n--- batch# = {batch_num} ---')

                    if not rows:
                        self.logger.info(f"No more rows to fetch.")
                        break
 
                    obj_list = [{
                        'id': row['id'],
                        'title': row['title'],
                        'abstract_text': row['abstract_text'],
                        'pubmed_id': row['pubmed_id']
                    } for row in rows]

                    try:
                        processed_values = active_pool.map(process_publication_article, obj_list)
                        val_list = [value for value in processed_values if value is not None]
                        skipped_count = len(processed_values) - len(val_list)
                        if skipped_count:
                            self.logger.warning(f"Skipped {skipped_count} publication_article rows in batch#{batch_num} because EPI classification failed; is_EPI remains NULL so they can be retried.")
                        print(val_list)
                    except Exception as e:
                        self.logger.error(f"Error processing batch#{batch_num}: {e}")
                        continue
 
                    try:
                        if not val_list:
                            self.logger.warning(f"No publication_article rows updated for batch#{batch_num}; all rows remain retryable.")
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

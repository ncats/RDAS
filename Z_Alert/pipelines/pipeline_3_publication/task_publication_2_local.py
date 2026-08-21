import json
import os
import sys
from typing import Any, Dict, List, Optional, Sequence

from dotenv import load_dotenv

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
    os.path.abspath(os.path.join(_dir, "../../..")),
])

load_dotenv()

from X_epi_nhs_predict_local import (
    NhsPredictionUnavailable,
    get_pipeline,
    postEpiExtractText,
    predict_article,
)
from pipelines.pipeline_base import PipelineBase
from utils.tools import _to_txt

"""
Local replacement for task_publication_2.py.

Updates is_EPI, is_NHS, epi_probability, and epi_extract for new
publication_article rows, but replaces these HTTP calls with X_epi_nhs_predict_local:

    EPI_CLASSIFY_API -> X_epi_nhs_predict_local classifier
    EPI_EXTRACT_API -> X_epi_nhs_predict_local extractor
    NHS_PREDICT_API -> X_epi_nhs_predict_local Natural History Study predictor
"""


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default

    try:
        parsed = int(value)
    except ValueError:
        print(f"{name} must be an integer. Using default {default}.")
        return default

    if parsed <= 0:
        print(f"{name} must be greater than zero. Using default {default}.")
        return default

    return parsed


def _publication_text(obj: Dict[str, Any]) -> str:
    title = _to_txt(obj["title"])
    abstract_text = _to_txt(obj["abstract_text"])
    return (title + " " + abstract_text).strip()


def get_nhs_extracts(texts: Sequence[str]) -> Optional[List[bool]]:
    """
    Predict whether supplied publication text is NIH/NHS-related locally.

    Returns None when no local NHS predictor is available, so the caller can
    still write the EPI fields and leave is_NHS NULL.
    """
    try:
        nhs_info = predict_article(texts)
    except NhsPredictionUnavailable:
        return None
    except Exception as e:
        print(f"NHS local prediction failed: {e}")
        return None

    predictions = nhs_info.get("predictions")
    if predictions is None:
        print("Local NHS prediction response is missing predictions.")
        return None

    if len(predictions) != len(texts):
        print("Local NHS prediction response has a different number of predictions than inputs.")
        return None

    try:
        return [prediction == 1 for prediction in predictions]
    except TypeError as e:
        print(f"Unable to read local NHS prediction values: {e}")
        return None


def get_is_epi(text: str) -> Optional[Dict[str, Any]]:
    try:
        prediction = get_pipeline().post_epi_classify_text(text)
    except Exception as e:
        print(f"Local EPI classification failed: {e}")
        return None

    if not isinstance(prediction, dict):
        print(f"Unexpected local EPI classification response type: {type(prediction).__name__}")
        return None

    if "IsEpi" not in prediction:
        print("Local EPI classification response is missing IsEpi.")
        return None

    return {
        "isEpi": prediction.get("IsEpi"),
        "probability": prediction.get("EPI_PROB"),
    }


def get_epi_extract(text: str) -> Optional[Dict[str, Any]]:
    try:
        epi_extract = postEpiExtractText(text, extract_diseases=False)
    except Exception as e:
        print(f"Local EPI extraction failed. text: {text}, error: {e}")
        return None

    if not isinstance(epi_extract, dict):
        print(f"Unexpected local EPI extraction response type: {type(epi_extract).__name__}")
        return None

    return epi_extract


class PublicationEpiNhsLocalClassificationTask(PipelineBase):

    def __init__(self, fetch_order: str = "ASC"):
        super().__init__(init_mysql=True, init_memgraph=False)
        self.fetch_order = fetch_order.upper()

        if self.fetch_order not in {"ASC", "DESC"}:
            raise ValueError("fetch_order must be ASC or DESC.")

    def find_new_data(self, gard_node) -> None:
        self.logger.info("PublicationEpiNhsLocalClassificationTask does not use find_new_data().")

    def process_new_data(self) -> None:
        fetch_is_new_query = f"SELECT id, pubmed_id, title, abstract_text FROM publication_article WHERE is_EPI is null AND is_new = 1 ORDER BY id {self.fetch_order} LIMIT %s"
        update_sql = " UPDATE publication_article SET is_EPI = %s, is_NHS = %s, epi_probability =%s, epi_extract = %s WHERE pubmed_id = %s "

        update_cursor = self.mysql.cursor()
        fetch_cursor = self.mysql.cursor(dictionary=True)
        self.logger.info(f"Fetching new publication_article rows in id {self.fetch_order} order.")

        batch_num = 0
        batch_size = _env_int("PUBLICATION_LOCAL_FETCH_BATCH_SIZE", 500)
        classifier_batch_size = _env_int("EPI_CLASSIFY_LOCAL_BATCH_SIZE", 32)

        try:
            classifier = get_pipeline()

            while True:
                fetch_cursor.execute(fetch_is_new_query, (batch_size,))
                rows = fetch_cursor.fetchall()

                batch_num += 1
                self.logger.info(f"\n--- batch# = {batch_num} ---")

                if not rows:
                    self.logger.info("No more rows to fetch.")
                    break

                obj_list = [{
                    "id": row["id"],
                    "title": row["title"],
                    "abstract_text": row["abstract_text"],
                    "pubmed_id": row["pubmed_id"],
                } for row in rows]
                texts_to_predict = [_publication_text(obj) for obj in obj_list]

                try:
                    epi_predictions = classifier.classify_texts(
                        texts_to_predict,
                        batch_size=classifier_batch_size,
                    )
                    nhs_predictions = get_nhs_extracts(texts_to_predict)
                    if nhs_predictions is None:
                        self.logger.warning(
                            f"NHS local prediction unavailable for batch#{batch_num}; "
                            "is_NHS will be updated as NULL."
                        )
                        nhs_predictions = [None] * len(obj_list)
                    val_list = []

                    for obj, text_to_predict, prediction, is_nhs in zip(obj_list, texts_to_predict, epi_predictions, nhs_predictions):
                        article_id = obj["id"]
                        pubmed_id = obj["pubmed_id"]
                        epi_prediction = {
                            "isEpi": prediction.get("IsEpi"),
                            "probability": prediction.get("EPI_PROB"),
                        }

                        is_epi = epi_prediction["isEpi"]
                        epi_probability = epi_prediction["probability"]

                        epi_info_message = f"OS.process_id:{os.getpid()}\tId:{article_id} - pubmed_id:{pubmed_id}\tis_EPI={is_epi}\tepiProbability={epi_probability}\tis_NHS={is_nhs}"
                        self.logger.info(epi_info_message)

                        epi_extract = None
                        if is_epi:
                            epi_extract_json = get_epi_extract(text_to_predict)
                            if epi_extract_json is None:
                                epi_extract_failure_message = f"OS.process_id:{os.getpid()}\tId:{article_id} - pubmed_id:{pubmed_id}\tEPI extraction failed; database row will not be updated."
                                self.logger.warning(epi_extract_failure_message)
                                continue

                            epi_extract = json.dumps(epi_extract_json)
                            self.logger.info(f"\t\t{epi_extract}")

                        val_list.append((is_epi, is_nhs, epi_probability, epi_extract, pubmed_id))

                    skipped_count = len(obj_list) - len(val_list)
                    if skipped_count:
                        self.logger.warning(
                            f"Skipped {skipped_count} publication_article rows in batch#{batch_num}; "
                            "EPI extraction failed, so is_EPI remains NULL and they can be retried."
                        )
                    self.logger.info(f"Prepared {len(val_list)} publication_article updates for batch#{batch_num}.")

                except Exception as e:
                    self.logger.error(f"Error processing batch#{batch_num}: {e}")
                    continue

                try:
                    if not val_list:
                        self.logger.warning(
                            f"No publication_article rows updated for batch#{batch_num}; "
                            "all rows remain retryable."
                        )
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

            self.close()


PublicationEpiNhsClassificationTask = PublicationEpiNhsLocalClassificationTask

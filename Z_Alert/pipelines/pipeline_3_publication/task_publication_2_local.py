import json
import os
import sys
from time import sleep
from math import isfinite
from typing import Any, Dict, List, Optional, Sequence

from dotenv import load_dotenv
from mysql.connector import InterfaceError

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
from baseclass.conn import DBConnection
from pipelines.pipeline_base import PipelineBase
from utils.tools import _to_float, _to_txt

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


def get_nhs_extracts(texts: Sequence[str]) -> Optional[List[Optional[bool]]]:

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

    if not isinstance(nhs_info, dict):
        return None
    predictions = nhs_info.get("predictions")
    if predictions is None:
        print("Local NHS prediction response is missing predictions.")
        return None

    if len(predictions) != len(texts):
        print("Local NHS prediction response has a different number of predictions than inputs.")
        return None

    try:
        # Missing/invalid individual decisions are unknown, not negative NHS classifications.
        return [prediction == 1 if prediction in (0, 1) else None for prediction in predictions]
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

        # Borrow only during database phases; model work must not hold a pool slot.
        super().__init__(init_mysql=False, init_memgraph=False)
        self.fetch_order = fetch_order.upper()

        if self.fetch_order not in {"ASC", "DESC"}:
            raise ValueError("fetch_order must be ASC or DESC.")

    def find_new_data(self, gard_node) -> None:

        self.logger.info("PublicationEpiNhsLocalClassificationTask does not use find_new_data().")

    def _execute_mysql_batch(self, query, values, fetch_rows: bool = False):

        """Use a short connection lease and retry only interrupted connections.

        Both SELECT and UPDATE finish and return their handles to the shared
        pool before model loading, classification or EPI extraction. Borrow a
        fresh handle on every attempt: close() invalidates a pooled handle,
        so ping(reconnect=True) on the previously released object is invalid.
        The pool checks/reconnects its physical connection on the next checkout.

        UPDATE retries reuse the already prepared values, without rerunning
        inference. For example, if a 500-article batch loses its connection at
        COMMIT, replaying the same field assignments is safe even if the server
        committed but its acknowledgement was lost. There are no inserts,
        counters or is_new changes in this operation. Session reset on return
        discards uncommitted updates; cleanup failures are logged separately
        so they cannot hide the original database error.
        """
        for attempt in range(1, 4):
            cursor = None
            try:
                self.mysql = DBConnection().mysql_conn()
                cursor = self.mysql.cursor(dictionary=fetch_rows)
                if fetch_rows:
                    cursor.execute(query, values)
                    result = cursor.fetchall()
                else:
                    cursor.executemany(query, values)
                    result = cursor.rowcount
                self.mysql.commit()
                return result
            except Exception as error:
                '''
                The installed C extension can raise its own MySQLInterfaceError
                at COMMIT, rather than mysql.connector.Error. Inspect errno on
                either exception type, and let SQL/data/model errors propagate.
                Recovered disconnects must be warnings: the GARD runner treats
                every ERROR log entry as an unsuccessful task.

                Pool checkout may wrap a failed reconnect in InterfaceError
                without an errno. Retry that connection failure as well as
                interrupted queries; SQL/data errors and pool exhaustion still
                propagate immediately instead of repeating expensive inference.
                '''
                connection_error = getattr(error, "errno", None) in {2002, 2003, 2006, 2013, 2055, 4031}
                reconnect_error = isinstance(error, InterfaceError) and getattr(error, "errno", None) in {-1, None}
                if not (connection_error or reconnect_error) or attempt == 3:
                    raise
                self.logger.warning(f"Publication MySQL connection interrupted on attempt {attempt}/3: {error}; retrying the same database batch without repeating model inference.")
            finally:
                mysql = self.mysql
                self.mysql = None
                for name, resource in (("cursor", cursor), ("connection", mysql)):
                    if resource is not None:
                        try:
                            resource.close()
                        except Exception as cleanup_error:
                            self.logger.warning(f"Unable to close publication MySQL {name}: {cleanup_error}")
            sleep(2 * attempt)

    def process_new_data(self) -> None:

        '''
        Visit each pending Article once during this task, even when model data
        is unavailable and is_EPI must stay NULL. The id boundary prevents a
        failed batch from being fetched repeatedly. ASC and DESC both use the
        existing primary key; the eligibility filters remain unchanged.
        For example, IDs 10/11 can retain NULL predictions while ID 12 and all
        later batches still run. No failed-request queue or GARD replay is used.
        '''
        comparison = ">" if self.fetch_order == "ASC" else "<"
        fetch_is_new_query = f"SELECT id, pubmed_id, title, abstract_text FROM publication_article WHERE is_EPI IS NULL AND is_new = 1 AND (%s IS NULL OR id {comparison} %s) ORDER BY id {self.fetch_order} LIMIT %s"
        update_sql = "UPDATE publication_article SET is_EPI = %s, is_NHS = %s, epi_probability = %s, epi_extract = %s WHERE pubmed_id = %s"
        self.logger.info(f"Fetching new publication_article rows in id {self.fetch_order} order.")
        batch_num = 0
        last_id = None
        batch_size = _env_int("PUBLICATION_LOCAL_FETCH_BATCH_SIZE", 500)
        classifier_batch_size = _env_int("EPI_CLASSIFY_LOCAL_BATCH_SIZE", 32)
        classifier = None
        classifier_load_attempted = False

        try:
            while True:
                rows = self._execute_mysql_batch(fetch_is_new_query, (last_id, last_id, batch_size), fetch_rows=True)
                if not rows:
                    self.logger.info("No more rows to fetch.")
                    break
                last_id = rows[-1]["id"]
                batch_num += 1
                self.logger.info(f"\n--- batch# = {batch_num} ---")
                texts_to_predict = [_publication_text(row) for row in rows]

                '''
                Loading an optional predictor can itself download model files.
                If loading fails, leave EPI fields unknown for this invocation
                rather than retrying the same download for every database batch.
                A later task instance can try again; successful cached models
                still survive normal task boundaries as before. Database calls
                stay outside these model-only exception handlers.
                '''
                if not classifier_load_attempted:
                    classifier_load_attempted = True
                    try:
                        classifier = get_pipeline()
                    except Exception as error:
                        self.logger.warning(f"EPI classifier unavailable ({type(error).__name__}); saving unknown EPI fields as SQL NULL and continuing.")

                epi_predictions = [None] * len(rows)
                if classifier is not None:
                    try:
                        predictions = classifier.classify_texts(texts_to_predict, batch_size=classifier_batch_size)
                        if not isinstance(predictions, list) or len(predictions) != len(rows):
                            raise ValueError("EPI prediction count does not match the Article batch.")
                        epi_predictions = predictions
                    except Exception as error:
                        self.logger.warning(f"EPI prediction unavailable for batch#{batch_num} ({type(error).__name__}); saving SQL NULL and continuing.")

                try:
                    nhs_predictions = get_nhs_extracts(texts_to_predict)
                    if not isinstance(nhs_predictions, list) or len(nhs_predictions) != len(rows):
                        raise ValueError("NHS predictions are unavailable or have an invalid count.")
                except Exception as error:
                    self.logger.warning(f"NHS prediction unavailable for batch#{batch_num} ({type(error).__name__}); saving is_NHS as SQL NULL and continuing.")
                    nhs_predictions = [None] * len(rows)

                val_list = []
                for row, text, prediction, is_nhs in zip(rows, texts_to_predict, epi_predictions, nhs_predictions):
                    prediction = prediction if isinstance(prediction, dict) else {}
                    is_epi = prediction.get("IsEpi")
                    if not isinstance(is_epi, bool):
                        is_epi = None
                    epi_probability = _to_float(prediction.get("EPI_PROB")) if is_epi is not None else None
                    if epi_probability is not None and (not isfinite(epi_probability) or not 0 <= epi_probability <= 1):
                        epi_probability = None
                    if not isinstance(is_nhs, bool):
                        is_nhs = None
                    self.logger.info(f"OS.process_id:{os.getpid()}\tId:{row['id']} - pubmed_id:{row['pubmed_id']}\tis_EPI={is_epi}\tepiProbability={epi_probability}\tis_NHS={is_nhs}")

                    epi_extract = None
                    if is_epi:
                        try:
                            extraction = get_epi_extract(text)
                            if not isinstance(extraction, dict):
                                raise ValueError("EPI extraction is unavailable.")
                            epi_extract = json.dumps(extraction)
                            self.logger.info(f"\t\t{epi_extract}")
                        except Exception as error:
                            '''
                            Keep the known EPI/NHS classifications even if NER
                            extraction failed. For example, (True, False, 0.9,
                            None, PMID) records a real EPI decision plus unknown
                            extraction, without fabricating an empty JSON result.
                            '''
                            self.logger.warning(f"EPI extraction unavailable for pubmed_id={row['pubmed_id']} ({type(error).__name__}); saving epi_extract as SQL NULL and continuing.")
                    val_list.append((is_epi, is_nhs, epi_probability, epi_extract, row["pubmed_id"]))

                # Python None is SQL NULL; even an all-unknown batch advances by last_id.
                self._execute_mysql_batch(update_sql, val_list)
                self.logger.info(f"Committed {len(val_list)} publication_article classification updates for batch#{batch_num}.")
        except Exception:
            self.logger.exception("Publication database update failed; unfinished rows remain pending.")
            raise
        finally:
            self.close()


PublicationEpiNhsClassificationTask = PublicationEpiNhsLocalClassificationTask

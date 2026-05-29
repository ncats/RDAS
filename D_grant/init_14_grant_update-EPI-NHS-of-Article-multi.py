"""
Update EPI/NHS classification fields for grant publications staged outside Article.

Reference:
    E_followup/2_batch_update-EPI-NHS-of-publications-multi.py

This grant-specific script updates rows in grant_publication_not_in_article,
which is expected to have the same article columns as publication_article:
    is_EPI
    is_NHS
    epi_probability
    epi_extract

Reference note kept from the source script:
    Batch update, for large numbers of rows where is_EPI/is_NHS are NULL.

Manual setup note from the source script:
    UPDATE rdas_db.publication_gard_searchterm_pubmed_mapping pgs
    JOIN rdas_db.gard g
        ON pgs.search_term = g.Label
    SET pgs.is_abbreviation = 1
    WHERE g.Label_Predicate_Mapping LIKE 'ABBRE%'
      AND pgs.is_abbreviation IS NULL;
"""

import json
import os
import sys
import time
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import mysql.connector
from dotenv import load_dotenv

# Add the project root to the Python path so this file can be run directly:
# python D_grant/init_14_grant_update-EPI-NHS-of-Article-multi.py
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.append(str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

from baseclass.conn import DBConnection as db
from utils.https_request import HTTPSUtils as HttpsUtil
from utils.tools import _id_range_generator, _to_txt, ask_to_continue, elapsed_time


SOURCE_TABLE = "grant_publication_not_in_article"
DEFAULT_ID_STEP = 1
DEFAULT_RANGE_BATCH_SIZE = 100
DEFAULT_PROCESS_COUNT = max(1, min(10, cpu_count() - 1))
DEFAULT_EPI_PREDICTION = {"isEpi": False, "probability": None}

PENDING_BOUNDS_QUERY = f"""
    SELECT
        MIN(id) AS min_id,
        MAX(id) AS max_id
    FROM rdas_db.{SOURCE_TABLE}
    WHERE is_EPI IS NULL
       OR is_NHS IS NULL
"""

PENDING_ARTICLES_QUERY = f"""
    SELECT
        id,
        pubmed_id,
        title,
        abstract_text
    FROM rdas_db.{SOURCE_TABLE}
    WHERE id BETWEEN %s AND %s
      AND pubmed_id IS NOT NULL
      AND (is_EPI IS NULL OR is_NHS IS NULL)
"""

UPDATE_ARTICLE_QUERY = f"""
    UPDATE rdas_db.{SOURCE_TABLE}
    SET
        is_EPI = %s,
        is_NHS = %s,
        epi_probability = %s,
        epi_extract = %s
    WHERE id = %s
"""


def get_nhs_extract(texts: Sequence[str]) -> bool:
    """Predict whether the supplied publication text is natural-history related."""
    def parse_api_response(response: Any) -> bool:
        try:
            nhs_info = response.json()

        except ValueError as exc:
            print(f"Invalid NHS prediction JSON response: {exc}")
            return False

        if not isinstance(nhs_info, dict):
            print(f"Unexpected NHS prediction response type: {type(nhs_info).__name__}")
            return False

        predictions = nhs_info.get("predictions")
        if not predictions:
            return False

        try:
            return predictions[0] == 1

        except (IndexError, TypeError) as exc:
            print(f"Unable to read NHS prediction value: {exc}")
            return False

    api_url = os.getenv("NHS_PREDICT_API")
    if not api_url:
        print("NHS_PREDICT_API is not configured.")
        return False

    return bool(HttpsUtil.with_api_retry(api_url, {"texts": texts}, parse_api_response))


def get_is_epi(text: str) -> Dict[str, Any]:
    """Predict whether publication text describes epidemiology."""
    def parse_api_response(response: Any) -> Dict[str, Any]:
        try:
            prediction = response.json()

        except ValueError as exc:
            print(f"Invalid EPI classification JSON response: {exc}")
            return dict(DEFAULT_EPI_PREDICTION)

        if not isinstance(prediction, dict):
            print(f"Unexpected EPI classification response type: {type(prediction).__name__}")
            return dict(DEFAULT_EPI_PREDICTION)

        return {
            "isEpi": prediction.get("IsEpi", False),
            "probability": prediction.get("EPI_PROB"),
        }

    api_url = os.getenv("EPI_CLASSIFY_API")
    if not api_url:
        print("EPI_CLASSIFY_API is not configured.")
        return dict(DEFAULT_EPI_PREDICTION)

    return HttpsUtil.with_api_retry(api_url, {"text": text}, parse_api_response) or dict(DEFAULT_EPI_PREDICTION)


def get_epi_extract(text: str) -> Optional[Dict[str, Any]]:
    """
    Extract epidemiological information from publication text.

    Example response from the reference script:
        {'DATE': ['1989'], 'LOC': ['Uruguay', 'Brazil'], 'STAT': ['1 in 10000']}
    """
    def parse_api_response(response: Any) -> Optional[Dict[str, Any]]:
        try:
            epi_extract = response.json()

        except ValueError as exc:
            print(f"Exception during get_epi_extract. text: {text}, error: {exc}")
            return None

        if not isinstance(epi_extract, dict):
            print(f"Unexpected EPI extraction response type: {type(epi_extract).__name__}")
            return None

        return epi_extract

    api_url = os.getenv("EPI_EXTRACT_API")
    if not api_url:
        print("EPI_EXTRACT_API is not configured.")
        return None

    return HttpsUtil.with_api_retry(api_url, {"text": text, "extract_diseases": False}, parse_api_response)


def process_article(obj: Dict[str, Any]) -> Tuple[bool, bool, Any, Optional[str], Any]:
    """Run EPI/NHS prediction and optional EPI extraction for one article row."""
    article_id = obj["id"]
    pubmed_id = obj["pubmed_id"]
    title = _to_txt(obj.get("title"))
    abstract_text = _to_txt(obj.get("abstract_text"))
    text_to_predict = (title + " " + abstract_text).strip()

    if not text_to_predict:
        print(f"OS.process_id:{os.getpid()}\tId:{article_id} - pubmed_id:{pubmed_id}\tblank text; defaulting EPI/NHS to False")
        return (False, False, None, None, article_id)

    epi_prediction = get_is_epi(text_to_predict)
    is_epi = bool(epi_prediction.get("isEpi", False))
    epi_probability = epi_prediction.get("probability")
    is_nhs = get_nhs_extract([text_to_predict])

    print(
        f"OS.process_id:{os.getpid()}\t"
        f"Id:{article_id} - pubmed_id:{pubmed_id}\t"
        f"is_EPI={is_epi}\tepiProbability={epi_probability}\tis_NHS={is_nhs}"
    )

    epi_extract = None
    if is_epi:
        epi_extract_json = get_epi_extract(text_to_predict)
        if epi_extract_json:
            epi_extract = json.dumps(epi_extract_json)
            print(f"\t\t{epi_extract}")

    # Update by id rather than pubmed_id so duplicate PMIDs in the staging table
    # cannot update more rows than the current selected record.
    return (is_epi, is_nhs, epi_probability, epi_extract, article_id)


def get_pending_id_bounds(cursor: Any) -> Optional[Tuple[int, int]]:
    """Return min/max source IDs still missing EPI or NHS classification."""
    cursor.execute(PENDING_BOUNDS_QUERY)
    row = cursor.fetchone()

    if not row or row["min_id"] is None or row["max_id"] is None:
        return None

    return int(row["min_id"]), int(row["max_id"])


def fetch_articles_for_range(cursor: Any, start_id: int, end_id: int) -> List[Dict[str, Any]]:
    """Fetch one ID range of unclassified grant publication rows."""
    cursor.execute(PENDING_ARTICLES_QUERY, (start_id, end_id))
    return cursor.fetchall()


def main() -> int:
    ok = ask_to_continue(f"Update is_EPI, is_NHS, epi_probability, and epi_extract in {SOURCE_TABLE}?")
    if not ok:
        print("Stopped.")
        return 0

    mysql = db().mysql_conn()
    if mysql is None:
        print("Unable to connect to MySQL.")
        return 1

    fetch_cursor = mysql.cursor(dictionary=True)
    update_cursor = mysql.cursor()

    start_time = time.time()
    total_ranges = 0
    failed_ranges = 0
    total_selected = 0
    total_updated = 0

    try:
        bounds = get_pending_id_bounds(fetch_cursor)
        if bounds is None:
            print(f"No pending EPI/NHS rows found in {SOURCE_TABLE}.")
            return 0

        min_id, max_id = bounds
        print(f"{SOURCE_TABLE} pending ID range: [{min_id}-{max_id}]")

        with Pool(processes=DEFAULT_PROCESS_COUNT) as active_pool:
            for start_id, end_id in _id_range_generator(min_id, max_id, DEFAULT_ID_STEP, DEFAULT_RANGE_BATCH_SIZE):
                total_ranges += 1
                range_label = f"[{start_id}-{end_id}]"
                print(f"\n{'=' * 12} Processing ID range {range_label} {'=' * 12}")

                try:
                    rows = fetch_articles_for_range(fetch_cursor, start_id, end_id)

                    if not rows:
                        print(f"{range_label} rows=0")
                        continue

                    total_selected += len(rows)
                    print(f"{range_label} rows={len(rows)}")

                    values = active_pool.map(process_article, rows)
                    if values:
                        update_cursor.executemany(UPDATE_ARTICLE_QUERY, values)
                        mysql.commit()
                        total_updated += len(values)

                    print(
                        f"{range_label} complete: "
                        f"selected={len(rows)}, updated={len(values)}, total_updated={total_updated}"
                    )

                except mysql.connector.Error as exc:
                    failed_ranges += 1
                    mysql.rollback()
                    print(f"Database error for range {range_label}: {exc}")

                except Exception as exc:
                    failed_ranges += 1
                    mysql.rollback()
                    print(f"Processing error for range {range_label}: {exc}")

    finally:
        update_cursor.close()
        fetch_cursor.close()
        mysql.close()

    end_time = time.time()
    hours, minutes, seconds = elapsed_time(start_time, end_time)

    print(f"\n{'=' * 12} Completed {'=' * 12}")
    print(
        f"Ranges={total_ranges}, failed_ranges={failed_ranges}, "
        f"selected={total_selected}, updated={total_updated}, "
        f"elapsed={hours} hours, {minutes} minutes, {seconds} seconds"
    )

    return 1 if failed_ranges else 0


if __name__ == "__main__":
    raise SystemExit(main())

"""
Add PubTator JSON for grant publications staged outside publication_article.

Reference:
    E_followup/1_pmids_not_in_Article_table_ADD_pubtator.py

This script is the grant-specific version of that follow-up script:
    1. Read PMIDs from grant_publication_not_in_article.
    2. Skip PMIDs already cached in publication_pubtator.
    3. Download PubTator JSON from the PubTator3 API.
    4. Insert raw JSON into publication_pubtator for later parsing.

The reference script keeps the PubTator calls single-threaded because
multiprocessing caused HTTP 429 errors:
    Request error: 429 Client Error: Too Many Requests

PubTator3 API usage guidance:
    Do not exceed three requests per second.
    In order not to overload the PubTator3 server, users should post no more
    than three requests per second.
"""

import json
import sys
import time
from pathlib import Path
from typing import Any, List, Optional, Tuple

import mysql.connector

# Add the project root to the Python path so this file can be run directly:
# python D_grant/init_13_grant_publications_not_in_Article_table_pubtator_multi.py
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.append(str(PROJECT_ROOT))

from baseclass.conn import DBConnection as db
from utils.pubtator_worker import PubtatorWorker
from utils.tools import _id_range_generator, ask_to_continue


SOURCE_TABLE = "grant_publication_not_in_article"
TARGET_TABLE = "publication_pubtator"

# Keep the same range size as the reference follow-up script. The range controls
# how many source table IDs are queried at once; the API calls inside each range
# are intentionally throttled one at a time.
DEFAULT_ID_STEP = 1
DEFAULT_RANGE_BATCH_SIZE = 100
DEFAULT_REQUEST_DELAY_SECONDS = 0.5

PENDING_BOUNDS_QUERY = f"""
    SELECT
        MIN(id) AS min_id,
        MAX(id) AS max_id
    FROM rdas_db.{SOURCE_TABLE}
"""

PMIDS_TO_DOWNLOAD_QUERY = f"""
    SELECT DISTINCT
        source.pubmed_id
    FROM rdas_db.{SOURCE_TABLE} AS source
    LEFT JOIN rdas_db.{TARGET_TABLE} AS pubtator
        ON pubtator.pubmed_id = source.pubmed_id
    WHERE source.id BETWEEN %s AND %s
      AND source.pubmed_id IS NOT NULL
      AND pubtator.pubmed_id IS NULL
"""

# The NOT EXISTS guard protects reruns if another process or a previous partial
# run inserted a PubTator row after the current batch was selected.
INSERT_PUBTATOR_QUERY = f"""
    INSERT INTO rdas_db.{TARGET_TABLE} (pubmed_id, source_json)
    SELECT %s, %s
    WHERE NOT EXISTS (
        SELECT 1
        FROM rdas_db.{TARGET_TABLE}
        WHERE pubmed_id = %s
    )
"""


def get_source_id_bounds(cursor: Any) -> Optional[Tuple[int, int]]:
    """Return the min/max source table IDs, or None when the source table is empty."""
    cursor.execute(PENDING_BOUNDS_QUERY)
    row = cursor.fetchone()

    if not row or row["min_id"] is None or row["max_id"] is None:
        return None

    return int(row["min_id"]), int(row["max_id"])


def get_pmids_need_pubtator(cursor: Any, start_id: int, end_id: int) -> List[Any]:
    """Select PMIDs in the source range that do not already have PubTator JSON."""
    cursor.execute(PMIDS_TO_DOWNLOAD_QUERY, (start_id, end_id))
    rows = cursor.fetchall()
    return [row["pubmed_id"] for row in rows if row.get("pubmed_id") is not None]


def serialize_source_json(source_json: Any) -> Optional[str]:
    """Convert PubTatorWorker output into text for the publication_pubtator table."""
    if source_json is None:
        return None

    if isinstance(source_json, str):
        return source_json

    return json.dumps(source_json)


def download_pubtator_batch(worker: PubtatorWorker, pmids: List[Any]) -> List[Tuple[Any, Optional[str], Any]]:
    """Download PubTator JSON serially and throttle requests to avoid 429 errors."""
    values = []

    for pmid in pmids:
        print(".", end=" ", file=sys.stdout, flush=True)

        pubmed_id, source_json = worker.download_by_pmid(pmid)
        values.append((pubmed_id, serialize_source_json(source_json), pubmed_id))

        time.sleep(DEFAULT_REQUEST_DELAY_SECONDS)

    print()
    return values


def main() -> int:
    ok = ask_to_continue(
        f"Add {SOURCE_TABLE} table's PubTator JSON into {TARGET_TABLE}?"
    )
    if not ok:
        print("Stopped.")
        return 0

    mysql = db().mysql_conn()
    if mysql is None:
        print("Unable to connect to MySQL.")
        return 1

    fetch_cursor = mysql.cursor(dictionary=True)
    insert_cursor = mysql.cursor()
    worker = PubtatorWorker()

    total_ranges = 0
    failed_ranges = 0
    total_pmids = 0
    total_insert_attempts = 0
    total_inserted_rows = 0
    total_with_source_json = 0

    try:
        bounds = get_source_id_bounds(fetch_cursor)
        if bounds is None:
            print(f"No rows found in {SOURCE_TABLE}.")
            return 0

        min_id, max_id = bounds
        print(f"{SOURCE_TABLE} ID range: [{min_id}-{max_id}]")

        for start_id, end_id in _id_range_generator(min_id, max_id, DEFAULT_ID_STEP, DEFAULT_RANGE_BATCH_SIZE):
            total_ranges += 1
            range_label = f"[{start_id}-{end_id}]"
            print(f"\n{'=' * 12} Processing ID range {range_label} {'=' * 12}")

            try:
                pmids = get_pmids_need_pubtator(fetch_cursor, start_id, end_id)

                if not pmids:
                    print(f"{range_label} pmids=0")
                    continue

                total_pmids += len(pmids)
                print(f"{range_label} pmids={len(pmids)}")

                values = download_pubtator_batch(worker, pmids)
                total_insert_attempts += len(values)
                total_with_source_json += sum(1 for _, source_json, _ in values if source_json)

                if values:
                    insert_cursor.executemany(INSERT_PUBTATOR_QUERY, values)
                    inserted_rows = insert_cursor.rowcount
                    mysql.commit()
                else:
                    inserted_rows = 0

                total_inserted_rows += inserted_rows

                print(
                    f"{range_label} complete: "
                    f"insert_attempts={len(values)}, "
                    f"inserted_rows={inserted_rows}, "
                    f"with_source_json={sum(1 for _, source_json, _ in values if source_json)}, "
                    f"total_inserted_rows={total_inserted_rows}"
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
        insert_cursor.close()
        fetch_cursor.close()
        mysql.close()

    print(f"\n{'=' * 12} Done {'=' * 12}")
    print(
        f"Ranges={total_ranges}, failed_ranges={failed_ranges}, "
        f"pmids={total_pmids}, insert_attempts={total_insert_attempts}, "
        f"inserted_rows={total_inserted_rows}, with_source_json={total_with_source_json}"
    )

    return 1 if failed_ranges else 0


if __name__ == "__main__":
    raise SystemExit(main())

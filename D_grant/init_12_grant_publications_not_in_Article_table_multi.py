"""
Download grant-linked PMIDs that are not already in publication_article.

Original operational notes kept here because they explain the expected staging
table and the relationship path used by the query.

Create table grant_publication_not_in_article the SAME as publication_article:
    The table grant_publication_not_in_article contains the PMIDs which are in
    Grant but not in publication_article.

Clean the grant_gard_project_relation data by:
    UPDATE rdas_db.grant_gard_project_relation
    SET core_project_num = NULL
    WHERE TRIM(core_project_num) = '';

Criteria:
    grant_publication links to grant_linktable via pmid.
    grant_linktable links to grant_gard_project_relation via project_number and core_project_num.
    grant_gard_project_relation links to grant_gard_project_relation_unique_application_id via application_id.

To retrieve gp.pmid from rdas_db.grant_publication that are not present in both
rdas_db.publication_article and rdas_db.grant_publication_not_in_article:
    SELECT DISTINCT gp.pmid
    FROM rdas_db.grant_publication AS gp
    JOIN rdas_db.grant_linktable AS gl
        ON gl.PMID = gp.PMID
    JOIN rdas_db.grant_gard_project_relation AS gpr
        ON gpr.core_project_num = gl.PROJECT_NUMBER
    JOIN rdas_db.grant_gard_project_relation_unique_application_id AS gpru
        ON gpru.application_id = gpr.application_id
    LEFT JOIN rdas_db.publication_article AS pa
        ON pa.pubmed_id = gp.PMID
    LEFT JOIN rdas_db.grant_publication_not_in_article AS gpn
        ON gpn.pubmed_id = gp.PMID
    WHERE gpru.pmid_processed IS NULL
      AND gpr.core_project_num IS NOT NULL
      AND gp.PMID IS NOT NULL
      AND pa.pubmed_id IS NULL
      AND gpn.pubmed_id IS NULL;

Check duplicates:
    SELECT pubmed_id, COUNT(*) AS cnt
    FROM rdas_db.grant_publication_not_in_article
    GROUP BY pubmed_id
    ORDER BY cnt DESC;
"""

import sys
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Any, List, Optional, Tuple

import mysql.connector

# Add the project root to the Python path so this file can be run directly:
# python D_grant/init_12_grant_publications_not_in_Article_table_multi.py
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.append(str(PROJECT_ROOT))

from baseclass.conn import DBConnection as db
from utils.publication_worker import PublicationWorker
from utils.tools import _id_range_generator, ask_to_continue


# The original comments and confirmation prompt describe this as a staging table.
TARGET_TABLE = "grant_publication_not_in_article"
PROCESSED_FLAG = 1

# Keep the original range size. A small range limits the number of PMIDs queued
# at once and makes each processed-flag commit easy to retry if a range fails.
DEFAULT_ID_STEP = 1
DEFAULT_RANGE_BATCH_SIZE = 5

# The old script used 22 worker processes. Keep that cap, but avoid launching
# more processes than the machine can reasonably use.
DEFAULT_PROCESS_COUNT = max(1, min(22, cpu_count() - 1))

PublicationRow = Tuple[Any, ...]
_PUBLICATION_WORKER: Optional[PublicationWorker] = None

PENDING_BOUNDS_QUERY = """
    SELECT
        MIN(id) AS min_id,
        MAX(id) AS max_id
    FROM rdas_db.grant_gard_project_relation_unique_application_id
    WHERE pmid_processed IS NULL
"""

PMIDS_TO_DOWNLOAD_QUERY = """
    SELECT DISTINCT
        gp.PMID AS pmid
    FROM rdas_db.grant_publication AS gp
    JOIN rdas_db.grant_linktable AS gl
        ON gl.PMID = gp.PMID
    JOIN rdas_db.grant_gard_project_relation AS gpr
        ON gpr.core_project_num = gl.PROJECT_NUMBER
    JOIN rdas_db.grant_gard_project_relation_unique_application_id AS gpru
        ON gpru.application_id = gpr.application_id
    LEFT JOIN rdas_db.publication_article AS pa
        ON pa.pubmed_id = gp.PMID
    LEFT JOIN rdas_db.grant_publication_not_in_article AS gpn
        ON gpn.pubmed_id = gp.PMID
    WHERE gpru.id BETWEEN %s AND %s
      AND gpru.pmid_processed IS NULL
      AND gpr.core_project_num IS NOT NULL
      AND gp.PMID IS NOT NULL
      AND pa.pubmed_id IS NULL
      AND gpn.pubmed_id IS NULL
"""

MARK_RANGE_PROCESSED_QUERY = """
    UPDATE rdas_db.grant_gard_project_relation_unique_application_id
    SET pmid_processed = %s
    WHERE id BETWEEN %s AND %s
      AND pmid_processed IS NULL
"""


def init_publication_worker() -> None:
    """Create one PublicationWorker per process instead of one per PMID."""
    global _PUBLICATION_WORKER
    _PUBLICATION_WORKER = PublicationWorker()


def download_by_pmid(pmid: Any) -> Optional[PublicationRow]:
    """Pool worker wrapper used by multiprocessing.Pool.map."""
    global _PUBLICATION_WORKER

    if _PUBLICATION_WORKER is None:
        _PUBLICATION_WORKER = PublicationWorker()

    return _PUBLICATION_WORKER.download_by_pmid(pmid)


def get_pending_id_bounds(cursor: Any) -> Optional[Tuple[int, int]]:
    """Return the min/max unprocessed work-table IDs, or None when finished."""
    cursor.execute(PENDING_BOUNDS_QUERY)
    row = cursor.fetchone()

    if not row or row["min_id"] is None or row["max_id"] is None:
        return None

    return int(row["min_id"]), int(row["max_id"])


def get_pmids_need_to_download(cursor: Any, start_id: int, end_id: int) -> List[Any]:
    """Fetch distinct missing PMIDs for one grant work-table ID range."""
    cursor.execute(PMIDS_TO_DOWNLOAD_QUERY, (start_id, end_id))
    rows = cursor.fetchall()
    return [row["pmid"] for row in rows if row.get("pmid") is not None]


def mark_range_processed(cursor: Any, start_id: int, end_id: int) -> int:
    """Mark a completed ID range so future runs skip it."""
    cursor.execute(MARK_RANGE_PROCESSED_QUERY, (PROCESSED_FLAG, start_id, end_id))
    return cursor.rowcount


def main() -> int:
    ok = ask_to_continue(
        "Find the Grant publication PMIDs which are not present in publication_article, "
        f"download them, and store into {TARGET_TABLE}?"
    )
    if not ok:
        print("Stopped.")
        return 0

    mysql = db().mysql_conn()
    if mysql is None:
        print("Unable to connect to MySQL.")
        return 1

    fetch_cursor = mysql.cursor(dictionary=True)
    update_cursor = mysql.cursor()
    insert_cursor = mysql.cursor()

    total_ranges = 0
    failed_ranges = 0
    total_pmids_seen = 0
    total_downloaded_rows = 0
    total_inserted_rows = 0

    try:
        bounds = get_pending_id_bounds(fetch_cursor)
        if bounds is None:
            print("No pending grant publication PMID ranges found.")
            return 0

        min_id, max_id = bounds
        print(f"Pending work-table ID range: [{min_id}-{max_id}]")

        insert_sql = PublicationWorker().get_insert_sql(TARGET_TABLE)

        # No overhead of Pool creation: create the Pool once and reuse it for
        # every ID range. pool.map() is blocking, so the DB update for a range
        # happens only after every PMID in that range has finished downloading.
        with Pool(processes=DEFAULT_PROCESS_COUNT, initializer=init_publication_worker) as pool:
            for start_id, end_id in _id_range_generator(min_id, max_id, DEFAULT_ID_STEP, DEFAULT_RANGE_BATCH_SIZE):
                total_ranges += 1
                range_label = f"[{start_id}-{end_id}]"
                print(f"\n{'=' * 12} Processing ID range {range_label} {'=' * 12}")

                try:
                    pmids = get_pmids_need_to_download(fetch_cursor, start_id, end_id)

                    if not pmids:
                        marked_count = mark_range_processed(update_cursor, start_id, end_id)
                        mysql.commit()
                        print(f"{range_label} pmids=0, marked_processed={marked_count}")
                        continue

                    total_pmids_seen += len(pmids)

                    downloaded_rows = pool.map(download_by_pmid, pmids)
                    batch_values = [row for row in downloaded_rows if row is not None]
                    total_downloaded_rows += len(batch_values)

                    if batch_values:
                        insert_cursor.executemany(insert_sql, batch_values)
                        total_inserted_rows += len(batch_values)

                    marked_count = mark_range_processed(update_cursor, start_id, end_id)
                    mysql.commit()

                    print(
                        f"{range_label} complete: "
                        f"pmids={len(pmids)}, "
                        f"downloaded_rows={len(batch_values)}, "
                        f"marked_processed={marked_count}, "
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
        update_cursor.close()
        fetch_cursor.close()
        mysql.close()

    print(f"\n{'=' * 12} All Done {'=' * 12}")
    print(
        f"Ranges={total_ranges}, failed_ranges={failed_ranges}, "
        f"pmids_seen={total_pmids_seen}, downloaded_rows={total_downloaded_rows}, "
        f"inserted_rows={total_inserted_rows}"
    )

    return 1 if failed_ranges else 0


if __name__ == "__main__":
    raise SystemExit(main())

"""
Download grant-linked PubMed articles that are missing from publication_article table.

Missing grant-linked articles are inserted directly into `publication_article` with `is_new = 1`, 
so the downstream publication graph tasks can process them the same way they process
other new publication rows.

Processing flow:
    1. Read pending rows from
       `grant_gard_project_relation_unique_application_id` where `is_new = 1` and `pmid_processed` is NULL.

    2. For each small work-table ID range, find distinct grant publication PMIDs
       connected through grant link-table rows and current GARD-project relationships.

    3. Skip PMIDs that already exist in `publication_article`.

    4. Download missing article metadata from Europe PMC using the shared `PublicationWorker`.

    5. Insert downloaded rows into `publication_article` with `is_new = 1`.

    6. Mark the work-table ID range as `pmid_processed = 1` after the download
       and insert attempts finish for that range.

Required inputs:
    `grant_publication`
        Supplies grant-linked PMIDs from NIH RePORTER publication exports.
    `grant_linktable`
        Links PMIDs to grant project/core project numbers.
    `grant_gard_project_relation`
        Keeps only grant projects that were matched to GARD diseases.
    `grant_gard_project_relation_unique_application_id`
        Work table for current new grant application IDs. This task uses
        `is_new = 1` and updates `pmid_processed`.
    `publication_article`
        Target table for downloaded article metadata.

Environment:
    `EURO_PEPMC_SERVICE_URL` must point at the Europe PMC REST search endpoint.
"""

# Reference: D_grant/init_12_grant_publications_not_in_Article_table_multi.py

import time
from multiprocessing import Pool, cpu_count
from typing import Any, List, Optional, Tuple

from pipelines.pipeline_4_grant.grant_base import GrantPipelineBase
from utils.publication_worker import PublicationWorker
from utils.tools import _id_range_generator, _time_hms


PROCESSED_FLAG = 1
DEFAULT_ID_STEP = 1
DEFAULT_RANGE_BATCH_SIZE = 5
DEFAULT_PROCESS_COUNT = 16

PublicationRow = Tuple[Any, ...]
_PUBLICATION_WORKER: Optional[PublicationWorker] = None

PENDING_BOUNDS_SQL = """
    SELECT
        MIN(gpru.id) AS min_id,
        MAX(gpru.id) AS max_id
    FROM grant_gard_project_relation_unique_application_id AS gpru
    INNER JOIN grant_project AS p
        ON p.APPLICATION_ID = gpru.application_id
        AND p.is_new = 1
    WHERE
        gpru.is_new = 1
        AND gpru.pmid_processed IS NULL
"""

PMIDS_TO_DOWNLOAD_SQL = """
    SELECT DISTINCT
        gp.PMID AS pmid
    FROM grant_publication AS gp
    INNER JOIN grant_linktable AS gl
        ON gl.PMID = gp.PMID
    INNER JOIN grant_gard_project_relation AS gpr
        ON gpr.core_project_num = gl.PROJECT_NUMBER
    INNER JOIN grant_gard_project_relation_unique_application_id AS gpru
        ON gpru.application_id = gpr.application_id
    INNER JOIN grant_project AS p
        ON p.APPLICATION_ID = gpru.application_id
        AND p.is_new = 1
    LEFT JOIN publication_article AS pa
        ON pa.pubmed_id = gp.PMID
    WHERE
        gpru.id BETWEEN %s AND %s
        AND gpru.is_new = 1
        AND gpru.pmid_processed IS NULL
        AND gpr.is_new = 1
        AND gpr.core_project_num IS NOT NULL
        AND TRIM(gpr.core_project_num) <> ''
        AND gp.PMID IS NOT NULL
        AND pa.pubmed_id IS NULL
"""

INSERT_PUBLICATION_ARTICLE_SQL = """
    INSERT INTO publication_article (
        pubmed_id, doi, title, abstract_text, affiliation,
        first_publication_date, publication_year, cited_by_count, is_open_access,
        in_EPMC, in_PMC, has_PDF, pub_type, source_json,
        is_new
    )
    SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1
    WHERE NOT EXISTS (
        SELECT 1
        FROM publication_article AS existing
        WHERE existing.pubmed_id <=> %s
        LIMIT 1
    )
"""

MARK_RANGE_PROCESSED_SQL = """
    UPDATE grant_gard_project_relation_unique_application_id
    SET pmid_processed = %s
    WHERE
        id BETWEEN %s AND %s
        AND is_new = 1
        AND pmid_processed IS NULL
"""


def init_publication_worker() -> None:
    """Create one PublicationWorker per worker process."""

    global _PUBLICATION_WORKER
    _PUBLICATION_WORKER = PublicationWorker()


def download_by_pmid(pmid: Any) -> Optional[PublicationRow]:
    """Download one PMID in a worker process and return an insert-ready row."""

    global _PUBLICATION_WORKER

    if _PUBLICATION_WORKER is None:
        _PUBLICATION_WORKER = PublicationWorker()

    return _PUBLICATION_WORKER.download_by_pmid(pmid)


class GrantPublicationArticleImportTask(GrantPipelineBase):
    """Download missing Article rows for current new grant/GARD relationships."""

    def __init__(self, id_step: int = DEFAULT_ID_STEP, range_batch_size: int = DEFAULT_RANGE_BATCH_SIZE, process_count: int = DEFAULT_PROCESS_COUNT):
        super().__init__(init_mysql=True, init_memgraph=False)
        self.id_step = id_step
        self.range_batch_size = range_batch_size
        self.process_count = process_count


    def find_new_data(self, gard_node) -> None:
        self.logger.info("GrantPublicationArticleImportTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Download and insert missing publication_article rows for new grants."""

        start_time = time.time()
        summary = {
            "ranges_seen": 0,
            "ranges_failed": 0,
            "pmids_seen": 0,
            "downloaded_rows": 0,
            "inserted_rows": 0,
            "work_rows_marked_processed": 0,
        }

        try:
            if not self._validate_runtime_options():
                return

            if self.mysql is None:
                self.logger.error("Unable to create MySQL connection.")
                return

            if not self._publication_download_configured():
                self.logger.error("EURO_PEPMC_SERVICE_URL is not configured. Grant publication article import will stop.")
                return

            bounds = self._get_pending_id_bounds()

            if bounds is None:
                self.logger.info(f"No pending grant publication PMID ranges found. Summary={summary}")
                return

            min_id, max_id = bounds
            self.logger.info(
                "Grant publication article import starting: "
                f"min_id={min_id}, max_id={max_id}, process_count={self.process_count}"
            )

            # Reuse a single pool across all ranges. The initializer used this
            # pattern to avoid repeatedly loading PublicationWorker resources.
            with Pool(processes=self.process_count, initializer=init_publication_worker) as pool:

                for start_id, end_id in _id_range_generator(min_id, max_id, self.id_step, self.range_batch_size):
                    
                    summary["ranges_seen"] += 1
                    range_label = f"[{start_id}-{end_id}]"
                    self.logger.info(f"Processing grant publication PMID range {range_label}.")

                    try:
                        range_summary = self._process_id_range(start_id, end_id, pool)

                        if range_summary is None:
                            summary["ranges_failed"] += 1
                            self.mysql.rollback()
                            continue

                        self.mysql.commit()

                        summary["pmids_seen"] += range_summary["pmid_count"]
                        summary["downloaded_rows"] += range_summary["downloaded_count"]
                        summary["inserted_rows"] += range_summary["inserted_count"]
                        summary["work_rows_marked_processed"] += range_summary["marked_processed_count"]

                        self.logger.info(
                            f"Completed range {range_label}: "
                            f"pmids={range_summary['pmid_count']}, "
                            f"downloaded_rows={range_summary['downloaded_count']}, "
                            f"inserted_rows={range_summary['inserted_count']}, "
                            f"marked_processed={range_summary['marked_processed_count']}"
                        )

                    except Exception:
                        summary["ranges_failed"] += 1
                        self.mysql.rollback()
                        self.logger.exception(f"Grant publication PMID range {range_label} failed. Continuing with next range.")
                        continue

            self.logger.info(f"Completed grant publication article import. Summary={summary}")

        except Exception:
            self.logger.exception(f"GrantPublicationArticleImportTask failed. Summary={summary}")
            return

        finally:
            hours, minutes, seconds = _time_hms(time.time() - start_time)
            self.logger.info(f"Total time elapsed: {hours} hours, {minutes} minutes, {seconds} seconds")
            self.close()


    def _validate_runtime_options(self) -> bool:
        """Validate range and multiprocessing settings before work starts."""

        if self.id_step <= 0:
            self.logger.error("id_step must be greater than 0")
            return False

        if self.range_batch_size <= 0:
            self.logger.error("range_batch_size must be greater than 0")
            return False

        if self.process_count <= 0:
            self.logger.error("process_count must be greater than 0")
            return False

        return True


    def _publication_download_configured(self) -> bool:
        """Return True when PublicationWorker has the Europe PMC base URL."""

        worker = PublicationWorker(self.logger)
        return bool(worker.base_url)


    def _get_pending_id_bounds(self) -> Optional[Tuple[int, int]]:
        """Return min/max pending work-table IDs for current new grant rows."""

        cursor = None

        try:
            cursor = self.mysql.cursor(dictionary=True)
            cursor.execute(PENDING_BOUNDS_SQL)
            row = cursor.fetchone() or {}

        finally:
            if cursor is not None:
                cursor.close()

        if row.get("min_id") is None or row.get("max_id") is None:
            return None

        return int(row["min_id"]), int(row["max_id"])


    def _process_id_range(self, start_id: int, end_id: int, pool: Pool) -> Optional[dict]:
        """Download missing PMIDs, insert article rows, and mark one range."""

        pmids = self._get_pmids_to_download(start_id, end_id)

        if not pmids:
            marked_count = self._mark_range_processed(start_id, end_id)
            return {
                "pmid_count": 0,
                "downloaded_count": 0,
                "inserted_count": 0,
                "marked_processed_count": marked_count,
            }

        downloaded_rows = pool.map(download_by_pmid, pmids)
        batch_values = [
            (*row, row[0])
            for row in downloaded_rows
            if row is not None
        ]
        inserted_count = self._insert_publication_articles(batch_values)
        marked_count = self._mark_range_processed(start_id, end_id)

        if len(batch_values) < len(pmids):
            self.logger.warning(
                f"Downloaded {len(batch_values)} of {len(pmids)} missing grant publication PMIDs "
                f"for work-table range [{start_id}-{end_id}]."
            )

        return {
            "pmid_count": len(pmids),
            "downloaded_count": len(batch_values),
            "inserted_count": inserted_count,
            "marked_processed_count": marked_count,
        }


    def _get_pmids_to_download(self, start_id: int, end_id: int) -> List[Any]:
        """Fetch distinct missing publication_article PMIDs for one ID range."""

        cursor = None

        try:
            cursor = self.mysql.cursor(dictionary=True)
            cursor.execute(PMIDS_TO_DOWNLOAD_SQL, (start_id, end_id))
            rows = cursor.fetchall()
            return [row["pmid"] for row in rows if row.get("pmid") is not None]

        finally:
            if cursor is not None:
                cursor.close()


    def _insert_publication_articles(self, batch_values: List[Tuple[Any, ...]]) -> int:
        """Insert downloaded Article rows with `is_new = 1`, skipping duplicates."""

        if not batch_values:
            return 0

        cursor = None

        try:
            cursor = self.mysql.cursor()
            cursor.executemany(INSERT_PUBLICATION_ARTICLE_SQL, batch_values)

            if cursor.rowcount and cursor.rowcount > 0:
                return cursor.rowcount

            return 0

        finally:
            if cursor is not None:
                cursor.close()


    def _mark_range_processed(self, start_id: int, end_id: int) -> int:
        """Mark one completed current-new grant work-table range as processed."""

        cursor = None

        try:
            cursor = self.mysql.cursor()
            cursor.execute(MARK_RANGE_PROCESSED_SQL, (PROCESSED_FLAG, start_id, end_id))

            if cursor.rowcount and cursor.rowcount > 0:
                return cursor.rowcount

            return 0

        finally:
            if cursor is not None:
                cursor.close()


if __name__ == "__main__":

    task = GrantPublicationArticleImportTask()
    task.process_new_data()

"""
Regenerate missing publication GARD/search-term/PMID mapping rows.

This makeup task reads the raw `publication_gard_pubmed.pubmed_ids` values
directly and inserts rows into `publication_gard_searchterm_pubmed_mapping`
without using GROUP_CONCAT. It keeps the full mapping identity:

    gard_id + search_term + pubmed_id

That means the same GARD/PMID pair is allowed to appear under multiple search
terms. Only exact duplicate triples are skipped, so the script is safe to rerun.

Run from the repository root:

    conda run -n rdas python Z_Alert/pipelines/pipeline_9_makeup/regenerate_publication_gard_searchterm_pubmed_mapping.py

Or, if the `rdas` conda environment is already active:

    python Z_Alert/pipelines/pipeline_9_makeup/regenerate_publication_gard_searchterm_pubmed_mapping.py
"""

import os
import sys
import time
from typing import Dict, Iterable, List, Optional, Set, Tuple


_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..", "..")),
    os.path.abspath(os.path.join(_dir, "..", "..", "..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _time_hms


class PublicationGardSearchtermPubmedMappingRegenerationTask(PipelineBase):
    """Repair the search-term-level publication/GARD mapping table."""

    SOURCE_BATCH_SIZE = 100
    INSERT_BATCH_SIZE = 1000
    MYSQL_LOCK_NAME = "rdas_publication_gard_searchterm_pubmed_mapping_regeneration"

    FETCH_SOURCE_ROWS_QUERY = '''
        SELECT
            id,
            gard_id,
            search_term,
            year_range,
            pubmed_ids
        FROM publication_gard_pubmed
        WHERE year_range != 'ignore'
        AND gard_id IS NOT NULL
        AND TRIM(gard_id) <> ''
        AND search_term IS NOT NULL
        AND TRIM(search_term) <> ''
        AND pubmed_ids IS NOT NULL
        ORDER BY gard_id, search_term, year_range, id
    '''

    FETCH_EXISTING_PUBMED_IDS_QUERY = '''
        SELECT DISTINCT pubmed_id
        FROM publication_gard_searchterm_pubmed_mapping
        WHERE gard_id = %s
        AND search_term = %s
        AND pubmed_id IS NOT NULL
    '''

    INSERT_MAPPING_QUERY = '''
        INSERT INTO publication_gard_searchterm_pubmed_mapping
            (gard_id, search_term, pubmed_id)
        SELECT %s, %s, %s
        WHERE NOT EXISTS (
            SELECT 1
            FROM publication_gard_searchterm_pubmed_mapping
            WHERE gard_id = %s
            AND search_term = %s
            AND pubmed_id = %s
        )
    '''

    def __init__(self):

        super().__init__(init_mysql=True, init_memgraph=False)


    def find_new_data(self, gard_node) -> None:

        self.logger.info("PublicationGardSearchtermPubmedMappingRegenerationTask does not use find_new_data().")


    def process_new_data(self) -> None:

        select_cursor = None
        check_cursor = None
        insert_cursor = None
        lock_acquired = False
        start_time = time.time()
        summary = {
            "source_rows_seen": 0,
            "groups_seen": 0,
            "source_group_pubmed_ids_seen": 0,
            "existing_exact_mappings_seen": 0,
            "exact_duplicates_skipped": 0,
            "rows_inserted": 0,
            "invalid_pubmed_ids_seen": 0,
            "empty_groups_seen": 0,
        }

        try:
            if self.mysql is None:
                self._log_progress("Unable to create MySQL connection.")
                return

            lock_acquired = self._acquire_mysql_lock()

            if not lock_acquired:
                self._log_progress(f"Another mapping regeneration is already running; lock={self.MYSQL_LOCK_NAME}.")
                return

            select_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            check_cursor = self.mysql.cursor(buffered=True)
            insert_cursor = self.mysql.cursor(buffered=True)

            self._log_progress(
                "Starting publication_gard_searchterm_pubmed_mapping regeneration. "
                "Mode=all source rows; duplicate rule=gard_id + search_term + pubmed_id."
            )

            select_cursor.execute(self.FETCH_SOURCE_ROWS_QUERY)
            current_group: Optional[Tuple[str, str]] = None
            current_source_rows = 0
            current_pubmed_ids: Set[int] = set()

            while True:
                rows = select_cursor.fetchmany(self.SOURCE_BATCH_SIZE)

                if not rows:
                    break

                for row in rows:
                    gard_id = str(row.get("gard_id") or "").strip()
                    search_term = str(row.get("search_term") or "").strip()
                    group_key = (gard_id, search_term)

                    if current_group is not None and group_key != current_group:
                        group_summary = self._flush_group(
                            check_cursor,
                            insert_cursor,
                            current_group[0],
                            current_group[1],
                            current_pubmed_ids,
                            current_source_rows,
                        )
                        self._update_summary(summary, group_summary)
                        current_source_rows = 0
                        current_pubmed_ids = set()

                    current_group = group_key
                    current_source_rows += 1
                    summary["source_rows_seen"] += 1
                    parsed_pubmed_ids, invalid_count = self._parse_pubmed_ids(row.get("pubmed_ids"))
                    current_pubmed_ids.update(parsed_pubmed_ids)
                    summary["invalid_pubmed_ids_seen"] += invalid_count

            if current_group is not None:
                group_summary = self._flush_group(
                    check_cursor,
                    insert_cursor,
                    current_group[0],
                    current_group[1],
                    current_pubmed_ids,
                    current_source_rows,
                )
                self._update_summary(summary, group_summary)

            hours, minutes, seconds = _time_hms(time.time() - start_time)
            self._log_progress(
                f"Completed mapping regeneration. Summary={summary}. "
                f"Total time={hours} hours, {minutes} minutes, {seconds} seconds."
            )

        except Exception:
            if self.mysql is not None and self.mysql.is_connected():
                self.mysql.rollback()

            self.logger.exception(f"Publication GARD/search-term/PMID mapping regeneration failed. Summary={summary}")
            print(f"Publication GARD/search-term/PMID mapping regeneration failed. Summary={summary}", flush=True)

        finally:
            if lock_acquired:
                self._release_mysql_lock()

            for cursor in (select_cursor, check_cursor, insert_cursor):
                if cursor is not None:
                    cursor.close()

            self.close()


    def _parse_pubmed_ids(self, pubmed_ids_text) -> Tuple[Set[int], int]:

        pubmed_ids = set()
        invalid_count = 0

        for raw_pubmed_id in self._iter_pubmed_id_values(pubmed_ids_text):
            try:
                pubmed_ids.add(int(raw_pubmed_id))
            except ValueError:
                invalid_count += 1

        return pubmed_ids, invalid_count


    def _iter_pubmed_id_values(self, pubmed_ids_text) -> Iterable[str]:

        for raw_pubmed_id in str(pubmed_ids_text or "").split(","):
            raw_pubmed_id = raw_pubmed_id.strip()

            if raw_pubmed_id:
                yield raw_pubmed_id


    def _flush_group(self, check_cursor, insert_cursor, gard_id: str, search_term: str, source_pubmed_ids: Set[int], source_rows: int) -> Dict[str, int]:

        group_summary = {
            "groups_seen": 1,
            "source_group_pubmed_ids_seen": len(source_pubmed_ids),
            "existing_exact_mappings_seen": 0,
            "exact_duplicates_skipped": 0,
            "rows_inserted": 0,
            "empty_groups_seen": 0,
        }

        if not source_pubmed_ids:
            group_summary["empty_groups_seen"] = 1
            self._log_progress(f"{gard_id} | {search_term} | source_rows={source_rows}, source_pmids=0, inserted=0.")
            return group_summary

        existing_pubmed_ids = self._fetch_existing_pubmed_ids(check_cursor, gard_id, search_term)
        missing_pubmed_ids = sorted(source_pubmed_ids - existing_pubmed_ids)

        group_summary["existing_exact_mappings_seen"] = len(existing_pubmed_ids)
        group_summary["exact_duplicates_skipped"] = len(source_pubmed_ids) - len(missing_pubmed_ids)

        values = []

        for pubmed_id in missing_pubmed_ids:
            values.append((gard_id, search_term, pubmed_id, gard_id, search_term, pubmed_id))

            if len(values) >= self.INSERT_BATCH_SIZE:
                group_summary["rows_inserted"] += self._insert_mapping_batch(insert_cursor, values)
                self.mysql.commit()
                values = []

        if values:
            group_summary["rows_inserted"] += self._insert_mapping_batch(insert_cursor, values)
            self.mysql.commit()

        self._log_progress(
            f"{gard_id} | {search_term} | source_rows={source_rows}, "
            f"source_pmids={len(source_pubmed_ids)}, existing_exact_pmids={len(existing_pubmed_ids)}, "
            f"inserted={group_summary['rows_inserted']}."
        )

        return group_summary


    def _fetch_existing_pubmed_ids(self, check_cursor, gard_id: str, search_term: str) -> Set[int]:

        check_cursor.execute(self.FETCH_EXISTING_PUBMED_IDS_QUERY, (gard_id, search_term))
        return {
            int(row[0])
            for row in check_cursor.fetchall()
            if row[0] is not None
        }


    def _insert_mapping_batch(self, insert_cursor, values: List[Tuple[str, str, int, str, str, int]]) -> int:

        if not values:
            return 0

        insert_cursor.executemany(self.INSERT_MAPPING_QUERY, values)
        return insert_cursor.rowcount if insert_cursor.rowcount >= 0 else len(values)


    def _update_summary(self, summary: Dict[str, int], group_summary: Dict[str, int]) -> None:

        for key, value in group_summary.items():
            summary[key] += value


    def _acquire_mysql_lock(self) -> bool:

        cursor = self.mysql.cursor(buffered=True)

        try:
            cursor.execute("SELECT GET_LOCK(%s, 0)", (self.MYSQL_LOCK_NAME,))
            row = cursor.fetchone()
            return bool(row and row[0] == 1)
        finally:
            cursor.close()


    def _release_mysql_lock(self) -> None:

        cursor = self.mysql.cursor(buffered=True)

        try:
            cursor.execute("SELECT RELEASE_LOCK(%s)", (self.MYSQL_LOCK_NAME,))
        finally:
            cursor.close()


    def _log_progress(self, message: str) -> None:

        self.logger.info(message)
        print(message, flush=True)


if __name__ == "__main__":

    PublicationGardSearchtermPubmedMappingRegenerationTask().process_new_data()

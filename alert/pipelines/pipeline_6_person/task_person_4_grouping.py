import copy
import os
import re
import string
import sys
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from typing import Any, Dict, List

import pandas as pd

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
    os.path.abspath(os.path.join(_dir, "../../..")),
])

from pipelines.pipeline_base import PipelineBase
from F_person.person_disambiguation import PersonDisambiguator
from utils.tools import _curr_timestamp

"""
Generate RDAS person groups.

It groups person_of_all_sources rows by last name, runs PersonDisambiguator, and
updates rdas_group_id. The affected last names come from rows where is_new = 1,
but disambiguation uses all people with those last names. Existing rdas_group_id
values are preserved and assigned to matching new rows.
"""

#F_person/4_generate_person_group.py


class _NoOpLogger:
    """Small logger used inside worker processes to avoid sharing parent loggers."""

    def info(self, message):
        pass

    def error(self, message):
        pass


def _process_last_name_group_worker(last_name: str, person_list: List[Dict[str, Any]],
        group_id_processed_flag: str, large_last_name_group_size: int, empty_first_name: str) -> Dict[str, Any]:
    """
    Worker-process function.

    It does CPU-heavy disambiguation only and returns SQL update tuples to the
    parent process. It does not open or use a MySQL connection.
    """

    tuples = []
    batches_processed = 0
    disambiguation_batches = _create_person_batches_by_first_name_initial(
        person_list,
        large_last_name_group_size,
        empty_first_name
    )

    for group_key, sublist in disambiguation_batches.items():
        if not sublist:
            continue

        batches_processed += 1
        disambiguator = PersonDisambiguator(last_name, sublist, logger=_NoOpLogger())
        df = disambiguator.process()
        df_subset = df[["id", "rdas_group_id", "final"]]

        tuples.extend(_build_group_id_update_tuples_for_worker(
            df_subset,
            last_name,
            group_id_processed_flag
        ))

    return {
        "last_name": last_name,
        "records": len(person_list),
        "batches_processed": batches_processed,
        "tuples": tuples,
    }


def _create_person_batches_by_first_name_initial(person_list: List[Dict[str, Any]], large_last_name_group_size: int, 
                                                 empty_first_name: str) -> Dict[str, List[Dict[str, Any]]]:
    """Split very large last-name groups by first initial before grouping."""

    if len(person_list) < large_last_name_group_size:
        return {"A-Z": person_list}

    grouped_by_letter = {}
    lowerchars = list(string.ascii_lowercase)
    lowerchars.append(empty_first_name)

    for char in lowerchars:
        if char == empty_first_name:
            grouped_by_letter[char] = [
                person for person in person_list if not person.get("first_name")
            ]
        else:
            grouped_by_letter[char] = [
                person
                for person in person_list
                if person.get("first_name") and person["first_name"][0].lower() == char
            ]

    return grouped_by_letter


def _build_group_id_update_tuples_for_worker(df_subset, last_name: str, group_id_processed_flag: str):
    """
    Build SQL update tuples in the worker process.

    This is a module-level function so multiprocessing can pickle and run it safely. 
    It returns tuples that the parent process writes to MySQL.
    """

    df = df_subset[["id", "rdas_group_id", "final"]].copy()
    records = df.to_dict("records")
    resolved_records = copy.deepcopy(records)

    '''
    PersonDisambiguator writes a temporary group value into the `final` column for the current run.
    Some rows in this batch may already have an existing `rdas_group_id` saved in `person_of_all_sources`
    from a previous full load or earlier alert run.
    Those existing database `rdas_group_id` values should be preserved because they already identify
    previously grouped people (in Memgraph).

    If PersonDisambiguator places a new row into the same temporary `final` group as a row that already
    has `rdas_group_id`, the new row should receive that existing database `rdas_group_id`.
    This prevents the same person group from getting a brand-new `rdas_group_id` during an alert update.

    `records` is the original disambiguator output. `resolved_records` is the copy we mutate before building SQL tuples.
    '''
    for record in records:
        new_rdas_group_id = record.get("final")
        existing_rdas_group_id = record.get("rdas_group_id")

        '''
        Case 1:
        The current row already has an existing rdas_group_id AND the
        disambiguator assigned it to a temporary final group.

        That means every row with the same temporary final group should use this
        existing rdas_group_id. This is how a new person row gets linked
        to the same Agent/person group as an older matching row.

        Example:
            Existing row: rdas_group_id = "Zhao_12", final = "group_3"
            New row:      rdas_group_id = NULL,      final = "group_3"

        After this block:
            New row final becomes "Zhao_12", so the SQL update assigns it the
            existing rdas_group_id instead of storing "group_3".
        '''
        if (
            not _is_empty_group_id_for_worker(existing_rdas_group_id)
            and not _is_empty_group_id_for_worker(new_rdas_group_id)
        ):
            for item in resolved_records:
                '''
                Only rows with the same non-empty temporary final group are
                updated. Blank final values are handled in the next case.
                '''
                if (
                    not _is_empty_group_id_for_worker(item.get("final"))
                    and item.get("final") == new_rdas_group_id
                ):
                    item["final"] = existing_rdas_group_id

        '''
        Case 2:
        The current row already has an existing rdas_group_id, but the
        disambiguator did not place it into a temporary final group.

        This can happen when the row is not connected strongly enough to other
        rows in this run. We still preserve its existing rdas_group_id in the
        resolved copy so it is not treated like a brand-new ungrouped person.
        '''
        if (
            not _is_empty_group_id_for_worker(existing_rdas_group_id)
            and _is_empty_group_id_for_worker(new_rdas_group_id)
        ):
            for item in resolved_records:
                '''
                Find the same existing grouped row in the mutable copy and copy
                its existing rdas_group_id into final. Later, rows that already
                have rdas_group_id are skipped for SQL updates, but keeping final
                resolved makes the data state explicit and prevents fallback IDs
                from being considered for existing grouped rows.
                '''
                if (
                    item.get("rdas_group_id") == existing_rdas_group_id
                    and _is_empty_group_id_for_worker(item.get("final"))
                ):
                    item["final"] = existing_rdas_group_id

    normalized_last_name = re.sub(r"\W+", "", last_name or "")
    fallback_timestamp = _curr_timestamp("%Y%m%d%H%M%S")
    tuples = []

    for index, item in enumerate(resolved_records):
        if not _is_empty_group_id_for_worker(item.get("rdas_group_id")):
            continue

        final_group_id = item.get("final")

        if _is_empty_group_id_for_worker(final_group_id):
            final_group_id = f"{normalized_last_name}_{fallback_timestamp}_{index}"

        tuples.append((final_group_id, group_id_processed_flag, item.get("id")))

    return tuples


def _is_empty_group_id_for_worker(value: Any) -> bool:
    """Return True for NULL, blank, or NaN group IDs."""

    if value is None:
        return True

    if isinstance(value, str):
        return not value.strip()

    return bool(pd.isna(value))


class NewPersonGroupingTask(PipelineBase):
    """Assign group IDs to new people without changing existing group IDs."""

    PERSON_TABLE = "person_of_all_sources"
    GRANT_PROJECT_TABLE = "grant_project"
    PUBLICATION_ARTICLE_TABLE = "publication_article"
    LARGE_LAST_NAME_GROUP_SIZE = 5000
    EMPTY_FIRST_NAME = "NONE"
    DEFAULT_MAX_WORKERS = 15
    MAX_PENDING_FUTURES_PER_WORKER = 2

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=False)
        self.group_id_processed_flag = _curr_timestamp("%Y%m%d%H%M%S")
        self.max_workers = self._resolve_max_workers()
        self.max_pending_futures = self.max_workers * self.MAX_PENDING_FUTURES_PER_WORKER


    def find_new_data(self, gard_node) -> None:
        self.logger.info("PersonGroupingTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Find affected last names, disambiguate people, and update group IDs."""

        total_last_names = 0
        total_people_updated = 0
        processed_last_names = set()
        pending_futures = {}

        try:
            self.logger.info(
                f"Starting PersonGroupingTask with max_workers={self.max_workers}; "
                f"max_pending_futures={self.max_pending_futures}."
            )

            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:

                for prefix in self._iter_last_name_prefixes():
                    self.logger.info(f"Processing last-name prefix: {prefix}")

                    last_names = self.get_newly_added_last_names_by_prefix(prefix)

                    if not last_names:
                        self.logger.info(f'No new last names starts with {prefix} found.\n')
                        continue

                    for last_name in last_names:
                        if not last_name:
                            continue

                        normalized_last_name = str(last_name).strip().lower()

                        if normalized_last_name in processed_last_names:
                            self.logger.info(f"Skipping already processed last_name={last_name}")
                            continue

                        processed_last_names.add(normalized_last_name)

                        self.logger.info(f"Processing last_name = {last_name}")

                        person_list = self.fetch_person_by_last_name_for_group_id_update(last_name)

                        if not person_list:
                            continue

                        future = executor.submit(
                            _process_last_name_group_worker,
                            last_name,
                            person_list,
                            self.group_id_processed_flag,
                            self.LARGE_LAST_NAME_GROUP_SIZE,
                            self.EMPTY_FIRST_NAME
                        )

                        pending_futures[future] = {"last_name": last_name, "records": len(person_list),}

                        '''
                        Do not submit every last_name to the process pool at once. Each pending future holds its person_list in
                        memory until the worker finishes, and large last-name groups can be expensive.

                        When the queue reaches max_pending_futures, wait until at least one worker finishes, then apply that worker's
                        returned SQL tuples in the parent process. 
                        This keeps memory bounded while still allowing several disambiguation jobs to run in parallel.
                        '''
                        if len(pending_futures) >= self.max_pending_futures:
                            completed_count, updated_count = self._drain_grouping_futures(pending_futures, wait_for_all=False )

                            total_last_names += completed_count
                            total_people_updated += updated_count

                completed_count, updated_count = self._drain_grouping_futures(pending_futures,  wait_for_all=True)

                total_last_names += completed_count
                total_people_updated += updated_count

            self.logger.info(
                f"Completed PersonGroupingTask. "
                f"Last names processed={total_last_names}; people updated={total_people_updated}."
            )

        except Exception as e:
            self.logger.error(f"PersonGroupingTask failed: {e}")

            if self.mysql:
                self.mysql.rollback()

        finally:
            ''' Explicitly close all db connections. '''
            self.close()


    def _resolve_max_workers(self) -> int:
        """Resolve multiprocessing worker count from env, capped by CPU count."""

        cpu_count = os.cpu_count() or 1
        configured_workers = os.getenv("PERSON_GROUPING_MAX_WORKERS")

        if configured_workers:
            try:
                return max(1, min(int(configured_workers), cpu_count))
            except ValueError:
                self.logger.info(
                    f"Invalid PERSON_GROUPING_MAX_WORKERS={configured_workers}; "
                    f"using default {self.DEFAULT_MAX_WORKERS}."
                )

        return max(1, min(self.DEFAULT_MAX_WORKERS, cpu_count))


    def _drain_grouping_futures(self, pending_futures, wait_for_all: bool):
        """Apply completed worker results using the parent MySQL connection."""

        if not pending_futures:
            return 0, 0

        completed_total = 0
        updated_total = 0

        if wait_for_all:
            done, _ = wait(pending_futures.keys())
        else:
            done, _ = wait(pending_futures.keys(), return_when=FIRST_COMPLETED)

        for future in done:
            metadata = pending_futures.pop(future)
            completed_count, updated_count = self._handle_grouping_future(future, metadata)
            
            completed_total += completed_count
            updated_total += updated_count

        return completed_total, updated_total


    def _handle_grouping_future(self, future, metadata):
        """Read one worker result and update MySQL in the parent process."""

        last_name = metadata.get("last_name")

        try:
            result = future.result()
        except Exception as e:
            self.logger.error(f"Person grouping worker failed for last_name={last_name}: {e}")
            return 0, 0

        tuples = result.get("tuples", [])
        updated_count = self.update_rdas_group_id_with_tuples(tuples, last_name)

        self.logger.info(
            f"Processed last_name={last_name}; records={result.get('records', 0)}; "
            f"batches={result.get('batches_processed', 0)}; update tuples={len(tuples)}; "
            f"people updated={updated_count}.\n"
        )

        return 1, updated_count


    def _iter_last_name_prefixes(self):

        '''
        Match the prefix traversal from F_person/4_generate_person_group.py.
        The two-character prefix keeps each database lookup small and avoids a
        leading wildcard, so MySQL can use the last_name index.
        '''
        lowercase = list(string.ascii_lowercase)
        uppercase = list(string.ascii_uppercase)

        uppercase.append("-")
        uppercase.append("'")
        uppercase.append("/")
        uppercase.extend(lowercase)

        # The second character list includes common punctuation seen in names,
        # keeping prefix scans indexed while covering names like O'Neil.
        lowercase.append(" ")
        lowercase.append("'")
        lowercase.append(".")
        lowercase.append("-")
        lowercase.append("_")

        for first_char in uppercase:
            for second_char in lowercase:
                yield first_char + second_char


    def get_newly_added_last_names_by_prefix(self, prefix: str) -> List[str]:
        """Return new-row last names under one prefix for regrouping."""

        # is_new only chooses which last names were affected by this alert run.
        # The full person set for each returned last name is loaded later.
        query = f'''
            SELECT DISTINCT last_name
            FROM {self.PERSON_TABLE}
            WHERE last_name LIKE %s
            AND is_new = 1
            AND (rdas_group_id IS NULL OR rdas_group_id = '')
        '''

        cursor = None

        try:
            cursor = self.mysql.cursor(buffered=True, dictionary=True)
            cursor.execute(query, (prefix + "%",))

            return [row["last_name"] for row in cursor.fetchall()]

        except Exception as e:
            self.logger.error(f"Error fetching last names for prefix={prefix}: {e}")
            return []

        finally:
            if cursor:
                cursor.close()


    def fetch_person_by_last_name_for_group_id_update(self, last_name: str) -> List[Dict[str, Any]]:

        '''
        Fetch the same publication, clinical trial, and grant fields expected by
        PersonDisambiguator.

        Do not filter this query by is_new. New rows only choose the affected
        last names; all matching people must be included so disambiguation can
        compare new people with existing people.
        '''
        params = (
            last_name,
            last_name,
            last_name,
        )

        query = f'''
            WITH publication_data AS (
                SELECT
                    ps.id, ps.associate_id, ps.associate_type, ps.source,
                    ps.first_name, ps.last_name, ps.affiliation,
                    ps.orcid, ps.email, ps.rdas_group_id,
                    pa.first_publication_date, pa.title, pa.abstract_text,
                    (
                        SELECT GROUP_CONCAT(sub.first_name, ' ', sub.last_name SEPARATOR ', ')
                        FROM {self.PERSON_TABLE} AS sub
                        WHERE
                            sub.source = 'Publication'
                            AND sub.associate_type = 'Author'
                            AND sub.associate_id = ps.associate_id
                    ) AS author_list
                FROM {self.PERSON_TABLE} AS ps
                JOIN {self.PUBLICATION_ARTICLE_TABLE} AS pa ON ps.associate_id = pa.pubmed_id
                WHERE
                    ps.last_name = %s
                    AND ps.source = 'Publication'
            ),
            clinical_trial_data AS (
                SELECT
                    ps.id, ps.associate_id, ps.associate_type, ps.source,
                    ps.first_name, ps.last_name, ps.affiliation,
                    ps.orcid, ps.email, ps.rdas_group_id,
                    '' AS first_publication_date,
                    '' AS title,
                    '' AS abstract_text,
                    (
                        SELECT GROUP_CONCAT(sub.first_name, ' ', sub.last_name SEPARATOR ', ')
                        FROM {self.PERSON_TABLE} AS sub
                        WHERE
                            sub.source = 'ClinicalTrial'
                            AND sub.associate_id = ps.associate_id
                    ) AS author_list
                FROM {self.PERSON_TABLE} AS ps
                WHERE
                    ps.last_name = %s
                    AND ps.source = 'ClinicalTrial'
            ),
            grant_project_data AS (
                SELECT
                    ps.id, ps.associate_id, ps.associate_type, ps.source,
                    ps.first_name, ps.last_name, ps.affiliation,
                    ps.orcid, ps.email, ps.rdas_group_id,
                    '' AS first_publication_date,
                    gp.project_title AS title,
                    '' AS abstract_text,
                    (
                        SELECT GROUP_CONCAT(sub.first_name, ' ', sub.last_name SEPARATOR ', ')
                        FROM {self.PERSON_TABLE} AS sub
                        WHERE
                            sub.source = 'GrantProject'
                            AND sub.associate_id = ps.associate_id
                    ) AS author_list
                FROM {self.PERSON_TABLE} AS ps
                JOIN {self.GRANT_PROJECT_TABLE} AS gp ON ps.associate_id = gp.application_id
                WHERE
                    ps.last_name = %s
                    AND ps.source = 'GrantProject'
            )
            SELECT
                *,
                TRIM(SUBSTRING_INDEX(author_list, ',', 1)) AS first_author,
                TRIM(SUBSTRING_INDEX(author_list, ',', -1)) AS last_author
            FROM publication_data
            UNION ALL
            SELECT
                *,
                TRIM(SUBSTRING_INDEX(author_list, ',', 1)) AS first_author,
                TRIM(SUBSTRING_INDEX(author_list, ',', -1)) AS last_author
            FROM clinical_trial_data
            UNION ALL
            SELECT
                *,
                TRIM(SUBSTRING_INDEX(author_list, ',', 1)) AS first_author,
                TRIM(SUBSTRING_INDEX(author_list, ',', -1)) AS last_author
            FROM grant_project_data
            ORDER BY source, last_name, first_name
        '''

        cursor = None

        try:
            cursor = self.mysql.cursor(buffered=True, dictionary=True)
            cursor.execute(query, params)
            person_list = []

            while True:
                batch = cursor.fetchmany(500)

                if not batch:
                    break

                person_list.extend(batch)

            return person_list

        except Exception as e:
            self.logger.error(f"Error fetching people for last_name={last_name}: {e}")
            return []

        finally:
            if cursor:
                cursor.close()


    def update_rdas_group_id_with_tuples(self, tuples, last_name: str) -> int:
        """Persist worker-built group ID tuples using the parent MySQL connection."""

        if not tuples:
            return 0

        update_sql = f'''
            UPDATE {self.PERSON_TABLE}
            SET rdas_group_id = %s,
                group_id_processed = %s
            WHERE id = %s
            AND (rdas_group_id IS NULL OR rdas_group_id = '')
        '''

        cursor = None

        try:
            cursor = self.mysql.cursor(buffered=True)
            cursor.executemany(update_sql, tuples)
            self.mysql.commit()

            return cursor.rowcount

        except Exception as e:
            self.logger.error(f"Error updating RDAS group ids for last_name={last_name}: {e}")

            if self.mysql:
                self.mysql.rollback()

            return 0

        finally:
            if cursor:
                cursor.close()

import copy
import os
import re
import string
import sys
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


class NewPersonGroupingTask(PipelineBase):
    """Assign group IDs to new people without changing existing group IDs."""

    PERSON_TABLE = "person_of_all_sources"
    GRANT_PROJECT_TABLE = "grant_project"
    PUBLICATION_ARTICLE_TABLE = "publication_article"
    LARGE_LAST_NAME_GROUP_SIZE = 5000
    EMPTY_FIRST_NAME = "NONE"

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=False)
        self.group_id_processed_flag = _curr_timestamp("%Y%m%d%H%M%S")


    def find_new_data(self, gard_node) -> None:
        self.logger.info("PersonGroupingTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Find affected last names, disambiguate people, and update group IDs."""

        total_last_names = 0
        total_people_updated = 0

        try:
            for prefix in self._iter_last_name_prefixes():
                self.logger.info(f"Processing last-name prefix: {prefix}")

                last_names = self.get_newly_added_last_names_by_prefix(prefix)

                if not last_names:
                    continue

                for last_name in last_names:
                    if not last_name:
                        continue

                    self.logger.info(f"Processing last_name={last_name}")

                    person_list = self.fetch_person_by_last_name_for_group_id_update(last_name)

                    if not person_list:
                        continue

                    updated_count = self.process_last_name_group(last_name, person_list)

                    total_last_names += 1
                    total_people_updated += updated_count

                    self.logger.info(
                        f"Processed last_name={last_name}; "
                        f"people updated={updated_count}; total people updated={total_people_updated}."
                    )

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


    def process_last_name_group(self, last_name: str, person_list: List[Dict[str, Any]]) -> int:
        """Run disambiguation for one last name and persist updated group IDs."""

        disambiguation_batches = self.create_person_batches_by_first_name_initial(person_list)

        total_updated = 0

        for group_key, sublist in disambiguation_batches.items():

            if not sublist:
                continue

            self.logger.info(f"Disambiguating last_name={last_name}; group={group_key}; records={len(sublist)}.")

            df_subset = self.disambiguate(last_name, sublist)

            updated_count = self.update_rdas_group_id(df_subset, last_name)
            total_updated += updated_count

        return total_updated


    def  create_person_batches_by_first_name_initial(self, person_list: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Split very large last-name groups by first initial before grouping."""

        if len(person_list) < self.LARGE_LAST_NAME_GROUP_SIZE:
            return {"A-Z": person_list}

        # Large last-name groups are expensive to compare all at once, so split
        # by first initial while keeping missing first names together.
        grouped_by_letter = {}
        lowerchars = list(string.ascii_lowercase)
        lowerchars.append(self.EMPTY_FIRST_NAME)

        for char in lowerchars:
            if char == self.EMPTY_FIRST_NAME:
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


    def disambiguate(self, last_name: str, person_list: List[Dict[str, Any]]):
        """Delegate person matching to the shared PersonDisambiguator."""

        disambiguator = PersonDisambiguator(last_name, person_list)
        df = disambiguator.process()

        return df[["id", "rdas_group_id", "final"]]


    def update_rdas_group_id(self, df_subset, last_name: str) -> int:
        """Persist group IDs only for new/unassigned person rows."""

        if df_subset is None or df_subset.empty:
            return 0

        tuples = self._build_group_id_update_tuples(df_subset, last_name)

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


    def _build_group_id_update_tuples(self, df_subset, last_name: str):

        '''
        Match PersonGroupIdUpdater's incremental behavior:
        keep existing rdas_group_id values stable, replace matching generated
        disambiguation groups with the existing ID, and update only rows that do
        not already have an rdas_group_id.
        '''
        '''
        Step 1:
        Keep only the columns needed to decide the final update.
        id identifies the MySQL row, rdas_group_id is the existing stable ID,
        and final is the temporary group from PersonDisambiguator.
        '''
        df = df_subset[["id", "rdas_group_id", "final"]].copy()

        '''
        Step 2:
        Work with dictionaries so the logic mirrors PersonGroupIdUpdater and
        each row is easy to compare.
        '''
        records = df.to_dict("records")

        '''
        Step 3:
        Keep the original disambiguator output unchanged while building a
        resolved copy. The copy is where generated final groups can be replaced
        by existing rdas_group_id values.
        '''
        resolved_records = copy.deepcopy(records)

        '''
        Step 4:
        For each row that already has an rdas_group_id, use that existing ID as
        the canonical group ID for every row assigned to the same temporary
        disambiguator group.
        '''
        for record in records:
            new_rdas_group_id = record.get("final")
            existing_rdas_group_id = record.get("rdas_group_id")

            '''
            Case A:
            Existing row has rdas_group_id, and PersonDisambiguator also
            assigned it a temporary final group. Any new row with the same final
            group should receive the existing rdas_group_id.
            '''
            if (
                not self._is_empty_group_id(existing_rdas_group_id)
                and not self._is_empty_group_id(new_rdas_group_id)
            ):
                for item in resolved_records:
                    if (
                        not self._is_empty_group_id(item.get("final"))
                        and item.get("final") == new_rdas_group_id
                    ):
                        item["final"] = existing_rdas_group_id

            '''
            Case B:
            Existing row has rdas_group_id, but its temporary final group is
            empty. Keep the existing ID on that row in the resolved copy so it
            is not treated as an ungrouped person later.
            '''
            if (
                not self._is_empty_group_id(existing_rdas_group_id)
                and self._is_empty_group_id(new_rdas_group_id)
            ):
                for item in resolved_records:
                    if (
                        item.get("rdas_group_id") == existing_rdas_group_id
                        and self._is_empty_group_id(item.get("final"))
                    ):
                        item["final"] = existing_rdas_group_id

        '''
        Step 5:
        Prepare a fallback ID for truly new/unmatched people. This is only used
        when a row has no existing rdas_group_id and the disambiguator did not
        assign it to any group.
        '''
        normalized_last_name = re.sub(r"\W+", "", last_name or "")
        fallback_timestamp = _curr_timestamp("%Y%m%d%H%M%S")
        tuples = []

        '''
        Step 6:
        Build SQL parameter tuples only for rows that do not already have
        rdas_group_id. Existing grouped rows are skipped so their stable IDs
        remain unchanged in MySQL and Memgraph.
        '''
        for index, item in enumerate(resolved_records):
            if not self._is_empty_group_id(item.get("rdas_group_id")):
                continue

            final_group_id = item.get("final")

            '''
            Step 7:
            If the new row matched an existing group, final_group_id is now that
            existing rdas_group_id. If it did not match anything, create a
            unique fallback group under this last name.
            '''
            if self._is_empty_group_id(final_group_id):
                final_group_id = f"{normalized_last_name}_{fallback_timestamp}_{index}"

            '''
            Step 8:
            Tuple order matches update_rdas_group_id():
            SET rdas_group_id = %s, group_id_processed = %s WHERE id = %s.
            '''
            tuples.append((final_group_id, self.group_id_processed_flag, item.get("id")))

        '''
        Step 9:
        The caller executes these updates with an extra SQL guard:
        AND (rdas_group_id IS NULL OR rdas_group_id = ''). That protects
        existing grouped rows even if this method is changed later.
        '''
        return tuples


    @staticmethod
    def _is_empty_group_id(value: Any) -> bool:
        """Return True for NULL, blank, or NaN group IDs."""

        if value is None:
            return True

        if isinstance(value, str):
            return not value.strip()

        return bool(pd.isna(value))

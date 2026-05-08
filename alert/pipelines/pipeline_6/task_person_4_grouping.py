import os
import re
import string
import sys
from typing import Any, Dict, List

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
    os.path.abspath(os.path.join(_dir, "../../..")),
])

from pipelines.pipeline_base import PipelineBase
from F_person.person_disambiguation import PersonDisambiguator

"""
Generate RDAS person groups.

It groups person_of_all_sources rows by last name, runs PersonDisambiguator, and
updates rdas_group_id. The affected last names come from rows where is_new = 1,
but disambiguation uses all people with those last names.
"""

#F_person/4_generate_person_group.py


class NewPersonGroupingTask(PipelineBase):

    PERSON_TABLE = "person_of_all_sources"
    GRANT_PROJECT_TABLE = "grant_project"
    PUBLICATION_ARTICLE_TABLE = "publication_article"
    LARGE_LAST_NAME_GROUP_SIZE = 5000
    EMPTY_FIRST_NAME = "NONE"

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=False)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("PersonGroupingTask does not use find_new_data().")


    def process_new_data(self) -> None:

        total_last_names = 0
        total_people_updated = 0

        try:
            for prefix in self.iter_last_name_prefixes():
                self.logger.info(f"Processing last-name prefix: {prefix}")

                last_names = self.get_last_names_by_prefix_for_group_id_update(prefix)

                if not last_names:
                    continue

                for last_name in last_names:
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


    def iter_last_name_prefixes(self):

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

        lowercase.append(" ")
        lowercase.append("'")
        lowercase.append(".")
        lowercase.append("-")
        lowercase.append("_")

        for first_char in uppercase:
            for second_char in lowercase:
                yield first_char + second_char


    def get_last_names_by_prefix_for_group_id_update(self, prefix: str) -> List[str]:

        query = f'''
            SELECT DISTINCT last_name
            FROM {self.PERSON_TABLE}
            WHERE last_name LIKE %s
            AND is_new = 1
            AND last_name IS NOT NULL
            ORDER BY last_name ASC
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

        grouped_by_letter = self.group_people_for_disambiguation(person_list)
        total_updated = 0

        for group_key, sublist in grouped_by_letter.items():
            if not sublist:
                continue

            self.logger.info(
                f"Disambiguating last_name={last_name}; group={group_key}; records={len(sublist)}."
            )

            df_subset = self.disambiguate(last_name, sublist)
            updated_count = self.update_rdas_group_id(df_subset, last_name)
            total_updated += updated_count

        return total_updated


    def group_people_for_disambiguation(self, person_list: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:

        if len(person_list) < self.LARGE_LAST_NAME_GROUP_SIZE:
            return {"A-Z": person_list}

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

        disambiguator = PersonDisambiguator(last_name, person_list)
        df = disambiguator.process()

        return df[["id", "final"]]


    def update_rdas_group_id(self, df_subset, last_name: str) -> int:

        if df_subset is None or df_subset.empty:
            return 0

        df = df_subset[["id", "final"]].copy()

        normalized_last_name = re.sub(r"\W+", "", last_name or "")
        df.loc[df["final"].isna(), "final"] = (
            normalized_last_name + "_x_" + df.index[df["final"].isna()].astype(str)
        )

        df = df[["final", "id"]]
        tuples = list(df.itertuples(index=False, name=None))

        if not tuples:
            return 0

        update_sql = f'''
            UPDATE {self.PERSON_TABLE}
            SET rdas_group_id = %s
            WHERE id = %s
        '''

        cursor = None

        try:
            cursor = self.mysql.cursor(buffered=True)
            cursor.executemany(update_sql, tuples)
            self.mysql.commit()

            return len(tuples)

        except Exception as e:
            self.logger.error(f"Error updating RDAS group ids for last_name={last_name}: {e}")

            if self.mysql:
                self.mysql.rollback()

            return 0

        finally:
            if cursor:
                cursor.close()

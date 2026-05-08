import json
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _normalize_tuple, _normalize_txt

"""
Create person rows for newly staged publication authors.

This task is the alert-pipeline version of:
F_person/1_generate_person_of_publication.py

It reads update_publication_article rows where is_new = 1, extracts authors from
source_json.authorList.author, and inserts those authors into person_of_all_sources
with is_new = 1.
"""


class NewPublicationPersonTask(PipelineBase):

    BATCH_SIZE = 100
    PERSON_TABLE = "person_of_all_sources"
    PUBLICATION_TABLE = "update_publication_article"
    ASSOCIATE_TYPE = "author"
    SOURCE = "Publication"
    ROLE = "author"

    FETCH_NEW_PUBLICATIONS_QUERY = f'''
        SELECT DISTINCT
            upa.pubmed_id,
            upa.source_json
        FROM {PUBLICATION_TABLE} AS upa
        LEFT JOIN {PERSON_TABLE} AS p
            ON p.associate_id = upa.pubmed_id
            AND p.source = 'Publication'
        WHERE upa.is_new = 1
        AND upa.pubmed_id IS NOT NULL
        AND upa.source_json IS NOT NULL
        AND p.associate_id IS NULL
    '''

    INSERT_PERSON_SQL = f'''
        INSERT INTO {PERSON_TABLE}
        (
            associate_id,
            associate_type,
            source,
            title,
            first_name,
            last_name,
            collective_name,
            role,
            affiliation,
            orcid,
            is_new
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=False)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("NewPublicationPersonTask does not use find_new_data().")


    def process_new_data(self) -> None:

        fetch_cursor = None
        insert_cursor = None
        total_inserted = 0
        batch_num = 0

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            insert_cursor = self.mysql.cursor(buffered=True)

            fetch_cursor.execute(self.FETCH_NEW_PUBLICATIONS_QUERY)

            while True:
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    self.logger.info("No more new publication rows for person extraction.")
                    break

                batch_num += 1
                person_rows = []

                for row in rows:
                    pubmed_id = row.get("pubmed_id")
                    source_json = row.get("source_json")

                    article_person_rows = self.create_publication_person_rows(pubmed_id, source_json)

                    if article_person_rows:
                        person_rows.extend(article_person_rows)

                if not person_rows:
                    self.logger.info(f"Batch #{batch_num}: no valid publication author rows found.")
                    continue

                try:
                    normalized_person_rows = [_normalize_tuple(person_row) for person_row in person_rows]

                    insert_cursor.executemany(self.INSERT_PERSON_SQL, normalized_person_rows)
                    self.mysql.commit()

                    total_inserted += len(normalized_person_rows)
                    self.logger.info(
                        f"Batch #{batch_num}: inserted {len(normalized_person_rows)} "
                        f"publication person rows. Total inserted={total_inserted}."
                    )

                except Exception as e:
                    self.logger.error(f"Error inserting publication person rows in batch #{batch_num}: {e}")

                    if self.mysql:
                        self.mysql.rollback()

            self.logger.info(f"Completed publication person extraction. Total inserted={total_inserted}.")

        except Exception as e:
            self.logger.error(f"NewPublicationPersonTask failed: {e}")

            if self.mysql:
                self.mysql.rollback()

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            if insert_cursor:
                insert_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def create_publication_person_rows(self, pubmed_id: Any, source_json: Any) -> List[Tuple[Any, ...]]:

        '''
        Extract author records from one Europe PMC source_json payload.
        Each output tuple matches INSERT_PERSON_SQL.
        '''
        if pubmed_id is None or source_json is None:
            return []

        try:
            article_obj = json.loads(source_json) if isinstance(source_json, str) else source_json
        except (json.JSONDecodeError, TypeError) as e:
            self.logger.error(f"Error parsing source_json for pubmed_id={pubmed_id}: {e}")
            return []

        if not isinstance(article_obj, dict):
            return []

        author_list = (article_obj.get("authorList") or {}).get("author", [])

        if isinstance(author_list, dict):
            author_list = [author_list]

        if not isinstance(author_list, list) or not author_list:
            return []

        person_rows = []

        for author in author_list:
            person_row = self.create_author_person_row(pubmed_id, author)

            if person_row:
                person_rows.append(person_row)

        return person_rows


    def create_author_person_row(self, pubmed_id: Any, author: Any) -> Optional[Tuple[Any, ...]]:

        if not isinstance(author, dict):
            return None

        first_name = self._truncate(author.get("firstName"), 250)
        last_name = self._truncate(author.get("lastName"), 250)
        orcid = self.extract_orcid(author)
        affiliation = self.extract_affiliation(author)

        if first_name is not None or last_name is not None:
            return (
                pubmed_id,
                self.ASSOCIATE_TYPE,
                self.SOURCE,
                None,
                first_name,
                last_name,
                None,
                self.ROLE,
                affiliation,
                orcid,
            )

        collective_name = _normalize_txt(author.get("collectiveName"))

        if collective_name:
            return (
                pubmed_id,
                self.ASSOCIATE_TYPE,
                self.SOURCE,
                None,
                None,
                None,
                self._truncate(collective_name, 3500),
                self.ROLE,
                affiliation,
                orcid,
            )

        return None


    def extract_orcid(self, author: Dict[str, Any]) -> Optional[str]:

        author_id = author.get("authorId") or {}

        if not isinstance(author_id, dict):
            return None

        if author_id.get("type") != "ORCID":
            return None

        return self._truncate(author_id.get("value"), 245)


    def extract_affiliation(self, author: Dict[str, Any]) -> Optional[str]:

        affiliation_list = (author.get("authorAffiliationDetailsList") or {}).get("authorAffiliation", [])

        if isinstance(affiliation_list, dict):
            affiliation_list = [affiliation_list]

        if not isinstance(affiliation_list, list) or not affiliation_list:
            return None

        first_affiliation = affiliation_list[0]

        if not isinstance(first_affiliation, dict):
            return None

        return self._truncate(first_affiliation.get("affiliation"), 4000)


    def _truncate(self, value: Any, max_length: int) -> Optional[str]:

        if value is None:
            return None

        value = _normalize_txt(value)

        if value is None:
            return None

        value = str(value)

        if not value:
            return None

        return value[:max_length]

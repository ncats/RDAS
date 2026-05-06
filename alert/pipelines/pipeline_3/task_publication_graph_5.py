import json
import os
import sys
from typing import Any, Dict, Optional

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _na_if_empty

"""
Create Journal nodes for new publication articles.

For each new row in update_publication_article (is_new = 1), parse
source_json.journalInfo.journal, create/merge the Journal node, and link the
matching Article node to the Journal with published_in.
"""
# Reference: C_publication/initializer/journal.py


class NewPublicationJournalGraphTask(PipelineBase):

    BATCH_SIZE = 100

    BATCH_CREATE = '''
        WITH $chunks AS chunks
        WHERE chunks IS NOT NULL AND size(chunks) > 0

        UNWIND chunks AS item
        MATCH (a:Article {pubmedId: item.pubmedId})

        CALL {
            WITH item, a
            WHERE item.journal.issn = 'N/A' AND item.journal.essn = 'N/A'
            MERGE (j:Journal {title: item.journal.title})
            ON CREATE SET
                j.issn = item.journal.issn,
                j.essn = item.journal.essn,
                j.nlmid = item.journal.nlmid
            MERGE (a)-[:published_in]->(j)
            RETURN j
            UNION
            WITH item, a
            WHERE item.journal.issn <> 'N/A' OR item.journal.essn <> 'N/A'
            MERGE (j:Journal {issn: item.journal.issn, essn: item.journal.essn})
            ON CREATE SET
                j.title = item.journal.title,
                j.nlmid = item.journal.nlmid
            MERGE (a)-[:published_in]->(j)
            RETURN j
        }
    '''

    FETCH_NEW_ARTICLES_QUERY = '''
        SELECT
            pubmed_id,
            source_json
        FROM update_publication_article
        WHERE is_new = 1
        AND pubmed_id IS NOT NULL
        AND source_json IS NOT NULL
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        self.logger.info("NewPublicationJournalGraphTask does not use find_new_data().")


    # implement
    def process_new_data(self) -> None:

        fetch_cursor = None
        count = 0
        batch_num = 0

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(self.FETCH_NEW_ARTICLES_QUERY)

            while True:
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    self.logger.info("No more rows to fetch.")
                    break

                batch_num += 1
                self.logger.info(f"--- batch# = {batch_num} ---")

                chunks = []

                for row in rows:
                    journal_chunk = self._create_journal_chunk(row)

                    if journal_chunk is None:
                        continue

                    chunks.append(journal_chunk)

                if not chunks:
                    self.logger.info("No valid Journal mappings to insert into Memgraph.")
                    continue

                try:
                    #self.memgraph.execute(self.BATCH_CREATE, {"chunks": chunks})

                    count += len(chunks)
                    self.logger.info(f"Submitted {len(chunks)} Article-Journal mappings to Memgraph. Total = {count}")

                except Exception as e:
                    self.logger.error(f"Error executing Journal batch create: {e}")

        except Exception as e:
            self.logger.error(f"Error creating Journal nodes in Memgraph: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def _create_journal_chunk(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:

        try:
            pubmed_id = int(row["pubmed_id"])
        except (TypeError, ValueError) as e:
            self.logger.error(f"Invalid pubmed_id found: {row.get('pubmed_id')}. Error: {e}")
            return None

        try:
            source_obj = json.loads(row.get("source_json") or "{}")
        except (json.JSONDecodeError, TypeError) as e:
            self.logger.error(f"Error parsing source_json for pubmed_id={pubmed_id}: {e}")
            return None

        journal = (source_obj.get("journalInfo") or {}).get("journal")

        if not journal:
            return None

        return {
            "pubmedId": pubmed_id,
            "journal": {
                "title": _na_if_empty(journal.get("title")),
                "issn": _na_if_empty(journal.get("issn")),
                "essn": _na_if_empty(journal.get("essn")),
                "nlmid": _na_if_empty(journal.get("nlmid")),
            },
        }

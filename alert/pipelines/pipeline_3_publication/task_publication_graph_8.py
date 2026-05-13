import os
import sys
from typing import Any, Dict, Optional

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase

"""
Create GARD-to-Article relationships for new publication articles.

For each publication row in publication_article where is_new = 1, find
the matching GARD/pubmed mapping in publication_gard_searchterm_pubmed_mapping
and create the Memgraph relationship:

    (GARD)-[:has_mention_in]->(Article)

The mapping table does not have an is_new flag, so publication_article
is the source of truth for deciding which article relationships are new for
this alert pipeline run.
"""

# Reference: C_publication/initializer/relationship_GARD.py


class NewPublicationGardArticleRelationshipTask(PipelineBase):
    """Link new Article nodes to their matching GARD disease nodes."""

    BATCH_SIZE = 300

    # MERGE keeps the relationship write idempotent when the same mapping is
    # seen again in a later run.
    BATCH_CREATE = '''
        UNWIND $chunks AS chunk
        MATCH (p:Article {pubmedId: chunk.pubmedId})
        MATCH (g:GARD {gardId: chunk.gardId})
        MERGE (g)-[:has_mention_in]->(p)
    '''

    # Newness comes from publication_article. The mapping table provides
    # the GARD/pubmed pairs and may mark false-positive mappings with is_valid.
    FETCH_NEW_RELATIONS_QUERY = '''
        SELECT DISTINCT
            pgspm.gard_id,
            pgspm.pubmed_id
        FROM publication_gard_searchterm_pubmed_mapping AS pgspm
        INNER JOIN publication_article AS pa
            ON pa.pubmed_id = pgspm.pubmed_id
        WHERE pa.is_new = 1
        AND pgspm.gard_id IS NOT NULL
        AND pgspm.pubmed_id IS NOT NULL
        AND (pgspm.is_valid IS NULL OR pgspm.is_valid = 1)
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        self.logger.info("NewPublicationGardArticleRelationshipTask does not use find_new_data().")


    # implement
    def process_new_data(self) -> None:
        """Read new GARD/pubmed mappings and write Article relationships."""

        fetch_cursor = None
        count = 0
        batch_num = 0

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(self.FETCH_NEW_RELATIONS_QUERY)

            while True:
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    self.logger.info("No more rows to fetch.")
                    break

                batch_num += 1
                self.logger.info(f"--- batch# = {batch_num} ---")

                chunks = []

                for row in rows:
                    # Normalize and validate IDs before sending them to
                    # Memgraph; bad rows are logged and skipped.
                    relation_chunk = self._create_relation_chunk(row)

                    if relation_chunk is None:
                        continue

                    chunks.append(relation_chunk)

                if not chunks:
                    self.logger.info("No valid GARD-to-Article relationships to insert into Memgraph.")
                    continue

                try:
                    self.memgraph.execute(self.BATCH_CREATE, {"chunks": chunks})

                    count += len(chunks)
                    self.logger.info(
                        f"Submitted {len(chunks)} GARD-to-Article relationships to Memgraph. "
                        f"Total = {count}"
                    )

                except Exception as e:
                    self.logger.error(f"Error executing GARD-to-Article relationship batch create: {e}")

        except Exception as e:
            self.logger.error(f"Error creating GARD-to-Article relationships in Memgraph: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def _create_relation_chunk(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Build one GARD-to-Article relationship chunk from a MySQL row."""

        gard_id = row.get("gard_id")

        if gard_id is None or not str(gard_id).strip():
            self.logger.error(f"Invalid gard_id found: {gard_id}")
            return None

        try:
            pubmed_id = int(row["pubmed_id"])
        except (TypeError, ValueError) as e:
            self.logger.error(f"Invalid pubmed_id found: {row.get('pubmed_id')}. Error: {e}")
            return None

        # Keep gardId as the trimmed string used by GARD nodes, and pubmedId as
        # an integer to match Article node identity.
        return {
            "gardId": str(gard_id).strip(),
            "pubmedId": pubmed_id,
        }

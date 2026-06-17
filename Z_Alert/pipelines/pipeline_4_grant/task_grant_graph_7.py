"""
Create Memgraph CoreProject-Article relationships for new grant publication rows.

This alert-pipeline graph task is based on
`D_grant/initializer/core_project_article_relation.py`. The historical
initializer starts from `grant_gard_project_relation_unique_core_project_num`
and joins to `grant_linktable`. This task narrows the graph update to current
alert rows by reading `grant_linktable` and `grant_publication` directly where
both source tables have `is_new = 1`.

Relationship direction:
    The initializer creates the relationship from CoreProject to Article:

        (CoreProject)-[:has_publication]->(Article)

    This task keeps the same direction and relationship type.

Processing flow:
    1. Read distinct current new `(PMID, PROJECT_NUMBER)` pairs from
       `grant_linktable` joined to `grant_publication`.
    2. Keep only core project numbers that appear in `grant_gard_project_relation`,
       preserving the initializer's GARD-related CoreProject scope without the
       materialized unique-core work table.
    3. Match the existing Article node by `pubmedId`.
    4. Match the existing CoreProject node by `coreProjectNumber`.
    5. MERGE the CoreProject -> Article `has_publication` relationship.

Notes:
    This task expects Article and CoreProject nodes to already exist in Memgraph.
    Run the publication Article graph task and task_grant_graph_3.py before this
    task so both MATCH clauses can create the relationship.
"""

# Reference: D_grant/initializer/core_project_article_relation.py

import os
import sys
from typing import Any, Dict, List, Optional

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase


class NewCoreProjectArticleRelationshipGraphTask(PipelineBase):
    """Upsert current alert-run CoreProject-to-Article relationships into Memgraph."""

    BATCH_SIZE = 200

    '''
    Relationship-only alert task: match existing nodes and merge the edge. This
    avoids creating partial CoreProject or Article nodes if an upstream graph
    task has not loaded them yet.
    '''
    UPSERT_RELATIONSHIPS_CYPHER = '''
        UNWIND $chunks AS chunk
        MATCH (a:Article {pubmedId: chunk.pubmedId})
        MATCH (cp:CoreProject {coreProjectNumber: chunk.coreProjectNumber})
        MERGE (cp)-[:has_publication]->(a)
    '''

    '''
    `grant_gard_project_relation_unique_core_project_num` is not needed here.
    The EXISTS clause keeps the same "GARD-related core project" scope by using
    the source relationship table directly, while `grant_linktable.is_new = 1`
    and `grant_publication.is_new = 1` define the current alert input.
    '''
    FETCH_NEW_RELATIONSHIPS_QUERY = '''
        SELECT DISTINCT
            gl.PMID AS pubmed_id,
            gl.PROJECT_NUMBER AS core_project_num
        FROM grant_linktable AS gl
        INNER JOIN grant_publication AS gp
            ON gp.PMID = gl.PMID
            AND gp.is_new = 1
        WHERE
            gl.is_new = 1
            AND gl.PMID IS NOT NULL
            AND gl.PROJECT_NUMBER IS NOT NULL
            AND TRIM(gl.PROJECT_NUMBER) <> ''
            AND EXISTS (
                SELECT 1
                FROM grant_gard_project_relation AS gpr
                WHERE gpr.core_project_num = gl.PROJECT_NUMBER
                LIMIT 1
            )
        ORDER BY
            gl.PMID,
            gl.PROJECT_NUMBER
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("NewCoreProjectArticleRelationshipGraphTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Fetch current new CoreProject-Article pairs and upsert them into Memgraph."""

        fetch_cursor = None
        summary = {
            "batches_seen": 0,
            "batches_failed": 0,
            "rows_seen": 0,
            "rows_skipped": 0,
            "relationships_submitted": 0,
        }

        try:
            if self.mysql is None:
                self.logger.error("Unable to create MySQL connection.")
                return

            if self.memgraph is None:
                self.logger.error("Unable to create Memgraph connection.")
                return

            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(self.FETCH_NEW_RELATIONSHIPS_QUERY)

            while True:
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    break

                summary["batches_seen"] += 1
                summary["rows_seen"] += len(rows)

                chunks = self._build_relationship_chunks(rows)
                summary["rows_skipped"] += len(rows) - len(chunks)

                if not chunks:
                    self.logger.info(f"CoreProject-Article graph batch {summary['batches_seen']} had no valid rows.")
                    continue

                try:
                    self.memgraph.execute(self.UPSERT_RELATIONSHIPS_CYPHER, {"chunks": chunks})

                    summary["relationships_submitted"] += len(chunks)
                    self.logger.info(
                        f"Submitted {len(chunks)} CoreProject-Article relationships to Memgraph. "
                        f"Total submitted={summary['relationships_submitted']}."
                    )

                except Exception:
                    summary["batches_failed"] += 1
                    self.logger.exception(f"CoreProject-Article graph batch {summary['batches_seen']} failed. Continuing with next batch.")
                    continue

            self.logger.info(f"Completed CoreProject-Article relationship graph load. Summary={summary}")

        except Exception:
            self.logger.exception(f"NewCoreProjectArticleRelationshipGraphTask failed. Summary={summary}")
            return

        finally:
            if fetch_cursor is not None:
                fetch_cursor.close()

            self.close()


    def _build_relationship_chunks(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert MySQL rows into Memgraph relationship payload dictionaries."""

        chunks = []

        for row in rows:
            chunk = self._create_relationship_chunk(row)

            if chunk is None:
                continue

            chunks.append(chunk)

        return chunks


    def _create_relationship_chunk(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Build one relationship payload, returning None when required keys are missing."""

        core_project_num = row.get("core_project_num")

        try:
            pubmed_id = int(row.get("pubmed_id"))
        except (TypeError, ValueError) as exc:
            self.logger.error(f"Skipping CoreProject-Article relation with invalid pubmed_id={row.get('pubmed_id')!r}. Error={exc}")
            return None

        if not core_project_num:
            self.logger.error(f"Skipping CoreProject-Article relation without core_project_num. pubmed_id={pubmed_id}")
            return None

        return {
            "pubmedId": pubmed_id,
            "coreProjectNumber": str(core_project_num),
        }

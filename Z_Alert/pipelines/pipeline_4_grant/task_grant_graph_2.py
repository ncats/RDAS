"""
Create or update Memgraph Project-GARD relationships for new grant rows.
 
The historical initializer reads every unprocessed row from `grant_gard_project_relation`. 
This alert task narrows that input to only rows marked `grant_gard_project_relation.is_new = 1`, 
which are the GARD-project matches generated or refreshed during the current grant alert run.

Relationship direction:
    The initializer creates the relationship from Project to GARD:

        (Project)-[:has_researched_disease]->(GARD)

    This task keeps the same direction and relationship type so downstream
    graph queries continue to see grant projects the same way.

Processing flow:
    1. Read current new rows from `grant_gard_project_relation`.
    2. Convert MySQL row names into the relationship payload names used by Memgraph.
    3. Match the existing Project node by `applicationId`.
    4. Match the existing GARD node by `gardId`.
    5. MERGE the relationship and update confidence/source properties.

Notes:
    This task does not create Project or GARD nodes. Run task_grant_graph_1.py
    first so Project nodes exist; GARD nodes are expected to already exist from
    the base graph initialization.
"""

#Reference: D_grant/initializer/gard_project_relation.py

import os
import sys
from typing import Any, Dict, List, Optional

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _empty_if_none


class NewGardProjectRelationshipGraphTask(PipelineBase):
    """Upsert current alert-run Project-to-GARD relationships into Memgraph."""

    BATCH_SIZE = 200

    ''' Create or update one has_researched_disease relationship per Project/GARD pair. '''
    UPSERT_RELATIONSHIPS_CYPHER = '''
        UNWIND $chunks AS chunk
        MATCH (p:Project {applicationId: chunk.applicationId})
        MATCH (g:GARD {gardId: chunk.gardId})
        MERGE (p)-[r:has_researched_disease]->(g)
        SET
            r.confidenceScore = chunk.confidenceScore,
            r.semanticSimilarity = chunk.semanticSimilarity,
            r.sourceType = chunk.sourceType
    '''

    '''
    Only current alert rows are eligible. Rows without application_id or
    gard_id cannot be connected to graph nodes and are skipped before Cypher.
    '''
    FETCH_NEW_RELATIONSHIPS_QUERY = '''
        SELECT
            id,
            gard_id,
            application_id,
            gard_name,
            source_type,
            confidence_score,
            semantic_similarity
        FROM grant_gard_project_relation
        WHERE
            is_new = 1
            AND application_id IS NOT NULL
            AND gard_id IS NOT NULL
        ORDER BY id
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("NewGardProjectRelationshipGraphTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Fetch new GARD-project rows in batches and upsert relationships."""

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
                    self.logger.info(f"GARD-project graph batch {summary['batches_seen']} had no valid relationship rows.")
                    continue

                try:
                    self.memgraph.execute(self.UPSERT_RELATIONSHIPS_CYPHER, {"chunks": chunks})

                    summary["relationships_submitted"] += len(chunks)
                    self.logger.info(
                        f"Submitted {len(chunks)} GARD-project relationships to Memgraph. "
                        f"Total submitted={summary['relationships_submitted']}."
                    )

                except Exception:
                    summary["batches_failed"] += 1
                    self.logger.exception(f"GARD-project graph batch {summary['batches_seen']} failed. Continuing with next batch.")
                    continue

            self.logger.info(f"Completed GARD-project relationship graph load. Summary={summary}")

        except Exception:
            self.logger.exception(f"NewGardProjectRelationshipGraphTask failed. Summary={summary}")
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

        gard_id = row.get("gard_id")
        application_id = row.get("application_id")

        if not gard_id:
            self.logger.error(f"Skipping GARD-project relation without gard_id. id={row.get('id')}")
            return None

        if application_id is None:
            self.logger.error(f"Skipping GARD-project relation without application_id. id={row.get('id')}")
            return None

        return {
            "gardId": gard_id,
            "applicationId": application_id,
            "gardName": _empty_if_none(row.get("gard_name")),
            "sourceType": _empty_if_none(row.get("source_type")),
            "confidenceScore": self._score_to_graph_value(row.get("confidence_score")),
            "semanticSimilarity": self._score_to_graph_value(row.get("semantic_similarity")),
        }


    def _score_to_graph_value(self, value: Any) -> str:
        """Convert MySQL score values to strings, matching the historical initializer."""

        if value is None:
            return ""

        return str(value)

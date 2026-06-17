"""
Create Memgraph CoreProject-GARD relationships for new grant relationship rows.

This alert-pipeline graph task is based on
`D_grant/initializer/core_project_GARD_relation.py`. The historical initializer
reads all rows from `grant_gard_project_relation` with a non-null
`core_project_num`. This task narrows the graph update to current alert rows by
reading `grant_gard_project_relation` directly where `is_new = 1`.

Relationship direction:
    The initializer creates the relationship from CoreProject to GARD:

        (CoreProject)-[:has_researched_disease]->(GARD)

    This task keeps the same direction and relationship type.

Processing flow:
    1. Read distinct current new `(gard_id, core_project_num)` pairs.
    2. Match the existing GARD node by `gardId`.
    3. Match the existing CoreProject node by `coreProjectNumber`.
    4. MERGE the CoreProject -> GARD `has_researched_disease` relationship.

Notes:
    This task expects CoreProject and GARD nodes to already exist in Memgraph.
    Run task_grant_graph_3.py before this task so CoreProject nodes exist;
    GARD nodes are expected to already exist from the base graph initialization.
"""

# Reference: D_grant/initializer/core_project_GARD_relation.py

import os
import sys
from typing import Any, Dict, List, Optional

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase


class NewCoreProjectGardRelationshipGraphTask(PipelineBase):
    """Upsert current alert-run CoreProject-to-GARD relationships into Memgraph."""

    BATCH_SIZE = 200

    '''
    The historical initializer does not set relationship properties here; it
    only guarantees the CoreProject -> GARD disease edge exists.
    '''
    UPSERT_RELATIONSHIPS_CYPHER = '''
        UNWIND $chunks AS chunk
        MATCH (disease:GARD {gardId: chunk.gardId})
        MATCH (cp:CoreProject {coreProjectNumber: chunk.coreProjectNumber})
        MERGE (cp)-[:has_researched_disease]->(disease)
    '''

    '''
    Use MIN(id) only as a stable ordering/debug value while grouping duplicate
    CoreProject/GARD pairs from the current alert run.
    '''
    FETCH_NEW_RELATIONSHIPS_QUERY = '''
        SELECT
            MIN(id) AS id,
            gard_id,
            core_project_num
        FROM grant_gard_project_relation
        WHERE
            is_new = 1
            AND gard_id IS NOT NULL
            AND core_project_num IS NOT NULL
        GROUP BY
            gard_id,
            core_project_num
        ORDER BY id
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("NewCoreProjectGardRelationshipGraphTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Fetch current new CoreProject-GARD pairs and upsert them into Memgraph."""

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
                    self.logger.info(f"CoreProject-GARD graph batch {summary['batches_seen']} had no valid rows.")
                    continue

                try:
                    self.memgraph.execute(self.UPSERT_RELATIONSHIPS_CYPHER, {"chunks": chunks})

                    summary["relationships_submitted"] += len(chunks)
                    self.logger.info(
                        f"Submitted {len(chunks)} CoreProject-GARD relationships to Memgraph. "
                        f"Total submitted={summary['relationships_submitted']}."
                    )

                except Exception:
                    summary["batches_failed"] += 1
                    self.logger.exception(f"CoreProject-GARD graph batch {summary['batches_seen']} failed. Continuing with next batch.")
                    continue

            self.logger.info(f"Completed CoreProject-GARD relationship graph load. Summary={summary}")

        except Exception:
            self.logger.exception(f"NewCoreProjectGardRelationshipGraphTask failed. Summary={summary}")
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


    def _create_relationship_chunk(self, row: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """Build one relationship payload, returning None when required keys are missing."""

        gard_id = row.get("gard_id")
        core_project_num = row.get("core_project_num")

        if not gard_id:
            self.logger.error(f"Skipping CoreProject-GARD relation without gard_id. id={row.get('id')}")
            return None

        if not core_project_num:
            self.logger.error(f"Skipping CoreProject-GARD relation without core_project_num. id={row.get('id')}")
            return None

        return {
            "gardId": str(gard_id),
            "coreProjectNumber": str(core_project_num),
        }

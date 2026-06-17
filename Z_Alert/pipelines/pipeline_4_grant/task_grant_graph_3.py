"""
Create or update Memgraph CoreProject nodes for new grant pipeline rows.

This alert-pipeline graph task is based on `D_grant/initializer/core_project.py`.
The historical initializer reads unprocessed rows from
`grant_gard_project_relation_unique_application_id`; this task narrows that
same source to the current alert batch by using `gpru.is_new = 1` and
`grant_project.is_new = 1`.

Processing flow:
    1. Read current new grant application IDs from `grant_gard_project_relation_unique_application_id`.
    2. Join each application ID to `grant_project` to find its `core_project_num`.
    3. Calculate the CoreProject total cost across all grant_project rows with
       the same core_project_num, matching the historical initializer behavior.
    4. MERGE one CoreProject node by `coreProjectNumber`.
    5. Update CoreProject.totalCost and MERGE the CoreProject -> Project
       `has_subproject` relationship.

Notes:
    This task expects Project nodes to already exist in Memgraph. Run
    task_grant_graph_1.py before this task so the MATCH on Project.applicationId
    can create the relationship.
"""

# Reference: D_grant/initializer/core_project.py

import os
import sys
from typing import Any, Dict, List, Optional

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _to_number_or_blank


class NewCoreProjectGraphTask(PipelineBase):
    """Upsert current alert-run CoreProject nodes and CoreProject-Project links into Memgraph."""

    BATCH_SIZE = 200

    '''
    MERGE keeps one CoreProject node per coreProjectNumber. The relationship is
    also merged so rerunning the same alert batch updates totalCost without
    creating duplicate CoreProject -> Project edges.
    '''
    UPSERT_CORE_PROJECTS_CYPHER = '''
        UNWIND $chunks AS chunk
        MERGE (cp:CoreProject {coreProjectNumber: chunk.coreProjectNumber})
        ON CREATE SET
            cp.coreProjectNumber = chunk.coreProjectNumber
        SET
            cp.totalCost = chunk.totalCost

        WITH cp, chunk
        MATCH (p:Project {applicationId: chunk.applicationId})
        MERGE (cp)-[:has_subproject]->(p)
    '''

    '''
    Start from gpru because task_grant_10/task_grant_11 mark current
    GARD-related application IDs there with is_new=1. The CoreProject node total
    cost is calculated from all MySQL grant_project rows for the same
    core_project_num, just like the full initializer.
    '''
    FETCH_NEW_CORE_PROJECTS_QUERY = '''
        SELECT DISTINCT
            gpru.id,
            p.application_id,
            p.core_project_num,
            cost.total_cost_1,
            cost.total_cost_2
        FROM grant_gard_project_relation_unique_application_id AS gpru
        INNER JOIN grant_project AS p
            ON p.application_id = gpru.application_id
            AND p.is_new = 1
        LEFT JOIN (
            SELECT
                p2.core_project_num,
                SUM(p2.TOTAL_COST) AS total_cost_1,
                SUM(p2.DIRECT_COST_AMT + p2.INDIRECT_COST_AMT) AS total_cost_2
            FROM grant_project AS p2
            WHERE p2.core_project_num IS NOT NULL
            GROUP BY p2.core_project_num
        ) AS cost
            ON p.core_project_num = cost.core_project_num
        WHERE
            gpru.is_new = 1
            AND p.application_id IS NOT NULL
            AND p.core_project_num IS NOT NULL
        ORDER BY gpru.id
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("NewCoreProjectGraphTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Fetch current new CoreProject rows in batches and upsert them into Memgraph."""

        fetch_cursor = None
        summary = {
            "batches_seen": 0,
            "batches_failed": 0,
            "rows_seen": 0,
            "rows_skipped": 0,
            "core_projects_submitted": 0,
        }

        try:
            if self.mysql is None:
                self.logger.error("Unable to create MySQL connection.")
                return

            if self.memgraph is None:
                self.logger.error("Unable to create Memgraph connection.")
                return

            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(self.FETCH_NEW_CORE_PROJECTS_QUERY)

            while True:
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    break

                summary["batches_seen"] += 1
                summary["rows_seen"] += len(rows)

                chunks = self._build_core_project_chunks(rows)
                summary["rows_skipped"] += len(rows) - len(chunks)

                if not chunks:
                    self.logger.info(f"CoreProject graph batch {summary['batches_seen']} had no valid rows.")
                    continue

                try:
                    self.memgraph.execute(self.UPSERT_CORE_PROJECTS_CYPHER, {"chunks": chunks})

                    summary["core_projects_submitted"] += len(chunks)
                    self.logger.info(
                        f"Submitted {len(chunks)} CoreProject rows to Memgraph. "
                        f"Total submitted={summary['core_projects_submitted']}."
                    )

                except Exception:
                    summary["batches_failed"] += 1
                    self.logger.exception(f"CoreProject graph batch {summary['batches_seen']} failed. Continuing with next batch.")
                    continue

            self.logger.info(f"Completed CoreProject graph load. Summary={summary}")

        except Exception:
            self.logger.exception(f"NewCoreProjectGraphTask failed. Summary={summary}")
            return

        finally:
            if fetch_cursor is not None:
                fetch_cursor.close()

            self.close()


    def _build_core_project_chunks(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert MySQL rows into Memgraph CoreProject payload dictionaries."""

        chunks = []

        for row in rows:
            chunk = self._create_core_project_chunk(row)

            if chunk is None:
                continue

            chunks.append(chunk)

        return chunks


    def _create_core_project_chunk(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Build one CoreProject payload, returning None when required keys are missing."""

        application_id = row.get("application_id")
        core_project_num = row.get("core_project_num")

        if application_id is None:
            self.logger.error(f"Skipping CoreProject row without application_id. gpru.id={row.get('id')}")
            return None

        if not core_project_num:
            self.logger.error(f"Skipping CoreProject row without core_project_num. gpru.id={row.get('id')}")
            return None

        # Keep CoreProject.totalCost numeric for Memgraph. Prefer the calculated
        # direct + indirect amount; fall back to the exported TOTAL_COST.
        total_cost = row.get("total_cost_2")

        if total_cost in (None, ""):
            total_cost = row.get("total_cost_1")

        return {
            "applicationId": application_id,
            "coreProjectNumber": core_project_num,
            "totalCost": _to_number_or_blank(total_cost),
        }

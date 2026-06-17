"""
Create or update Memgraph Patent nodes for new grant patent rows.

This alert-pipeline graph task is based on `D_grant/initializer/patent.py`.
The historical initializer starts from `grant_gard_project_relation_unique_core_project_num` and uses a processed flag.
This task narrows the graph update to current alert rows by reading `grant_patent` directly where `is_new = 1`.

Relationship direction:
    The initializer creates the relationship from CoreProject to Patent:

        (CoreProject)-[:has_patent]->(Patent)

    This task keeps the same direction and relationship type.

Processing flow:
    1. Read current new rows from `grant_patent`.
    2. Convert patent rows into the Patent node property names used in Memgraph.
    3. MERGE Patent by `patentId`.
    4. Match the existing CoreProject node by `coreProjectNumber`.
    5. MERGE the CoreProject -> Patent `has_patent` relationship.

Notes:
    This task expects CoreProject nodes to already exist in Memgraph. Run
    task_grant_graph_3.py before this task so the MATCH on
    CoreProject.coreProjectNumber can create the relationship.
"""

# Reference: D_grant/initializer/patent.py

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


class NewPatentGraphTask(PipelineBase):
    """Upsert current alert-run Patent nodes and CoreProject-Patent links into Memgraph."""

    BATCH_SIZE = 200

    '''
    MERGE keeps one Patent node per patentId. The relationship is merged from
    CoreProject to Patent, matching the historical initializer and avoiding
    duplicate edges when a current patent row is processed more than once.
    '''
    UPSERT_PATENTS_CYPHER = '''
        UNWIND $chunks AS chunk
        MERGE (p:Patent {patentId: chunk.patentId})
        ON CREATE SET
            p.patentId = chunk.patentId
        SET
            p.title = chunk.title,
            p.orgName = chunk.orgName

        WITH p, chunk
        MATCH (cp:CoreProject {coreProjectNumber: chunk.coreProjectNumber})
        MERGE (cp)-[:has_patent]->(p)
    '''

    '''
    Read new patent rows directly from grant_patent. PROJECT_ID is the
    CoreProject number used by the historical initializer when it joins patents
    to grant_gard_project_relation_unique_core_project_num.core_project_num.
    '''
    FETCH_NEW_PATENTS_QUERY = '''
        SELECT
            id,
            PATENT_ID AS patent_id,
            PROJECT_ID AS project_id,
            PATENT_TITLE AS patent_title,
            PATENT_ORG_NAME AS patent_org_name
        FROM grant_patent
        WHERE
            is_new = 1
            AND PATENT_ID IS NOT NULL
            AND PROJECT_ID IS NOT NULL
        ORDER BY id
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("NewPatentGraphTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Fetch current new Patent rows in batches and upsert them into Memgraph."""

        fetch_cursor = None
        summary = {
            "batches_seen": 0,
            "batches_failed": 0,
            "rows_seen": 0,
            "rows_skipped": 0,
            "patents_submitted": 0,
        }

        try:
            if self.mysql is None:
                self.logger.error("Unable to create MySQL connection.")
                return

            if self.memgraph is None:
                self.logger.error("Unable to create Memgraph connection.")
                return

            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(self.FETCH_NEW_PATENTS_QUERY)

            while True:
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    break

                summary["batches_seen"] += 1
                summary["rows_seen"] += len(rows)

                chunks = self._build_patent_chunks(rows)
                summary["rows_skipped"] += len(rows) - len(chunks)

                if not chunks:
                    self.logger.info(f"Patent graph batch {summary['batches_seen']} had no valid rows.")
                    continue

                try:
                    self.memgraph.execute(self.UPSERT_PATENTS_CYPHER, {"chunks": chunks})

                    summary["patents_submitted"] += len(chunks)
                    self.logger.info(
                        f"Submitted {len(chunks)} Patent rows to Memgraph. "
                        f"Total submitted={summary['patents_submitted']}."
                    )

                except Exception:
                    summary["batches_failed"] += 1
                    self.logger.exception(f"Patent graph batch {summary['batches_seen']} failed. Continuing with next batch.")
                    continue

            self.logger.info(f"Completed Patent graph load. Summary={summary}")

        except Exception:
            self.logger.exception(f"NewPatentGraphTask failed. Summary={summary}")
            return

        finally:
            if fetch_cursor is not None:
                fetch_cursor.close()

            self.close()


    def _build_patent_chunks(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert MySQL rows into Memgraph Patent payload dictionaries."""

        chunks = []

        for row in rows:
            chunk = self._create_patent_chunk(row)

            if chunk is None:
                continue

            chunks.append(chunk)

        return chunks


    def _create_patent_chunk(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Build one Patent payload, returning None when required keys are missing."""

        patent_id = row.get("patent_id")
        core_project_num = row.get("project_id")

        if not patent_id:
            self.logger.error(f"Skipping Patent row without patent_id. id={row.get('id')}")
            return None

        if not core_project_num:
            self.logger.error(f"Skipping Patent row without project_id/core_project_num. id={row.get('id')}")
            return None

        return {
            "coreProjectNumber": _empty_if_none(core_project_num),
            "patentId": _empty_if_none(patent_id),
            "title": _empty_if_none(row.get("patent_title")),
            "orgName": _empty_if_none(row.get("patent_org_name")),
        }

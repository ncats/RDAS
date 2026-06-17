"""
Create Memgraph funding Organization nodes for new grant CoreProject rows.

This alert-pipeline graph task is based on `D_grant/initializer/funding_IC.py`.
The historical initializer reads unprocessed rows from
`grant_gard_project_relation_unique_application_id` and joins each application
to `grant_project`. This task narrows the graph update to current alert rows by
requiring both `gpru.is_new = 1` and `grant_project.is_new = 1`.

Relationship direction:
    The initializer creates the relationship from CoreProject to Organization:

        (CoreProject)-[:has_funding_organization]->(Organization)

    This task keeps the same direction and relationship type.

Processing flow:
    1. Read current new application IDs from `grant_gard_project_relation_unique_application_id`.
    2. Join each application ID to current new `grant_project` rows.
    3. Use `grant_project.IC_NAME` as the funding organization name.
    4. MERGE Organization by `_idx_key = _make_hash_key(IC_NAME)`.
    5. Match the existing CoreProject node by `coreProjectNumber`.
    6. MERGE the CoreProject -> Organization `has_funding_organization`
       relationship.

Notes:
    This task expects CoreProject nodes to already exist in Memgraph. Run
    task_grant_graph_3.py before this task so the MATCH on
    CoreProject.coreProjectNumber can create the relationship.
"""

# Reference: D_grant/initializer/funding_IC.py

import os
import sys
from typing import Any, Dict, List, Optional

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _make_hash_key


class NewFundingIcGraphTask(PipelineBase):
    """Upsert current alert-run funding Organization nodes and CoreProject links into Memgraph."""

    BATCH_SIZE = 300

    '''
    Match CoreProject first so the task only creates Organization nodes when the
    grant CoreProject graph step has already loaded the source CoreProject.
    Organization defaults match D_grant/initializer/funding_IC.py.
    '''
    UPSERT_FUNDING_ORGS_CYPHER = '''
        UNWIND $chunks AS chunk
        MATCH (cp:CoreProject {coreProjectNumber: chunk.coreProjectNumber})

        MERGE (org:Organization {_idx_key: chunk._idx_key})
        ON CREATE SET
            org.name = chunk.name,
            org.displayName = '',
            org.ror_id = '',
            org.website = '',
            org.types = []

        MERGE (cp)-[:has_funding_organization]->(org)
    '''

    '''
    Preserve the initializer's coreProjectNumber fallback: prefer
    core_project_num, then full_project_num. IC_NAME is hashed directly by
    _make_hash_key without removing parenthetical text.
    '''
    FETCH_NEW_FUNDING_ICS_QUERY = '''
        SELECT DISTINCT
            gpru.id,
            p.application_id,
            p.core_project_num,
            p.full_project_num,
            p.IC_NAME AS ic_name
        FROM grant_gard_project_relation_unique_application_id AS gpru
        INNER JOIN grant_project AS p
            ON p.application_id = gpru.application_id
            AND p.is_new = 1
        WHERE
            gpru.is_new = 1
            AND p.application_id IS NOT NULL
            AND p.IC_NAME IS NOT NULL
            AND TRIM(p.IC_NAME) <> ''
            AND (
                p.core_project_num IS NOT NULL
                OR p.full_project_num IS NOT NULL
            )
        ORDER BY gpru.id
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("NewFundingIcGraphTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Fetch current new funding IC rows and upsert them into Memgraph."""

        fetch_cursor = None
        summary = {
            "batches_seen": 0,
            "batches_failed": 0,
            "rows_seen": 0,
            "rows_skipped": 0,
            "organizations_submitted": 0,
        }

        try:
            if self.mysql is None:
                self.logger.error("Unable to create MySQL connection.")
                return

            if self.memgraph is None:
                self.logger.error("Unable to create Memgraph connection.")
                return

            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(self.FETCH_NEW_FUNDING_ICS_QUERY)

            while True:
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    break

                summary["batches_seen"] += 1
                summary["rows_seen"] += len(rows)

                chunks = self._build_funding_org_chunks(rows)
                summary["rows_skipped"] += len(rows) - len(chunks)

                if not chunks:
                    self.logger.info(f"Funding IC graph batch {summary['batches_seen']} had no valid rows.")
                    continue

                try:
                    self.memgraph.execute(self.UPSERT_FUNDING_ORGS_CYPHER, {"chunks": chunks})

                    summary["organizations_submitted"] += len(chunks)
                    self.logger.info(
                        f"Submitted {len(chunks)} funding Organization rows to Memgraph. "
                        f"Total submitted={summary['organizations_submitted']}."
                    )

                except Exception:
                    summary["batches_failed"] += 1
                    self.logger.exception(f"Funding IC graph batch {summary['batches_seen']} failed. Continuing with next batch.")
                    continue

            self.logger.info(f"Completed funding IC graph load. Summary={summary}")

        except Exception:
            self.logger.exception(f"NewFundingIcGraphTask failed. Summary={summary}")
            return

        finally:
            if fetch_cursor is not None:
                fetch_cursor.close()

            self.close()


    def _build_funding_org_chunks(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert MySQL rows into Memgraph funding Organization payload dictionaries."""

        chunks = []

        for row in rows:
            chunk = self._create_funding_org_chunk(row)

            if chunk is None:
                continue

            chunks.append(chunk)

        return chunks


    def _create_funding_org_chunk(self, row: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """Build one funding Organization payload, returning None when required keys are missing."""

        core_project_num = row.get("core_project_num") or row.get("full_project_num")
        ic_name = row.get("ic_name")

        if not core_project_num:
            self.logger.error(f"Skipping funding IC row without core_project_num/full_project_num. gpru.id={row.get('id')}")
            return None

        if not ic_name:
            self.logger.error(f"Skipping funding IC row without IC_NAME. gpru.id={row.get('id')}")
            return None

        ic_name = str(ic_name).strip()

        if not ic_name:
            self.logger.error(f"Skipping funding IC row with blank IC_NAME. gpru.id={row.get('id')}")
            return None

        return {
            "coreProjectNumber": str(core_project_num).strip(),
            "name": ic_name,
            "_idx_key": _make_hash_key(ic_name),
        }

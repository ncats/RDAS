"""
Create or update Memgraph Project nodes for new grant pipeline rows.

This alert-pipeline graph task is based on `D_grant/initializer/project.py`.
The historical initializer creates Project nodes from
`grant_gard_project_relation_unique_application_id`; this task narrows that
same source to current alert rows by using `gpru.is_new = 1` and
`grant_project.is_new = 1`.

Processing flow:
    1. Fetch current new grant application IDs from `grant_gard_project_relation_unique_application_id`.
    2. Join each application ID to `grant_project` for Project properties.
    3. Join to `grant_abstract` using both application ID and fiscal year, 
       so an abstract from another fiscal year is not accidentally attached.
    4. Convert each MySQL row into the Project node property names used in Memgraph.
    5. MERGE Project by `applicationId`; if it exists, update its properties.

Cost handling:
    Project.totalCost is stored as a number, not a formatted dollar string.
    The task prefers `DIRECT_COST_AMT + INDIRECT_COST_AMT`; when that sum is
    missing, it falls back to `TOTAL_COST`.
"""

# Reference: D_grant/initializer/project.py

import os
import sys
from typing import Any, Dict, List, Optional

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _date_string, _empty_if_none, _to_number_or_blank


class NewProjectGraphTask(PipelineBase):
    """Upsert current alert-run grant Project rows into Memgraph."""

    BATCH_SIZE = 200

    '''
    MERGE keeps one Project node per applicationId. SET n += props updates
    existing nodes with fresh grant_project values while preserving any
    unrelated properties that may have been added by later graph workflows.
    '''
    UPSERT_PROJECTS_CYPHER = '''
        UNWIND $batch_chunks AS props
        MERGE (n:Project {applicationId: props.applicationId})
        SET n += props
    '''

    '''
    This query intentionally starts from gpru because task_grant_10/11 mark
    current GARD-related application IDs there with is_new=1. The graph should
    only load grant Projects that are relevant to the alert batch.
    '''
    FETCH_NEW_PROJECTS_QUERY = '''
        SELECT DISTINCT
            gpru.id,
            p.application_id,
            p.application_type,
            p.project_title,
            p.project_terms,
            p.ACTIVITY,
            p.FY,
            p.PHR,
            p.SUPPORT_YEAR,
            p.TOTAL_COST AS total_cost_1,
            p.DIRECT_COST_AMT + p.INDIRECT_COST_AMT AS total_cost_2,
            p.FOA_NUMBER,
            p.FULL_PROJECT_NUM,
            p.CORE_PROJECT_NUM,
            p.CFDA_CODE,
            p.SERIAL_NUMBER,
            p.STUDY_SECTION,
            p.STUDY_SECTION_NAME,
            p.FUNDING_MECHANISM,
            a.abstract_text
        FROM grant_gard_project_relation_unique_application_id AS gpru
        INNER JOIN grant_project AS p
            ON p.application_id = gpru.application_id
            AND p.is_new = 1
        LEFT JOIN grant_abstract AS a
            ON a.application_id = p.application_id
            AND a.year = p.FY
        WHERE
            gpru.is_new = 1
            AND p.application_id IS NOT NULL
        ORDER BY gpru.id
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("NewProjectGraphTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Fetch new grant Project rows in batches and upsert them into Memgraph."""

        fetch_cursor = None
        summary = {
            "batches_seen": 0,
            "batches_failed": 0,
            "rows_seen": 0,
            "rows_skipped": 0,
            "projects_submitted": 0,
        }

        try:
            if self.mysql is None:
                self.logger.error("Unable to create MySQL connection.")
                return

            if self.memgraph is None:
                self.logger.error("Unable to create Memgraph connection.")
                return

            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(self.FETCH_NEW_PROJECTS_QUERY)

            while True:
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    break

                summary["batches_seen"] += 1
                summary["rows_seen"] += len(rows)

                batch_chunks = self._build_project_nodes(rows)
                summary["rows_skipped"] += len(rows) - len(batch_chunks)

                if not batch_chunks:
                    self.logger.info(f"Project graph batch {summary['batches_seen']} had no valid Project rows.")
                    continue

                try:
                    self.memgraph.execute(self.UPSERT_PROJECTS_CYPHER, {"batch_chunks": batch_chunks})

                    summary["projects_submitted"] += len(batch_chunks)
                    self.logger.info(
                        f"Submitted {len(batch_chunks)} Project nodes to Memgraph. "
                        f"Total submitted={summary['projects_submitted']}."
                    )

                except Exception:
                    summary["batches_failed"] += 1
                    self.logger.exception(f"Project graph batch {summary['batches_seen']} failed. Continuing with next batch.")
                    continue

            self.logger.info(f"Completed Project graph load. Summary={summary}")

        except Exception:
            self.logger.exception(f"NewProjectGraphTask failed. Summary={summary}")
            return

        finally:
            if fetch_cursor is not None:
                fetch_cursor.close()

            self.close()


    def _build_project_nodes(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert MySQL rows into Memgraph Project property dictionaries."""

        project_nodes = []

        for row in rows:
            project_node = self._create_project_node(row)

            if project_node is None:
                continue

            project_nodes.append(project_node)

        return project_nodes


    def _create_project_node(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Build one Project node payload, returning None when the key is missing."""

        application_id = row.get("application_id")

        if application_id is None:
            self.logger.error(f"Skipping Project row without application_id. gpru.id={row.get('id')}")
            return None

        # Keep Project.totalCost numeric for Memgraph. Prefer the calculated
        # direct + indirect amount; fall back to the exported TOTAL_COST.
        total_cost = row.get("total_cost_2")

        if total_cost in (None, ""):
            total_cost = row.get("total_cost_1")

        return {
            "applicationId": application_id,
            "abstract": _empty_if_none(row.get("abstract_text")),
            "activity": _empty_if_none(row.get("ACTIVITY")),
            "applicationType": _empty_if_none(row.get("application_type")),
            "cfdaCode": _empty_if_none(row.get("CFDA_CODE")),
            "coreProjectNumber": _empty_if_none(row.get("CORE_PROJECT_NUM")),
            "dateCreatedRDAS": _date_string(),
            "foaNumber": _empty_if_none(row.get("FOA_NUMBER")),
            "fullProjectNumber": _empty_if_none(row.get("FULL_PROJECT_NUM")),
            "fundingMechanism": _empty_if_none(row.get("FUNDING_MECHANISM")),
            "fundingYear": row.get("FY"),
            "phr": _empty_if_none(row.get("PHR")),
            "serialNumber": _empty_if_none(row.get("SERIAL_NUMBER")),
            "studySection": _empty_if_none(row.get("STUDY_SECTION")),
            "studySectionName": _empty_if_none(row.get("STUDY_SECTION_NAME")),
            "supportYear": row.get("SUPPORT_YEAR"),
            "terms": _empty_if_none(row.get("project_terms")),
            "title": _empty_if_none(row.get("project_title")),
            "totalCost": _to_number_or_blank(total_cost),
        }

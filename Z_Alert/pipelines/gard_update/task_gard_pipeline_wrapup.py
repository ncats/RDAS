"""
Final GARD update pipeline step.

GARD initialization does not use `is_new` marker columns like the alert update
pipelines. The wrap-up step therefore records summary counts only.
"""

import os
import sys
from typing import Any, Dict

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "../..")),
    os.path.abspath(os.path.join(_dir, "../../..")),
])

from pipelines.pipeline_base import PipelineBase


MYSQL_GARD_SUMMARY_SQL = """
    SELECT
        COUNT(*) AS gard_rows,
        COUNT(DISTINCT GardID) AS gard_ids,
        COUNT(DISTINCT CASE WHEN Label_Predicate_Type = 'Name' THEN GardID END) AS gard_ids_with_names
    FROM gard
"""


class GardUpdatePipelineWrapUpTask(PipelineBase):

    """Log final MySQL and Memgraph counts for the GARD update run."""

    def __init__(self):

        super().__init__(init_mysql=True, init_memgraph=True)


    def find_new_data(self, gard_node) -> None:

        raise NotImplementedError("GardUpdatePipelineWrapUpTask does not implement find_new_data().")


    def process_new_data(self) -> None:

        cursor = None

        try:
            cursor = self.mysql.cursor(dictionary=True)
            cursor.execute(MYSQL_GARD_SUMMARY_SQL)
            mysql_summary = cursor.fetchone() or {}

            memgraph_summary = {
                "gard_nodes": self._fetch_memgraph_count("MATCH (g:GARD) RETURN count(g) AS count"),
                "gard_disease_nodes": self._fetch_memgraph_count("MATCH (g:GARD:Disease) RETURN count(g) AS count"),
                "gard_gene_relationships": self._fetch_memgraph_count("MATCH (:GARD)-[r:has_associated_gene]->(:Gene) RETURN count(r) AS count"),
                "gard_phenotype_relationships": self._fetch_memgraph_count("MATCH (:GARD)-[r:has_phenotype]->(:Phenotype) RETURN count(r) AS count"),
                "gard_hierarchy_relationships": self._fetch_memgraph_count("MATCH (:GARD)-[r:subclass_of]->(:GARD) RETURN count(r) AS count"),
            }

            self.logger.info(f"GARD update MySQL summary: {mysql_summary}")
            self.logger.info(f"GARD update Memgraph summary: {memgraph_summary}")
            self.logger.info("Completed GARD update pipeline wrap-up.")

        except Exception:
            self.logger.exception("GardUpdatePipelineWrapUpTask failed.")
            raise

        finally:
            if cursor:
                cursor.close()

            self.close()


    def _fetch_memgraph_count(self, cypher: str) -> int:

        """Run one Memgraph count query and return the numeric result."""

        rows = list(self.memgraph.execute_and_fetch(cypher))

        if not rows:
            return 0

        row: Dict[str, Any] = rows[0]
        return int(row.get("count") or 0)


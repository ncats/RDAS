"""
Step 2: initialize Memgraph GARD/Disease nodes from MySQL table `gard`.

This is a concrete graph task. It keeps the Cypher and row transformation in
this file instead of importing the legacy initializer class.
"""

import os
import sys
from typing import Any, Dict, List

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "../..")),
    os.path.abspath(os.path.join(_dir, "../../..")),
])

from pipelines.gard_update.gard_common import create_memgraph_indexes_if_missing, none_to_empty, split_delimited_values, validate_batch_size
from pipelines.pipeline_base import PipelineBase


FETCH_GARD_NODE_ROWS_SQL = """
    SELECT
        GardID,
        MONDO_ID,
        GROUP_CONCAT(DISTINCT ORPHA_Code) AS orphaCode,
        GROUP_CONCAT(DISTINCT Classification_Level) AS classificationLevel,
        GROUP_CONCAT(DISTINCT Disorder_Type) AS disorderType,
        MAX(CASE WHEN Label_Predicate_Type = 'Name' THEN Label END) AS name,
        GROUP_CONCAT(CASE WHEN Label_Predicate_Type = 'Synonym' THEN Label END SEPARATOR '$$$') AS synonyms,
        GROUP_CONCAT(Label_Xref SEPARATOR ',') AS xrefs,
        Label_Source
    FROM gard
    WHERE
        Label_Predicate_Mapping != 'DEPRECATED'
        AND LENGTH(Label) > 3
    GROUP BY
        GardID,
        MONDO_ID,
        Label_Source
"""

COUNT_GARD_NODE_ROWS_SQL = f"SELECT COUNT(*) AS row_count FROM ({FETCH_GARD_NODE_ROWS_SQL}) AS gard_nodes"

UPSERT_GARD_NODES_CYPHER = """
    UNWIND $chunks AS chunk
    MERGE (d:GARD {gardId: chunk.gardId})
    ON CREATE SET
        d.gardName = chunk.gardName,
        d.classificationLevel = chunk.classificationLevel,
        d.disorderType = chunk.disorderType,
        d.synonyms = chunk.synonyms,
        d.countEpiArticles = 0,
        d.countNhsArticles = 0
    ON MATCH SET
        d.gardName = chunk.gardName,
        d.classificationLevel = chunk.classificationLevel,
        d.disorderType = chunk.disorderType,
        d.synonyms = chunk.synonyms
"""

ADD_DISEASE_LABEL_CYPHER = "MATCH (n:GARD) SET n:Disease"

INITIALIZE_COUNT_PROPERTIES_CYPHER = """
    MATCH (n:GARD)
    SET
        n.countArticles = coalesce(n.countArticles, 0),
        n.countProjects = coalesce(n.countProjects, 0),
        n.countTrials = coalesce(n.countTrials, 0),
        n.countGenes = coalesce(n.countGenes, 0),
        n.countPhenotypes = coalesce(n.countPhenotypes, 0),
        n.countEpiArticles = coalesce(n.countEpiArticles, 0),
        n.countNhsArticles = coalesce(n.countNhsArticles, 0)
"""


class GardGraphNodeInitializationTask(PipelineBase):

    """Create/update `:GARD` nodes and attach the shared `:Disease` label."""

    def __init__(self, batch_size: int = 100):

        super().__init__(init_mysql=True, init_memgraph=True)
        self.batch_size = validate_batch_size(batch_size)


    def find_new_data(self, gard_node) -> None:

        raise NotImplementedError("GardGraphNodeInitializationTask does not implement find_new_data().")


    def process_new_data(self) -> None:

        cursor = None

        try:
            create_memgraph_indexes_if_missing(
                self.memgraph,
                {
                    "GARD": ["gardId"],
                    "Disease": ["gardId"],
                },
                self.logger,
            )

            source_count = self._count_source_rows()
            self.logger.info(f"MySQL source GARD node rows available={source_count}.")

            cursor = self.mysql.cursor(dictionary=True, buffered=True)
            cursor.execute(FETCH_GARD_NODE_ROWS_SQL)

            total_submitted = 0
            batch_number = 0

            while True:
                rows = cursor.fetchmany(self.batch_size)

                if not rows:
                    break

                chunks = [
                    chunk
                    for row in rows
                    for chunk in [self._build_gard_node_chunk(row)]
                    if chunk
                ]

                if not chunks:
                    continue

                self.memgraph.execute(UPSERT_GARD_NODES_CYPHER, {"chunks": chunks})
                total_submitted += len(chunks)
                batch_number += 1
                self.logger.info(f"Upserted GARD node batch={batch_number}, rows={len(chunks)}, total={total_submitted}.")

            """
            A `GARD` node is also a disease node in the RDAS graph. Count fields
            are initialized with coalesce so a re-run fills missing properties
            without resetting counts that follow-up tasks already calculated.
            """
            self.memgraph.execute(ADD_DISEASE_LABEL_CYPHER)
            self.memgraph.execute(INITIALIZE_COUNT_PROPERTIES_CYPHER)
            self.logger.info(f"Completed Memgraph GARD node initialization. Submitted rows={total_submitted}.")

        except Exception:
            self.logger.exception("GardGraphNodeInitializationTask failed.")
            raise

        finally:
            if cursor:
                cursor.close()

            self.close()


    def _count_source_rows(self) -> int:

        """Count the grouped MySQL rows that feed the GARD graph node load."""

        cursor = self.mysql.cursor(dictionary=True)

        try:
            cursor.execute(COUNT_GARD_NODE_ROWS_SQL)
            row = cursor.fetchone() or {}
            return int(row.get("row_count") or 0)

        finally:
            cursor.close()


    @staticmethod
    def _build_gard_node_chunk(row: Dict[str, Any]) -> Dict[str, Any]:

        """Convert one grouped MySQL `gard` row into Memgraph properties."""

        row = none_to_empty(row)
        gard_id = str(row["GardID"]).strip()

        if not gard_id:
            return {}

        return {
            "gardId": gard_id,
            "gardName": row["name"],
            "classificationLevel": split_delimited_values(row["classificationLevel"]),
            "disorderType": split_delimited_values(row["disorderType"]),
            "synonyms": split_delimited_values(row["synonyms"], delimiter="$$$"),
            "xrefs": split_delimited_values(row["xrefs"]),
        }

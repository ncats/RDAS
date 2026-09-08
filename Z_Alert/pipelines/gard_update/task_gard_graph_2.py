"""
Step 3: update Memgraph GARD nodes with cross-reference properties.

The task reads `GARD_Disease_Xref_pivoted-20250214.csv` from the local
`gard_update/data/2025` folder and updates existing `:GARD` nodes.
"""

import os
import sys
from typing import Any, Dict, List

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "../..")),
    os.path.abspath(os.path.join(_dir, "../../..")),
])

from pipelines.gard_update.gard_common import GARD_XREF_FILE, create_memgraph_indexes_if_missing, iter_csv_dict_rows, log_csv_file_summary, resolve_data_file, split_bracketed_values, validate_batch_size
from pipelines.pipeline_base import PipelineBase


UPDATE_GARD_XREFS_CYPHER = """
    UNWIND $chunks AS chunk
    MATCH (x:GARD {gardId: chunk.gardId})
    SET
        x.diseaseType = chunk.diseaseType,
        x.mondo = chunk.mondo,
        x.orphanet = chunk.orphanet,
        x.omim = chunk.omim,
        x.omimps = chunk.omimps,
        x.medGen = chunk.medGen,
        x.umls = chunk.umls,
        x.mesh = chunk.mesh,
        x.nctid = chunk.nctid,
        x.sctid = chunk.sctid,
        x.doid = chunk.doid,
        x.idc10cm = chunk.idc10cm
"""


class GardGraphXrefUpdateTask(PipelineBase):
    """Update xref-related properties on existing `:GARD` nodes."""

    def __init__(self, data_file: Any = GARD_XREF_FILE, batch_size: int = 100):

        super().__init__(init_mysql=False, init_memgraph=True)
        self.data_file = resolve_data_file(data_file)
        self.batch_size = validate_batch_size(batch_size)


    def find_new_data(self, gard_node) -> None:
        raise NotImplementedError("GardGraphXrefUpdateTask does not implement find_new_data().")


    def process_new_data(self) -> None:

        try:
            log_csv_file_summary(self.logger, "GARD xref", self.data_file)
            create_memgraph_indexes_if_missing(self.memgraph, {"GARD": ["gardId"]}, self.logger)

            chunks: List[Dict[str, Any]] = []
            total_submitted = 0
            batch_number = 0

            for row_index, row in enumerate(iter_csv_dict_rows(self.data_file), start=2):
                try:
                    chunks.append(self._build_xref_chunk(row))

                except KeyError as e:
                    self.logger.error(f"Skipping xref CSV row {row_index}; missing required column: {e}")
                    continue

                if len(chunks) >= self.batch_size:
                    batch_number += 1
                    total_submitted = self._submit_batch(chunks, batch_number, total_submitted)
                    chunks = []

            if chunks:
                batch_number += 1
                total_submitted = self._submit_batch(chunks, batch_number, total_submitted)

            self.logger.info(f"Completed GARD xref graph update. Submitted rows={total_submitted}.")

        except Exception:
            self.logger.exception("GardGraphXrefUpdateTask failed.")
            raise

        finally:
            self.close()


    def _submit_batch(self, chunks: List[Dict[str, Any]], batch_number: int, total_submitted: int) -> int:
        """Submit one xref batch and return the new submitted count."""

        self.memgraph.execute(UPDATE_GARD_XREFS_CYPHER, {"chunks": chunks})
        total_submitted += len(chunks)
        self.logger.info(f"Updated GARD xref batch={batch_number}, rows={len(chunks)}, total={total_submitted}.")

        return total_submitted


    @staticmethod
    def _build_xref_chunk(row: Dict[str, Any]) -> Dict[str, Any]:
        """Convert one xref CSV row into Memgraph properties."""

        return {
            "gardId": row["GardID_RDIPv2"],
            "diseaseType": row.get("Type") or "",
            "mondo": row.get("Equivalent_MONDO") or "",
            "orphanet": row.get("Equivalent_Orphanet") or "",
            "omim": row.get("Equivalent_OMIM") or "",
            "omimps": row.get("Equivalent_OMIMPS") or "",
            "medGen": row.get("Equivalent_MedGen") or "",
            "umls": row.get("Equivalent_UMLS") or "",
            "mesh": split_bracketed_values(row.get("ExactMatch_MESH")),
            "nctid": split_bracketed_values(row.get("ExactMatch_NCIT")),
            "sctid": split_bracketed_values(row.get("ExactMatch_SCTID")),
            "doid": split_bracketed_values(row.get("ExactMatch_DOID")),
            "idc10cm": split_bracketed_values(row.get("ExactMatch_ICD10CM")),
        }

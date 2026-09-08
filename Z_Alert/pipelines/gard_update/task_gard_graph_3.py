"""
Step 4: initialize Gene nodes and GARD-to-Gene relationships in Memgraph.

This concrete graph task reads the local GARD gene association CSV and writes
`:Gene` nodes plus `:has_associated_gene` relationships directly.
"""

import os
import sys
from typing import Any, Dict, List

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "../..")),
    os.path.abspath(os.path.join(_dir, "../../..")),
])

from pipelines.gard_update.gard_common import GARD_GENE_FILE, clean_optional_text, create_memgraph_indexes_if_missing, iter_csv_dict_rows, log_csv_file_summary, resolve_data_file, split_bracketed_values, validate_batch_size
from pipelines.pipeline_base import PipelineBase


UPSERT_GARD_GENE_CYPHER = """
    UNWIND $chunks AS chunk
    MATCH (g:GARD {gardId: chunk.gardId})
    WITH g, chunk
    WHERE chunk.geneIdentifier <> ''
    MERGE (d:Gene {geneIdentifier: chunk.geneIdentifier})
    ON CREATE SET
        d.geneSymbol = chunk.geneSymbol,
        d.geneSynonyms = chunk.geneSynonyms,
        d.geneTitle = chunk.geneTitle,
        d.geneType = chunk.geneType,
        d.geneUrl = chunk.geneUrl,
        d.locus = chunk.locus,
        d.locusGroup = chunk.locusGroup,
        d.medGen = chunk.medGen,
        d.mondo = chunk.mondo,
        d.omim = chunk.omim,
        d.orphanet = chunk.orphanet,
        d.umls = chunk.umls
    MERGE (g)-[r:has_associated_gene]->(d)
    SET r.Reference = chunk.reference
"""


class GardGraphGeneRelationshipInitializationTask(PipelineBase):

    """Create Gene nodes and connect them to existing GARD nodes."""

    def __init__(self, data_file: Any = GARD_GENE_FILE, batch_size: int = 100):

        super().__init__(init_mysql=False, init_memgraph=True)
        self.data_file = resolve_data_file(data_file)
        self.batch_size = validate_batch_size(batch_size)


    def find_new_data(self, gard_node) -> None:

        raise NotImplementedError("GardGraphGeneRelationshipInitializationTask does not implement find_new_data().")


    def process_new_data(self) -> None:

        try:
            log_csv_file_summary(self.logger, "GARD gene", self.data_file)
            create_memgraph_indexes_if_missing(
                self.memgraph,
                {
                    "GARD": ["gardId"],
                    "Gene": ["geneIdentifier"],
                },
                self.logger,
            )

            chunks: List[Dict[str, Any]] = []
            total_submitted = 0
            batch_number = 0

            for row_index, row in enumerate(iter_csv_dict_rows(self.data_file), start=2):
                try:
                    chunk = self._build_gene_chunk(row)

                except KeyError as e:
                    self.logger.error(f"Skipping gene CSV row {row_index}; missing required column: {e}")
                    continue

                if not chunk:
                    continue

                chunks.append(chunk)

                if len(chunks) >= self.batch_size:
                    batch_number += 1
                    total_submitted = self._submit_batch(chunks, batch_number, total_submitted)
                    chunks = []

            if chunks:
                batch_number += 1
                total_submitted = self._submit_batch(chunks, batch_number, total_submitted)

            self.logger.info(f"Completed GARD gene graph initialization. Submitted rows={total_submitted}.")

        except Exception:
            self.logger.exception("GardGraphGeneRelationshipInitializationTask failed.")
            raise

        finally:
            self.close()


    def _submit_batch(self, chunks: List[Dict[str, Any]], batch_number: int, total_submitted: int) -> int:

        """Submit one Gene relationship batch and return the new submitted count."""

        self.memgraph.execute(UPSERT_GARD_GENE_CYPHER, {"chunks": chunks})
        total_submitted += len(chunks)
        self.logger.info(f"Upserted GARD gene batch={batch_number}, rows={len(chunks)}, total={total_submitted}.")
        return total_submitted


    @staticmethod
    def _build_gene_chunk(row: Dict[str, Any]) -> Dict[str, Any]:

        """Convert one gene association CSV row into graph-ready properties."""

        gard_id = clean_optional_text(row["GardID"])
        gene_identifier = clean_optional_text(row["GeneIdentifier"])

        if not gard_id or not gene_identifier:
            return {}

        return {
            "gardId": gard_id,
            "geneIdentifier": gene_identifier,
            "geneSymbol": clean_optional_text(row.get("GeneSymbol")),
            "geneSynonyms": split_bracketed_values(row.get("GeneSynonym")),
            "geneTitle": clean_optional_text(row.get("GeneTitle")),
            "geneType": clean_optional_text(row.get("GeneType")),
            "geneUrl": clean_optional_text(row.get("gene_url")),
            "locus": clean_optional_text(row.get("Locus")),
            "locusGroup": clean_optional_text(row.get("locus_group")),
            "medGen": clean_optional_text(row.get("Equivalent_MedGen")),
            "mondo": clean_optional_text(row.get("Equivalent_MONDO")),
            "omim": clean_optional_text(row.get("OMIM")),
            "orphanet": clean_optional_text(row.get("Equivalent_Orphanet")),
            "reference": split_bracketed_values(row.get("Reference"), delimiter=";"),
            "umls": clean_optional_text(row.get("Equivalent_UMLS")),
        }

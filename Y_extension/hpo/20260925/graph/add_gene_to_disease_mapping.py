"""Load HPO gene-to-disease associations from MySQL into Memgraph."""

import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from baseclass.conn import DBConnection


BATCH_SIZE = 1_000
SELECT_SQL = """
    SELECT id, ncbi_gene_id, gene_symbol, disease_id
    FROM rdas_db.extension_hpo_genes_to_disease
"""
MERGE_CYPHER = """
    UNWIND $rows AS row
    MATCH (gard:GARD)
    WHERE gard.gardId = row.diseaseId
       OR gard.mondo = row.diseaseId
       OR gard.omim = row.diseaseId
       OR gard.orphanet = row.diseaseId
    MERGE (gene:Gene {geneIdentifier: row.ncbiGeneId})
    ON CREATE SET
        gene.geneSymbol = row.geneSymbol,
        gene.geneSynonyms = [],
        gene.geneTitle = '',
        gene.geneType = '',
        gene.geneUrl = '',
        gene.locus = '',
        gene.locusGroup = '',
        gene.medGen = '',
        gene.mondo = row.mondo,
        gene.omim = row.omim,
        gene.orphanet = row.orphanet,
        gene.umls = '',
        gene.countDiseases = 0,
        gene.source = 'hpo'
    MERGE (gard)-[:has_associated_gene]->(gene)
    RETURN count(DISTINCT row.sourceRowId) AS matchedCount
"""


def main() -> None:

    connections = DBConnection()
    mysql = connections.mysql_conn()
    memgraph = connections.memgraph_conn()

    if mysql is None:
        raise ConnectionError("Unable to connect to MySQL.")

    if memgraph is None:
        mysql.close()
        raise ConnectionError("Unable to connect to Memgraph.")

    cursor = mysql.cursor(dictionary=True)
    total_source_rows = 0
    total_matched_rows = 0
    total_ignored_rows = 0
    batch_count = 0

    try:
        cursor.execute(SELECT_SQL)

        while True:
            mysql_rows = cursor.fetchmany(BATCH_SIZE)

            if not mysql_rows:
                break

            rows = []

            for row in mysql_rows:
                if not row["ncbi_gene_id"] or not row["disease_id"]:
                    continue

                disease_id = str(row["disease_id"]).strip()
                rows.append(
                    {
                        "sourceRowId": row["id"],
                        "ncbiGeneId": str(row["ncbi_gene_id"]).strip(),
                        "geneSymbol": str(row["gene_symbol"] or "").strip(),
                        "diseaseId": disease_id,
                        "mondo": disease_id if disease_id.startswith("MONDO:") else "",
                        "omim": disease_id if disease_id.startswith("OMIM:") else "",
                        "orphanet": disease_id if disease_id.startswith("ORPHA:") else "",
                    }
                )

            result = list(memgraph.execute_and_fetch(MERGE_CYPHER, {"rows": rows}))
            matched_rows = int(result[0]["matchedCount"]) if result else 0
            ignored_rows = len(mysql_rows) - matched_rows

            batch_count += 1
            total_source_rows += len(mysql_rows)
            total_matched_rows += matched_rows
            total_ignored_rows += ignored_rows
            print(
                f"Batch {batch_count}: source rows={len(mysql_rows)}, "
                f"matched rows={matched_rows}, ignored rows={ignored_rows}, "
                f"total source rows={total_source_rows}, total matched rows={total_matched_rows}, "
                f"total ignored rows={total_ignored_rows}.",
                flush=True,
            )

        print(
            f"Completed graph load: source rows={total_source_rows}, "
            f"matched rows={total_matched_rows}, ignored rows={total_ignored_rows}.",
        )

    finally:
        cursor.close()

        if mysql.is_connected():
            mysql.close()


# conda run --no-capture-output -n rdas python Y_extension/hpo/20260925/graph/add_gene_to_disease_mapping.py
if __name__ == "__main__":
    main()

"""Create Gene-to-Phenotype relationships from the HPO MySQL mapping table."""

import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from baseclass.conn import DBConnection


BATCH_SIZE = 1_000

SELECT_SQL = """
    SELECT id, ncbi_gene_id, hpo_id
    FROM rdas_db.extension_hpo_genes_to_phenotype
"""

MERGE_CYPHER = """
    UNWIND $rows AS row
    MATCH (gene:Gene {geneIdentifier: row.geneIdentifier})
    MATCH (phenotype:Phenotype {hpoId: row.hpoId})
    MERGE (gene)-[:has_phenotype_association]->(phenotype)
    RETURN count(DISTINCT row.sourceRowId) AS matchedCount
"""


def main() -> None:

    connections = DBConnection()
    print("Connecting to MySQL...", flush=True)
    mysql = connections.mysql_conn()

    if mysql is None:
        raise ConnectionError("Unable to connect to MySQL.")
    
    print("Connected to MySQL. Connecting to Memgraph...", flush=True)

    memgraph = connections.memgraph_conn()

    if memgraph is None:
        mysql.close()
        raise ConnectionError("Unable to connect to Memgraph.")

    print("Connected to Memgraph.", flush=True)

    cursor = mysql.cursor(dictionary=True)
    total_source_rows = 0
    total_matched_rows = 0
    total_ignored_rows = 0
    batch_count = 0

    try:
        print("Executing the MySQL source query...", flush=True)

        cursor.execute(SELECT_SQL)
        print("MySQL source query is ready.", flush=True)

        while True:

            print(f"Fetching MySQL batch {batch_count + 1}...", flush=True)
            mysql_rows = cursor.fetchmany(BATCH_SIZE)

            if not mysql_rows:
                print("No more MySQL rows.", flush=True)
                break

            batch_count += 1
            rows = []
            print(f"Batch {batch_count}: fetched {len(mysql_rows)} source rows.", flush=True)

            """
            MySQL stores a bare NCBI gene identifier, for example:
                id=1, ncbi_gene_id="10", hpo_id="HP:0000007"

            HPO-created Gene nodes use the identifier format "NCBIGene:10".
            Add that prefix before matching the existing Gene node. Rows with a
            missing gene or phenotype identifier cannot form a relationship and
            are counted as ignored.
            """
            for row in mysql_rows:

                if not row["ncbi_gene_id"] or not row["hpo_id"]:
                    continue

                ncbi_gene_id = str(row["ncbi_gene_id"]).strip()
                gene_identifier = ncbi_gene_id if ncbi_gene_id.startswith("NCBIGene:") else f"NCBIGene:{ncbi_gene_id}"
                
                rows.append({
                        "sourceRowId": row["id"],
                        "geneIdentifier": gene_identifier,
                        "hpoId": str(row["hpo_id"]).strip(),
                    })

            print(
                f"Batch {batch_count}: submitting {len(rows)} rows for indexed Gene and Phenotype matching...",
                flush=True,
            )

            result = list(memgraph.execute_and_fetch(MERGE_CYPHER, {"rows": rows}))

            matched_rows = int(result[0]["matchedCount"]) if result else 0

            ignored_rows = len(mysql_rows) - matched_rows
            total_source_rows += len(mysql_rows)
            total_matched_rows += matched_rows
            total_ignored_rows += ignored_rows

            print(
                f"Batch {batch_count}: matched rows={matched_rows}, ignored rows={ignored_rows}, "
                f"total source rows={total_source_rows}, total matched rows={total_matched_rows}, "
                f"total ignored rows={total_ignored_rows}.",
                flush=True,
            )

        print(
            f"Completed Gene-to-Phenotype mapping: source rows={total_source_rows}, "
            f"matched rows={total_matched_rows}, ignored rows={total_ignored_rows}.",
        )

    finally:
        cursor.close()

        if mysql.is_connected():
            mysql.close()


# conda run --no-capture-output -n rdas python Y_extension/hpo/20260925/graph/step_3_2_genes_to_phenotype_mapping.py
if __name__ == "__main__":
    main()

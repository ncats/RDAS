"""Create Gene-to-Phenotype relationships from both HPO MySQL mapping tables."""

import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from baseclass.conn import DBConnection


BATCH_SIZE = 1_000

SOURCE_QUERIES = (
    (
        "extension_hpo_genes_to_phenotype",
        """
            SELECT id, ncbi_gene_id, hpo_id
            FROM rdas_db.extension_hpo_genes_to_phenotype
        """,
    ),
    (
        "extension_hpo_phenotype_to_genes",
        """
            SELECT id, ncbi_gene_id, hpo_id
            FROM rdas_db.extension_hpo_phenotype_to_genes
        """,
    ),
)

MERGE_CYPHER = """
    UNWIND $rows AS row
    MATCH (gene:Gene {geneIdentifier: row.geneIdentifier})
    MATCH (phenotype:Phenotype {hpoId: row.hpoId})
    MERGE (gene)-[:has_phenotype_association]->(phenotype)
    RETURN count(DISTINCT row.sourceRowId) AS matchedCount
"""


def process_source(cursor, memgraph, source_name: str, select_sql: str) -> tuple[int, int, int, int]:

    """
    Both source tables describe a Gene/Phenotype association, but their column
    order and direction are different:

        extension_hpo_genes_to_phenotype:
            id=1, ncbi_gene_id="10", hpo_id="HP:0000007"

        extension_hpo_phenotype_to_genes:
            id=1, hpo_id="HP:0009830", ncbi_gene_id="773"

    Selecting the three columns by name converts both table layouts into the
    same payload. disease_id is intentionally not selected or checked. A row
    can create the direct relationship whenever its Gene and Phenotype nodes
    exist, even if they are associated with different GARD nodes.

    The return values are batch count, source-row count, matched-row count,
    and ignored-row count for this table.
    """
    source_rows = 0
    matched_rows = 0
    ignored_rows = 0
    batch_count = 0

    print(f"[{source_name}] Executing the MySQL source query...", flush=True)

    cursor.execute(select_sql)
    print(f"[{source_name}] MySQL source query is ready.", flush=True)

    while True:

        print(f"[{source_name}] Fetching MySQL batch {batch_count + 1}...", flush=True)
        mysql_rows = cursor.fetchmany(BATCH_SIZE)

        if not mysql_rows:
            print(f"[{source_name}] No more MySQL rows.", flush=True)
            break

        batch_count += 1
        rows = []
        print(f"[{source_name}] Batch {batch_count}: fetched {len(mysql_rows)} source rows.", flush=True)

        """
        MySQL stores a bare NCBI gene identifier such as "10", while existing
        Gene nodes store geneIdentifier="NCBIGene:10". Add the prefix when it
        is absent. Phenotype nodes use the HPO identifier without additional
        normalization, for example hpoId="HP:0000007".

        A source-qualified row key such as
        "extension_hpo_genes_to_phenotype:1" prevents row id=1 in one table
        from being confused with row id=1 in the other table when matched rows
        are counted. Rows missing ncbi_gene_id or hpo_id are left out and
        counted as ignored. disease_id has no effect on this mapping.
        """
        for row in mysql_rows:

            if not row["ncbi_gene_id"] or not row["hpo_id"]:
                continue

            ncbi_gene_id = str(row["ncbi_gene_id"]).strip()
            gene_identifier = ncbi_gene_id if ncbi_gene_id.startswith("NCBIGene:") else f"NCBIGene:{ncbi_gene_id}"

            rows.append({
                "sourceRowId": f"{source_name}:{row['id']}",
                "geneIdentifier": gene_identifier,
                "hpoId": str(row["hpo_id"]).strip(),
            })

        """
        For ncbi_gene_id="10" and hpo_id="HP:0000007", the generated Cypher
        directly matches these two nodes:

            (gene:Gene {geneIdentifier: "NCBIGene:10"})
            (phenotype:Phenotype {hpoId: "HP:0000007"})

        MERGE then creates or reuses:

            (gene)-[:has_phenotype_association]->(phenotype)

        No GARD node or disease identifier participates in the match. MERGE
        also prevents the second source table, or a duplicate source row, from
        creating a duplicate relationship between the same two nodes.
        """
        print(
            f"[{source_name}] Batch {batch_count}: submitting {len(rows)} Gene/Phenotype mappings...",
            flush=True,
        )

        result = list(memgraph.execute_and_fetch(MERGE_CYPHER, {"rows": rows})) if rows else []
        batch_matched_rows = int(result[0]["matchedCount"]) if result else 0

        print(
            f"[{source_name}] Batch {batch_count}: mapping completed; matched rows={batch_matched_rows}.",
            flush=True,
        )

        batch_ignored_rows = len(mysql_rows) - batch_matched_rows
        source_rows += len(mysql_rows)
        matched_rows += batch_matched_rows
        ignored_rows += batch_ignored_rows

        print(
            f"[{source_name}] Batch {batch_count}: matched rows={batch_matched_rows}, "
            f"ignored rows={batch_ignored_rows}, total source rows={source_rows}, "
            f"total matched rows={matched_rows}, total ignored rows={ignored_rows}.",
            flush=True,
        )

    print(
        f"[{source_name}] Completed: batches={batch_count}, source rows={source_rows}, "
        f"matched rows={matched_rows}, ignored rows={ignored_rows}.",
        flush=True,
    )
    return batch_count, source_rows, matched_rows, ignored_rows


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
    total_batches = 0
    total_source_rows = 0
    total_matched_rows = 0
    total_ignored_rows = 0

    try:
        """
        Process the two large tables sequentially. Reusing one MySQL cursor and
        one Memgraph connection keeps the loader simple and avoids holding rows
        from both sources in memory at the same time.
        """
        for source_name, select_sql in SOURCE_QUERIES:

            batch_count, source_rows, matched_rows, ignored_rows = process_source(
                cursor, memgraph,
                source_name, select_sql,
            )

            total_batches += batch_count
            total_source_rows += source_rows
            total_matched_rows += matched_rows
            total_ignored_rows += ignored_rows

        print(
            f"Completed both Gene-to-Phenotype sources: batches={total_batches}, source rows={total_source_rows}, "
            f"matched rows={total_matched_rows}, ignored rows={total_ignored_rows}.",
            flush=True,
        )

    finally:
        cursor.close()

        if mysql.is_connected():
            mysql.close()


# conda run --no-capture-output -n rdas python Y_extension/hpo/20260925/graph/step_4_v2_add_genes_to_phenotype_mapping.py
if __name__ == "__main__":
    main()

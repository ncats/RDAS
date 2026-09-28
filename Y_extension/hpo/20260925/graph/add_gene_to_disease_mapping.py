"""Load HPO gene-to-disease associations from MySQL into Memgraph."""

import sys
from collections import defaultdict
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from baseclass.conn import DBConnection


BATCH_SIZE = 1_000

# SELECT DISTINCT LEFT(disease_id, 5) AS disease_id_prefix FROM rdas_db.extension_hpo_genes_to_disease;
GARD_PROPERTY_BY_PREFIX = {
    "MONDO": "mondo",
    "OMIM": "omim",
    "ORPHA": "orphanet",
}
SELECT_SQL = """
    SELECT id, ncbi_gene_id, gene_symbol, disease_id
    FROM rdas_db.extension_hpo_genes_to_disease
"""
MERGE_CYPHER = """
    UNWIND $rows AS row
    MATCH (gard:GARD {GARD_PROPERTY: row.diseaseId})
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
            rows_by_gard_property = defaultdict(list)
            print(f"Batch {batch_count}: fetched {len(mysql_rows)} source rows.", flush=True)

            """
            A source row looks like:
                id=1
                ncbi_gene_id="NCBIGene:64170"
                gene_symbol="CARD9"
                disease_id="OMIM:212050"

            Rows without both identifiers cannot produce either a Gene node or
            a GARD-to-Gene relationship, so they are ignored before Memgraph.
            """
            for row in mysql_rows:
                if not row["ncbi_gene_id"] or not row["disease_id"]:
                    continue

                """
                The identifier prefix selects the indexed GARD property used by
                the Cypher MATCH. Examples:
                    MONDO:0011308 -> GARD.mondo
                    OMIM:212050   -> GARD.omim
                    ORPHA:53693   -> GARD.orphanet

                An unknown prefix has no defined GARD property and is ignored.
                """
                disease_id = str(row["disease_id"]).strip()

                disease_prefix = disease_id.split(":", 1)[0]
                gard_property = GARD_PROPERTY_BY_PREFIX.get(disease_prefix)

                if not gard_property:
                    continue

                """
                For the OMIM example above, the graph-ready value is:
                    sourceRowId=1
                    ncbiGeneId="NCBIGene:64170"
                    geneSymbol="CARD9"
                    diseaseId="OMIM:212050"
                    mondo=""
                    omim="OMIM:212050"
                    orphanet=""

                Grouping rows by gard_property lets each Memgraph query use one
                property index instead of evaluating a slower multi-property OR.
                """
                rows_by_gard_property[gard_property].append(
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

            matched_rows = 0

            """
            Replace the Cypher placeholder with the trusted property name from
            GARD_PROPERTY_BY_PREFIX. For an OMIM group this produces:
                MATCH (gard:GARD {omim: row.diseaseId})

            execute_and_fetch returns a row such as {"matchedCount": 844}.
            A source row with no matching GARD node never reaches MERGE and is
            included in ignored_rows below.
            """
            for gard_property, rows in rows_by_gard_property.items():
                print(
                    f"Batch {batch_count}: submitting {len(rows)} rows using indexed GARD.{gard_property} matching...",
                    flush=True,
                )

                merge_cypher = MERGE_CYPHER.replace("GARD_PROPERTY", gard_property)

                result = list(memgraph.execute_and_fetch(merge_cypher, {"rows": rows}))
                
                group_matched_rows = int(result[0]["matchedCount"]) if result else 0
                matched_rows += group_matched_rows
                print(
                    f"Batch {batch_count}: GARD.{gard_property} matching completed; matched rows={group_matched_rows}.",
                    flush=True,
                )

            ignored_rows = len(mysql_rows) - matched_rows

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

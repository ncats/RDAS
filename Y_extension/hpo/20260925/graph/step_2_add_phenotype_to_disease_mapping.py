"""Load HPO disease-to-phenotype associations from MySQL into Memgraph."""

import sys
from collections import defaultdict
from pathlib import Path

# Create and populate table extension_hpo_distinct_phenotye before running this script.
"""
CREATE TABLE extension_hpo_distinct_phenotye (
    id INT AUTO_INCREMENT PRIMARY KEY,
    hpo_id VARCHAR(50) NOT NULL,     
    hpo_name VARCHAR(255),    
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_hpo_id (hpo_id)
);

INSERT INTO extension_hpo_distinct_phenotye (hpo_id, hpo_name)
SELECT hpo_id, hpo_name
FROM rdas_db.extension_hpo_genes_to_phenotype
GROUP BY hpo_id, hpo_name;
"""

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from baseclass.conn import DBConnection


BATCH_SIZE = 1_000

GARD_PROPERTY_BY_PREFIX = {
    "GARD": "gardId",
    "MONDO": "mondo",
    "OMIM": "omim",
    "ORPHA": "orphanet",
}

SELECT_SQL = """
    SELECT
        annotation.id AS source_row_id,
        annotation.database_id AS disease_id,
        phenotype.hpo_id,
        phenotype.hpo_name,
        annotation.evidence,
        annotation.reference,
        annotation.frequency
    FROM rdas_db.extension_hpo_phneotype_hpoa AS annotation
    LEFT JOIN rdas_db.extension_hpo_distinct_phenotye AS phenotype
        ON phenotype.hpo_id = annotation.hpo_id
"""

MERGE_CYPHER = """
    UNWIND $rows AS row
    MATCH (gard:GARD {GARD_PROPERTY: row.diseaseId})
    MERGE (phenotype:Phenotype {hpoId: row.hpoId})
    ON CREATE SET phenotype.countDiseases = 0
    SET phenotype.hpoTerm = row.hpoName
    MERGE (gard)-[relationship:has_phenotype]->(phenotype)
    SET
        relationship.evidence = row.evidence,
        relationship.references = row.references,
        relationship.hpoTermFrequency = row.frequency
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
            One joined MySQL row looks like:
                source_row_id=8001
                disease_id="OMIM:619340"
                hpo_id="HP:0011097"
                hpo_name="Epileptic spasm"
                evidence="PCS"
                reference="PMID:31675180"
                frequency="1/2"

            The LEFT JOIN keeps annotation rows whose HPO identifier is missing
            from extension_hpo_distinct_phenotye. Those rows have hpo_id=None
            here and are deliberately ignored before any Memgraph write.
            """
            for row in mysql_rows:
                if not row["disease_id"] or not row["hpo_id"]:
                    continue

                """
                Route each disease identifier to its matching indexed property:
                    GARD:0000001  -> GARD.gardId
                    MONDO:0011308 -> GARD.mondo
                    OMIM:619340   -> GARD.omim
                    ORPHA:53693   -> GARD.orphanet

                DECIPHER identifiers have no configured GARD property, so a
                value such as DECIPHER:58 is ignored.
                """
                disease_id = str(row["disease_id"]).strip()
                disease_prefix = disease_id.split(":", 1)[0]
                gard_property = GARD_PROPERTY_BY_PREFIX.get(disease_prefix)

                if not gard_property:
                    continue

                """
                References become a Memgraph list. For example:
                    "PMID:123;PMID:456" -> ["PMID:123", "PMID:456"]

                The example row above becomes this graph-ready payload:
                    sourceRowId=8001
                    diseaseId="OMIM:619340"
                    hpoId="HP:0011097"
                    hpoName="Epileptic spasm"
                    evidence="PCS"
                    references=["PMID:31675180"]
                    frequency="1/2"
                """
                references = [
                    reference.strip()
                    for reference in str(row["reference"] or "").split(";")
                    if reference.strip()
                ]
                rows_by_gard_property[gard_property].append(
                    {
                        "sourceRowId": row["source_row_id"],
                        "diseaseId": disease_id,
                        "hpoId": str(row["hpo_id"]).strip(),
                        "hpoName": str(row["hpo_name"] or "").strip(),
                        "evidence": str(row["evidence"] or "").strip(),
                        "references": references,
                        "frequency": str(row["frequency"] or "").strip(),
                    }
                )

            matched_rows = 0

            """
            Each group replaces the trusted GARD_PROPERTY placeholder with one
            indexed property. The OMIM example produces:
                MATCH (gard:GARD {omim: row.diseaseId})

            If no GARD node matches, the row stops at MATCH: no Phenotype node
            or has_phenotype relationship is created. A successful row merges:
                (:GARD)-[:has_phenotype]->
                (:Phenotype {hpoId: "HP:0011097", hpoTerm: "Epileptic spasm"})

            The relationship stores evidence, references, and term frequency.
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


# conda run --no-capture-output -n rdas python Y_extension/hpo/20260925/graph/step_2_add_phenotype_to_disease_mapping.py
if __name__ == "__main__":
    main()

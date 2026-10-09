"""Replace the RARe-SOURCE diseases table with the dated CSV export."""

import csv
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from baseclass.conn import DBConnection


DATA_FILE = SCRIPT_DIR.parent / "data" / "RARe-SOURCE-Browse-Rare-Diseases-09-29-2026.csv"
TABLE_NAME = "rdas_db.extension_rare_source_diseases"
BATCH_SIZE = 1_000

CSV_HEADERS = (
    "Rare Disease Name",
    "Disease Aliases",
    "Associated Genes",
    "Disease Annotations",
    "OMIM",
    "Orphanet",
    "UMLS",
    "Mesh",
    "ICD10CM",
    "Gene Descriptions",
    "Links",
)

COLUMNS = (
    "rare_disease_name",
    "disease_aliases",
    "associated_genes",
    "disease_annotations",
    "omim",
    "orphanet",
    "umls",
    "mesh",
    "icd10cm",
    "gene_descriptions",
    "links",
)

INSERT_SQL = f"""
    INSERT INTO {TABLE_NAME} ({", ".join(COLUMNS)})
    VALUES ({", ".join(["%s"] * len(COLUMNS))})
"""


def main() -> None:

    print(f"Connecting to MySQL to load {DATA_FILE.name}...", flush=True)
    connection = DBConnection().mysql_conn()

    if connection is None:
        raise ConnectionError("Unable to connect to MySQL.")

    cursor = connection.cursor()
    inserted_count = 0
    batch_count = 0

    try:
        with DATA_FILE.open("r", encoding="utf-8-sig", newline="") as csv_file:
            reader = csv.DictReader(csv_file)

            if tuple(reader.fieldnames or ()) != CSV_HEADERS:
                raise ValueError(f"Unexpected CSV columns: {reader.fieldnames}")

            print(f"Deleting existing rows from {TABLE_NAME}...", flush=True)
            cursor.execute(f"DELETE FROM {TABLE_NAME}")
            batch = []

            for row_number, row in enumerate(reader, start=2):
                rare_disease_name = row["Rare Disease Name"].strip()
                associated_genes = row["Associated Genes"].strip()
                disease_annotations = row["Disease Annotations"].strip()
                gene_descriptions = row["Gene Descriptions"].strip()
                links = row["Links"].strip()

                """
                All 7,200 source link values contain one extra literal quote at
                the end after correct CSV parsing, for example:
                    https://...q[]=Creatine transporter deficiency"
                Remove only that final export artifact so MySQL stores a valid
                URL. Quotes elsewhere in a field remain unchanged.
                """
                if links.endswith('"'):
                    links = links[:-1]

                if not all((rare_disease_name, associated_genes, disease_annotations, gene_descriptions, links)):
                    raise ValueError(f"Missing required value in CSV record {row_number}.")

                """
                Example mapping:
                    Rare Disease Name="Creatine transporter deficiency"
                    OMIM="300352"
                    Orphanet="52503"
                    UMLS="C1845862"

                Empty identifiers become SQL NULL. Multi-value aliases, genes,
                MeSH identifiers, and gene descriptions remain in their source
                strings so no information is discarded during this raw load.
                """
                batch.append((
                    rare_disease_name,
                    row["Disease Aliases"].strip() or None,
                    associated_genes,
                    disease_annotations,
                    row["OMIM"].strip() or None,
                    row["Orphanet"].strip() or None,
                    row["UMLS"].strip() or None,
                    row["Mesh"].strip() or None,
                    row["ICD10CM"].strip() or None,
                    gene_descriptions,
                    links,
                ))

                if len(batch) == BATCH_SIZE:
                    cursor.executemany(INSERT_SQL, batch)
                    inserted_count += len(batch)
                    batch_count += 1
                    batch.clear()
                    print(f"Batch {batch_count}: prepared {inserted_count} disease rows.", flush=True)

            if batch:
                cursor.executemany(INSERT_SQL, batch)
                inserted_count += len(batch)
                batch_count += 1
                print(f"Batch {batch_count}: prepared {inserted_count} disease rows.", flush=True)

        connection.commit()
        print(f"Inserted {inserted_count} rows into {TABLE_NAME}.", flush=True)

    except Exception:
        connection.rollback()
        print("Disease load failed; the transaction was rolled back.", flush=True)
        raise

    finally:
        cursor.close()

        if connection.is_connected():
            connection.close()


# conda run --no-capture-output -n rdas python Y_extension/RARe-SOURCE/20260929/msql/load_rare_source_diseases.py
if __name__ == "__main__":
    main()

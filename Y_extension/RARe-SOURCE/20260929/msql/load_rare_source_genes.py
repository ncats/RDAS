"""Replace the RARe-SOURCE genes table with the dated CSV export."""

import csv
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from baseclass.conn import DBConnection


DATA_FILE = SCRIPT_DIR.parent / "data" / "RARe-SOURCE-Browse-Genes-09-29-2026.csv"
TABLE_NAME = "rdas_db.extension_rare_source_genes"
BATCH_SIZE = 1_000

CSV_HEADERS = (
    "Gene Information",
    "Description",
    "Associated Rare Diseases",
    "Gene Annotations",
    "Gene IDs",
    "Ensembl Gene ID",
    "HGNC ID",
    "Links",
)

COLUMNS = (
    "gene_symbol",
    "description",
    "associated_rare_diseases",
    "gene_annotations",
    "gene_id",
    "ensembl_gene_id",
    "hgnc_id",
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

            """
            This dated file is a complete table snapshot. DELETE and all
            inserts use the same transaction, so rollback restores the old
            rows if CSV validation or a batch insert fails.
            """
            print(f"Deleting existing rows from {TABLE_NAME}...", flush=True)
            cursor.execute(f"DELETE FROM {TABLE_NAME}")
            batch = []

            for row_number, row in enumerate(reader, start=2):
                gene_symbol = row["Gene Information"].strip()
                description = row["Description"].strip()
                associated_rare_diseases = row["Associated Rare Diseases"].strip()
                gene_annotations = row["Gene Annotations"].strip()
                links = row["Links"].strip()

                if not all((gene_symbol, description, associated_rare_diseases, gene_annotations, links)):
                    raise ValueError(f"Missing required value in CSV row {row_number}.")

                """
                Example mapping:
                    Gene Information="SLC6A8" -> gene_symbol="SLC6A8"
                    Gene IDs="6535"            -> gene_id="6535"
                    HGNC ID="11055"            -> hgnc_id="11055"

                Empty external identifiers become SQL NULL. This preserves the
                valid MT-TC and SLC7A2-IT1 rows, whose identifier fields are
                blank in the source export.
                """
                batch.append((
                    gene_symbol,
                    description,
                    associated_rare_diseases,
                    gene_annotations,
                    row["Gene IDs"].strip() or None,
                    row["Ensembl Gene ID"].strip() or None,
                    row["HGNC ID"].strip() or None,
                    links,
                ))

                if len(batch) == BATCH_SIZE:
                    cursor.executemany(INSERT_SQL, batch)
                    inserted_count += len(batch)
                    batch_count += 1
                    batch.clear()
                    print(f"Batch {batch_count}: prepared {inserted_count} gene rows.", flush=True)

            if batch:
                cursor.executemany(INSERT_SQL, batch)
                inserted_count += len(batch)
                batch_count += 1
                print(f"Batch {batch_count}: prepared {inserted_count} gene rows.", flush=True)

        connection.commit()
        print(f"Inserted {inserted_count} rows into {TABLE_NAME}.", flush=True)

    except Exception:
        connection.rollback()
        print("Gene load failed; the transaction was rolled back.", flush=True)
        raise

    finally:
        cursor.close()

        if connection.is_connected():
            connection.close()


# conda run --no-capture-output -n rdas python Y_extension/RARe-SOURCE/20260929/msql/load_rare_source_genes.py
if __name__ == "__main__":
    main()

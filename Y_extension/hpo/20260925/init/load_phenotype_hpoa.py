"""Replace extension_pho_phneotype_hpoa with the dated HPO annotation data."""

import csv
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from baseclass.conn import DBConnection


DATA_FILE = SCRIPT_DIR.parent / "data" / "phenotype.hpoa.tsv"
TABLE_NAME = "extension_pho_phneotype_hpoa"
BATCH_SIZE = 1_000

COLUMNS = (
    "database_id",
    "disease_name",
    "qualifier",
    "hpo_id",
    "reference",
    "evidence",
    "onset",
    "frequency",
    "sex",
    "modifier",
    "aspect",
    "biocuration",
)

INSERT_SQL = f"""
    INSERT INTO {TABLE_NAME} ({", ".join(COLUMNS)})
    VALUES ({", ".join(["%s"] * len(COLUMNS))})
"""


def main() -> None:

    connection = DBConnection().mysql_conn()

    if connection is None:
        raise ConnectionError("Unable to connect to MySQL.")

    cursor = connection.cursor()
    inserted_count = 0
    batch_count = 0

    try:
        with DATA_FILE.open("r", encoding="utf-8-sig", newline="") as tsv_file:
            reader = csv.DictReader(tsv_file, delimiter="\t")

            if tuple(reader.fieldnames or ()) != COLUMNS:
                raise ValueError(f"Unexpected TSV columns: {reader.fieldnames}")

            # DELETE is transactional, so the old rows return if any insert fails.
            cursor.execute(f"DELETE FROM {TABLE_NAME}")
            batch = []

            for row in reader:
                batch.append(tuple(row[column] for column in COLUMNS))

                if len(batch) == BATCH_SIZE:
                    cursor.executemany(INSERT_SQL, batch)
                    inserted_count += len(batch)
                    batch_count += 1
                    batch.clear()
                    print(f"Batch {batch_count}: prepared {inserted_count} rows.", flush=True)

            if batch:
                cursor.executemany(INSERT_SQL, batch)
                inserted_count += len(batch)
                batch_count += 1
                print(f"Batch {batch_count}: prepared {inserted_count} rows.", flush=True)

        connection.commit()
        print(f"Inserted {inserted_count} rows into {TABLE_NAME}.")

    except Exception:
        connection.rollback()
        raise

    finally:
        cursor.close()

        if connection.is_connected():
            connection.close()


# conda run --no-capture-output -n rdas python Y_extension/hpo/20260925/init/load_phenotype_hpoa.py
if __name__ == "__main__":
    main()

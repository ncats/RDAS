"""
    Reads the tsv file and uploads them into the rdas database in the new table name. 
"""

import csv
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from baseclass.conn import DBConnection as db

# Table and file
TABLE_NAME = "rdas_db.gard_terms_deduped_sip"
TSV_PATH = "clean_disease_matched_terms.tsv"
BATCH_SIZE = 500

# Maps the TSV header names to the column names in the table, in case header
# text/casing varies slightly between files.
HEADER_MAP = {
    "gardid": "gard_id",
    "name": "name",
    "synonym": "synonym",
    "matched term": "matched_term",
}


# Helper method to create the rdas table with columns if it doesn't already exist.
def create_table(conn):
    cursor = conn.cursor()

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            gard_id VARCHAR(50),
            name VARCHAR(500),
            synonym VARCHAR(500),
            matched_term VARCHAR(255),
            INDEX idx_gard_id (gard_id),
            INDEX idx_matched_term (matched_term)
        )
    """
    cursor.execute(create_sql)
    conn.commit()
    cursor.close()
    print(f"Table `{TABLE_NAME}` ready.\n")



# Helper method to read rows from the TSV file, yielding (gard_id, name, synonym, matched_term)
# tuples. Handles ragged/blank lines and normalizes the header names.
def read_rows(tsv_path: str):
    # Open the tsv file to read
    with open(tsv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter="\t")
        header = next(reader)

        # Normalize the headers 
        normalized_header = [h.strip().lower() for h in header]
        try:
            # Map each table column name to the index of the column in the tsv file header.
            col_indexes = {
                HEADER_MAP[h]: i
                for i, h in enumerate(normalized_header)
                if h in HEADER_MAP
            }
        except KeyError as exc:
            raise ValueError(f"Unrecognized column in header {header}: {exc}") from exc

        # Check that every column the table needs has been found from the tsv.
        missing = set(HEADER_MAP.values()) - set(col_indexes.keys())
        if missing:
            raise ValueError(f"TSV file is missing expected columns: {missing}")

        # Go through each row of the tsv file.
        for row in reader:
            if not row or all(not cell.strip() for cell in row):
                continue  # skip blank lines
            # Fills empty cells with empty strings.
            if len(row) < len(header):
                row = row + [""] * (len(header) - len(row))
            # Returns gard_id, name, synonym, matched_term contents of the row.
            yield (
                row[col_indexes["gard_id"]].strip(),
                row[col_indexes["name"]].strip(),
                row[col_indexes["synonym"]].strip(),
                row[col_indexes["matched_term"]].strip(),
            )


# Execute the process by reading the TSV in batches and inserting each batch into the MySQL table.
def insert_terms():
    # Create a connection to MySQL
    conn = db().mysql_conn()

    # If connection can't form, raise an error
    if conn is None:
        raise ConnectionError("Unable to connect to MySQL.")

    write_cursor = conn.cursor()

    # Create new table using helper method.
    create_table(conn)

    # Insert sql query created.
    insert_sql = f"""
        INSERT INTO `{TABLE_NAME}` (gard_id, name, synonym, matched_term)
        VALUES (%s, %s, %s, %s)
    """

    # Counter to keep track of batch number, total rows inserted, and list to hold the batch contents
    batch_num = 0
    total_inserted = 0
    batch = []

    try:
        # Go through each row of the tsv
        for row in read_rows(TSV_PATH):
            # Add the row to the batch
            batch.append(row)

            # Once batch size is reached, push to rdas database table
            if len(batch) >= BATCH_SIZE:
                batch_num += 1
                print(f"Batch {batch_num}...", end="\n", flush=True)

                write_cursor.executemany(insert_sql, batch)
                conn.commit()
                total_inserted += len(batch)
                print(f"Inserted {len(batch)} rows. Total: {total_inserted}\n")
                batch = []

        # Insert any leftover rows that didn't fill a full batch
        if batch:
            batch_num += 1
            print(f"Batch {batch_num}...", end="\n", flush=True)

            write_cursor.executemany(insert_sql, batch)
            conn.commit()
            total_inserted += len(batch)
            print(f"Inserted {len(batch)} rows. Total: {total_inserted}\n")

    except Exception as exc:
        print(f"Could not finish executing the process: {exc}\n")

    finally:
        print(f"Total rows inserted: {total_inserted}\n")
        if write_cursor is not None:
            write_cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()


def main():
    insert_terms()


if __name__ == "__main__":
    raise SystemExit(main())
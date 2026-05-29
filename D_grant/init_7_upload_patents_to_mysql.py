"""
Upload manually downloaded NIH RePORTER patent CSV files into MySQL.

Expected input files:
    D_grant/data/patents/*.csv

Notes:
    - This loader appends rows. If you are doing a full reload, truncate the
      table manually before running:
          TRUNCATE TABLE grant_patent;
      This deletes all rows and resets AUTO_INCREMENT to 1.
    - For table grant_patent, the step = 1.
    - If convert_csv_files_to_utf8(dir) doesn't work, manually save as:
      CSV UTF-8 (Comma delimited)(.csv).
    - Optional inspection before upload:
      check_column_max_length(
          dir,
          ["PATENT_ID", "PATENT_TITLE", "PROJECT_ID", "PATENT_ORG_NAME"],
      )
"""

import csv
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

# Add the project root to the Python path when this file is run directly.
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

TABLE_NAME = "grant_patent"
DEFAULT_PATENTS_DIR = SCRIPT_DIR / "data" / "patents"
DEFAULT_BATCH_SIZE = 1000

# Keep the INSERT column order explicit. PATENT_ID looks numeric in the source
# file, but the MySQL schema stores it as varchar and the Memgraph initializer
# treats it as an identifier, so this loader intentionally keeps it as text.
PATENT_FIELDS: Tuple[Tuple[str, str], ...] = (
    ("PATENT_ID", "PATENT_ID"),
    ("PROJECT_ID", "PROJECT_ID"),
    ("PATENT_TITLE", "PATENT_TITLE"),
    ("PATENT_ORG_NAME", "PATENT_ORG_NAME"),
)


def upload_patents(dir_path: os.PathLike = DEFAULT_PATENTS_DIR, batch_size: int = DEFAULT_BATCH_SIZE) -> Dict[str, int]:
    """Upload all patent CSV files from dir_path into grant_patent."""

    from baseclass.conn import DBConnection as db
    from utils.tools import _normalize_tuple, detect_file_encoding

    patents_dir = Path(dir_path).expanduser().resolve()

    if batch_size <= 0:
        raise ValueError("batch_size must be greater than 0")

    # Patent titles can be long. Raise the csv module field size limit once so a
    # large title cell does not fail parsing before MySQL receives the row.
    max_csv_field_size = sys.maxsize

    while True:
        try:
            csv.field_size_limit(max_csv_field_size)
            break

        except OverflowError:
            max_csv_field_size = int(max_csv_field_size / 10)

    if not patents_dir.is_dir():
        raise FileNotFoundError(f"Patent CSV directory does not exist: {patents_dir}")

    # Get all CSV files (case-insensitive), in deterministic order.
    csv_files = sorted(patents_dir.glob("*.[Cc][Ss][Vv]"))

    if not csv_files:
        print(f"No patent CSV files found in: {patents_dir}")
        return {"files_processed": 0, "rows_inserted": 0, "failed_files": 0}

    db_columns = [f"`{db_column}`" for _, db_column in PATENT_FIELDS]
    insert_sql = f"""
        INSERT INTO `{TABLE_NAME}` ({", ".join(db_columns)})
        VALUES ({", ".join(["%s"] * len(PATENT_FIELDS))})
    """

    conn = db().mysql_conn()

    if conn is None:
        raise ConnectionError("Unable to create MySQL connection.")

    cursor = None
    summary = {"files_processed": 0, "rows_inserted": 0, "failed_files": 0}

    try:
        cursor = conn.cursor()

        for csv_file in csv_files:
            try:
                row_batch: List[Tuple[Any, ...]] = []
                inserted_count = 0

                print(f"\n{csv_file.name}")

                # Patents.csv commonly includes a UTF-8 byte-order mark on the
                # first header. Reading UTF-8 as utf-8-sig strips that marker so
                # DictReader sees "PATENT_ID" exactly.
                detected_encoding, confidence = detect_file_encoding(csv_file)
                encoding = detected_encoding or "utf-8-sig"
                normalized_encoding = encoding.strip().lower().replace("_", "-")

                if normalized_encoding in {"utf-8", "utf8", "utf-8-sig"}:
                    encoding = "utf-8-sig"

                print(
                    f"Detected encoding: {detected_encoding} "
                    f"(confidence: {confidence:.2%}); reading as {encoding}"
                )

                with csv_file.open("r", newline="", encoding=encoding, errors="replace") as file_obj:
                    reader = csv.DictReader(file_obj)

                    # Validate required headers once before processing rows. A
                    # missing PATENT_ID or PROJECT_ID should fail the file
                    # immediately instead of silently inserting NULL values.
                    if not reader.fieldnames:
                        raise ValueError(f"{csv_file.name} does not contain a CSV header row.")

                    csv_columns = set(reader.fieldnames)
                    missing_columns = [
                        csv_column
                        for csv_column, _ in PATENT_FIELDS
                        if csv_column not in csv_columns
                    ]

                    if missing_columns:
                        raise ValueError(
                            f"{csv_file.name} is missing required column(s): "
                            f"{', '.join(missing_columns)}"
                        )

                    for row in reader:
                        values = []

                        for csv_column, _ in PATENT_FIELDS:
                            value = row.get(csv_column)

                            if isinstance(value, str):
                                value = value.strip()

                            if value == "":
                                value = None

                            values.append(value)

                        # remove unwanted characters
                        row_batch.append(_normalize_tuple(tuple(values)))

                        if len(row_batch) >= batch_size:
                            # Save rows of a csv file into mysql. Commit each
                            # batch so memory stays bounded and the transaction
                            # does not grow across the full patent file.
                            try:
                                cursor.executemany(insert_sql, row_batch)
                                conn.commit()

                            except Exception:
                                conn.rollback()
                                raise

                            inserted_count += len(row_batch)
                            row_batch.clear()

                            if inserted_count % 10000 == 0:
                                print(f"{inserted_count} rows inserted...", flush=True)
                            else:
                                print(".", end=" ", flush=True)

                # Upload the leftover
                if row_batch:
                    try:
                        cursor.executemany(insert_sql, row_batch)
                        conn.commit()

                    except Exception:
                        conn.rollback()
                        raise

                    inserted_count += len(row_batch)
                    row_batch.clear()

                print(f"\n{csv_file.name}: inserted {inserted_count} row(s)\n")

                summary["files_processed"] += 1
                summary["rows_inserted"] += inserted_count

            except Exception as exc:
                summary["failed_files"] += 1
                print(f"\nFailed to upload {csv_file.name}: {exc}")
                raise

    finally:
        if cursor is not None:
            cursor.close()

        if conn is not None and conn.is_connected():
            conn.close()

    return summary


def main() -> int:
    """Run the patent CSV upload."""

    from utils.tools import ask_to_continue

    if not ask_to_continue("*** Upload the grant Patents into MySQL database? *** "):
        print("\n------------------------ Stopped ------------------------\n")
        return 1

    summary = upload_patents()

    print(
        "\nUpload complete: "
        f"files_processed={summary['files_processed']}, "
        f"rows_inserted={summary['rows_inserted']}, "
        f"failed_files={summary['failed_files']}\n"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

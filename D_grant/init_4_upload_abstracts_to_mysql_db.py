"""
Upload NIH RePORTER abstract CSV files into the MySQL grant_abstract table.

Expected input files:
    D_grant/data/abstracts/*.csv

Notes:
    - This loader appends rows. If you are doing a full reload, truncate the
      table manually before running:
          TRUNCATE TABLE grant_abstract;
      This deletes all rows and resets AUTO_INCREMENT to 1.
    - For table grant_abstract, the step = 1.
    - If convert_csv_files_to_utf8(dir) doesn't work, manually save as:
      CSV UTF-8 (Comma delimited)(.csv).
    - Optional inspection before upload:
      check_column_max_length(dir, ['APPLICATION_ID','ABSTRACT_TEXT'])
"""

import csv
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

# Add the project root to the Python path when this file is run directly.
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

TABLE_NAME = "grant_abstract"
DEFAULT_ABSTRACTS_DIR = SCRIPT_DIR / "data" / "abstracts"
DEFAULT_BATCH_SIZE = 1000
MIN_REPORTER_ABSTRACT_YEAR = 1985
MAX_REPORTER_ABSTRACT_YEAR = 2025
ABSTRACT_COLUMNS = ("YEAR", "APPLICATION_ID", "ABSTRACT_TEXT")
REQUIRED_CSV_COLUMNS = ("APPLICATION_ID", "ABSTRACT_TEXT")


def get_year(filename: str) -> int:
    """Extract and validate the fiscal year from a RePORTER abstract filename."""

    pattern = r"[A-Za-z0-9_]*?(\d{4})(?=_new\.csv|\.csv)"
    match = re.match(pattern, filename)

    if not match:
        raise ValueError(f"Filename '{filename}' does not match the expected pattern {pattern}")

    year = int(match.group(1))  # Returns the year (4 digits)

    if year > MAX_REPORTER_ABSTRACT_YEAR or year < MIN_REPORTER_ABSTRACT_YEAR:
        raise ValueError(
            f"The Year cannot less than {MIN_REPORTER_ABSTRACT_YEAR} "
            f"or greater than {MAX_REPORTER_ABSTRACT_YEAR}"
        )

    return year


def upload_abstracts(dir_path: os.PathLike = DEFAULT_ABSTRACTS_DIR, batch_size: int = DEFAULT_BATCH_SIZE) -> Dict[str, int]:
    """Upload all abstract CSV files from dir_path into grant_abstract."""

    from baseclass.conn import DBConnection as db
    from utils.tools import _normalize_tuple, convert_to_int, detect_file_encoding

    abstracts_dir = Path(dir_path).expanduser().resolve()

    if batch_size <= 0:
        raise ValueError("batch_size must be greater than 0")

    # ABSTRACT_TEXT is stored as mediumtext and can be large. Raise the csv
    # module field size limit before streaming rows so large abstracts do not
    # fail parsing.
    max_csv_field_size = sys.maxsize

    while True:
        try:
            csv.field_size_limit(max_csv_field_size)
            break

        except OverflowError:
            max_csv_field_size = int(max_csv_field_size / 10)

    if not abstracts_dir.is_dir():
        raise FileNotFoundError(f"Abstract CSV directory does not exist: {abstracts_dir}")

    # Get all CSV files (case-insensitive), in deterministic order.
    csv_files = sorted(abstracts_dir.glob("*.[Cc][Ss][Vv]"))

    if not csv_files:
        print(f"No abstract CSV files found in: {abstracts_dir}")
        return {"files_processed": 0, "rows_inserted": 0, "failed_files": 0}

    insert_sql = f"""
        INSERT INTO `{TABLE_NAME}` ({", ".join(f"`{column}`" for column in ABSTRACT_COLUMNS)})
        VALUES ({", ".join(["%s"] * len(ABSTRACT_COLUMNS))})
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
                year = get_year(csv_file.name)
                row_batch: List[Tuple[Any, ...]] = []
                inserted_count = 0

                print(f"\n[Year: {year}]: {csv_file.name}")

                detected_encoding, confidence = detect_file_encoding(csv_file)
                encoding = detected_encoding or "utf-8-sig"
                normalized_encoding = encoding.strip().lower().replace("_", "-")

                # utf-8-sig reads normal UTF-8 and also removes a UTF-8 BOM from
                # the first CSV header when one is present.
                if normalized_encoding in {"utf-8", "utf8", "utf-8-sig"}:
                    encoding = "utf-8-sig"

                print(
                    f"Detected encoding: {detected_encoding} "
                    f"(confidence: {confidence:.2%}); reading as {encoding}"
                )

                with csv_file.open("r", newline="", encoding=encoding, errors="replace") as file_obj:
                    reader = csv.DictReader(file_obj)

                    # Validate required headers once. A missing APPLICATION_ID
                    # or ABSTRACT_TEXT should fail the file immediately instead
                    # of silently inserting NULL values.
                    if not reader.fieldnames:
                        raise ValueError(f"{csv_file.name} does not contain a CSV header row.")

                    csv_columns = set(reader.fieldnames)
                    missing_columns = [
                        column
                        for column in REQUIRED_CSV_COLUMNS
                        if column not in csv_columns
                    ]

                    if missing_columns:
                        raise ValueError(
                            f"{csv_file.name} is missing required column(s): "
                            f"{', '.join(missing_columns)}"
                        )

                    for row_number, row in enumerate(reader, start=2):
                        try:
                            application_id = convert_to_int(row.get("APPLICATION_ID"))

                        except ValueError as exc:
                            raise ValueError(
                                f"{csv_file.name} row {row_number}, column APPLICATION_ID: {exc}"
                            ) from exc

                        abstract_text = row.get("ABSTRACT_TEXT")

                        if isinstance(abstract_text, str):
                            abstract_text = abstract_text.strip()

                        if abstract_text == "":
                            abstract_text = None

                        data_tuple = (
                            year,
                            application_id,  # APPLICATION_ID
                            abstract_text,  # ABSTRACT_TEXT
                        )

                        # remove unwanted characters
                        row_batch.append(_normalize_tuple(data_tuple))

                        if len(row_batch) >= batch_size:
                            # Save rows of a csv file into mysql. Commit each
                            # batch so memory stays bounded and the transaction
                            # does not grow across an entire source file.
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
    """Run the abstract CSV upload."""

    from utils.tools import ask_to_continue

    if not ask_to_continue("*** Upload the grant Abstracts into MySQL database? *** "):
        print("\n------------------------ Stopped ------------------------\n")
        return 1

    summary = upload_abstracts()

    print(
        "\nUpload complete: "
        f"files_processed={summary['files_processed']}, "
        f"rows_inserted={summary['rows_inserted']}, "
        f"failed_files={summary['failed_files']}\n"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

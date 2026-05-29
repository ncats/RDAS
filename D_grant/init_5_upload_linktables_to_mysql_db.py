"""
Upload NIH RePORTER publication-project link CSV files into grant_linktable.

Expected input files:
    D_grant/data/linktables/*.csv

Notes:
    - This loader appends rows. If you are doing a full reload, truncate the
      table manually before running:
          TRUNCATE TABLE grant_linktable;
      This deletes all rows and resets AUTO_INCREMENT to 1.
    - For table grant_linktable, the step = 1.
    - If convert_csv_files_to_utf8(dir) doesn't work, manually save as:
      CSV UTF-8 (Comma delimited)(.csv).
    - Optional inspection before upload:
      check_column_max_length(dir, ['PMID','PROJECT_NUMBER'])
      The original inspection comment was kept, but the columns are adjusted
      here for this linktable file format.
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

TABLE_NAME = "grant_linktable"
DEFAULT_LINKTABLES_DIR = SCRIPT_DIR / "data" / "linktables"
DEFAULT_BATCH_SIZE = 1000
MIN_REPORTER_LINKTABLE_YEAR = 1980
MAX_REPORTER_LINKTABLE_YEAR = 2025
LINKTABLE_COLUMNS = ("YEAR", "PMID", "PROJECT_NUMBER")
REQUIRED_CSV_COLUMNS = ("PMID", "PROJECT_NUMBER")


def get_year(filename: str) -> int:
    """Extract and validate the fiscal year from a RePORTER linktable filename."""

    pattern = r"[A-Za-z0-9_]*?(?:FY)?(\d{4})\.[Cc][Ss][Vv]$"
    match = re.match(pattern, filename)

    if not match:
        raise ValueError(f"Filename '{filename}' does not match the expected pattern {pattern}")

    year = int(match.group(1))  # Returns the year (4 digits)

    if year > MAX_REPORTER_LINKTABLE_YEAR or year < MIN_REPORTER_LINKTABLE_YEAR:
        raise ValueError(
            f"The Year cannot less than {MIN_REPORTER_LINKTABLE_YEAR} "
            f"or greater than {MAX_REPORTER_LINKTABLE_YEAR}"
        )

    return year


def upload_linktables(dir_path: os.PathLike = DEFAULT_LINKTABLES_DIR, batch_size: int = DEFAULT_BATCH_SIZE) -> Dict[str, int]:
    """Upload all linktable CSV files from dir_path into grant_linktable."""

    from baseclass.conn import DBConnection as db
    from utils.tools import _normalize_tuple, convert_to_int, detect_file_encoding

    linktables_dir = Path(dir_path).expanduser().resolve()

    if batch_size <= 0:
        raise ValueError("batch_size must be greater than 0")

    # Linktable files are simple two-column CSV files, but some yearly files are
    # large. Raise the csv module field size limit once so unusual cells do not
    # fail parsing before MySQL receives the row.
    max_csv_field_size = sys.maxsize

    while True:
        try:
            csv.field_size_limit(max_csv_field_size)
            break

        except OverflowError:
            max_csv_field_size = int(max_csv_field_size / 10)

    if not linktables_dir.is_dir():
        raise FileNotFoundError(f"Linktable CSV directory does not exist: {linktables_dir}")

    # Get all CSV files (case-insensitive), in deterministic order.
    csv_files = sorted(linktables_dir.glob("*.[Cc][Ss][Vv]"))

    if not csv_files:
        print(f"No linktable CSV files found in: {linktables_dir}")
        return {"files_processed": 0, "rows_inserted": 0, "failed_files": 0}

    insert_sql = f"""
        INSERT INTO `{TABLE_NAME}` ({", ".join(f"`{column}`" for column in LINKTABLE_COLUMNS)})
        VALUES ({", ".join(["%s"] * len(LINKTABLE_COLUMNS))})
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

                # RePORTER exports are not guaranteed to use one encoding for
                # every historical file. Detect the likely encoding and use
                # utf-8-sig for UTF-8 files so a byte-order mark does not become
                # part of the first header name.
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

                    # Validate required headers once. A missing PMID or
                    # PROJECT_NUMBER should fail the file immediately instead
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
                            pmid = convert_to_int(row.get("PMID"))

                        except ValueError as exc:
                            raise ValueError(
                                f"{csv_file.name} row {row_number}, column PMID: {exc}"
                            ) from exc

                        project_number = row.get("PROJECT_NUMBER")

                        if isinstance(project_number, str):
                            project_number = project_number.strip()

                        if project_number == "":
                            project_number = None

                        data_tuple = (
                            year,
                            pmid,  # PMID
                            project_number,  # PROJECT_NUMBER
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
    """Run the linktable CSV upload."""

    from utils.tools import ask_to_continue

    if not ask_to_continue("*** Upload the grant Linktables into MySQL database? *** "):
        print("\n------------------------ Stopped ------------------------\n")
        return 1

    summary = upload_linktables()

    print(
        "\nUpload complete: "
        f"files_processed={summary['files_processed']}, "
        f"rows_inserted={summary['rows_inserted']}, "
        f"failed_files={summary['failed_files']}\n"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

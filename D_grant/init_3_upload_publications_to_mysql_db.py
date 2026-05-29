"""
Upload NIH RePORTER publication CSV files into the MySQL grant_publication table.

Expected input files:
    D_grant/data/publications/*.csv

Notes:
    - This loader appends rows. If you are doing a full reload, truncate the
      table manually before running:
          TRUNCATE TABLE grant_publication;
      This deletes all rows and resets AUTO_INCREMENT to 1.
    - For table grant_publication, the step = 1.
    - If convert_csv_files_to_utf8(dir) doesn't work, manually save as:
      CSV UTF-8 (Comma delimited)(.csv).
    - Optional inspection before upload:
      check_column_max_length(dir, [
          'AFFILIATION','AUTHOR_LIST','JOURNAL_TITLE','JOURNAL_ISSUE',
          'JOURNAL_TITLE_ABBR','PUB_TITLE','PAGE_NUMBER','PUB_DATE',
          'JOURNAL_VOLUME'
      ])
"""

import csv
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Add the project root to the Python path when this file is run directly.
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

TABLE_NAME = "grant_publication"
DEFAULT_PUBLICATIONS_DIR = SCRIPT_DIR / "data" / "publications"
DEFAULT_BATCH_SIZE = 1000

# Keep the INSERT column order explicit. The table contains additional pipeline
# columns such as related_to_gard and processed; this loader only fills the
# source publication fields listed here.
PUBLICATION_FIELDS: Tuple[Tuple[str, str, Optional[str]], ...] = (
    ("AFFILIATION", "AFFILIATION", None),
    ("AUTHOR_LIST", "AUTHOR_LIST", None),
    ("COUNTRY", "COUNTRY", None),
    ("ISSN", "ISSN", None),
    ("JOURNAL_ISSUE", "JOURNAL_ISSUE", None),
    ("JOURNAL_TITLE", "JOURNAL_TITLE", None),
    ("JOURNAL_TITLE_ABBR", "JOURNAL_TITLE_ABBR", None),
    ("JOURNAL_VOLUME", "JOURNAL_VOLUME", None),
    ("LANG", "LANG", None),
    ("PAGE_NUMBER", "PAGE_NUMBER", None),
    ("PMC_ID", "PMC_ID", "pmc_id"),
    ("PMID", "PMID", "int"),
    ("PUB_DATE", "PUB_DATE", None),
    ("PUB_TITLE", "PUB_TITLE", None),
    ("PUB_YEAR", "PUB_YEAR", "int"),
)


def upload_publications(dir_path: os.PathLike = DEFAULT_PUBLICATIONS_DIR, batch_size: int = DEFAULT_BATCH_SIZE) -> Dict[str, int]:
    """Upload all publication CSV files from dir_path into grant_publication."""

    from baseclass.conn import DBConnection as db
    from utils.tools import _normalize_tuple, convert_to_int, detect_file_encoding

    publications_dir = Path(dir_path).expanduser().resolve()

    if batch_size <= 0:
        raise ValueError("batch_size must be greater than 0")

    # Publication titles, affiliations, and author lists can be long. Raise the
    # csv module field size limit before streaming files so large text cells do
    # not fail parsing.
    max_csv_field_size = sys.maxsize

    while True:
        try:
            csv.field_size_limit(max_csv_field_size)
            break

        except OverflowError:
            max_csv_field_size = int(max_csv_field_size / 10)

    if not publications_dir.is_dir():
        raise FileNotFoundError(f"Publication CSV directory does not exist: {publications_dir}")

    # Get all CSV files (case-insensitive), in deterministic order.
    csv_files = sorted(publications_dir.glob("*.[Cc][Ss][Vv]"))

    if not csv_files:
        print(f"No publication CSV files found in: {publications_dir}")
        return {"files_processed": 0, "rows_inserted": 0, "failed_files": 0}

    db_columns = [f"`{db_column}`" for _, db_column, _ in PUBLICATION_FIELDS]
    placeholders = ["%s"] * len(PUBLICATION_FIELDS)
    insert_sql = f"""
        INSERT INTO `{TABLE_NAME}` ({", ".join(db_columns)})
        VALUES ({", ".join(placeholders)})
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

                detected_encoding, confidence = detect_file_encoding(csv_file)
                encoding = detected_encoding or "utf-8-sig"
                normalized_encoding = encoding.strip().lower().replace("_", "-")

                # utf-8-sig reads normal UTF-8 and also removes a UTF-8 BOM from
                # the first CSV header when one is present.
                if normalized_encoding in {"utf-8", "utf8", "utf-8-sig"}:
                    encoding = "utf-8-sig"

                print(f"\n{csv_file.name}")
                print(
                    f"Detected encoding: {detected_encoding} "
                    f"(confidence: {confidence:.2%}); reading as {encoding}"
                )

                with csv_file.open("r", newline="", encoding=encoding, errors="replace") as file_obj:
                    reader = csv.DictReader(file_obj)

                    # Validate all required headers once before processing rows.
                    # Missing columns should fail the file immediately instead
                    # of silently inserting NULL into important columns.
                    if not reader.fieldnames:
                        raise ValueError(f"{csv_file.name} does not contain a CSV header row.")

                    csv_columns = set(reader.fieldnames)
                    missing_columns = [
                        csv_column
                        for csv_column, _, _ in PUBLICATION_FIELDS
                        if csv_column not in csv_columns
                    ]

                    if missing_columns:
                        raise ValueError(
                            f"{csv_file.name} is missing required column(s): "
                            f"{', '.join(missing_columns)}"
                        )

                    for row_number, row in enumerate(reader, start=2):
                        values = []

                        for csv_column, _, converter in PUBLICATION_FIELDS:
                            value = row.get(csv_column)

                            try:
                                if value is None:
                                    converted_value = None

                                else:
                                    if isinstance(value, str):
                                        value = value.strip()

                                    if value == "":
                                        converted_value = None
                                    elif converter == "int":
                                        converted_value = convert_to_int(value)
                                    elif converter == "pmc_id":
                                        # Original note preserved:
                                        # ValueError: invalid literal for int()
                                        # with base 10: '6587139.2'
                                        #
                                        # PMC_ID occasionally appears as a
                                        # decimal-looking value. Convert PMC_ID
                                        # to integer and truncate decimal if
                                        # present, matching the original script.
                                        converted_value = convert_to_int(value, allow_decimal=True)
                                    else:
                                        converted_value = value

                            except ValueError as exc:
                                raise ValueError(
                                    f"{csv_file.name} row {row_number}, "
                                    f"column {csv_column}: {exc}"
                                ) from exc

                            values.append(converted_value)

                        # remove unwanted characters
                        row_batch.append(_normalize_tuple(tuple(values)))

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
    """Run the publication CSV upload."""

    from utils.tools import ask_to_continue

    if not ask_to_continue("*** Upload the grant Publications into MySQL database? *** "):
        print("\n------------------------ Stopped ------------------------\n")
        return 1

    summary = upload_publications()

    print(
        "\nUpload complete: "
        f"files_processed={summary['files_processed']}, "
        f"rows_inserted={summary['rows_inserted']}, "
        f"failed_files={summary['failed_files']}\n"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

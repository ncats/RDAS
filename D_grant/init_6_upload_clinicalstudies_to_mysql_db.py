"""
Upload manually downloaded NIH RePORTER clinical study CSV files into MySQL.

Expected input files:
    D_grant/data/clinical_studies/*.csv

Notes:
    - This loader appends rows. If you are doing a full reload, truncate the
      table manually before running:
          TRUNCATE TABLE grant_clinical_study;
      This deletes all rows and resets AUTO_INCREMENT to 1.
    - For table grant_clinical_study, the step = 1.
    - If convert_csv_files_to_utf8(dir) doesn't work, manually save as:
      CSV UTF-8 (Comma delimited)(.csv).
    - Optional inspection before upload:
      check_column_max_length(
          dir,
          ["Core Project Number", "ClinicalTrials.gov ID", "Study", "Study Status"],
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

TABLE_NAME = "grant_clinical_study"
DEFAULT_CLINICAL_STUDIES_DIR = SCRIPT_DIR / "data" / "clinical_studies"
DEFAULT_BATCH_SIZE = 1000

# Keep the INSERT column order explicit and next to the CSV-to-DB mapping. This
# makes it much harder for the CSV field order and MySQL field order to drift
# apart when the loader is updated later.
CLINICAL_STUDY_FIELDS: Tuple[Tuple[str, str], ...] = (
    ("Core Project Number", "core_project_num"),
    ("ClinicalTrials.gov ID", "nctid"),
    ("Study", "study"),
    ("Study Status", "study_status"),
)


def upload_clinical_studies(dir_path: os.PathLike = DEFAULT_CLINICAL_STUDIES_DIR, batch_size: int = DEFAULT_BATCH_SIZE) -> Dict[str, int]:
    """Upload all clinical study CSV files from dir_path into grant_clinical_study."""

    from baseclass.conn import DBConnection as db
    from utils.tools import _normalize_tuple, detect_file_encoding

    clinical_studies_dir = Path(dir_path).expanduser().resolve()

    if batch_size <= 0:
        raise ValueError("batch_size must be greater than 0")

    # Study titles are normally short, but the csv module field limit is process
    # global and cheap to raise. Doing this once keeps this loader consistent
    # with the larger grant import scripts.
    max_csv_field_size = sys.maxsize

    while True:
        try:
            csv.field_size_limit(max_csv_field_size)
            break

        except OverflowError:
            max_csv_field_size = int(max_csv_field_size / 10)

    if not clinical_studies_dir.is_dir():
        raise FileNotFoundError(
            f"Clinical study CSV directory does not exist: {clinical_studies_dir}"
        )

    # Get all CSV files (case-insensitive), in deterministic order.
    csv_files = sorted(clinical_studies_dir.glob("*.[Cc][Ss][Vv]"))

    if not csv_files:
        print(f"No clinical study CSV files found in: {clinical_studies_dir}")
        return {"files_processed": 0, "rows_inserted": 0, "failed_files": 0}

    db_columns = [f"`{db_column}`" for _, db_column in CLINICAL_STUDY_FIELDS]
    insert_sql = f"""
        INSERT INTO `{TABLE_NAME}` ({", ".join(db_columns)})
        VALUES ({", ".join(["%s"] * len(CLINICAL_STUDY_FIELDS))})
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

                # ClinicalStudies.csv commonly includes a UTF-8 byte-order mark
                # on the first header. Reading UTF-8 as utf-8-sig strips that
                # marker so DictReader sees "Core Project Number" exactly.
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
                    # missing project number or NCT ID should fail the file
                    # immediately instead of silently inserting NULL values.
                    if not reader.fieldnames:
                        raise ValueError(f"{csv_file.name} does not contain a CSV header row.")

                    csv_columns = set(reader.fieldnames)
                    missing_columns = [
                        csv_column
                        for csv_column, _ in CLINICAL_STUDY_FIELDS
                        if csv_column not in csv_columns
                    ]

                    if missing_columns:
                        raise ValueError(
                            f"{csv_file.name} is missing required column(s): "
                            f"{', '.join(missing_columns)}"
                        )

                    for row in reader:
                        values = []

                        for csv_column, _ in CLINICAL_STUDY_FIELDS:
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
                            # does not grow across the full clinical study file.
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
    """Run the clinical study CSV upload."""

    from utils.tools import ask_to_continue

    if not ask_to_continue("*** Upload the grant Clinical Studies into MySQL database? *** "):
        print("\n------------------------ Stopped ------------------------\n")
        return 1

    summary = upload_clinical_studies()

    print(
        "\nUpload complete: "
        f"files_processed={summary['files_processed']}, "
        f"rows_inserted={summary['rows_inserted']}, "
        f"failed_files={summary['failed_files']}\n"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

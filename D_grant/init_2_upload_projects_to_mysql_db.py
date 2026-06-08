"""
Upload NIH RePORTER project CSV files into the MySQL grant_project table.

Expected input files:
    D_grant/data/projects/RePORTER_PRJ_C_FY1985.CSV
    D_grant/data/projects/RePORTER_PRJ_C_FY1986.CSV
    ...

Usage:
    python D_grant/init_2_upload_projects_to_mysql_db.py

Notes:
    - This loader appends rows. If you are doing a full reload, truncate
      grant_project manually before running:
          TRUNCATE TABLE grant_project;
    - RePORTER project files changed layout after FY2005. Files through FY2005
      use FOA_NUMBER, while FY2006 and later use OPPORTUNITY NUMBER plus
      several cost/organization columns.
"""

import csv
import os
import re
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Add the project root to the Python path when this file is run directly.
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

TABLE_NAME = "grant_project"
DEFAULT_PROJECTS_DIR = SCRIPT_DIR / "data" / "projects"
DEFAULT_BATCH_SIZE = 1000
MIN_REPORTER_PROJECT_YEAR = 1985
MAX_REPORTER_PROJECT_YEAR = date.today().year + 1


@dataclass(frozen=True)
class FieldConfig:
    """Map one CSV column to one grant_project database column."""

    csv_column: str
    db_column: str
    converter: Optional[str] = None


# Tuple[FieldConfig, ...] - tuple of FieldConfig objects, with any length
COMMON_PROJECT_FIELDS: Tuple[FieldConfig, ...] = (
    FieldConfig("APPLICATION_ID", "APPLICATION_ID", "int"),
    FieldConfig("ACTIVITY", "ACTIVITY"),
    FieldConfig("ADMINISTERING_IC", "ADMINISTERING_IC"),
    FieldConfig("APPLICATION_TYPE", "APPLICATION_TYPE", "int"),
    FieldConfig("ARRA_FUNDED", "ARRA_FUNDED"),
    FieldConfig("AWARD_NOTICE_DATE", "AWARD_NOTICE_DATE", "date"),
    FieldConfig("BUDGET_START", "BUDGET_START", "date"),
    FieldConfig("BUDGET_END", "BUDGET_END", "date"),
    FieldConfig("CFDA_CODE", "CFDA_CODE"),
    FieldConfig("CORE_PROJECT_NUM", "CORE_PROJECT_NUM"),
    FieldConfig("ED_INST_TYPE", "ED_INST_TYPE"),
    FieldConfig("FULL_PROJECT_NUM", "FULL_PROJECT_NUM"),
    FieldConfig("SUBPROJECT_ID", "SUBPROJECT_ID"),
    FieldConfig("FUNDING_ICs", "FUNDING_ICs"),
    FieldConfig("FY", "FY", "int"),
    FieldConfig("IC_NAME", "IC_NAME"),
    FieldConfig("NIH_SPENDING_CATS", "NIH_SPENDING_CATS"),
    FieldConfig("ORG_CITY", "ORG_CITY"),
    FieldConfig("ORG_COUNTRY", "ORG_COUNTRY"),
    FieldConfig("ORG_DEPT", "ORG_DEPT"),
    FieldConfig("ORG_DISTRICT", "ORG_DISTRICT"),
    FieldConfig("ORG_DUNS", "ORG_DUNS"),
    FieldConfig("ORG_FIPS", "ORG_FIPS"),
    FieldConfig("ORG_NAME", "ORG_NAME"),
    FieldConfig("ORG_STATE", "ORG_STATE"),
    FieldConfig("ORG_ZIPCODE", "ORG_ZIPCODE"),
    FieldConfig("PHR", "PHR"),
    FieldConfig("PI_IDS", "PI_IDS"),
    FieldConfig("PI_NAMEs", "PI_NAMEs"),
    FieldConfig("PROGRAM_OFFICER_NAME", "PROGRAM_OFFICER_NAME"),
    FieldConfig("PROJECT_START", "PROJECT_START", "date"),
    FieldConfig("PROJECT_END", "PROJECT_END", "date"),
    FieldConfig("PROJECT_TERMS", "PROJECT_TERMS"),
    FieldConfig("PROJECT_TITLE", "PROJECT_TITLE"),
    FieldConfig("SERIAL_NUMBER", "SERIAL_NUMBER"),
    FieldConfig("STUDY_SECTION", "STUDY_SECTION"),
    FieldConfig("STUDY_SECTION_NAME", "STUDY_SECTION_NAME"),
    FieldConfig("SUFFIX", "SUFFIX"),
    FieldConfig("SUPPORT_YEAR", "SUPPORT_YEAR", "int"),
    FieldConfig("TOTAL_COST", "TOTAL_COST", "int"),
    FieldConfig("TOTAL_COST_SUB_PROJECT", "TOTAL_COST_SUB_PROJECT", "int"),
)

PRE_2006_PROJECT_FIELDS: Tuple[FieldConfig, ...] = (
    FieldConfig("FOA_NUMBER", "FOA_NUMBER"),
)

POST_2005_PROJECT_FIELDS: Tuple[FieldConfig, ...] = (
    FieldConfig("OPPORTUNITY NUMBER", "OPPORTUNITY_NUMBER"),
    FieldConfig("FUNDING_MECHANISM", "FUNDING_MECHANISM"),
    FieldConfig("ORG_IPF_CODE", "ORG_IPF_CODE", "int"),
    FieldConfig("DIRECT_COST_AMT", "DIRECT_COST_AMT", "int"),
    FieldConfig("INDIRECT_COST_AMT", "INDIRECT_COST_AMT", "int"),
)


def upload_projects(dir_path: os.PathLike = DEFAULT_PROJECTS_DIR, batch_size: int = DEFAULT_BATCH_SIZE, ) -> Dict[str, int]:
    """
    Upload all RePORTER project CSV files from dir_path into grant_project.

    The CSV files are streamed row by row. Only batch_size rows are kept in
    memory at a time, which is important because the yearly RePORTER project
    files can be large.
    """

    from baseclass.conn import DBConnection as db
    from utils.tools import _normalize_tuple, convert_to_int, detect_file_encoding, parse_date

    projects_dir = Path(dir_path).expanduser().resolve()

    if batch_size <= 0:
        raise ValueError("batch_size must be greater than 0")

    # Some RePORTER text columns can be large. Raise the csv module limit once
    # before streaming files so PHR/PROJECT_TERMS fields do not fail parsing.
    max_csv_field_size = sys.maxsize

    while True:
        try:
            csv.field_size_limit(max_csv_field_size)
            break

        except OverflowError:
            max_csv_field_size = int(max_csv_field_size / 10)

    if not projects_dir.is_dir():
        raise FileNotFoundError(f"Project CSV directory does not exist: {projects_dir}")

    csv_files = sorted(projects_dir.glob("RePORTER_PRJ_C_FY*.[Cc][Ss][Vv]"))

    if not csv_files:
        print(f"No RePORTER project CSV files found in: {projects_dir}")
        return {"files_processed": 0, "rows_inserted": 0, "failed_files": 0}

    conn = db().mysql_conn()
    cursor = None
    summary = {"files_processed": 0, "rows_inserted": 0, "failed_files": 0}

    try:
        cursor = conn.cursor()

        for csv_file in csv_files:
            try:
                # The project CSV schema changes by fiscal year. Keeping the
                # field list year-aware lets one loader handle both historical
                # files and current files while still generating one explicit
                # INSERT column list for MySQL.
                year = _get_year(csv_file.name)
                fields = COMMON_PROJECT_FIELDS + (
                    POST_2005_PROJECT_FIELDS if year > 2005 else PRE_2006_PROJECT_FIELDS
                )

                # Build the INSERT statement from the exact field mapping used
                # below to convert row values. This prevents a common loader
                # bug where the VALUES placeholders drift away from the column
                # order after adding/removing a CSV field.
                db_columns = [f"`{field.db_column}`" for field in fields]
                placeholders = ["%s"] * len(db_columns)
                insert_sql = f"""
                    INSERT INTO `{TABLE_NAME}` ({", ".join(db_columns)})
                    VALUES ({", ".join(placeholders)})
                """

                row_batch: List[Tuple[Any, ...]] = []
                inserted_count = 0

                # RePORTER files have not always used the same encoding. Detect
                # the likely encoding from the file bytes, then normalize UTF-8
                # to utf-8-sig so Python strips a byte-order mark from the first
                # header column when present.
                detected_encoding, confidence = detect_file_encoding(csv_file)
                encoding = detected_encoding or "utf-8-sig"
                normalized_encoding = encoding.strip().lower().replace("_", "-")

                # utf-8-sig reads normal UTF-8 and also removes a UTF-8 BOM from
                # the first CSV header when one is present.
                if normalized_encoding in {"utf-8", "utf8", "utf-8-sig"}:
                    encoding = "utf-8-sig"

                print(f"\n{csv_file.name}")
                print(f"[Year={year}] post_2005_format={year > 2005}")
                print(
                    f"Detected encoding: {detected_encoding} "
                    f"(confidence: {confidence:.2%}); reading as {encoding}"
                )

                with csv_file.open("r", newline="", encoding=encoding, errors="replace") as file_obj:
                    reader = csv.DictReader(file_obj)

                    # Validate headers once before processing rows. A missing
                    # field should fail the file immediately instead of creating
                    # rows with silent NULL values in important columns.
                    if not reader.fieldnames:
                        raise ValueError(f"{csv_file.name} does not contain a CSV header row.")

                    csv_columns = set(reader.fieldnames)
                    missing_columns = [
                        field.csv_column
                        for field in fields
                        if field.csv_column not in csv_columns
                    ]

                    if missing_columns:
                        raise ValueError(
                            f"{csv_file.name} is missing required column(s): "
                            f"{', '.join(missing_columns)}"
                        )

                    for row_number, row in enumerate(reader, start=2):
                        values = []

                        # Convert the current CSV row in the same order as the
                        # generated INSERT columns. Empty strings become NULL;
                        # dates use the shared parse_date helper; integer-ish
                        # money/count fields allow comma separators.
                        for field in fields:
                            value = row.get(field.csv_column)

                            try:
                                if value is None:
                                    converted_value = None

                                else:
                                    if isinstance(value, str):
                                        value = value.strip()

                                    if value == "":
                                        converted_value = None
                                    elif field.converter == "int":
                                        converted_value = convert_to_int(value)
                                    elif field.converter == "date":
                                        converted_value = parse_date(value)
                                    else:
                                        converted_value = value

                            except ValueError as exc:
                                raise ValueError(
                                    f"{csv_file.name} row {row_number}, "
                                    f"column {field.csv_column}: {exc}"
                                ) from exc

                            values.append(converted_value)

                        # Keep using the shared normalizer so text cleanup stays
                        # consistent with the rest of the RDAS import pipeline.
                        row_batch.append(_normalize_tuple(tuple(values)))

                        if len(row_batch) >= batch_size:
                            # Commit each batch. This keeps memory bounded and
                            # avoids one huge transaction across a yearly file.
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

                if row_batch:
                    # Flush the final partial batch for the file.
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


def _get_year(filename: str) -> int:
    """Extract and validate the fiscal year from a RePORTER project filename."""

    pattern = r"RePORTER_PRJ_C_FY(\d{4})\.[Cc][Ss][Vv]$"
    match = re.match(pattern, filename)

    if not match:
        raise ValueError(
            f"Filename '{filename}' does not match the expected pattern "
            "'RePORTER_PRJ_C_FY<year>.CSV'"
        )

    year = int(match.group(1))

    if year < MIN_REPORTER_PROJECT_YEAR or year > MAX_REPORTER_PROJECT_YEAR:
        raise ValueError(
            f"Year {year} is outside the expected RePORTER project range "
            f"{MIN_REPORTER_PROJECT_YEAR}-{MAX_REPORTER_PROJECT_YEAR}."
        )

    return year


def main() -> int:
    """Run the project CSV upload."""

    from utils.tools import ask_to_continue

    if not ask_to_continue("*** Upload the grant Projects into MySQL database? *** "):
        print("\n------------------------ Stopped ------------------------\n")
        return 1

    summary = upload_projects()

    print(
        "\nUpload complete: "
        f"files_processed={summary['files_processed']}, "
        f"rows_inserted={summary['rows_inserted']}, "
        f"failed_files={summary['failed_files']}\n"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

import csv
import os
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pipelines.pipeline_4_grant.grant_base import DEFAULT_BATCH_SIZE, DEFAULT_PROJECTS_DIR, GrantPipelineBase
from utils.tools import _time_hms


"""
Upload NIH RePORTER project CSV files into grant_project for the grant alert pipeline.

Rows are matched by APPLICATION_ID because the alert workflow refreshes a yearly
project export. Existing APPLICATION_ID rows are updated and marked is_new = 1.
Missing APPLICATION_ID rows are inserted and also marked is_new = 1.
"""

# Reference: D_grant/init_2_upload_projects_to_mysql_db.py

TABLE_NAME = "grant_project"


@dataclass(frozen=True)
class FieldConfig:
    """Map one CSV column to one grant_project database column."""

    csv_column: str
    db_column: str
    converter: Optional[str] = None
    alternate_csv_columns: Tuple[str, ...] = ()

    def source_columns(self) -> Tuple[str, ...]:
        """Return accepted CSV header names for this database field."""

        return (self.csv_column,) + self.alternate_csv_columns


    def source_label(self) -> str:
        """Return a readable label for validation errors."""

        if not self.alternate_csv_columns:
            return self.csv_column

        return f"{self.csv_column} or {' or '.join(self.alternate_csv_columns)}"


COMMON_PROJECT_FIELDS: Tuple[FieldConfig, ...] = (
    FieldConfig("APPLICATION_ID", "APPLICATION_ID", "int"),
    FieldConfig("ACTIVITY", "ACTIVITY"),
    FieldConfig("ADMINISTERING_IC", "ADMINISTERING_IC"),
    FieldConfig("APPLICATION_TYPE", "APPLICATION_TYPE", "int"),
    FieldConfig("ARRA_FUNDED", "ARRA_FUNDED"),
    FieldConfig("AWARD_NOTICE_DATE", "AWARD_NOTICE_DATE", "date"),
    FieldConfig("BUDGET_START", "BUDGET_START", "date"),
    FieldConfig("BUDGET_END", "BUDGET_END", "date"),
    FieldConfig("CFDA_CODE", "CFDA_CODE", alternate_csv_columns=("ASSISTANCE_LISTING_NUMBER",)),
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


class GrantProjectUploadTask(GrantPipelineBase):
    """Upsert downloaded NIH RePORTER project CSV rows into MySQL grant_project."""

    def __init__(self, years: Sequence[int], projects_dir: os.PathLike = DEFAULT_PROJECTS_DIR, batch_size: int = DEFAULT_BATCH_SIZE):
        super().__init__(init_mysql=True, init_memgraph=False)

        self.years = self._resolve_years(years, required=True)
        self.projects_dir = Path(projects_dir).expanduser().resolve()
        self.batch_size = batch_size


    def find_new_data(self, gard_node) -> None:
        self.logger.info("GrantProjectUploadTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Read project CSV files and upsert rows by APPLICATION_ID."""

        from utils.tools import detect_file_encoding

        start_time = time.time()
        summary = {
            "files_processed": 0,
            "rows_seen": 0,
            "rows_inserted": 0,
            "rows_updated": 0,
            "rows_skipped_missing_application_id": 0,
            "failed_files": 0,
        }
        cursor = None

        try:
            if self.batch_size <= 0:
                raise ValueError("batch_size must be greater than 0")

            self._raise_csv_field_size_limit()

            if not self.projects_dir.is_dir():
                raise FileNotFoundError(f"Project CSV directory does not exist: {self.projects_dir}")

            csv_files = []

            for csv_file in sorted(self.projects_dir.glob("RePORTER_PRJ_C_FY*.[Cc][Ss][Vv]")):
                year = self._get_reporter_project_year(csv_file.name)

                if year in self.years:
                    csv_files.append((csv_file, year))

            if not csv_files:
                raise FileNotFoundError(f"No RePORTER project CSV files found for years={self.years} in: {self.projects_dir}")

            cursor = self.mysql.cursor()

            for csv_file, year in csv_files:
                try:
                    fields = COMMON_PROJECT_FIELDS + (POST_2005_PROJECT_FIELDS if year > 2005 else PRE_2006_PROJECT_FIELDS)
                    insert_sql, update_sql = self._build_upsert_sql(fields)
                    detected_encoding, confidence = detect_file_encoding(csv_file)
                    encoding = self._normalize_detected_encoding(detected_encoding)
                    file_summary = {
                        "rows_seen": 0,
                        "rows_inserted": 0,
                        "rows_updated": 0,
                        "rows_skipped_missing_application_id": 0,
                    }
                    row_batch: List[Tuple[Any, ...]] = []

                    self.logger.info(
                        f"Processing {csv_file.name}: year={year}, post_2005_format={year > 2005}, "
                        f"encoding={detected_encoding}, confidence={confidence:.2%}, read_as={encoding}"
                    )

                    with csv_file.open("r", newline="", encoding=encoding, errors="replace") as file_obj:
                        
                        reader = csv.DictReader(file_obj)
                        self._validate_csv_headers(csv_file.name, reader.fieldnames, fields)

                        for row_number, row in enumerate(reader, start=2):
                            values = self._build_row_values(csv_file.name, row_number, row, fields)
                            application_id = values[0]

                            if application_id is None:
                                file_summary["rows_skipped_missing_application_id"] += 1
                                continue

                            row_batch.append(values)
                            file_summary["rows_seen"] += 1

                            if len(row_batch) >= self.batch_size:
                                self._flush_row_batch(cursor, fields, insert_sql, update_sql, row_batch, file_summary)
                                self.mysql.commit()
                                row_batch.clear()

                    if row_batch:
                        self._flush_row_batch(cursor, fields, insert_sql, update_sql, row_batch, file_summary)
                        self.mysql.commit()
                        row_batch.clear()

                    summary["files_processed"] += 1

                    for key in ("rows_seen", "rows_inserted", "rows_updated", "rows_skipped_missing_application_id"):
                        summary[key] += file_summary[key]

                    self.logger.info(f"Finished {csv_file.name}: {file_summary}")

                except Exception as e:
                    summary["failed_files"] += 1
                    self.mysql.rollback()
                    self.logger.error(f"Failed to upload {csv_file.name}: {e}")
                    raise

            self.logger.info(f"Completed grant project upload. Summary={summary}")

        except Exception as e:
            self.logger.error(f"GrantProjectUploadTask failed: {e}")
            raise

        finally:
            if cursor:
                cursor.close()

            hours, minutes, seconds = _time_hms(time.time() - start_time)
            self.logger.info(f"Total time elapsed: {hours} hours, {minutes} minutes, {seconds} seconds")

            ''' Explicitly close all db connections. '''
            self.close()


    def _build_upsert_sql(self, fields: Sequence[FieldConfig]) -> Tuple[str, str]:
        """Build INSERT and UPDATE statements from the selected project fields."""

        db_columns = [f"`{field.db_column}`" for field in fields]
        insert_columns = db_columns + ["`is_new`"]
        insert_placeholders = ["%s"] * len(insert_columns)
        update_assignments = [
            f"`{field.db_column}` = %s"
            for field in fields
            if field.db_column != "APPLICATION_ID"
        ]
        update_assignments.append("`is_new` = 1")
        insert_sql = f"""
            INSERT INTO `{TABLE_NAME}` ({", ".join(insert_columns)})
            VALUES ({", ".join(insert_placeholders)})
        """
        update_sql = f"""
            UPDATE `{TABLE_NAME}`
            SET {", ".join(update_assignments)}
            WHERE `APPLICATION_ID` = %s
        """

        return insert_sql, update_sql


    def _flush_row_batch(self, cursor: Any, fields: Sequence[FieldConfig], insert_sql: str, update_sql: str, row_batch: List[Tuple[Any, ...]], summary: Dict[str, int]) -> None:
        """Insert missing APPLICATION_ID rows and update existing rows."""

        deduped_rows = self._dedupe_rows_by_application_id(row_batch)
        existing_application_ids = self._get_existing_application_ids(cursor, [row[0] for row in deduped_rows])
        insert_values = []
        update_values = []

        for row_values in deduped_rows:
            application_id = row_values[0]

            if application_id in existing_application_ids:
                update_values.append(self._build_update_values(fields, row_values, application_id))
            else:
                insert_values.append(tuple(row_values) + (1,))

        if update_values:
            cursor.executemany(update_sql, update_values)
            summary["rows_updated"] += len(update_values)

        if insert_values:
            cursor.executemany(insert_sql, insert_values)
            summary["rows_inserted"] += len(insert_values)


    def _dedupe_rows_by_application_id(self, row_batch: List[Tuple[Any, ...]]) -> List[Tuple[Any, ...]]:
        """Keep the last row in the batch when a CSV repeats APPLICATION_ID."""

        rows_by_application_id = {}

        for row_values in row_batch:
            rows_by_application_id[row_values[0]] = row_values

        return list(rows_by_application_id.values())


    def _get_existing_application_ids(self, cursor: Any, application_ids: Sequence[Any]) -> set:
        """Return APPLICATION_ID values that already exist in grant_project."""

        if not application_ids:
            return set()

        placeholders = ", ".join(["%s"] * len(application_ids))
        query = f"""
            SELECT DISTINCT APPLICATION_ID
            FROM `{TABLE_NAME}`
            WHERE APPLICATION_ID IN ({placeholders})
        """

        cursor.execute(query, tuple(application_ids))

        return {row[0] for row in cursor.fetchall()}


    def _build_update_values(self, fields: Sequence[FieldConfig], row_values: Tuple[Any, ...], application_id: Any) -> Tuple[Any, ...]:
        """Build an UPDATE value tuple, excluding APPLICATION_ID from SET."""

        values = [
            value
            for field, value in zip(fields, row_values)
            if field.db_column != "APPLICATION_ID"
        ]
        values.append(application_id)

        return tuple(values)


    def _build_row_values(self, filename: str, row_number: int, row: Dict[str, Any], fields: Sequence[FieldConfig]) -> Tuple[Any, ...]:
        """Convert one CSV row into a normalized tuple matching fields."""

        from utils.tools import _normalize_tuple, convert_to_int, parse_date

        values = []

        for field in fields:
            source_column = field.csv_column
            value = None

            for csv_column in field.source_columns():
                if csv_column in row:
                    source_column = csv_column
                    value = row.get(csv_column)
                    break

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

            except ValueError as e:
                raise ValueError(f"{filename} row {row_number}, column {source_column}: {e}") from e

            values.append(converted_value)

        return _normalize_tuple(tuple(values))


    def _validate_csv_headers(self, filename: str, fieldnames: Optional[List[str]], fields: Sequence[FieldConfig]) -> None:
        """Validate that the CSV has every required column for its fiscal year."""

        if not fieldnames:
            raise ValueError(f"{filename} does not contain a CSV header row.")

        csv_columns = set(fieldnames)
        missing_columns = [
            field.source_label()
            for field in fields
            if not any(csv_column in csv_columns for csv_column in field.source_columns())
        ]

        if missing_columns:
            raise ValueError(f"{filename} is missing required column(s): {', '.join(missing_columns)}")


if __name__ == "__main__":

    task = GrantProjectUploadTask(years=[date.today().year - 1])
    task.process_new_data()

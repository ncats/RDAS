import csv
import os
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pipelines.pipeline_4_grant.grant_base import GrantPipelineBase
from utils.tools import _time_hms


"""
Upload NIH RePORTER publication-project linktable CSV files into grant_linktable.

This task is the alert-pipeline version of the full linktable initializer. It
only processes the fiscal years passed to the constructor, and it gets YEAR
from the NIH RePORTER filename, for example:
RePORTER_PUBLNK_C_FY2025.csv -> YEAR = 2025.

Each source row links a publication PMID to a NIH project number for one fiscal
year. grant_linktable does not currently enforce a unique key for
`(YEAR, PMID, PROJECT_NUMBER)`, so this task checks existing mappings first
instead of using `ON DUPLICATE KEY UPDATE`.

If the mapping already exists, the task updates `is_new = 1`. If the mapping is
missing, the task inserts it with `is_new = 1`. This keeps reruns from creating
duplicate mappings while still marking all refreshed yearly mappings for
downstream alert processing.
"""

# Reference: D_grant/init_5_upload_linktables_to_mysql_db.py

TABLE_NAME = "grant_linktable"
LINKTABLE_FILE_PREFIX = "RePORTER_PUBLNK_C_FY"

GRANT_LINKTABLE_INSERT_SQL = """
    INSERT INTO `grant_linktable` (`YEAR`, `PMID`, `PROJECT_NUMBER`, `is_new`)
    VALUES (%s, %s, %s, %s)
"""

GRANT_LINKTABLE_UPDATE_IS_NEW_SQL = """
    UPDATE `grant_linktable`
    SET `is_new` = 1
    WHERE `YEAR` = %s
      AND `PMID` = %s
      AND `PROJECT_NUMBER` <=> %s
"""


@dataclass(frozen=True)
class LinktableField:
    """Map one linktable CSV column to one grant_linktable database column."""

    csv_column: str
    db_column: str
    converter: Optional[str] = None


LINKTABLE_FIELDS: Tuple[LinktableField, ...] = (
    LinktableField("PMID", "PMID", "int"),
    LinktableField("PROJECT_NUMBER", "PROJECT_NUMBER"),
)

PMID_FIELD_INDEX = next(index for index, field in enumerate(LINKTABLE_FIELDS) if field.db_column == "PMID")
PROJECT_NUMBER_FIELD_INDEX = next(index for index, field in enumerate(LINKTABLE_FIELDS) if field.db_column == "PROJECT_NUMBER")


class GrantLinktableUploadTask(GrantPipelineBase):
    """Upload downloaded NIH RePORTER linktable CSV rows into MySQL grant_linktable."""

    def __init__(self, years: Sequence[int], linktables_dir: Optional[os.PathLike] = None, batch_size: Optional[int] = None):
        super().__init__(init_mysql=True, init_memgraph=False)

        self.years = self._resolve_years(years, required=True)
        self.linktables_dir = Path(linktables_dir or self.DEFAULT_LINKTABLES_DIR).expanduser().resolve()
        self.batch_size = batch_size if batch_size is not None else self.DEFAULT_BATCH_SIZE


    def find_new_data(self, gard_node) -> None:
        self.logger.info("GrantLinktableUploadTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Read linktable CSV files for the configured years and insert/update rows."""

        from utils.tools import detect_file_encoding

        start_time = time.time()
        summary = {
            "files_processed": 0,
            "rows_seen": 0,
            "rows_inserted": 0,
            "rows_updated": 0,
            "rows_skipped_missing_pmid": 0,
            "failed_files": 0,
        }
        cursor = None

        try:
            if self.batch_size <= 0:
                raise ValueError("batch_size must be greater than 0")

            # Linktable rows are small, but yearly files can be large. Raise
            # the CSV field limit once so unusual cells do not fail parsing.
            self._raise_csv_field_size_limit()

            if not self.linktables_dir.is_dir():
                raise FileNotFoundError(f"Linktable CSV directory does not exist: {self.linktables_dir}")

            csv_files = []

            for csv_file in sorted(self.linktables_dir.glob(f"{LINKTABLE_FILE_PREFIX}*.[Cc][Ss][Vv]")):
                # The linktable CSV itself does not carry YEAR. The initializer
                # derives YEAR from the source file name, so this task follows
                # the same rule for alert pipeline loads.
                year = self._get_reporter_linktable_year(csv_file.name)

                if year in self.years:
                    csv_files.append((csv_file, year))

            if not csv_files:
                raise FileNotFoundError(f"No RePORTER linktable CSV files found for years={self.years} in: {self.linktables_dir}")

            cursor = self.mysql.cursor()

            for csv_file, year in csv_files:
                try:
                    detected_encoding, confidence = detect_file_encoding(csv_file)
                    encoding = self._normalize_detected_encoding(detected_encoding)
                    file_summary = {
                        "rows_seen": 0,
                        "rows_inserted": 0,
                        "rows_updated": 0,
                        "rows_skipped_missing_pmid": 0,
                    }
                    row_batch: List[Tuple[Any, ...]] = []

                    self.logger.info(f"Processing {csv_file.name}: year={year}, encoding={detected_encoding}, confidence={confidence:.2%}, read_as={encoding}")

                    with csv_file.open("r", newline="", encoding=encoding, errors="replace") as file_obj:
                        reader = csv.DictReader(file_obj)
                        self._validate_csv_headers(csv_file.name, reader.fieldnames, LINKTABLE_FIELDS)

                        for row_number, row in enumerate(reader, start=2):
                            values = self._build_row_values(csv_file.name, row_number, row, LINKTABLE_FIELDS)
                            pmid = values[PMID_FIELD_INDEX]

                            if pmid is None:
                                file_summary["rows_skipped_missing_pmid"] += 1
                                continue

                            row_batch.append(values)
                            file_summary["rows_seen"] += 1

                            if len(row_batch) >= self.batch_size:
                                self._flush_row_batch(cursor, year, row_batch, file_summary)
                                self.mysql.commit()
                                row_batch.clear()

                    if row_batch:
                        self._flush_row_batch(cursor, year, row_batch, file_summary)
                        self.mysql.commit()
                        row_batch.clear()

                    summary["files_processed"] += 1

                    for key in ("rows_seen", "rows_inserted", "rows_updated", "rows_skipped_missing_pmid"):
                        summary[key] += file_summary[key]

                    self.logger.info(f"Finished {csv_file.name}: {file_summary}")

                except Exception:
                    summary["failed_files"] += 1
                    self.mysql.rollback()
                    self.logger.exception(f"Failed to upload grant linktable file={csv_file.name}, year={year}, file_summary={file_summary}. Rolled back this file and continuing.")
                    continue

            self.logger.info(f"Completed grant linktable upload. Summary={summary}")

        except Exception:
            self.logger.exception(f"GrantLinktableUploadTask failed. years={self.years}, linktables_dir={self.linktables_dir}, summary={summary}")
            raise

        finally:
            if cursor:
                cursor.close()

            hours, minutes, seconds = _time_hms(time.time() - start_time)
            self.logger.info(f"Total time elapsed: {hours} hours, {minutes} minutes, {seconds} seconds")

            ''' Explicitly close all db connections. '''
            self.close()


    def _get_reporter_linktable_year(self, filename: str) -> int:
        """Extract and validate the fiscal year from a RePORTER linktable filename."""

        return self._get_reporter_export_year(filename, LINKTABLE_FILE_PREFIX, "RePORTER linktable")


    def _flush_row_batch(self, cursor: Any, year: int, row_batch: List[Tuple[Any, ...]], summary: Dict[str, int]) -> None:
        """Insert missing mappings and mark existing mappings as new."""

        # The table has no unique key for `(YEAR, PMID, PROJECT_NUMBER)`.
        # Deduping the batch and checking existing mappings keeps reruns from
        # inserting duplicate rows while still setting is_new=1 on old rows.
        deduped_rows = self._dedupe_rows_by_link_key(row_batch)
        existing_keys = self._get_existing_link_keys(cursor, year, deduped_rows)
        insert_values = []
        update_values = []

        for row_values in deduped_rows:
            pmid = row_values[PMID_FIELD_INDEX]
            project_number = row_values[PROJECT_NUMBER_FIELD_INDEX]
            key = (year, pmid, project_number)

            if key in existing_keys:
                # Use NULL-safe equality in the UPDATE SQL so existing rows
                # with PROJECT_NUMBER = NULL can still be refreshed.
                update_values.append((year, pmid, project_number))
            else:
                insert_values.append((year, pmid, project_number, 1))

        if update_values:
            cursor.executemany(GRANT_LINKTABLE_UPDATE_IS_NEW_SQL, update_values)
            summary["rows_updated"] += len(update_values)

        if insert_values:
            cursor.executemany(GRANT_LINKTABLE_INSERT_SQL, insert_values)
            summary["rows_inserted"] += len(insert_values)


    def _dedupe_rows_by_link_key(self, row_batch: List[Tuple[Any, ...]]) -> List[Tuple[Any, ...]]:
        """Keep the last row when a CSV repeats the same PMID/project mapping."""

        rows_by_link_key = {}

        for row_values in row_batch:
            rows_by_link_key[(row_values[PMID_FIELD_INDEX], row_values[PROJECT_NUMBER_FIELD_INDEX])] = row_values

        return list(rows_by_link_key.values())


    def _get_existing_link_keys(self, cursor: Any, year: int, rows: Sequence[Tuple[Any, ...]]) -> set:
        """Return existing `(YEAR, PMID, PROJECT_NUMBER)` mappings for one batch."""

        if not rows:
            return set()

        pmids = sorted({row[PMID_FIELD_INDEX] for row in rows if row[PMID_FIELD_INDEX] is not None})

        if not pmids:
            return set()

        # grant_linktable has an index on PMID. Fetching by year plus batched
        # PMIDs lets MySQL use that index, then Python performs the exact
        # project-number comparison for idempotent insert/update decisions.
        placeholders = ", ".join(["%s"] * len(pmids))
        query = f"""
            SELECT DISTINCT `YEAR`, `PMID`, `PROJECT_NUMBER`
            FROM `{TABLE_NAME}`
            WHERE `YEAR` = %s
              AND `PMID` IN ({placeholders})
        """

        cursor.execute(query, tuple([year] + pmids))

        return {(row[0], row[1], row[2]) for row in cursor.fetchall()}


    def _build_row_values(self, filename: str, row_number: int, row: Dict[str, Any], fields: Sequence[LinktableField]) -> Tuple[Any, ...]:
        """Convert one CSV row into a normalized tuple matching LINKTABLE_FIELDS."""

        from utils.tools import _normalize_tuple, convert_to_int

        values = []

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
                    else:
                        converted_value = value

            except ValueError as e:
                raise ValueError(f"{filename} row {row_number}, column {field.csv_column}: {e}") from e

            values.append(converted_value)

        # ID and CREATED are database-owned. YEAR is passed separately because
        # it comes from the file name rather than from a CSV column.
        return _normalize_tuple(tuple(values))


    def _validate_csv_headers(self, filename: str, fieldnames: Optional[List[str]], fields: Sequence[LinktableField]) -> None:
        """Validate that the CSV has every required linktable column."""

        if not fieldnames:
            raise ValueError(f"{filename} does not contain a CSV header row.")

        csv_columns = set(fieldnames)
        missing_columns = [
            field.csv_column
            for field in fields
            if field.csv_column not in csv_columns
        ]

        if missing_columns:
            raise ValueError(f"{filename} is missing required column(s): {', '.join(missing_columns)}")


if __name__ == "__main__":

    task = GrantLinktableUploadTask(years=[date.today().year - 1])
    task.process_new_data()

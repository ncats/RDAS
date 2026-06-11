import csv
import os
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pipelines.pipeline_4_grant.grant_base import BASE_DIR, GrantPipelineBase
from utils.tools import _time_hms


"""
Upload NIH RePORTER abstract CSV files into grant_abstract for the grant alert pipeline.

This task is the alert-pipeline version of the full abstract initializer. It
only processes the fiscal years passed to the constructor, and it expects the
year to come from the NIH RePORTER file name, for example:
RePORTER_PRJABS_C_FY2025.csv -> YEAR = 2025.

Each abstract row is identified by `(APPLICATION_ID, YEAR)`. If that pair is
already in grant_abstract, the task updates ABSTRACT_TEXT and marks the row
`is_new = 1`. If that pair is not present, the task inserts a new row with
`is_new = 1`.

The table has a unique key on `(APPLICATION_ID, YEAR)`, so the task uses
`INSERT ... ON DUPLICATE KEY UPDATE`. This makes reruns safe: MySQL inserts a
new abstract when the application/year pair is new, or refreshes the existing
row when that pair already exists.
"""

# Reference: D_grant/init_4_upload_abstracts_to_mysql_db.py

TABLE_NAME = "grant_abstract"
DEFAULT_ABSTRACTS_DIR = BASE_DIR / "abstracts"
ABSTRACT_FILE_PREFIX = "RePORTER_PRJABS_C_FY"

GRANT_ABSTRACT_UPSERT_SQL = """
    INSERT INTO `grant_abstract` (`YEAR`, `APPLICATION_ID`, `ABSTRACT_TEXT`, `is_new`)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        `ABSTRACT_TEXT` = VALUES(`ABSTRACT_TEXT`),
        `is_new` = 1
"""


@dataclass(frozen=True)
class AbstractField:
    """Map one abstract CSV column to one grant_abstract database column."""

    csv_column: str
    db_column: str
    converter: Optional[str] = None


ABSTRACT_FIELDS: Tuple[AbstractField, ...] = (
    AbstractField("APPLICATION_ID", "APPLICATION_ID", "int"),
    AbstractField("ABSTRACT_TEXT", "ABSTRACT_TEXT"),
)

APPLICATION_ID_FIELD_INDEX = next(index for index, field in enumerate(ABSTRACT_FIELDS) if field.db_column == "APPLICATION_ID")
ABSTRACT_TEXT_FIELD_INDEX = next(index for index, field in enumerate(ABSTRACT_FIELDS) if field.db_column == "ABSTRACT_TEXT")


class GrantAbstractUploadTask(GrantPipelineBase):
    """Upsert downloaded NIH RePORTER abstract CSV rows into MySQL grant_abstract."""

    def __init__(self, years: Sequence[int], abstracts_dir: os.PathLike = DEFAULT_ABSTRACTS_DIR):

        super().__init__(init_mysql=True, init_memgraph=False)

        self.years = self._resolve_years(years, required=True)
        self.abstracts_dir = Path(abstracts_dir).expanduser().resolve()
        self.batch_size = 20


    def find_new_data(self, gard_node) -> None:
        self.logger.info("GrantAbstractUploadTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Read abstract CSV files for the configured years and upsert rows."""

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

            # ABSTRACT_TEXT is a mediumtext field, so a single CSV value can be much larger than Python's default csv parser limit.
            self._raise_csv_field_size_limit()

            if not self.abstracts_dir.is_dir():
                raise FileNotFoundError(f"Abstract CSV directory does not exist: {self.abstracts_dir}")

            csv_files = []

            for csv_file in sorted(self.abstracts_dir.glob(f"{ABSTRACT_FILE_PREFIX}*.[Cc][Ss][Vv]")):
                # RePORTER abstract exports do not store the fiscal year in a dedicated CSV column. 
                # The existing initializer also derives YEAR from the source filename, so this task keeps that rule.
                year = self._get_reporter_abstract_year(csv_file.name)

                if year in self.years:
                    csv_files.append((csv_file, year))

            if not csv_files:
                raise FileNotFoundError(f"No RePORTER abstract CSV files found for years={self.years} in: {self.abstracts_dir}")

            cursor = self.mysql.cursor()

            for csv_file, year in csv_files:
                try:
                    detected_encoding, confidence = detect_file_encoding(csv_file)
                    encoding = self._normalize_detected_encoding(detected_encoding)

                    file_summary = {
                        "rows_seen": 0,
                        "rows_inserted": 0,
                        "rows_updated": 0,
                        "rows_skipped_missing_application_id": 0,
                    }

                    row_batch: List[Tuple[Any, ...]] = []

                    self.logger.info(f"Processing {csv_file.name}: year={year}, encoding={detected_encoding}, confidence={confidence:.2%}, read_as={encoding}")

                    with csv_file.open("r", newline="", encoding=encoding, errors="replace") as file_obj:

                        reader = csv.DictReader(file_obj)
                        self._validate_csv_headers(csv_file.name, reader.fieldnames, ABSTRACT_FIELDS)

                        for row_number, row in enumerate(reader, start=2):

                            values = self._build_row_values(csv_file.name, row_number, row, ABSTRACT_FIELDS)
                            application_id = values[APPLICATION_ID_FIELD_INDEX]

                            if application_id is None:
                                file_summary["rows_skipped_missing_application_id"] += 1
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

                    for key in ("rows_seen", "rows_inserted", "rows_updated", "rows_skipped_missing_application_id"):
                        summary[key] += file_summary[key]

                    self.logger.info(f"Finished {csv_file.name}: {file_summary}")

                except Exception:
                    summary["failed_files"] += 1
                    self.mysql.rollback()
                    self.logger.exception(f"Failed to upload grant abstract file={csv_file.name}, year={year}, file_summary={file_summary}. Rolled back this file and continuing.")
                    continue

            self.logger.info(f"Completed grant abstract upload. Summary={summary}")

        except Exception:
            self.logger.exception(f"GrantAbstractUploadTask failed. years={self.years}, abstracts_dir={self.abstracts_dir}, summary={summary}")
            raise

        finally:
            if cursor:
                cursor.close()

            hours, minutes, seconds = _time_hms(time.time() - start_time)
            self.logger.info(f"Total time elapsed: {hours} hours, {minutes} minutes, {seconds} seconds")

            ''' Explicitly close all db connections. '''
            self.close()


    def _get_reporter_abstract_year(self, filename: str) -> int:
        """Extract and validate the fiscal year from a RePORTER abstract filename."""

        return self._get_reporter_export_year(filename, ABSTRACT_FILE_PREFIX, "RePORTER abstract")


    def _flush_row_batch(self, cursor: Any, year: int, row_batch: List[Tuple[Any, ...]], summary: Dict[str, int]) -> None:
        """Upsert abstract rows by the unique `(APPLICATION_ID, YEAR)` key."""

        # A RePORTER file should not repeat the same application/year pair, but
        # deduping makes the task deterministic if the source file does contain
        # a duplicate row. The last row wins, matching task_grant_3/4 behavior.
        deduped_rows = self._dedupe_rows_by_application_id_and_year(row_batch)
        existing_keys = self._get_existing_application_year_keys(cursor, [(row[APPLICATION_ID_FIELD_INDEX], year) for row in deduped_rows])
        upsert_values = []
        updated_count = 0

        for row_values in deduped_rows:
            application_id = row_values[APPLICATION_ID_FIELD_INDEX]
            key = (application_id, year)
            abstract_text = row_values[ABSTRACT_TEXT_FIELD_INDEX]
            upsert_values.append((year, application_id, abstract_text, 1))

            if key in existing_keys:
                updated_count += 1

        if upsert_values:
            # The unique key controls whether each value tuple becomes an
            # INSERT or an UPDATE. The existing-key precheck above is only for
            # log counters; correctness comes from MySQL's unique constraint.
            cursor.executemany(GRANT_ABSTRACT_UPSERT_SQL, upsert_values)
            summary["rows_updated"] += updated_count
            summary["rows_inserted"] += len(upsert_values) - updated_count


    def _dedupe_rows_by_application_id_and_year(self, row_batch: List[Tuple[Any, ...]]) -> List[Tuple[Any, ...]]:
        """Keep the last row in the batch when a CSV repeats APPLICATION_ID."""

        rows_by_application_id = {}

        for row_values in row_batch:
            rows_by_application_id[row_values[APPLICATION_ID_FIELD_INDEX]] = row_values

        return list(rows_by_application_id.values())


    def _get_existing_application_year_keys(self, cursor: Any, application_year_keys: Sequence[Tuple[Any, int]]) -> set:
        """Return `(APPLICATION_ID, YEAR)` keys that already exist in grant_abstract."""

        if not application_year_keys:
            return set()

        # The table has a unique key on `(APPLICATION_ID, YEAR)`, so this
        # composite lookup is fast and lets the task keep clear insert/update
        # counters while still relying on native upsert for the write.
        placeholders = ", ".join(["(%s, %s)"] * len(application_year_keys))
        query = f"""
            SELECT DISTINCT APPLICATION_ID, YEAR
            FROM `{TABLE_NAME}`
            WHERE (APPLICATION_ID, YEAR) IN ({placeholders})
        """
        params = []

        for application_id, year in application_year_keys:
            params.extend((application_id, year))

        cursor.execute(query, tuple(params))

        return {(row[0], row[1]) for row in cursor.fetchall()}


    def _build_row_values(self, filename: str, row_number: int, row: Dict[str, Any], fields: Sequence[AbstractField]) -> Tuple[Any, ...]:
        """Convert one CSV row into a normalized tuple matching ABSTRACT_FIELDS."""

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

        # The tuple intentionally excludes ID/CREATED because MySQL owns those
        # columns. YEAR is passed separately to the insert/update layer.
        return _normalize_tuple(tuple(values))


    def _validate_csv_headers(self, filename: str, fieldnames: Optional[List[str]], fields: Sequence[AbstractField]) -> None:
        """Validate that the CSV has every required abstract column."""

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

    task = GrantAbstractUploadTask(years=[date.today().year - 1])
    task.process_new_data()

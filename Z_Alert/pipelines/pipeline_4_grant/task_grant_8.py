import csv
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pipelines.pipeline_4_grant.grant_base import GrantPipelineBase
from utils.tools import _time_hms


"""
Upload NIH RePORTER patent CSV files into grant_patent.

This task is the alert-pipeline version of the full patent initializer. The NIH
patent export is not fiscal-year-specific, so this task intentionally does not
accept a years argument. It processes the patent CSV files currently downloaded
under the grant pipeline patents data directory.

Each patent row is identified by `(PATENT_ID, PROJECT_ID)`. A patent can be
linked to more than one NIH project, so PATENT_ID alone is not enough for
idempotent alert-pipeline reloads. If the pair already exists, the task
refreshes PATENT_TITLE/PATENT_ORG_NAME and sets `is_new = 1`. If the pair is
missing, the task inserts a new row with `is_new = 1`.

Rows missing PATENT_ID or PROJECT_ID are skipped because they cannot be safely
matched on a rerun.
"""

# Reference: D_grant/init_7_upload_patents_to_mysql.py

TABLE_NAME = "grant_patent"

GRANT_PATENT_INSERT_SQL = """
    INSERT INTO `grant_patent` (`PATENT_ID`, `PROJECT_ID`, `PATENT_TITLE`, `PATENT_ORG_NAME`, `is_new`)
    VALUES (%s, %s, %s, %s, %s)
"""

GRANT_PATENT_UPDATE_SQL = """
    UPDATE `grant_patent`
    SET `PATENT_TITLE` = %s,
        `PATENT_ORG_NAME` = %s,
        `is_new` = 1
    WHERE `PATENT_ID` = %s
      AND `PROJECT_ID` = %s
"""


@dataclass(frozen=True)
class PatentField:
    """Map one patent CSV column to one grant_patent database column."""

    csv_column: str
    db_column: str


PATENT_FIELDS: Tuple[PatentField, ...] = (
    PatentField("PATENT_ID", "PATENT_ID"),
    PatentField("PROJECT_ID", "PROJECT_ID"),
    PatentField("PATENT_TITLE", "PATENT_TITLE"),
    PatentField("PATENT_ORG_NAME", "PATENT_ORG_NAME"),
)

PATENT_ID_FIELD_INDEX = next(index for index, field in enumerate(PATENT_FIELDS) if field.db_column == "PATENT_ID")
PROJECT_ID_FIELD_INDEX = next(index for index, field in enumerate(PATENT_FIELDS) if field.db_column == "PROJECT_ID")
PATENT_TITLE_FIELD_INDEX = next(index for index, field in enumerate(PATENT_FIELDS) if field.db_column == "PATENT_TITLE")
PATENT_ORG_NAME_FIELD_INDEX = next(index for index, field in enumerate(PATENT_FIELDS) if field.db_column == "PATENT_ORG_NAME")


class GrantPatentUploadTask(GrantPipelineBase):
    """Upload downloaded NIH RePORTER patent rows into MySQL grant_patent."""

    def __init__(self, patents_dir: Optional[os.PathLike] = None, batch_size: Optional[int] = None):
        super().__init__(init_mysql=True, init_memgraph=False)

        self.patents_dir = Path(patents_dir or self.DEFAULT_PATENTS_DIR).expanduser().resolve()
        self.batch_size = batch_size if batch_size is not None else self.DEFAULT_BATCH_SIZE


    def find_new_data(self, gard_node) -> None:
        self.logger.info("GrantPatentUploadTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Read patent CSV files and insert/update rows."""

        from utils.tools import detect_file_encoding

        start_time = time.time()
        summary = {
            "files_processed": 0,
            "rows_seen": 0,
            "rows_inserted": 0,
            "rows_updated": 0,
            "rows_skipped_missing_key": 0,
            "failed_files": 0,
        }
        cursor = None

        try:
            if self.batch_size <= 0:
                raise ValueError("batch_size must be greater than 0")

            # Patent titles can be long. Raise the CSV field limit once so a
            # large title cell does not fail parsing before MySQL receives it.
            self._raise_csv_field_size_limit()

            if not self.patents_dir.is_dir():
                raise FileNotFoundError(f"Patent CSV directory does not exist: {self.patents_dir}")

            csv_files = sorted(self.patents_dir.glob("*.[Cc][Ss][Vv]"))

            if not csv_files:
                raise FileNotFoundError(f"No patent CSV files found in: {self.patents_dir}")

            cursor = self.mysql.cursor()

            for csv_file in csv_files:
                try:
                    detected_encoding, confidence = detect_file_encoding(csv_file)
                    encoding = self._normalize_detected_encoding(detected_encoding)
                    file_summary = {
                        "rows_seen": 0,
                        "rows_inserted": 0,
                        "rows_updated": 0,
                        "rows_skipped_missing_key": 0,
                    }
                    row_batch: List[Tuple[Any, ...]] = []

                    self.logger.info(f"Processing {csv_file.name}: encoding={detected_encoding}, confidence={confidence:.2%}, read_as={encoding}")

                    with csv_file.open("r", newline="", encoding=encoding, errors="replace") as file_obj:
                        reader = csv.DictReader(file_obj)
                        self._validate_csv_headers(csv_file.name, reader.fieldnames, PATENT_FIELDS)

                        for row_number, row in enumerate(reader, start=2):
                            values = self._build_row_values(csv_file.name, row_number, row, PATENT_FIELDS)
                            patent_id = values[PATENT_ID_FIELD_INDEX]
                            project_id = values[PROJECT_ID_FIELD_INDEX]

                            if patent_id is None or project_id is None:
                                file_summary["rows_skipped_missing_key"] += 1
                                continue

                            row_batch.append(values)
                            file_summary["rows_seen"] += 1

                            if len(row_batch) >= self.batch_size:
                                self._flush_row_batch(cursor, row_batch, file_summary)
                                self.mysql.commit()
                                row_batch.clear()

                    if row_batch:
                        self._flush_row_batch(cursor, row_batch, file_summary)
                        self.mysql.commit()
                        row_batch.clear()

                    summary["files_processed"] += 1

                    for key in ("rows_seen", "rows_inserted", "rows_updated", "rows_skipped_missing_key"):
                        summary[key] += file_summary[key]

                    self.logger.info(f"Finished {csv_file.name}: {file_summary}")

                except Exception:
                    summary["failed_files"] += 1
                    self.mysql.rollback()
                    self.logger.exception(f"Failed to upload grant patent file={csv_file.name}, file_summary={file_summary}. Rolled back this file and continuing.")
                    continue

            self.logger.info(f"Completed grant patent upload. Summary={summary}")

        except Exception:
            self.logger.exception(f"GrantPatentUploadTask failed. patents_dir={self.patents_dir}, summary={summary}")
            raise

        finally:
            if cursor:
                cursor.close()

            hours, minutes, seconds = _time_hms(time.time() - start_time)
            self.logger.info(f"Total time elapsed: {hours} hours, {minutes} minutes, {seconds} seconds")

            ''' Explicitly close all db connections. '''
            self.close()


    def _flush_row_batch(self, cursor: Any, row_batch: List[Tuple[Any, ...]], summary: Dict[str, int]) -> None:
        """Insert missing patent rows and refresh existing rows."""

        # grant_patent has no unique key for `(PATENT_ID, PROJECT_ID)`.
        # Deduping and checking existing keys prevents duplicate rows when this
        # annual alert task is rerun against the same Patents.csv export.
        deduped_rows = self._dedupe_rows_by_patent_key(row_batch)
        existing_keys = self._get_existing_patent_keys(cursor, deduped_rows)
        insert_values = []
        update_values = []

        for row_values in deduped_rows:
            patent_id = row_values[PATENT_ID_FIELD_INDEX]
            project_id = row_values[PROJECT_ID_FIELD_INDEX]
            patent_title = row_values[PATENT_TITLE_FIELD_INDEX]
            patent_org_name = row_values[PATENT_ORG_NAME_FIELD_INDEX]
            key = (patent_id, project_id)

            if key in existing_keys:
                update_values.append((patent_title, patent_org_name, patent_id, project_id))
            else:
                insert_values.append((patent_id, project_id, patent_title, patent_org_name, 1))

        if update_values:
            cursor.executemany(GRANT_PATENT_UPDATE_SQL, update_values)
            summary["rows_updated"] += len(update_values)

        if insert_values:
            cursor.executemany(GRANT_PATENT_INSERT_SQL, insert_values)
            summary["rows_inserted"] += len(insert_values)


    def _dedupe_rows_by_patent_key(self, row_batch: List[Tuple[Any, ...]]) -> List[Tuple[Any, ...]]:
        """Keep the last row when a CSV repeats the same patent/project pair."""

        rows_by_patent_key = {}

        for row_values in row_batch:
            key = (row_values[PATENT_ID_FIELD_INDEX], row_values[PROJECT_ID_FIELD_INDEX])
            rows_by_patent_key[key] = row_values

        return list(rows_by_patent_key.values())


    def _get_existing_patent_keys(self, cursor: Any, rows: Sequence[Tuple[Any, ...]]) -> set:
        """Return existing `(PATENT_ID, PROJECT_ID)` keys for one batch."""

        if not rows:
            return set()

        project_ids = sorted({row[PROJECT_ID_FIELD_INDEX] for row in rows if row[PROJECT_ID_FIELD_INDEX] is not None})

        if not project_ids:
            return set()

        # grant_patent has an index on PROJECT_ID. Fetch by the batched project
        # values, then use the returned PATENT_ID values for exact idempotent
        # insert/update decisions.
        placeholders = ", ".join(["%s"] * len(project_ids))
        query = f"""
            SELECT DISTINCT `PATENT_ID`, `PROJECT_ID`
            FROM `{TABLE_NAME}`
            WHERE `PROJECT_ID` IN ({placeholders})
        """

        cursor.execute(query, tuple(project_ids))

        return {(row[0], row[1]) for row in cursor.fetchall()}


    def _build_row_values(self, filename: str, row_number: int, row: Dict[str, Any], fields: Sequence[PatentField]) -> Tuple[Any, ...]:
        """Convert one CSV row into a normalized tuple matching PATENT_FIELDS."""

        from utils.tools import _normalize_tuple

        values = []

        for field in fields:
            value = row.get(field.csv_column)

            if isinstance(value, str):
                value = value.strip()

            if value == "":
                value = None

            values.append(value)

        # ID, CREATED, and is_new are database/task-owned columns. PATENT_ID is
        # intentionally kept as text because the schema and graph initializer
        # treat it as an identifier, not a numeric measurement.
        return _normalize_tuple(tuple(values))


    def _validate_csv_headers(self, filename: str, fieldnames: Optional[List[str]], fields: Sequence[PatentField]) -> None:
        """Validate that the CSV has every required patent column."""

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

    task = GrantPatentUploadTask()
    task.process_new_data()

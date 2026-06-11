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

Each patent row is identified by the unique key `(PATENT_ID, PROJECT_ID)`. A
patent can be linked to more than one NIH project, so PATENT_ID alone is not
enough for idempotent alert-pipeline reloads. This task deduplicates the input
files on that pair, preferring reasonable title/organization values over blank
or placeholder values, then uses the `uq_grant_patent_patent_project` unique key
to insert new pairs or refresh changed title/organization values with
`is_new = 1`.

Rows missing PATENT_ID or PROJECT_ID are skipped because they cannot be safely
matched on a rerun.
"""

# Reference: D_grant/init_7_upload_patents_to_mysql.py

#
# SELECT DISTINCT DATE(created) FROM rdas_db.grant_patent;
#

TABLE_NAME = "grant_patent"

UNIQUE_PATENT_PROJECT_KEY_NAME = "uq_grant_patent_patent_project"
UNIQUE_PATENT_PROJECT_KEY_COLUMNS = ("PATENT_ID", "PROJECT_ID")

NO_REASONABLE_TEXT_VALUES = {
    "N/A",
    "NA",
    "NONE",
    "NULL",
    "UNKNOWN",
    "UNAVAILABLE",
    "NOT APPLICABLE",
    "NOT AVAILABLE",
}

GRANT_PATENT_UPSERT_SQL = """
    INSERT INTO `grant_patent` (`PATENT_ID`, `PROJECT_ID`, `PATENT_TITLE`, `PATENT_ORG_NAME`, `is_new`)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        `is_new` = IF(
            (
                VALUES(`PATENT_TITLE`) IS NOT NULL
                AND NOT (`PATENT_TITLE` <=> VALUES(`PATENT_TITLE`))
            )
            OR (
                VALUES(`PATENT_ORG_NAME`) IS NOT NULL
                AND NOT (`PATENT_ORG_NAME` <=> VALUES(`PATENT_ORG_NAME`))
            ),
            1,
            `is_new`
        ),
        `PATENT_TITLE` = IF(
            VALUES(`PATENT_TITLE`) IS NOT NULL
            AND NOT (`PATENT_TITLE` <=> VALUES(`PATENT_TITLE`)),
            VALUES(`PATENT_TITLE`),
            `PATENT_TITLE`
        ),
        `PATENT_ORG_NAME` = IF(
            VALUES(`PATENT_ORG_NAME`) IS NOT NULL
            AND NOT (`PATENT_ORG_NAME` <=> VALUES(`PATENT_ORG_NAME`)),
            VALUES(`PATENT_ORG_NAME`),
            `PATENT_ORG_NAME`
        )
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
        """Read patent CSV files and upsert changed patent/project rows."""

        from utils.tools import detect_file_encoding

        start_time = time.time()
        summary = {
            "files_processed": 0,
            "rows_seen": 0,
            "rows_deduped": 0,
            "rows_submitted": 0,
            "mysql_rows_affected": 0,
            "rows_skipped_missing_key": 0,
            "duplicate_patent_project_rows": 0,
            "duplicate_title_replacements": 0,
            "duplicate_org_replacements": 0,
            "duplicate_title_conflicts_kept": 0,
            "duplicate_org_conflicts_kept": 0,
            "failed_files": 0,
        }
        cursor = None
        rows_by_patent_key: Dict[Tuple[Any, Any], Tuple[Any, ...]] = {}

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

            for csv_file in csv_files:
                file_summary = {
                    "rows_seen": 0,
                    "rows_skipped_missing_key": 0,
                    "duplicate_patent_project_rows": 0,
                    "duplicate_title_replacements": 0,
                    "duplicate_org_replacements": 0,
                    "duplicate_title_conflicts_kept": 0,
                    "duplicate_org_conflicts_kept": 0,
                }

                try:
                    detected_encoding, confidence = detect_file_encoding(csv_file)
                    encoding = self._normalize_detected_encoding(detected_encoding)

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

                            file_summary["rows_seen"] += 1
                            self._merge_row_into_patent_map(rows_by_patent_key, values, file_summary)

                    summary["files_processed"] += 1

                    for key in (
                        "rows_seen",
                        "rows_skipped_missing_key",
                        "duplicate_patent_project_rows",
                        "duplicate_title_replacements",
                        "duplicate_org_replacements",
                        "duplicate_title_conflicts_kept",
                        "duplicate_org_conflicts_kept",
                    ):
                        summary[key] += file_summary[key]

                    self.logger.info(f"Finished {csv_file.name}: {file_summary}")

                except Exception:
                    summary["failed_files"] += 1
                    self.logger.exception(f"Failed to parse grant patent file={csv_file.name}, file_summary={file_summary}. Continuing without writing rows.")
                    continue

            if summary["failed_files"] > 0:
                raise RuntimeError(f"Failed to parse {summary['failed_files']} patent CSV file(s); no grant_patent rows were written.")

            cursor = self.mysql.cursor()
            self._validate_unique_patent_project_key(cursor)
            deduped_rows = list(rows_by_patent_key.values())
            summary["rows_deduped"] = len(deduped_rows)

            for start_index in range(0, len(deduped_rows), self.batch_size):
                row_batch = deduped_rows[start_index:start_index + self.batch_size]
                self._flush_row_batch(cursor, row_batch, summary)
                self.mysql.commit()

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
        """Upsert new or changed patent rows after full-file deduplication."""

        insert_values = []

        for row_values in row_batch:
            patent_id = row_values[PATENT_ID_FIELD_INDEX]
            project_id = row_values[PROJECT_ID_FIELD_INDEX]
            patent_title = row_values[PATENT_TITLE_FIELD_INDEX]
            patent_org_name = row_values[PATENT_ORG_NAME_FIELD_INDEX]
            insert_values.append((patent_id, project_id, patent_title, patent_org_name, 1))

        if insert_values:
            cursor.executemany(GRANT_PATENT_UPSERT_SQL, insert_values)
            summary["rows_submitted"] += len(insert_values)
            summary["mysql_rows_affected"] += cursor.rowcount


    def _merge_row_into_patent_map(self, rows_by_patent_key: Dict[Tuple[Any, Any], Tuple[Any, ...]], row_values: Tuple[Any, ...], summary: Dict[str, int]) -> None:
        """Merge one normalized CSV row into the full input patent map."""

        key = (row_values[PATENT_ID_FIELD_INDEX], row_values[PROJECT_ID_FIELD_INDEX])
        existing_values = rows_by_patent_key.get(key)

        if existing_values is None:
            rows_by_patent_key[key] = row_values
            return

        summary["duplicate_patent_project_rows"] += 1
        merged_values = list(existing_values)

        # Duplicate NIH rows often differ only because one copy has a blank
        # organization. Keep the useful text when the other copy is missing or
        # placeholder-like, but do not guess between two different useful values.
        merge_rules = (
            (PATENT_TITLE_FIELD_INDEX, "duplicate_title_replacements", "duplicate_title_conflicts_kept"),
            (PATENT_ORG_NAME_FIELD_INDEX, "duplicate_org_replacements", "duplicate_org_conflicts_kept"),
        )

        for field_index, replacement_key, conflict_key in merge_rules:
            existing_value = merged_values[field_index]
            new_value = row_values[field_index]
            existing_is_reasonable = self._is_reasonable_text_value(existing_value)
            new_is_reasonable = self._is_reasonable_text_value(new_value)

            if not existing_is_reasonable and new_is_reasonable:
                merged_values[field_index] = new_value
                summary[replacement_key] += 1

            elif existing_is_reasonable and new_is_reasonable and existing_value != new_value:
                summary[conflict_key] += 1

        rows_by_patent_key[key] = tuple(merged_values)


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

            if field.db_column in {"PATENT_TITLE", "PATENT_ORG_NAME"} and not self._is_reasonable_text_value(value):
                value = None

            values.append(value)

        # ID, CREATED, and is_new are database/task-owned columns. PATENT_ID is
        # intentionally kept as text because the schema and graph initializer
        # treat it as an identifier, not a numeric measurement.
        return _normalize_tuple(tuple(values))


    def _is_reasonable_text_value(self, value: Any) -> bool:
        """Return True when text is useful enough to load over a blank value."""

        if value is None:
            return False

        text_value = str(value).strip()

        if not text_value:
            return False

        return text_value.upper() not in NO_REASONABLE_TEXT_VALUES


    def _validate_unique_patent_project_key(self, cursor: Any) -> None:
        """Confirm MySQL can route duplicate patent/project pairs to UPDATE."""

        cursor.execute(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s
              AND INDEX_NAME = %s
              AND NON_UNIQUE = 0
            ORDER BY SEQ_IN_INDEX
            """,
            (TABLE_NAME, UNIQUE_PATENT_PROJECT_KEY_NAME),
        )

        index_columns = tuple(row[0] for row in cursor.fetchall())

        if index_columns != UNIQUE_PATENT_PROJECT_KEY_COLUMNS:
            raise ValueError(
                f"{TABLE_NAME}.{UNIQUE_PATENT_PROJECT_KEY_NAME} must be a unique key on "
                f"({', '.join(UNIQUE_PATENT_PROJECT_KEY_COLUMNS)}); found columns={index_columns or 'none'}"
            )


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

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
Upload NIH RePORTER clinical study CSV files into grant_clinical_study.

This task is the alert-pipeline version of the full clinical study initializer.
It accepts `years` so main_grant.py can call it with the same annual interface
as the other grant upload tasks. The NIH clinical study export itself is not
year-specific, and the grant_clinical_study table has no YEAR column, so the
provided years are used as the annual run context rather than as a row filter.

Each clinical study row is identified by `(core_project_num, nctid)`. If that
pair already exists, the task refreshes study/study_status and sets `is_new = 1`.
If the pair is missing, the task inserts a new row with `is_new = 1`. Rows
without either part of the identity key are skipped because they cannot be
safely refreshed on reruns.
"""

# Reference: D_grant/init_6_upload_clinicalstudies_to_mysql_db.py

TABLE_NAME = "grant_clinical_study"

GRANT_CLINICAL_STUDY_INSERT_SQL = """
    INSERT INTO `grant_clinical_study` (`core_project_num`, `nctid`, `study`, `study_status`, `is_new`)
    VALUES (%s, %s, %s, %s, %s)
"""

GRANT_CLINICAL_STUDY_UPDATE_SQL = """
    UPDATE `grant_clinical_study`
    SET `study` = %s,
        `study_status` = %s,
        `is_new` = 1
    WHERE `core_project_num` = %s
      AND `nctid` = %s
"""


@dataclass(frozen=True)
class ClinicalStudyField:
    """Map one clinical study CSV column to one grant_clinical_study column."""

    csv_column: str
    db_column: str


CLINICAL_STUDY_FIELDS: Tuple[ClinicalStudyField, ...] = (
    ClinicalStudyField("Core Project Number", "core_project_num"),
    ClinicalStudyField("ClinicalTrials.gov ID", "nctid"),
    ClinicalStudyField("Study", "study"),
    ClinicalStudyField("Study Status", "study_status"),
)

CORE_PROJECT_NUM_FIELD_INDEX = next(index for index, field in enumerate(CLINICAL_STUDY_FIELDS) if field.db_column == "core_project_num")
NCTID_FIELD_INDEX = next(index for index, field in enumerate(CLINICAL_STUDY_FIELDS) if field.db_column == "nctid")
STUDY_FIELD_INDEX = next(index for index, field in enumerate(CLINICAL_STUDY_FIELDS) if field.db_column == "study")
STUDY_STATUS_FIELD_INDEX = next(index for index, field in enumerate(CLINICAL_STUDY_FIELDS) if field.db_column == "study_status")


class GrantClinicalStudyUploadTask(GrantPipelineBase):
    """Upload downloaded NIH RePORTER clinical study rows into MySQL."""

    def __init__(self, years: Sequence[int], clinical_studies_dir: Optional[os.PathLike] = None, batch_size: Optional[int] = None):
        super().__init__(init_mysql=True, init_memgraph=False)

        self.years = self._resolve_years(years, required=True)
        self.clinical_studies_dir = Path(clinical_studies_dir or self.DEFAULT_CLINICAL_STUDIES_DIR).expanduser().resolve()
        self.batch_size = batch_size if batch_size is not None else self.DEFAULT_BATCH_SIZE


    def find_new_data(self, gard_node) -> None:
        self.logger.info("GrantClinicalStudyUploadTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Read clinical study CSV files and insert/update rows."""

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

            # Keep this consistent with the other grant CSV tasks. 
            # Clinical study titles are usually small, but larger values should not fail in Python before MySQL receives the row.
            self._raise_csv_field_size_limit()

            if not self.clinical_studies_dir.is_dir():
                raise FileNotFoundError(f"Clinical study CSV directory does not exist: {self.clinical_studies_dir}")

            csv_files = sorted(self.clinical_studies_dir.glob("*.[Cc][Ss][Vv]"))

            if not csv_files:
                raise FileNotFoundError(f"No clinical study CSV files found for years={self.years} in: {self.clinical_studies_dir}")

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

                    self.logger.info(
                        f"Processing {csv_file.name}: years={self.years}, "
                        f"encoding={detected_encoding}, confidence={confidence:.2%}, read_as={encoding}"
                    )

                    with csv_file.open("r", newline="", encoding=encoding, errors="replace") as file_obj:
                        reader = csv.DictReader(file_obj)
                        self._validate_csv_headers(csv_file.name, reader.fieldnames, CLINICAL_STUDY_FIELDS)

                        for row_number, row in enumerate(reader, start=2):
                            values = self._build_row_values(csv_file.name, row_number, row, CLINICAL_STUDY_FIELDS)
                            core_project_num = values[CORE_PROJECT_NUM_FIELD_INDEX]
                            nctid = values[NCTID_FIELD_INDEX]

                            if core_project_num is None or nctid is None:
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
                    self.logger.exception(f"Failed to upload grant clinical study file={csv_file.name}, years={self.years}, file_summary={file_summary}. Rolled back this file and continuing.")
                    continue

            self.logger.info(f"Completed grant clinical study upload. Summary={summary}")

        except Exception:
            self.logger.exception(f"GrantClinicalStudyUploadTask failed. years={self.years}, clinical_studies_dir={self.clinical_studies_dir}, summary={summary}")
            raise

        finally:
            if cursor:
                cursor.close()

            hours, minutes, seconds = _time_hms(time.time() - start_time)
            self.logger.info(f"Total time elapsed: {hours} hours, {minutes} minutes, {seconds} seconds")

            ''' Explicitly close all db connections. '''
            self.close()


    def _flush_row_batch(self, cursor: Any, row_batch: List[Tuple[Any, ...]], summary: Dict[str, int]) -> None:
        """Insert missing clinical study rows and refresh existing rows."""

        # The table has no unique key for `(core_project_num, nctid)`. Deduping
        # and checking existing keys avoids duplicate rows on reruns while still
        # marking refreshed rows with is_new=1 for downstream alert steps.
        deduped_rows = self._dedupe_rows_by_clinical_study_key(row_batch)
        existing_keys = self._get_existing_clinical_study_keys(cursor, deduped_rows)
        insert_values = []
        update_values = []

        for row_values in deduped_rows:
            core_project_num = row_values[CORE_PROJECT_NUM_FIELD_INDEX]
            nctid = row_values[NCTID_FIELD_INDEX]
            study = row_values[STUDY_FIELD_INDEX]
            study_status = row_values[STUDY_STATUS_FIELD_INDEX]
            key = (core_project_num, nctid)

            if key in existing_keys:
                update_values.append((study, study_status, core_project_num, nctid))
            else:
                insert_values.append((core_project_num, nctid, study, study_status, 1))

        if update_values:
            cursor.executemany(GRANT_CLINICAL_STUDY_UPDATE_SQL, update_values)
            summary["rows_updated"] += len(update_values)

        if insert_values:
            cursor.executemany(GRANT_CLINICAL_STUDY_INSERT_SQL, insert_values)
            summary["rows_inserted"] += len(insert_values)


    def _dedupe_rows_by_clinical_study_key(self, row_batch: List[Tuple[Any, ...]]) -> List[Tuple[Any, ...]]:
        """Keep the last row when a CSV repeats the same core project/NCT ID."""

        rows_by_clinical_study_key = {}

        for row_values in row_batch:
            key = (row_values[CORE_PROJECT_NUM_FIELD_INDEX], row_values[NCTID_FIELD_INDEX])
            rows_by_clinical_study_key[key] = row_values

        return list(rows_by_clinical_study_key.values())


    def _get_existing_clinical_study_keys(self, cursor: Any, rows: Sequence[Tuple[Any, ...]]) -> set:
        """Return existing `(core_project_num, nctid)` keys for one batch."""

        if not rows:
            return set()

        core_project_nums = sorted({row[CORE_PROJECT_NUM_FIELD_INDEX] for row in rows if row[CORE_PROJECT_NUM_FIELD_INDEX] is not None})

        if not core_project_nums:
            return set()

        # grant_clinical_study has an index on core_project_num. Fetch by the
        # batched core project values, then use the returned nctid values for
        # exact idempotent insert/update decisions.
        placeholders = ", ".join(["%s"] * len(core_project_nums))
        query = f"""
            SELECT DISTINCT `core_project_num`, `nctid`
            FROM `{TABLE_NAME}`
            WHERE `core_project_num` IN ({placeholders})
        """

        cursor.execute(query, tuple(core_project_nums))

        return {(row[0], row[1]) for row in cursor.fetchall()}


    def _build_row_values(self, filename: str, row_number: int, row: Dict[str, Any], fields: Sequence[ClinicalStudyField]) -> Tuple[Any, ...]:
        """Convert one CSV row into a normalized tuple matching CLINICAL_STUDY_FIELDS."""

        from utils.tools import _normalize_tuple

        values = []

        for field in fields:
            value = row.get(field.csv_column)

            if isinstance(value, str):
                value = value.strip()

            if value == "":
                value = None

            values.append(value)

        # ID, processed, created, and is_new are database/task-owned columns.
        # The CSV supplies only the core project, NCT ID, title, and status.
        return _normalize_tuple(tuple(values))


    def _validate_csv_headers(self, filename: str, fieldnames: Optional[List[str]], fields: Sequence[ClinicalStudyField]) -> None:
        """Validate that the CSV has every required clinical study column."""

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

    task = GrantClinicalStudyUploadTask(years=[date.today().year - 1])
    task.process_new_data()

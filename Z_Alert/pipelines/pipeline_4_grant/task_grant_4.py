import csv
import os
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pipelines.pipeline_4_grant.grant_base import DEFAULT_BATCH_SIZE, DEFAULT_PUBLICATIONS_DIR, GrantPipelineBase
from utils.tools import _time_hms


"""
Upload NIH RePORTER publication CSV files into grant_publication for the grant alert pipeline.

The initializer appends every publication row during a full load. This alert
task is year-scoped and idempotent by PMID: existing PMID rows are updated and
marked is_new = 1, while missing PMID rows are inserted with is_new = 1.
"""

# Reference: D_grant/init_3_upload_publications_to_mysql_db.py

TABLE_NAME = "grant_publication"


@dataclass(frozen=True)
class PublicationField:
    """Map one publication CSV column to one grant_publication database column."""

    csv_column: str
    db_column: str
    converter: Optional[str] = None


PUBLICATION_FIELDS: Tuple[PublicationField, ...] = (
    PublicationField("AFFILIATION", "AFFILIATION"),
    PublicationField("AUTHOR_LIST", "AUTHOR_LIST"),
    PublicationField("COUNTRY", "COUNTRY"),
    PublicationField("ISSN", "ISSN"),
    PublicationField("JOURNAL_ISSUE", "JOURNAL_ISSUE"),
    PublicationField("JOURNAL_TITLE", "JOURNAL_TITLE"),
    PublicationField("JOURNAL_TITLE_ABBR", "JOURNAL_TITLE_ABBR"),
    PublicationField("JOURNAL_VOLUME", "JOURNAL_VOLUME"),
    PublicationField("LANG", "LANG"),
    PublicationField("PAGE_NUMBER", "PAGE_NUMBER"),
    PublicationField("PMC_ID", "PMC_ID", "pmc_id"),
    PublicationField("PMID", "PMID", "int"),
    PublicationField("PUB_DATE", "PUB_DATE"),
    PublicationField("PUB_TITLE", "PUB_TITLE"),
    PublicationField("PUB_YEAR", "PUB_YEAR", "int"),
)

PMID_FIELD_INDEX = next(index for index, field in enumerate(PUBLICATION_FIELDS) if field.db_column == "PMID")


class GrantPublicationUploadTask(GrantPipelineBase):
    """Upsert downloaded NIH RePORTER publication CSV rows into MySQL grant_publication."""

    def __init__(self, years: Sequence[int], publications_dir: os.PathLike = DEFAULT_PUBLICATIONS_DIR, batch_size: int = DEFAULT_BATCH_SIZE):
        super().__init__(init_mysql=True, init_memgraph=False)

        self.years = self._resolve_years(years, required=True)
        self.publications_dir = Path(publications_dir).expanduser().resolve()
        self.batch_size = batch_size


    def find_new_data(self, gard_node) -> None:
        self.logger.info("GrantPublicationUploadTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Read publication CSV files for the configured years and upsert rows by PMID."""

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

            self._raise_csv_field_size_limit()

            if not self.publications_dir.is_dir():
                raise FileNotFoundError(f"Publication CSV directory does not exist: {self.publications_dir}")

            csv_files = []

            for csv_file in sorted(self.publications_dir.glob("RePORTER_PUB_C_FY*.[Cc][Ss][Vv]")):

                year = self._get_reporter_publication_year(csv_file.name)

                if year in self.years:
                    csv_files.append((csv_file, year))

            if not csv_files:
                raise FileNotFoundError(f"No RePORTER publication CSV files found for years={self.years} in: {self.publications_dir}")

            cursor = self.mysql.cursor()
            insert_sql, update_sql = self._build_upsert_sql(PUBLICATION_FIELDS)

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
                        self._validate_csv_headers(csv_file.name, reader.fieldnames, PUBLICATION_FIELDS)

                        for row_number, row in enumerate(reader, start=2):

                            values = self._build_row_values(csv_file.name, row_number, row, PUBLICATION_FIELDS)
                            pmid = values[PMID_FIELD_INDEX]

                            if pmid is None:
                                file_summary["rows_skipped_missing_pmid"] += 1
                                continue

                            row_batch.append(values)
                            file_summary["rows_seen"] += 1

                            if len(row_batch) >= self.batch_size:
                                
                                self._flush_row_batch(cursor, insert_sql, update_sql, row_batch, file_summary)
                                self.mysql.commit()
                                row_batch.clear()

                    if row_batch:
                        self._flush_row_batch(cursor, insert_sql, update_sql, row_batch, file_summary)
                        self.mysql.commit()
                        row_batch.clear()

                    summary["files_processed"] += 1

                    for key in ("rows_seen", "rows_inserted", "rows_updated", "rows_skipped_missing_pmid"):
                        summary[key] += file_summary[key]

                    self.logger.info(f"Finished {csv_file.name}: {file_summary}")

                except Exception:
                    summary["failed_files"] += 1
                    self.mysql.rollback()
                    self.logger.exception(f"Failed to upload grant publication file={csv_file.name}, year={year}, file_summary={file_summary}. Rolled back this file.")
                    raise

            self.logger.info(f"Completed grant publication upload. Summary={summary}")

        except Exception:
            self.logger.exception(f"GrantPublicationUploadTask failed. years={self.years}, publications_dir={self.publications_dir}, summary={summary}")
            raise

        finally:
            if cursor:
                cursor.close()

            hours, minutes, seconds = _time_hms(time.time() - start_time)
            self.logger.info(f"Total time elapsed: {hours} hours, {minutes} minutes, {seconds} seconds")

            ''' Explicitly close all db connections. '''
            self.close()


    def _build_upsert_sql(self, fields: Sequence[PublicationField]) -> Tuple[str, str]:
        """Build INSERT and UPDATE statements for grant_publication."""

        db_columns = [f"`{field.db_column}`" for field in fields]
        insert_columns = db_columns + ["`is_new`"]
        insert_placeholders = ["%s"] * len(insert_columns)
        update_assignments = [
            f"`{field.db_column}` = %s"
            for field in fields
            if field.db_column != "PMID"
        ]
        update_assignments.append("`is_new` = 1")
        insert_sql = f"""
            INSERT INTO `{TABLE_NAME}` ({", ".join(insert_columns)})
            VALUES ({", ".join(insert_placeholders)})
        """
        update_sql = f"""
            UPDATE `{TABLE_NAME}`
            SET {", ".join(update_assignments)}
            WHERE `PMID` = %s
        """

        return insert_sql, update_sql


    def _flush_row_batch(self, cursor: Any, insert_sql: str, update_sql: str, row_batch: List[Tuple[Any, ...]], summary: Dict[str, int]) -> None:
        """Insert missing PMID rows and update existing PMID rows."""

        deduped_rows = self._dedupe_rows_by_pmid(row_batch)
        existing_pmids = self._get_existing_pmids(cursor, [row[PMID_FIELD_INDEX] for row in deduped_rows])
        insert_values = []
        update_values = []

        for row_values in deduped_rows:
            pmid = row_values[PMID_FIELD_INDEX]

            if pmid in existing_pmids:
                update_values.append(self._build_update_values(row_values, pmid))
            else:
                insert_values.append(tuple(row_values) + (1,))

        if update_values:
            cursor.executemany(update_sql, update_values)
            summary["rows_updated"] += len(update_values)

        if insert_values:
            cursor.executemany(insert_sql, insert_values)
            summary["rows_inserted"] += len(insert_values)


    def _dedupe_rows_by_pmid(self, row_batch: List[Tuple[Any, ...]]) -> List[Tuple[Any, ...]]:
        """Keep the last row in the batch when a CSV repeats PMID."""

        rows_by_pmid = {}

        for row_values in row_batch:
            rows_by_pmid[row_values[PMID_FIELD_INDEX]] = row_values

        return list(rows_by_pmid.values())


    def _get_existing_pmids(self, cursor: Any, pmids: Sequence[Any]) -> set:
        """Return PMID values that already exist in grant_publication."""

        if not pmids:
            return set()

        placeholders = ", ".join(["%s"] * len(pmids))
        query = f"""
            SELECT DISTINCT PMID
            FROM `{TABLE_NAME}`
            WHERE PMID IN ({placeholders})
        """

        cursor.execute(query, tuple(pmids))

        return {row[0] for row in cursor.fetchall()}


    def _build_update_values(self, row_values: Tuple[Any, ...], pmid: Any) -> Tuple[Any, ...]:
        """Build an UPDATE value tuple, excluding PMID from SET."""

        values = [
            value
            for field, value in zip(PUBLICATION_FIELDS, row_values)
            if field.db_column != "PMID"
        ]
        values.append(pmid)

        return tuple(values)


    def _build_row_values(self, filename: str, row_number: int, row: Dict[str, Any], fields: Sequence[PublicationField]) -> Tuple[Any, ...]:
        """Convert one CSV row into a normalized tuple matching fields."""

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
                    elif field.converter == "pmc_id":
                        # PMC_ID can appear as a decimal-looking value in NIH
                        # exports, so preserve the initializer behavior here.
                        converted_value = convert_to_int(value, allow_decimal=True)
                    else:
                        converted_value = value

            except ValueError as e:
                raise ValueError(f"{filename} row {row_number}, column {field.csv_column}: {e}") from e

            values.append(converted_value)

        return _normalize_tuple(tuple(values))


    def _validate_csv_headers(self, filename: str, fieldnames: Optional[List[str]], fields: Sequence[PublicationField]) -> None:
        """Validate that the CSV has every required publication column."""

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

    task = GrantPublicationUploadTask(years=[date.today().year - 1])
    task.process_new_data()

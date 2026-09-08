"""
Step 1: load GARD nomenclature rows into MySQL table `gard`.

This task is the MySQL side of the GARD update flow. It reads the raw
nomenclature CSV that is already present under `gard_update/data/2025` and
stores one row per source label in the existing `gard` table.
"""

import csv
import os
import sys
from typing import Any, Dict, List, Tuple

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "../..")),
    os.path.abspath(os.path.join(_dir, "../../..")),
])

from pipelines.gard_update.gard_common import GARD_NOMENCLATURE_FILE, count_csv_rows, resolve_data_file, validate_batch_size
from pipelines.pipeline_base import PipelineBase
from utils.tools import _na, _try_parse_int


INSERT_GARD_SQL = """
    INSERT INTO gard (
        GardID,
        MONDO_ID,
        Label,
        Label_Predicate_Type,
        Label_Predicate_Mapping,
        Label_Predicate,
        Label_Xref,
        Label_Source,
        ORPHA_Code,
        Disorder_Type,
        Classification_Level
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
 

class GardMysqlNomenclatureLoadTask(PipelineBase):
    """Load refreshed raw GARD nomenclature data into MySQL."""

    def __init__(self, data_file: Any = GARD_NOMENCLATURE_FILE, batch_size: int = 500, clear_existing: bool = False, allow_append: bool = False):

        super().__init__(init_mysql=True, init_memgraph=False)
        self.data_file = resolve_data_file(data_file)
        self.batch_size = validate_batch_size(batch_size)
        self.clear_existing = clear_existing
        self.allow_append = allow_append


    def find_new_data(self, gard_node) -> None:
        raise NotImplementedError("GardMysqlNomenclatureLoadTask does not implement find_new_data().")


    def process_new_data(self) -> None:
        """
        Insert GARD nomenclature rows into MySQL in batches.

        The `gard` table does not have a uniqueness constraint on `GardID` plus
        label metadata, so a blind re-run can duplicate rows. For that reason,
        this task skips loading when the table is already populated unless the
        caller explicitly requests a full rebuild or an append.
        """

        cursor = None

        try:
            csv_row_count = count_csv_rows(self.data_file)
            existing_row_count = self._get_existing_row_count()

            self.logger.info(f"GARD nomenclature input file: {self.data_file}")
            self.logger.info(f"CSV rows available={csv_row_count}; existing MySQL gard rows={existing_row_count}.")

            # Do nothing !!!
            if existing_row_count > 0 and not self.clear_existing and not self.allow_append:
                self.logger.info("Skipped MySQL gard load because the table already has rows. Set clear_existing=True for a rebuild or allow_append=True for an intentional append.")
                return

            cursor = self.mysql.cursor()

            if self.clear_existing:
                self.logger.info("Clearing existing MySQL gard rows with TRUNCATE TABLE gard.")
                cursor.execute("TRUNCATE TABLE gard")
                self.mysql.commit()

            inserted_count = self._insert_rows(cursor)
            self.logger.info(f"Completed MySQL GARD nomenclature load. Inserted rows={inserted_count}.")

        except Exception:
            self.logger.exception("GardMysqlNomenclatureLoadTask failed.")

            if self.mysql:
                self.mysql.rollback()

            raise

        finally:
            if cursor:
                cursor.close()

            self.close()


    def _get_existing_row_count(self) -> int:
        """Return the current number of rows in MySQL table `gard`."""

        cursor = self.mysql.cursor(dictionary=True)

        try:
            cursor.execute("SELECT COUNT(*) AS row_count FROM gard")
            row = cursor.fetchone() or {}
            return int(row.get("row_count") or 0)

        finally:
            cursor.close()


    def _insert_rows(self, cursor) -> int:
        """Stream the CSV and insert rows with executemany batches."""

        insert_values: List[Tuple[Any, ...]] = []
        inserted_count = 0

        with self.data_file.open("r", encoding="utf-8-sig", newline="") as csv_file:

            reader = csv.DictReader(csv_file)
            self.logger.info(f"GARD nomenclature CSV fields: {', '.join(reader.fieldnames or [])}")

            for row_index, row in enumerate(reader, start=2):
                try:
                    insert_values.append(self._build_insert_values(row))

                except KeyError as e:
                    self.logger.error(f"Skipping CSV row {row_index}; missing required column: {e}")
                    continue

                if len(insert_values) >= self.batch_size:
                    inserted_count = self._flush_insert_batch(cursor, insert_values, inserted_count)

            inserted_count = self._flush_insert_batch(cursor, insert_values, inserted_count)

        return inserted_count


    def _flush_insert_batch(self, cursor, insert_values: List[Tuple[Any, ...]], inserted_count: int) -> int:
        """Write one queued insert batch and return the updated total."""

        if not insert_values:
            return inserted_count

        batch_count = len(insert_values)
        cursor.executemany(INSERT_GARD_SQL, insert_values)
        self.mysql.commit()

        inserted_count += batch_count
        self.logger.info(f"Inserted MySQL gard rows={inserted_count}.")
        insert_values.clear()

        return inserted_count


    @staticmethod
    def _build_insert_values(row: Dict[str, Any]) -> Tuple[Any, ...]:
        """Convert one raw nomenclature CSV row into the `gard` insert tuple."""

        xref = (row["Label_Xref"] or "").strip("[]")
        label_predicate = (row["Label_Predicate"] or "").strip("[]")
        label_predicate_mapping = (row["Label_Predicate_Mapping"] or "").strip("[]")

        return (
            row["GARD_ID"],
            row["MONDO_ID"],
            row["Label"],
            row["Label_Predicate_Type"],
            label_predicate_mapping,
            label_predicate,
            xref,
            row["Label_Source"],
            _try_parse_int(row.get("ORPHA_Code")),
            _na(row.get("DisorderType")),
            _na(row.get("ClassificationLevel")),
        )

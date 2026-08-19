import os
import sys
import time
from typing import Any, Dict, List, Optional

import requests

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from pipelines.pipeline_2_clinical_trial.clinical_trial_study_change_handler import ClinicalTrialStudyChangeHandler


class ClinicalTrialExistingStudyUpdateChecker(PipelineBase):

    '''
    Re-check existing clinical_trial_unique NCT IDs against ClinicalTrials.gov.

    The normal discovery task only compares studies that appear in the disease
    name search window for updated GARD rows. This helper closes the gap for
    studies that are already known to RDAS by directly fetching every existing
    NCT ID and reusing ClinicalTrialStudyChangeHandler to update changed rows.
    '''

    DEFAULT_BATCH_SIZE = 100
    REQUEST_TIMEOUT_SECONDS = 10
    REQUEST_SLEEP_SECONDS = 0.0

    def __init__(self, batch_size: int = DEFAULT_BATCH_SIZE, nctid_limit: Optional[int] = None):

        '''
        Create the MySQL-backed helper and keep the API URL in one place.

        batch_size controls how many clinical_trial_unique rows are read per
        database batch. nctid_limit is only for manual test runs; the production
        runner leaves it as None so every existing NCT ID is checked.
        '''
        super().__init__(init_mysql=True, init_memgraph=False)

        self.batch_size = batch_size
        self.nctid_limit = nctid_limit
        self.clinical_trials_studies_api = (
            os.getenv("CLINICAL_TRIAL_STUDIES_API")
            or os.getenv("CLINICAL_TRAIL_STUDY_URL")
        )
        self.study_change_handler = ClinicalTrialStudyChangeHandler(self.mysql, self.logger)


    def find_new_data(self, gard_node) -> None:

        raise NotImplementedError("ClinicalTrialExistingStudyUpdateChecker does not implement find_new_data().")


    def process_new_data(self) -> None:

        '''
        Fetch and compare all existing NCT IDs stored in clinical_trial_unique.

        Progress is logged for each batch and each changed NCT ID is logged with
        its GARD/disease context and JSON differences. Unchanged rows are counted
        in the final summary without writing anything back to MySQL.
        '''
        if not self.clinical_trials_studies_api:
            self.logger.error("CLINICAL_TRIAL_STUDIES_API is not configured.")
            return

        total_nctids = self._count_existing_nctids()
        if self.nctid_limit is not None:
            total_nctids = min(total_nctids, self.nctid_limit)

        checked_count = 0
        unchanged_count = 0
        updated_count = 0
        inserted_count = 0
        failed_count = 0
        missing_context_count = 0

        self.logger.info(f"Starting existing ClinicalTrials.gov study update check for {total_nctids} NCTIDs.")

        for batch_num, rows in enumerate(self._iter_existing_nctid_batches(), 1):
            nctids = [row["nctid"] for row in rows]
            contexts = self._load_clinical_trial_contexts(nctids)

            self.logger.info(
                f"Existing NCTID update check batch #{batch_num}: "
                f"batch_size={len(rows)}, checked={checked_count}/{total_nctids}."
            )

            for row in rows:
                nctid = row["nctid"]
                checked_count += 1

                context = contexts.get(nctid)
                if context is None:
                    missing_context_count += 1
                    self.logger.warning(
                        f"Skipping existing ClinicalTrials.gov update check for NCTID={nctid}: "
                        "no clinical_trial row provides GARD/disease context."
                    )
                    continue

                fetched_study = self._fetch_study_by_nctid(nctid)
                if fetched_study is None:
                    failed_count += 1
                    self.logger.error(f"Unable to fetch current ClinicalTrials.gov study JSON for existing NCTID={nctid}.")
                    continue

                try:
                    result = self.study_change_handler.save_or_update_study(
                        context["gard_id"],
                        context["disease_name"],
                        nctid,
                        fetched_study,
                        self._study_url(nctid),
                    )

                except Exception as error:
                    failed_count += 1
                    self.logger.error(f"Failed to compare/update existing NCTID={nctid}: {error}", exc_info=True)
                    continue

                action = result.get("action")
                if action == "updated":
                    updated_count += 1
                    self.logger.info(
                        f"Existing ClinicalTrials.gov study changed: NCTID={nctid}, "
                        f"gardId={context['gard_id']}, disease={context['disease_name']}, "
                        f"differences={result.get('differences')}"
                    )

                elif action == "inserted":
                    inserted_count += 1
                    self.logger.info(
                        f"Existing NCTID was missing from clinical_trial_unique and was inserted by handler: "
                        f"NCTID={nctid}, gardId={context['gard_id']}, disease={context['disease_name']}."
                    )

                else:
                    unchanged_count += 1

                if checked_count % self.batch_size == 0 or checked_count == total_nctids:
                    self.logger.info(
                        f"Existing NCTID update progress: checked={checked_count}/{total_nctids}, "
                        f"updated={updated_count}, unchanged={unchanged_count}, "
                        f"failed={failed_count}, missing_context={missing_context_count}."
                    )

                if self.REQUEST_SLEEP_SECONDS > 0:
                    time.sleep(self.REQUEST_SLEEP_SECONDS)

        self.logger.info(
            f"Completed existing ClinicalTrials.gov study update check: checked={checked_count}, "
            f"updated={updated_count}, inserted={inserted_count}, unchanged={unchanged_count}, "
            f"failed={failed_count}, missing_context={missing_context_count}."
        )


    def _count_existing_nctids(self) -> int:

        ''' Count non-empty NCT IDs in clinical_trial_unique for progress logging. '''
        cursor = self.mysql.cursor(dictionary=True, buffered=True)

        try:
            cursor.execute(
                '''
                SELECT COUNT(*) AS row_count
                FROM clinical_trial_unique
                WHERE nctid IS NOT NULL
                AND nctid <> ''
                '''
            )
            row = cursor.fetchone()
            return int(row.get("row_count") or 0) if row else 0

        finally:
            cursor.close()


    def _iter_existing_nctid_batches(self):

        '''
        Yield clinical_trial_unique NCT IDs in stable id order.

        Using id > last_id avoids OFFSET scans on the large unique table and
        keeps the refresh memory footprint bounded to one small batch at a time.
        '''
        last_id = 0
        fetched_count = 0

        while True:
            limit = self.batch_size
            if self.nctid_limit is not None:
                remaining = self.nctid_limit - fetched_count
                if remaining <= 0:
                    break

                limit = min(limit, remaining)

            cursor = self.mysql.cursor(dictionary=True, buffered=True)

            try:
                cursor.execute(
                    '''
                    SELECT id, nctid
                    FROM clinical_trial_unique
                    WHERE id > %s
                    AND nctid IS NOT NULL
                    AND nctid <> ''
                    ORDER BY id
                    LIMIT %s
                    ''',
                    (last_id, limit),
                )
                rows = cursor.fetchall()

            finally:
                cursor.close()

            if not rows:
                break

            last_id = max(row["id"] for row in rows)
            fetched_count += len(rows)

            yield rows


    def _load_clinical_trial_contexts(self, nctids: List[str]) -> Dict[str, Dict[str, Any]]:

        '''
        Load one clinical_trial GARD/disease context for each NCT ID.

        clinical_trial can hold multiple disease rows for one NCT ID. The first
        row is enough for the update handler because changed existing studies
        update all clinical_trial rows with the same NCT ID.
        '''
        if not nctids:
            return {}

        placeholders = ", ".join(["%s"] * len(nctids))
        select_sql = f'''
            SELECT nctid, gardId AS gard_id, disease AS disease_name
            FROM clinical_trial
            WHERE nctid IN ({placeholders})
            ORDER BY nctid, id
        '''

        cursor = self.mysql.cursor(dictionary=True, buffered=True)
        contexts: Dict[str, Dict[str, Any]] = {}

        try:
            cursor.execute(select_sql, tuple(nctids))

            for row in cursor.fetchall():
                nctid = row.get("nctid")

                if nctid and nctid not in contexts:
                    contexts[nctid] = {
                        "gard_id": row.get("gard_id"),
                        "disease_name": row.get("disease_name") or "",
                    }

        finally:
            cursor.close()

        return contexts


    def _fetch_study_by_nctid(self, nctid: str) -> Optional[Dict[str, Any]]:

        '''
        Fetch the current full study JSON for one existing NCT ID.

        Each existing NCT ID is checked directly by identifier, so the update
        check does not depend on disease search terms or GARD updated dates.
        '''
        try:
            response = requests.get(self._study_url(nctid), timeout=self.REQUEST_TIMEOUT_SECONDS)

            if response.status_code >= 400:
                self.logger.error(f"ClinicalTrials.gov request failed for NCTID={nctid}: status={response.status_code}.")
                return None

            return response.json()

        except requests.exceptions.RequestException as error:
            self.logger.error(f"ClinicalTrials.gov request failed for NCTID={nctid}: {error}.")
            return None


    def _study_url(self, nctid: str) -> str:

        ''' Build the direct ClinicalTrials.gov study URL for one NCT ID. '''
        return f"{self.clinical_trials_studies_api.rstrip('/')}/{nctid}"

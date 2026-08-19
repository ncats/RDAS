import copy
import json
from typing import Any, Dict, List, Optional, Tuple


class ClinicalTrialStudyChangeHandler:

    '''
    Handle the database decision for one fetched ClinicalTrials.gov study.

    NewClinicalTrialDiscoveryTask is responsible for finding NCT IDs and
    fetching full study JSON. This helper is responsible for deciding whether
    the fetched JSON is new, unchanged, or changed compared with the canonical
    row in clinical_trial_unique.
    '''

    MAX_DIFF_VALUE_LENGTH = 500
    IGNORED_COMPARE_PATHS = (
        ("derivedSection", "miscInfoModule", "versionHolder"),
    )

    def __init__(self, mysql: Any, logger: Any):

        '''
        Keep the MySQL connection and logger owned by the parent pipeline task.
        The helper does not open or close connections because PipelineBase still
        owns the task lifecycle.
        '''
        self.mysql = mysql
        self.logger = logger


    def save_or_update_study(self, gard_id: str, disease_name: str, nctid: str, fetched_study: Dict[str, Any], source_url: str) -> Dict[str, Any]:

        '''
        Save one fetched study into the clinical-trial staging tables.

        The clinical_trial table can contain multiple rows for the same NCT ID
        because a trial can match more than one GARD disease term. For change
        detection, clinical_trial_unique is the safer source because it has one
        row per NCT ID. If the unique row is missing, this method inserts into
        clinical_trial so task_clinical_trial_2.py can create the unique row in
        the next pipeline step.
        '''
        stored_study = self.load_unique_study(nctid)
        serialized_study = self._serialize_for_storage(fetched_study)

        try:
            if stored_study is None:
                self._insert_clinical_trial(gard_id, disease_name, nctid, serialized_study, source_url)
                self.mysql.commit()
                self._log_info(f"New nctid added: {nctid} for: {gard_id}")

                return {
                    "action": "inserted",
                    "is_same": False,
                    "differences": [
                        {
                            "path": "$",
                            "reason": "missing_from_clinical_trial_unique",
                            "stored": None,
                            "fetched": self._preview_json_value(fetched_study),
                        }
                    ],
                }

            is_same, differences = self.compare_studies(stored_study, fetched_study)

            if is_same:
                self._log_info(f"Existing nctid unchanged: {nctid}")

                return {
                    "action": "unchanged",
                    "is_same": True,
                    "differences": [],
                }

            self._update_changed_study(gard_id, disease_name, nctid, serialized_study, source_url)
            self.mysql.commit()
            self._log_info(f"Existing nctid changed and marked is_new=1: {nctid}")

            return {
                "action": "updated",
                "is_same": False,
                "differences": differences,
            }

        except Exception:
            if self.mysql:
                self.mysql.rollback()

            raise


    def load_unique_study(self, nctid: str) -> Optional[str]:

        '''
        Load the canonical stored study JSON from clinical_trial_unique.

        This table has a UNIQUE key on nctid, so it avoids the ambiguity of
        clinical_trial, where the same NCT ID may appear once for each matching
        GARD disease term.
        '''
        select_sql = '''
            SELECT studies
            FROM clinical_trial_unique
            WHERE nctid = %s
        '''

        cursor = self.mysql.cursor(dictionary=True, buffered=True)

        try:
            cursor.execute(select_sql, (nctid,))
            row = cursor.fetchone()

            if row is None:
                return None

            return row.get("studies")

        finally:
            cursor.close()


    def compare_studies(self, stored_study: Any, fetched_study: Any, max_differences: int = 50) -> Tuple[bool, List[Dict[str, Any]]]:

        '''
        Compare stored and fetched study JSON after normalization.

        JSON strings can differ only by whitespace or object key order. To avoid
        false positives, both sides are parsed and serialized with sorted keys
        before comparison. When the normalized documents differ, a capped list of
        path-level differences is returned for logs or manual review.
        '''
        stored_obj = self._study_value_for_comparison(stored_study)
        fetched_obj = self._study_value_for_comparison(fetched_study)

        if self._normalized_json(stored_obj) == self._normalized_json(fetched_obj):
            return True, []

        differences: List[Dict[str, Any]] = []
        self._collect_json_differences(stored_obj, fetched_obj, "$", differences, max_differences)

        if not differences:
            differences.append(
                {
                    "path": "$",
                    "reason": "normalized_json_differs",
                    "stored": self._preview_json_value(stored_obj),
                    "fetched": self._preview_json_value(fetched_obj),
                }
            )

        return False, differences


    def _insert_clinical_trial(self, gard_id: str, disease_name: str, nctid: str, serialized_study: str, source_url: str) -> None:

        '''
        Insert a newly discovered study into clinical_trial.

        The next pipeline step reads clinical_trial rows with is_new=1 and
        copies the latest row for each NCT ID into clinical_trial_unique.
        '''
        insert_sql = '''
            INSERT INTO clinical_trial (gardId, disease, nctid, studies, url, is_new)
            VALUES (%s, %s, %s, %s, %s, 1)
        '''

        cursor = self.mysql.cursor()

        try:
            cursor.execute(insert_sql, (gard_id, disease_name, nctid, serialized_study, source_url))

        finally:
            cursor.close()


    def _update_changed_study(self, gard_id: str, disease_name: str, nctid: str, serialized_study: str, source_url: str) -> None:

        '''
        Mark changed existing studies as new in both clinical-trial tables.

        clinical_trial_unique drives many later pipeline_2 graph/enrichment
        tasks through WHERE is_new = 1. Resetting brief_title and brief_summary
        lets task_clinical_trial_2.py recalculate those fields from the updated
        study JSON instead of leaving stale title/summary text in place.
        Resetting alert_sent keeps a real content change visible to AlertSender
        even when the same clinical_trial row already produced an older alert.
        '''
        clinical_trial_row_count = self._clinical_trial_row_count(nctid)

        if clinical_trial_row_count == 0:
            self._insert_clinical_trial(gard_id, disease_name, nctid, serialized_study, source_url)

        else:
            update_clinical_trial_sql = '''
                UPDATE clinical_trial
                SET studies = %s,
                    url = %s,
                    brief_title = NULL,
                    brief_summary = NULL,
                    is_new = 1,
                    alert_sent = '0'
                WHERE nctid = %s
            '''

            cursor = self.mysql.cursor()

            try:
                cursor.execute(update_clinical_trial_sql, (serialized_study, source_url, nctid))

            finally:
                cursor.close()

        update_unique_sql = '''
            UPDATE clinical_trial_unique
            SET studies = %s,
                brief_title = NULL,
                brief_summary = NULL,
                is_new = 1
            WHERE nctid = %s
        '''

        cursor = self.mysql.cursor()

        try:
            cursor.execute(update_unique_sql, (serialized_study, nctid))

        finally:
            cursor.close()


    def _clinical_trial_row_count(self, nctid: str) -> int:

        '''
        Count clinical_trial rows before updating changed study JSON.

        MySQL cursor.rowcount on UPDATE reports changed rows, not always matched
        rows, so an explicit count prevents inserting a duplicate row when an
        existing row already matches the new values.
        '''
        select_sql = '''
            SELECT COUNT(*) AS row_count
            FROM clinical_trial
            WHERE nctid = %s
        '''

        cursor = self.mysql.cursor(dictionary=True, buffered=True)

        try:
            cursor.execute(select_sql, (nctid,))
            row = cursor.fetchone()
            return int(row.get("row_count") or 0) if row else 0

        finally:
            cursor.close()


    def _coerce_json_value(self, value: Any) -> Any:

        '''
        Convert a stored JSON string into a Python object when possible.

        Stored studies are MEDIUMTEXT values, while freshly fetched studies are
        already Python dictionaries. Invalid stored JSON is returned as raw text
        so the comparison still reports a difference instead of crashing.
        '''
        if isinstance(value, str):
            text = value.strip()

            if not text:
                return None

            try:
                return json.loads(text)

            except json.JSONDecodeError:
                return text

        return value


    def _study_value_for_comparison(self, value: Any) -> Any:

        '''
        Normalize one study JSON object before equality/difference checks.

        ClinicalTrials.gov includes generated metadata fields that can change
        without a clinical study-content change. Removing those known volatile
        paths prevents the existing-NCTID refresh from marking a study as new
        only because generated API metadata changed.
        '''
        comparison_value = copy.deepcopy(self._coerce_json_value(value))

        for path in self.IGNORED_COMPARE_PATHS:
            self._remove_nested_path(comparison_value, path)

        return comparison_value


    def _remove_nested_path(self, value: Any, path: Tuple[str, ...]) -> None:

        '''
        Remove one ignored comparison path from a copied JSON object.

        Missing keys are expected because ClinicalTrials.gov response sections
        vary by study. In those cases, this method simply leaves the object
        unchanged.
        '''
        current_value = value

        for key in path[:-1]:
            if not isinstance(current_value, dict):
                return

            current_value = current_value.get(key)

        if isinstance(current_value, dict):
            current_value.pop(path[-1], None)


    def _serialize_for_storage(self, value: Any) -> str:

        '''
        Serialize fetched ClinicalTrials.gov JSON for the studies MEDIUMTEXT column.

        The storage format does not need sorted keys because comparison handles
        normalization separately. Keeping compact JSON matches the existing
        pipeline behavior and avoids inflating large study records.
        '''
        return json.dumps(value, ensure_ascii=False, default=str)


    def _normalized_json(self, value: Any) -> str:

        '''
        Build a stable JSON string used only for equality comparison.

        sort_keys=True removes object-key-order noise, and compact separators
        remove whitespace noise, so semantically identical JSON objects compare
        as equal.
        '''
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


    def _collect_json_differences(self, stored_value: Any, fetched_value: Any, path: str, differences: List[Dict[str, Any]], max_differences: int) -> None:

        '''
        Recursively collect path-level differences between two JSON-compatible values.

        The list is capped because a changed ClinicalTrials.gov response can have
        many nested differences. The first differences are enough to explain why
        the study was marked as changed without flooding logs or test output.
        '''
        if len(differences) >= max_differences:
            return

        if type(stored_value) is not type(fetched_value):
            differences.append(self._difference(path, "type_changed", stored_value, fetched_value))
            return

        if isinstance(stored_value, dict):
            self._collect_dict_differences(stored_value, fetched_value, path, differences, max_differences)
            return

        if isinstance(stored_value, list):
            self._collect_list_differences(stored_value, fetched_value, path, differences, max_differences)
            return

        if stored_value != fetched_value:
            differences.append(self._difference(path, "value_changed", stored_value, fetched_value))


    def _collect_dict_differences(self, stored_dict: Dict[str, Any], fetched_dict: Dict[str, Any], path: str, differences: List[Dict[str, Any]], max_differences: int) -> None:

        '''
        Compare dictionary keys first, then recurse into shared keys.

        Sorting keys makes the printed differences stable between runs, which is
        helpful when the manual NCT ID test is used to inspect a real API change.
        '''
        stored_keys = set(stored_dict.keys())
        fetched_keys = set(fetched_dict.keys())

        for key in sorted(stored_keys - fetched_keys):
            if len(differences) >= max_differences:
                return

            differences.append(self._difference(f"{path}.{key}", "missing_from_fetched", stored_dict.get(key), None))

        for key in sorted(fetched_keys - stored_keys):
            if len(differences) >= max_differences:
                return

            differences.append(self._difference(f"{path}.{key}", "missing_from_stored", None, fetched_dict.get(key)))

        for key in sorted(stored_keys & fetched_keys):
            if len(differences) >= max_differences:
                return

            self._collect_json_differences(stored_dict.get(key), fetched_dict.get(key), f"{path}.{key}", differences, max_differences)


    def _collect_list_differences(self, stored_list: List[Any], fetched_list: List[Any], path: str, differences: List[Dict[str, Any]], max_differences: int) -> None:

        '''
        Compare list lengths and then compare items by index.

        ClinicalTrials.gov arrays usually have stable ordering. If that ever
        changes upstream, the item-level differences make the ordering issue
        visible in the test output.
        '''
        if len(stored_list) != len(fetched_list):
            differences.append(
                {
                    "path": path,
                    "reason": "list_length_changed",
                    "stored_length": len(stored_list),
                    "fetched_length": len(fetched_list),
                }
            )

        for index, (stored_item, fetched_item) in enumerate(zip(stored_list, fetched_list)):
            if len(differences) >= max_differences:
                return

            self._collect_json_differences(stored_item, fetched_item, f"{path}[{index}]", differences, max_differences)


    def _difference(self, path: str, reason: str, stored_value: Any, fetched_value: Any) -> Dict[str, Any]:

        '''
        Format one difference as a JSON-serializable dictionary.

        Large nested values are shortened so printed test output remains readable
        even when a whole ClinicalTrials.gov section is added or removed.
        '''
        return {
            "path": path,
            "reason": reason,
            "stored": self._preview_json_value(stored_value),
            "fetched": self._preview_json_value(fetched_value),
        }


    def _preview_json_value(self, value: Any) -> Any:

        '''
        Return small scalar values as-is and shorten large nested values.

        Difference output should identify the changed location and give enough
        context to inspect it, while avoiding extremely large values in logs.
        '''
        if value is None or isinstance(value, (bool, int, float)):
            return value

        if isinstance(value, str):
            return value if len(value) <= self.MAX_DIFF_VALUE_LENGTH else f"{value[:self.MAX_DIFF_VALUE_LENGTH]}..."

        text = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
        return text if len(text) <= self.MAX_DIFF_VALUE_LENGTH else f"{text[:self.MAX_DIFF_VALUE_LENGTH]}..."


    def _log_info(self, message: str) -> None:

        if self.logger is not None:
            self.logger.info(message)

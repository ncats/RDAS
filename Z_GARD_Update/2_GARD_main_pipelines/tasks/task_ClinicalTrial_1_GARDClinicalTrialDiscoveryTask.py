"""Search clinical-trial IDs, reuse cached studies, then download missing studies.

Invocation: run_pipeline_for_each_gard_node() creates this task for each GARD
record before publication discovery. The runner supplies one shared reuse helper,
whose current GARD ID has already been set by begin_gard_node().

Illustrative data shape; the trial IDs below do not assert real disease matches:
    gard_node = {
        "gardId": "GARD:0000016",
        "filtered_names": ["Coma"],
        "updated": "1971-01-01",
    }
    searched_nctids = ["NCT01234567", "NCT07654321"]

If the first study is usable in either database and the second is missing,
copy/reuse the first and download only the second. Both destination trial
mappings use gardId="GARD:0000016", disease="Coma", and is_new=1. A source study
associated with another disease must not bring that disease's mapping with it.

HTTP calls use the existing bounded thread pool because they wait on the API.
Workers return study JSON; the parent performs every database write. Each ID page
finishes before requesting the next page, limiting retained study payloads."""

import re
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat

from pipelines.pipeline_2_clinical_trial.task_clinical_trial_1 import NewClinicalTrialDiscoveryTask
from utils.tools import _resolve_worker_count, _safe_get


class GARDClinicalTrialDiscoveryTask(NewClinicalTrialDiscoveryTask):
    """Search NCT IDs first, copy cached studies, and download only missing IDs."""

    __module__ = NewClinicalTrialDiscoveryTask.__module__

    def __init__(self, reuse):

        super().__init__()
        self.reuse = reuse

    def _generate_GARD_ID_and_nctId(self, gardId, names, last_update_date):

        '''
        Keep the Alert search expression, pagination, study downloader, and
        change handler. The GARD build adds a batched cache lookup before full
        study downloads, and always creates mappings for the CURRENT disease.
        HTTP workers only return study JSON; database copies and change-handler
        writes stay in this parent thread. Threads fit these network waits.
        '''
        if not self.clinical_trials_studies_api:
            raise RuntimeError("CLINICAL_TRIAL_STUDIES_API is not configured.")
        studies_api = self.clinical_trials_studies_api.rstrip("/")
        worker_count = _resolve_worker_count(4, env_var_name="CLINICAL_TRIAL_DOWNLOAD_MAX_WORKERS", logger=self.logger)
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            for name in names:
                initial_query = f'{studies_api}?query.cond=(EXPANSION[Term]{name} OR AREA[DetailedDescription]EXPANSION[Term]{name} OR AREA[BriefSummary]EXPANSION[Term]{name}) AND AREA[LastUpdatePostDate]RANGE[{last_update_date},MAX]&fields=NCTId&pageSize=1000&countTotal=true'
                page_token = None
                seen_tokens = set()
                while True:
                    result = self.call_get_nctids(initial_query, pageToken=page_token)
                    if not isinstance(result, dict) or not result or not isinstance(result.get("studies", []), list) or ("studies" not in result and result.get("totalCount") != 0):
                        raise RuntimeError(f"Invalid ClinicalTrials.gov ID search response for {gardId}: {name}.")
                    identifiers = list(dict.fromkeys(study["protocolSection"]["identificationModule"]["nctId"] for study in result.get("studies", [])))
                    if any(not re.fullmatch(r"NCT\d{8}", nctid) for nctid in identifiers):
                        raise ValueError("ClinicalTrials.gov search returned an invalid NCT ID.")
                    handled = self.reuse.copy_clinical_trials(self.mysql, self.logger, gardId, name, identifiers, initial_query)
                    missing = [nctid for nctid in identifiers if nctid not in handled]
                    ''' Limit queued study payloads as well as active HTTP workers. '''
                    for start in range(0, len(missing), self.reuse.BATCH_SIZE):
                        download_batch = missing[start:start + self.reuse.BATCH_SIZE]
                        for nctid, study in zip(download_batch, executor.map(self._fetch_study_by_nctid, repeat(studies_api), download_batch)):
                            if not isinstance(study, dict) or _safe_get(study, "protocolSection", "identificationModule", "nctId") != nctid:
                                raise RuntimeError(f"No valid downloaded study for {nctid}; leave {gardId} pending.")
                            self.study_change_handler.save_or_update_study(gardId, name, nctid, study, initial_query)
                        '''
                        The shared change handler can update an existing trial
                        without adding a new GARD/term mapping. Revisit the now
                        valid target study to ensure the CURRENT GARD mapping
                        exists even when an invalid cache needed a fresh fetch.
                        '''
                        self.reuse.copy_clinical_trials(self.mysql, self.logger, gardId, name, download_batch, initial_query)
                    self.logger.info(f"{gardId}: {name}; NCT IDs={len(identifiers)}, reused={len(handled)}, downloaded={len(missing)}.")
                    page_token = result.get("nextPageToken")
                    if not page_token:
                        break
                    if page_token in seen_tokens:
                        raise RuntimeError("ClinicalTrials.gov repeated a pagination token.")
                    seen_tokens.add(page_token)

"""GARD-only discovery and import tasks that prefer existing RDAS data."""

import re
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat

from pipelines.pipeline_2_clinical_trial.task_clinical_trial_1 import NewClinicalTrialDiscoveryTask
from pipelines.pipeline_2_clinical_trial.task_clinical_trial_5 import ClinicalTrialPmidArticleImportTask
from pipelines.pipeline_3_publication.task_publication_1 import NewPublicationDiscoveryTask
from pipelines.pipeline_3_publication.task_publication_3 import GardOmimPublicationMappingTask
from pipelines.pipeline_3_publication.task_publication_4 import PublicationOminDataRetrievalTask
from pipelines.pipeline_3_publication.task_publication_8 import NewOmimPublicationArticleImportTask
from pipelines.pipeline_3_publication.task_publication_graph_8 import NewPublicationGardArticleRelationshipTask
from utils.tools import _escape_sql_literal, _resolve_worker_count, _safe_get


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


class GARDPublicationDiscoveryTask(NewPublicationDiscoveryTask):
    """Retain complete PubMed ID searches and create current-GARD mappings."""

    __module__ = NewPublicationDiscoveryTask.__module__

    def __init__(self, reuse):

        super().__init__()
        self.reuse = reuse

    def find_new_publications(self, gard_id, search_term, pubmed_ids):

        '''
        Copy complete cached metadata and available enrichment before the shared
        importer checks for missing Articles. Its bounded HTTP pool downloads
        only remaining PMIDs and creates mappings using this gard_id and term.
        Matching source mapping rows are copied only for this exact GARD/term;
        the shared importer adds associations missing from that cache.
        '''
        if gard_id != self.reuse.current_gard_id:
            raise ValueError("Publication mappings must use the current GARD node.")
        self.reuse.copy_publications(self.mysql, self.logger, pubmed_ids, search_term=search_term)
        super().find_new_publications(gard_id, search_term, pubmed_ids)

    def retrieve_pubmed_ids(self, gard_id, search_term, mindate=None, maxdate=None):

        '''
        Preserve the shared live search and complete date splitting. Copy
        compatible source search history after every live slice has committed,
        so historical keys cannot block later live response inserts. The helper
        checks current mappings in batches rather than accumulating all PMIDs.
        '''
        if gard_id != self.reuse.current_gard_id:
            raise ValueError("Publication searches must use the current GARD node.")
        for pubmed_ids in super().retrieve_pubmed_ids(gard_id, search_term, mindate, maxdate):
            yield pubmed_ids
        self.reuse.copy_publication_search_history(self.mysql, self.logger, gard_id, search_term, mindate, maxdate)


class GARDOMIMPublicationMappingTask(GardOmimPublicationMappingTask):
    """Copy current source associations and let shared mapping add new GARD xrefs."""

    __module__ = GardOmimPublicationMappingTask.__module__

    def __init__(self, reuse, gard_id):

        if gard_id != reuse.current_gard_id:
            raise ValueError("OMIM mapping tasks require the current GARD node.")
        super().__init__(gard_id=gard_id)
        self.reuse = reuse

    def process_new_data(self):

        '''
        Reuse only source pairs still present in current GARD cross-references.
        The inherited task keeps its normal NOT EXISTS behavior for any pairs
        absent from production, without changing the regular Alert task.
        '''
        self.reuse.copy_gard_omim_mappings(self.mysql, self.logger, self.gard_id)
        super().process_new_data()


class GARDOMIMDataRetrievalTask(PublicationOminDataRetrievalTask):
    """Reuse current GARD's OMIM JSON/reference pairs before missing-entry HTTP calls."""

    __module__ = PublicationOminDataRetrievalTask.__module__

    def __init__(self, reuse, gard_id):

        if gard_id != reuse.current_gard_id:
            raise ValueError("OMIM retrieval tasks require the current GARD node.")
        super().__init__(gard_id=gard_id)
        self.reuse = reuse

    def process_new_data(self):

        '''
        The shared retrieval query already excludes OMIM IDs in the target.
        Copying before it runs avoids those API calls; genuinely absent or
        invalid source entries continue through the normal downloader.
        '''
        self.reuse.copy_omim_data(self.mysql, self.logger, self.gard_id)
        super().process_new_data()


class GARDClinicalTrialPmidArticleImportTask(ClinicalTrialPmidArticleImportTask):
    """Reuse publication records referenced by copied or newly downloaded trials."""

    __module__ = ClinicalTrialPmidArticleImportTask.__module__

    def __init__(self, reuse):

        super().__init__()
        self.reuse = reuse

    def process_new_data(self):

        '''
        Trial reference PMIDs can be absent from the disease's PubMed search.
        Copy their cached article/annotation data too, then let the existing
        importer download references absent from both databases.
        '''
        self.reuse.copy_trial_publications(self.mysql, self.logger)
        super().process_new_data()


class GARDOMIMPublicationArticleImportTask(NewOmimPublicationArticleImportTask):
    """Apply the same PMID cache lookup to the current GARD node's OMIM references."""

    __module__ = NewOmimPublicationArticleImportTask.__module__

    def __init__(self, reuse):

        super().__init__()
        self.reuse = reuse

    def get_unique_new_omim_id_list(self):

        '''
        A restarted build may contain pending associations from earlier nodes.
        Keep this import tied to CURRENT GARD mappings; the helper has staged
        their OMIM JSON even when a shared OMIM entry was completed previously.
        '''
        cursor = self.mysql.cursor(buffered=True)
        try:
            cursor.execute(
                '''SELECT DISTINCT omim_id FROM publication_gard_omim_mapping
                   WHERE gard_id = %s AND is_new = 1 AND omim_id IS NOT NULL''',
                (self.reuse.current_gard_id,),
            )
            return [row[0] for row in cursor.fetchall()]
        finally:
            cursor.close()

    def _retrieve_missing_publication_articles(self, pubmed_ids):

        '''
        The inherited task creates provenance only for the staged OMIM entries.
        Reuse each referenced Article before its missing-PMID downloader runs;
        no unrelated source OMIM/GARD mappings are imported.
        '''
        self.reuse.copy_publications(self.mysql, self.logger, pubmed_ids)
        super()._retrieve_missing_publication_articles(pubmed_ids)


class GARDPublicationGardArticleRelationshipTask(NewPublicationGardArticleRelationshipTask):
    """Link current-GARD mappings even when the target Article was imported earlier."""

    __module__ = NewPublicationGardArticleRelationshipTask.__module__

    def __init__(self, gard_id):

        if not re.fullmatch(r"GARD:\d{7}", gard_id):
            raise ValueError(f"Invalid current GARD ID: {gard_id!r}")
        super().__init__()
        '''
        Existing Articles stay is_new=0 to avoid repeating enrichment, graph
        metadata writes, and author extraction for every disease. A new mapping
        still needs its edge. Scope the shared relationship query to the exact
        current GARD ID rather than requiring a newly inserted Article.
        The shared task executes a constant SQL string, so serialize this
        validated internal ID with the existing SQL-literal helper.
        '''
        self.FETCH_NEW_RELATIONS_QUERY = self.FETCH_NEW_RELATIONS_QUERY.replace("WHERE pa.is_new = 1", f"WHERE pgspm.gard_id = {_escape_sql_literal(gard_id)}")

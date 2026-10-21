
import os
import sys
import logging
import re
from pathlib import Path

import spacy


PROJECT_ROOT = Path(__file__).resolve().parents[2]
Z_ALERT_ROOT = PROJECT_ROOT / "Z_Alert"
GARD_PIPELINE_ROOT = Path(__file__).resolve().parent
for import_path in (str(PROJECT_ROOT), str(Z_ALERT_ROOT), str(GARD_PIPELINE_ROOT)):
    if import_path not in sys.path:
        sys.path.insert(0, import_path)

from Z_Alert.main import AlertPipelineRunner
from pipelines.pipeline_base import PipelineBase
from pipelines.pipeline_2_clinical_trial.task_clinical_trial_6 import NewClinicalTrialAnnotationTask
from pipelines.pipeline_3_publication.task_publication_7 import PublicationFalsePositiveFilterTask
from pipelines.pipeline_5_followup.task_pipeline_followup_update_3 import DiseaseCountsRefreshTask
from pipelines.pipeline_7_graph_maintenance.task_pipeline_maintenance_3 import NewOrganizationSourceTrackingTask
from gard_person_tasks import GARDOrganizationSourceTrackingTask, GARDPersonAgentGraphTask, GARDPersonGroupingTask
from gard_existing_data import GARDExistingDataReuse
from gard_reuse_tasks import GARDClinicalTrialDiscoveryTask, GARDPublicationDiscoveryTask, GARDClinicalTrialPmidArticleImportTask, GARDOMIMPublicationMappingTask, GARDOMIMDataRetrievalTask, GARDOMIMPublicationArticleImportTask, GARDPublicationGardArticleRelationshipTask
from utils.tools import _escape_sql_literal


class GARDTaskErrorCapture(logging.Handler):
    """Remember task errors that shared Alert tasks log without raising."""

    def __init__(self):

        super().__init__(level=logging.ERROR)
        self.had_error = False
        self.first_error = None

    def emit(self, record):

        self.had_error = True
        '''
        Keep the first failure message after the shared task closes its logger.
        It explains the original failure in the runner's final traceback even
        when the task catches the exception and returns normally.
        '''
        if self.first_error is None:
            self.first_error = record.getMessage()


class GARDTaskErrorCheckingMixin:
    """Prevent GARD checkpoints and flag cleanup after a logged task failure."""

    def process_new_data(self) -> None:

        '''
        Some shared tasks catch failures, log them, and return normally. Attach
        this handler after task initialization so normal task log configuration
        is preserved. Keep a reference even when a task closes its own logger;
        an error must still stop the build before pending flags are cleared.
        The recurring Alert runner does not use this GARD-specific mixin.
        '''
        task_logger = self.logger
        error_capture = GARDTaskErrorCapture()
        task_logger.addHandler(error_capture)
        try:
            super().process_new_data()
        finally:
            task_logger.removeHandler(error_capture)
            error_capture.close()

        if error_capture.had_error:
            raise RuntimeError(f"{type(self).__name__} logged an error: {error_capture.first_error}; pending GARD work needs a retry.")


class GARDCountsRefreshTask(GARDTaskErrorCheckingMixin, DiseaseCountsRefreshTask):
    """Run the full count refresh at a GARD checkpoint and surface logged errors."""

    __module__ = DiseaseCountsRefreshTask.__module__


class GARDModelCachingClinicalTrialAnnotationTask(NewClinicalTrialAnnotationTask):
    """
    Reuse scispaCy models while the GARD runner processes multiple GARD nodes.

    The shared Alert runner creates a new annotation task for each call to
    run_clinical_trial_mysql_updates(). In the per-node GARD runner, that would
    reload both NER models and both UMLS linkers for every disease. The loaded
    objects are kept on this subclass so the next task instance can reuse them.

    Only the models and linkers are shared. Each task still opens and closes its
    own MySQL connection and logger through the inherited task lifecycle. This
    cache lives only in the current Python process; a new pipeline run loads
    the models once again.
    """

    # Sequential GARD tasks share models; MySQL connections and loggers remain per task.
    _cached_models = None
    # Pipeline error logging uses the task's module to choose its error log.
    __module__ = NewClinicalTrialAnnotationTask.__module__

    def load_models(self):

        """Return the cached model/linker tuple, loading it on the first successful call."""

        if GARDModelCachingClinicalTrialAnnotationTask._cached_models is not None:
            self.logger.info("Reusing clinical-trial annotation models and UMLS linkers loaded for this GARD run.")
            return GARDModelCachingClinicalTrialAnnotationTask._cached_models

        models = super().load_models()
        # Do not cache a partial or failed load; the next node can retry it.
        if all(model is not None for model in models):
            GARDModelCachingClinicalTrialAnnotationTask._cached_models = models

        return models


class GARDModelCachingPublicationFalsePositiveFilterTask(PublicationFalsePositiveFilterTask):
    """
    Reuse the small English spaCy model across this run's GARD node iterations.

    The shared publication task loads en_core_web_sm in its constructor. The
    GARD runner constructs that task for every node, including nodes with no
    abbreviation mappings to validate. Keep the model on this subclass so its
    pipeline is loaded once per Python process and reused by later task instances.

    The model is used sequentially for text parsing. MySQL connections, loggers,
    and UMLS endpoint settings still belong to each new task instance. The
    regular Alert runner continues to use PublicationFalsePositiveFilterTask.
    """

    _cached_nlp = None
    # Keep this task's errors in the publication pipeline error log.
    __module__ = PublicationFalsePositiveFilterTask.__module__

    def __init__(self, gard_id=None):

        # Bypass only the parent's spacy.load(); PipelineBase still opens per-task resources.
        PipelineBase.__init__(self, init_mysql=True, init_memgraph=False)
        try:
            self.api_key = os.getenv("UMLS_API_KEY")
            self.umls_search_api = os.getenv("UMLS_SEARCH_API")
            self.umls_cui_api_template = os.getenv("UMLS_CUI_API_TEMPLATE")

            '''
            A newly discovered mapping can point to an Article already fully
            imported for another disease. Validate this GARD node's mappings
            without marking that Article new again and re-extracting authors.
            These query overrides belong only to the GARD caching subclass.
            '''
            if gard_id is not None:
                if not re.fullmatch(r"GARD:\d{7}", gard_id):
                    raise ValueError(f"Invalid current GARD ID: {gard_id!r}")
                literal = _escape_sql_literal(gard_id)
                self.SET_ABBREVIATION_SQL = self.SET_ABBREVIATION_SQL.replace("AND pa.is_new = 1", f"AND pgs.gard_id = {literal}")
                self.FETCH_ABBREVIATION_ROWS_QUERY = self.FETCH_ABBREVIATION_ROWS_QUERY.replace("AND a.is_new = 1", f"AND gsp.gard_id = {literal}")

            if GARDModelCachingPublicationFalsePositiveFilterTask._cached_nlp is None:
                GARDModelCachingPublicationFalsePositiveFilterTask._cached_nlp = spacy.load("en_core_web_sm")
                self.logger.info("Loaded en_core_web_sm for GARD publication filtering.")
            else:
                self.logger.info("Reusing en_core_web_sm for GARD publication filtering.")

            self.nlp = GARDModelCachingPublicationFalsePositiveFilterTask._cached_nlp
        except Exception:
            self.close()
            raise


class GARDToMySQLAndMemgraphUpdateRunner(AlertPipelineRunner):

    # A failed task must not reach wrap-up or mark a GARD checkpoint complete.
    raise_task_errors = True

    # Refresh full-database disease counts after this many completed diseases.
    DISEASE_COUNTS_REFRESH_NODE_BATCH_SIZE = 500

    def __init__(self):

        super().__init__()

        self.START_DATE = '1971-01-01'
        self._nodes_since_counts_refresh = 0
        self.existing_data_reuse = GARDExistingDataReuse()
        self.current_gard_id = None

        self.logger.info(
            "GARDToMySQLAndMemgraphUpdateRunner initialized; "
            f"disease counts refresh every {self.DISEASE_COUNTS_REFRESH_NODE_BATCH_SIZE} GARD nodes; "
            "person extraction runs per node; grouping and Agent updates run after all nodes."
        )
        self.logger.info("GARD discovery reuses rdas_db clinical-trial/publication data before downloading missing records.")


    def _run_pipeline_task(self, task_class, task_name=None, **kwargs) -> None:

        '''
        The shared follow-up step calls DiseaseCountsRefreshTask for every
        node, but that task scans all GARD nodes in Memgraph. Defer only that
        full refresh here. Person extraction remains per node, while grouping,
        Agent updates, and person flag cleanup form the final build phase.
        '''
        if task_class is DiseaseCountsRefreshTask:
            return

        from pipelines.pipeline_2_clinical_trial.task_clinical_trial_5 import ClinicalTrialPmidArticleImportTask
        from pipelines.pipeline_3_publication.task_publication_3 import GardOmimPublicationMappingTask
        from pipelines.pipeline_3_publication.task_publication_4 import PublicationOminDataRetrievalTask
        from pipelines.pipeline_3_publication.task_publication_8 import NewOmimPublicationArticleImportTask
        from pipelines.pipeline_3_publication.task_publication_graph_8 import NewPublicationGardArticleRelationshipTask

        enrichment_task_name = task_class.__name__
        if task_class is ClinicalTrialPmidArticleImportTask:
            task_name = task_name or task_class.__name__
            task_class = GARDClinicalTrialPmidArticleImportTask
            kwargs["reuse"] = self.existing_data_reuse
        elif task_class is NewOmimPublicationArticleImportTask:
            task_name = task_name or task_class.__name__
            task_class = GARDOMIMPublicationArticleImportTask
            kwargs["reuse"] = self.existing_data_reuse
        elif task_class in (GardOmimPublicationMappingTask, PublicationOminDataRetrievalTask):
            '''
            Cover the remaining publication* cache tables inside the GARD
            runner only. Inherited per-GARD orchestration supplies gard_id;
            these tasks validate it before copying OMIM mappings and metadata.
            '''
            task_name = task_name or task_class.__name__
            task_class = GARDOMIMPublicationMappingTask if task_class is GardOmimPublicationMappingTask else GARDOMIMDataRetrievalTask
            kwargs["reuse"] = self.existing_data_reuse
        elif task_class is NewPublicationGardArticleRelationshipTask:
            task_name = task_name or task_class.__name__
            task_class = GARDPublicationGardArticleRelationshipTask
            kwargs["gard_id"] = self.current_gard_id

        # Substitute model-caching tasks only for this GARD runner.
        if task_class is NewClinicalTrialAnnotationTask:
            task_class = GARDModelCachingClinicalTrialAnnotationTask
            task_name = task_name or NewClinicalTrialAnnotationTask.__name__
        elif task_class is PublicationFalsePositiveFilterTask:
            task_class = GARDModelCachingPublicationFalsePositiveFilterTask
            task_name = task_name or PublicationFalsePositiveFilterTask.__name__
            kwargs["gard_id"] = self.current_gard_id
        elif task_class is NewOrganizationSourceTrackingTask:
            task_class = GARDOrganizationSourceTrackingTask
            task_name = task_name or NewOrganizationSourceTrackingTask.__name__

        '''
        Reuse the shared runner's task lifecycle and timing, with a local mixin
        that surfaces swallowed task errors. Preserve the task name and module
        so its logger and pipeline error file still resolve correctly. Workers
        receive the shared grouping function and plain values, not task objects.
        '''
        checked_task_class = task_class
        if not issubclass(task_class, GARDTaskErrorCheckingMixin):
            checked_task_class = type(task_class.__name__, (GARDTaskErrorCheckingMixin, task_class), {"__module__": task_class.__module__})
        '''
        Reused RxNorm/annotation inputs are excluded only while their expensive
        MySQL task runs. Restore is_new=1 before graph/person tasks, including
        when a task fails. Skip construction entirely when no enrichment is
        missing, so an all-cached node does not load unused annotation models.
        '''
        with self.existing_data_reuse.enrichment_scope(enrichment_task_name, self.logger) as has_work:
            if has_work:
                super()._run_pipeline_task(checked_task_class, task_name=task_name, **kwargs)


    def run_disease_counts_refresh(self) -> None:

        """Refresh disease counts after a batch of GARD nodes."""

        '''
        Disease counts use all saved MySQL and graph data, so per-node wrap-up
        can clear is_new flags before this full refresh. Call the base task
        runner directly to bypass the per-node skip above. Fail this checkpoint
        when the shared count task raises or logs an error, leaving the current
        GARD node available for a retry.
        '''
        previous_raise_task_errors = self.raise_task_errors
        self.raise_task_errors = True
        try:
            super()._run_pipeline_task(GARDCountsRefreshTask, task_name=DiseaseCountsRefreshTask.__name__)
        finally:
            self.raise_task_errors = previous_raise_task_errors


    def run_pipeline_for_each_gard_node(self) -> None:

        self.gard_discovery_completed = False

        from Z_Alert.pipelines.pipeline_1_gard.task_gard_1 import GardNodeNamesTask

        gard_task = GardNodeNamesTask()
        total_gard_nodes = 0

        '''
        Step 1:
        Read GARD nodes in batches instead of loading every GARD node into
        memory at once. Each GARD node contains the names/synonyms used by
        the downstream clinical trial and publication discovery tasks.
        '''
        try:
            self.existing_data_reuse.recover_interrupted_scopes(gard_task.mysql, self.logger)
            cursor = gard_task.mysql.cursor()
            try:
                cursor.execute("SELECT COUNT(*) FROM gard")
                gard_row_count = cursor.fetchone()[0]
            finally:
                cursor.close()

            if gard_row_count == 0:
                raise RuntimeError(
                    "MySQL table gard is empty. Complete "
                    "Z_GARD_Update/1_GARD_DBs_update/main-run.sh before running pipeline 2."
                )

            '''
            Resume this rebuild from rows whose updated date is still NULL.
            Completed nodes keep their checkpoint across restarts and date
            changes. Leave marking until the existing per-node downstream
            steps finish; the shared selector's default remains date-based
            for the recurring Alert runner.
            '''
            for batch in gard_task.get_gard_nodes(mark_updated_before_yield=False, only_unprocessed=True):
                self.logger.info(f"\n\n*** Processing GARD discovery batch with {len(batch)} nodes. ***\n")

                for gard_node in batch:
                    self.current_gard_id = gard_node["gardId"]
                    self.existing_data_reuse.begin_gard_node(self.current_gard_id)
                    '''
                    Step 2:
                    Keep the filtered_names list produced by GardNodeNamesTask.
                    filtered_names is the cleaned search-name list used to look
                    for new clinical trials and publications for this disease.
                    '''
                    filtered_names = gard_node.get("filtered_names")
                    synonyms = ", ".join(gard_node["synonyms"])

                    self.logger.info(
                        f"\n*** {gard_node['gardId']}: {gard_node['gardName']}:\n"
                        f"Synonyms      : {synonyms}\n"
                        f"Filtered names: {filtered_names}\n"
                    )

                    '''
                    Step 3:
                    Compose a JSON object
                    '''
                    target_gard_node={
                        "gardId": gard_node['gardId'],
                        "updated": self.START_DATE,
                        "filtered_names": filtered_names
                    }

                    '''
                    Step 4:
                    Search external/new clinical trial data for this GARD node.
                    Any newly discovered trial ids are staged for the later
                    clinical-trial MySQL and Memgraph update steps.
                    '''
                    clinical_trial_task = None
                    publication_task = None
                    discovery_errors = GARDTaskErrorCapture()
                    discovery_loggers = []
                    try:
                        clinical_trial_task = GARDClinicalTrialDiscoveryTask(self.existing_data_reuse)
                        clinical_trial_task.logger.addHandler(discovery_errors)
                        discovery_loggers.append(clinical_trial_task.logger)
                        clinical_trial_task.find_new_data(target_gard_node)
                        if discovery_errors.had_error:
                            raise RuntimeError(f"Clinical-trial discovery failed for {gard_node['gardId']}; leave its checkpoint pending.")

                        '''
                        Step 5:
                        Search publication data for this GARD node using the same
                        normalized search window and filtered disease names. Newly
                        discovered PMIDs are staged for later publication updates.
                        '''
                        publication_task = GARDPublicationDiscoveryTask(self.existing_data_reuse)
                        publication_task.logger.addHandler(discovery_errors)
                        discovery_loggers.append(publication_task.logger)
                        publication_task.find_new_data(target_gard_node)
                        if discovery_errors.had_error:
                            raise RuntimeError(f"Publication discovery failed for {gard_node['gardId']}; leave its checkpoint pending.")

                        # Stage only this GARD node's OMIM mappings and fetch
                        # missing shared OMIM entries before the next node.
                        self.run_omim_updates_for_gard_node(gard_node["gardId"])

                    finally:
                        '''
                        Discovery runs directly rather than through the task
                        runner. Surface its logged errors before source cleanup
                        or completion marking, then release the local handlers.
                        '''
                        for task_logger in discovery_loggers:
                            task_logger.removeHandler(discovery_errors)
                        discovery_errors.close()
                        # Later steps can run for a long time, so release these
                        # per-node database connections before starting them.
                        self._close_task_if_needed(clinical_trial_task)
                        self._close_task_if_needed(publication_task)

                    # Finish the staged MySQL and graph work before discovering
                    # the next GARD node, keeping each update batch bounded.
                    self.run_post_discovery_steps(gard_node["gardId"])

                    self._nodes_since_counts_refresh += 1
                    if self._nodes_since_counts_refresh >= self.DISEASE_COUNTS_REFRESH_NODE_BATCH_SIZE:
                        self._run_step_with_timing(
                            f"Disease counts refresh after {self._nodes_since_counts_refresh} GARD nodes",
                            self.run_disease_counts_refresh,
                        )
                        self._nodes_since_counts_refresh = 0

                    # Save the source-processing checkpoint. Pending person
                    # flags survive this checkpoint until finalization succeeds.
                    gard_task._set_updated_flag({gard_node["gardId"]})

                    total_gard_nodes += 1

                    self.logger.info(f"\n****** Processed: {gard_node['gardId']}: {gard_node['gardName']} ******\n")
                    self.logger.info(f"\n###### Total GARD nodes processed {total_gard_nodes} ######\n")

            # Refresh counts for the last partial batch. With no unprocessed
            # nodes, refresh once to finish a batch from a previous run.
            if self._nodes_since_counts_refresh or total_gard_nodes == 0:
                self._run_step_with_timing(
                    "Final disease counts refresh",
                    self.run_disease_counts_refresh,
                )
                self._nodes_since_counts_refresh = 0

            # Final person processing is allowed only after the complete loop
            # and its final count refresh return successfully, including a
            # restart that finds no remaining GARD nodes.
            self.gard_discovery_completed = True

        finally:
            self._close_task_if_needed(gard_task)
            self.logger.info(f"Processed {total_gard_nodes} GARD nodes.")


    def run_publication_mysql_updates(self) -> None:

        '''
        Import current OMIM reference Articles before enrichment, so copied and
        newly downloaded OMIM PMIDs receive the same classification/PubTator/
        substance processing as direct-search and trial-reference PMIDs in this
        GARD iteration. Source OMIM mappings were staged per current GARD ID by
        discovery. The regular Alert runner retains its existing task order.
        '''
        from pipelines.pipeline_3_publication.task_publication_2_local import PublicationEpiNhsClassificationTask
        from pipelines.pipeline_3_publication.task_publication_5 import NewPublicationPubtatorRetrievalTask
        from pipelines.pipeline_3_publication.task_publication_6 import NewPublicationChemicalSubstanceTask
        from pipelines.pipeline_3_publication.task_publication_8 import NewOmimPublicationArticleImportTask

        self._run_pipeline_task(NewOmimPublicationArticleImportTask)
        self._run_pipeline_task(PublicationEpiNhsClassificationTask)
        self._run_pipeline_task(NewPublicationPubtatorRetrievalTask)
        self._run_pipeline_task(NewPublicationChemicalSubstanceTask)
        self._run_pipeline_task(PublicationFalsePositiveFilterTask)


    def run_person_extraction(self) -> None:

        """Extract publication, clinical-trial, and grant people before source wrap-up."""

        '''
        These tasks consume the source tables' is_new flags and create pending
        person rows. Finish extraction before each source wrap-up, but retain
        the resulting person is_new flags throughout the one-time build.
        '''
        from pipelines.pipeline_6_person.task_person_1_publication import NewPublicationPersonTask
        from pipelines.pipeline_6_person.task_person_2_clinical_trial import NewClinicalTrialPersonTask
        from pipelines.pipeline_6_person.task_person_3_grant import NewGrantPersonTask

        self._run_pipeline_task(NewPublicationPersonTask)
        self._run_pipeline_task(NewClinicalTrialPersonTask)
        self._run_pipeline_task(NewGrantPersonTask)


    def _run_person_grouping_and_graph_updates(self) -> None:

        """Group the accumulated people, then build Agents from bounded source batches."""

        '''
        Agent identity depends on the group IDs assigned by the grouping task,
        so these two tasks remain together and in this order. The rebuild-only
        graph task reconciles all imported trials using their latest study
        data; it does not depend on source is_new flags already cleared per node.
        '''
        self._run_pipeline_task(GARDPersonGroupingTask)
        self._run_pipeline_task(GARDPersonAgentGraphTask)


    def _run_person_organization_updates(self) -> None:

        """Finish organization enrichment and source tracking for the final person backlog."""

        '''
        Consume person affiliations once after Agents exist. Trial/project
        source tracking has already run per node; replaying it here would
        depend on source flags that have deliberately been cleared.
        '''
        from pipelines.pipeline_7_graph_maintenance.task_pipeline_maintenance_1 import OrganizationLocationRorLookupTask
        from pipelines.pipeline_7_graph_maintenance.task_pipeline_maintenance_2 import OrganizationLocationGraphSyncTask
        from pipelines.pipeline_7_graph_maintenance.task_pipeline_maintenance_wrapup import OrganizationLocationMaintenanceWrapUpTask

        self._run_pipeline_task(OrganizationLocationRorLookupTask)
        self._run_pipeline_task(OrganizationLocationGraphSyncTask)
        self._run_pipeline_task(GARDOrganizationSourceTrackingTask, person_only=True)
        self._run_pipeline_task(OrganizationLocationMaintenanceWrapUpTask)


    def run_final_person_updates(self) -> None:

        """Finalize people after all GARD nodes, retaining pending flags on any failure."""

        from pipelines.pipeline_6_person.task_person_pipeline_wrapup import PersonPipelineWrapUpTask

        if not self.gard_discovery_completed:
            raise RuntimeError("Finish all GARD source processing before final person updates.")

        '''
        This phase also runs when a restart finds zero unprocessed GARD nodes.
        Their source checkpoints can already be saved while person is_new
        still identifies an unfinished grouping/Agent backlog. Never put the
        person reset in finally: any failed task must leave that work pending.
        '''
        self._run_step_with_timing("Final person grouping and Agent graph updates", self._run_person_grouping_and_graph_updates)
        self._run_step_with_timing("Final person organization updates", self._run_person_organization_updates)
        self._run_pipeline_task(PersonPipelineWrapUpTask)


    def run_pipeline_wrapup(self) -> None:

        """Clear consumed source flags for one GARD node while retaining pending people."""

        '''
        Clinical-trial/publication work and their person extraction have
        finished for this node. Clearing those source flags keeps the next
        node's enrichment bounded. Person flags must remain set until the
        separate final phase succeeds, so do not run PersonPipelineWrapUpTask.
        '''
        from pipelines.pipeline_2_clinical_trial.task_clinical_trial_pipeline_wrapup import ClinicalTrialPipelineWrapUpTask
        from pipelines.pipeline_3_publication.task_publication_pipeline_wrapup import PublicationPipelineWrapUpTask
        from pipelines.pipeline_7_graph_maintenance.task_pipeline_maintenance_wrapup import OrganizationLocationMaintenanceWrapUpTask

        self._run_pipeline_task(ClinicalTrialPipelineWrapUpTask)
        self._run_pipeline_task(PublicationPipelineWrapUpTask)
        self._run_pipeline_task(OrganizationLocationMaintenanceWrapUpTask)


    def run_post_discovery_steps(self, gard_id: str) -> None:

        """Finish source processing and person extraction for one GARD node."""

        steps = (
            ("Step 2: clinical-trial MySQL updates", self.run_clinical_trial_mysql_updates),
            ("Step 3: publication MySQL updates", self.run_publication_mysql_updates),
            ("Step 5: clinical-trial graph updates", self.run_clinical_trial_graph_updates),
            ("Step 6: publication graph updates", self.run_publication_graph_updates),
            ("Step 7: per-node pipeline follow-up updates", self.run_pipeline_followup_updates),
            #("Step 8: send alert emails", self.send_alert_emails), #NO NEED to send alerts
            ("Step 9a: extract people", self.run_person_extraction),
            ("Step 10: pipeline maintenance", self.run_pipeline_maintenance),
            ("Step 11: pipeline wrap-up", self.run_pipeline_wrapup),
        )

        '''
        Extract people before source cleanup, but retain their pending flags.
        Grouping, Agent writes, and affiliation tracking run only in the final
        phase after this complete GARD loop. The count checkpoint is unchanged.
        '''
        for step_name, step_func in steps:
            self._run_step_with_timing(f"{gard_id}: {step_name}", step_func)


if __name__ == "__main__":

    runner = GARDToMySQLAndMemgraphUpdateRunner()

    try:
        runner._run_step_with_timing(
            "GARD nodes: discovery and per-node updates",
            runner.run_pipeline_for_each_gard_node,
        )
        runner._run_step_with_timing(
            "Final person processing after all GARD nodes",
            runner.run_final_person_updates,
        )

    finally:
        runner.close()
        runner._archive_log_files_by_date()

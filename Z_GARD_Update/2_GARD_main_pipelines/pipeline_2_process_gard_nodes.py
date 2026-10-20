
import os
import sys
import logging
from pathlib import Path

import psutil
import spacy


PROJECT_ROOT = Path(__file__).resolve().parents[2]
Z_ALERT_ROOT = PROJECT_ROOT / "Z_Alert"
for import_path in (str(PROJECT_ROOT), str(Z_ALERT_ROOT)):
    if import_path not in sys.path:
        sys.path.insert(0, import_path)

from Z_Alert.main import AlertPipelineRunner
from baseclass.conn import DBConnection
from pipelines.pipeline_base import PipelineBase
from pipelines.pipeline_2_clinical_trial.task_clinical_trial_6 import NewClinicalTrialAnnotationTask
from pipelines.pipeline_3_publication.task_publication_7 import PublicationFalsePositiveFilterTask
from pipelines.pipeline_5_followup.task_pipeline_followup_update_3 import DiseaseCountsRefreshTask
from pipelines.pipeline_6_person.task_person_4_grouping import NewPersonGroupingTask
from pipelines.pipeline_6_person.task_person_5_graph import NewPersonAgentGraphTask
from pipelines.pipeline_6_person.task_person_pipeline_wrapup import PersonPipelineWrapUpTask
from pipelines.pipeline_7_graph_maintenance.task_pipeline_maintenance_3 import NewOrganizationSourceTrackingTask
from utils.tools import _resolve_worker_count


PENDING_AGENT_TRIAL_TABLE = "gard_pending_agent_trial"


class GARDCheckpointErrorCapture(logging.Handler):
    """Remember task errors that a shared Alert task logs without raising."""

    def __init__(self):

        super().__init__(level=logging.ERROR)
        self.had_error = False

    def emit(self, record):

        self.had_error = True


class GARDCheckpointTaskErrorGuard:
    """Fail a GARD checkpoint when its task logged an internal error."""

    def process_new_data(self) -> None:

        '''
        Several shared Alert tasks catch their own exceptions and only log an
        error. At a checkpoint, clearing person flags or the pending trial
        worklist after such an error would lose the rows needed for retry.
        Capture ERROR records from this task and make the checkpoint fail,
        while leaving the shared task behavior unchanged for the Alert runner.
        '''
        task_logger = self.logger
        error_capture = GARDCheckpointErrorCapture()
        task_logger.addHandler(error_capture)
        try:
            super().process_new_data()
        finally:
            task_logger.removeHandler(error_capture)
            error_capture.close()

        if error_capture.had_error:
            raise RuntimeError(f"{type(self).__name__} logged an error; checkpoint rows remain staged.")


class GARDCheckpointDiseaseCountsTask(GARDCheckpointTaskErrorGuard, DiseaseCountsRefreshTask):
    """Run the full disease count refresh only at a GARD checkpoint."""

    __module__ = DiseaseCountsRefreshTask.__module__


class GARDCheckpointPersonGroupingTask(GARDCheckpointTaskErrorGuard, NewPersonGroupingTask):
    """Group all new people accumulated since the previous checkpoint."""

    __module__ = NewPersonGroupingTask.__module__
    MAX_CHECKPOINT_WORKERS = 4
    RAM_BUDGET_PER_WORKER = 6 * 1024 ** 3

    def _resolve_max_workers(self) -> int:

        '''
        Person disambiguation is CPU-bound, so it uses processes. On macOS
        each worker starts a separate Python interpreter and imports the NLP
        dependencies; the GARD parent also holds cached models. Cap the pool
        by available RAM as well as CPU so a large 500-node checkpoint does
        not launch more workers than the machine can keep alive. Honor a lower
        PERSON_GROUPING_MAX_WORKERS request from the operator.
        '''
        cpu_and_requested_limit = _resolve_worker_count(
            self.DEFAULT_MAX_WORKERS,
            env_var_name="PERSON_GROUPING_MAX_WORKERS",
            logger=self.logger,
        )
        ram_limit = max(1, psutil.virtual_memory().available // self.RAM_BUDGET_PER_WORKER)
        resolved = min(cpu_and_requested_limit, self.MAX_CHECKPOINT_WORKERS, ram_limit)
        self.logger.info(f"GARD person grouping uses {resolved} process workers after CPU and RAM caps.")
        return resolved


class GARDStreamingMySQLConnection:
    """Use streaming cursors for a large Agent checkpoint's person row scan."""

    def __init__(self, connection):

        self._connection = connection

    def cursor(self, *args, **kwargs):

        # The shared Agent task fetches person rows in 500-row batches but asks
        # MySQL Connector for a buffered cursor, which first loads the entire
        # 500-node result into memory. Its query results are fully consumed
        # before the next query, so an unbuffered cursor preserves behavior
        # while bounding parent-process memory for this GARD task only.
        if kwargs.get("dictionary") and kwargs.get("buffered"):
            kwargs["buffered"] = False
        return self._connection.cursor(*args, **kwargs)

    def __getattr__(self, name):

        return getattr(self._connection, name)


class GARDCheckpointAgentGraphTask(GARDCheckpointTaskErrorGuard, NewPersonAgentGraphTask):
    """Build Agents and clean changed trials recorded before per-node wrap-up."""

    __module__ = NewPersonAgentGraphTask.__module__

    # The clinical trial wrap-up clears is_new after each GARD node. The durable
    # worklist preserves those NCT IDs, including trials whose people were all
    # removed and therefore have no new person rows at the checkpoint.
    FETCH_CHANGED_CLINICAL_TRIAL_NCTIDS_QUERY = f'''
        SELECT nctid FROM {PENDING_AGENT_TRIAL_TABLE}
    '''
    FETCH_CHANGED_CLINICAL_TRIAL_STUDIES_QUERY = f'''
        SELECT c.nctid, c.studies
        FROM clinical_trial_unique AS c
        INNER JOIN {PENDING_AGENT_TRIAL_TABLE} AS pending ON pending.nctid = c.nctid
        WHERE c.studies IS NOT NULL
    '''

    def process_new_data(self) -> None:

        self.mysql = GARDStreamingMySQLConnection(self.mysql)
        super().process_new_data()


class GARDPerNodeOrganizationSourceTrackingTask(NewOrganizationSourceTrackingTask):
    """Track trial and grant sources per node, leaving person sources staged."""

    __module__ = NewOrganizationSourceTrackingTask.__module__

    def track_person_sources(self) -> None:

        # The person.is_new rows persist until the 500-node checkpoint. Scanning
        # them after every node would repeat an ever-growing source worklist.
        self.logger.info("Person Organization sources are deferred to the GARD checkpoint.")


class GARDCheckpointPersonSourceTrackingTask(GARDCheckpointTaskErrorGuard, NewOrganizationSourceTrackingTask):
    """Track staged person affiliations once before their is_new flags clear."""

    __module__ = NewOrganizationSourceTrackingTask.__module__

    def track_clinical_trial_sources(self) -> None:

        # These sources were consumed before each clinical trial wrap-up.
        pass

    def track_core_project_sources(self) -> None:

        # Grant sources are consumed with the per-node maintenance step.
        pass


class GARDCheckpointPersonWrapUpTask(GARDCheckpointTaskErrorGuard, PersonPipelineWrapUpTask):
    """Clear staged person flags only after grouping, Agent, and source tasks."""

    __module__ = PersonPipelineWrapUpTask.__module__


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

    def __init__(self):

        # Bypass only the parent's spacy.load(); PipelineBase still opens per-task resources.
        PipelineBase.__init__(self, init_mysql=True, init_memgraph=False)
        try:
            self.api_key = os.getenv("UMLS_API_KEY")
            self.umls_search_api = os.getenv("UMLS_SEARCH_API")
            self.umls_cui_api_template = os.getenv("UMLS_CUI_API_TEMPLATE")

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

    # Full counts and person regrouping run after this many completed diseases.
    SHARED_CHECKPOINT_NODE_BATCH_SIZE = 500

    def __init__(self):

        super().__init__()

        self.START_DATE = '1971-01-01'
        self._nodes_since_shared_checkpoint = 0
        self._gard_mysql = None

        self.logger.info(
            "GARDToMySQLAndMemgraphUpdateRunner initialized; "
            f"shared counts and person tasks run every {self.SHARED_CHECKPOINT_NODE_BATCH_SIZE} GARD nodes."
        )


    def _run_pipeline_task(self, task_class, task_name=None, **kwargs) -> None:

        '''
        These shared tasks can consume work from many GARD nodes together.
        Keep person.is_new set until the checkpoint. Changed clinical trial
        IDs are saved before each per-node wrap-up so later Agent graph cleanup
        still sees every trial changed in the checkpoint window.
        '''
        if task_class in (
            DiseaseCountsRefreshTask,
            NewPersonGroupingTask,
            NewPersonAgentGraphTask,
            PersonPipelineWrapUpTask,
        ):
            return

        if task_class is NewOrganizationSourceTrackingTask:
            task_class = GARDPerNodeOrganizationSourceTrackingTask
            task_name = task_name or NewOrganizationSourceTrackingTask.__name__

        # Substitute model-caching tasks only for this GARD runner.
        if task_class is NewClinicalTrialAnnotationTask:
            task_class = GARDModelCachingClinicalTrialAnnotationTask
            task_name = task_name or NewClinicalTrialAnnotationTask.__name__
        elif task_class is PublicationFalsePositiveFilterTask:
            task_class = GARDModelCachingPublicationFalsePositiveFilterTask
            task_name = task_name or PublicationFalsePositiveFilterTask.__name__

        super()._run_pipeline_task(task_class, task_name=task_name, **kwargs)


    def run_shared_counts_and_person_checkpoint(self) -> None:

        """Refresh counts and consume the accumulated person and trial worklist."""

        '''
        The full count refresh is independent of is_new flags. Person grouping,
        Agent creation, and affiliation tracking must all finish before person
        flags and changed-trial IDs are cleared. The shared Alert runner normally
        continues after task errors, so use fail-fast execution only here; a
        failed checkpoint must leave its durable worklist available for retry.
        '''
        previous_raise_task_errors = self.raise_task_errors
        self.raise_task_errors = True
        try:
            tasks = (
                (GARDCheckpointDiseaseCountsTask, DiseaseCountsRefreshTask.__name__),
                (GARDCheckpointPersonGroupingTask, NewPersonGroupingTask.__name__),
                (GARDCheckpointAgentGraphTask, NewPersonAgentGraphTask.__name__),
                (GARDCheckpointPersonSourceTrackingTask, NewOrganizationSourceTrackingTask.__name__),
                (GARDCheckpointPersonWrapUpTask, PersonPipelineWrapUpTask.__name__),
            )
            for task_class, task_name in tasks:
                super()._run_pipeline_task(task_class, task_name=task_name)

            checkpoint_mysql = DBConnection().mysql_conn()
            if checkpoint_mysql is None:
                raise RuntimeError("Cannot connect to MySQL to clear the GARD checkpoint worklist.")
            try:
                cursor = checkpoint_mysql.cursor()
                try:
                    cursor.execute(f"DELETE FROM {PENDING_AGENT_TRIAL_TABLE}")
                    cleared_trials = cursor.rowcount
                    checkpoint_mysql.commit()
                    self.logger.info(f"Cleared {cleared_trials} changed clinical trials after the person checkpoint.")
                finally:
                    cursor.close()
            finally:
                checkpoint_mysql.close()
        finally:
            self.raise_task_errors = previous_raise_task_errors


    def ensure_pending_agent_trial_table(self) -> None:

        """Create the durable changed-trial worklist before processing GARD nodes."""

        cursor = self._gard_mysql.cursor()
        try:
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {PENDING_AGENT_TRIAL_TABLE} (
                    nctid VARCHAR(255) NOT NULL PRIMARY KEY,
                    created DATETIME DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_unicode_ci
            ''')
            self._gard_mysql.commit()
        finally:
            cursor.close()


    def stage_pending_agent_trials(self) -> None:

        """Save changed NCT IDs before the per-node clinical trial wrap-up."""

        '''
        A changed trial may have no new person rows when all its old contacts
        or investigators were removed. Saving the NCT ID from is_new=1 now is
        the only way the later Agent checkpoint can remove stale graph edges
        after the normal per-node wrap-up clears clinical_trial_unique.is_new.
        INSERT IGNORE keeps retries and shared NCT IDs idempotent.
        '''
        cursor = self._gard_mysql.cursor()
        try:
            cursor.execute(f'''
                INSERT IGNORE INTO {PENDING_AGENT_TRIAL_TABLE} (nctid)
                SELECT nctid FROM clinical_trial_unique
                WHERE is_new = 1 AND nctid IS NOT NULL
            ''')
            staged_trials = cursor.rowcount
            self._gard_mysql.commit()
            self.logger.info(f"Staged {staged_trials} changed clinical trials for the next Agent checkpoint.")
        except Exception:
            self._gard_mysql.rollback()
            raise
        finally:
            cursor.close()



    def run_pipeline_for_each_gard_node(self) -> None:

        from Z_Alert.pipelines.pipeline_1_gard.task_gard_1 import GardNodeNamesTask
        from Z_Alert.pipelines.pipeline_2_clinical_trial.task_clinical_trial_1 import NewClinicalTrialDiscoveryTask
        from Z_Alert.pipelines.pipeline_3_publication.task_publication_1 import NewPublicationDiscoveryTask

        gard_task = GardNodeNamesTask()
        self._gard_mysql = gard_task.mysql

        total_gard_nodes = 0

        '''
        Step 1:
        Read GARD nodes in batches instead of loading every GARD node into
        memory at once. Each GARD node contains the names/synonyms used by
        the downstream clinical trial and publication discovery tasks.
        '''
        try:
            self.ensure_pending_agent_trial_table()
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

            for batch in gard_task.get_gard_nodes(mark_updated_before_yield=False):
                self.logger.info(f"\n\n*** Processing GARD discovery batch with {len(batch)} nodes. ***\n")

                for gard_node in batch:
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
                    try:
                        clinical_trial_task = NewClinicalTrialDiscoveryTask()
                        clinical_trial_task.find_new_data(target_gard_node)

                        '''
                        Step 5:
                        Search publication data for this GARD node using the same
                        normalized search window and filtered disease names. Newly
                        discovered PMIDs are staged for later publication updates.
                        '''
                        publication_task = NewPublicationDiscoveryTask()
                        publication_task.find_new_data(target_gard_node)

                        # Stage only this GARD node's OMIM mappings and fetch
                        # missing shared OMIM entries before the next node.
                        self.run_omim_updates_for_gard_node(gard_node["gardId"])

                    finally:
                        # Later steps can run for a long time, so release these
                        # per-node database connections before starting them.
                        self._close_task_if_needed(clinical_trial_task)
                        self._close_task_if_needed(publication_task)

                    # Finish the staged MySQL and graph work before discovering
                    # the next GARD node, keeping each update batch bounded.
                    self.run_post_discovery_steps(gard_node["gardId"])

                    self._nodes_since_shared_checkpoint += 1
                    if self._nodes_since_shared_checkpoint >= self.SHARED_CHECKPOINT_NODE_BATCH_SIZE:
                        self._run_step_with_timing(
                            f"Shared counts and person checkpoint after {self._nodes_since_shared_checkpoint} GARD nodes",
                            self.run_shared_counts_and_person_checkpoint,
                        )
                        self._nodes_since_shared_checkpoint = 0

                    # Person rows and changed trial IDs stay staged until the
                    # next checkpoint, even if this process is restarted.
                    gard_task._set_updated_flag({gard_node["gardId"]})

                    total_gard_nodes += 1

                    self.logger.info(f"\n****** Processed: {gard_node['gardId']}: {gard_node['gardName']} ******\n")
                    self.logger.info(f"\n###### Total GARD nodes processed {total_gard_nodes} ######\n")

            # Complete the last partial batch. Also run once when there are no
            # unprocessed GARD nodes, so a restart can finish a previously
            # interrupted batch whose nodes were already marked updated.
            if self._nodes_since_shared_checkpoint or total_gard_nodes == 0:
                self._run_step_with_timing(
                    "Final shared counts and person checkpoint",
                    self.run_shared_counts_and_person_checkpoint,
                )
                self._nodes_since_shared_checkpoint = 0

        finally:
            self.gard_discovery_completed = True
            self._close_task_if_needed(gard_task)
            self.logger.info(f"Processed {total_gard_nodes} GARD nodes.")

    def run_post_discovery_steps(self, gard_id: str) -> None:

        """Finish per-node work and retain person rows for the next checkpoint."""

        steps = (
            ("Step 2: clinical-trial MySQL updates", self.run_clinical_trial_mysql_updates),
            ("Step 3: publication MySQL updates", self.run_publication_mysql_updates),
            ("Step 5: clinical-trial graph updates", self.run_clinical_trial_graph_updates),
            ("Step 6: publication graph updates", self.run_publication_graph_updates),
            ("Step 7: per-node pipeline follow-up updates", self.run_pipeline_followup_updates),
            #("Step 8: send alert emails", self.send_alert_emails), #NO NEED to send alerts
            ("Step 9: stage people for the checkpoint", self.run_regroup_the_person),
            ("Step 10: pipeline maintenance", self.run_pipeline_maintenance),
            ("Stage changed clinical trials for the Agent checkpoint", self.stage_pending_agent_trials),
            ("Step 11: pipeline wrap-up", self.run_pipeline_wrapup),
        )

        # Trial and publication rows are consumed and reset for this GARD node.
        # Person rows stay is_new=1 until grouping, Agent graph creation, and
        # person-source tracking all succeed at the 500-node checkpoint.
        # The index initialization step remains disabled, as in Z_Alert/main.py.
        for step_name, step_func in steps:
            self._run_step_with_timing(f"{gard_id}: {step_name}", step_func)


if __name__ == "__main__":

    runner = GARDToMySQLAndMemgraphUpdateRunner()

    try:
        runner._run_step_with_timing(
            "GARD nodes: discovery and per-node updates",
            runner.run_pipeline_for_each_gard_node,
        )

    finally:
        runner.close()
        runner._archive_log_files_by_date()

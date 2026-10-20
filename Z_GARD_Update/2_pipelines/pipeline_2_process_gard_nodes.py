
import os
import sys
from pathlib import Path

import spacy


PROJECT_ROOT = Path(__file__).resolve().parents[2]
Z_ALERT_ROOT = PROJECT_ROOT / "Z_Alert"
for import_path in (str(PROJECT_ROOT), str(Z_ALERT_ROOT)):
    if import_path not in sys.path:
        sys.path.insert(0, import_path)

from Z_Alert.main import AlertPipelineRunner
from pipelines.pipeline_base import PipelineBase
from pipelines.pipeline_2_clinical_trial.task_clinical_trial_6 import NewClinicalTrialAnnotationTask
from pipelines.pipeline_3_publication.task_publication_7 import PublicationFalsePositiveFilterTask


class GARDClinicalTrialAnnotationTask(NewClinicalTrialAnnotationTask):
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

        if GARDClinicalTrialAnnotationTask._cached_models is not None:
            self.logger.info("Reusing clinical-trial annotation models and UMLS linkers loaded for this GARD run.")
            return GARDClinicalTrialAnnotationTask._cached_models

        models = super().load_models()
        # Do not cache a partial or failed load; the next node can retry it.
        if all(model is not None for model in models):
            GARDClinicalTrialAnnotationTask._cached_models = models

        return models


class GARDPublicationFalsePositiveFilterTask(PublicationFalsePositiveFilterTask):
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

            if GARDPublicationFalsePositiveFilterTask._cached_nlp is None:
                GARDPublicationFalsePositiveFilterTask._cached_nlp = spacy.load("en_core_web_sm")
                self.logger.info("Loaded en_core_web_sm for GARD publication filtering.")
            else:
                self.logger.info("Reusing en_core_web_sm for GARD publication filtering.")

            self.nlp = GARDPublicationFalsePositiveFilterTask._cached_nlp
        except Exception:
            self.close()
            raise


class GARDToMySQLAndMemgraphUpdateRunner(AlertPipelineRunner):

    def __init__(self):

        super().__init__()

        self.START_DATE = '1971-01-01'

        self.logger.info("GARDToMySQLAndMemgraphUpdateRunner initialized.")


    def _run_pipeline_task(self, task_class, task_name=None, **kwargs) -> None:

        # Substitute model-caching tasks only for this GARD runner.
        if task_class is NewClinicalTrialAnnotationTask:
            task_class = GARDClinicalTrialAnnotationTask
            task_name = task_name or NewClinicalTrialAnnotationTask.__name__
        elif task_class is PublicationFalsePositiveFilterTask:
            task_class = GARDPublicationFalsePositiveFilterTask
            task_name = task_name or PublicationFalsePositiveFilterTask.__name__

        super()._run_pipeline_task(task_class, task_name=task_name, **kwargs)



    def run_pipeline_for_each_gard_node(self) -> None:

        from Z_Alert.pipelines.pipeline_1_gard.task_gard_1 import GardNodeNamesTask
        from Z_Alert.pipelines.pipeline_2_clinical_trial.task_clinical_trial_1 import NewClinicalTrialDiscoveryTask
        from Z_Alert.pipelines.pipeline_3_publication.task_publication_1 import NewPublicationDiscoveryTask

        gard_task = GardNodeNamesTask()

        total_gard_nodes = 0

        '''
        Step 1:
        Read GARD nodes in batches instead of loading every GARD node into
        memory at once. Each GARD node contains the names/synonyms used by
        the downstream clinical trial and publication discovery tasks.
        '''
        try:
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
                    gard_task._set_updated_flag({gard_node["gardId"]})

                    total_gard_nodes += 1

                    self.logger.info(f"\n****** Processed: {gard_node['gardId']}: {gard_node['gardName']} ******\n")
                    self.logger.info(f"\n###### Total GARD nodes processed {total_gard_nodes} ######\n")

        finally:
            self.gard_discovery_completed = True
            self._close_task_if_needed(gard_task)
            self.logger.info(f"Processed {total_gard_nodes} GARD nodes.")

    def run_post_discovery_steps(self, gard_id: str) -> None:

        """Consume staged rows before advancing to the next GARD node."""

        steps = (
            ("Step 2: clinical-trial MySQL updates", self.run_clinical_trial_mysql_updates),
            ("Step 3: publication MySQL updates", self.run_publication_mysql_updates),
            ("Step 5: clinical-trial graph updates", self.run_clinical_trial_graph_updates),
            ("Step 6: publication graph updates", self.run_publication_graph_updates),
            ("Step 7: pipeline follow-up updates", self.run_pipeline_followup_updates),
            #("Step 8: send alert emails", self.send_alert_emails), #NO NEED to send alerts
            ("Step 9: regroup people", self.run_regroup_the_person),
            ("Step 10: pipeline maintenance", self.run_pipeline_maintenance),
            ("Step 11: pipeline wrap-up", self.run_pipeline_wrapup),
        )

        # The shared tasks consume staged rows, then wrap-up resets their flags
        # before discovery moves to the next GARD node.
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

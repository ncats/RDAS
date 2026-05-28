import os
import shutil
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Optional, Type
from dotenv import load_dotenv

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, ".")),
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])
load_dotenv(os.path.abspath(os.path.join(_dir, "..", ".env")))

from utils.applogger import AppLogger
from utils.tools import _time_hms
from init_index import MemgraphIndexInitializationTask

class AlertPipelineRunner:
    """
    This class owns the high-level order only. Each individual pipeline task
    still opens, uses, and closes its own database connections.
    """
 
    def __init__(self, look_back_days: int = 7):

        self.look_back_days = look_back_days
        self.log_dir = os.path.expanduser(os.getenv("ALERT_LOG_DIR", "logs"))
        os.makedirs(self.log_dir, exist_ok=True)

        class_name = type(self).__name__
        self.log_file = f"{self.log_dir}/alert-{class_name}.log"
        self.logger = AppLogger(class_name, self.log_file).get_logger()

        self.logger.info(f'\n\n{"*" * 20} The {class_name} is initialized. {"*" * 20}\n')
        self.logger.info(f"AlertPipelineRunner configured with look_back_days={self.look_back_days}.")



    def run_find_new_clinical_trial_and_publication_updates(self) -> None:
        """Search for new clinical trials and publications for updated GARD nodes."""

        total_gard_nodes = 0
        gard_task = None
        clinical_trial_task = None
        publication_task = None

        try:
            from pipelines.pipeline_1_gard.task_gard_1 import GardNodeNamesTask
            from pipelines.pipeline_2_clinical_trial.task_clinical_trial_1 import NewClinicalTrialDiscoveryTask
            from pipelines.pipeline_3_publication.task_publication_1 import NewPublicationDiscoveryTask

            gard_task = GardNodeNamesTask()
            clinical_trial_task = NewClinicalTrialDiscoveryTask()
            publication_task = NewPublicationDiscoveryTask()

            '''
            Step 1:
            Read GARD nodes in batches instead of loading every GARD node into
            memory at once. Each GARD node contains the names/synonyms used by
            the downstream clinical trial and publication discovery tasks.
            '''
            for batch in gard_task.get_gard_nodes():
                self.logger.info(f"\n\n*** Processing GARD discovery batch with {len(batch)} nodes. ***\n")

                for gard_node in batch:
                    '''
                    Step 2:
                    Keep the filtered_names list produced by GardNodeNamesTask.
                    filtered_names is the cleaned search-name list used to look
                    for new clinical trials and publications for this disease.
                    '''
                    filtered_names = gard_node.get("filtered_names")

                    '''
                    Step 3:
                    Use the GARD node's stored updated date as the lower bound
                    for discovery. If the node has never been updated, fall back
                    to today minus look_back_days so the first alert run searches
                    a controlled recent window instead of an unbounded history.
                    '''
                    last_update_date = gard_node.get("updated")
                    if last_update_date is None:
                        last_update_date = date.today() - timedelta(days=self.look_back_days)

                    '''
                    Step 4:
                    Write the normalized values back onto gard_node before
                    passing it downstream. Both discovery tasks expect the node
                    dictionary to include:

                        updated:
                            the date to search after
                        filtered_names:
                            the disease names/synonyms to search with
                    '''
                    gard_node["updated"] = last_update_date
                    gard_node["filtered_names"] = filtered_names

                    '''
                    Step 5:
                    Read the GARD id and display name for logging. The graph and
                    query layers may return either camelCase or snake_case keys,
                    so both forms are checked before falling back to defaults.
                    '''
                    gard_id = gard_node.get("gardId") or gard_node.get("gard_id") or "UNKNOWN_GARD_ID"
                    gard_name = gard_node.get("gardName") or gard_node.get("gard_name") or ""
                    self.logger.info(f'processing GARD ID: {gard_id}, name: {gard_name}')

                    '''
                    Step 6:
                    Search external/new clinical trial data for this GARD node.
                    Any newly discovered trial ids are staged for the later
                    clinical-trial MySQL and Memgraph update steps.
                    '''
                    clinical_trial_task.find_new_data(gard_node)

                    '''
                    Step 7:
                    Search publication data for this GARD node using the same
                    normalized search window and filtered disease names. Newly
                    discovered PMIDs are staged for later publication updates.
                    '''
                    publication_task.find_new_data(gard_node) 

                    total_gard_nodes += 1
                
                self.logger.info(f"\n# Total GARD nodes processed {total_gard_nodes}\n")

        except Exception as e:
            self.logger.error(f"run_find_new_clinical_trial_and_publication_updates() failed: {e}") 

        finally:
            for task in (clinical_trial_task, publication_task, gard_task):
                self._close_task_if_needed(task)

            self.logger.info(f"Processed {total_gard_nodes} GARD nodes.")



    def run_clinical_trial_mysql_updates(self) -> None:
        """Run clinical-trial MySQL staging and enrichment tasks."""

        # Import here because ClinicalTrialDrugInterventionMappingTask loads the spaCy model.
        from pipelines.pipeline_2_clinical_trial.task_clinical_trial_2 import NewClinicalTrialImportTask
        from pipelines.pipeline_2_clinical_trial.task_clinical_trial_3 import ClinicalTrialDrugInterventionMappingTask
        from pipelines.pipeline_2_clinical_trial.task_clinical_trial_4 import ClinicalTrialPublicationMappingTask
        from pipelines.pipeline_2_clinical_trial.task_clinical_trial_5 import ClinicalTrialPmidArticleImportTask
        from pipelines.pipeline_2_clinical_trial.task_clinical_trial_6 import NewClinicalTrialAnnotationTask

        self._run_pipeline_task(NewClinicalTrialImportTask)
        self._run_pipeline_task(ClinicalTrialDrugInterventionMappingTask)
        self._run_pipeline_task(ClinicalTrialPublicationMappingTask)
        self._run_pipeline_task(ClinicalTrialPmidArticleImportTask)
        self._run_pipeline_task(NewClinicalTrialAnnotationTask)


    def run_clinical_trial_graph_updates(self) -> None:
        """Run clinical-trial Memgraph node and relationship update tasks."""

        from pipelines.pipeline_2_clinical_trial.task_clinical_trial_graph_1 import NewClinicalTrialGraphTask
        from pipelines.pipeline_2_clinical_trial.task_clinical_trial_graph_2 import NewClinicalTrialGardRelationshipTask
        from pipelines.pipeline_2_clinical_trial.task_clinical_trial_graph_3 import NewClinicalTrialConditionGraphTask
        from pipelines.pipeline_2_clinical_trial.task_clinical_trial_graph_4 import NewClinicalTrialInterventionGraphTask
        from pipelines.pipeline_2_clinical_trial.task_clinical_trial_graph_5 import NewClinicalTrialDrugGraphTask
        from pipelines.pipeline_2_clinical_trial.task_clinical_trial_graph_6 import NewClinicalTrialParticipantGraphTask
        from pipelines.pipeline_2_clinical_trial.task_clinical_trial_graph_7 import NewClinicalTrialPrimaryOutcomeGraphTask
        from pipelines.pipeline_2_clinical_trial.task_clinical_trial_graph_8 import NewClinicalTrialStudyDesignGraphTask
        from pipelines.pipeline_2_clinical_trial.task_clinical_trial_graph_9 import NewClinicalTrialIndividualPatientDataGraphTask
        from pipelines.pipeline_2_clinical_trial.task_clinical_trial_graph_10 import NewClinicalTrialAnnotationGraphTask
        from pipelines.pipeline_2_clinical_trial.task_clinical_trial_graph_11 import NewClinicalTrialOrganizationLocationGraphTask
 
        self._run_pipeline_task(NewClinicalTrialGraphTask)
        self._run_pipeline_task(NewClinicalTrialGardRelationshipTask)
        self._run_pipeline_task(NewClinicalTrialConditionGraphTask)
        self._run_pipeline_task(NewClinicalTrialInterventionGraphTask)
        self._run_pipeline_task(NewClinicalTrialDrugGraphTask)
        self._run_pipeline_task(NewClinicalTrialParticipantGraphTask)
        self._run_pipeline_task(NewClinicalTrialPrimaryOutcomeGraphTask)
        self._run_pipeline_task(NewClinicalTrialStudyDesignGraphTask)
        self._run_pipeline_task(NewClinicalTrialIndividualPatientDataGraphTask)
        self._run_pipeline_task(NewClinicalTrialAnnotationGraphTask)
        self._run_pipeline_task(NewClinicalTrialOrganizationLocationGraphTask)


    def run_publication_mysql_updates(self) -> None:
        """Run publication MySQL enrichment tasks."""

        self.logger.info("\n\nStarting run_publication_mysql_updates().")

        from pipelines.pipeline_3_publication.task_publication_2 import PublicationEpiNhsClassificationTask
        from pipelines.pipeline_3_publication.task_publication_3 import GardOmimPublicationMappingTask
        from pipelines.pipeline_3_publication.task_publication_4 import PublicationOminDataRetrievalTask
        from pipelines.pipeline_3_publication.task_publication_5 import NewPublicationPubtatorRetrievalTask
        from pipelines.pipeline_3_publication.task_publication_6 import NewPublicationChemicalSubstanceTask
        from pipelines.pipeline_3_publication.task_publication_7 import PublicationFalsePositiveFilterTask
        from pipelines.pipeline_3_publication.task_publication_8 import NewOmimPublicationArticleImportTask


        self._run_pipeline_task(PublicationEpiNhsClassificationTask)
        self._run_pipeline_task(GardOmimPublicationMappingTask)
        self._run_pipeline_task(PublicationOminDataRetrievalTask)
        self._run_pipeline_task(NewPublicationPubtatorRetrievalTask)
        self._run_pipeline_task(NewPublicationChemicalSubstanceTask)
        self._run_pipeline_task(PublicationFalsePositiveFilterTask)
        self._run_pipeline_task(NewOmimPublicationArticleImportTask)

        self.logger.info("Completed run_publication_mysql_updates().")



    def run_publication_graph_updates(self) -> None:
        """Run publication Memgraph node and relationship update tasks."""

        from pipelines.pipeline_3_publication.task_publication_graph_1 import NewPublicationArticleGraphTask
        from pipelines.pipeline_3_publication.task_publication_graph_2 import NewPublicationArticleNodeAttrsUpdateTask
        from pipelines.pipeline_3_publication.task_publication_graph_3 import NewPublicationEpidemiologyGraphTask
        from pipelines.pipeline_3_publication.task_publication_graph_4 import NewPublicationKeywordGraphTask
        from pipelines.pipeline_3_publication.task_publication_graph_5 import NewPublicationJournalGraphTask
        from pipelines.pipeline_3_publication.task_publication_graph_6 import NewPublicationMeshTermGraphTask
        from pipelines.pipeline_3_publication.task_publication_graph_7 import GardPublicationEpiNhsCountUpdateTask
        from pipelines.pipeline_3_publication.task_publication_graph_8 import NewPublicationGardArticleRelationshipTask
        from pipelines.pipeline_3_publication.task_publication_graph_9 import NewPublicationPubtatorGraphTask
        from pipelines.pipeline_3_publication.task_publication_graph_10 import NewPublicationSubstanceGraphTask
        from pipelines.pipeline_3_publication.task_publication_graph_11 import NewPublicationOmimRefGraphTask

        self._run_pipeline_task(NewPublicationArticleGraphTask)
        self._run_pipeline_task(NewPublicationArticleNodeAttrsUpdateTask)
        self._run_pipeline_task(NewPublicationEpidemiologyGraphTask)
        self._run_pipeline_task(NewPublicationKeywordGraphTask)
        self._run_pipeline_task(NewPublicationJournalGraphTask)
        self._run_pipeline_task(NewPublicationMeshTermGraphTask)
        self._run_pipeline_task(GardPublicationEpiNhsCountUpdateTask)
        self._run_pipeline_task(NewPublicationGardArticleRelationshipTask)
        self._run_pipeline_task(NewPublicationPubtatorGraphTask)
        self._run_pipeline_task(NewPublicationSubstanceGraphTask)
        self._run_pipeline_task(NewPublicationOmimRefGraphTask)



    def run_pipeline_followup_updates(self) -> None:
        """Run final graph statistics updates after pipeline data loads finish."""

        from pipelines.pipeline_5_followup.task_pipeline_followup_update_1 import GardRelationshipCountRefreshTask
        from pipelines.pipeline_5_followup.task_pipeline_followup_update_2 import ArticleGeneReviewFlagUpdateTask
        from pipelines.pipeline_5_followup.task_pipeline_followup_update_3 import OrganizationLocationRorLookupTask
        from pipelines.pipeline_5_followup.task_pipeline_followup_update_4 import OrganizationLocationGraphSyncTask
        from pipelines.pipeline_5_followup.task_pipeline_followup_update_5 import DiseaseCountsRefreshTask

        self._run_pipeline_task(GardRelationshipCountRefreshTask)
        self._run_pipeline_task(ArticleGeneReviewFlagUpdateTask)

        #self._run_pipeline_task(OrganizationLocationRorLookupTask)
        #self._run_pipeline_task(OrganizationLocationGraphSyncTask)
        self._run_pipeline_task(DiseaseCountsRefreshTask)
 

    def send_alert_emails(self, look_back_days: Optional[int] = None) -> None:
        """Send alert emails for the newly staged records."""

        alert_sender = None
        days = look_back_days if look_back_days is not None else self.look_back_days

        try:
            from alert_sender import AlertSender

            alert_sender = AlertSender(days)
            alert_sender.find_new_and_send_alert()

        except Exception as e:
            self.logger.error(f"send_alert_emails() failed: {e}")
            raise

        finally:
            self._close_task_if_needed(alert_sender)



    def run_regroup_the_person(self) -> None:

        from pipelines.pipeline_6_person.task_person_1_publication import NewPublicationPersonTask
        from pipelines.pipeline_6_person.task_person_2_clinical_trial import NewClinicalTrialPersonTask
        from pipelines.pipeline_6_person.task_person_3_grant import NewGrantPersonTask
        from pipelines.pipeline_6_person.task_person_4_grouping import NewPersonGroupingTask

        from pipelines.pipeline_6_person.task_person_5_graph import NewPersonAgentGraphTask
       
        self._run_pipeline_task(NewPublicationPersonTask)
        self._run_pipeline_task(NewClinicalTrialPersonTask)
        self._run_pipeline_task(NewGrantPersonTask)
        self._run_pipeline_task(NewPersonGroupingTask)

        self._run_pipeline_task(NewPersonAgentGraphTask)


    def run_pipeline_wrapup(self) -> None:

        from pipelines.pipeline_2_clinical_trial.task_clinical_trial_pipeline_wrapup import ClinicalTrialPipelineWrapUpTask
        from pipelines.pipeline_3_publication.task_publication_pipeline_wrapup import PublicationPipelineWrapUpTask
        from pipelines.pipeline_6_person.task_person_pipeline_wrapup import PersonPipelineWrapUpTask

        self._run_pipeline_task(ClinicalTrialPipelineWrapUpTask)
        self._run_pipeline_task(PublicationPipelineWrapUpTask)
        self._run_pipeline_task(PersonPipelineWrapUpTask)



    def _run_pipeline_task(self, task_class: Type[Any], task_name: Optional[str] = None) -> None:
        """Run one pipeline task and log the duration."""

        name = task_name or task_class.__name__

        start_time = time.time()
        task = None

        self.logger.info(f"\n*** Starting task: {name} ***\n")

        try:
            task = task_class()
            task.process_new_data()

            elapsed = time.time() - start_time
            hours, minutes, seconds = _time_hms(elapsed)
            self.logger.info(f"\n*** Finished task: {name} in {hours} hours, {minutes} minutes, {seconds} seconds ***\n")

        except Exception as e:
            self.logger.error(f"Task failed: {name}. Error: {e}")
            raise
        
        finally:
            self._close_task_if_needed(task)

 

    def _close_task_if_needed(self, task) -> None:
        """Close a child task only if it still owns an open database handle."""

        if task is None:
            return

        if getattr(task, "mysql", None) is None and getattr(task, "memgraph", None) is None:
            return

        task.close()
 

    def _next_available_archive_path(self, path: Path) -> Path:
        """Return a non-existing archive path without overwriting older logs."""

        count = 1
        candidate = path

        while candidate.exists():
            candidate = path.with_name(f"{path.name}.{count}")
            count += 1

        return candidate
    

    def _archive_log_files_by_date(self) -> None:
        """Move top-level log files into a date-stamped archive folder."""

        log_dir = Path(self.log_dir)
        archive_dir = log_dir / date.today().strftime("%Y%m%d")
        archive_dir.mkdir(parents=True, exist_ok=True)

        log_files = [
            path
            for pattern in ("*.log", "*.log.*")
            for path in log_dir.glob(pattern)
            if path.is_file()
        ]

        for log_file in log_files:
            target = archive_dir / log_file.name
            if target.exists():
                target = self._next_available_archive_path(target)

            shutil.move(str(log_file), str(target))

 

if __name__ == "__main__":

    LOOK_BACK_DAYS = 7
    
    total_run_start_time = time.time()

    runner = AlertPipelineRunner(look_back_days = LOOK_BACK_DAYS)

    #local helper function
    def _run_step_with_timing(step_name, step_func) -> None:

        step_start_time = time.time()
        runner.logger.info(f"\n\n{'*' * 20} Starting {step_name} {'*' * 20}\n")

        try:
            step_func()

        finally:
            hours, minutes, seconds = _time_hms(time.time() - step_start_time)
            runner.logger.info(f"\n\n{'*' * 20} Finished {step_name} in {hours} hours, {minutes} minutes, {seconds} seconds {'*' * 20}\n\n")


    try:
        # Step 1
        _run_step_with_timing(
            "Step 1: run_find_new_clinical_trial_and_publication_updates()",
            runner.run_find_new_clinical_trial_and_publication_updates,
        )
        
        # Step 2
        _run_step_with_timing(
            "Step 2: run_clinical_trial_mysql_updates()",
            runner.run_clinical_trial_mysql_updates,
        )
       
        # Step 3
        _run_step_with_timing(
            "Step 3: run_publication_mysql_updates()",
            runner.run_publication_mysql_updates,
        )
        
        # Step 4
        _run_step_with_timing(
            "Step 4: MemgraphIndexInitializationTask().process_new_data()",
            # lambda: MemgraphIndexInitializationTask().process_new_data(),
            lambda: runner.logger.info(f'\n\n{"*" * 30} MemgraphIndexInitializationTask().process_new_data() is disabled {"*" * 30}\n\n')
        )
        
        # Step 5
        _run_step_with_timing(
            "Step 5: run_clinical_trial_graph_updates()",
            runner.run_clinical_trial_graph_updates,
        )
        
        # Step 6
        _run_step_with_timing(
            "Step 6: run_publication_graph_updates()",
            runner.run_publication_graph_updates,
        )
        
        # Step 7
        _run_step_with_timing(
            "Step 7: run_pipeline_followup_updates()",
            runner.run_pipeline_followup_updates,
        )
        
        # Step 8
        _run_step_with_timing(
            "Step 8: send_alert_emails()",
            runner.send_alert_emails,
        )
        
        # Step 9
        _run_step_with_timing(
            "Step 9: run_regroup_the_person()",
            runner.run_regroup_the_person,
        )
         
        # Step 10
        _run_step_with_timing(
            "Step 10: run_pipeline_wrapup()",
            runner.run_pipeline_wrapup,
        )
         
    finally:
        total_hours, total_minutes, total_seconds = _time_hms(time.time() - total_run_start_time)
        runner.logger.info(
            f"\n\n{'=' * 20} Total alert pipeline run time: "
            f"{total_hours} hours, {total_minutes} minutes, {total_seconds} seconds "
            f"{'=' * 20}\n\n"
        )

        """ Flush and close the runner logger. """
        if getattr(runner, "logger", None):
            logger = runner.logger

            for handler in list(logger.handlers):
                handler.flush()
                handler.close()
                logger.removeHandler(handler)

            runner.logger = None
            print("Logger closed")

        ''' Create a date-stamped archive directory under runner.log_dir, like 20260526, then move all *.log and *.log.* files into it. '''
        runner._archive_log_files_by_date()

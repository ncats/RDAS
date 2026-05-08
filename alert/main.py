import os
import sys
import time
from datetime import date, timedelta
from typing import Any, Optional, Type

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, ".")),
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from utils.applogger import AppLogger
from utils.tools import _is_english, _is_under_char_threshold, _time_hms

class AlertPipelineRunner:
    """
    This class owns the high-level order only. Each individual pipeline task
    still opens, uses, and closes its own database connections.
    """

    # Testing override from the original script. Set this to None for PRODUCTION.
    TEST_LAST_UPDATE_DATE = date(2025, 7, 1)


    def __init__(self, look_back_days: int = 7):

        self.look_back_days = look_back_days
        self.log_dir = "logs"
        os.makedirs(self.log_dir, exist_ok=True)

        class_name = type(self).__name__
        self.log_file = f"{self.log_dir}/alert-{class_name}.log"
        self.logger = AppLogger(class_name, self.log_file).get_logger()

        self.logger.info(f'\n\n{"*" * 20} The {class_name} is initialized. {"*" * 20}\n')
        self.logger.info(f"AlertPipelineRunner configured with look_back_days={self.look_back_days}.")



    def run_find_new_clinical_trial_and_publication_updates(self) -> None:
        """Search for new clinical trials and publications for updated GARD nodes."""

        self.logger.info("Starting run_find_new_clinical_trial_and_publication_updates().")

        start_time = time.time()
        total_gard_nodes = 0
        gard_task = None
        clinical_trial_task = None
        publication_task = None

        try:
            from pipelines.pipeline_1.task_gard_1 import GardNodeNamesTask
            from pipelines.pipeline_2.task_clinical_trial_1 import NewClinicalTrialDiscoveryTask
            from pipelines.pipeline_3.task_publication_1 import NewPublicationDiscoveryTask

            gard_task = GardNodeNamesTask()
            clinical_trial_task = NewClinicalTrialDiscoveryTask()
            publication_task = NewPublicationDiscoveryTask()

            for batch in gard_task.get_gard_nodes():
                self.logger.info(f"Processing GARD discovery batch with {len(batch)} nodes.")

                for gard_node in batch:
                    filtered_names = self._get_filtered_gard_names(gard_node)

                    last_update_date = gard_node.get("updated")
                    if last_update_date is None:
                        last_update_date = date.today() - timedelta(days=self.look_back_days)

                    '''
                    # TEST_LAST_UPDATE_DATE keeps the old testing behavior in one place;
                    set it to None to use the real last_update_date.
                    '''
                    gard_node["updated"] = self.TEST_LAST_UPDATE_DATE or last_update_date
                    gard_node["filtered_names"] = filtered_names

                    gard_id = gard_node.get("gardId") or gard_node.get("gard_id") or "UNKNOWN_GARD_ID"
                    gard_name = gard_node.get("gardName") or gard_node.get("gard_name") or ""

                    # 1
                    ct_start_time = time.time()
 
                    clinical_trial_task.find_new_data(gard_node)

                    ''' log the total run time '''
                    ct_hours, ct_minutes, ct_seconds = _time_hms(time.time() - ct_start_time )
                    self.logger.info(
                        f"Finished NewClinicalTrialDiscoveryTask.find_new_data() for {gard_id} {gard_name} "
                        f"in {ct_hours} hours, {ct_minutes} minutes, "
                        f"{ct_seconds} seconds."
                    )

                    # 2
                    pub_start_time = time.time()

                    publication_task.find_new_data(gard_node)

                    ''' log the total run time '''
                    pub_hours, pub_minutes, pub_seconds = _time_hms(time.time() - pub_start_time)
                    self.logger.info(
                        f"Finished NewPublicationDiscoveryTask.find_new_data() for {gard_id} {gard_name} "
                        f"in {pub_hours} hours, {pub_minutes} minutes, "
                        f"{pub_seconds} seconds."
                    )

                    total_gard_nodes += 1

            self.logger.info(
                "Completed run_find_new_clinical_trial_and_publication_updates(); "
                f"processed {total_gard_nodes} GARD nodes."
            )

        except Exception as e:
            self.logger.error(f"run_find_new_clinical_trial_and_publication_updates() failed: {e}") 

        finally:
            for task in (clinical_trial_task, publication_task, gard_task):
                self._close_task_if_needed(task)

            ''' log the total run time '''
            elapsed = time.time() - start_time
            hours, minutes, seconds = _time_hms(elapsed)
            self.logger.info(
                "Finished run_find_new_clinical_trial_and_publication_updates() "
                f"in {hours} hours, {minutes} minutes, {seconds} seconds. "
                f"Processed {total_gard_nodes} GARD nodes."
            )


    def run_mysql_database_updates(self) -> None:
        """Run all MySQL update stages."""

        self.logger.info("Starting run_mysql_database_updates().")

        self.run_clinical_trial_mysql_updates()
        self.run_publication_mysql_updates()

        self.logger.info("Completed run_mysql_database_updates().")



    def run_memgraph_database_updates(self) -> None:
        """Run all Memgraph update stages."""

        self.logger.info("Starting run_memgraph_database_updates().")

        self.run_clinical_trial_graph_updates()

        self.run_publication_graph_updates()

        self.logger.info("Completed run_memgraph_database_updates().")



    def run_clinical_trial_mysql_updates(self) -> None:
        """Run clinical-trial MySQL staging and enrichment tasks."""

        self.logger.info("Starting run_clinical_trial_mysql_updates().")

        # Import here because ClinicalTrialDrugInterventionMappingTask loads the spaCy model.
        from pipelines.pipeline_2.task_clinical_trial_2 import NewClinicalTrialImportTask
        from pipelines.pipeline_2.task_clinical_trial_3 import ClinicalTrialDrugInterventionMappingTask
        from pipelines.pipeline_2.task_clinical_trial_4 import ClinicalTrialPublicationMappingTask
        from pipelines.pipeline_2.task_clinical_trial_5 import ClinicalTrialPmidArticleImportTask
        from pipelines.pipeline_2.task_clinical_trial_6 import NewClinicalTrialAnnotationTask

        self._run_pipeline_task(NewClinicalTrialImportTask)
        self._run_pipeline_task(ClinicalTrialDrugInterventionMappingTask)
        self._run_pipeline_task(ClinicalTrialPublicationMappingTask)
        self._run_pipeline_task(ClinicalTrialPmidArticleImportTask)
        self._run_pipeline_task(NewClinicalTrialAnnotationTask)

        self.logger.info("Completed run_clinical_trial_mysql_updates().")



    def run_clinical_trial_graph_updates(self) -> None:
        """Run clinical-trial Memgraph node and relationship update tasks."""

        self.logger.info("Starting run_clinical_trial_graph_updates().")

        from pipelines.pipeline_2.task_clinical_trial_graph_1 import NewClinicalTrialGraphTask
        from pipelines.pipeline_2.task_clinical_trial_graph_2 import NewClinicalTrialGardRelationshipTask
        from pipelines.pipeline_2.task_clinical_trial_graph_3 import NewClinicalTrialConditionGraphTask
        from pipelines.pipeline_2.task_clinical_trial_graph_4 import NewClinicalTrialInterventionGraphTask
        from pipelines.pipeline_2.task_clinical_trial_graph_5 import NewClinicalTrialDrugGraphTask
        from pipelines.pipeline_2.task_clinical_trial_graph_6 import NewClinicalTrialParticipantGraphTask
        from pipelines.pipeline_2.task_clinical_trial_graph_7 import NewClinicalTrialPrimaryOutcomeGraphTask
        from pipelines.pipeline_2.task_clinical_trial_graph_8 import NewClinicalTrialStudyDesignGraphTask
        from pipelines.pipeline_2.task_clinical_trial_graph_9 import NewClinicalTrialIndividualPatientDataGraphTask 
        from pipelines.pipeline_2.task_clinical_trial_graph_10 import NewClinicalTrialAnnotationGraphTask

    
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


        self.logger.info("Completed run_clinical_trial_graph_updates().")



    def run_publication_mysql_updates(self) -> None:
        """Run publication MySQL enrichment tasks."""

        self.logger.info("Starting run_publication_mysql_updates().")

        from pipelines.pipeline_3.task_publication_2 import PublicationEpiNhsClassificationTask
        from pipelines.pipeline_3.task_publication_3 import GardOmimPublicationMappingTask
        from pipelines.pipeline_3.task_publication_4 import PublicationOminDataRetrievalTask
        from pipelines.pipeline_3.task_publication_5 import NewPublicationPubtatorRetrievalTask
        from pipelines.pipeline_3.task_publication_6 import NewPublicationChemicalSubstanceTask
        from pipelines.pipeline_3.task_publication_7 import PublicationFalsePositiveFilterTask
        from pipelines.pipeline_3.task_publication_8 import NewPublicationArticleImportTask
        from pipelines.pipeline_3.task_publication_9 import NewOmimPublicationArticleImportTask


        self._run_pipeline_task(PublicationEpiNhsClassificationTask)
        self._run_pipeline_task(GardOmimPublicationMappingTask)
        self._run_pipeline_task(PublicationOminDataRetrievalTask)
        self._run_pipeline_task(NewPublicationPubtatorRetrievalTask)
        self._run_pipeline_task(NewPublicationChemicalSubstanceTask)
        self._run_pipeline_task(PublicationFalsePositiveFilterTask)
        self._run_pipeline_task(NewPublicationArticleImportTask)
        self._run_pipeline_task(NewOmimPublicationArticleImportTask)


        self.logger.info("Completed run_publication_mysql_updates().")


    def run_publication_graph_updates(self) -> None:
        """Run publication Memgraph node and relationship update tasks."""

        self.logger.info("Starting run_publication_graph_updates().")

        from pipelines.pipeline_3.task_publication_graph_1 import NewPublicationArticleGraphTask
        from pipelines.pipeline_3.task_publication_graph_2 import NewPublicationArticleNodeAttrsUpdateTask
        from pipelines.pipeline_3.task_publication_graph_3 import NewPublicationEpidemiologyGraphTask
        from pipelines.pipeline_3.task_publication_graph_4 import NewPublicationKeywordGraphTask
        from pipelines.pipeline_3.task_publication_graph_5 import NewPublicationJournalGraphTask
        from pipelines.pipeline_3.task_publication_graph_6 import NewPublicationMeshTermGraphTask
        from pipelines.pipeline_3.task_publication_graph_7 import GardPublicationEpiNhsCountUpdateTask
        from pipelines.pipeline_3.task_publication_graph_8 import NewPublicationGardArticleRelationshipTask
        from pipelines.pipeline_3.task_publication_graph_9 import NewPublicationPubtatorGraphTask
        from pipelines.pipeline_3.task_publication_graph_10 import NewPublicationSubstanceGraphTask
        from pipelines.pipeline_3.task_publication_graph_11 import NewPublicationOmimRefGraphTask


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


        self.logger.info("Completed run_publication_graph_updates().")



    def run_pipeline_followup_update(self) -> None:
        """Run final graph statistics updates after pipeline data loads finish."""

        self.logger.info("Starting run_pipeline_followup_update().")

        from pipelines.pipeline_5.task_pipeline_completion_update_1 import GardRelationshipCountRefreshTask
        from pipelines.pipeline_5.task_pipeline_completion_update_2 import ArticleGeneReviewFlagUpdateTask

        self._run_pipeline_task(GardRelationshipCountRefreshTask)
        self._run_pipeline_task(ArticleGeneReviewFlagUpdateTask)

        self.logger.info("Completed run_pipeline_followup_update().")



    def send_alert(self, look_back_days: Optional[int] = None) -> None:
        """Send alert emails for the newly staged records."""

        self.logger.info("Starting send_alert().")

        alert_sender = None
        days = look_back_days if look_back_days is not None else self.look_back_days

        try:
            from alert_sender import AlertSender

            alert_sender = AlertSender(days)
            alert_sender.find_new_and_send_alert()

            self.logger.info("Completed send_alert().")

        except Exception as e:
            self.logger.error(f"send_alert() failed: {e}")
            raise

        finally:
            self._close_task_if_needed(alert_sender)


    def run_pipeline_wrapup(self) -> None:
 
        from pipelines.pipeline_2.task_clinical_trial_pipeline_wrapup import ClinicalTrialPipelineWrapUpTask
        from pipelines.pipeline_3.task_publication_pipeline_wrapup import PublicationPipelineWrapUpTask

        self._run_pipeline_task(ClinicalTrialPipelineWrapUpTask)
        self._run_pipeline_task(PublicationPipelineWrapUpTask)




    def _run_pipeline_task(self, task_class: Type[Any], task_name: Optional[str] = None) -> None:
        """Run one pipeline task and log the duration."""

        name = task_name or task_class.__name__

        start_time = time.time()
        task = None

        self.logger.info(f"Starting task: {name}")

        try:
            task = task_class()
            task.process_new_data()

            elapsed = time.time() - start_time
            hours, minutes, seconds = _time_hms(elapsed)
            self.logger.info(
                f"Finished task: {name} in {hours} hours, {minutes} minutes, {seconds} seconds."
            )

        except Exception as e:
            self.logger.error(f"Task failed: {name}. Error: {e}")
            raise

        finally:
            self._close_task_if_needed(task)



    def _get_filtered_gard_names(self, gard_node) -> list:
        """Build the disease search names used by the first trial/publication tasks."""

        name = gard_node["gardName"]
        synonyms = gard_node["synonyms"]

        english_synonyms = [syn for syn in synonyms if _is_english(syn)]
        short_synonyms = [syn for syn in synonyms if _is_under_char_threshold(syn)]

        filtered_synonyms = [syn for syn in synonyms if syn in english_synonyms]
        filtered_synonyms = [syn for syn in filtered_synonyms if syn not in short_synonyms]

        return [name] + filtered_synonyms



    def _close_task_if_needed(self, task) -> None:
        """Close a child task only if it still owns an open database handle."""

        if task is None:
            return

        if getattr(task, "mysql", None) is None and getattr(task, "memgraph", None) is None:
            return

        task.close()



    def close(self) -> None:
        """Flush and close the runner logger."""

        if not getattr(self, "logger", None):
            return

        for handler in list(self.logger.handlers):
            handler.flush()
            handler.close()
            self.logger.removeHandler(handler)

        self.logger = None
        print("Logger closed")



if __name__ == "__main__":

    runner = AlertPipelineRunner(look_back_days=7)

    try:
        # Step 1
        # runner.run_find_new_clinical_trial_and_publication_updates()

        # Step 2
        # runner.run_mysql_database_updates()

        # Step 3
        # runner.run_memgraph_database_updates()

        # Step 7
        # runner.run_pipeline_followup_update()

        # Step 9
        # runner.send_alert()

        # runner.run_pipeline_wrapup()

        pass

    finally:
        runner.close()

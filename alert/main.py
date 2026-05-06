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

from utils.tools import _is_english, _is_under_char_threshold
from utils.applogger import AppLogger


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

        gard_task = None
        clinical_trial_task = None
        publication_task = None

        try:
            from pipelines.pipeline_1.task_gard_1 import GARDTask_1
            from pipelines.pipeline_2.task_clinical_trial_1 import ClinicalTrialTask_1
            from pipelines.pipeline_3.task_publication_1 import PublicationTask_1

            gard_task = GARDTask_1()
            clinical_trial_task = ClinicalTrialTask_1()
            publication_task = PublicationTask_1()

            total_gard_nodes = 0

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

                    clinical_trial_task.find_new_data(gard_node)
                    publication_task.find_new_data(gard_node)

                    total_gard_nodes += 1

            self.logger.info(
                "Completed run_find_new_clinical_trial_and_publication_updates(); "
                f"processed {total_gard_nodes} GARD nodes."
            )

        except Exception as e:
            self.logger.error(f"run_find_new_clinical_trial_and_publication_updates() failed: {e}")
            raise

        finally:
            for task in (clinical_trial_task, publication_task, gard_task):
                self._close_task_if_needed(task)


    def run_mysql_database_updates(self) -> None:
        """Run all MySQL update stages."""

        self.logger.info("Starting run_mysql_database_updates().")

        self.run_clinical_trial_mysql_updates()
        self.run_publication_mysql_updates()

        self.logger.info("Completed run_mysql_database_updates().")


    def run_clinical_trial_mysql_updates(self) -> None:
        """Run clinical-trial MySQL staging and enrichment tasks."""

        self.logger.info("Starting run_clinical_trial_mysql_updates().")

        # Import here because ClinicalTrialTask_3 loads the spaCy model.
        from pipelines.pipeline_2.task_clinical_trial_2 import ClinicalTrialTask_2
        from pipelines.pipeline_2.task_clinical_trial_3 import ClinicalTrialTask_3
        from pipelines.pipeline_2.task_clinical_trial_4 import ClinicalTrialTask_4
        from pipelines.pipeline_2.task_clinical_trial_5 import ClinicalTrialTask_5
        from pipelines.pipeline_2.task_clinical_trial_6 import ClinicalTrialTask_6

        self._run_pipeline_task(ClinicalTrialTask_2)
        self._run_pipeline_task(ClinicalTrialTask_3)
        self._run_pipeline_task(ClinicalTrialTask_4)
        self._run_pipeline_task(ClinicalTrialTask_5)
        self._run_pipeline_task(ClinicalTrialTask_6)

        self.logger.info("Completed run_clinical_trial_mysql_updates().")


    def run_publication_mysql_updates(self) -> None:
        """Run publication MySQL enrichment tasks."""

        self.logger.info("Starting run_publication_mysql_updates().")

        from pipelines.pipeline_3.task_publication_2 import PublicationTask_2
        from pipelines.pipeline_3.task_publication_3 import PublicationTask_3
        from pipelines.pipeline_3.task_publication_4 import PublicationTask_4
        from pipelines.pipeline_3.task_publication_5 import PublicationTask_5
        from pipelines.pipeline_3.task_publication_6 import PublicationTask_6
        from pipelines.pipeline_3.task_publication_7 import PublicationTask_7

        self._run_pipeline_task(PublicationTask_2)
        self._run_pipeline_task(PublicationTask_3)
        self._run_pipeline_task(PublicationTask_4)
        self._run_pipeline_task(PublicationTask_5)
        self._run_pipeline_task(PublicationTask_6)
        self._run_pipeline_task(PublicationTask_7)

        self.logger.info("Completed run_publication_mysql_updates().")


    def run_memgraph_database_updates(self) -> None:
        """Run all Memgraph update stages."""

        self.logger.info("Starting run_memgraph_database_updates().")

        self.run_clinical_trial_graph_updates()
        self.run_publication_graph_updates()

        self.logger.info("Completed run_memgraph_database_updates().")


    def run_clinical_trial_graph_updates(self) -> None:
        """Run clinical-trial Memgraph node and relationship update tasks."""

        self.logger.info("Starting run_clinical_trial_graph_updates().")

        from pipelines.pipeline_2.task_clinical_trial_graph_1 import ClinicalTrialGraphTask_1
        from pipelines.pipeline_2.task_clinical_trial_graph_2 import ClinicalTrialGraphTask_2
        from pipelines.pipeline_2.task_clinical_trial_graph_3 import ClinicalTrialGraphTask_3
        from pipelines.pipeline_2.task_clinical_trial_graph_4 import ClinicalTrialGraphTask_4
        from pipelines.pipeline_2.task_clinical_trial_graph_5 import ClinicalTrialGraphTask_5
        from pipelines.pipeline_2.task_clinical_trial_graph_6 import ClinicalTrialGraphTask_6
        from pipelines.pipeline_2.task_clinical_trial_graph_7 import ClinicalTrialGraphTask_7
        from pipelines.pipeline_2.task_clinical_trial_graph_8 import ClinicalTrialGraphTask_8
        from pipelines.pipeline_2.task_clinical_trial_graph_9 import ClinicalTrialGraphTask_9
        from pipelines.pipeline_2.task_clinical_trial_graph_10 import ClinicalTrialGraphTask_10
        from pipelines.pipeline_2.task_clinical_trial_graph_11 import ClinicalTrialGraphTask_11

        self._run_pipeline_task(ClinicalTrialGraphTask_1)
        self._run_pipeline_task(ClinicalTrialGraphTask_2)
        self._run_pipeline_task(ClinicalTrialGraphTask_3)
        self._run_pipeline_task(ClinicalTrialGraphTask_4)
        self._run_pipeline_task(ClinicalTrialGraphTask_5)
        self._run_pipeline_task(ClinicalTrialGraphTask_6)
        self._run_pipeline_task(ClinicalTrialGraphTask_7)
        self._run_pipeline_task(ClinicalTrialGraphTask_8)
        self._run_pipeline_task(ClinicalTrialGraphTask_9)
        self._run_pipeline_task(ClinicalTrialGraphTask_10)
        self._run_pipeline_task(ClinicalTrialGraphTask_11)

        self.logger.info("Completed run_clinical_trial_graph_updates().")


    def run_publication_graph_updates(self) -> None:
        """Run publication Memgraph node and relationship update tasks."""

        self.logger.info("Starting run_publication_graph_updates().")

        from pipelines.pipeline_3.task_publication_graph_1 import PublicationGraphTask_1

        self._run_pipeline_task(PublicationGraphTask_1)

        self.logger.info("Completed run_publication_graph_updates().")



    def run_pipeline_completion_update(self) -> None:
        """Run final graph statistics updates after pipeline data loads finish."""

        self.logger.info("Starting run_pipeline_completion_update().")

        from pipelines.pipeline_5.task_pipeline_completion_update_1 import PipelineCompletionUpdateTask_1
        from pipelines.pipeline_5.task_pipeline_completion_update_2 import PipelineCompletionUpdateTask_2

        self._run_pipeline_task(PipelineCompletionUpdateTask_1)
        self._run_pipeline_task(PipelineCompletionUpdateTask_2)

        self.logger.info("Completed run_pipeline_completion_update().")



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
            self.logger.info(f"Finished task: {name} in {elapsed:.2f} seconds.")

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
        # runner.run_pipeline_completion_update()

        # Step 9
        # runner.send_alert()

        pass

    finally:
        runner.close()

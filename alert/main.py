import os
import sys
import time
from datetime import date, timedelta
from typing import Optional, Type

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, ".")),
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from utils.tools import _is_english, _is_under_char_threshold
from pipelines.pipeline_base import PipelineBase
from pipelines.pipeline_1.task_gard_1 import GARDTask_1
from pipelines.pipeline_2.task_clinical_trial_1 import ClinicalTrialTask_1
from pipelines.pipeline_3.task_publication_1 import PublicationTask_1
from alert_sender import AlertSender


class AlertPipelineRunner(PipelineBase):
    """
    This class owns the high-level order only. Each individual pipeline task
    still opens, uses, and closes its own database connections.
    """

    def __init__(self, look_back_days: int = 7):

        super().__init__(init_mysql=False, init_memgraph=False)

        self.look_back_days = look_back_days
        self.logger.info(f"AlertPipelineRunner configured with look_back_days={self.look_back_days}.")


    def _run_pipeline_task(self, task_class: Type[PipelineBase], task_name: Optional[str] = None) -> None:

        """Run one PipelineBase task and log the duration."""

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
            '''
            # Most tasks close themselves, but this prevents leaked connections if a task raises before reaching its own cleanup block.
            '''
            if task is not None and (
                getattr(task, "mysql", None) is not None
                or getattr(task, "memgraph", None) is not None
            ):
                task.close()



    def find_new_data(self, gard_node=None) -> None:
        """Run the GARD-driven discovery step for new trials and publications."""

        self.logger.info("Starting find_new_data().")

        self.run_find_new_clinical_trial_and_publication_updates()

        self.logger.info("Completed find_new_data().")


    def process_new_data(self) -> None:
        """Run the full data update workflow in order."""

        start_time = time.time()
        self.logger.info("Starting full data update pipeline workflow.")

        try:
            self.find_new_data()
            self.run_mysql_database_updates()
            self.run_memgraph_database_updates()
            self.run_pipeline_completion_update()

            elapsed = time.time() - start_time
            self.logger.info(f"Completed full data update pipeline workflow in {elapsed:.2f} seconds.")

        except Exception as e:
            self.logger.error(f"Full data update pipeline workflow failed: {e}")
            raise


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

        self._run_pipeline_task(PublicationTask_2)
        self._run_pipeline_task(PublicationTask_3)
        self._run_pipeline_task(PublicationTask_4)
        self._run_pipeline_task(PublicationTask_5)
        self._run_pipeline_task(PublicationTask_6)

        self.logger.info("Completed run_publication_mysql_updates().")


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


    def run_pipeline_completion_update(self) -> None:
        """Run final graph statistics updates after pipeline data loads finish."""

        self.logger.info("Starting run_pipeline_completion_update().")

        from pipelines.pipeline_5.task_pipeline_completion_update_1 import PipelineCompletionUpdateTask_1

        self._run_pipeline_task(PipelineCompletionUpdateTask_1)

        self.logger.info("Completed run_pipeline_completion_update().")


    def run_find_new_clinical_trial_and_publication_updates(self) -> None:
        """Search for new clinical trials and publications for updated GARD nodes."""

        self.logger.info("Starting run_find_new_clinical_trial_and_publication_updates().")

        gard_task = None
        clinical_trial_task = None
        publication_task = None

        try:
            gard_task = GARDTask_1()
            clinical_trial_task = ClinicalTrialTask_1()
            publication_task = PublicationTask_1()

            total_gard_nodes = 0

            for batch in gard_task.get_gard_nodes():
                self.logger.info(f"Processing GARD discovery batch with {len(batch)} nodes.")

                for gard_node in batch:
                    name = gard_node["gardName"]
                    synonyms = gard_node["synonyms"]

                    english_synonyms = [syn for syn in synonyms if _is_english(syn)]
                    short_synonyms = [syn for syn in synonyms if _is_under_char_threshold(syn)]

                    filtered_synonyms = [syn for syn in synonyms if syn in english_synonyms]
                    filtered_synonyms = [syn for syn in filtered_synonyms if syn not in short_synonyms]

                    last_update_date = gard_node.get("updated")
                    if last_update_date is None:
                        last_update_date = date.today() - timedelta(days=self.look_back_days)

                    # Testing override from the original script. Use last_update_date
                    # below for production.
                    gard_node["updated"] = date(2025, 7, 1)
                    # gard_node["updated"] = last_update_date

                    gard_node["filtered_names"] = [name] + filtered_synonyms

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
                if task is not None and (
                    getattr(task, "mysql", None) is not None
                    or getattr(task, "memgraph", None) is not None
                ):
                    task.close()


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

        self.logger.info("Completed run_memgraph_database_updates().")


    def send_alert(self, look_back_days: Optional[int] = None) -> None:
        """Send alert emails for the newly staged records."""

        self.logger.info("Starting send_alert().")

        alert_sender = None
        days = look_back_days if look_back_days is not None else self.look_back_days

        try:
            alert_sender = AlertSender(days)
            alert_sender.find_new_and_send_alert()

            self.logger.info("Completed send_alert().")

        except Exception as e:
            self.logger.error(f"send_alert() failed: {e}")
            raise

        finally:
            if alert_sender is not None and (
                getattr(alert_sender, "mysql", None) is not None
                or getattr(alert_sender, "memgraph", None) is not None
            ):
                alert_sender.close()




if __name__ == "__main__":

    runner = AlertPipelineRunner(look_back_days=7)

    try:
        # Step 1
        # runner.find_new_data()

        # Step 2
        # runner.run_mysql_database_updates()

        # Step 3
        # runner.run_memgraph_database_updates()

        # Step 7
        # runner.run_pipeline_completion_update()

        # Step 9
        # runner.send_alert()

        # Full workflow
        # runner.process_new_data()
        pass

    finally:
        runner.close()

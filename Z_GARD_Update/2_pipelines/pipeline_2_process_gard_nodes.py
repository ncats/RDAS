
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
Z_ALERT_ROOT = PROJECT_ROOT / "Z_Alert"
for import_path in (str(PROJECT_ROOT), str(Z_ALERT_ROOT)):
    if import_path not in sys.path:
        sys.path.insert(0, import_path)

from Z_Alert.main import AlertPipelineRunner

class GARDToMySQLAndMemgraphUpdateRunner(AlertPipelineRunner):

    def __init__(self):

        super().__init__()

        self.START_DATE = '1971-01-01'

        self.logger.info("GARDToMySQLAndMemgraphUpdateRunner initialized.")



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

                    total_gard_nodes += 1

                    self.logger.info(f"\n****** Processed: {gard_node['gardId']}: {gard_node['gardName']} ******\n")
                    self.logger.info(f"\n###### Total GARD nodes processed {total_gard_nodes} ######\n")

        finally:
            self.gard_discovery_completed = True
            self._close_task_if_needed(gard_task)
            self.logger.info(f"Processed {total_gard_nodes} GARD nodes.")

        # The update tasks read shared is_new rows across all GARD IDs. Run them
        # once after discovery, even when a retry has no eligible GARD nodes but
        # previously staged rows still need to be processed.
        self.run_post_discovery_steps()


    def run_post_discovery_steps(self) -> None:

        """Run the shared alert update steps after GARD discovery finishes."""

        steps = (
            ("Step 2: clinical-trial MySQL updates", self.run_clinical_trial_mysql_updates),
            ("Step 3: publication MySQL updates", self.run_publication_mysql_updates),
            ("Step 5: clinical-trial graph updates", self.run_clinical_trial_graph_updates),
            ("Step 6: publication graph updates", self.run_publication_graph_updates),
            ("Step 7: pipeline follow-up updates", self.run_pipeline_followup_updates),
            ("Step 8: send alert emails", self.send_alert_emails),
            ("Step 9: regroup people", self.run_regroup_the_person),
            ("Step 10: pipeline maintenance", self.run_pipeline_maintenance),
            ("Step 11: pipeline wrap-up", self.run_pipeline_wrapup),
        )

        # These database and API steps consume all newly discovered rows.
        # The index initialization step remains disabled, as in Z_Alert/main.py.
        for step_name, step_func in steps:
            self._run_step_with_timing(step_name, step_func)


if __name__ == "__main__":

    runner = GARDToMySQLAndMemgraphUpdateRunner()

    try:
        runner._run_step_with_timing(
            "GARD nodes: discovery and shared updates",
            runner.run_pipeline_for_each_gard_node,
        )

    finally:
        runner.close()
        runner._archive_log_files_by_date()

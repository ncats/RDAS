
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



    def run_find_new_clinical_trial_and_publication(self) -> None:

        from Z_Alert.pipelines.pipeline_1_gard.task_gard_1 import GardNodeNamesTask
        from Z_Alert.pipelines.pipeline_2_clinical_trial.task_clinical_trial_1 import NewClinicalTrialDiscoveryTask
        from Z_Alert.pipelines.pipeline_3_publication.task_publication_1 import NewPublicationDiscoveryTask

        gard_task = GardNodeNamesTask()
        clinical_trial_task = NewClinicalTrialDiscoveryTask()
        publication_task = NewPublicationDiscoveryTask()

        total_gard_nodes = 0

        '''
        Step 1:
        Read GARD nodes in batches instead of loading every GARD node into
        memory at once. Each GARD node contains the names/synonyms used by
        the downstream clinical trial and publication discovery tasks.
        '''
        try:
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
                        f"{gard_node['gardId']}: {gard_node['gardName']}:\n"
                        f"Synonyms      : {synonyms}\n"
                        f"Filtered names: {filtered_names}"
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
                    clinical_trial_task.find_new_data(target_gard_node)

                    '''
                    Step 5:
                    Search publication data for this GARD node using the same
                    normalized search window and filtered disease names. Newly
                    discovered PMIDs are staged for later publication updates.
                    '''
                    publication_task.find_new_data(target_gard_node)

                    total_gard_nodes += 1
                    self.logger.info(f"\n# Total GARD nodes processed {total_gard_nodes}\n")

        finally:
            gard_task.close()


if __name__ == "__main__":

    runner = GARDToMySQLAndMemgraphUpdateRunner()

    try:
        runner._run_step_with_timing(
            "GARD Step 1: discover new clinical trials and publications",
            runner.run_find_new_clinical_trial_and_publication,
        )
        runner._run_step_with_timing(
            "GARD Step 2: update clinical-trial MySQL data",
            runner.run_clinical_trial_mysql_updates,
        )
        runner._run_step_with_timing(
            "GARD Step 3: update publication MySQL data",
            runner.run_publication_mysql_updates,
        )
        runner._run_step_with_timing(
            "GARD Step 4: update clinical-trial graph data",
            runner.run_clinical_trial_graph_updates,
        )
        runner._run_step_with_timing(
            "GARD Step 5: update publication graph data",
            runner.run_publication_graph_updates,
        )

    finally:
        runner.close()

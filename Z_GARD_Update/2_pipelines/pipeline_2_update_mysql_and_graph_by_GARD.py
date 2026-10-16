
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
Z_ALERT_ROOT = PROJECT_ROOT / "Z_Alert"
for import_path in (str(PROJECT_ROOT), str(Z_ALERT_ROOT)):
    if import_path not in sys.path:
        sys.path.insert(0, import_path)

from pipeline_runner_base import PipelineRunnerBase

class GARDToMySQLAndMemgraphUpdateRunner(PipelineRunnerBase):

    def __init__(self):

        super().__init__()

        self.START_DATE = '1971-01-01'

        self.logger.info("GARDToMySQLAndMemgraphUpdateRunner initialized.")



    def iterate_new_gard_nodes(self) -> None:

        from Z_Alert.pipelines.pipeline_1_gard.task_gard_1 import GardNodeNamesTask
        from Z_Alert.pipelines.pipeline_2_clinical_trial.task_clinical_trial_1 import NewClinicalTrialDiscoveryTask

        gard_task = GardNodeNamesTask()
        clinical_trial_task = NewClinicalTrialDiscoveryTask()

        try:
            for batch in gard_task.get_gard_nodes():

                for gard_node in batch:
                    '''
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

                    target_gard_node={
                        "gardId": gard_node['gardId'],
                        "updated": self.START_DATE,
                        "filtered_names": filtered_names
                    }

                    clinical_trial_task.find_new_data(target_gard_node)

        finally:
            gard_task.close()


if __name__ == "__main__":

    runner = GARDToMySQLAndMemgraphUpdateRunner()

    try:
        runner.iterate_new_gard_nodes()

    finally:
        runner.close()

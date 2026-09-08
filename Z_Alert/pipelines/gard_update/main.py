"""
Run the GARD update initialization pipeline.

This entrypoint mirrors the structure of `Z_Alert/main.py`: the runner owns the
high-level step order, while each concrete task owns its own database work.
"""

import os
import sys
import time
from dotenv import load_dotenv

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, ".")),
    os.path.abspath(os.path.join(_dir, "../..")),
    os.path.abspath(os.path.join(_dir, "../../..")),
])
load_dotenv(os.path.abspath(os.path.join(_dir, "../../..", ".env")))

from pipeline_runner_base import PipelineRunnerBase
from utils.tools import _time_hms


class GardUpdatePipelineRunner(PipelineRunnerBase):

    """
    Own the GARD update step order.

    The MySQL step is intentionally separate from the Memgraph steps because a
    full GARD refresh has two phases:

        1. Load the refreshed nomenclature rows into MySQL table `gard`.
        2. Build/refresh GARD graph nodes, xrefs, and relationships in Memgraph.
    """

    def __init__(self, clear_existing_mysql_gard: bool = False,  allow_append_mysql_gard: bool = False):

        # Number of raw GARD nomenclature CSV rows inserted before each MySQL commit.
        self.mysql_batch_size = 500

        # Number of grouped MySQL `gard` rows sent to Memgraph per GARD/Disease node MERGE batch.
        self.graph_node_batch_size = 100

        # Number of xref CSV rows sent to Memgraph per GARD xref property update batch.
        self.graph_xref_batch_size = 100

        # Number of gene association CSV rows sent to Memgraph per GARD-to-Gene relationship batch.
        self.graph_gene_batch_size = 100

        # Number of phenotype association CSV rows sent to Memgraph per GARD-to-Phenotype relationship batch.
        self.graph_phenotype_batch_size = 200

        # Number of hierarchy CSV rows sent to Memgraph per GARD-to-GARD subclass_of relationship batch.
        self.graph_hierarchy_batch_size = 200

        # When True, Step 1 truncates MySQL table `gard` before loading the refreshed nomenclature CSV.
        self.clear_existing_mysql_gard = clear_existing_mysql_gard

        # When True, Step 1 appends to MySQL table `gard`; keep False unless duplicate raw rows are intentional.
        self.allow_append_mysql_gard = allow_append_mysql_gard

        super().__init__()



    def run_gard_mysql_initialization(self) -> None:

        """Run the MySQL-side GARD nomenclature load."""

        from pipelines.gard_update.task_gard_1 import GardMysqlNomenclatureLoadTask

        self._run_pipeline_task(
            GardMysqlNomenclatureLoadTask,
            batch_size=self.mysql_batch_size,
            clear_existing=self.clear_existing_mysql_gard,
            allow_append=self.allow_append_mysql_gard,
        )


    def run_gard_memgraph_initialization(self) -> None:

        """Run the Memgraph-side GARD node and relationship initialization tasks."""

        from pipelines.gard_update.task_gard_graph_1 import GardGraphNodeInitializationTask
        from pipelines.gard_update.task_gard_graph_2 import GardGraphXrefUpdateTask
        from pipelines.gard_update.task_gard_graph_3 import GardGraphGeneRelationshipInitializationTask
        from pipelines.gard_update.task_gard_graph_4 import GardGraphPhenotypeRelationshipInitializationTask
        from pipelines.gard_update.task_gard_graph_5 import GardGraphHierarchyRelationshipInitializationTask

        self._run_pipeline_task(GardGraphNodeInitializationTask, batch_size=self.graph_node_batch_size)
        self._run_pipeline_task(GardGraphXrefUpdateTask, batch_size=self.graph_xref_batch_size)
        self._run_pipeline_task(GardGraphGeneRelationshipInitializationTask, batch_size=self.graph_gene_batch_size)
        self._run_pipeline_task(GardGraphPhenotypeRelationshipInitializationTask, batch_size=self.graph_phenotype_batch_size)
        self._run_pipeline_task(GardGraphHierarchyRelationshipInitializationTask, batch_size=self.graph_hierarchy_batch_size)


    def run_gard_update_wrapup(self) -> None:

        """Run the final GARD update summary task."""
        from pipelines.gard_update.task_gard_pipeline_wrapup import GardUpdatePipelineWrapUpTask

        self._run_pipeline_task(GardUpdatePipelineWrapUpTask)



if __name__ == "__main__":

    """
    Default behavior:
    - Assumes `gard_update/data` already contains the refreshed raw CSV files.
    - Runs the MySQL task, but that task skips loading if table `gard` already
      has rows unless CLEAR_EXISTING_MYSQL_GARD or ALLOW_APPEND_MYSQL_GARD is
      set to True.
    - Runs all concrete Memgraph initialization tasks in dependency order.
    """

    RUN_GARD_MYSQL_INITIALIZATION = True
    RUN_GARD_MEMGRAPH_INITIALIZATION = True
    RUN_GARD_UPDATE_WRAPUP = True

    # When True, Step 1 truncates MySQL table `gard` before loading the refreshed nomenclature CSV.
    CLEAR_EXISTING_MYSQL_GARD = False

    # When True, Step 1 appends to MySQL table `gard`; keep False unless duplicate raw rows are intentional.
    ALLOW_APPEND_MYSQL_GARD = False

    total_run_start_time = time.time()

    runner = GardUpdatePipelineRunner(clear_existing_mysql_gard=CLEAR_EXISTING_MYSQL_GARD, allow_append_mysql_gard=ALLOW_APPEND_MYSQL_GARD, )

    try:
        runner._run_step_with_timing(
            "Step 1: run_gard_mysql_initialization()",
            runner.run_gard_mysql_initialization if RUN_GARD_MYSQL_INITIALIZATION else lambda: runner.logger.info("*** Skip Step 1 --- run_gard_mysql_initialization --- ***\n\n"),
        )

        runner._run_step_with_timing(
            "Step 2: run_gard_memgraph_initialization()",
            runner.run_gard_memgraph_initialization if RUN_GARD_MEMGRAPH_INITIALIZATION else lambda: runner.logger.info("*** Skip Step 2 --- run_gard_memgraph_initialization --- ***\n\n"),
        )

        runner._run_step_with_timing(
            "Step 3: run_gard_update_wrapup()",
            runner.run_gard_update_wrapup if RUN_GARD_UPDATE_WRAPUP else lambda: runner.logger.info("*** Skip Step 3 --- run_gard_update_wrapup --- ***\n\n"),
        )

    finally:
        total_hours, total_minutes, total_seconds = _time_hms(time.time() - total_run_start_time)
        runner.logger.info(
            f"\n\n{'=' * 20} Total GARD update pipeline run time: "
            f"{total_hours} hours, {total_minutes} minutes, {total_seconds} seconds "
            f"{'=' * 20}\n\n"
        )

        """ Flush and close the runner logger. """
        runner.close()

        """
        Create a date-stamped archive directory under runner.log_dir, then move
        all *.log and *.log.* files into it, matching the alert pipeline runner.
        """
        runner._archive_log_files_by_date()

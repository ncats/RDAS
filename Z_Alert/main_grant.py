import os
import sys
import time

from dotenv import load_dotenv

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, ".")),
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])
load_dotenv(os.path.abspath(os.path.join(_dir, "..", ".env")))

from pipeline_runner_base import PipelineRunnerBase
from pipelines.pipeline_4_grant.grant_base import GrantPipelineBase
from utils.tools import _time_hms



# This year's data may not be published yet.
LAST_YEAR = GrantPipelineBase.LATEST_COMPLETED_REPORTER_YEAR

class GrantPipelineRunner(PipelineRunnerBase):
    """Run grant-specific alert pipeline tasks without touching Z_Alert/main.py."""

    """ Step 1 """
    def download_reporters_data(self) -> None:
         
        """ Download projects, abstracts, publications, linktables files by year. """
        from pipelines.pipeline_4_grant.task_grant_1 import GrantExporterDownloadTask
        self._run_pipeline_task(GrantExporterDownloadTask, years=[LAST_YEAR], )
 
        """ Download NIH RePORTER patent and clinical-study files """
        from pipelines.pipeline_4_grant.task_grant_2 import GrantPatentClinicalStudyDownloadTask
        self._run_pipeline_task(GrantPatentClinicalStudyDownloadTask)


    """ Step 2 """
    def upload_reporters_data_to_mysql(self) -> None:

        """ Upload project files into MySQL. """
        from pipelines.pipeline_4_grant.task_grant_3 import GrantProjectUploadTask
        self._run_pipeline_task(GrantProjectUploadTask, years=[LAST_YEAR])

        from pipelines.pipeline_4_grant.task_grant_4 import GrantPublicationUploadTask
        self._run_pipeline_task(GrantPublicationUploadTask, years=[LAST_YEAR])

        from pipelines.pipeline_4_grant.task_grant_5 import GrantAbstractUploadTask       
        self._run_pipeline_task(GrantAbstractUploadTask, years=[LAST_YEAR])

        from pipelines.pipeline_4_grant.task_grant_6 import GrantLinktableUploadTask
        self._run_pipeline_task(GrantLinktableUploadTask, years=[LAST_YEAR])

        from pipelines.pipeline_4_grant.task_grant_7 import GrantClinicalStudyUploadTask
        self._run_pipeline_task(GrantClinicalStudyUploadTask, years=[LAST_YEAR])

        from pipelines.pipeline_4_grant.task_grant_8 import GrantPatentUploadTask
        self._run_pipeline_task(GrantPatentUploadTask)


    """ Step 3 """
    def build_relationship(self) -> None:

        # Only if `rdas_db.gard` table changes, REBUILD or REFRESH `grant_gard_processed_names`
        # so the processed search terms stay aligned with the source GARD labels.
        from pipelines.pipeline_4_grant.task_grant_9 import GrantGardNameProcessingTask
        #self._run_pipeline_task(GrantGardNameProcessingTask)

        from pipelines.pipeline_4_grant.task_grant_10 import GrantGardProjectRelationshipTask
        self._run_pipeline_task(GrantGardProjectRelationshipTask)

        from pipelines.pipeline_4_grant.task_grant_11 import GrantProjectAnnotationTask
        self._run_pipeline_task(GrantProjectAnnotationTask)

        
    """ Step 4 """
    def graph_updates(self) -> None:

        from pipelines.pipeline_4_grant.task_grant_graph_1 import NewProjectGraphTask
        self._run_pipeline_task(NewProjectGraphTask)

        from pipelines.pipeline_4_grant.task_grant_graph_2 import NewGardProjectRelationshipGraphTask
        self._run_pipeline_task(NewGardProjectRelationshipGraphTask)

        from pipelines.pipeline_4_grant.task_grant_graph_3 import NewCoreProjectGraphTask
        self._run_pipeline_task(NewCoreProjectGraphTask)

        from pipelines.pipeline_4_grant.task_grant_graph_4 import NewPatentGraphTask
        self._run_pipeline_task(NewPatentGraphTask)

        

    """ Step 5 """
    def followup_updates(self) -> None:

        from pipelines.pipeline_4_grant.task_grant_12 import GrantPublicationArticleImportTask
        #self._run_pipeline_task(GrantPublicationArticleImportTask)

        ''' Re-use '''
        from pipelines.pipeline_3_publication.task_publication_2 import PublicationEpiNhsClassificationTask
        self._run_pipeline_task(PublicationEpiNhsClassificationTask)



    """ Step 6 """
    def run_pipeline_wrapup(self) -> None:
        from pipelines.pipeline_4_grant.task_grant_pipeline_wrapup import GrantPipelineWrapUpTask 
        self._run_pipeline_task(GrantPipelineWrapUpTask)



if __name__ == "__main__":
    
    total_run_start_time = time.time()

    runner = GrantPipelineRunner()

    try:
        runner.logger.info(f"Grant pipeline generated latest completed year={LAST_YEAR}.")
 
        # Step 1
        runner._run_step_with_timing(
            "Step 1: download_reporters_data()",
            lambda: runner.logger.info("\n\n*** Skip Step 1 ***\n\n"),
            # lambda: runner.download_reporters_data(),
        )

        # Step 2
        runner._run_step_with_timing(
            "Step 2: upload_reporters_data_to_mysql()",
            lambda: runner.logger.info("\n\n*** Skip Step 2 ***\n\n"),
            #lambda: runner.upload_reporters_data_to_mysql(),
        )

        # Step 3
        runner._run_step_with_timing(
            "Step 3: build_relationship()",
            lambda: runner.logger.info("\n\n*** Skip Step 3 ***\n\n"),
            #lambda: runner.build_relationship(),
        )

        # Step 4
        runner._run_step_with_timing(
            "Step 4: graph_updates()",
            #lambda: runner.logger.info("\n\n*** Skip Step 4 ***\n\n"),
            lambda: runner.graph_updates(),
        )

        # Step 5
        runner._run_step_with_timing(
            "Step 5: followup_updates()",
            lambda: runner.logger.info("\n\n*** Skip Step 5 ***\n\n"),
            #lambda: runner.followup_updates(),
        )

        # Step 6
        runner._run_step_with_timing(
            "Step 6: run_pipeline_wrapup()",
            lambda: runner.logger.info("\n\n*** Skip Step 6 ***\n\n"),
            #runner.run_pipeline_wrapup,
        )


    finally:
        total_hours, total_minutes, total_seconds = _time_hms(time.time() - total_run_start_time)
        runner.logger.info(
            f"\n\n{'=' * 20} Total grant pipeline run time: "
            f"{total_hours} hours, {total_minutes} minutes, {total_seconds} seconds "
            f"{'=' * 20}\n\n"
        )

        """ Flush and close the runner logger. """
        runner.close()

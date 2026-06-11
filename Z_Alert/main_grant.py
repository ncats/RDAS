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
        #self._run_pipeline_task(GrantProjectUploadTask, years=[LAST_YEAR])

        from pipelines.pipeline_4_grant.task_grant_4 import GrantPublicationUploadTask
        #self._run_pipeline_task(GrantPublicationUploadTask, years=[LAST_YEAR])

        from pipelines.pipeline_4_grant.task_grant_5 import GrantAbstractUploadTask       
        self._run_pipeline_task(GrantAbstractUploadTask, years=[LAST_YEAR])



if __name__ == "__main__":
    
    total_run_start_time = time.time()

    runner = GrantPipelineRunner()

    try:
        runner.logger.info(f"Grant pipeline generated latest completed year={LAST_YEAR}.")
 
        # Step 1
        runner._run_step_with_timing(
            "Step 1: download_reporters_data()",
            # lambda: runner.download_reporters_data(),
            lambda: runner.logger.info("\n\n*** Skip Step 1 ***\n\n"),
        )

        # Step 2
        runner._run_step_with_timing(
            "Step 2: upload_reporters_data_to_mysql()",
            lambda: runner.upload_reporters_data_to_mysql(),
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

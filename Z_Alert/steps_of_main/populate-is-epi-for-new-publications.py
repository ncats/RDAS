#!/usr/bin/env python3
import os
import sys
import time

from dotenv import load_dotenv

_dir = os.path.dirname(__file__)
_alert_dir = os.path.abspath(os.path.join(_dir, ".."))
_repo_dir = os.path.abspath(os.path.join(_alert_dir, ".."))
sys.path.extend([
    _alert_dir,
    _repo_dir,
    os.path.abspath(os.path.join(_repo_dir, "..")),
])
load_dotenv(os.path.join(_repo_dir, ".env"))

from pipeline_runner_base import PipelineRunnerBase
from pipelines.pipeline_3_publication.task_publication_2 import PublicationEpiNhsClassificationTask
from utils.tools import _time_hms

TASK_NAME = "Populate is_EPI/is_NHS for new publication rows"


def main() -> None:

    '''
    Run only the publication EPI/NHS classification task.

    PublicationEpiNhsClassificationTask already limits its work to rows in
    publication_article where is_new = 1 and is_EPI IS NULL. The task also
    fills is_NHS, epi_probability, and epi_extract for the same rows.

    Example:
        source /Users/zhaot3/anaconda3/etc/profile.d/conda.sh
        conda activate rdas
        ./Z_Alert/steps_of_main/populate-is-epi-for-new-publications.py
    '''
    total_run_start_time = time.time()
    runner = PipelineRunnerBase()

    try:
        runner._run_pipeline_task(PublicationEpiNhsClassificationTask, task_name=TASK_NAME)

    finally:
        total_hours, total_minutes, total_seconds = _time_hms(time.time() - total_run_start_time)
        runner.logger.info(
            f"\n\n{'=' * 20} Total {TASK_NAME} run time: "
            f"{total_hours} hours, {total_minutes} minutes, {total_seconds} seconds "
            f"{'=' * 20}\n\n"
        )
        runner.close()


if __name__ == "__main__":
    main()

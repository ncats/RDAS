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
from pipelines.pipeline_3_publication.task_publication_5 import NewPublicationPubtatorRetrievalTask
from utils.tools import _time_hms

TASK_NAME = "Retrieve and parse PubTator for new publication rows"


def main() -> None:

    '''
    Run only the PubTator retrieval and parsing task for new publication rows.

    NewPublicationPubtatorRetrievalTask already limits its MySQL work to
    publication_article rows where is_new = 1:

    1. retrieve_pubtator() downloads raw PubTator JSON for PubMed IDs that are
       not already in publication_pubtator.
    2. parse_pubtator() reads publication_pubtator.source_json and inserts
       normalized annotation rows into publication_pubtator_parsed.

    This script does not create Memgraph PubtatorAnnotation nodes. The graph
    update is still handled later by NewPublicationPubtatorGraphTask.

    Example:
        source /Users/zhaot3/anaconda3/etc/profile.d/conda.sh
        conda activate rdas
        ./Z_Alert/steps_of_main/retrieve-pubtator-for-new-publications.py
    '''
    total_run_start_time = time.time()
    runner = PipelineRunnerBase()

    try:
        runner._run_pipeline_task(NewPublicationPubtatorRetrievalTask, task_name=TASK_NAME)

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

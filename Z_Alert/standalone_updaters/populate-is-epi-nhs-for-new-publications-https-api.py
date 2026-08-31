#!/usr/bin/env python3
import os
import sys
import time

import requests
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

EPI_CLASSIFY_API_URL = "https://rdas.ncats.nih.gov/api/epi/postEpiClassifyText/"
EPI_CLASSIFY_API_CHECK_INTERVAL_SECONDS = 5 * 60
EPI_CLASSIFY_API_CHECK_TIMEOUT_SECONDS = 30
EPI_CLASSIFY_API_CHECK_PAYLOAD = {"text": "EPI classifier health check."}

os.environ["EPI_CLASSIFY_API"] = EPI_CLASSIFY_API_URL

from pipeline_runner_base import PipelineRunnerBase
from pipelines.pipeline_3_publication.task_publication_2 import PublicationEpiNhsClassificationTask
from utils.tools import _time_hms

TASK_NAME = "Populate is_EPI/is_NHS for new publication rows"


def is_epi_classify_api_alive(runner: PipelineRunnerBase) -> bool:

    '''
    Check whether the EPI classifier endpoint is ready before running the
    publication classification task.

    The endpoint is a classifier POST route, so this method sends a tiny valid
    payload instead of using GET or HEAD. A successful HTTP status means the URL
    is alive enough for PublicationEpiNhsClassificationTask to start.
    '''
    try:
        response = requests.post(
            EPI_CLASSIFY_API_URL,
            json=EPI_CLASSIFY_API_CHECK_PAYLOAD,
            timeout=EPI_CLASSIFY_API_CHECK_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        runner.logger.info(f"EPI classifier API is alive: {EPI_CLASSIFY_API_URL}")
        return True

    except requests.RequestException as error:
        runner.logger.warning(f"EPI classifier API is not ready: {EPI_CLASSIFY_API_URL}. Error: {error}")
        return False


def wait_for_epi_classify_api(runner: PipelineRunnerBase) -> None:

    '''
    Wait until the EPI classifier endpoint is reachable.

    The script checks once immediately. If the endpoint is down, it sleeps for
    five minutes and checks again. It keeps repeating this loop until the API is
    alive, then main() starts _run_pipeline_task().
    '''
    while not is_epi_classify_api_alive(runner):
        runner.logger.info(
            f"Will check EPI classifier API again in "
            f"{EPI_CLASSIFY_API_CHECK_INTERVAL_SECONDS // 60} minutes."
        )
        time.sleep(EPI_CLASSIFY_API_CHECK_INTERVAL_SECONDS)


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

    Behavior:
        The script checks https://rdas.ncats.nih.gov/api/epi/postEpiClassifyText/
        before starting. If the URL is down, it waits five minutes and checks
        again until the URL is alive.
    '''
    total_run_start_time = time.time()
    runner = PipelineRunnerBase()

    try:
        wait_for_epi_classify_api(runner)
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

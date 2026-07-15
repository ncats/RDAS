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

from main import AlertPipelineRunner
from utils.tools import _time_hms

LOOK_BACK_DAYS = 7
STEP_NAME = "Step 11: run_pipeline_wrapup()"


def main() -> None:

    total_run_start_time = time.time()
    runner = AlertPipelineRunner(look_back_days=LOOK_BACK_DAYS)

    try:
        runner._run_step_with_timing(STEP_NAME, runner.run_pipeline_wrapup)

    finally:
        total_hours, total_minutes, total_seconds = _time_hms(time.time() - total_run_start_time)
        runner.logger.info(
            f"\n\n{'=' * 20} Total alert pipeline step run time: "
            f"{total_hours} hours, {total_minutes} minutes, {total_seconds} seconds "
            f"{'=' * 20}\n\n"
        )
        runner.close()


if __name__ == "__main__":
    main()

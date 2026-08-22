"""
Re-run the grant relationship rebuild after MySQL/Memgraph cleanup.

Run from the repository root:

    conda run -n rdas python Z_Alert/pipelines/pipeline_4_grant/x_rerun_grant_relationship_rebuild.py

Or, if your shell already has the rdas conda environment activated:

    python Z_Alert/pipelines/pipeline_4_grant/x_rerun_grant_relationship_rebuild.py

This script intentionally runs the same Step 3 and Step 4 methods from
Z_Alert/main_grant.py so the re-run stays aligned with the real grant pipeline.
"""

import os
import sys
import time

from dotenv import load_dotenv


_dir = os.path.dirname(__file__)
_z_alert_dir = os.path.abspath(os.path.join(_dir, "..", ".."))
_repo_dir = os.path.abspath(os.path.join(_z_alert_dir, ".."))

sys.path.extend([
    _z_alert_dir,
    _repo_dir,
])

load_dotenv(os.path.join(_z_alert_dir, ".env"))

from main_grant import GrantPipelineRunner
from utils.tools import _time_hms


if __name__ == "__main__":

    total_run_start_time = time.time()
    runner = GrantPipelineRunner()

    try:
        runner._run_step_with_timing(
            "Step 3: build_relationship()",
            lambda: runner.build_relationship(),
        )

        runner._run_step_with_timing(
            "Step 4: graph_updates()",
            lambda: runner.graph_updates(),
        )

    finally:
        total_hours, total_minutes, total_seconds = _time_hms(time.time() - total_run_start_time)
        runner.logger.info(
            f"\n\n{'=' * 20} Grant relationship rebuild run time: "
            f"{total_hours} hours, {total_minutes} minutes, {total_seconds} seconds "
            f"{'=' * 20}\n\n"
        )
        runner.close()

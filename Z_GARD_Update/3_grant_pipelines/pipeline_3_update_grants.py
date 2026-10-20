"""Launch the shared grant pipeline against the updated GARD database.

Z_Alert.main_grant owns the grant stages and the GARD-specific name refresh.
The shell launcher selects the new database by copying .env.newdb to .env.
"""

import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
Z_ALERT_ROOT = PROJECT_ROOT / "Z_Alert"
for import_path in (str(PROJECT_ROOT), str(Z_ALERT_ROOT)):
    if import_path not in sys.path:
        sys.path.insert(0, import_path)

from Z_Alert.main_grant import GARDGrantUpdateRunner, LAST_YEAR, run_grant_pipeline
from utils.tools import _time_hms


if __name__ == "__main__":

    total_run_start_time = time.time()
    runner = GARDGrantUpdateRunner()

    try:
        runner.logger.info(f"Grant pipeline latest completed RePORTER year={LAST_YEAR}.")

        # The imported runner handles the GARD name refresh during Step 3.
        run_grant_pipeline(runner)

    finally:
        hours, minutes, seconds = _time_hms(time.time() - total_run_start_time)
        runner.logger.info(
            f"Grant update run time: {hours} hours, {minutes} minutes, {seconds} seconds."
        )
        runner.close()
        runner._archive_log_files_by_date()

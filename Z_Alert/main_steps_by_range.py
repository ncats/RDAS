import time

from main import AlertPipelineRunner
from utils.tools import _time_hms


if __name__ == "__main__":

    LOOK_BACK_DAYS = 7

    # STARTS_FROM_STEP is inclusive. END_AT_STEP is exclusive.
    # Example: STARTS_FROM_STEP = 4 and END_AT_STEP = 8 runs steps 4, 5, 6, and 7.
    STARTS_FROM_STEP = 1
    END_AT_STEP = 12

    if not isinstance(STARTS_FROM_STEP, int) or STARTS_FROM_STEP < 1 or STARTS_FROM_STEP > 11:
        raise ValueError("STARTS_FROM_STEP must be an integer from 1 to 11.")

    if not isinstance(END_AT_STEP, int) or END_AT_STEP < 2 or END_AT_STEP > 12:
        raise ValueError("END_AT_STEP must be an integer from 2 to 12.")

    if STARTS_FROM_STEP >= END_AT_STEP:
        raise ValueError("STARTS_FROM_STEP must be less than END_AT_STEP.")

    total_run_start_time = time.time()

    runner = AlertPipelineRunner(look_back_days=LOOK_BACK_DAYS)

    steps = [
        (
            1,
            "Step 1: run_find_new_clinical_trial_and_publication_updates()",
            runner.run_find_new_clinical_trial_and_publication_updates,
        ),
        (
            2,
            "Step 2: run_clinical_trial_mysql_updates()",
            runner.run_clinical_trial_mysql_updates,
        ),
        (
            3,
            "Step 3: run_publication_mysql_updates()",
            runner.run_publication_mysql_updates,
        ),
        (
            4,
            "Step 4: run_memgraph_index_initialization()",
            lambda: runner.logger.info(f'{"*" * 30} --- run_memgraph_index_initialization --- is disabled {"*" * 30}\n\n'),
        ),
        (
            5,
            "Step 5: run_clinical_trial_graph_updates()",
            runner.run_clinical_trial_graph_updates,
        ),
        (
            6,
            "Step 6: run_publication_graph_updates()",
            runner.run_publication_graph_updates,
        ),
        (
            7,
            "Step 7: run_pipeline_followup_updates()",
            runner.run_pipeline_followup_updates,
        ),
        (
            8,
            "Step 8: send_alert_emails()",
            runner.send_alert_emails,
        ),
        (
            9,
            "Step 9: run_regroup_the_person()",
            runner.run_regroup_the_person,
        ),
        (
            10,
            "Step 10: run_pipeline_maintenance()",
            runner.run_pipeline_maintenance,
        ),
        (
            11,
            "Step 11: run_pipeline_wrapup()",
            runner.run_pipeline_wrapup,
        ),
    ]

    try:
        runner.logger.info(f"Starting alert pipeline for step range [{STARTS_FROM_STEP}, {END_AT_STEP}).")

        for step_number, step_name, step_func in steps:
            if step_number < STARTS_FROM_STEP:
                runner.logger.info(f"Skipping {step_name} because STARTS_FROM_STEP={STARTS_FROM_STEP}.")
                continue

            if step_number >= END_AT_STEP:
                runner.logger.info(f"Stopping before {step_name} because END_AT_STEP={END_AT_STEP}.")
                break

            runner._run_step_with_timing(step_name, step_func)

    finally:
        total_hours, total_minutes, total_seconds = _time_hms(time.time() - total_run_start_time)
        runner.logger.info(
            f"\n\n{'=' * 20} Total alert pipeline run time: "
            f"{total_hours} hours, {total_minutes} minutes, {total_seconds} seconds "
            f"{'=' * 20}\n\n"
        )

        """ Flush and close the runner logger. """
        runner.close()

        '''
        Create a date-stamped archive directory under runner.log_dir,
        like 20260526, then move all *.log and *.log.* files into it.
        '''
        runner._archive_log_files_by_date()

import os
import shutil
import time
from datetime import date
from pathlib import Path
from typing import Any, Callable, Optional, Type

from pipelines.pipeline_error_logging import attach_pipeline_error_file_handler, remove_pipeline_error_file_handler
from utils.applogger import AppLogger
from utils.tools import _time_hms


class PipelineRunnerBase:
    """Shared runner plumbing for alert entrypoints."""

    def __init__(self):
        self.log_dir = os.path.expanduser(os.getenv("ALERT_LOG_DIR", "logs"))
        os.makedirs(self.log_dir, exist_ok=True)

        class_name = type(self).__name__
        self.log_file = f"{self.log_dir}/alert-{class_name}.log"
        self.logger = AppLogger(class_name, self.log_file).get_logger()
        self.logger.info(f'\n\n{"*" * 20} The {class_name} is initialized. {"*" * 20}\n')


    def _run_pipeline_task(self, task_class: Type[Any], task_name: Optional[str] = None, **kwargs) -> None:
        """Run one pipeline task and log the duration."""

        name = task_name or task_class.__name__
        start_time = time.time()
        task = None
        pipeline_error_handler = None
        pipeline_error_handler_added = False

        self.logger.info(f"\n*** Starting task: {name} ***\n")

        try:
            pipeline_error_handler, pipeline_error_handler_added = attach_pipeline_error_file_handler(
                self.logger,
                self.log_dir,
                module_name=task_class.__module__,
                task_class=task_class,
            )
            task = task_class(**kwargs)
            task.process_new_data()

            hours, minutes, seconds = _time_hms(time.time() - start_time)
            self.logger.info(f"\n*** Finished task: {name} in {hours} hours, {minutes} minutes, {seconds} seconds ***\n")

        except Exception:
            self.logger.exception(f"Task failed: {name}.")
            raise

        finally:
            self._close_task_if_needed(task)

            if pipeline_error_handler_added:
                remove_pipeline_error_file_handler(self.logger, pipeline_error_handler)


    def _run_step_with_timing(self, step_name: str, step_func: Callable[[], None]) -> None:
        """Run one high-level pipeline step and log the duration."""

        step_start_time = time.time()
        self.logger.info(f"\n\n{'*' * 20} Starting {step_name} {'*' * 20}\n")

        try:
            step_func()

        finally:
            hours, minutes, seconds = _time_hms(time.time() - step_start_time)
            self.logger.info(f"\n\n{'*' * 20} Finished {step_name} in {hours} hours, {minutes} minutes, {seconds} seconds {'*' * 20}\n\n")


    def _close_task_if_needed(self, task) -> None:
        """Close a child task only if it still owns an open database handle."""

        if task is None:
            return

        if getattr(task, "mysql", None) is None and getattr(task, "memgraph", None) is None:
            return

        task.close()


    def close(self) -> None:
        """Flush and close the runner logger."""

        if getattr(self, "logger", None):
            logger = self.logger

            for handler in list(logger.handlers):
                handler.flush()
                handler.close()
                logger.removeHandler(handler)

            self.logger = None
            print("Logger closed")


    def _next_available_archive_path(self, path: Path) -> Path:
        """Return a non-existing archive path without overwriting older logs."""

        count = 1
        candidate = path

        while candidate.exists():
            candidate = path.with_name(f"{path.name}.{count}")
            count += 1

        return candidate


    def _archive_log_files_by_date(self) -> None:
        """Move top-level log files into a date-stamped archive folder."""

        log_dir = Path(self.log_dir)
        archive_dir = log_dir / date.today().strftime("%Y%m%d")
        archive_dir.mkdir(parents=True, exist_ok=True)

        log_files = [
            path
            for pattern in ("*.log", "*.log.*")
            for path in log_dir.glob(pattern)
            if path.is_file()
        ]

        for log_file in log_files:
            target = archive_dir / log_file.name

            if target.exists():
                target = self._next_available_archive_path(target)

            shutil.move(str(log_file), str(target))

import inspect
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Iterable, List, Optional, Tuple, Type


PIPELINE_ERROR_LOG_FILES: Tuple[Tuple[str, str, str], ...] = (
    ("pipelines.pipeline_0_setup", "pipeline_0_setup", "error-alert-index.log"),
    ("pipelines.pipeline_1_gard", "pipeline_1_gard", "error-alert-pipeline-1-gard.log"),
    ("pipelines.pipeline_2_clinical_trial", "pipeline_2_clinical_trial", "error-alert-pipeline-2-clinical-trial.log"),
    ("pipelines.pipeline_3_publication", "pipeline_3_publication", "error-alert-pipeline-3-publication.log"),
    ("pipelines.pipeline_4_grant", "pipeline_4_grant", "error-alert-pipeline-4-grant.log"),
    ("pipelines.pipeline_5_followup", "pipeline_5_followup", "error-alert-pipeline-5-followup.log"),
    ("pipelines.pipeline_6_person", "pipeline_6_person", "error-alert-pipeline-6-person.log"),
    ("pipelines.pipeline_7_graph_maintenance", "pipeline_7_graph_maintenance", "error-alert-pipeline-7-graph-maintenance.log"),
)
Z_ALERT_ERROR_LOG_FILES: Tuple[Tuple[str, str], ...] = (
    ("alert_sender.py", "error-alert-email.log"),
)
PIPELINE_ERROR_LOG_MAX_BYTES = 1024 * 1024 * 10
PIPELINE_ERROR_LOG_BACKUP_COUNT = 10


def resolve_pipeline_error_log_file(module_name: Optional[str], task_class: Optional[Type] = None) -> Optional[str]:
    """Return the pipeline-specific error log filename for a task module."""

    normalized_module_name = module_name or ""
    normalized_module_tail = normalized_module_name.rsplit(".", 1)[-1]

    for module_prefix, _directory_name, log_file in PIPELINE_ERROR_LOG_FILES:
        if normalized_module_name.startswith(module_prefix) or f".{module_prefix}" in normalized_module_name:
            return log_file

    for file_name, log_file in Z_ALERT_ERROR_LOG_FILES:
        if normalized_module_tail == Path(file_name).stem:
            return log_file

    if task_class is None:
        return None

    try:
        task_file_path = Path(inspect.getfile(task_class)).resolve()

    except (OSError, TypeError):
        return None

    for file_name, log_file in Z_ALERT_ERROR_LOG_FILES:
        if task_file_path.name == file_name:
            return log_file

    path_parts = set(task_file_path.parts)

    for _module_prefix, directory_name, log_file in PIPELINE_ERROR_LOG_FILES:
        if directory_name in path_parts:
            return log_file

    return None


def attach_pipeline_error_file_handler(logger, log_dir, module_name: Optional[str] = None, task_class: Optional[Type] = None, error_log_file: Optional[str] = None):
    """Attach one ERROR-only rotating file handler for the task's pipeline."""

    if logger is None:
        return None, False

    error_log_file = error_log_file or resolve_pipeline_error_log_file(module_name, task_class)

    if not error_log_file:
        return None, False

    error_log_path = Path(log_dir) / error_log_file
    error_log_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_error_log_path = str(error_log_path.resolve())

    for handler in logger.handlers:
        if getattr(handler, "_pipeline_error_log_path", None) == resolved_error_log_path:
            return handler, False

    error_handler = RotatingFileHandler(
        resolved_error_log_path,
        maxBytes=PIPELINE_ERROR_LOG_MAX_BYTES,
        backupCount=PIPELINE_ERROR_LOG_BACKUP_COUNT,
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s:%(filename)s:%(lineno)d]: %(message)s"))
    error_handler._pipeline_error_log_path = resolved_error_log_path
    logger.addHandler(error_handler)

    return error_handler, True


def attach_pipeline_error_file_handlers(logger, log_dir, error_log_files: Iterable[str]) -> List[Tuple[logging.Handler, bool]]:
    """Attach several ERROR-only file handlers to one logger for a mixed pipeline step."""

    handler_records = []
    seen_log_files = set()

    for error_log_file in error_log_files:
        if not error_log_file or error_log_file in seen_log_files:
            continue

        handler, added = attach_pipeline_error_file_handler(
            logger,
            log_dir,
            error_log_file=error_log_file,
        )

        if handler is not None:
            handler_records.append((handler, added))
            seen_log_files.add(error_log_file)

    return handler_records


def remove_pipeline_error_file_handler(logger, handler) -> None:
    """Flush, close, and remove a temporary pipeline error handler."""

    if logger is None or handler is None:
        return

    handler.flush()
    handler.close()
    logger.removeHandler(handler)


def remove_pipeline_error_file_handlers(logger, handler_records: Iterable[Tuple[logging.Handler, bool]]) -> None:
    """Remove only the temporary error handlers that were added for one step."""

    for handler, added in handler_records:
        if added:
            remove_pipeline_error_file_handler(logger, handler)

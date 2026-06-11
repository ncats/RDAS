import csv
import logging
import os
import re
import shutil
import sys
import zipfile
from datetime import date
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

import requests
from requests import Session

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
    os.path.abspath(os.path.join(_dir, "../../..")),
])

from pipelines.pipeline_base import PipelineBase

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[2]
BASE_DIR = PROJECT_ROOT / "Z_Alert" / "pipelines" / "pipeline_4_grant" / "data"
DEFAULT_PROJECTS_DIR = BASE_DIR / "projects"
DEFAULT_PUBLICATIONS_DIR = BASE_DIR / "publications"
DEFAULT_BATCH_SIZE = 1000
MIN_REPORTER_PROJECT_YEAR = 2000
MAX_REPORTER_PROJECT_YEAR = date.today().year
LATEST_COMPLETED_REPORTER_YEAR = date.today().year - 1

BASE_URL = "https://reporter.nih.gov/exporter"
DEFAULT_EXPORTER_CATEGORIES = ("projects", "abstracts", "publications", "linktables")
DEFAULT_CHUNK_SIZE = 1024 * 1024
DEFAULT_REQUEST_TIMEOUT = (10, 120)
GRANT_ERROR_LOG_FILE = "alert-grant-errors.log"
GRANT_ERROR_LOG_MAX_BYTES = 1024 * 1024 * 10
GRANT_ERROR_LOG_BACKUP_COUNT = 10


def attach_grant_error_file_handler(logger, log_dir: os.PathLike) -> None:
    """Attach a shared grant ERROR-only rotating file handler to a logger."""

    if logger is None:
        return

    error_log_path = Path(log_dir) / GRANT_ERROR_LOG_FILE
    resolved_error_log_path = str(error_log_path.resolve())

    for handler in logger.handlers:
        if getattr(handler, "_grant_error_log_path", None) == resolved_error_log_path:
            return

    error_handler = RotatingFileHandler(
        resolved_error_log_path,
        maxBytes=GRANT_ERROR_LOG_MAX_BYTES,
        backupCount=GRANT_ERROR_LOG_BACKUP_COUNT,
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s:%(filename)s:%(lineno)d]: %(message)s"))
    error_handler._grant_error_log_path = resolved_error_log_path
    logger.addHandler(error_handler)


class GrantPipelineBase(PipelineBase):
    """Shared helpers for grant alert pipeline tasks."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._attach_grant_error_file_handler()


    def _attach_grant_error_file_handler(self) -> None:
        """Write every grant task ERROR log to a shared grant error log file."""

        attach_grant_error_file_handler(getattr(self, "logger", None), self.log_dir)


    def _stream_download(self, url: str, output_path: Path, session: Optional[Session] = None, timeout: Tuple[int, int] = DEFAULT_REQUEST_TIMEOUT) -> None:
        """Stream one URL to disk."""

        close_session = session is None
        http = session or requests.Session()

        try:
            with http.get(url, stream=True, timeout=timeout) as response:
                response.raise_for_status()

                with output_path.open("wb") as file_obj:
                    for chunk in response.iter_content(chunk_size=DEFAULT_CHUNK_SIZE):
                        if not chunk:
                            continue

                        file_obj.write(chunk)

        finally:
            if close_session:
                http.close()


    def _download_file(self, url: str, filename: os.PathLike, session: Optional[Session] = None, timeout: Tuple[int, int] = DEFAULT_REQUEST_TIMEOUT) -> bool:
        """Download one URL to disk using a temporary .part file."""

        output_path = Path(filename)

        if output_path.exists():
            self.logger.info(f"Skipping {output_path.name}; file already exists.")
            return True

        output_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = output_path.with_name(f"{output_path.name}.part")

        try:
            if temp_path.exists():
                temp_path.unlink()

            self._stream_download(url, temp_path, session=session, timeout=timeout)
            temp_path.replace(output_path)
            self.logger.info(f"Saved {output_path}")
            return True

        except (OSError, requests.RequestException) as e:
            self.logger.exception(f"Failed to download {url} to {output_path}")

            if temp_path.exists():
                temp_path.unlink()

            return False


    def _extract_zip_safely(self, zip_path: Path, output_dir: Path, overwrite: bool = False) -> List[Path]:
        """Extract one zip file after validating every member destination."""

        extracted_files = []
        output_root = output_dir.resolve()

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            for member in zip_ref.infolist():
                member_path = output_root / member.filename
                resolved_member_path = member_path.resolve()

                # Keep archive members inside the target directory to avoid
                # writing outside the grant alert data directory.
                if output_root != resolved_member_path and output_root not in resolved_member_path.parents:
                    raise ValueError(f"Unsafe zip member path: {member.filename}")

                if member.is_dir():
                    resolved_member_path.mkdir(parents=True, exist_ok=True)
                    continue

                if resolved_member_path.exists() and not overwrite:
                    continue

                resolved_member_path.parent.mkdir(parents=True, exist_ok=True)

                with zip_ref.open(member, "r") as source, resolved_member_path.open("wb") as target:
                    shutil.copyfileobj(source, target)

                extracted_files.append(resolved_member_path)

        return extracted_files


    def _convert_directories_to_utf8(self, directories: Sequence[Path]) -> int:
        """Convert selected directories to UTF-8 and remove converter backups."""

        from utils.tools import convert_csv_files_to_utf8

        converted_count = 0

        for files_dir in directories:
            if not files_dir.exists():
                self.logger.info(f"Skipping UTF-8 conversion; directory does not exist: {files_dir}")
                continue

            convert_csv_files_to_utf8(str(files_dir))
            self._remove_utf8_conversion_backups(files_dir)
            converted_count += 1

        return converted_count


    def _remove_utf8_conversion_backups(self, files_dir: Path) -> None:
        """Remove .bak files created by convert_csv_files_to_utf8 after conversion succeeds."""

        for backup_path in files_dir.glob("*.bak"):
            backup_path.unlink()
            self.logger.info(f"Removed UTF-8 conversion backup: {backup_path}")


    def _raise_csv_field_size_limit(self) -> None:
        """Raise csv field-size limit so large text columns do not fail parsing."""

        max_csv_field_size = sys.maxsize

        while True:
            try:
                csv.field_size_limit(max_csv_field_size)
                return

            except OverflowError:
                max_csv_field_size = int(max_csv_field_size / 10)


    def _resolve_years(self, years: Optional[Sequence[int]], required: bool = False, default_year: Optional[int] = None, min_year: int = MIN_REPORTER_PROJECT_YEAR, max_year: int = MAX_REPORTER_PROJECT_YEAR) -> List[int]:
        """Validate caller-provided fiscal years."""

        if years is None:
            if required:
                raise ValueError("years must be provided.")

            years = [default_year if default_year is not None else max_year]

        cleaned_years = sorted({int(year) for year in years})

        if not cleaned_years:
            raise ValueError("years must include at least one fiscal year.")

        for year in cleaned_years:
            if year < min_year or year > max_year:
                raise ValueError(f"Year {year} is outside the expected RePORTER export range {min_year}-{max_year}.")

        return cleaned_years


    def _get_reporter_project_year(self, filename: str) -> int:
        """Extract and validate the fiscal year from a RePORTER project filename."""

        return self._get_reporter_export_year(filename, "RePORTER_PRJ_C_FY", "RePORTER project")


    def _get_reporter_publication_year(self, filename: str) -> int:
        """Extract and validate the fiscal year from a RePORTER publication filename."""

        return self._get_reporter_export_year(filename, "RePORTER_PUB_C_FY", "RePORTER publication")


    def _get_reporter_export_year(self, filename: str, filename_prefix: str, export_label: str) -> int:
        """Extract and validate the fiscal year from a RePORTER export filename."""

        pattern = rf"{re.escape(filename_prefix)}(\d{{4}})\.[Cc][Ss][Vv]$"
        match = re.match(pattern, filename)

        if not match:
            raise ValueError(f"Filename '{filename}' does not match the expected pattern '{filename_prefix}<year>.CSV'")

        year = int(match.group(1))

        if year < MIN_REPORTER_PROJECT_YEAR or year > MAX_REPORTER_PROJECT_YEAR:
            raise ValueError(f"Year {year} is outside the expected {export_label} range {MIN_REPORTER_PROJECT_YEAR}-{MAX_REPORTER_PROJECT_YEAR}.")

        return year


    def _normalize_detected_encoding(self, detected_encoding: Optional[str]) -> str:
        """Return the encoding used to read a CSV file."""

        encoding = detected_encoding or "utf-8-sig"
        normalized_encoding = encoding.strip().lower().replace("_", "-")

        if normalized_encoding in {"utf-8", "utf8", "utf-8-sig"}:
            return "utf-8-sig"

        return encoding

import time
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import requests

from pipelines.pipeline_4_grant.grant_base import GrantPipelineBase
from utils.tools import _time_hms

"""
Download and unzip NIH RePORTER ExPORTER grant files for the alert pipeline.

This task uses the same download URL, file naming, and safe unzip helper as
the grant initializer, but it keeps the implementation local so the alert
pipeline does not import initializer scripts. It downloads by explicit year so
the alert pipeline does not need to sweep the full historical range.

The annual grant runner passes the target year directly. Downloaded zip files
are always unzipped, then the selected category folders are converted to UTF-8.
"""

# Reference: D_grant/init_1_download_and_unzip_grant_files.py


class GrantExporterDownloadTask(GrantPipelineBase):
    """Download, unzip, and UTF-8 normalize NIH RePORTER grant export files by year."""

    VALID_CATEGORIES = set(GrantPipelineBase.DEFAULT_EXPORTER_CATEGORIES)

    def __init__(self, years: Optional[Sequence[int]] = None, categories: Optional[Sequence[str]] = None):
        super().__init__(init_mysql=False, init_memgraph=False)

        self.years = self._resolve_years(years, default_year=self.LATEST_COMPLETED_REPORTER_YEAR, min_year=self.LATEST_COMPLETED_REPORTER_YEAR - 1, max_year=self.LATEST_COMPLETED_REPORTER_YEAR)
        self.categories = self._resolve_categories(categories)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("GrantExporterDownloadTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Download configured grant export years and unzip each downloaded zip."""

        start_time = time.time()
        summary = {
            "downloaded_or_existing": 0,
            "download_failed": 0,
            "zip_files_checked": 0,
            "extracted_files": 0,
            "unzip_failed": 0,
            "utf8_converted_categories": 0,
        }

        self.logger.info(f"Starting NIH RePORTER grant download task: categories={self.categories}, years={self.years}, unzip=True, convert_utf8=True, data_dir={self.BASE_DIR}")

        try:
            with requests.Session() as session:

                for category in self.categories:

                    category_dir = self.BASE_DIR / category
                    category_dir.mkdir(parents=True, exist_ok=True)

                    for year in self.years:
                        zip_path = category_dir / f"nih_{category}_{year}.zip"
                        url = f"{self.BASE_URL}/{category}/download/{year}"

                        if self._download_file(url, zip_path, session=session):
                            summary["downloaded_or_existing"] += 1
                            self._extract_downloaded_zip(zip_path, category_dir, summary)
                        else:
                            summary["download_failed"] += 1
                            self.logger.error(f"Failed to download NIH {category} file for year={year}: {url}")

            if summary["download_failed"] or summary["unzip_failed"]:
                self.logger.error(f"NIH RePORTER grant download incomplete. Summary={summary}")
                raise RuntimeError("NIH RePORTER grant download or unzip failed; skipping UTF-8 conversion.")

            summary["utf8_converted_categories"] = self._convert_directories_to_utf8([self.BASE_DIR / category for category in self.categories])

            self.logger.info(f"Completed NIH RePORTER grant download task. Summary={summary}")

        except Exception:
            self.logger.exception(f"GrantExporterDownloadTask failed. categories={self.categories}, years={self.years}, summary={summary}")
            raise

        finally:
            hours, minutes, seconds = _time_hms(time.time() - start_time)
            self.logger.info(f"Total time elapsed: {hours} hours, {minutes} minutes, {seconds} seconds")

            ''' Explicitly close all db connections. '''
            self.close()


    def _extract_downloaded_zip(self, zip_path: Path, output_dir: Path, summary: Dict[str, int]) -> None:
        """Extract one downloaded zip with the initializer's zip-slip protection."""

        summary["zip_files_checked"] += 1

        try:
            extracted_count = len(self._extract_zip_safely(zip_path, output_dir))
            summary["extracted_files"] += extracted_count
            self.logger.info(f"Extracted {zip_path.name}: {extracted_count} file(s)")

        except Exception:
            summary["unzip_failed"] += 1
            self.logger.exception(f"Failed to unzip {zip_path} into {output_dir}")


    def _resolve_categories(self, categories: Optional[Sequence[str]]) -> List[str]:
        """Resolve and validate grant exporter categories."""

        if categories is None:
            categories = self.DEFAULT_EXPORTER_CATEGORIES

        cleaned_categories = []

        for category in categories:
            normalized_category = str(category).strip().lower()

            if normalized_category not in self.VALID_CATEGORIES:
                valid_values = ", ".join(sorted(self.VALID_CATEGORIES))
                raise ValueError(f"Invalid grant exporter category: {category}. Expected one of: {valid_values}.")

            cleaned_categories.append(normalized_category)

        return list(dict.fromkeys(cleaned_categories))


if __name__ == "__main__":

    task = GrantExporterDownloadTask()
    task.process_new_data()

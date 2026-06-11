import time
import zipfile
from pathlib import Path
from typing import Dict, List, Tuple

import requests
from requests import Session

from pipelines.pipeline_4_grant.grant_base import GrantPipelineBase
from utils.tools import _time_hms

DOWNLOAD_SOURCES: Tuple[Dict[str, str], ...] = (
    {
        "name": "patents",
        "endpoint": "patents/download",
        "directory": "patents",
        "filename": "Patents.csv",
        "zip_filename": "nih_patents.zip",
    },
    {
        "name": "clinicalstudies",
        "endpoint": "clinicalstudies/download",
        "directory": "clinical_studies",
        "filename": "ClinicalStudies.csv",
        "zip_filename": "nih_clinicalstudies.zip",
    },
)

"""
Download NIH RePORTER ExPORTER patent and clinical-study files.

These files are not year-specific in the NIH ExPORTER UI. The annual grant
pipeline refreshes them directly, saves them under the names expected by the
grant loaders, and converts the target folders to UTF-8 after successful
download/extraction.
"""

# Reference: https://github.com/ncats/RDAS/blob/f7369b363ba2a4a1714b98e1303add9bc6732d91/D_grant/init_1_download_and_unzip_grant_files.py


class GrantPatentClinicalStudyDownloadTask(GrantPipelineBase):
    """Download, extract if needed, and UTF-8 normalize patent and clinical-study CSV files."""

    def __init__(self):
        super().__init__(init_mysql=False, init_memgraph=False)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("GrantPatentClinicalStudyDownloadTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Download non-year NIH grant exporter files."""

        start_time = time.time()
        summary = {
            "downloaded": 0,
            "download_failed": 0,
            "zip_files_checked": 0,
            "extracted_files": 0,
            "utf8_converted_directories": 0,
        }

        self.logger.info(f"Starting NIH RePORTER patent/clinical-study download task: data_dir={self.BASE_DIR}")

        try:
            with requests.Session() as session:
                for source in DOWNLOAD_SOURCES:
                    if self._download_source(source, session, summary):
                        summary["downloaded"] += 1
                    else:
                        summary["download_failed"] += 1

            if summary["download_failed"]:
                self.logger.error(f"NIH RePORTER patent/clinical-study download incomplete. Summary={summary}")
                raise RuntimeError("NIH RePORTER patent/clinical-study download failed; skipping UTF-8 conversion.")

            summary["utf8_converted_directories"] = self._convert_directories_to_utf8([self.BASE_DIR / source["directory"] for source in DOWNLOAD_SOURCES])

            self.logger.info(f"Completed NIH RePORTER patent/clinical-study download task. Summary={summary}")

        except Exception:
            self.logger.exception(f"GrantPatentClinicalStudyDownloadTask failed. summary={summary}")
            raise

        finally:
            hours, minutes, seconds = _time_hms(time.time() - start_time)
            self.logger.info(f"Total time elapsed: {hours} hours, {minutes} minutes, {seconds} seconds")

            ''' Explicitly close all db connections. '''
            self.close()


    def _download_source(self, source: Dict[str, str], session: Session, summary: Dict[str, int]) -> bool:
        """Download one NIH non-year exporter file."""

        output_dir = self.BASE_DIR / source["directory"]
        output_dir.mkdir(parents=True, exist_ok=True)

        download_url = f"{self.BASE_URL}/{source['endpoint']}"
        target_csv = output_dir / source["filename"]
        temp_path = output_dir / f"{source['filename']}.part"

        try:
            if temp_path.exists():
                temp_path.unlink()

            self._stream_download(download_url, temp_path, session)

            if zipfile.is_zipfile(temp_path):
                summary["zip_files_checked"] += 1
                zip_path = output_dir / source["zip_filename"]
                temp_path.replace(zip_path)

                if target_csv.exists():
                    target_csv.unlink()

                extracted_files = self._extract_zip_safely(zip_path, output_dir, overwrite=True)
                summary["extracted_files"] += len(extracted_files)
                self._ensure_expected_csv(target_csv, extracted_files)
                self.logger.info(f"Downloaded and extracted {source['name']} file to {target_csv}")
            else:
                temp_path.replace(target_csv)
                self.logger.info(f"Downloaded {source['name']} file to {target_csv}")

            return True

        except Exception:
            self.logger.exception(f"Failed to download NIH {source['name']} file from {download_url} to {output_dir}")

            if temp_path.exists():
                temp_path.unlink()

            return False


    def _ensure_expected_csv(self, target_csv: Path, extracted_files: List[Path]) -> None:
        """Rename the extracted CSV when NIH uses a different member name."""

        if target_csv.exists():
            return

        csv_files = [path for path in extracted_files if path.suffix.lower() == ".csv"]

        if len(csv_files) != 1:
            raise RuntimeError(f"Expected one extracted CSV for {target_csv.name}, found {len(csv_files)}.")

        csv_files[0].replace(target_csv)


if __name__ == "__main__":

    task = GrantPatentClinicalStudyDownloadTask()
    task.process_new_data()

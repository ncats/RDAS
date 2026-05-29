"""
Download, unzip, and normalize NIH RePORTER ExPORTER grant files.

Examples:
    python D_grant/init_1_download_and_unzip_grant_files.py --download
    python D_grant/init_1_download_and_unzip_grant_files.py --unzip
    python D_grant/init_1_download_and_unzip_grant_files.py --convert

Notes:
    - DEFAULT_END_YEAR is exclusive. 1985 to 2025 downloads 1985 through 2024.
    - BASE_DIR is D_grant/data, independent of the current working directory.
    - Patents and clinical studies are still manual downloads from the NIH ExPORTER website.
"""

import argparse
import os
import shutil
import sys
import zipfile
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from requests import Session
from tqdm import tqdm

# Add the project root to the Python path when this file is run directly.
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

BASE_URL = "https://reporter.nih.gov/exporter"
BASE_DIR = SCRIPT_DIR / "data"
DEFAULT_CATEGORIES = ("projects", "abstracts", "publications", "linktables")
DEFAULT_START_YEAR = 1985
DEFAULT_END_YEAR = 2025
DEFAULT_CHUNK_SIZE = 1024 * 1024
DEFAULT_REQUEST_TIMEOUT = (10, 120)


def download_file(url: str, filename: os.PathLike, session: Optional[Session] = None, timeout: Tuple[int, int] = DEFAULT_REQUEST_TIMEOUT) -> bool:
    """
    Download one URL to disk using streaming IO.

    The file is first written to a .part path and then atomically renamed to the
    final path. This prevents a failed download from leaving a corrupt .zip file
    that later steps might try to unzip.
    """

    output_path = Path(filename)

    if output_path.exists():
        print(f"Skipping {output_path.name}; file already exists.")
        return True

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = output_path.with_name(f"{output_path.name}.part")

    # Reuse a provided session when downloading many files. If this helper is
    # called by itself, create and close a temporary session inside this method.
    close_session = session is None
    http = session or requests.Session()

    try:
        if temp_path.exists():
            temp_path.unlink()

        with http.get(url, stream=True, timeout=timeout) as response:
            response.raise_for_status()
            total_size = int(response.headers.get("content-length") or 0)

            with temp_path.open("wb") as file_obj:
                progress = tqdm(
                    total=total_size or None,
                    unit="B",
                    unit_scale=True,
                    desc=output_path.name,
                    leave=False,
                )

                with progress:
                    for chunk in response.iter_content(chunk_size=DEFAULT_CHUNK_SIZE):
                        if not chunk:
                            continue

                        file_obj.write(chunk)
                        progress.update(len(chunk))

        temp_path.replace(output_path)
        print(f"Saved {output_path}")
        return True

    except (OSError, requests.RequestException) as exc:
        print(f"Failed to download {url}: {exc}")

        if temp_path.exists():
            temp_path.unlink()

        return False

    finally:
        if close_session:
            http.close()


def export_by_category(category: str, session: Optional[Session] = None) -> Dict[str, int]:
    """
    Download all NIH ExPORTER zip files for one category over a year range.

    Returns summary counts so callers can log or test the result without
    parsing printed output.
    """

    output_dir = BASE_DIR / category
    output_dir.mkdir(parents=True, exist_ok=True)

    years = range(DEFAULT_START_YEAR, DEFAULT_END_YEAR)
    summary = {"downloaded_or_existing": 0, "failed": 0}
    close_session = session is None
    http = session or requests.Session()

    try:
        for year in tqdm(years, desc=f"Downloading NIH {category} files"):
            file_url = f"{BASE_URL}/{category}/download/{year}"
            output_path = output_dir / f"nih_{category}_{year}.zip"

            if download_file(file_url, output_path, session=http):
                summary["downloaded_or_existing"] += 1
            else:
                summary["failed"] += 1

    finally:
        if close_session:
            http.close()

    return summary


def unzip_files_from_to(input_dir: os.PathLike, output_dir: os.PathLike) -> Dict[str, int]:
    """
    Extract all zip files from input_dir into output_dir.

    Each member path is checked before extraction to avoid zip-slip paths such
    as ../../somewhere_else/file.csv. That matters because these files are
    downloaded from outside the repository and should not be trusted blindly.
    """

    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        print(f"Zip directory does not exist: {input_path}")
        return {"zip_files": 0, "extracted_files": 0, "failed": 0}

    zip_files = sorted(input_path.glob("*.zip"))
    summary = {"zip_files": len(zip_files), "extracted_files": 0, "failed": 0}

    if not zip_files:
        print(f"No zip files found in {input_path}")
        return summary

    for zip_path in tqdm(zip_files, desc="Unzipping files"):
        try:
            extracted_count = extract_zip_safely(zip_path, output_path)
            summary["extracted_files"] += extracted_count
            print(f"Extracted {zip_path.name}: {extracted_count} file(s)")

        except (OSError, zipfile.BadZipFile, ValueError) as exc:
            summary["failed"] += 1
            print(f"Failed to unzip {zip_path}: {exc}")

    print(f"Batch unzip directory complete: {input_path}")
    return summary


def extract_zip_safely(zip_path: Path, output_dir: Path) -> int:
    """Extract one zip file after validating every member destination."""

    extracted_count = 0
    output_root = output_dir.resolve()

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        for member in zip_ref.infolist():
            member_path = output_root / member.filename
            resolved_member_path = member_path.resolve()

            # A valid zip member must stay inside output_root after resolving
            # relative path components. This prevents archives from writing
            # outside the intended data directory.
            if output_root != resolved_member_path and output_root not in resolved_member_path.parents:
                raise ValueError(f"Unsafe zip member path: {member.filename}")

            if member.is_dir():
                resolved_member_path.mkdir(parents=True, exist_ok=True)
                continue

            if resolved_member_path.exists():
                continue

            resolved_member_path.parent.mkdir(parents=True, exist_ok=True)

            with zip_ref.open(member, "r") as source, resolved_member_path.open("wb") as target:
                shutil.copyfileobj(source, target)

            extracted_count += 1

    return extracted_count


def convert_categories_to_utf8(data_dir: os.PathLike, categories: Iterable[str]) -> None:
    """Convert downloaded CSV files to UTF-8 for the selected categories."""

    # Lazy import keeps --help, --download, and --unzip fast. utils.tools imports
    # NLP/data libraries that are only needed for the CSV conversion action.
    from utils.tools import convert_csv_files_to_utf8

    for category in categories:
        files_dir = Path(data_dir) / category

        if not files_dir.exists():
            print(f"Skipping UTF-8 conversion; directory does not exist: {files_dir}")
            continue

        convert_csv_files_to_utf8(files_dir)


def main(argv: Optional[List[str]] = None) -> int:
    """Run the requested download/unzip/convert actions."""

    parser = argparse.ArgumentParser(
        description="Download, unzip, and UTF-8 normalize NIH RePORTER ExPORTER grant files.",
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help=f"Download {DEFAULT_START_YEAR}-{DEFAULT_END_YEAR - 1} zip files for all default categories.",
    )
    parser.add_argument(
        "--unzip",
        action="store_true",
        help="Unzip downloaded files for all default categories.",
    )
    parser.add_argument(
        "--convert",
        action="store_true",
        help="Convert CSV files in all default category directories to UTF-8.",
    )
    args = parser.parse_args(argv)

    if not any((args.download, args.unzip, args.convert)):
        print("No action selected. Use --download, --unzip, and/or --convert.\n")
        parser.print_help()
        return 0

    if args.download:
        with requests.Session() as session:
            for category in DEFAULT_CATEGORIES:
                summary = export_by_category(category=category, session=session)
                print(f"Download summary for {category}: {summary}")

    if args.unzip:
        for category in DEFAULT_CATEGORIES:
            category_dir = BASE_DIR / category
            summary = unzip_files_from_to(category_dir, category_dir)
            print(f"Unzip summary for {category}: {summary}")

    if args.convert:
        convert_categories_to_utf8(BASE_DIR, DEFAULT_CATEGORIES)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

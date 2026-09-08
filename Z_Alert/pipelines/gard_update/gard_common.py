"""
Shared helpers for the GARD update pipeline.

The GARD update tasks assume the refreshed raw data already exists under
`Z_Alert/pipelines/gard_update/data`. No copy step is included here because the
operator controls when new source files are placed in that folder.
"""

import csv
import sys
from ast import literal_eval
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Sequence, Tuple


SCRIPT_DIR = Path(__file__).resolve().parent
Z_ALERT_DIR = SCRIPT_DIR.parents[1]
PROJECT_ROOT = SCRIPT_DIR.parents[2]
DATA_DIR = SCRIPT_DIR / "data"
DATA_2025_DIR = DATA_DIR / "2025"

GARD_NOMENCLATURE_FILE = DATA_2025_DIR / "GARD_Nomenclature_eng_2_10_2025_WithOrphanetData.csv"
GARD_XREF_FILE = DATA_2025_DIR / "GARD_Disease_Xref_pivoted-20250214.csv"
GARD_GENE_FILE = DATA_2025_DIR / "GARD-Disease-to-Gene-Associations_reformatted_20250224.csv"
GARD_PHENOTYPE_FILE = DATA_2025_DIR / "GARD_Disease_To_Phenotype_reformated-20250225.csv"
GARD_HIERARCHY_FILE = DATA_DIR / "GARD_classification.csv"

REQUIRED_GARD_DATA_FILES: Tuple[Path, ...] = (
    GARD_NOMENCLATURE_FILE,
    GARD_XREF_FILE,
    GARD_GENE_FILE,
    GARD_PHENOTYPE_FILE,
    GARD_HIERARCHY_FILE,
)


def ensure_project_paths_on_path() -> None:

    """Add the project import roots used by `Z_Alert/main.py` style scripts."""

    for path in (Z_ALERT_DIR, PROJECT_ROOT):
        path_text = str(path)

        if path_text not in sys.path:
            sys.path.append(path_text)


def resolve_data_file(file_path: Any) -> Path:

    """Resolve and validate one GARD update data file."""

    path = Path(file_path).expanduser().resolve()

    if not path.exists():
        raise FileNotFoundError(f"GARD update data file does not exist: {path}")

    if not path.is_file():
        raise ValueError(f"GARD update data path is not a file: {path}")

    return path


def validate_required_data_files() -> List[Path]:

    """Return validated required data files, raising early when one is missing."""

    return [resolve_data_file(file_path) for file_path in REQUIRED_GARD_DATA_FILES]


def count_csv_rows(file_path: Path) -> int:

    """Count CSV data rows, excluding the header."""

    with file_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.reader(csv_file)
        next(reader, None)
        return sum(1 for _ in reader)


def iter_csv_dict_rows(file_path: Path) -> Iterator[Dict[str, str]]:

    """Yield CSV rows as dictionaries while handling UTF-8 BOM headers."""

    with file_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)

        for row in reader:
            yield row


def clean_optional_text(value: Any, default: str = "") -> str:

    """Return a clean string for nullable CSV/database values."""

    if value is None:
        return default

    text = str(value).strip()
    return text if text else default


def none_to_empty(row: Mapping[str, Any]) -> Dict[str, Any]:

    """Convert None values to empty strings before graph-property splitting."""

    return {
        key: "" if value is None else value
        for key, value in row.items()
    }


def split_delimited_values(value: Any, delimiter: str = ",") -> List[str]:

    """Split a delimited scalar into a clean list and drop empty values."""

    text = clean_optional_text(value)

    if not text:
        return []

    return [
        part.strip()
        for part in text.split(delimiter)
        if part and part.strip()
    ]


def split_bracketed_values(value: Any, delimiter: str = ",") -> List[str]:

    """Split GARD CSV fields stored like `[a, b, c]` into a clean list."""

    text = clean_optional_text(value).strip("[]")

    if not text:
        return []

    return [
        part.strip().strip("'\"")
        for part in text.split(delimiter)
        if part and part.strip().strip("'\"")
    ]


def validate_batch_size(batch_size: int) -> int:

    """Normalize a caller-provided batch size to a positive integer."""

    try:
        batch_size = int(batch_size)
    except (TypeError, ValueError):
        batch_size = 100

    return max(1, batch_size)


def _index_property_list(value: Any) -> List[str]:

    """Normalize SHOW INDEX INFO property values across Memgraph versions."""

    if value is None or value == "":
        return []

    if isinstance(value, list):
        return value

    if isinstance(value, tuple):
        return list(value)

    if isinstance(value, str):
        text = value.strip()

        if not text:
            return []

        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = literal_eval(text)
                return parsed if isinstance(parsed, list) else [text]
            except (SyntaxError, ValueError):
                return [text]

        return [text]

    return [str(value)]


def _index_type(row: Mapping[str, Any]) -> str:

    """Return the Memgraph index type column across driver/key variants."""

    return str(row.get("index type") or row.get("type") or row.get("index_type") or "")


def is_memgraph_index_field_exists(memgraph: Any, label_name: str, field: str) -> bool:

    """Return True when a label-property index already exists."""

    for row in memgraph.execute_and_fetch("SHOW INDEX INFO"):
        if row.get("label") != label_name:
            continue

        index_type = _index_type(row)
        properties = _index_property_list(row.get("property"))

        if index_type and index_type != "label+property":
            continue

        if properties == [field]:
            return True

    return False


def create_memgraph_indexes_if_missing(memgraph: Any, index_config: Mapping[str, Sequence[str]], logger: Any) -> Tuple[int, int, int]:

    """
    Create only the Memgraph indexes required by a GARD update task.

    These concrete GARD tasks can be run outside the normal alert pipeline, so
    each task checks its own lookup indexes before writing. That keeps the
    initializer portable while avoiding a dependency on the older `InitBase`
    initializer classes or on the full pipeline_0 setup task.
    """

    created = 0
    skipped = 0
    errors = 0

    for label, fields in index_config.items():
        for field in fields:
            if is_memgraph_index_field_exists(memgraph, label, field):
                skipped += 1
                logger.info(f"Index already exists: :{label}({field})")
                continue

            command = f"CREATE INDEX ON :{label}({field});"

            try:
                memgraph.execute(command)
                created += 1
                logger.info(f"Created Memgraph index: {command}")

            except Exception as e:
                errors += 1
                logger.error(f"Error creating Memgraph index {command}: {e}")

    return created, skipped, errors


def log_csv_file_summary(logger: Any, label: str, file_path: Path) -> int:

    """Log the resolved CSV path and row count for a GARD update input."""

    row_count = count_csv_rows(file_path)
    logger.info(f"{label} input file: {file_path}")
    logger.info(f"{label} CSV rows available={row_count}.")
    return row_count

"""
Shared Memgraph index helper functions.

These functions are the PipelineBase-friendly version of the older
`InitBase.create_indexes()` helpers: the caller passes the existing Memgraph
connection and logger, so no helper opens its own database connection or owns
task lifecycle cleanup.
"""

from ast import literal_eval
from typing import Any, List, Mapping, Sequence, Tuple


def _log_info(logger: Any, message: str) -> None:

    """Log an info message, falling back to stdout when no logger is supplied."""

    if logger:
        logger.info(message)
    else:
        print(message)


def _log_error(logger: Any, message: str) -> None:

    """Log an error message, falling back to stdout when no logger is supplied."""

    if logger:
        logger.error(message)
    else:
        print(message)


def get_index_info(memgraph: Any) -> List[Mapping[str, Any]]:

    """Return Memgraph SHOW INDEX INFO rows as a list."""

    return list(memgraph.execute_and_fetch("SHOW INDEX INFO"))


def property_list(value: Any) -> List[str]:

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


def index_type(row: Mapping[str, Any]) -> str:

    """Return the Memgraph index type column regardless of driver naming."""

    return str(row.get("index type") or row.get("type") or row.get("index_type") or "")


def is_label_index_exists(memgraph: Any, label_name: str) -> bool:

    """Return True when Memgraph already has a label-only index."""

    for row in get_index_info(memgraph):
        if row.get("label") != label_name:
            continue

        row_index_type = index_type(row)
        properties = property_list(row.get("property"))

        if row_index_type == "label" or not properties:
            return True

    return False


def is_index_field_exists(memgraph: Any, label_name: str, field: str) -> bool:

    """Return True when Memgraph already has a label-property index."""

    for row in get_index_info(memgraph):
        if row.get("label") != label_name:
            continue

        row_index_type = index_type(row)
        properties = property_list(row.get("property"))

        if row_index_type and row_index_type != "label+property":
            continue

        if properties == [field]:
            return True

    return False


def is_text_index_exists(memgraph: Any, name: str, label_name: str, fields: Sequence[str]) -> bool:

    """Return True when Memgraph already has the named label text index."""

    for row in get_index_info(memgraph):
        if row.get("label") != label_name:
            continue

        row_index_type = index_type(row)
        properties = property_list(row.get("property"))

        if "label_text" not in row_index_type:
            continue

        if name in row_index_type or properties == list(fields):
            return True

    return False


def create_label_index(memgraph: Any, logger: Any, label: str) -> bool:

    """Create one Memgraph label-only index."""

    command = f"CREATE INDEX ON :{label};"

    try:
        memgraph.execute(command)
        _log_info(logger, f"Created label index: {command}")
        return True

    except Exception as e:
        _log_error(logger, f"Error creating label index {command}: {e}")
        return False


def create_index(memgraph: Any, logger: Any, label: str, field: str) -> bool:

    """Create one Memgraph label-property index."""

    command = f"CREATE INDEX ON :{label}({field});"

    try:
        memgraph.execute(command)
        _log_info(logger, f"Created index: {command}")
        return True

    except Exception as e:
        _log_error(logger, f"Error creating index {command}: {e}")
        return False


def create_text_index(memgraph: Any, logger: Any, name: str, label: str, fields: Sequence[str]) -> bool:

    """Create one Memgraph text index."""

    properties = ", ".join(fields)
    command = f"CREATE TEXT INDEX {name} ON :{label}({properties});"

    try:
        memgraph.execute(command)
        _log_info(logger, f"Created text index: {command}")
        return True

    except Exception as e:
        _log_error(logger, f"Error creating text index {command}: {e}")
        return False


def create_indexes(memgraph: Any, logger: Any, label: str, fields: Sequence[str]) -> Tuple[int, int, int]:

    """Create label-property indexes for one node label, matching InitBase style."""

    created = 0
    skipped = 0
    errors = 0

    for field in fields:
        if is_index_field_exists(memgraph, label, field):
            skipped += 1
            _log_info(logger, f"Index already exists: :{label}({field})")
            continue

        if create_index(memgraph, logger, label, field):
            created += 1
        else:
            errors += 1

    return created, skipped, errors

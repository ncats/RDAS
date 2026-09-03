"""
Export Memgraph indexes into replayable Cypher statements.

Run from the project root:

    conda run -n rdas python export_memgraph_indexes.py

By default, the script writes `memgraph_indexes.cypher` in the project root.
"""

from ast import literal_eval
from pathlib import Path
from typing import Any, Dict, List, Optional
import argparse
import re

from dotenv import load_dotenv

from baseclass.conn import DBConnection as db


DEFAULT_OUTPUT_FILE = "memgraph_indexes.cypher"
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def cypher_identifier(value: Any) -> str:

    """Return a Cypher-safe label, property, or index identifier."""

    text = str(value)

    if IDENTIFIER_RE.match(text):
        return text

    return f"`{text.replace('`', '``')}`"


def property_list(value: Any) -> List[str]:

    """Normalize SHOW INDEX INFO property values across Memgraph versions."""

    if value is None or value == "":
        return []

    if isinstance(value, list):
        return [str(item) for item in value]

    if isinstance(value, tuple):
        return [str(item) for item in value]

    if isinstance(value, str):
        text = value.strip()

        if not text:
            return []

        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = literal_eval(text)
                return [str(item) for item in parsed] if isinstance(parsed, list) else [text]
            except (SyntaxError, ValueError):
                return [text]

        return [text]

    return [str(value)]


def index_type(row: Dict[str, Any]) -> str:

    """Return the Memgraph index type column regardless of driver naming."""

    return str(row.get("index type") or row.get("type") or row.get("index_type") or "")


def create_index_statement(row: Dict[str, Any]) -> Optional[str]:

    """Convert one SHOW INDEX INFO row into a CREATE INDEX statement."""

    label = row.get("label")

    if not label:
        return None

    label_name = cypher_identifier(label)
    properties = property_list(row.get("property"))
    quoted_properties = ", ".join(cypher_identifier(property_name) for property_name in properties)
    current_index_type = index_type(row)

    if "label_text" in current_index_type:
        '''
        Memgraph text indexes are named. SHOW INDEX INFO may return the name,
        but older versions can omit it, so fall back to a stable generated name.
        '''
        index_name = cypher_identifier(row.get("name") or f"{str(label).lower()}_text_index")
        return f"CREATE TEXT INDEX {index_name} ON :{label_name}({quoted_properties});"

    if not properties or current_index_type == "label":
        return f"CREATE INDEX ON :{label_name};"

    return f"CREATE INDEX ON :{label_name}({quoted_properties});"


def export_memgraph_indexes(output_file: Path) -> int:

    """Read Memgraph indexes and write replayable CREATE INDEX statements."""

    load_dotenv()
    memgraph = db().memgraph_conn()
    rows = list(memgraph.execute_and_fetch("SHOW INDEX INFO"))
    statements = []

    for row in rows:
        statement = create_index_statement(row)

        if statement:
            statements.append(statement)

    output_file.write_text("\n".join(statements) + ("\n" if statements else ""), encoding="utf-8")

    return len(statements)


def parse_args() -> argparse.Namespace:

    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Export Memgraph indexes into replayable Cypher.")
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT_FILE,
        help=f"Output Cypher file path. Default: {DEFAULT_OUTPUT_FILE}",
    )

    return parser.parse_args()


def main() -> None:

    """Run the Memgraph index export."""

    args = parse_args()
    output_file = Path(args.output)
    statement_count = export_memgraph_indexes(output_file)
    print(f"Exported {statement_count} Memgraph index statement(s) to {output_file}")


if __name__ == "__main__":
    main()

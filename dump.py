import argparse
import json
import os
import re
import sys
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from dotenv import load_dotenv


_REPO_DIR = Path(__file__).resolve().parent
load_dotenv(_REPO_DIR / ".env")

_DEFAULT_OUTPUT_DIR = _REPO_DIR / "memgraph_dumps"
_SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


class MemgraphDumper:
    """Export Memgraph data through the repo's existing DBConnection helper."""

    def __init__(self, output_dir: os.PathLike = _DEFAULT_OUTPUT_DIR, batch_size: int = 5000, overwrite: bool = True):

        from baseclass.conn import DBConnection

        self.output_dir = Path(output_dir)
        self.batch_size = batch_size
        self.overwrite = overwrite
        self.cypherl_output_path = self.output_dir / "memgraph_dump.cypherl"
        self.json_output_path = self.output_dir / "memgraph_dump.json"
        self.labels_output_dir = self.output_dir / "labels"
        self.memgraph = DBConnection().memgraph_conn()

        if self.memgraph is None:
            raise RuntimeError("Unable to create a Memgraph connection. Check MEMGRAPH_* values in .env.")


    def dump_whole_database_cypherl(self) -> Path:

        """Dump the whole database to a local CYPHERL file using Memgraph's DUMP DATABASE query."""

        path = self._prepare_output_path(self.cypherl_output_path)
        row_count = 0

        with path.open("w", encoding="utf-8") as file_handle:
            for row in self._execute_and_fetch("DUMP DATABASE;"):
                for statement in self._cypher_statements_from_dump_row(row):
                    statement = statement.strip()

                    if not statement:
                        continue

                    file_handle.write(statement)

                    if not statement.endswith(";"):
                        file_handle.write(";")

                    file_handle.write("\n")
                    row_count += 1

        print(f"Wrote {row_count} Cypher statements to {path}")
        return path


    def dump_whole_database_json(self) -> Path:

        """Dump the whole database to a local JSON file with separate node and relationship arrays."""

        path = self._prepare_output_path(self.json_output_path)
        node_count = 0
        relationship_count = 0

        with path.open("w", encoding="utf-8") as file_handle:
            file_handle.write("{\n  \"nodes\": [\n")
            node_count = self._write_json_rows(file_handle, self._iter_node_rows(), self._node_json_record, "    ")
            file_handle.write("\n  ],\n  \"relationships\": [\n")
            relationship_count = self._write_json_rows(file_handle, self._iter_relationship_rows(), self._relationship_json_record, "    ")
            file_handle.write("\n  ]\n}\n")

        print(f"Wrote {node_count} nodes and {relationship_count} relationships to {path}")
        return path


    def dump_each_label_json(self) -> List[Path]:

        """Dump nodes for every Memgraph node label into separate local JSON files."""

        self.labels_output_dir.mkdir(parents=True, exist_ok=True)

        paths = []

        for label_name in self._get_node_labels():
            paths.append(self.dump_label_json(label_name))

        print(f"Wrote {len(paths)} label JSON files to {self.labels_output_dir}")
        return paths


    def dump_label_json(self, label_name: str) -> Path:

        """Dump nodes for one Memgraph node label into a local JSON file."""

        if not label_name:
            raise ValueError("label_name is required.")

        path = self._prepare_output_path(self.labels_output_dir / f"{self._safe_filename(label_name)}.json")
        quoted_label = self._quote_label(label_name)

        with path.open("w", encoding="utf-8") as file_handle:
            file_handle.write("[\n")
            count = self._write_json_rows(file_handle, self._iter_node_rows(quoted_label=quoted_label), self._node_json_record, "  ")
            file_handle.write("\n]\n")

        print(f"Wrote {count} {label_name} nodes to {path}")
        return path


    def _execute_and_fetch(self, query: str, params: Optional[Dict[str, Any]] = None) -> Iterable[Dict[str, Any]]:

        if params is None:
            return self.memgraph.execute_and_fetch(query)

        return self.memgraph.execute_and_fetch(query, params)


    def _prepare_output_path(self, output_path: os.PathLike) -> Path:

        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        if path.exists() and not self.overwrite:
            raise FileExistsError(f"{path} already exists. Set overwrite=True on MemgraphDumper to replace it.")

        return path


    def _cypher_statements_from_dump_row(self, row: Any) -> Iterable[str]:

        if isinstance(row, str):
            yield row
            return

        if isinstance(row, dict):
            for key in ("query", "statement", "cypher", "data", "dump"):
                if key in row:
                    yield from self._coerce_dump_value_to_statements(row[key])
                    return

            if len(row) == 1:
                yield from self._coerce_dump_value_to_statements(next(iter(row.values())))
                return

        yield str(row)


    def _coerce_dump_value_to_statements(self, value: Any) -> Iterable[str]:

        if value is None:
            return

        if isinstance(value, str):
            for statement in value.splitlines():
                yield statement
            return

        if isinstance(value, list):
            for item in value:
                yield from self._coerce_dump_value_to_statements(item)
            return

        yield str(value)


    def _iter_node_rows(self, quoted_label: Optional[str] = None) -> Iterable[Dict[str, Any]]:

        label_clause = f":{quoted_label}" if quoted_label else ""
        last_id = -1

        while True:
            query = f"""
                MATCH (n{label_clause})
                WHERE id(n) > $last_id
                RETURN id(n) AS id, labels(n) AS labels, properties(n) AS properties
                ORDER BY id(n)
                LIMIT $batch_size
            """
            rows = list(self._execute_and_fetch(query, {"last_id": last_id, "batch_size": self.batch_size}))

            if not rows:
                break

            for row in rows:
                last_id = max(last_id, int(row["id"]))
                yield row


    def _iter_relationship_rows(self) -> Iterable[Dict[str, Any]]:

        last_id = -1

        while True:
            query = """
                MATCH ()-[r]->()
                WHERE id(r) > $last_id
                RETURN
                    id(r) AS id,
                    id(startNode(r)) AS start,
                    id(endNode(r)) AS end,
                    type(r) AS label,
                    properties(r) AS properties
                ORDER BY id(r)
                LIMIT $batch_size
            """
            rows = list(self._execute_and_fetch(query, {"last_id": last_id, "batch_size": self.batch_size}))

            if not rows:
                break

            for row in rows:
                last_id = max(last_id, int(row["id"]))
                yield row


    def _get_node_labels(self) -> List[str]:

        query = """
            MATCH (n)
            UNWIND labels(n) AS label
            RETURN DISTINCT label
            ORDER BY label
        """

        return [row["label"] for row in self._execute_and_fetch(query)]


    def _write_json_rows(self, file_handle: Any, rows: Iterable[Dict[str, Any]], record_builder: Any, indent: str) -> int:

        count = 0

        for row in rows:
            if count:
                file_handle.write(",\n")

            file_handle.write(indent)
            json.dump(record_builder(row), file_handle, ensure_ascii=False, default=self._json_default)
            count += 1

        return count


    def _node_json_record(self, row: Dict[str, Any]) -> Dict[str, Any]:

        return {
            "type": "node",
            "id": row["id"],
            "labels": row.get("labels") or [],
            "properties": row.get("properties") or {},
        }


    def _relationship_json_record(self, row: Dict[str, Any]) -> Dict[str, Any]:

        return {
            "type": "relationship",
            "id": row["id"],
            "label": row["label"],
            "start": row["start"],
            "end": row["end"],
            "properties": row.get("properties") or {},
        }


    def _quote_label(self, label_name: str) -> str:

        return f"`{label_name.replace('`', '``')}`"


    def _safe_filename(self, label_name: str) -> str:

        filename = _SAFE_FILENAME_RE.sub("_", label_name.strip()).strip("._")
        return filename or "label"


    def _json_default(self, value: Any) -> Any:

        # JSON cannot preserve every Memgraph/Python value type directly. Keep
        # native JSON types as-is and convert temporal/decimal/unknown values to
        # stable readable values instead of failing halfway through a large dump.
        if isinstance(value, (datetime, date, time)):
            return value.isoformat()

        if isinstance(value, Decimal):
            return int(value) if value == value.to_integral_value() else float(value)

        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")

        return str(value)


def _main() -> int:

    parser = argparse.ArgumentParser(description="Dump the RDAS Memgraph database.")
    parser.add_argument("--output-dir", default=str(_DEFAULT_OUTPUT_DIR), help="Default output directory for generated dump files.")
    parser.add_argument("--batch-size", type=int, default=5000, help="Number of nodes or relationships to fetch per JSON batch.")
    parser.add_argument("--no-overwrite", action="store_true", help="Fail if the target output file already exists.")

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("cypherl", help="Dump the whole database to a CYPHERL file.")
    subparsers.add_parser("json", help="Dump the whole database to a JSON file.")
    subparsers.add_parser("labels", help="Dump every node label to a separate JSON file.")

    label_parser = subparsers.add_parser("label", help="Dump one node label to a JSON file.")
    label_parser.add_argument("label_name", help="Memgraph node label to export.")

    args = parser.parse_args()
    dumper = MemgraphDumper(output_dir=args.output_dir, batch_size=args.batch_size, overwrite=not args.no_overwrite)

    if args.command == "cypherl":
        dumper.dump_whole_database_cypherl()
    elif args.command == "json":
        dumper.dump_whole_database_json()
    elif args.command == "labels":
        dumper.dump_each_label_json()
    elif args.command == "label":
        dumper.dump_label_json(args.label_name)

    return 0


if __name__ == "__main__":

    '''
    python dump.py cypherl
    python dump.py json
    python dump.py labels
    python dump.py label GARD
    '''
    
    sys.exit(_main())

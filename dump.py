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

    def __init__(self, output_dir: os.PathLike = _DEFAULT_OUTPUT_DIR, batch_size: int = 5000):

        '''
        Keep the dump configuration on the dumper instance instead of passing
        paths into each export method. This makes the four public dump methods
        simple commands: connect once, then write to the standard dump paths.
        The dump files overwrite by default because each writer opens its file
        in write mode.
        '''
        from baseclass.conn import DBConnection

        self.output_dir = Path(output_dir)
        self.batch_size = batch_size
        self.progress_interval = max(1, batch_size)
        self.dump_date = date.today().strftime("%Y%m%d")

        '''
        The whole-database exports have date-stamped names under output_dir.
        Per-label exports go under labels/ so a full label split cannot mix with
        the single-file JSON or CYPHERL dump.
        '''
        self.cypherl_output_path = self.output_dir / f"memgraph_dump-{self.dump_date}.cypherl"
        self.json_output_path = self.output_dir / f"memgraph_dump-{self.dump_date}.json"
        self.labels_output_dir = self.output_dir / "labels"

        '''
        Create the dump directories once during initialization. The export
        methods can then focus only on writing their configured files.
        '''
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.labels_output_dir.mkdir(parents=True, exist_ok=True)

        '''
        Match the rest of the repo by using DBConnection().memgraph_conn().
        That helper reads MEMGRAPH_* values from the already-loaded .env file.
        '''
        self.memgraph = DBConnection().memgraph_conn()

        if self.memgraph is None:
            raise RuntimeError("Unable to create a Memgraph connection. Check MEMGRAPH_* values in .env.")


    def dump_whole_database_cypherl(self) -> Path:

        '''
        Use Memgraph's native DUMP DATABASE command for the restore-friendly
        export. This file should contain Cypher/CYPHERL statements that can
        recreate graph data and supported metadata such as indexes/constraints.
        The row shape can vary by Memgraph/gqlalchemy version, so each returned
        row is normalized by _cypher_statements_from_dump_row() before writing.
        '''

        path = self.cypherl_output_path
        row_count = 0

        self._print_progress(f"Starting whole database CYPHERL dump to {path}")

        # "w" - overwrites the existing file by default
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

                    if row_count % self.progress_interval == 0:
                        self._print_progress(f"CYPHERL dump progress: wrote {row_count:,} statements")

        self._print_progress(f"Finished whole database CYPHERL dump: wrote {row_count:,} statements to {path}")

        return path


    def dump_whole_database_json(self) -> Path:

        '''
        Write one local JSON document with two top-level arrays:
            nodes: all graph nodes with id, labels, and properties
            relationships: all relationships with id, type, endpoints, properties
        The output is easier to inspect than CYPHERL, but it is not intended to
        be a drop-in restore format because internal ids are included only as
        references for this exported snapshot.
        '''
        
        node_count = 0
        relationship_count = 0
        path = self.json_output_path

        self._print_progress(f"Starting whole database JSON dump to {path}")

        # "w" - overwrites the existing file by default
        with path.open("w", encoding="utf-8") as file_handle:

            self._print_progress("Whole database JSON dump: writing nodes")

            file_handle.write("{\n  \"nodes\": [\n")
            node_count = self._write_json_rows(file_handle, self._iter_node_rows(), self._node_json_record, "    ", "Whole database JSON node progress")

            self._print_progress(f"Whole database JSON dump: finished nodes ({node_count:,})")

            self._print_progress("Whole database JSON dump: writing relationships")

            file_handle.write("\n  ],\n  \"relationships\": [\n")
            relationship_count = self._write_json_rows(file_handle, self._iter_relationship_rows(), self._relationship_json_record, "    ", "Whole database JSON relationship progress")
            file_handle.write("\n  ]\n}\n")

        self._print_progress(f"Finished whole database JSON dump: wrote {node_count:,} nodes and {relationship_count:,} relationships to {path}")

        return path


    def dump_each_label_json(self) -> List[Path]:

        '''
        Discover every node label currently present in Memgraph, then reuse the
        one-label export for each label. A node with multiple labels will appear
        in each matching label file, which is usually what label-scoped review
        needs and matches how Cypher label filters behave.
        '''

        paths = []
        labels = self._get_node_labels()

        self._print_progress(f"Starting per-label JSON dump for {len(labels):,} labels into {self.labels_output_dir}")

        for index, label_name in enumerate(labels, start=1):

            self._print_progress(f"Per-label JSON dump progress: label {index:,}/{len(labels):,} ({label_name})")
            paths.append(self.dump_label_json(label_name))

        self._print_progress(f"Finished per-label JSON dump: wrote {len(paths):,} label JSON files to {self.labels_output_dir}")
        return paths


    def dump_label_json(self, label_name: str) -> Path:

        '''
        Export only nodes that have the requested label. Relationships are not
        included here because the user's label-split requirement is for node
        label files, and relationships can connect nodes across multiple labels.
        Use dump_whole_database_json() when relationship endpoint context is
        needed alongside node data.
        '''

        if not label_name:
            raise ValueError("label_name is required.")

        path = self.labels_output_dir / f"{self._safe_filename(label_name)}-{self.dump_date}.json"
        quoted_label = self._quote_label(label_name)

        self._print_progress(f"Starting JSON dump for label {label_name} to {path}")

        # "w" - overwrites the existing file by default
        with path.open("w", encoding="utf-8") as file_handle:

            file_handle.write("[\n")
            count = self._write_json_rows(file_handle, self._iter_node_rows(quoted_label=quoted_label), self._node_json_record, "  ", f"Label {label_name} JSON progress")
            file_handle.write("\n]\n")

        self._print_progress(f"Finished JSON dump for label {label_name}: wrote {count:,} nodes to {path}")

        return path


    def _execute_and_fetch(self, query: str, params: Optional[Dict[str, Any]] = None) -> Iterable[Dict[str, Any]]:

        '''
        Keep all gqlalchemy calls behind one small method so the public dump
        code does not repeat the parameter/no-parameter branching.
        '''
        if params is None:
            return self.memgraph.execute_and_fetch(query)

        return self.memgraph.execute_and_fetch(query, params)


    def _print_progress(self, message: str) -> None:

        '''
        Print progress immediately so long-running dump commands keep the shell
        visibly moving even when stdout is buffered by the surrounding process.
        '''
        print(message, flush=True)


    def _cypher_statements_from_dump_row(self, row: Any) -> Iterable[str]:

        '''
        DUMP DATABASE normally streams textual Cypher statements, but different
        client/server versions may wrap those statements in a single-column dict
        or use a named field. Normalize those shapes into plain statement text.
        '''
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

        '''
        Convert nested dump values into individual statement strings. Splitting
        string values by line keeps large dump payloads readable and lets the
        writer add a missing semicolon consistently.
        '''
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

        '''
        Stream nodes by internal id so a full database export does not need to
        hold every node in memory. The optional label clause is already quoted
        before it reaches this method because Cypher labels cannot be passed as
        normal query parameters.
        '''
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

        '''
        Stream relationships by internal id for the same reason as node export:
        large Memgraph databases should be written incrementally instead of
        collected into one Python list.
        '''
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

        '''
        Use the graph itself as the source of truth for label names so new RDAS
        labels are automatically picked up by dump_each_label_json().
        '''
        query = """
            MATCH (n)
            UNWIND labels(n) AS label
            RETURN DISTINCT label
            ORDER BY label
        """

        return [row["label"] for row in self._execute_and_fetch(query)]


    def _write_json_rows(self, file_handle: Any, rows: Iterable[Dict[str, Any]], record_builder: Any, indent: str, progress_label: Optional[str] = None) -> int:

        '''
        Stream JSON array items manually so callers can write very large arrays
        without building a full list first. The count also becomes the progress
        number printed by each public export method.
        '''
        count = 0

        for row in rows:
            if count:
                file_handle.write(",\n")

            file_handle.write(indent)
            json.dump(record_builder(row), file_handle, ensure_ascii=False, default=self._json_default)
            count += 1

            if progress_label is not None and count % self.progress_interval == 0:
                self._print_progress(f"{progress_label}: wrote {count:,} records")

        return count


    def _node_json_record(self, row: Dict[str, Any]) -> Dict[str, Any]:

        '''
        Keep node JSON records explicit and stable: a small type marker, the
        Memgraph internal id for snapshot-local reference, all labels, and the
        property dictionary exactly as returned by Cypher.
        '''
        return {
            "type": "node",
            "id": row["id"],
            "labels": row.get("labels") or [],
            "properties": row.get("properties") or {},
        }


    def _relationship_json_record(self, row: Dict[str, Any]) -> Dict[str, Any]:

        '''
        Keep relationship JSON records parallel to node records. start/end are
        internal node ids from this same dump, so they are useful for inspecting
        topology inside the exported snapshot.
        '''
        return {
            "type": "relationship",
            "id": row["id"],
            "label": row["label"],
            "start": row["start"],
            "end": row["end"],
            "properties": row.get("properties") or {},
        }


    def _quote_label(self, label_name: str) -> str:

        '''
        Labels are part of Cypher syntax, not parameter values. Backtick quoting
        lets labels with unusual characters export safely and doubles embedded
        backticks so the generated query remains valid.
        '''
        return f"`{label_name.replace('`', '``')}`"


    def _safe_filename(self, label_name: str) -> str:

        '''
        Convert label names into portable filenames. Most RDAS labels are simple
        already, but this avoids path separators or whitespace creating awkward
        file paths when labels are added later.
        '''
        filename = _SAFE_FILENAME_RE.sub("_", label_name.strip()).strip("._")
        return filename or "label"


    def _json_default(self, value: Any) -> Any:

        '''
        json.dump() cannot preserve every Memgraph/Python value type directly.
        Keep native JSON types as-is and convert temporal/decimal/unknown values
        to stable readable values instead of failing halfway through a dump.
        '''
        if isinstance(value, (datetime, date, time)):
            return value.isoformat()

        if isinstance(value, Decimal):
            return int(value) if value == value.to_integral_value() else float(value)

        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")

        return str(value)


def _main() -> int:

    '''
    Provide a small CLI around the four dump methods so the script can be used
    directly from the repo root without importing MemgraphDumper in a shell.
    Output paths still live on the constructor via --output-dir.
    '''

    '''
    Show common command examples in `python dump.py --help` so the user can copy
    a complete command without reading the source file.
    '''
    #python dump.py --help
    #python dump.py label --help

    command_examples = """
        Examples:
        python dump.py cypherl
        python dump.py json
        python dump.py labels
        python dump.py label GARD
        python dump.py --output-dir /tmp/memgraph_dumps json
        python dump.py --batch-size 10000 labels
    """

    '''
    The parser owns the top-level CLI description and keeps the examples text
    formatted with its line breaks instead of collapsing it into one paragraph.
    '''
    parser = argparse.ArgumentParser(
        description="Dump the RDAS Memgraph database.",
        epilog=command_examples,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    '''
    These global options apply to every dump command:
        --output-dir changes the constructor-level output directory.
        --batch-size controls JSON fetch batches and progress intervals.
        Existing dump files are overwritten by default.
    '''
    parser.add_argument("--output-dir", default=str(_DEFAULT_OUTPUT_DIR), help="Default output directory for generated dump files.")
    parser.add_argument("--batch-size", type=int, default=5000, help="Number of nodes or relationships to fetch per JSON batch.")

    '''
    Subcommands map directly to the public dump methods on MemgraphDumper. The
    required=True setting makes argparse fail fast when no dump type is chosen.
    '''
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("cypherl", help="Dump the whole database to a CYPHERL file.")
    subparsers.add_parser("json", help="Dump the whole database to a JSON file.")
    subparsers.add_parser("labels", help="Dump every node label to a separate JSON file.")

    '''
    The label subcommand is the only command that needs an extra positional
    value, because the output file name is generated from this Memgraph label.
    '''
    label_parser = subparsers.add_parser("label", help="Dump one node label to a JSON file.")
    label_parser.add_argument("label_name", help="Memgraph node label to export.")

    '''
    Parse the command line once, then construct the dumper with the shared output
    setting before dispatching to the selected dump method.
    '''
    args = parser.parse_args()
    dumper = MemgraphDumper(output_dir=args.output_dir, batch_size=args.batch_size)

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

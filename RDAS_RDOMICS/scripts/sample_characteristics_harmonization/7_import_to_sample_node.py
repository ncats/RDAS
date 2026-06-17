#!/usr/bin/env python3
"""
Import Sample subcategory properties into Memgraph Sample nodes.

For each row, updates the corresponding (Sample {Sample_id}) node by setting
only the non-empty properties present in that row. Empty cells are ignored.
"""

import os
import sys
import json
import time
import pandas as pd
from neo4j import GraphDatabase
from typing import Dict, Any, List, Optional
import re
import argparse
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = CURRENT_DIR.parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

# Toggle: when False, use property names as-is (user guarantees safety)
# Set STRICT_CYPHER_ESCAPING=True to enable backtick-quoting for property keys.
STRICT_CYPHER_ESCAPING = False

from utils import ensure_parent_dir, load_paths


def _sanitize_param_name(key: str) -> str:
    """Create a Cypher-safe parameter name for a given property key."""
    safe = re.sub(r"[^A-Za-z0-9_]", "_", str(key))
    if not safe or not (safe[0].isalpha() or safe[0] == "_"):
        safe = f"p_{safe}"
    return f"prop_{safe}"


def _quote_property(key: str) -> str:
    """Quote a property key with backticks for Cypher/openCypher."""
    return f"`{str(key).replace('`', '``')}`"


def _param_name_for_key(key: str) -> str:
    # Always sanitize parameter names; Cypher param identifiers cannot contain hyphens/parentheses, etc.
    return _sanitize_param_name(key)


def _property_for_key(key: str) -> str:
    if STRICT_CYPHER_ESCAPING:
        return _quote_property(key)
    # Auto-quote only when key is not a valid unquoted identifier
    # Valid: starts with letter or underscore, followed by letters/digits/underscore
    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", str(key)):
        return str(key)
    return _quote_property(key)


def build_set_clause(props: Dict[str, Any]) -> (str, Dict[str, Any]):
    """Build a Cypher SET clause and parameter dict for given property dict.
    Returns (set_clause_string, params_dict)
    Example: ({'A': 'x', 'B': 'y'}) -> ("SET s.A = $A, s.B = $B", {'A': 'x', 'B': 'y'})
    """
    assignments: List[str] = []
    params: Dict[str, Any] = {}
    for key, value in props.items():
        param_key = _param_name_for_key(key)
        assignments.append(f"s.{_property_for_key(key)} = ${param_key}")
        params[param_key] = value
    set_clause = "SET " + ", ".join(assignments)
    return set_clause, params


def _load_resume(resume_file: Optional[str]) -> Optional[Dict[str, Any]]:
    if not resume_file:
        return None
    try:
        if os.path.exists(resume_file):
            with open(resume_file, "r") as f:
                return json.load(f)
    except Exception:
        return None
    return None


def _save_resume(resume_file: Optional[str], state: Dict[str, Any]) -> None:
    if not resume_file:
        return
    tmp_path = f"{resume_file}.tmp"
    with open(tmp_path, "w") as f:
        json.dump(state, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, resume_file)


def import_sample_properties(config_path: str | None = None,
                             resume_file: Optional[str] = None,
                             start_index: int = 0,
                             batch_size: int = 500,
                             reset_resume: bool = False) -> None:
    neo4j_uri = os.environ.get("NEO4J_URI")
    neo4j_user = os.environ.get("NEO4J_USER")
    neo4j_password = os.environ.get("NEO4J_PASSWORD")
    if not all([neo4j_uri, neo4j_user, neo4j_password]):
        raise ValueError("Set NEO4J_URI, NEO4J_USER, and NEO4J_PASSWORD before running this step.")

    paths = load_paths(config_path)
    input_csv = paths["sample_properties_by_subcategory"]
    resume_file = resume_file or paths["sample_import_resume_file"]

    if not os.path.exists(input_csv):
        raise FileNotFoundError(f"Input CSV not found: {input_csv}")

    print("Reading sample properties from CSV...", flush=True)
    df = pd.read_csv(input_csv, low_memory=False)

    if 'Sample_id' not in df.columns:
        raise ValueError("Input CSV must contain 'Sample_id' column")

    # Columns to consider as properties (exclude Sample_id)
    prop_columns = [c for c in df.columns if c != 'Sample_id']
    print(f"Found {len(prop_columns)} property columns to import", flush=True)

    total_rows = len(df)

    # Initialize or load resume state
    resume_state = None if reset_resume else _load_resume(resume_file)
    if resume_state and resume_state.get("input_csv") == input_csv and resume_state.get("total_rows") == total_rows:
        start_index = max(start_index, int(resume_state.get("next_index", 0)))
        print(f"Resuming from row index {start_index} based on resume file: {resume_file}", flush=True)
    else:
        if resume_file and not reset_resume:
            if resume_state is not None:
                print("Resume file present but does not match current job. Starting from beginning.", flush=True)
        # Save initial resume state
        ensure_parent_dir(resume_file)
        _save_resume(resume_file, {"input_csv": input_csv, "total_rows": total_rows, "next_index": start_index, "updated_nodes": 0, "updated_props_total": 0, "skipped_rows": 0, "last_update_ts": time.time()})

    print("Connecting to Memgraph database...", flush=True)
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    updated_nodes = int((resume_state or {}).get("updated_nodes", 0))
    updated_props_total = int((resume_state or {}).get("updated_props_total", 0))
    skipped_rows = int((resume_state or {}).get("skipped_rows", 0))

    with driver.session() as session:
        for start in range(start_index, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            batch = df.iloc[start:end]
            batch_updates = 0
            batch_props = 0

            for _, row in batch.iterrows():
                sample_id = row['Sample_id']
                # Collect non-empty properties
                props: Dict[str, Any] = {}
                for col in prop_columns:
                    val = row[col]
                    # Treat NaN or empty string as missing
                    if pd.isna(val):
                        continue
                    sval = str(val).strip()
                    if not sval:
                        continue
                    props[col] = sval

                if not props:
                    skipped_rows += 1
                    continue

                set_clause, params = build_set_clause(props)
                params['sample_id'] = sample_id

                query = f"""
                MATCH (s:Sample {{Sample_id: $sample_id}})
                {set_clause}
                RETURN count(s) as updated
                """
                result = session.run(query, **params)
                updated = result.single()["updated"]
                if updated:
                    batch_updates += 1
                    batch_props += len(props)

            updated_nodes += batch_updates
            updated_props_total += batch_props
            # Progress output per batch
            print(f"Processed rows {start+1}-{end}: updated {batch_updates} nodes, set {batch_props} properties", flush=True)

            # Save resume checkpoint at end of each batch
            _save_resume(resume_file, {
                "input_csv": input_csv,
                "total_rows": total_rows,
                "next_index": end,
                "updated_nodes": updated_nodes,
                "updated_props_total": updated_props_total,
                "skipped_rows": skipped_rows,
                "last_update_ts": time.time(),
            })

    driver.close()
    print(f"Import complete. Updated {updated_nodes} Sample nodes. Total properties set: {updated_props_total}. Skipped rows (no properties): {skipped_rows}.", flush=True)

    # Mark completion and remove resume file
    if resume_file and os.path.exists(resume_file):
        try:
            os.remove(resume_file)
        except Exception:
            pass


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import sample subcategory properties into Memgraph with resume support.")
    parser.add_argument("--config", help="Optional path to a YAML config file.")
    parser.add_argument("--resume_file", type=str, help="Path to resume checkpoint file.")
    parser.add_argument("--start_index", type=int, default=0, help="Row index to start processing from (overrides resume if higher).")
    parser.add_argument("--batch_size", type=int, default=500, help="Number of rows per batch.")
    parser.add_argument("--reset_resume", action="store_true", help="Ignore existing resume file and start fresh.")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    import_sample_properties(
        config_path=args.config,
        resume_file=args.resume_file,
        start_index=args.start_index,
        batch_size=args.batch_size,
        reset_resume=args.reset_resume,
    )

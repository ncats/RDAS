#!/usr/bin/env python3

"""
Extract Author nodes and relationships linked to prior Clinical Trials, Grants, and Articles.

Inputs:
- Reads associate IDs from three JSON files (articles, clinical trials, grants) or via explicit arguments
  - Articles: uses 'pubmed_id' list
  - Clinical trials: uses 'nctid' list
  - Grants: uses 'application_id' list
- Queries `person_of_all_sources` by associate_id to fetch authors: first_name, last_name, affiliation, orc_id

Deduplication:
- Unique author identity defined by (first_name, last_name) combination (normalized trimming)
- Each unique author is assigned a generated author_id (e.g., AUTH000001, AUTH000002, ...)

Outputs (JSON arrays):
1) authors file: { author_id, first_name, last_name, affiliation, orc_id }
2) associate->author relations: { associate_id, author_id }
3) collaborations (co-authorship within the same associate_id): { author_id_1, author_id_2 }

Example:
  python 5_extract_authors.py \
    --articles_json step1_data_extraction/articles_GARD_0006233.json \
    --trials_json step1_data_extraction/clinical_trials_GARD_0006233.json \
    --grants_json step1_data_extraction/grants_GARD_0006233.json \
    --authors_output step1_data_extraction/authors_GARD_0006233.json \
    --assoc_rel_output step1_data_extraction/author_associate_relations_GARD_0006233.json \
    --collab_output step1_data_extraction/author_collaborations_GARD_0006233.json
"""

import argparse
import itertools
import json
import os
import sys
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Best-effort tqdm import with fallback
try:
    from tqdm import tqdm  # type: ignore
except Exception:  # pragma: no cover
    class tqdm:  # type: ignore
        def __init__(self, total=None, desc=None, unit=None, dynamic_ncols=None):
            self.total = total
        def update(self, n=1):
            pass
        def close(self):
            pass


def import_mysql_driver():
    try:
        import pymysql  # type: ignore
        return ("pymysql", pymysql)
    except Exception:
        pass
    try:
        import mysql.connector as mysql_connector  # type: ignore
        return ("mysql-connector", mysql_connector)
    except Exception:
        pass
    raise RuntimeError(
        "No MySQL driver found. Please install either 'pymysql' or 'mysql-connector-python'."
    )


def connect_mysql(host: str, port: int, user: str, password: str, database: str):
    driver_name, module = import_mysql_driver()
    if driver_name == "pymysql":
        return module.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset="utf8mb4",
            cursorclass=module.cursors.DictCursor,
            read_timeout=300,
            write_timeout=300,
            connect_timeout=30,
        )
    return module.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        connection_timeout=30,
    )


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract Authors and relationships from person_of_all_sources.")
    parser.add_argument("--host", default=os.getenv("MYSQL_HOST"), help="MySQL host")
    parser.add_argument("--port", default=int(os.getenv("MYSQL_PORT", "3306")), type=int, help="MySQL port")
    parser.add_argument("--database", default=os.getenv("MYSQL_DATABASE"), help="Database name")
    parser.add_argument("--user", default=os.getenv("MYSQL_USER"), help="Database user")
    parser.add_argument("--articles_json", required=True, help="Path to articles JSON from step 4")
    parser.add_argument("--trials_json", required=True, help="Path to clinical trials JSON from step 2")
    parser.add_argument("--grants_json", required=True, help="Path to grants JSON from step 3")
    parser.add_argument("--authors_output", required=True, help="Path to output Authors JSON file")
    parser.add_argument("--assoc_rel_output", required=True, help="Path to output associate->author relations JSON file")
    parser.add_argument("--collab_output", required=True, help="Path to output author collaborations JSON file")
    parser.add_argument("--batch_size", type=int, default=500, help="Batch size for fetching authors by associate_id (default: 500)")
    return parser.parse_args(argv)


def chunked(iterable: Iterable[Any], size: int) -> Iterable[List[Any]]:
    buf: List[Any] = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def ensure_output_directory(output_path: str) -> None:
    directory = os.path.dirname(os.path.abspath(output_path))
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


def load_ids_from_json(path: str, key: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    ids: List[str] = []
    for item in data:
        val = item.get(key)
        if val is None:
            continue
        s = str(val).strip()
        if s:
            ids.append(s)
    # Deduplicate while preserving order
    seen = set()
    result: List[str] = []
    for s in ids:
        if s not in seen:
            seen.add(s)
            result.append(s)
    return result


def fetch_authors_for_associate_ids(conn, associate_ids: List[str]) -> List[Dict[str, Any]]:
    if not associate_ids:
        return []
    result: List[Dict[str, Any]] = []
    try:
        cur = conn.cursor(dictionary=True)
    except TypeError:
        cur = conn.cursor()
    try:
        for batch in chunked(associate_ids, max(1, int(min(1000, len(associate_ids))))):
            placeholders = ",".join(["%s"] * len(batch))
            sql = (
                "SELECT associate_id, first_name, last_name, affiliation, orc_id "
                f"FROM person_of_all_sources WHERE associate_id IN ({placeholders})"
            )
            cur.execute(sql, batch)
            rows = cur.fetchall()
            for row in rows:
                if isinstance(row, dict):
                    result.append(row)
                else:
                    associate_id, first_name, last_name, affiliation, orc_id = row
                    result.append({
                        "associate_id": associate_id,
                        "first_name": first_name,
                        "last_name": last_name,
                        "affiliation": affiliation,
                        "orc_id": orc_id,
                    })
    finally:
        try:
            cur.close()
        except Exception:
            pass
    return result


def normalize_name(name: Optional[str]) -> str:
    if name is None:
        return ""
    return " ".join(str(name).strip().split())


def author_key(first_name: Optional[str], last_name: Optional[str]) -> Tuple[str, str]:
    return (normalize_name(first_name), normalize_name(last_name))


def generate_author_id(index: int) -> str:
    return f"AUTH{index:06d}"


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    if not all((args.host, args.user, args.database)):
        print("Set MYSQL_HOST, MYSQL_USER, and MYSQL_DATABASE", file=sys.stderr)
        return 2
    password = os.getenv("MYSQL_PASSWORD")
    if not password:
        print("Set MYSQL_PASSWORD", file=sys.stderr)
        return 2

    # Load associate IDs from the three inputs
    try:
        pubmed_ids = load_ids_from_json(args.articles_json, "pubmed_id")
        nctids = load_ids_from_json(args.trials_json, "nctid")
        app_ids = load_ids_from_json(args.grants_json, "application_id")
    except Exception as e:
        print(f"ERROR: Failed to read input JSONs: {e}", file=sys.stderr)
        return 2

    # Combine and deduplicate associate IDs
    combined: List[str] = []
    seen = set()
    for seq in (pubmed_ids, nctids, app_ids):
        for s in seq:
            if s not in seen:
                seen.add(s)
                combined.append(s)

    total_ids = len(combined)
    print(f"Total associate_ids to process: {total_ids}")

    # Prepare outputs
    ensure_output_directory(args.authors_output)
    ensure_output_directory(args.assoc_rel_output)
    ensure_output_directory(args.collab_output)

    # Connect DB
    try:
        conn = connect_mysql(
            host=args.host,
            port=args.port,
            user=args.user,
            password=password,
            database=args.database,
        )
    except Exception as e:
        print(f"ERROR: Failed to connect to MySQL: {e}", file=sys.stderr)
        return 1

    # Author mapping and outputs
    author_map: Dict[Tuple[str, str], str] = {}  # (first,last) -> author_id
    author_attrs: Dict[str, Dict[str, Any]] = {}  # author_id -> attributes
    assoc_to_authors: Dict[str, List[str]] = {}   # associate_id -> list of author_ids (unique, ordered)

    pbar = tqdm(total=total_ids, desc="Associate IDs", unit="id", dynamic_ncols=True)

    try:
        # Stream over associate ids in batches for DB queries
        for batch in chunked(combined, max(1, int(args.batch_size))):
            rows = fetch_authors_for_associate_ids(conn, batch)
            # Build per-associate unique author sets
            per_assoc_seen: Dict[str, set] = {}
            for row in rows:
                associate_id = str(row.get("associate_id") or "").strip()
                first_name = row.get("first_name")
                last_name = row.get("last_name")
                affiliation = row.get("affiliation")
                orc_id = row.get("orc_id")

                key = author_key(first_name, last_name)
                if key not in author_map:
                    author_id = generate_author_id(len(author_map) + 1)
                    author_map[key] = author_id
                    # Capture values as arrays to keep all unique entries
                    aff_norm = normalize_name(affiliation)
                    orc_norm = (str(orc_id).strip() if orc_id not in (None, "") else "")
                    author_attrs[author_id] = {
                        "author_id": author_id,
                        "first_name": key[0] or None,
                        "last_name": key[1] or None,
                        "affiliation": ([aff_norm] if aff_norm else []),
                        "orc_id": ([orc_norm] if orc_norm else []),
                    }
                else:
                    author_id = author_map[key]
                    # Update affiliation/orc_id collections with new non-empty values
                    aff_norm = normalize_name(affiliation)
                    if aff_norm:
                        aff_list = author_attrs[author_id].setdefault("affiliation", [])
                        if aff_norm not in aff_list:
                            aff_list.append(aff_norm)
                    if orc_id not in (None, ""):
                        orc_norm = str(orc_id).strip()
                        if orc_norm:
                            orc_list = author_attrs[author_id].setdefault("orc_id", [])
                            if orc_norm not in orc_list:
                                orc_list.append(orc_norm)

                if associate_id:
                    if associate_id not in per_assoc_seen:
                        per_assoc_seen[associate_id] = set()
                    if associate_id not in assoc_to_authors:
                        assoc_to_authors[associate_id] = []
                    if author_id not in per_assoc_seen[associate_id]:
                        per_assoc_seen[associate_id].add(author_id)
                        assoc_to_authors[associate_id].append(author_id)

            pbar.update(len(batch))
        pbar.close()

        # Write authors output
        with open(args.authors_output, "w", encoding="utf-8") as f:
            f.write("[\n")
            first = True
            for author_id in sorted(author_attrs.keys()):
                if not first:
                    f.write(",\n")
                json.dump(author_attrs[author_id], f, ensure_ascii=False)
                first = False
            f.write("\n]\n")
        print(f"Wrote Authors to: {args.authors_output} (total {len(author_attrs)})")

        # Write associate->author relations
        with open(args.assoc_rel_output, "w", encoding="utf-8") as f:
            f.write("[\n")
            first = True
            rel_count = 0
            for associate_id in combined:
                author_ids = assoc_to_authors.get(associate_id, [])
                for aid in author_ids:
                    record = {"associate_id": associate_id, "author_id": aid}
                    if not first:
                        f.write(",\n")
                    json.dump(record, f, ensure_ascii=False)
                    first = False
                    rel_count += 1
            f.write("\n]\n")
        print(f"Wrote associate->author relations to: {args.assoc_rel_output} (total {rel_count})")

        # Build and write collaborations (unique pairs across all associates)
        seen_pairs = set()
        with open(args.collab_output, "w", encoding="utf-8") as f:
            f.write("[\n")
            first = True
            pair_count = 0
            for author_ids in assoc_to_authors.values():
                if len(author_ids) < 2:
                    continue
                for a, b in itertools.combinations(author_ids, 2):
                    pair = tuple(sorted((a, b)))
                    if pair in seen_pairs:
                        continue
                    seen_pairs.add(pair)
                    record = {"author_id_1": pair[0], "author_id_2": pair[1]}
                    if not first:
                        f.write(",\n")
                    json.dump(record, f, ensure_ascii=False)
                    first = False
                    pair_count += 1
            f.write("\n]\n")
        print(f"Wrote author collaborations to: {args.collab_output} (total {pair_count})")

    except Exception as e:
        print(f"ERROR: Extraction failed: {e}", file=sys.stderr)
        try:
            conn.close()
        except Exception:
            pass
        return 1

    try:
        conn.close()
    except Exception:
        pass

    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3

"""
Extract Grant nodes linked to a given GARD ID.

Flow:
- Read GardId from step1 JSON (or accept --gard_id)
- Query `grant_gard_project_relation` for DISTINCT application_id by gard_id
- For those application_ids, query in batches:
  - `grant_project` for PROJECT_TITLE, PROJECT_TERMS
  - `grant_abstract` for ABSTRACT_TEXT, YEAR (use latest YEAR if multiple per application)
- Stream-write JSON array with fields per record:
  application_id, title, abstract, terms, year, gard_id

Example:
  python 3_extract_grant.py \
    --gard_json step1_data_extraction/gard_node_GARD_0006233.json \
    --output step1_data_extraction/grants_GARD_0006233.json
"""

import argparse
import json
import os
import sys
from typing import Any, Dict, List, Optional, Tuple, Iterable

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
    parser = argparse.ArgumentParser(description="Extract Grant nodes associated with a GARD ID.")
    parser.add_argument("--host", default=os.getenv("MYSQL_HOST"), help="MySQL host")
    parser.add_argument("--port", default=int(os.getenv("MYSQL_PORT", "3306")), type=int, help="MySQL port")
    parser.add_argument("--database", default=os.getenv("MYSQL_DATABASE"), help="Database name")
    parser.add_argument("--user", default=os.getenv("MYSQL_USER"), help="Database user")
    parser.add_argument("--gard_json", required=False, help="Path to step1 GARD JSON (to read GardId)")
    parser.add_argument("--gard_id", required=False, help="GardID override, e.g., GARD:0006233")
    parser.add_argument("--output", required=True, help="Path to output JSON file")
    parser.add_argument("--batch_size", type=int, default=200, help="Batch size for fetching grant data (default: 200)")
    return parser.parse_args(argv)


def load_gard_id(gard_json_path: Optional[str], gard_id_override: Optional[str]) -> str:
    if gard_id_override:
        return gard_id_override
    if not gard_json_path:
        raise ValueError("Either --gard_id or --gard_json must be provided")
    with open(gard_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    gard_id = data.get("GardId")
    if not gard_id:
        raise ValueError(f"GardId not found in {gard_json_path}")
    return gard_id


def chunked(iterable: Iterable[Any], size: int) -> Iterable[List[Any]]:
    buf: List[Any] = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def fetch_application_ids(conn, gard_id: str) -> List[str]:
    sql = (
        "SELECT DISTINCT application_id FROM grant_gard_project_relation "
        "WHERE gard_id = %s"
    )
    try:
        cur = conn.cursor(dictionary=True)
    except TypeError:
        cur = conn.cursor()
    try:
        cur.execute(sql, (gard_id,))
        rows = cur.fetchall()
    finally:
        try:
            cur.close()
        except Exception:
            pass
    app_ids: List[str] = []
    for row in rows:
        if isinstance(row, dict):
            aid = row.get("application_id")
        else:
            aid = row[0]
        if aid is None:
            continue
        s = str(aid).strip()
        if s:
            app_ids.append(s)
    # Deduplicate while preserving order
    seen = set()
    result: List[str] = []
    for s in app_ids:
        if s not in seen:
            seen.add(s)
            result.append(s)
    return result


def fetch_project_map(conn, application_ids: List[str]) -> Dict[str, Tuple[Optional[str], Optional[str]]]:
    """Return map: application_id -> (PROJECT_TITLE, PROJECT_TERMS)"""
    if not application_ids:
        return {}
    result: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    try:
        cur = conn.cursor(dictionary=True)
    except TypeError:
        cur = conn.cursor()
    try:
        for batch in chunked(application_ids,  max(1,  int( min(1000, len(application_ids)) ))):
            placeholders = ",".join(["%s"] * len(batch))
            sql = f"SELECT APPLICATION_ID, PROJECT_TITLE, PROJECT_TERMS FROM grant_project WHERE APPLICATION_ID IN ({placeholders})"
            cur.execute(sql, batch)
            for row in cur.fetchall():
                if isinstance(row, dict):
                    aid = row.get("APPLICATION_ID")
                    title = row.get("PROJECT_TITLE")
                    terms = row.get("PROJECT_TERMS")
                else:
                    aid, title, terms = row[0], row[1], row[2]
                if aid is None:
                    continue
                result[str(aid).strip()] = (title, terms)
    finally:
        try:
            cur.close()
        except Exception:
            pass
    return result


def fetch_abstract_map(conn, application_ids: List[str]) -> Dict[str, Tuple[Optional[str], Optional[int]]]:
    """Return map: application_id -> (ABSTRACT_TEXT, YEAR) choosing latest YEAR if multiple."""
    if not application_ids:
        return {}
    result: Dict[str, Tuple[Optional[str], Optional[int]]] = {}
    try:
        cur = conn.cursor(dictionary=True)
    except TypeError:
        cur = conn.cursor()
    try:
        for batch in chunked(application_ids, max(1, int(min(1000, len(application_ids))))):
            placeholders = ",".join(["%s"] * len(batch))
            sql = f"SELECT APPLICATION_ID, ABSTRACT_TEXT, YEAR FROM grant_abstract WHERE APPLICATION_ID IN ({placeholders})"
            cur.execute(sql, batch)
            for row in cur.fetchall():
                if isinstance(row, dict):
                    aid = row.get("APPLICATION_ID")
                    abstract = row.get("ABSTRACT_TEXT")
                    year = row.get("YEAR")
                else:
                    aid, abstract, year = row[0], row[1], row[2]
                if aid is None:
                    continue
                key = str(aid).strip()
                # Choose the latest year if multiple
                try:
                    yval = int(year) if year is not None else None
                except Exception:
                    yval = None
                if key not in result:
                    result[key] = (abstract, yval)
                else:
                    _, existing_year = result[key]
                    if existing_year is None or (yval is not None and yval > existing_year):
                        result[key] = (abstract, yval)
    finally:
        try:
            cur.close()
        except Exception:
            pass
    return result


def ensure_output_directory(output_path: str) -> None:
    directory = os.path.dirname(os.path.abspath(output_path))
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    if not all((args.host, args.user, args.database)):
        print("Set MYSQL_HOST, MYSQL_USER, and MYSQL_DATABASE", file=sys.stderr)
        return 2
    password = os.getenv("MYSQL_PASSWORD")
    if not password:
        print("Set MYSQL_PASSWORD", file=sys.stderr)
        return 2

    try:
        gard_id = load_gard_id(args.gard_json, args.gard_id)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

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

    try:
        application_ids = fetch_application_ids(conn, gard_id)
        total = len(application_ids)
        print(f"Total application_ids to process: {total}")

        ensure_output_directory(args.output)
        written = 0
        with open(args.output, "w", encoding="utf-8") as f:
            f.write("[\n")
            first = True
            pbar = tqdm(total=total, desc="Grants", unit="grant", dynamic_ncols=True)
            for batch in chunked(application_ids, max(1, int(args.batch_size))):
                proj_map = fetch_project_map(conn, batch)
                abs_map = fetch_abstract_map(conn, batch)
                for aid in batch:
                    title, terms = proj_map.get(aid, (None, None))
                    abstract, year = abs_map.get(aid, (None, None))
                    record = {
                        "application_id": aid,
                        "title": title,
                        "abstract": abstract,
                        "terms": terms,
                        "year": year,
                        "gard_id": gard_id,
                    }
                    if not first:
                        f.write(",\n")
                    json.dump(record, f, ensure_ascii=False)
                    first = False
                    written += 1
                    pbar.update(1)
            pbar.close()
            f.write("\n]\n")
        print(f"Wrote Grant data to: {args.output}")
        print(f"Total records: {written}")
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

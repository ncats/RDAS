#!/usr/bin/env python3

"""
Extract a single GARD node from the `gard` table and save as JSON for graph ingestion.

- Connects to MariaDB/MySQL
- Queries rows for a specific GardID in table `gard`
- Uses the first non-null Label as GardName and puts the rest into Synonyms (unique, order-preserving)
- Uses the first non-null values for Classification_Level and Disorder_Type
- Outputs JSON with fields: GardId, GardName, Synonyms, Classification_Level, Disorder_Type

Example:
  python 1_extract_gard_node.py \
    --gard_id GARD:0006233 \
    --output ./gard_node_GARD_0006233.json

Password handling:
- Set environment variable MYSQL_PASSWORD
"""

import argparse
import json
import os
import sys
from typing import Any, Dict, List, Optional


def import_mysql_driver():
    """Try to import a MySQL/MariaDB driver, preferring PyMySQL. Return (driver_name, module)."""
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


def connect_mysql(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
):
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
            read_timeout=60,
            write_timeout=60,
            connect_timeout=15,
        )

    # mysql-connector-python
    return module.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        connection_timeout=15,
    )


def ensure_output_directory(output_path: str) -> None:
    directory = os.path.dirname(os.path.abspath(output_path))
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


def unique_preserve_order(items: List[Optional[str]]) -> List[str]:
    seen = set()
    result: List[str] = []
    for item in items:
        if item is None:
            continue
        key = item.strip()
        if not key:
            continue
        lower = key.lower()
        if lower in seen:
            continue
        seen.add(lower)
        result.append(key)
    return result


def fetch_gard_rows(conn, gard_id: str) -> List[Dict[str, Any]]:
    sql = (
        "SELECT GardID, Label, Classification_Level, Disorder_Type "
        "FROM gard WHERE GardID = %s "
        "ORDER BY (Label IS NULL) ASC, Label ASC"
    )
    # Prefer dictionary rows when supported
    try:
        cur = conn.cursor(dictionary=True)  # mysql-connector
    except TypeError:
        cur = conn.cursor()  # PyMySQL with DictCursor set at connection
    try:
        cur.execute(sql, (gard_id,))
        rows = cur.fetchall()
    finally:
        try:
            cur.close()
        except Exception:
            pass
    return rows


def build_gard_node(rows: List[Dict[str, Any]], gard_id: str) -> Dict[str, Any]:
    if not rows:
        raise ValueError(f"No rows found for GardID '{gard_id}' in table 'gard'.")

    labels = [row.get("Label") for row in rows]
    labels_unique = unique_preserve_order(labels)

    def letter_count(s: str) -> int:
        return sum(1 for ch in s if ch.isalpha())

    gard_name = None
    for label in labels_unique:
        if letter_count(label) >= 3:
            gard_name = label
            break
    if gard_name is None:
        gard_name = labels_unique[0] if labels_unique else None

    synonyms = [l for l in labels_unique if l != gard_name]

    # First non-null for classification and disorder type
    classification_level = next((r.get("Classification_Level") for r in rows if r.get("Classification_Level") is not None), None)
    disorder_type = next((r.get("Disorder_Type") for r in rows if r.get("Disorder_Type") is not None), None)

    node = {
        "GardId": gard_id,
        "GardName": gard_name,
        "Synonyms": synonyms,
        "Classification_Level": classification_level,
        "Disorder_Type": disorder_type,
    }
    return node


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract a GARD node from MySQL and save as JSON.")
    parser.add_argument("--host", default=os.getenv("MYSQL_HOST"), help="MySQL host")
    parser.add_argument("--port", default=int(os.getenv("MYSQL_PORT", "3306")), type=int, help="MySQL port")
    parser.add_argument("--database", default=os.getenv("MYSQL_DATABASE"), help="Database name")
    parser.add_argument("--user", default=os.getenv("MYSQL_USER"), help="Database user")
    parser.add_argument("--gard_id", default="GARD:0006233", help="Target GardID, e.g., GARD:0006233")
    parser.add_argument(
        "--output",
        default=None,
        help="Path to output JSON file (defaults to ./gard_node_<GARDID>.json)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    if not all((args.host, args.user, args.database)):
        print("Set MYSQL_HOST, MYSQL_USER, and MYSQL_DATABASE", file=sys.stderr)
        return 2
    password = os.getenv("MYSQL_PASSWORD")
    if not password:
        print("Set MYSQL_PASSWORD", file=sys.stderr)
        return 2


    output_path = args.output
    if not output_path:
        safe_gard = args.gard_id.replace(":", "_")
        output_path = os.path.abspath(f"gard_node_{safe_gard}.json")

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
        rows = fetch_gard_rows(conn, args.gard_id)
        node = build_gard_node(rows, args.gard_id)
    except Exception as e:
        print(f"ERROR: Failed to query/build node: {e}", file=sys.stderr)
        try:
            conn.close()
        except Exception:
            pass
        return 1

    try:
        ensure_output_directory(output_path)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(node, f, ensure_ascii=False, indent=2)
        print(f"Wrote GARD node to: {output_path}")
    except Exception as e:
        print(f"ERROR: Failed to write output file: {e}", file=sys.stderr)
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

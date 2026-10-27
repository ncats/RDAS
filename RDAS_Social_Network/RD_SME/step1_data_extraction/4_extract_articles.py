#!/usr/bin/env python3

"""
Extract Article nodes linked to a given GARD ID.

Flow:
- Read GardId from step1 JSON (or accept --gard_id)
- Query `publication_gard_pubmed` for rows by gard_id, parse comma-separated pubmed_ids into distinct IDs
- For those pubmed_ids, query in batches from `publication_article` to retrieve details
- Stream-write JSON array with fields per record:
  pubmed_id, doi, gard_id, title, abstract_Text, affiliation, first_publication_date,
  publication_year, cited_by_count, is_open_access, in_EPMC, in_PMC, is_EPI, pub_type

Example:
  python 4_extract_articles.py \
    --gard_json step1_data_extraction/gard_node_GARD_0006233.json \
    --output step1_data_extraction/articles_GARD_0006233.json
"""

import argparse
import json
import os
import sys
from typing import Any, Dict, Iterable, List, Optional

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
    parser = argparse.ArgumentParser(description="Extract Article nodes associated with a GARD ID.")
    parser.add_argument("--host", default=os.getenv("MYSQL_HOST"), help="MySQL host")
    parser.add_argument("--port", default=int(os.getenv("MYSQL_PORT", "3306")), type=int, help="MySQL port")
    parser.add_argument("--database", default=os.getenv("MYSQL_DATABASE"), help="Database name")
    parser.add_argument("--user", default=os.getenv("MYSQL_USER"), help="Database user")
    parser.add_argument("--gard_json", required=False, help="Path to step1 GARD JSON (to read GardId)")
    parser.add_argument("--gard_id", required=False, help="GardID override, e.g., GARD:0006233")
    parser.add_argument("--output", required=True, help="Path to output JSON file")
    parser.add_argument("--batch_size", type=int, default=300, help="Batch size for fetching article data (default: 300)")
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


def fetch_pubmed_ids(conn, gard_id: str) -> List[str]:
    sql = (
        "SELECT pubmed_ids FROM publication_gard_pubmed WHERE gard_id = %s"
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

    seen = set()
    ordered_ids: List[str] = []
    def add_pid(val: Any):
        if val is None:
            return
        s = str(val).strip()
        if not s:
            return
        # Split by comma if list stored as comma-separated
        parts = [p.strip() for p in s.split(',')]
        for p in parts:
            if not p:
                continue
            # Filter to numeric IDs; keep as string
            if p not in seen:
                seen.add(p)
                ordered_ids.append(p)

    for row in rows:
        if isinstance(row, dict):
            add_pid(row.get("pubmed_ids"))
        else:
            add_pid(row[0])

    return ordered_ids


def fetch_article_map(conn, pubmed_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    if not pubmed_ids:
        return {}
    result: Dict[str, Dict[str, Any]] = {}
    try:
        cur = conn.cursor(dictionary=True)
    except TypeError:
        cur = conn.cursor()
    try:
        for batch in chunked(pubmed_ids, max(1, int(min(1000, len(pubmed_ids))))):
            placeholders = ",".join(["%s"] * len(batch))
            sql = (
                "SELECT pubmed_id, doi, title, abstract_text, affiliation, first_publication_date, "
                "publication_year, cited_by_count, is_open_access, in_EPMC, in_PMC, is_EPI, pub_type "
                f"FROM publication_article WHERE pubmed_id IN ({placeholders})"
            )
            cur.execute(sql, batch)
            for row in cur.fetchall():
                if isinstance(row, dict):
                    # Build case-insensitive dict for robust key access
                    lower = {str(k).lower(): v for k, v in row.items()}
                    pid = str(lower.get("pubmed_id") or "").strip()
                    if not pid:
                        continue
                    result[pid] = {
                        "pubmed_id": pid,
                        "doi": lower.get("doi"),
                        "title": lower.get("title"),
                        "abstract_text": lower.get("abstract_text"),
                        "affiliation": lower.get("affiliation"),
                        "first_publication_date": lower.get("first_publication_date"),
                        "publication_year": lower.get("publication_year"),
                        "cited_by_count": lower.get("cited_by_count"),
                        "is_open_access": lower.get("is_open_access"),
                        "in_epmc": lower.get("in_epmc"),
                        "in_pmc": lower.get("in_pmc"),
                        "is_epi": lower.get("is_epi"),
                        "pub_type": lower.get("pub_type"),
                    }
                else:
                    (
                        pid, doi, title, abstract_text, affiliation,
                        first_publication_date, publication_year, cited_by_count,
                        is_open_access, in_EPMC, in_PMC, is_EPI, pub_type
                    ) = row
                    pid_s = str(pid).strip()
                    result[pid_s] = {
                        "pubmed_id": pid_s,
                        "doi": doi,
                        "title": title,
                        "abstract_text": abstract_text,
                        "affiliation": affiliation,
                        "first_publication_date": first_publication_date,
                        "publication_year": publication_year,
                        "cited_by_count": cited_by_count,
                        "is_open_access": is_open_access,
                        "in_epmc": in_EPMC,
                        "in_pmc": in_PMC,
                        "is_epi": is_EPI,
                        "pub_type": pub_type,
                    }
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
        pubmed_ids = fetch_pubmed_ids(conn, gard_id)
        total = len(pubmed_ids)
        print(f"Total distinct pubmed_ids to process: {total}")

        ensure_output_directory(args.output)
        written = 0
        with open(args.output, "w", encoding="utf-8") as f:
            f.write("[\n")
            first = True
            pbar = tqdm(total=total, desc="Articles", unit="article", dynamic_ncols=True)
            for batch in chunked(pubmed_ids, max(1, int(args.batch_size))):
                article_map = fetch_article_map(conn, batch)
                for pid in batch:
                    article = article_map.get(pid, {})
                    record = {
                        "pubmed_id": pid,
                        "doi": article.get("doi"),
                        "gard_id": gard_id,
                        "title": article.get("title"),
                        "abstract_Text": article.get("abstract_text"),
                        "affiliation": article.get("affiliation"),
                        "first_publication_date": article.get("first_publication_date"),
                        "publication_year": article.get("publication_year"),
                        "cited_by_count": article.get("cited_by_count"),
                        "is_open_access": article.get("is_open_access"),
                        "in_EPMC": article.get("in_epmc"),
                        "in_PMC": article.get("in_pmc"),
                        "is_EPI": article.get("is_epi"),
                        "pub_type": article.get("pub_type"),
                    }
                    if not first:
                        f.write(",\n")
                    json.dump(record, f, ensure_ascii=False)
                    first = False
                    written += 1
                    pbar.update(1)
            pbar.close()
            f.write("\n]\n")
        print(f"Wrote Article data to: {args.output}")
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

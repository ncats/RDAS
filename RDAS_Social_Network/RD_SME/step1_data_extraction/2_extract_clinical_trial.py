#!/usr/bin/env python3

"""
Extract ClinicalTrial nodes linked to a given GARD ID.

- Reads GardId from a JSON file produced in step 1 (or accepts --gard_id)
- From table `clinical_trail_intervention_drug`, fetches rows by gardId and gathers (nctid, intervention)
- From table `clinical_trial`, fetches `studies` JSON for each nctid and extracts
  briefTitle, officialTitle, briefSummary
- Outputs a JSON array of objects with fields:
  nctid, briefTitle, officialTitle, briefSummary, intervention

Titles extraction rules:
- briefTitle: prefer protocolSection.descriptionModule.briefTitle, else protocolSection.identificationModule.briefTitle
- officialTitle: prefer protocolSection.descriptionModule.officialTitle, else protocolSection.identificationModule.officialTitle
- briefSummary: from protocolSection.descriptionModule.briefSummary

Example:
  python 2_extract_clinical_trial.py \
    --gard_json step1_data_extraction/gard_node_GARD_0006233.json \
    --output step1_data_extraction/clinical_trials_GARD_0006233.json
"""

import argparse
import json
import os
import sys
from typing import Any, Dict, List, Optional, Tuple, Iterable

# Best-effort tqdm import with fallback for non-interactive environments
try:
    from tqdm import tqdm  # type: ignore
except Exception:  # pragma: no cover
    class tqdm:  # type: ignore
        def __init__(self, total=None, desc=None, unit=None, ncols=None, dynamic_ncols=None):
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
    parser = argparse.ArgumentParser(description="Extract ClinicalTrial nodes associated with a GARD ID.")
    parser.add_argument("--host", default=os.getenv("MYSQL_HOST"), help="MySQL host")
    parser.add_argument("--port", default=int(os.getenv("MYSQL_PORT", "3306")), type=int, help="MySQL port")
    parser.add_argument("--database", default=os.getenv("MYSQL_DATABASE"), help="Database name")
    parser.add_argument("--user", default=os.getenv("MYSQL_USER"), help="Database user")
    parser.add_argument("--gard_json", required=False, help="Path to step1 GARD JSON (to read GardId)")
    parser.add_argument("--gard_id", required=False, help="GardID override, e.g., GARD:0006233")
    parser.add_argument("--output", required=True, help="Path to output JSON file")
    parser.add_argument("--studies_batch_size", type=int, default=100, help="Batch size for fetching studies by nctid (default: 100)")
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


def fetch_interventions_by_gard(conn, gard_id: str) -> List[Dict[str, Any]]:
    sql = (
        "SELECT nctid, intervention FROM clinical_trail_intervention_drug "
        "WHERE gardId = %s"
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
    return rows


def chunked(iterable: Iterable[str], size: int) -> Iterable[List[str]]:
    buf: List[str] = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def fetch_studies_for_nctids(conn, nctids: List[str], batch_size: int = 100) -> Dict[str, Optional[str]]:
    if not nctids:
        return {}
    result: Dict[str, Optional[str]] = {}
    try:
        cur_dict = conn.cursor(dictionary=True)
    except TypeError:
        cur_dict = conn.cursor()
    try:
        for batch in chunked(nctids, max(1, int(batch_size))):
            placeholders = ",".join(["%s"] * len(batch))
            sql = f"SELECT nctid, studies FROM clinical_trial WHERE nctid IN ({placeholders})"
            cur_dict.execute(sql, batch)
            for row in cur_dict.fetchall():
                # If not dict rows, map by index
                if isinstance(row, dict):
                    nid = row.get("nctid")
                    studies = row.get("studies")
                else:
                    nid, studies = row[0], row[1]
                result[str(nid)] = studies
    finally:
        try:
            cur_dict.close()
        except Exception:
            pass
    return result


def extract_titles_from_studies(studies_json_text: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not studies_json_text:
        return (None, None, None)
    try:
        doc = json.loads(studies_json_text)
    except Exception:
        return (None, None, None)

    ps = (doc or {}).get("protocolSection", {})
    desc = ps.get("descriptionModule", {}) if isinstance(ps, dict) else {}
    ident = ps.get("identificationModule", {}) if isinstance(ps, dict) else {}

    brief_title = None
    official_title = None
    brief_summary = None

    # Prefer descriptionModule for titles if present, else identificationModule
    if isinstance(desc, dict):
        brief_title = desc.get("briefTitle") or brief_title
        official_title = desc.get("officialTitle") or official_title
        brief_summary = desc.get("briefSummary") or brief_summary
    if isinstance(ident, dict):
        brief_title = brief_title or ident.get("briefTitle")
        official_title = official_title or ident.get("officialTitle")

    if isinstance(brief_summary, dict):
        # Some datasets store summary as {text: ...}
        brief_summary = brief_summary.get("text")

    return (brief_title, official_title, brief_summary)


def build_output(intervention_rows: List[Dict[str, Any]], nctid_to_studies: Dict[str, Optional[str]]) -> List[Dict[str, Any]]:
    # Group interventions by nctid (dedupe, preserve order)
    from collections import OrderedDict

    nctid_to_interventions: Dict[str, List[str]] = {}
    seen_map: Dict[str, set] = {}

    for row in intervention_rows:
        nctid = str(row.get("nctid")).strip() if row.get("nctid") is not None else None
        intervention = row.get("intervention")
        if not nctid:
            continue
        if nctid not in nctid_to_interventions:
            nctid_to_interventions[nctid] = []
            seen_map[nctid] = set()
        if isinstance(intervention, str):
            key = intervention.strip()
            if key and key.lower() not in seen_map[nctid]:
                seen_map[nctid].add(key.lower())
                nctid_to_interventions[nctid].append(key)

    output: List[Dict[str, Any]] = []
    for nctid, interventions in nctid_to_interventions.items():
        titles = extract_titles_from_studies(nctid_to_studies.get(nctid))
        brief_title, official_title, brief_summary = titles
        output.append({
            "nctid": nctid,
            "briefTitle": brief_title,
            "officialTitle": official_title,
            "briefSummary": brief_summary,
            "intervention": interventions,
        })

    # Stable sort by nctid
    output.sort(key=lambda x: x.get("nctid") or "")
    return output


def build_interventions_map(intervention_rows: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    nctid_to_interventions: Dict[str, List[str]] = {}
    seen_map: Dict[str, set] = {}
    for row in intervention_rows:
        nctid_raw = row.get("nctid")
        if nctid_raw is None:
            continue
        nctid = str(nctid_raw).strip()
        if not nctid:
            continue
        if nctid not in nctid_to_interventions:
            nctid_to_interventions[nctid] = []
            seen_map[nctid] = set()
        intervention = row.get("intervention")
        if isinstance(intervention, str):
            key = intervention.strip()
            if key and key.lower() not in seen_map[nctid]:
                seen_map[nctid].add(key.lower())
                nctid_to_interventions[nctid].append(key)
    return nctid_to_interventions


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

    gard_id = None
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
        intervention_rows = fetch_interventions_by_gard(conn, gard_id)
        distinct_nctids = sorted({str(r.get("nctid")).strip() for r in intervention_rows if r.get("nctid") is not None})
        interventions_map = build_interventions_map(intervention_rows)

        total_nctids = len(distinct_nctids)
        print(f"Total NCTIDs to process: {total_nctids}")

        ensure_output_directory(args.output)
        total_written = 0
        with open(args.output, "w", encoding="utf-8") as f:
            f.write("[\n")
            first = True
            pbar = tqdm(total=total_nctids, desc="Clinical trials", unit="trial", dynamic_ncols=True)
            for batch in chunked(distinct_nctids, max(1, int(args.studies_batch_size))):
                nctid_to_studies = fetch_studies_for_nctids(conn, batch, batch_size=len(batch))
                for nctid in batch:
                    brief_title, official_title, brief_summary = extract_titles_from_studies(nctid_to_studies.get(nctid))
                    record = {
                        "nctid": nctid,
                        "briefTitle": brief_title,
                        "officialTitle": official_title,
                        "briefSummary": brief_summary,
                        "intervention": interventions_map.get(nctid, []),
                    }
                    if not first:
                        f.write(",\n")
                    json.dump(record, f, ensure_ascii=False)
                    first = False
                    total_written += 1
                    pbar.update(1)
            pbar.close()
            f.write("\n]\n")

        print(f"Wrote ClinicalTrial data to: {args.output}")
        print(f"Total records: {total_written}")
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

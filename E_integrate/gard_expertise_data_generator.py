import os
import random
import re
import sys
import time
import hashlib

from collections import defaultdict
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Union

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])


def _random_str() -> str:
    return f"{time.time()}{random.random()}"


def _make_hash_key(input_str: str = None) -> str:
    if not input_str:
        input_str = _random_str()

    cleaned = re.sub(r"[^\x20-\x7E]+", "", str(input_str))
    normalized = re.sub(r"\s+", "_", cleaned).lower()

    if not normalized:
        normalized = _random_str().lower()

    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _query(mysql_connection, sql_query: str,params: Optional[Union[Sequence[Any], Mapping[str, Any]]] = None,) -> List[Dict[str, Any]]:

    cursor = None
    try:
        cursor = mysql_connection.cursor(dictionary=True, buffered=True)
        if params is None:
            cursor.execute(sql_query)
        else:
            cursor.execute(sql_query, params)
        return cursor.fetchall() if cursor.description is not None else []
    finally:
        if cursor is not None:
            cursor.close()


def _build_author_name(first: Optional[str], last: Optional[str]) -> Optional[str]:
    return " ".join(part for part in (first, last) if part) or None


def _get_expertise_by_source( mysql_connection, gard_id: str, query: str, source_type: str, id_field: str, title_field: str, abstract_field: str, ) -> Dict[str, Any]:
    
    rows = _query(mysql_connection, query, (gard_id,)) or []
    if not rows:
        return {"gardId": gard_id, "person": []}

    grouped: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        group_id = row.get("rdas_group_id")
        if not group_id:
            continue
        grouped[group_id].append(row)

    persons: List[Dict[str, Any]] = []

    for rdas_group_id, group_rows in grouped.items():

        first_name = next((r.get("first_name") for r in group_rows if r.get("first_name")), None)
        last_name = next((r.get("last_name") for r in group_rows if r.get("last_name")), None)
        author_name = _build_author_name(first_name, last_name)

        abstracts: List[Dict[str, Any]] = [
            {
                "type": source_type,
                "abstract_id": row.get(id_field),
                "title": row.get(title_field),
                "abstract": row.get(abstract_field),
                "table_row_id": row.get("table_row_id"),
            }
            for row in group_rows
        ]

        persons.append(
            {
                "author_id": _make_hash_key(rdas_group_id),
                "rdas_group_id": rdas_group_id,
                "first_name": first_name,
                "last_name": last_name,
                "author_name": author_name,
                "full_abstract": abstracts,
            }
        )

    return {"gardId": gard_id, "person": persons}


def _articles(mysql_connection, gard_id: str) -> Dict[str, Any]:

    query = """
        SELECT pg.gard_id, pg.pubmed_id, p.id AS table_row_id, p.first_name, p.last_name, p.rdas_group_id, pa.title, pa.abstract_text
        FROM rdas_db.publication_gard_searchterm_pubmed_mapping pg
        LEFT JOIN rdas_db.publication_article pa
            ON pa.pubmed_id = pg.pubmed_id
        LEFT JOIN rdas_db.person_of_all_sources p
            ON p.associate_id_int = pg.pubmed_id AND p.source = 'Publication'
        WHERE pg.gard_id = %s
    """
    return _get_expertise_by_source(
        mysql_connection,
        gard_id,
        query,
        source_type="Article",
        id_field="pubmed_id",
        title_field="title",
        abstract_field="abstract_text",
    )


def _clinical_trials(mysql_connection, gard_id: str) -> Dict[str, Any]:

    query = """
        SELECT DISTINCT
            d.gardid, d.nctid,
            p.id AS table_row_id, p.first_name, p.last_name, p.rdas_group_id,
            u.brief_title, u.brief_summary
        FROM (
            SELECT DISTINCT gardId, nctid
            FROM rdas_db.clinical_trial
            WHERE gardId = %s
              AND nctid IS NOT NULL
        ) AS d
        JOIN rdas_db.person_of_all_sources AS p
            ON p.associate_id = d.nctid
           AND p.source = 'ClinicalTrial'
        LEFT JOIN rdas_db.clinical_trial_unique AS u
            ON d.nctid = u.nctid
    """
    return _get_expertise_by_source(
        mysql_connection,
        gard_id,
        query,
        source_type="ClinicalTrial",
        id_field="nctid",
        title_field="brief_title",
        abstract_field="brief_summary",
    )


def _grants(mysql_connection, gard_id: str) -> Dict[str, Any]:
    query = """
        SELECT
            ggpr.gard_id, ggpr.application_id,
            p.id AS table_row_id, p.first_name, p.last_name, p.rdas_group_id,
            ga.abstract_text, gp.project_title
        FROM rdas_db.grant_gard_project_relation ggpr
        LEFT JOIN rdas_db.person_of_all_sources p
            ON p.associate_id_int = ggpr.application_id
           AND p.source = 'GrantProject'
        LEFT JOIN rdas_db.grant_abstract ga
            ON ga.application_id = ggpr.application_id
        LEFT JOIN rdas_db.grant_project gp
            ON ggpr.application_id = gp.application_id
        WHERE ggpr.gard_id = %s
          AND p.rdas_group_id IS NOT NULL
    """
    return _get_expertise_by_source(
        mysql_connection,
        gard_id,
        query,
        source_type="Grant",
        id_field="application_id",
        title_field="project_title",
        abstract_field="abstract_text",
    )


def _merge_person_sources(*sources: Dict[str, Any]) -> Dict[str, Any]:
    if not sources:
        return {"gardId": None, "person": []}

    gard_id = sources[0].get("gardId")
    merged: Dict[Any, Dict[str, Any]] = {}

    for source in sources:
        for person in source.get("person", []):
            group_id = person.get("rdas_group_id")
            if not group_id:
                continue

            if group_id not in merged:
                merged[group_id] = {
                    "author_id": person.get("author_id"),
                    "rdas_group_id": group_id,
                    "first_name": person.get("first_name"),
                    "last_name": person.get("last_name"),
                    "author_name": person.get("author_name"),
                    "full_abstract": list(person.get("full_abstract", [])),
                }
                continue

            existing = merged[group_id]
            if not existing.get("first_name") and person.get("first_name"):
                existing["first_name"] = person.get("first_name")
            if not existing.get("last_name") and person.get("last_name"):
                existing["last_name"] = person.get("last_name")
            if not existing.get("author_name") and person.get("author_name"):
                existing["author_name"] = person.get("author_name")
            existing["full_abstract"].extend(person.get("full_abstract", []))

    return {"gardId": gard_id, "person": list(merged.values())}


def gard_expertise_generator(mysql_connection, batch_size: int = 20, ) -> Iterator[Dict[str, Any]]:
    
    last_id = 0

    while True:
        rows = _query(
            mysql_connection,
            """
                SELECT id, gardid
                FROM rdas_db.gard
                WHERE id > %s
                ORDER BY id
                LIMIT %s
            """,
            (last_id, batch_size),
        )

        if not rows:
            break

        for row in rows:
            last_id = row["id"]
            gard_id = row["gardid"]

            yield _merge_person_sources(
                _articles(mysql_connection, gard_id),
                _clinical_trials(mysql_connection, gard_id),
                _grants(mysql_connection, gard_id),
            )

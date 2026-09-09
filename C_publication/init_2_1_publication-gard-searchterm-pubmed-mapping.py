import argparse
import os
import sys
import time
from typing import Iterable, List, Set, Tuple

from dotenv import load_dotenv

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
load_dotenv()

from baseclass.conn import DBConnection as db
from utils.tools import ask_to_continue


"""
Generate gard_id -> search_term -> pubmed_id mapping rows.

Do this before fetching unique publication articles from
publication_gard_searchterm_pubmed_mapping. The previous implementation used
GROUP_CONCAT(pubmed_ids), which silently truncated large diseases such as
leukemia when the concatenated PMID list exceeded group_concat_max_len.
"""

publication_gard_pubmed = 'publication_gard_pubmed'
publication_gard_searchterm_pubmed_mapping = 'publication_gard_searchterm_pubmed_mapping'

SOURCE_BATCH_SIZE = 100
INSERT_BATCH_SIZE = 1000

fetch_existing_pubmed_ids_sql = f'''
    SELECT pubmed_id
    FROM {publication_gard_searchterm_pubmed_mapping}
    WHERE gard_id = %s
    AND search_term = %s
    AND pubmed_id IS NOT NULL
'''

insert_mapping_sql = f'''
    INSERT INTO {publication_gard_searchterm_pubmed_mapping}
        (gard_id, search_term, pubmed_id)
    SELECT %s, %s, %s
    WHERE NOT EXISTS (
        SELECT 1
        FROM {publication_gard_searchterm_pubmed_mapping}
        WHERE gard_id = %s
        AND search_term = %s
        AND pubmed_id = %s
    )
'''


def parse_args():

    """Parse optional repair scope arguments."""

    parser = argparse.ArgumentParser(description="Generate or repair publication GARD/search-term/PMID mapping rows without GROUP_CONCAT.")
    parser.add_argument("--gard-id", help="Only process one GARD ID, for example GARD:0024146.")
    parser.add_argument("--search-term", help="Only process one search term under the selected/all GARD IDs.")
    parser.add_argument("--dry-run", action="store_true", help="Report missing mapping rows without inserting them.")
    parser.add_argument("--yes", action="store_true", help="Run without the interactive confirmation prompt.")
    return parser.parse_args()


def build_source_query(gard_id: str = None, search_term: str = None):

    """Build the source query and parameters for the requested repair scope."""

    where_clauses = [
        "year_range != 'ignore'",
        "pubmed_ids IS NOT NULL",
    ]
    params = []

    if gard_id:
        where_clauses.append("gard_id = %s")
        params.append(gard_id)

    if search_term:
        where_clauses.append("search_term = %s")
        params.append(search_term)

    query = f'''
        SELECT
            id,
            gard_id,
            search_term,
            year_range,
            pubmed_ids
        FROM rdas_db.{publication_gard_pubmed}
        WHERE {" AND ".join(where_clauses)}
        ORDER BY gard_id, search_term, year_range, id
    '''

    return query, tuple(params)


def iter_pubmed_ids(pubmed_ids_text: str) -> Iterable[int]:

    """Yield valid PubMed IDs from one comma-delimited publication_gard_pubmed value."""

    for raw_pubmed_id in str(pubmed_ids_text or "").split(","):
        raw_pubmed_id = raw_pubmed_id.strip()

        if not raw_pubmed_id:
            continue

        try:
            yield int(raw_pubmed_id)
        except ValueError:
            print(f"Skipping invalid PubMed ID value: {raw_pubmed_id!r}", flush=True)


def fetch_existing_pubmed_ids(check_cursor, gard_id: str, search_term: str) -> Set[int]:

    """Return existing mapping PMIDs for one GARD/search-term pair."""

    check_cursor.execute(fetch_existing_pubmed_ids_sql, (gard_id, search_term))
    return {
        int(row[0])
        for row in check_cursor.fetchall()
        if row[0] is not None
    }


def insert_mapping_batch(insert_cursor, values: List[Tuple[str, str, int, str, str, int]]) -> int:

    """Insert one batch and return the affected row count."""

    if not values:
        return 0

    insert_cursor.executemany(insert_mapping_sql, values)
    return insert_cursor.rowcount if insert_cursor.rowcount >= 0 else len(values)


def flush_mapping_group(mysql, check_cursor, insert_cursor, gard_id: str, search_term: str, source_pubmed_ids: Set[int], source_rows: int, dry_run: bool = False) -> int:

    """Insert missing mapping rows for one GARD/search-term group."""

    if not gard_id or not search_term:
        return 0

    existing_pubmed_ids = fetch_existing_pubmed_ids(check_cursor, gard_id, search_term)
    original_existing_count = len(existing_pubmed_ids)
    values = []
    inserted_count = 0
    missing_pubmed_ids = sorted(source_pubmed_ids - existing_pubmed_ids)

    if dry_run:
        print(
            f"{gard_id} | {search_term} | source_rows={source_rows}, "
            f"source_pmids={len(source_pubmed_ids)}, existing_pmids={original_existing_count}, "
            f"would_insert={len(missing_pubmed_ids)}",
            flush=True,
        )
        return len(missing_pubmed_ids)

    for pubmed_id in missing_pubmed_ids:
        values.append((gard_id, search_term, pubmed_id, gard_id, search_term, pubmed_id))

        if len(values) >= INSERT_BATCH_SIZE:
            inserted_count += insert_mapping_batch(insert_cursor, values)
            mysql.commit()
            values = []

    if values:
        inserted_count += insert_mapping_batch(insert_cursor, values)
        mysql.commit()

    print(
        f"{gard_id} | {search_term} | source_rows={source_rows}, "
        f"source_pmids={len(source_pubmed_ids)}, existing_pmids={original_existing_count}, "
        f"inserted={inserted_count}",
        flush=True,
    )

    return inserted_count


if __name__ == '__main__':

    args = parse_args()

    if not args.yes:
        ok = ask_to_continue(f'Generate gard_id - search_term - pubmed_id mappings in {publication_gard_searchterm_pubmed_mapping}?')
        if not ok:
            sys.exit('------Stopped ------')

    start_time = time.time()
    mysql = db().mysql_conn()
    select_cursor = mysql.cursor(buffered=True)
    check_cursor = mysql.cursor(buffered=True)
    insert_cursor = mysql.cursor()

    total_source_rows = 0
    total_groups = 0
    total_inserted = 0
    current_group = None
    current_source_rows = 0
    current_pubmed_ids: Set[int] = set()

    try:
        scoped_source_query, source_params = build_source_query(args.gard_id, args.search_term)
        print(
            f"Starting mapping generation/repair with gard_id={args.gard_id or 'ALL'}, "
            f"search_term={args.search_term or 'ALL'}.",
            flush=True,
        )
        select_cursor.execute(scoped_source_query, source_params)

        while True:
            rows = select_cursor.fetchmany(SOURCE_BATCH_SIZE)

            if not rows:
                break

            for row in rows:
                _, gard_id, search_term, _, pubmed_ids_text = row
                group_key = (gard_id, search_term)

                if current_group is not None and group_key != current_group:
                    total_inserted += flush_mapping_group(
                        mysql,
                        check_cursor,
                        insert_cursor,
                        current_group[0],
                        current_group[1],
                        current_pubmed_ids,
                        current_source_rows,
                        args.dry_run,
                    )
                    total_groups += 1
                    current_pubmed_ids = set()
                    current_source_rows = 0

                current_group = group_key
                current_source_rows += 1
                total_source_rows += 1
                current_pubmed_ids.update(iter_pubmed_ids(pubmed_ids_text))

        if current_group is not None:
            total_inserted += flush_mapping_group(
                mysql,
                check_cursor,
                insert_cursor,
                current_group[0],
                current_group[1],
                current_pubmed_ids,
                current_source_rows,
                args.dry_run,
            )
            total_groups += 1

        elapsed = time.time() - start_time
        action_label = "Would insert" if args.dry_run else "Inserted"
        print(
            f"\n{action_label} {total_inserted} missing rows from {total_groups} grouped gard/search_term pairs "
            f"and {total_source_rows} publication_gard_pubmed source rows in {elapsed:.2f} seconds.\n",
            flush=True,
        )

    finally:
        if select_cursor:
            select_cursor.close()

        if check_cursor:
            check_cursor.close()

        if insert_cursor:
            insert_cursor.close()

        if mysql:
            mysql.close()

    print(f'\n\n---------- ALL DONE ----------\n\n', flush=True)

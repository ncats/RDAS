"""
Process GARD names/synonyms into match-friendly search terms for grant matching.

This script updates the derived columns in grant_gard_processed_names:
    Synonyms_sw
    Synonyms_sw_bow
    Synonyms_sw_stem
    Synonyms_sw_stem_bow

Manual setup preserved from the original script:

    1. Manually create table grant_gard_processed_names:

        CREATE TABLE rdas_db.grant_gard_processed_names AS (
            SELECT
                GardID,
                MAX(CASE WHEN Label_Predicate_Type = 'Name' THEN Label END) AS `name`,
                GROUP_CONCAT(
                    CASE WHEN Label_Predicate_Type = 'Synonym' THEN Label END
                    SEPARATOR '$$$'
                ) AS `synonyms`,
                Label_Source AS data_source
            FROM rdas_db.gard
            WHERE
                Label_Predicate_Mapping != 'DEPRECATED'
                AND LENGTH(Label) > 3
            GROUP BY
                GardID, MONDO_ID, Label_Source
        );

    2. Add the derived columns:

        ALTER TABLE `rdas_db`.`grant_gard_processed_names`
        ADD COLUMN `Synonyms_sw` TEXT NULL AFTER `data_source`,
        ADD COLUMN `Synonyms_sw_bow` TEXT NULL AFTER `Synonyms_sw`,
        ADD COLUMN `Synonyms_sw_stem` TEXT NULL AFTER `Synonyms_sw_bow`,
        ADD COLUMN `Synonyms_sw_stem_bow` TEXT NULL AFTER `Synonyms_sw_stem`,
        ADD COLUMN `created` DATETIME NULL DEFAULT Current_timestamp()
            AFTER `Synonyms_sw_stem_bow`;

NLTK data note preserved from the original script:
    _stem_text uses NLTK tokenization. If NLTK data is missing, install it in
    your local NLTK data directory, for example:

        cd /Users/zhaot3/nltk_data
        wget --no-check-certificate https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages/tokenizers/punkt.zip
        wget --no-check-certificate https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages/corpora/stopwords.zip
        wget --no-check-certificate https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages/corpora/english_wordnet.zip
"""

'''
These are derived search-term columns used for matching grant text against GARD disease names/synonyms.

1. Synonyms_sw: normalized GARD name + synonyms.

    lowercased
    ASCII-normalized
    duplicates removed
    terms shorter than/equal to 4 chars removed, except sars

2. Synonyms_sw_bow: same as Synonyms_sw, but with extra word-order variants for two-word terms.

    Example: fanconi anemia also adds anemia fanconi
    bow likely means “bag of words”

3. Synonyms_sw_stem: stemmed versions of the normalized terms.

    Uses _stem_text() from utils.tools
    Example conceptually: diseases -> diseas

4. Synonyms_sw_stem_bow: stemmed terms plus the two-word order variants.

    It combines the stemmed form and the bag-of-words permutation behavior.

Important note: even though the column name has sw, the script says it does not remove stop words. That name is historical/misleading.
'''

import os
import sys
from itertools import permutations
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

# Add the project root to the Python path when this file is run directly.
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

TABLE_NAME = "grant_gard_processed_names"
TERM_SEPARATOR = "$$$"
DEFAULT_BATCH_SIZE = 500
MIN_TERM_LENGTH = 4
SARS_TERM = "sars"

SELECT_SQL = f"""
    SELECT
        `GardID` AS gardid,
        `name`,
        `synonyms`,
        `data_source`
    FROM `{TABLE_NAME}`
"""

UPDATE_SQL = f"""
    UPDATE `{TABLE_NAME}`
    SET
        `Synonyms_sw` = %s,
        `Synonyms_sw_bow` = %s,
        `Synonyms_sw_stem` = %s,
        `Synonyms_sw_stem_bow` = %s
    WHERE
        `GardID` <=> %s
        AND `name` <=> %s
        AND `data_source` <=> %s
        AND `synonyms` <=> %s
"""


def normalize_term(value: Any) -> str:
    """Normalize one GARD name/synonym into the lowercase form used for matching."""

    from utils.tools import _normalize_txt

    if value is None:
        return ""

    return _normalize_txt(str(value)).replace('"', "").lower().strip()


def split_synonyms(value: Any) -> List[str]:
    """Split the $$$-delimited synonym string and normalize each concrete term."""

    if not value:
        return []

    terms: List[str] = []

    for raw_term in str(value).split(TERM_SEPARATOR):
        term = normalize_term(raw_term)

        if term:
            terms.append(term)

    return terms


def dedupe_terms(terms: Iterable[str]) -> List[str]:
    """Remove duplicate terms while preserving first-seen order for stable output."""

    seen = set()
    unique_terms: List[str] = []

    for term in terms:
        if not term or term in seen:
            continue

        seen.add(term)
        unique_terms.append(term)

    return unique_terms


def generate_term_orders(term: str) -> List[str]:
    """
    Generate the historical bag-of-words variant for one term.

    The original script only expanded two-word terms, producing both word
    orders. Terms with one word or three-or-more words are left unchanged.
    """

    words = term.split()

    if len(words) != 2:
        return [term]

    return [" ".join(permutation) for permutation in permutations(words)]


def generate_term_orders_for_terms(terms: Iterable[str]) -> List[str]:
    """Generate two-word order variants for every term in the input list."""

    ordered_terms: List[str] = []

    for term in terms:
        ordered_terms.extend(generate_term_orders(term))

    return ordered_terms


def filter_terms_by_length(terms: Iterable[str]) -> List[str]:
    """
    Apply the original length filter.

    Historical behavior kept terms longer than four characters and kept "sars"
    as a special short disease term. The original script had a TODO asking Qian
    about the order of this check; this version preserves the original order:
    generate/stem first, then filter.
    """

    return [
        term
        for term in terms
        if len(term) > MIN_TERM_LENGTH or term == SARS_TERM
    ]


def stem_terms(terms: Iterable[str]) -> List[str]:
    """Stem terms with the shared utils.tools helper and keep useful stems."""

    from utils.tools import _stem_text

    stemmed_terms: List[str] = []

    for term in terms:
        stemmed_term = _stem_text(term)

        if len(stemmed_term) > 2:
            stemmed_terms.append(stemmed_term)

    return stemmed_terms


def build_processed_terms(name: Any, synonyms: Any) -> Tuple[str, str, str, str]:
    """Build the four derived term strings for one GARD processed-name row."""

    terms = split_synonyms(synonyms)
    primary_name = normalize_term(name)

    if primary_name:
        terms.append(primary_name)

    # The column names include "sw", but the historical script did not remove
    # stop words here. Keep that behavior so downstream matching does not drift.
    terms = dedupe_terms(terms)
    stemmed_terms = stem_terms(terms)

    synonyms_sw = dedupe_terms(filter_terms_by_length(terms))
    synonyms_sw_bow = dedupe_terms(filter_terms_by_length(generate_term_orders_for_terms(terms)))
    synonyms_sw_stem = dedupe_terms(filter_terms_by_length(stemmed_terms))
    synonyms_sw_stem_bow = dedupe_terms(
        filter_terms_by_length(generate_term_orders_for_terms(stemmed_terms))
    )

    return (
        TERM_SEPARATOR.join(synonyms_sw),
        TERM_SEPARATOR.join(synonyms_sw_bow),
        TERM_SEPARATOR.join(synonyms_sw_stem),
        TERM_SEPARATOR.join(synonyms_sw_stem_bow),
    )


def build_update_tuple(row: Dict[str, Any]) -> Tuple[Any, ...]:
    """Build one UPDATE parameter tuple from one grant_gard_processed_names row."""

    from utils.tools import _normalize_tuple

    processed_values = _normalize_tuple(
        build_processed_terms(
            row.get("name"),
            row.get("synonyms"),
        )
    )

    # The WHERE parameters intentionally use the raw database values. Normalized
    # values are only for the derived columns; normalizing the natural key fields
    # could prevent the UPDATE from matching rows containing non-ASCII labels.
    return (
        *processed_values,
        row.get("gardid"),
        row.get("name"),
        row.get("data_source"),
        row.get("synonyms"),
    )


def process_gard_names(batch_size: int = DEFAULT_BATCH_SIZE) -> Dict[str, int]:
    """Read GARD name rows, build derived terms, and update MySQL in batches."""

    from baseclass.conn import DBConnection as db

    if batch_size <= 0:
        raise ValueError("batch_size must be greater than 0")

    read_conn = db().mysql_conn()
    write_conn = db().mysql_conn()

    if read_conn is None or write_conn is None:
        if read_conn is not None and read_conn.is_connected():
            read_conn.close()

        if write_conn is not None and write_conn.is_connected():
            write_conn.close()

        raise ConnectionError("Unable to create MySQL connection.")

    read_cursor = None
    update_cursor = None
    summary = {"rows_processed": 0, "rows_updated": 0, "failed_batches": 0}

    try:
        # Separate read/write connections allow the SELECT cursor to stream
        # fetchmany() batches while updates are committed independently.
        read_cursor = read_conn.cursor(dictionary=True)
        update_cursor = write_conn.cursor()
        read_cursor.execute(SELECT_SQL)

        while True:
            rows = read_cursor.fetchmany(batch_size)

            if not rows:
                break

            update_values = [build_update_tuple(row) for row in rows]

            try:
                update_cursor.executemany(UPDATE_SQL, update_values)
                write_conn.commit()

            except Exception:
                summary["failed_batches"] += 1
                write_conn.rollback()
                raise

            summary["rows_processed"] += len(rows)

            if update_cursor.rowcount and update_cursor.rowcount > 0:
                summary["rows_updated"] += update_cursor.rowcount

            print(f"Processed {summary['rows_processed']} GARD name row(s)...", flush=True)

        if summary["rows_processed"] == 0:
            print("\n------------------------ The grant_gard_processed_names table is empty ------------------------\n")

    finally:
        if read_cursor is not None:
            read_cursor.close()

        if update_cursor is not None:
            update_cursor.close()

        if read_conn is not None and read_conn.is_connected():
            read_conn.close()

        if write_conn is not None and write_conn.is_connected():
            write_conn.close()

    return summary


def main() -> int:
    """Run the GARD name processing step."""

    from utils.tools import ask_to_continue

    if not ask_to_continue("*** Process and Upload the GARD names into MySQL database? *** "):
        print("\n------------------------ Stopped ------------------------\n")
        return 1

    summary = process_gard_names()

    print(
        "\nGARD name processing complete: "
        f"rows_processed={summary['rows_processed']}, "
        f"rows_updated={summary['rows_updated']}, "
        f"failed_batches={summary['failed_batches']}\n"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

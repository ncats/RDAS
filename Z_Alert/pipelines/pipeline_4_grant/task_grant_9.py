"""
Prepare GARD disease names and synonyms for grant-to-GARD text matching.

This alert-pipeline task should mirror the preprocessing behavior in
`D_grant/init_8_process_GARD_names.py`. It is a term-preparation step, not the
grant/GARD relationship matcher. Later matching tasks read these derived terms
from `grant_gard_processed_names` and search for them in grant project text such
as titles, abstracts, and project terms.

Source table contract:
    `grant_gard_processed_names` must already contain one row per
    `(GardID, MONDO_ID, Label_Source)` grouping from the `gard` table. This task
    does not create or initially populate those base rows. The historical
    initializer documents that manual population as:

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

Input columns:
    `GardID`
        GARD disease identifier.
    `name`
        Primary disease label for the GARD row.
    `synonyms`
        All synonym labels joined into one `$$$`-separated string.
    `data_source`
        Source system for the GARD labels.

Derived columns refreshed by this step:
    `Synonyms_sw`
        Normalized search terms built from `synonyms` plus the primary `name`.
        Normalization lowercases text, converts non-ASCII characters to ASCII,
        removes double quotes, trims whitespace, removes duplicates while keeping
        first-seen order, and keeps only terms longer than four characters. The
        short disease term `sars` is kept as a special exception.

    `Synonyms_sw_bow`
        The same normalized terms, with an additional historical bag-of-words
        expansion for two-word terms. For example, `fanconi anemia` also yields
        `anemia fanconi`. One-word terms and terms with three or more words are
        left unchanged.

    `Synonyms_sw_stem`
        Stemmed versions of the normalized terms using `utils.tools._stem_text`.
        These terms help later matching catch simple word-form variation.

    `Synonyms_sw_stem_bow`
        Stemmed terms with the same two-word bag-of-words expansion used for
        `Synonyms_sw_bow`.

Historical behavior to preserve:
    - Append the primary `name` to the synonym list before deduplication.
    - Split synonym strings only on the `$$$` separator.
    - Do not remove stop words. The `sw` part of the column names is historical
      and misleading; `init_8_process_GARD_names.py` explicitly preserves the
      no-stop-word-removal behavior.
    - Apply length filtering after stemming and after generating word-order
      variants, matching the initializer's output.
    - Stemming depends on the NLTK data needed by `utils.tools._stem_text`.

NLTK data note preserved from the original script:
    _stem_text uses NLTK tokenization. If NLTK data is missing, install it in
    your local NLTK data directory, for example:

        cd /Users/zhaot3/nltk_data
        wget --no-check-certificate https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages/tokenizers/punkt.zip
        wget --no-check-certificate https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages/corpora/stopwords.zip
        wget --no-check-certificate https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages/corpora/english_wordnet.zip
"""
# Reference: D_grant/init_8_process_GARD_names.py

# If `rdas_db.gard` changes, rebuild or refresh `grant_gard_processed_names`
# so the processed search terms stay aligned with the source GARD labels.

import time
from itertools import permutations
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pipelines.pipeline_4_grant.grant_base import GrantPipelineBase
from utils.tools import _time_hms


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

    The initializer only expanded two-word terms, producing both word orders.
    Terms with one word or three-or-more words are left unchanged.
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
    Apply the original length filter from the initializer.

    Keep terms longer than four characters and keep "sars" as a special short
    disease term. This runs after stemming/order expansion to preserve the
    historical output.
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

    # The column names include "sw", but the initializer does not remove stop
    # words here. Keep that behavior so downstream matching stays compatible.
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

    # The WHERE parameters intentionally use raw database values. Normalizing the
    # natural key fields could prevent the UPDATE from matching rows containing
    # non-ASCII labels.
    return (
        *processed_values,
        row.get("gardid"),
        row.get("name"),
        row.get("data_source"),
        row.get("synonyms"),
    )


class GrantGardNameProcessingTask(GrantPipelineBase):
    """Refresh GARD processed-name search columns used by grant matching."""

    def __init__(self, batch_size: Optional[int] = None):
        super().__init__(init_mysql=True, init_memgraph=False)
        self.batch_size = batch_size if batch_size is not None else DEFAULT_BATCH_SIZE


    def find_new_data(self, gard_node) -> None:
        self.logger.info("GrantGardNameProcessingTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Read GARD name rows, build derived terms, and update MySQL in batches."""

        from baseclass.conn import DBConnection as db

        start_time = time.time()
        read_conn = None
        read_cursor = None
        update_cursor = None
        summary = {"rows_processed": 0, "rows_updated": 0, "failed_batches": 0}

        try:
            if self.batch_size <= 0:
                raise ValueError("batch_size must be greater than 0")

            read_conn = db().mysql_conn()

            if read_conn is None or self.mysql is None:
                raise ConnectionError("Unable to create MySQL connection.")

            # Separate read/write connections allow the SELECT cursor to stream
            # fetchmany() batches while updates are committed independently.
            read_cursor = read_conn.cursor(dictionary=True)
            update_cursor = self.mysql.cursor()
            read_cursor.execute(SELECT_SQL)

            while True:
                rows = read_cursor.fetchmany(self.batch_size)

                if not rows:
                    break

                update_values = [build_update_tuple(row) for row in rows]

                try:
                    update_cursor.executemany(UPDATE_SQL, update_values)
                    self.mysql.commit()

                except Exception:
                    summary["failed_batches"] += 1
                    self.mysql.rollback()
                    raise

                summary["rows_processed"] += len(rows)

                if update_cursor.rowcount and update_cursor.rowcount > 0:
                    summary["rows_updated"] += update_cursor.rowcount

                self.logger.info(f"Processed {summary['rows_processed']} GARD name row(s).")

            if summary["rows_processed"] == 0:
                self.logger.warning(f"The {TABLE_NAME} table is empty.")

            self.logger.info(f"Completed GARD name processing. Summary={summary}")

        except Exception:
            self.logger.exception(f"GrantGardNameProcessingTask failed. summary={summary}")
            raise

        finally:
            if read_cursor is not None:
                read_cursor.close()

            if update_cursor is not None:
                update_cursor.close()

            if read_conn is not None and read_conn.is_connected():
                read_conn.close()

            hours, minutes, seconds = _time_hms(time.time() - start_time)
            self.logger.info(f"Total time elapsed: {hours} hours, {minutes} minutes, {seconds} seconds")

            self.close()


if __name__ == "__main__":

    task = GrantGardNameProcessingTask()
    task.process_new_data()

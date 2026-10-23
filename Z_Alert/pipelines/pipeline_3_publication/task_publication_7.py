import os
import re
import sys
from time import sleep
from typing import List, Optional

import requests
import spacy

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from baseclass.conn import DBConnection
from pipelines.pipeline_base import PipelineBase

"""
Filter false-positive publication/GARD mappings caused by abbreviations.

Step 1 marks publication_gard_searchterm_pubmed_mapping rows as abbreviation
matches when their search_term is a GARD abbreviation label.

Step 2 validates only abbreviation rows whose is_valid value is still NULL.
Valid abbreviations keep the publication/GARD relationship usable
(is_valid = 1); false positives are marked as is_valid = 0.
Unavailable UMLS lookups are retried, then saved as SQL NULL with warnings so
the remaining publications and GARD nodes can continue processing.
"""

# Reference: C_publication/init_8_publication-filter-out-false-positives-of-article.py


SEMANTIC_TYPE_LIST = {
    "Disease or Syndrome",
    "Neoplastic Process",
    "Mental or Behavioral Dysfunction",
    "Injury or Poisoning",
    "Congenital Abnormality",
    "Acquired Abnormality",
    "Environmental Factor",
    "Organism",
    "Physiologic Function",
}

UMLS_API_FAILED = "UMLS_API_FAILED"


class PublicationFalsePositiveFilterTask(PipelineBase):
    """
    Validate abbreviation-based publication/GARD matches.

    Abbreviations can create false-positive publication matches. This task marks
    abbreviation rows, then checks whether the abbreviation appears in the
    abstract as a disease-like expanded term before keeping the mapping valid.
    """

    BATCH_SIZE = 100
    UMLS_MAX_ATTEMPTS = 4
    UMLS_REQUEST_TIMEOUT_SECONDS = 30
    UMLS_RETRY_BASE_DELAY_SECONDS = 2
    UMLS_MAX_RETRY_DELAY_SECONDS = 30

    # Mark current-run publication mappings whose search term is a GARD
    # abbreviation label. Only new publication rows are touched.
    SET_ABBREVIATION_SQL = '''
        UPDATE publication_gard_searchterm_pubmed_mapping pgs
        JOIN gard g
            ON pgs.search_term = g.Label
        JOIN publication_article pa
            ON pa.pubmed_id = pgs.pubmed_id
        SET pgs.is_abbreviation = 1
        WHERE g.Label_Predicate_Mapping LIKE 'ABBRE%'
        AND pa.is_new = 1
    '''

    # Validate only abbreviation mappings that have not already received an
    # is_valid decision.
    FETCH_ABBREVIATION_ROWS_QUERY = '''
        SELECT
            gsp.id,
            gsp.gard_id,
            gsp.search_term,
            gsp.pubmed_id,
            a.abstract_text
        FROM publication_gard_searchterm_pubmed_mapping AS gsp
        INNER JOIN publication_article AS a
            ON gsp.pubmed_id = a.pubmed_id
        WHERE gsp.is_abbreviation = 1
        AND gsp.is_valid IS NULL
        AND a.is_new = 1
    '''

    UPDATE_IS_VALID_SQL = '''
        UPDATE publication_gard_searchterm_pubmed_mapping
        SET is_valid = %s
        WHERE id = %s
    '''

    def __init__(self):
        """Initialize MySQL access, UMLS endpoint config, and SpaCy NLP."""
        
        super().__init__(init_mysql=True, init_memgraph=False)
        self.api_key = os.getenv("UMLS_API_KEY")
        self.umls_search_api = os.getenv("UMLS_SEARCH_API")
        self.umls_cui_api_template = os.getenv("UMLS_CUI_API_TEMPLATE")
        self.nlp = spacy.load("en_core_web_sm")


    # Not implemented
    def find_new_data(self) -> None:
        raise NotImplementedError("PublicationFalsePositiveFilterTask does not implement find_new_data().")


    # implement
    def process_new_data(self) -> None:

        """Save valid, invalid or unknown abbreviation decisions in batches."""

        fetch_cursor = None
        update_cursor = None
        total = 0
        unknown_total = 0

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            update_cursor = self.mysql.cursor(buffered=True)

            # Step 1 from the module header: identify mappings created from
            # GARD abbreviation labels.
            self.set_abbreviation_flag(update_cursor)

            '''
            Snapshot the candidates once, matching the original buffered query.
            Re-running the NULL selector after every batch would repeatedly
            select failed lookups and could keep this long GARD run stuck.
            Release the read lease before HTTP retries, which can take minutes;
            database updates below borrow a fresh connection for each batch.
            '''
            fetch_cursor.execute(self.FETCH_ABBREVIATION_ROWS_QUERY)
            pending_rows = fetch_cursor.fetchall()
            for cursor in (fetch_cursor, update_cursor):
                cursor.close()
            fetch_cursor = None
            update_cursor = None
            mysql = self.mysql
            self.mysql = None
            try:
                mysql.close()
            except Exception as exc:
                self.logger.warning(f"Unable to cleanly return abbreviation read connection to its pool: {exc}")

            for batch_num, start in enumerate(range(0, len(pending_rows), self.BATCH_SIZE), start=1):
                rows = pending_rows[start:start + self.BATCH_SIZE]
                self.logger.info(f"--- batch# = {batch_num} ---")

                update_values = []

                for row in rows:
                    row_id = row["id"]
                    search_term = row.get("search_term")
                    abstract = row.get("abstract_text")

                    try:
                        # Step 2 from the module header: decide whether this
                        # abbreviation resolves to a disease-like phrase.
                        is_valid = self.verify(search_term, abstract)
                    except Exception as e:
                        self.logger.warning(
                            f"Error validating abbreviation row id={row_id}, "
                            f"search_term={search_term}, pubmed_id={row.get('pubmed_id')}: {e}. "
                            "Saving is_valid=NULL and continuing."
                        )
                        is_valid = None

                    if is_valid is None:
                        '''
                        Example: all four UMLS attempts for "severe TBI" fail
                        with HTTP 502. Append (None, row_id), not (0, row_id):
                        Connector/Python writes None as SQL NULL, preserving an
                        unknown outcome instead of rejecting the association.
                        Warnings do not trigger the GARD error-checking mixin.
                        '''
                        update_values.append((None, row_id))
                        unknown_total += 1
                        self.logger.warning(
                            f"No validation decision for abbreviation row id={row_id}, "
                            f"search_term={search_term}, pubmed_id={row.get('pubmed_id')}; "
                            "saving is_valid=NULL and continuing with the next row."
                        )
                        continue

                    update_values.append((1 if is_valid else 0, row_id))
                    total += 1

                    self.logger.info(
                        f"Validated abbreviation row id={row_id}, gard_id={row.get('gard_id')}, "
                        f"search_term={search_term}, pubmed_id={row.get('pubmed_id')}, "
                        f"is_valid={1 if is_valid else 0}. Total={total}"
                    )

                self.update_is_valid(update_values)

            self.logger.info(f"Validated {total} abbreviation publication/GARD mapping rows; saved is_valid=NULL for {unknown_total} unavailable decisions.")

        except Exception as e:
            self.logger.error(f"Error filtering false-positive publication/GARD mappings: {e}")

        finally:
            '''
            Always reach pooled connection release even if an interrupted read
            makes cursor.close() fail. Session reset rolls back any uncommitted
            setup work; another rollback on a dead socket would hide the error.
            '''
            for cursor in (fetch_cursor, update_cursor):
                if cursor is not None:
                    try:
                        cursor.close()
                    except Exception as exc:
                        self.logger.warning(f"Unable to cleanly close abbreviation query cursor: {exc}")

            ''' Explicitly close all db connections. '''
            self.close()


    def set_abbreviation_flag(self, cursor) -> None:
        """Set is_abbreviation for current-run mappings that use GARD abbreviations."""

        try:
            cursor.execute(self.SET_ABBREVIATION_SQL)
            self.mysql.commit()
            self.logger.info(f"Set is_abbreviation = 1 for {cursor.rowcount} publication/GARD mapping rows.")

        except Exception as e:
            self.logger.error(f"Error setting publication/GARD abbreviation flags: {e}")
            self.mysql.rollback()
            raise


    def update_is_valid(self, update_values) -> None:

        """Borrow a fresh lease to persist 1, 0 or SQL NULL after HTTP processing."""

        if not update_values:
            return

        mysql = None
        cursor = None
        try:
            mysql = DBConnection().mysql_conn()
            cursor = mysql.cursor(buffered=True)
            cursor.executemany(self.UPDATE_IS_VALID_SQL, update_values)
            mysql.commit()
            '''
            NULL-to-NULL assignments may report zero changed rows even though
            the UPDATE succeeded. Report submitted outcomes separately from
            changed rows so an API outage is not mistaken for a write failure.
            '''
            self.logger.info(f"Committed {len(update_values)} publication/GARD validation updates; {cursor.rowcount} rows changed.")

        except Exception:
            '''
            An unavailable UMLS decision is a normal NULL value. A database
            failure is different: report it and preserve pending GARD work
            because no outcome can be saved reliably. Returning/resetting the
            lease below discards uncommitted writes without a second failing
            rollback masking the original query error.
            '''
            self.logger.exception("Error saving abbreviation is_valid values.")
            raise
        finally:
            for resource in (cursor, mysql):
                if resource is not None:
                    try:
                        resource.close()
                    except Exception as exc:
                        self.logger.warning(f"Unable to cleanly release abbreviation update resources: {exc}")


    def find_first_sentence(self, text: str, abbreviation: str) -> Optional[str]:
        """Find the first abstract sentence containing the abbreviation."""

        if not text or not abbreviation:
            return None

        pattern = re.escape(abbreviation.strip())

        # Some terms have common hyphenated variants that should still match.
        if abbreviation.strip().casefold() == "flnms":
            pattern = r"F[-]?LNMs"

        sentences = re.split(r"(?<=\.|\!|\?)\s+", text)

        for sentence in sentences:
            if re.search(pattern, sentence, flags=re.IGNORECASE):
                return sentence.strip()

        return None


    def find_full_name_of_abbreviation(self, text: str, abbreviation: str) -> Optional[str]:
        """Extract the phrase immediately before an abbreviation in parentheses."""

        if not text or not abbreviation:
            return None

        pattern = re.compile(r"([A-Za-z\s\-]+)\s*\(([^)]+)\)\s*(?:i\.e\.\s*([^;]+))?")

        for match in re.finditer(pattern, text):
            abbreviation_text = match.group(2) or ""

            if abbreviation.casefold() in abbreviation_text.casefold():
                return match.group(1).strip()

        return None


    def extract_noun_phrases(self, sentence: str) -> List[str]:
        """Use SpaCy noun chunks to isolate candidate disease phrases."""

        doc = self.nlp(sentence)
        return [chunk.text for chunk in doc.noun_chunks]


    def _fetch_umls_items(self, url: str, params: dict, result_field: str, description: str) -> Optional[List[dict]]:

        """Retry a UMLS GET, returning None when usable data remains unavailable.

        Both search and semantic-type calls use the same bounded policy. Retry
        network failures, HTTP 408/429/5xx, invalid JSON and malformed result
        lists up to four total attempts, with delays of 2, 4 and 8 seconds.
        Other HTTP failures, such as 401/403, return unknown immediately.

        The shared HTTPSUtils GET helper has no request timeout/params support
        and stops on HTTP 5xx. Keep this lookup-specific policy local instead
        of changing unrelated API callers. Never log the prepared request URL
        or exception text, which may contain the UMLS API key.
        """
        for attempt in range(1, self.UMLS_MAX_ATTEMPTS + 1):
            response = None
            retry_after = ""
            try:
                response = requests.get(url, params=params, timeout=self.UMLS_REQUEST_TIMEOUT_SECONDS)
                retry_after = response.headers.get("Retry-After", "").strip()
                if response.status_code == 200:
                    data = response.json()
                    result = data.get("result") if isinstance(data, dict) else None
                    items = result.get(result_field) if isinstance(result, dict) else None
                    if isinstance(items, list) and all(isinstance(item, dict) for item in items):
                        return items
                    reason = f"missing or malformed result.{result_field} list"
                else:
                    reason = f"HTTP {response.status_code}"
                    if response.status_code not in {408, 429} and not 500 <= response.status_code <= 599:
                        self.logger.warning(f"{description} unavailable ({reason}); the validation decision will be SQL NULL and processing will continue.")
                        return None
            except (requests.RequestException, ValueError) as exc:
                reason = type(exc).__name__
            finally:
                if response is not None:
                    response.close()

            if attempt == self.UMLS_MAX_ATTEMPTS:
                self.logger.warning(f"{description} unavailable after {attempt} attempts ({reason}); the validation decision will be SQL NULL and processing will continue.")
                return None

            '''
            Bound server-requested waits as well as our exponential backoff.
            Numeric Retry-After values can extend the next wait up to 30 seconds;
            missing, date-formatted or invalid headers use the normal backoff.
            '''
            delay = min(self.UMLS_RETRY_BASE_DELAY_SECONDS * 2 ** (attempt - 1), self.UMLS_MAX_RETRY_DELAY_SECONDS)
            if retry_after.isdigit():
                delay = min(max(delay, int(retry_after)), self.UMLS_MAX_RETRY_DELAY_SECONDS)
            self.logger.warning(f"{description} attempt {attempt}/{self.UMLS_MAX_ATTEMPTS} failed ({reason}); retrying in {delay} seconds.")
            sleep(delay)

    def get_cui(self, term: str) -> Optional[str]:

        """Look up an exact CUI, distinguishing no match from an unavailable API."""

        if not term:
            return None

        if not self.api_key or not self.umls_search_api:
            self.logger.warning("UMLS_API_KEY or UMLS_SEARCH_API is not configured; saving unknown validation decisions as SQL NULL.")
            return UMLS_API_FAILED

        '''
        Let Requests encode the term separately from the API URL: phrases such
        as "severe TBI" or terms containing '&' remain a single string value.
        An unavailable request returns the sentinel. A successful empty search
        returns None; both mean verify() has no UMLS decision and saves NULL.
        '''
        results = self._fetch_umls_items(self.umls_search_api, {"string": term, "searchType": "exact", "apiKey": self.api_key}, "results", f"UMLS search for {term!r}")
        if results is None:
            return UMLS_API_FAILED
        if not results:
            return None

        cui = results[0].get("ui")
        if cui == "NONE":
            return None
        if not isinstance(cui, str) or not cui:
            self.logger.warning(f"UMLS search for {term!r} returned no usable CUI; saving is_valid=NULL and continuing.")
            return UMLS_API_FAILED
        return cui


    def get_semantic_types(self, cui: str) -> Optional[List[str]]:

        """Fetch UMLS semantic type names for a CUI."""

        if not cui:
            return []

        if not self.api_key or not self.umls_cui_api_template:
            self.logger.warning("UMLS_API_KEY or UMLS_CUI_API_TEMPLATE is not configured; saving unknown validation decisions as SQL NULL.")
            return None

        semantic_type_url = self.umls_cui_api_template.format(cui=cui)
        semantic_types = self._fetch_umls_items(semantic_type_url, {"apiKey": self.api_key}, "semanticTypes", f"UMLS semantic types for CUI {cui}")
        if semantic_types is None:
            return None

        return [
            semantic_type.get("name")
            for semantic_type in semantic_types
            if semantic_type.get("name")
        ]


    def is_a_disease_by_spacy(self, term: str) -> bool:
        """Fallback disease check when UMLS has no exact CUI match."""

        if not term:
            return False

        doc = self.nlp(term)

        for ent in doc.ents:
            if ent.label_ in {"DISEASE", "DISORDER", "SYMPTOM", "MEDICAL_CONDITION", "PATHOLOGY"}:
                return True

        return False


    def verify(self, search_term: str, abstract: str) -> Optional[bool]:

        """
        Return True when an abbreviation appears to expand to a disease term.

        The method checks the first sentence containing the abbreviation, pulls
        the expanded phrase before the abbreviation, then checks UMLS semantic
        types. If a CUI or semantic types cannot be obtained, return None so
        the caller saves SQL NULL and continues. A missing UMLS result must not
        become a false decision just because the local model lacks a label.
        """

        if not search_term or not abstract:
            return False

        first_sentence = self.find_first_sentence(abstract, search_term)

        if not first_sentence:
            return False

        full_name = self.find_full_name_of_abbreviation(first_sentence, search_term)

        if not full_name:
            return False

        noun_phrases = self.extract_noun_phrases(full_name)

        if not noun_phrases:
            return False

        last_noun_phrase = noun_phrases[-1]
        cui = self.get_cui(last_noun_phrase)

        if cui == UMLS_API_FAILED or not cui:
            return None

        semantic_types = self.get_semantic_types(cui)
        if not semantic_types:
            return None
        return any(semantic_type in SEMANTIC_TYPE_LIST for semantic_type in semantic_types)

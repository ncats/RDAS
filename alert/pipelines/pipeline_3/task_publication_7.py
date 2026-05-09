import os
import re
import sys
from typing import List, Optional

import requests
import spacy

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase

"""
Filter false-positive publication/GARD mappings caused by abbreviations.

Step 1 marks publication_gard_searchterm_pubmed_mapping rows as abbreviation
matches when their search_term is a GARD abbreviation label.

Step 2 validates only abbreviation rows whose is_valid value is still NULL.
Valid abbreviations keep the publication/GARD relationship usable
(is_valid = 1); false positives are marked as is_valid = 0.
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


class PublicationFalsePositiveFilterTask(PipelineBase):

    BATCH_SIZE = 100

    SET_ABBREVIATION_SQL = '''
        UPDATE publication_gard_searchterm_pubmed_mapping pgs
        JOIN gard g
            ON pgs.search_term = g.Label
        JOIN update_publication_article upa
            ON upa.pubmed_id = pgs.pubmed_id
        SET pgs.is_abbreviation = 1
        WHERE g.Label_Predicate_Mapping LIKE 'ABBRE%'
        AND upa.is_new = 1
    '''

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
        INNER JOIN update_publication_article AS upa
            ON upa.pubmed_id = gsp.pubmed_id
        WHERE gsp.is_abbreviation = 1
        AND gsp.is_valid IS NULL
        AND upa.is_new = 1
    '''

    UPDATE_IS_VALID_SQL = '''
        UPDATE publication_gard_searchterm_pubmed_mapping
        SET is_valid = %s
        WHERE id = %s
    '''

    def __init__(self):
        
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

        fetch_cursor = None
        update_cursor = None
        total = 0
        batch_num = 0

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            update_cursor = self.mysql.cursor(buffered=True)

            self.set_abbreviation_flag(update_cursor)

            fetch_cursor.execute(self.FETCH_ABBREVIATION_ROWS_QUERY)

            while True:
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    self.logger.info("No more abbreviation rows to validate.")
                    break

                batch_num += 1
                self.logger.info(f"--- batch# = {batch_num} ---")

                update_values = []

                for row in rows:
                    row_id = row["id"]
                    search_term = row.get("search_term")
                    abstract = row.get("abstract_text")

                    try:
                        is_valid = self.verify(search_term, abstract)
                    except Exception as e:
                        self.logger.error(
                            f"Error validating abbreviation row id={row_id}, "
                            f"search_term={search_term}, pubmed_id={row.get('pubmed_id')}: {e}"
                        )
                        is_valid = False

                    update_values.append((1 if is_valid else 0, row_id))
                    total += 1

                    self.logger.info(
                        f"Validated abbreviation row id={row_id}, gard_id={row.get('gard_id')}, "
                        f"search_term={search_term}, pubmed_id={row.get('pubmed_id')}, "
                        f"is_valid={1 if is_valid else 0}. Total={total}"
                    )

                self.update_is_valid(update_cursor, update_values)

            self.logger.info(f"Validated {total} abbreviation publication/GARD mapping rows.")

        except Exception as e:
            self.logger.error(f"Error filtering false-positive publication/GARD mappings: {e}")

            if self.mysql:
                self.mysql.rollback()

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            if update_cursor:
                update_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def set_abbreviation_flag(self, cursor) -> None:

        try:
            cursor.execute(self.SET_ABBREVIATION_SQL)
            self.mysql.commit()
            self.logger.info(f"Set is_abbreviation = 1 for {cursor.rowcount} publication/GARD mapping rows.")

        except Exception as e:
            self.logger.error(f"Error setting publication/GARD abbreviation flags: {e}")
            self.mysql.rollback()
            raise


    def update_is_valid(self, cursor, update_values) -> None:

        if not update_values:
            return

        try:
            cursor.executemany(self.UPDATE_IS_VALID_SQL, update_values)
            self.mysql.commit()
            self.logger.info(f"Updated is_valid for {cursor.rowcount} publication/GARD mapping rows.")

        except Exception as e:
            self.logger.error(f"Error updating abbreviation is_valid values: {e}")
            self.mysql.rollback()


    def find_first_sentence(self, text: str, abbreviation: str) -> Optional[str]:

        if not text or not abbreviation:
            return None

        pattern = re.escape(abbreviation.strip())

        if abbreviation.strip().casefold() == "flnms":
            pattern = r"F[-]?LNMs"

        sentences = re.split(r"(?<=\.|\!|\?)\s+", text)

        for sentence in sentences:
            if re.search(pattern, sentence, flags=re.IGNORECASE):
                return sentence.strip()

        return None


    def find_full_name_of_abbreviation(self, text: str, abbreviation: str) -> Optional[str]:

        if not text or not abbreviation:
            return None

        pattern = re.compile(r"([A-Za-z\s\-]+)\s*\(([^)]+)\)\s*(?:i\.e\.\s*([^;]+))?")

        for match in re.finditer(pattern, text):
            abbreviation_text = match.group(2) or ""

            if abbreviation.casefold() in abbreviation_text.casefold():
                return match.group(1).strip()

        return None


    def extract_noun_phrases(self, sentence: str) -> List[str]:
        doc = self.nlp(sentence)
        return [chunk.text for chunk in doc.noun_chunks]


    def get_cui(self, term: str) -> Optional[str]:

        if not term:
            return None

        if not self.api_key or not self.umls_search_api:
            self.logger.error("UMLS_API_KEY or UMLS_SEARCH_API is not configured.")
            return None

        search_url = f"{self.umls_search_api}?string={term}&searchType=exact&apiKey={self.api_key}"

        try:
            response = requests.get(search_url, timeout=30)
        except requests.RequestException as e:
            self.logger.error(f"Error searching UMLS term '{term}': {e}")
            return None

        if response.status_code != 200:
            self.logger.error(f"Error searching UMLS term '{term}': {response.status_code}, {response.text}")
            return None

        data = response.json()
        results = data.get("result", {}).get("results", [])

        if not results:
            return None

        return results[0].get("ui")


    def get_semantic_types(self, cui: str) -> List[str]:

        if not cui:
            return []

        if not self.api_key or not self.umls_cui_api_template:
            self.logger.error("UMLS_API_KEY or UMLS_CUI_API_TEMPLATE is not configured.")
            return []

        semantic_type_url = f"{self.umls_cui_api_template.format(cui=cui)}?apiKey={self.api_key}"

        try:
            response = requests.get(semantic_type_url, timeout=30)
        except requests.RequestException as e:
            self.logger.error(f"Error retrieving semantic types for CUI {cui}: {e}")
            return []

        if response.status_code != 200:
            self.logger.error(f"Error retrieving semantic types for CUI {cui}: {response.status_code}, {response.text}")
            return []

        data = response.json()
        semantic_types = data.get("result", {}).get("semanticTypes", [])

        return [
            semantic_type.get("name")
            for semantic_type in semantic_types
            if semantic_type.get("name")
        ]


    def is_a_disease_by_spacy(self, term: str) -> bool:

        if not term:
            return False

        doc = self.nlp(term)

        for ent in doc.ents:
            if ent.label_ in {"DISEASE", "DISORDER", "SYMPTOM", "MEDICAL_CONDITION", "PATHOLOGY"}:
                return True

        return False


    def verify(self, search_term: str, abstract: str) -> bool:

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

        if cui:
            semantic_types = self.get_semantic_types(cui)
            return any(semantic_type in SEMANTIC_TYPE_LIST for semantic_type in semantic_types)

        return self.is_a_disease_by_spacy(last_noun_phrase)

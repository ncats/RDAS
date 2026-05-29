"""
Find relationships between GARD diseases and NIH grant projects.

Core purpose preserved from the original script:
    The primary goal of this script is to identify and quantify the relevance
    of various GARD diseases to NIH grant projects. It does this by performing
    text analysis on project titles, public health relevance statements, and
    abstract texts to find mentions of GARD disease names and synonyms.

This file is the single-process version of the pipeline. It is easier to debug
than the multiprocessing version because one Python process owns the MySQL
connection, spaCy model, ClinicalBERT model, and progress output.

Operational notes preserved from the original script:

    1. Find and inspect duplicate APPLICATION_ID values in grant_abstract:

        SELECT application_id
        FROM rdas_db.grant_abstract
        GROUP BY application_id
        HAVING COUNT(1) > 1;

        SELECT *
        FROM rdas_db.grant_abstract
        WHERE APPLICATION_ID IN (
            7916889, 10200508, 10224557, 10410101,
            10711865, 10817330, 10991546, 10993253
        )
        ORDER BY APPLICATION_ID;

    2. Create indexes on application_id and year on grant_abstract, and on
       grant_project as needed:

        ALTER TABLE `rdas_db`.`grant_abstract`
        ADD INDEX `idx_grant_abstract_year` (`YEAR` ASC) VISIBLE,
        ADD INDEX `idx_grant_abstract_app_id` (`APPLICATION_ID` ASC) VISIBLE,
        ADD INDEX `idx_grant_abstract_app_id_yr` (`APPLICATION_ID` ASC, `YEAR` ASC) VISIBLE;

    3. No duplicate APPLICATION_ID in grant_project. Use p.FY = a.YEAR to
       de-duplicate APPLICATION_ID in grant_abstract:

        SELECT p.APPLICATION_ID, p.FY, p.PROJECT_TITLE, p.PHR, a.ABSTRACT_TEXT
        FROM rdas_db.grant_project p, rdas_db.grant_abstract a
        WHERE p.APPLICATION_ID = a.APPLICATION_ID
          AND p.FY = a.YEAR;

    4. Check generated relationship results:

        SELECT gard_id, COUNT(gard_id)
        FROM rdas_db.grant_gard_project_relation
        GROUP BY gard_id
        ORDER BY COUNT(gard_id) DESC;

        SELECT application_id, COUNT(application_id)
        FROM rdas_db.grant_gard_project_relation
        GROUP BY application_id
        ORDER BY COUNT(application_id) DESC;

        SELECT application_id, gard_id, COUNT(*)
        FROM rdas_db.grant_gard_project_relation
        GROUP BY application_id, gard_id
        ORDER BY COUNT(*) DESC;

        SELECT application_id, gard_id, COUNT(*)
        FROM rdas_db.grant_gard_project_relation
        GROUP BY application_id, gard_id
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC;

NLTK data note preserved from the original script:
    _stem_text uses NLTK tokenization. If NLTK data is missing, install it in
    your local NLTK data directory, for example:

        cd /Users/zhaot3/nltk_data
        wget --no-check-certificate https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages/tokenizers/punkt.zip
        wget --no-check-certificate https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages/corpora/stopwords.zip
        wget --no-check-certificate https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages/corpora/english_wordnet.zip
"""

import math
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Add the project root to the Python path when this file is run directly.
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

WORD_PATTERN = re.compile(r"\b\w+\b")
GARD_TERM_SEPARATOR = "$$$"

DEFAULT_ID_STEP = 1
DEFAULT_RANGE_BATCH_SIZE = 100
DEFAULT_FETCH_SIZE = 250
DEFAULT_INSERT_BATCH_SIZE = 100

GARD_PROCESSED_NAMES: List[Dict[str, Any]] = []
GARD_ID_BY_NAME: Dict[str, Any] = {}
SPACY_MODEL = None
CLINICAL_BERT_TOKENIZER = None
CLINICAL_BERT_MODEL = None

PROJECT_ID_BOUNDS_SQL = """
    SELECT
        MIN(p.id) AS min_id,
        MAX(p.id) AS max_id
    FROM rdas_db.grant_project p
    INNER JOIN rdas_db.grant_abstract a
        ON p.APPLICATION_ID = a.APPLICATION_ID
        AND p.FY = a.YEAR
    LEFT JOIN rdas_db.grant_gard_project_relation gpr
        ON p.APPLICATION_ID = gpr.application_id
    WHERE gpr.application_id IS NULL
"""

PROJECT_SELECT_SQL = """
    SELECT
        p.id,
        p.APPLICATION_ID,
        p.FY,
        p.PROJECT_TITLE,
        p.PHR,
        p.core_project_num,
        a.ABSTRACT_TEXT
    FROM rdas_db.grant_project p
    INNER JOIN rdas_db.grant_abstract a
        ON p.APPLICATION_ID = a.APPLICATION_ID
        AND p.FY = a.YEAR
    LEFT JOIN rdas_db.grant_gard_project_relation gpr
        ON p.APPLICATION_ID = gpr.application_id
    WHERE
        p.id BETWEEN %s AND %s
        AND gpr.application_id IS NULL
"""

RELATION_INSERT_SQL = """
    INSERT INTO grant_gard_project_relation (
        gard_id,
        application_id,
        gard_name,
        source_type,
        confidence_score,
        semantic_similarity,
        core_project_num,
        raw_result
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

GARD_PROCESSED_NAMES_SQL = """
    SELECT
        gardid,
        name,
        synonyms,
        synonyms_sw,
        synonyms_sw_bow,
        synonyms_sw_stem,
        synonyms_sw_stem_bow
    FROM grant_gard_processed_names
"""


def split_sentence(sentence: str) -> List[str]:
    """Split text with the original word regex."""

    return WORD_PATTERN.findall(sentence)


def text_has_all_words(text_words: Iterable[str], term: str) -> bool:
    """Return True when every token in term appears in the text token list."""

    word_set = set(text_words)

    for word in split_sentence(term):
        if word not in word_set:
            return False

    return True


def safe_split_terms(value: Any) -> List[str]:
    """Split $$$-delimited GARD processed-name fields into clean term lists."""

    if value is None:
        return []

    return [
        term
        for term in str(value).split(GARD_TERM_SEPARATOR)
        if term
    ]


def load_gard_processed_names() -> List[Dict[str, Any]]:
    """Load processed GARD names and term lists from MySQL."""

    from baseclass.conn import DBConnection as db

    mysql = db().mysql_conn()

    if mysql is None:
        raise ConnectionError("Unable to create MySQL connection.")

    cursor = None

    try:
        cursor = mysql.cursor(dictionary=True)
        cursor.execute(GARD_PROCESSED_NAMES_SQL)
        rows = cursor.fetchall()

    finally:
        if cursor is not None:
            cursor.close()

        if mysql is not None and mysql.is_connected():
            mysql.close()

    return [
        {
            **row,
            "synonyms": safe_split_terms(row.get("synonyms")),
            "synonyms_sw": safe_split_terms(row.get("synonyms_sw")),
            "synonyms_sw_bow": safe_split_terms(row.get("synonyms_sw_bow")),
            "synonyms_sw_stem": safe_split_terms(row.get("synonyms_sw_stem")),
            "synonyms_sw_stem_bow": safe_split_terms(row.get("synonyms_sw_stem_bow")),
        }
        for row in rows
    ]


def ensure_gard_terms_loaded() -> None:
    """Load GARD term globals once per process."""

    global GARD_PROCESSED_NAMES
    global GARD_ID_BY_NAME

    if GARD_PROCESSED_NAMES:
        return

    GARD_PROCESSED_NAMES = load_gard_processed_names()
    GARD_ID_BY_NAME = {
        row["name"]: row["gardid"]
        for row in GARD_PROCESSED_NAMES
        if row.get("name")
    }


def get_spacy_model():
    """Load the spaCy sentence model once for this single-process run."""

    global SPACY_MODEL

    if SPACY_MODEL is None:
        import spacy

        SPACY_MODEL = spacy.load("en_core_web_sm")

    return SPACY_MODEL


def get_clinicalbert_components():
    """Load ClinicalBERT tokenizer/model once for this single-process run."""

    global CLINICAL_BERT_TOKENIZER
    global CLINICAL_BERT_MODEL

    if CLINICAL_BERT_TOKENIZER is None or CLINICAL_BERT_MODEL is None:
        from transformers import AutoModel, AutoTokenizer

        # ClinicalBERT model preserved from the original script:
        # "emilyalsentzer/Bio_ClinicalBERT"
        model_name = "emilyalsentzer/Bio_ClinicalBERT"
        hf_token = os.getenv("HUGGINGFACE_TOKEN")

        CLINICAL_BERT_TOKENIZER = AutoTokenizer.from_pretrained(model_name, token=hf_token)
        CLINICAL_BERT_MODEL = AutoModel.from_pretrained(model_name, token=hf_token)
        CLINICAL_BERT_MODEL.eval()

    return CLINICAL_BERT_TOKENIZER, CLINICAL_BERT_MODEL


def get_gard_terms_for_text(text: str, list_check: str) -> Dict[str, List[int]]:
    """Find GARD term occurrences in title/abstract text for one term column."""

    ensure_gard_terms_loaded()

    if list_check in {"Synonyms_stem", "Synonyms_sw_stem", "Synonyms_stem_bow", "Synonyms_sw_stem_bow"}:
        from utils.tools import _stem_text

        normalized_text = _stem_text(text.lower())

    elif list_check in {"Synonyms_sw_nltk"}:
        from utils.tools import _remove_stop_words

        normalized_text = _remove_stop_words(text.lower())

    else:
        normalized_text = text.lower()

    text_words = split_sentence(normalized_text)
    output: Dict[str, List[int]] = {}
    term_column = list_check.lower()

    for gard in GARD_PROCESSED_NAMES:
        gard_name = gard["name"]

        if not gard_name:
            continue

        gard_names_to_check = gard[term_column]

        for gard_term in gard_names_to_check:
            if gard_term in normalized_text and text_has_all_words(text_words, gard_term):
                count = text_words.count(gard_term) if len(gard_term.split()) == 1 else normalized_text.count(gard_term)
                output[gard_name] = [output[gard_name][0] + count] if gard_name in output else [count]

    return output


def get_gard_title_stem_exact(text: str) -> Optional[Dict[str, int]]:
    """Find title matches using exact and stemmed bag-of-words term columns."""

    exact_matching = get_gard_terms_for_text(text, "Synonyms_sw_bow") or {}
    stemming_check = get_gard_terms_for_text(text, "Synonyms_sw_stem_bow") or {}
    combined_dict = {**exact_matching, **stemming_check}

    # Remove keys that are part of another key, preserving the original title
    # matching behavior that prefers the longer disease name.
    keys_to_remove = {
        key1
        for key1 in combined_dict
        for key2 in combined_dict
        if key1 != key2 and key1 in key2
    }

    combined_dict = {
        key: 1
        for key in combined_dict
        if key not in keys_to_remove
    }

    return combined_dict or None


def semantic_similarity(input_text: str, target_term: str) -> float:
    """
    Calculate ClinicalBERT cosine similarity between text and a GARD term.

    The original script loaded ClinicalBERT inside every call. This version
    loads the tokenizer/model once and reuses them for the full single-process
    run.
    """

    import torch
    from sentence_transformers import util

    tokenizer, model = get_clinicalbert_components()
    input_tokens = tokenizer(input_text, return_tensors="pt", padding=True, truncation=True, max_length=512)
    term_tokens = tokenizer(target_term, return_tensors="pt", padding=True, truncation=True, max_length=512)

    with torch.no_grad():
        input_embedding = model(**input_tokens).last_hidden_state.mean(dim=1)
        term_embedding = model(**term_tokens).last_hidden_state.mean(dim=1)

    similarity = util.pytorch_cos_sim(input_embedding, term_embedding)
    return similarity.item()


def semantic_similarity_with_fallback(input_text: str, target_term: str) -> float:
    """
    Retry ClinicalBERT similarity with shorter text slices.

    The original code used nested try/except blocks with 2000, 1500, and 500
    character fallbacks. The sequence is preserved here in a compact loop.
    """

    fallback_limits = (
        (None, None),
        (2000, 2000),
        (1500, 1500),
        (500, 1000),
    )
    last_error: Optional[Exception] = None

    for text_limit, term_limit in fallback_limits:
        try:
            text_value = input_text if text_limit is None else input_text[:text_limit]
            term_value = target_term if term_limit is None else target_term[:term_limit]
            return semantic_similarity(text_value, term_value)

        except Exception as exc:
            last_error = exc

    print(f"ClinicalBERT similarity failed after fallbacks: {last_error}")
    return 0.0


def normalize_score(value: float) -> float:
    """Normalize confidence score with the original log-base-20 formula."""

    if value < 20:
        return math.log(value) / math.log(20)

    return 1


def normalize_combined_dictionary(input_text: str, dict1: Dict[str, int], dict2: Dict[str, int], dict3: Dict[str, int], dict4: Dict[str, int], source_type: str) -> Dict[str, List[float]]:
    """Combine weighted match dictionaries and add ClinicalBERT similarity."""

    if source_type == "title":
        factor = 20
    elif source_type == "statement":
        factor = 2
    else:
        factor = 1

    # Original relative weights preserved:
    # - first sentence matches x5
    # - priority/goal sentence matches x7
    # - future-positive sentence matches x3
    # - present-positive sentence matches x1
    weighted_dict1 = {key: value * 5 for key, value in dict1.items()}
    weighted_dict2 = {key: value * 7 for key, value in dict2.items()}
    weighted_dict3 = {key: value * 3 for key, value in dict3.items()}

    combined_dict = {
        key: weighted_dict1.get(key, 0) + weighted_dict2.get(key, 0) + weighted_dict3.get(key, 0) + dict4.get(key, 0)
        for key in set(weighted_dict1) | set(weighted_dict2) | set(weighted_dict3) | set(dict4)
    }

    if sum(combined_dict.values()) == 0:
        return {}

    result_dict: Dict[str, List[float]] = {}

    for gard_name, value in combined_dict.items():
        # Original note preserved:
        # No SourceDescription in the new code, just let defin = key.
        definition = gard_name
        score = normalize_score(20 if source_type == "title" else 1 + (factor * value // 2))
        result_dict[gard_name] = [
            score,
            semantic_similarity_with_fallback(input_text.lower(), definition),
        ]

    return result_dict


def get_verb_tense(verb) -> str:
    """Determine the coarse tense category for one spaCy verb token."""

    if "VBD" in verb.tag_:
        return "past"

    if ("MD" in verb.tag_ and "will" in verb.lemma_.lower()) or "aim" in verb.lemma_.lower():
        return "future"

    if "VBP" in verb.tag_ or "VBZ" in verb.tag_:
        return "present"

    return "unknown"


def is_sentence_negated(sentence) -> bool:
    """Return True when spaCy marks a sentence token as negation."""

    for token in sentence:
        if token.dep_ == "neg":
            return True

    return False


def check_sentence_priority(text: str) -> Tuple[str, str, str, str]:
    """
    Split text into the same priority buckets used by the original script.

    Returns:
        first_sentence, priority_goal_sentences, future_positive_sentences,
        present_positive_sentences
    """

    nlp = get_spacy_model()
    doc = nlp(text)
    first_sentence = ""
    priority = ""
    future_positive = ""
    present_positive = ""

    for index, sentence in enumerate(doc.sents, start=1):
        sentence_tenses = set()

        for token in sentence:
            if token.pos_ in {"VERB", "AUX"}:
                sentence_tenses.add(get_verb_tense(token))

        if is_sentence_negated(sentence) or "past" in sentence_tenses:
            continue

        sentence_text = sentence.text

        if index == 1:
            first_sentence = sentence_text
        elif "the goal of" in sentence_text.lower() or "aim" in sentence_text.lower():
            priority += sentence_text
        elif "future" in sentence_tenses:
            future_positive += sentence_text
        elif "present" in sentence_tenses:
            present_positive += sentence_text

    return first_sentence, priority, future_positive, present_positive


def sum_and_update(target_dict: Dict[str, int], source_dict: Dict[str, List[int]]) -> None:
    """Add source match counts into target_dict."""

    for key, value in source_dict.items():
        target_dict[key] = target_dict.get(key, 0) + sum(value)


def combine_dictionaries_count(dict1: Dict[str, List[int]], dict2: Dict[str, List[int]]) -> Dict[str, int]:
    """Combine exact and stemmed match count dictionaries."""

    combined_dict: Dict[str, int] = {}
    sum_and_update(combined_dict, dict1)
    sum_and_update(combined_dict, dict2)
    return combined_dict


def remove_shorter_contained_keys(combined_dict: Dict[str, int]) -> Dict[str, int]:
    """Remove disease names that are substrings of stronger/longer matches."""

    keys_to_remove = set()

    for key1 in combined_dict:
        for key2 in combined_dict:
            if key1 != key2 and key1 in key2 and combined_dict[key1] <= combined_dict[key2]:
                keys_to_remove.add(key1)

    for key in keys_to_remove:
        del combined_dict[key]

    return combined_dict


def get_gard_abstract_stem_exact(text: str) -> Dict[str, int]:
    """Find abstract/statement matches using exact and stemmed GARD terms."""

    if not text or not isinstance(text, str):
        return {}

    exact_matching = get_gard_terms_for_text(text, "Synonyms_sw") or {}
    stemming_check = get_gard_terms_for_text(text, "Synonyms_sw_stem") or {}
    combined_dict = combine_dictionaries_count(exact_matching, stemming_check)

    if not combined_dict:
        return {}

    return remove_shorter_contained_keys(combined_dict)


def get_gard_id_by_name(gard_name: str) -> Optional[str]:
    """Return the first GARD ID for a disease name, matching historical behavior."""

    ensure_gard_terms_loaded()
    return GARD_ID_BY_NAME.get(gard_name)


def normalize_text_value(value: Any) -> str:
    """Convert a nullable DB text value to a stripped string."""

    if value is None:
        return ""

    return str(value).strip()


def process_text_and_normalize(text: str, source_type: str) -> Optional[Dict[str, List[float]]]:
    """Process statement/abstract text and return normalized GARD matches."""

    if not text or text.isspace():
        return None

    first_sentence, priority, future_positive, present_positive = check_sentence_priority(text)
    name1 = get_gard_abstract_stem_exact(first_sentence)
    name2 = get_gard_abstract_stem_exact(priority)
    name3 = get_gard_abstract_stem_exact(future_positive)
    name4 = get_gard_abstract_stem_exact(present_positive)
    result_dict = normalize_combined_dictionary(text, name1, name2, name3, name4, source_type)

    return result_dict or None


def project_gard_relationship(project_title: Any, public_health_relevance_statement: Any, abstract_text: Any) -> Tuple[Optional[Dict[str, List[float]]], str]:
    """Find the highest-priority GARD relationship source for one grant project."""

    title = normalize_text_value(project_title)
    phr = normalize_text_value(public_health_relevance_statement)
    abstract = normalize_text_value(abstract_text)

    # The original code returned early when all three values were strings,
    # which skipped normal rows. The intended guard is to skip rows with no text.
    if not any((title, phr, abstract)):
        return None, ""

    # 1. Processing project_title
    if title:
        name_dict = get_gard_title_stem_exact(title)

        if name_dict:
            similarity_text = abstract or title
            return normalize_combined_dictionary(similarity_text, name_dict, {}, {}, {}, "title"), "title"

    # 2. Processing public_health_relevance_statement
    if phr:
        result = process_text_and_normalize(phr, "statement")

        if result:
            return result, "statement"

    # 3. Processing abstract_text
    if abstract:
        result = process_text_and_normalize(abstract, "abstract")

        if result:
            return result, "abstract"

    return None, ""


def get_pending_project_id_bounds() -> Tuple[Optional[int], Optional[int]]:
    """Return min/max grant_project IDs that still need GARD relationship checks."""

    from baseclass.conn import DBConnection as db

    mysql = db().mysql_conn()

    if mysql is None:
        raise ConnectionError("Unable to create MySQL connection.")

    cursor = None

    try:
        cursor = mysql.cursor(dictionary=True)
        cursor.execute(PROJECT_ID_BOUNDS_SQL)
        row = cursor.fetchone() or {}
        return row.get("min_id"), row.get("max_id")

    finally:
        if cursor is not None:
            cursor.close()

        if mysql is not None and mysql.is_connected():
            mysql.close()


def flush_relationships(mysql, insert_cursor, insert_values: List[Tuple[Any, ...]]) -> int:
    """Insert one relationship batch and clear the caller-owned list."""

    if not insert_values:
        return 0

    try:
        insert_cursor.executemany(RELATION_INSERT_SQL, insert_values)
        mysql.commit()
        inserted_count = len(insert_values)
        insert_values.clear()
        return inserted_count

    except Exception:
        mysql.rollback()
        raise


def build_relationship_rows(project_row: Dict[str, Any], result_dict: Dict[str, List[float]], source_type: str) -> List[Tuple[Any, ...]]:
    """Convert one project result dictionary into insert-ready tuples."""

    from utils.tools import _normalize_tuple, _val

    application_id = project_row["APPLICATION_ID"]
    core_project_num = _val(project_row.get("core_project_num"))
    raw_result = str(result_dict)
    relationship_rows: List[Tuple[Any, ...]] = []

    for gard_name, value in result_dict.items():
        confidence_score = value[0]
        semantic_similarity_value = value[1]
        gard_id = get_gard_id_by_name(gard_name)

        relationship_rows.append(
            _normalize_tuple(
                (
                    gard_id,
                    application_id,
                    gard_name,
                    source_type,
                    confidence_score,
                    semantic_similarity_value,
                    core_project_num,
                    raw_result,
                )
            )
        )

    return relationship_rows


def process_id_range(mysql, dict_cursor, insert_cursor, id_range: Tuple[int, int]) -> Dict[str, int]:
    """Process one project ID range in the current process."""

    ensure_gard_terms_loaded()

    start_id, end_id = id_range
    insert_values: List[Tuple[Any, ...]] = []
    summary = {
        "projects_scanned": 0,
        "projects_with_results": 0,
        "relationships_inserted": 0,
    }

    dict_cursor.execute(PROJECT_SELECT_SQL, (start_id, end_id))

    while True:
        rows = dict_cursor.fetchmany(DEFAULT_FETCH_SIZE)

        if not rows:
            break

        for row in rows:
            summary["projects_scanned"] += 1
            project_id = row["id"]
            application_id = row["APPLICATION_ID"]

            print(
                f"projects_scanned={summary['projects_scanned']}, "
                f"id={project_id}, application_id={application_id}",
                flush=True,
            )

            result_dict, source_type = project_gard_relationship(
                row.get("PROJECT_TITLE"),
                row.get("PHR"),
                row.get("ABSTRACT_TEXT"),
            )

            if not result_dict:
                continue

            summary["projects_with_results"] += 1
            print(f"\tapplication_id: {application_id}, source_type: {source_type}")
            print(f"\t{result_dict}")

            insert_values.extend(build_relationship_rows(row, result_dict, source_type))

            if len(insert_values) >= DEFAULT_INSERT_BATCH_SIZE:
                summary["relationships_inserted"] += flush_relationships(
                    mysql,
                    insert_cursor,
                    insert_values,
                )

    summary["relationships_inserted"] += flush_relationships(
        mysql,
        insert_cursor,
        insert_values,
    )

    return summary


def merge_summary(total_summary: Dict[str, int], range_summary: Dict[str, int]) -> None:
    """Add one ID-range summary into the run total."""

    for key, value in range_summary.items():
        total_summary[key] = total_summary.get(key, 0) + value


def process_pending_projects() -> Dict[str, int]:
    """Process all pending grant projects sequentially."""

    from baseclass.conn import DBConnection as db
    from utils.tools import _id_range_generator

    min_id, max_id = get_pending_project_id_bounds()

    if min_id is None or max_id is None:
        print("No pending grant projects found for GARD relationship processing.")
        return {
            "projects_scanned": 0,
            "projects_with_results": 0,
            "relationships_inserted": 0,
        }

    id_ranges = list(
        _id_range_generator(
            min_id,
            max_id,
            DEFAULT_ID_STEP,
            DEFAULT_RANGE_BATCH_SIZE,
        )
    )

    print(
        "\nSingle-process GARD-project relationship processing starting: "
        f"min_id={min_id}, max_id={max_id}, id_ranges={len(id_ranges)}\n"
    )

    mysql = db().mysql_conn()

    if mysql is None:
        raise ConnectionError("Unable to create MySQL connection.")

    dict_cursor = None
    insert_cursor = None
    total_summary = {
        "projects_scanned": 0,
        "projects_with_results": 0,
        "relationships_inserted": 0,
    }

    try:
        dict_cursor = mysql.cursor(dictionary=True)
        insert_cursor = mysql.cursor()

        for id_range in id_ranges:
            range_summary = process_id_range(mysql, dict_cursor, insert_cursor, id_range)
            merge_summary(total_summary, range_summary)
            print(f"Progress summary: {total_summary}", flush=True)

    finally:
        if dict_cursor is not None:
            dict_cursor.close()

        if insert_cursor is not None:
            insert_cursor.close()

        if mysql is not None and mysql.is_connected():
            mysql.close()

    return total_summary


def main() -> int:
    """Run the single-process GARD-project relationship pipeline."""

    from utils.tools import ask_to_continue

    if not ask_to_continue("*** Find relationships between GARD and Grant Project? *** "):
        print("\n------------------------ Stopped ------------------------\n")
        return 1

    summary = process_pending_projects()

    print(
        "\nGARD-project relationship processing complete: "
        f"projects_scanned={summary['projects_scanned']}, "
        f"projects_with_results={summary['projects_with_results']}, "
        f"relationships_inserted={summary['relationships_inserted']}\n"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

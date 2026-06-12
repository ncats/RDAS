"""
Find relationships between GARD diseases and NIH grant projects.

This alert-pipeline task is based on
`D_grant/init_9_GARD_and_Project_relationship.multi.py`.

It processes only new grant projects (`grant_project.is_new = 1`). A project is
eligible when it has a matching abstract row and does not already have rows in
`grant_gard_project_relation`.
For each eligible project, the task searches the project title, PHR,
and abstract for processed GARD disease terms, scores any matches,
and inserts the resulting GARD-project relationship rows.

To speed up processing, the task splits eligible `grant_project.id` ranges
across multiple worker processes. Each worker opens its own MySQL connection,
loads the processed GARD disease terms once, scans its assigned projects, and
writes relationship rows in batches.

Required inputs:
    `grant_project`
        Project title, PHR, fiscal year, application ID, and core project number.
    `grant_abstract`
        Abstract text joined by `APPLICATION_ID` and fiscal year.
    `grant_gard_processed_names`
        Processed GARD names produced by task_grant_9.py / init_8.
    `grant_gard_project_relation`
        Output table for generated relationships.

Matching priority:
    1. `grant_project.PROJECT_TITLE`
    2. `grant_project.PHR`
    3. `grant_abstract.ABSTRACT_TEXT`

The task preserves the initializer's matching strategy:
    - exact and stemmed GARD term matching
    - two-word bag-of-words term variants from `grant_gard_processed_names`
    - spaCy sentence priority logic for PHR and abstract text
    - ClinicalBERT semantic similarity scoring
    - batched relationship inserts

NLTK data note preserved from the initializer:
    _stem_text uses NLTK tokenization. If NLTK data is missing, install it in
    your local NLTK data directory, for example:

        cd /Users/zhaot3/nltk_data
        wget --no-check-certificate https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages/tokenizers/punkt.zip
        wget --no-check-certificate https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages/corpora/stopwords.zip
        wget --no-check-certificate https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages/corpora/english_wordnet.zip
"""

# Reference: D_grant/init_9_GARD_and_Project_relationship.multi.py

import math
import os
import re
import time
import warnings
from multiprocessing import Pool
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pipelines.pipeline_4_grant.grant_base import GrantPipelineBase
from utils.tools import _time_hms


WORD_PATTERN = re.compile(r"\b\w+\b")
GARD_TERM_SEPARATOR = "$$$"
LOG_FILE_PATH = GrantPipelineBase.PROJECT_ROOT / "logs" / "grant_GARD_Project_relation_process.log"

# PyTorch emits this warning while transformers loads/runs ClinicalBERT. This
# task does not use storage APIs directly, so keep the filter narrow and avoid
# cluttering pipeline logs with the dependency deprecation warning.
warnings.filterwarnings(
    "ignore",
    message=r"TypedStorage is deprecated\..*",
    category=UserWarning,
    module=r"torch\._utils",
)

DEFAULT_ID_STEP = 1
DEFAULT_RANGE_BATCH_SIZE = 2000
DEFAULT_FETCH_SIZE = 250
DEFAULT_INSERT_BATCH_SIZE = 100
DEFAULT_NUM_PROCESSES = min(4, os.cpu_count() or 1)

GARD_PROCESSED_NAMES: List[Dict[str, Any]] = []
GARD_ID_BY_NAME: Dict[str, Any] = {}
SPACY_MODEL = None
CLINICAL_BERT_TOKENIZER = None
CLINICAL_BERT_MODEL = None
 


PROJECT_SELECT_SQL = """
    SELECT
        p.id,
        p.APPLICATION_ID,
        p.FY,
        p.PROJECT_TITLE,
        p.PHR,
        p.core_project_num,
        a.ABSTRACT_TEXT
    FROM grant_project p
    INNER JOIN grant_abstract a
        ON p.APPLICATION_ID = a.APPLICATION_ID
        AND p.FY = a.YEAR
    LEFT JOIN grant_gard_project_relation gpr
        ON p.APPLICATION_ID = gpr.application_id
    WHERE
        p.id BETWEEN %s AND %s
        AND p.is_new = 1
        AND gpr.application_id IS NULL
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
        return []

    cursor = None

    try:
        cursor = mysql.cursor(dictionary=True)
        cursor.execute("SELECT gardid, name, synonyms, synonyms_sw, synonyms_sw_bow, synonyms_sw_stem, synonyms_sw_stem_bow FROM grant_gard_processed_names")
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


def ensure_gard_terms_loaded() -> bool:
    """Load GARD term globals once per process."""

    global GARD_PROCESSED_NAMES
    global GARD_ID_BY_NAME

    if GARD_PROCESSED_NAMES:
        return True

    GARD_PROCESSED_NAMES = load_gard_processed_names()

    GARD_ID_BY_NAME = {
        row["name"]: row["gardid"]
        for row in GARD_PROCESSED_NAMES
        if row.get("name")
    }

    return bool(GARD_PROCESSED_NAMES)


def get_spacy_model():
    """Load the spaCy sentence model once per worker process."""

    global SPACY_MODEL

    if SPACY_MODEL is None:
        import spacy

        SPACY_MODEL = spacy.load("en_core_web_sm")

    return SPACY_MODEL


def get_clinicalbert_components():
    """Load ClinicalBERT tokenizer/model once per worker process."""

    global CLINICAL_BERT_TOKENIZER
    global CLINICAL_BERT_MODEL

    if CLINICAL_BERT_TOKENIZER is None or CLINICAL_BERT_MODEL is None:
        from transformers import AutoModel, AutoTokenizer

        model_name = "emilyalsentzer/Bio_ClinicalBERT"
        hf_token = os.getenv("HUGGINGFACE_TOKEN")

        CLINICAL_BERT_TOKENIZER = AutoTokenizer.from_pretrained(model_name, token=hf_token)
        CLINICAL_BERT_MODEL = AutoModel.from_pretrained(model_name, token=hf_token)
        CLINICAL_BERT_MODEL.eval()

    return CLINICAL_BERT_TOKENIZER, CLINICAL_BERT_MODEL


def get_gard_terms_for_text(text: str, list_check: str) -> Dict[str, List[int]]:
    """Find GARD term occurrences in title/abstract text for one term column."""

    if not ensure_gard_terms_loaded():
        return {}

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
    """Calculate ClinicalBERT cosine similarity between text and a GARD term."""

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
    """Retry ClinicalBERT similarity with shorter text slices."""

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

    append_worker_log(f"ClinicalBERT similarity failed after fallbacks: {last_error}")
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
        score = normalize_score(20 if source_type == "title" else 1 + (factor * value // 2))
        result_dict[gard_name] = [
            score,
            semantic_similarity_with_fallback(input_text.lower(), gard_name),
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
    """Split text into first, priority, future-positive, and present-positive buckets."""

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

    if not ensure_gard_terms_loaded():
        return None

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

    if not any((title, phr, abstract)):
        return None, ""

    if title:
        name_dict = get_gard_title_stem_exact(title)

        if name_dict:
            similarity_text = abstract or title
            return normalize_combined_dictionary(similarity_text, name_dict, {}, {}, {}, "title"), "title"

    if phr:
        result = process_text_and_normalize(phr, "statement")

        if result:
            return result, "statement"

    if abstract:
        result = process_text_and_normalize(abstract, "abstract")

        if result:
            return result, "abstract"

    return None, ""


def append_worker_log(message: str) -> None:
    """Append worker progress to the historical process log."""

    from utils.tools import _append_to_file

    LOG_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _append_to_file(LOG_FILE_PATH, message)


def flush_relationships(mysql, insert_cursor, insert_values: List[Tuple[Any, ...]]) -> Tuple[int, int]:
    """Insert one relationship batch and clear the caller-owned list."""

    if not insert_values:
        return 0, 0

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

    try:
        #insert_cursor.executemany(RELATION_INSERT_SQL, insert_values)
        print(insert_values)
        #mysql.commit()
        inserted_count = len(insert_values)
        insert_values.clear()
        return inserted_count, 0

    except Exception as exc:
        mysql.rollback()
        append_worker_log(f"Relationship insert batch failed: {exc}")
        insert_values.clear()
        return 0, 1


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


def process_id_range(worker_args: Tuple[int, int, int, int]) -> Dict[str, int]:
    """Process one project ID range in a worker process."""

    from baseclass.conn import DBConnection as db

    start_id, end_id, fetch_size, insert_batch_size = worker_args
    mysql = None
    dict_cursor = None
    insert_cursor = None
    insert_values: List[Tuple[Any, ...]] = []
    summary = {
        "projects_scanned": 0,
        "projects_with_results": 0,
        "projects_failed": 0,
        "relationships_inserted": 0,
        "relationship_insert_failed_batches": 0,
        "failed_ranges": 0,
    }

    try:
        if not ensure_gard_terms_loaded():
            summary["failed_ranges"] += 1
            append_worker_log(f"[{start_id}-{end_id}]: no processed GARD terms found.")
            return summary

        mysql = db().mysql_conn()

        if mysql is None:
            summary["failed_ranges"] += 1
            append_worker_log(f"[{start_id}-{end_id}]: unable to create MySQL connection.")
            return summary

        dict_cursor = mysql.cursor(dictionary=True)
        insert_cursor = mysql.cursor()
        dict_cursor.execute(PROJECT_SELECT_SQL, (start_id, end_id))

        while True:
            rows = dict_cursor.fetchmany(fetch_size)

            if not rows:
                break

            for row in rows:
                summary["projects_scanned"] += 1
                project_id = row["id"]
                application_id = row["APPLICATION_ID"]
                message = f"[{start_id}-{end_id}]: id={project_id}, application_id={application_id}"
                append_worker_log(message)

                try:
                    result_dict, source_type = project_gard_relationship(
                        row.get("PROJECT_TITLE"),
                        row.get("PHR"),
                        row.get("ABSTRACT_TEXT"),
                    )

                except Exception as exc:
                    summary["projects_failed"] += 1
                    append_worker_log(f"{message}; relationship matching failed: {exc}")
                    continue

                if not result_dict:
                    continue

                summary["projects_with_results"] += 1
                append_worker_log(f"{message}; source_type={source_type}; result={result_dict}")
                insert_values.extend(build_relationship_rows(row, result_dict, source_type))

                if len(insert_values) >= insert_batch_size:
                    inserted_count, failed_batches = flush_relationships(mysql, insert_cursor, insert_values)
                    summary["relationships_inserted"] += inserted_count
                    summary["relationship_insert_failed_batches"] += failed_batches

        inserted_count, failed_batches = flush_relationships(mysql, insert_cursor, insert_values)
        summary["relationships_inserted"] += inserted_count
        summary["relationship_insert_failed_batches"] += failed_batches
        return summary

    except Exception as exc:
        summary["failed_ranges"] += 1
        append_worker_log(f"[{start_id}-{end_id}]: range processing failed: {exc}")
        return summary

    finally:
        if dict_cursor is not None:
            dict_cursor.close()

        if insert_cursor is not None:
            insert_cursor.close()

        if mysql is not None and mysql.is_connected():
            mysql.close()


def merge_summary(total_summary: Dict[str, int], range_summary: Dict[str, int]) -> None:
    """Add one worker summary into the parent process total."""

    for key, value in range_summary.items():
        total_summary[key] = total_summary.get(key, 0) + value


class GrantGardProjectRelationshipTask(GrantPipelineBase):
    """Find and insert GARD disease relationships for pending grant projects."""

    def __init__(self, id_step: int = DEFAULT_ID_STEP, range_batch_size: int = DEFAULT_RANGE_BATCH_SIZE, fetch_size: int = DEFAULT_FETCH_SIZE, insert_batch_size: int = DEFAULT_INSERT_BATCH_SIZE, num_processes: int = DEFAULT_NUM_PROCESSES):

        super().__init__(init_mysql=True, init_memgraph=False)
        self.id_step = id_step
        self.range_batch_size = range_batch_size
        self.fetch_size = fetch_size
        self.insert_batch_size = insert_batch_size
        self.num_processes = num_processes


    def find_new_data(self, gard_node) -> None:
        self.logger.info("GrantGardProjectRelationshipTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Run multiprocessing GARD/project relationship matching."""

        from utils.tools import _id_range_generator

        start_time = time.time()
        total_summary = {
            "projects_scanned": 0,
            "projects_with_results": 0,
            "projects_failed": 0,
            "relationships_inserted": 0,
            "relationship_insert_failed_batches": 0,
            "failed_ranges": 0,
        }

        try: 
            LOG_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)

            min_id, max_id = self._get_pending_project_id_bounds()

            if min_id is None or max_id is None:
                self.logger.info("No pending grant projects found for GARD relationship processing.")
                return

            id_ranges = list(
                _id_range_generator(
                    min_id,
                    max_id,
                    self.id_step,
                    self.range_batch_size,
                )
            )
            worker_args = [
                (start_id, end_id, self.fetch_size, self.insert_batch_size)
                for start_id, end_id in id_ranges
            ]

            self.logger.info(
                "GARD-project relationship processing starting: "
                f"min_id={min_id}, max_id={max_id}, id_ranges={len(worker_args)}, "
                f"processes={self.num_processes}"
            )

            if self.num_processes == 1:
                for args in worker_args:
                    merge_summary(total_summary, process_id_range(args))
                    self.logger.info(f"Progress summary: {total_summary}")

            else:
                with Pool(processes=self.num_processes) as pool:
                    for range_summary in pool.imap_unordered(process_id_range, worker_args, chunksize=1):
                        merge_summary(total_summary, range_summary)
                        self.logger.info(f"Progress summary: {total_summary}")

            self.logger.info(f"Completed GARD-project relationship processing. Summary={total_summary}")

        except Exception:
            self.logger.exception(f"GrantGardProjectRelationshipTask failed. Summary={total_summary}")
            return

        finally:
            hours, minutes, seconds = _time_hms(time.time() - start_time)
            self.logger.info(f"Total time elapsed: {hours} hours, {minutes} minutes, {seconds} seconds")
            self.close()


    
    def _get_pending_project_id_bounds(self) -> Tuple[Optional[int], Optional[int]]:
        """Return min/max grant_project IDs that still need GARD relationship checks."""

        PROJECT_ID_BOUNDS_SQL = """
            SELECT
                MIN(p.id) AS min_id,
                MAX(p.id) AS max_id
            FROM grant_project p
            INNER JOIN grant_abstract a
                ON p.APPLICATION_ID = a.APPLICATION_ID
                AND p.FY = a.YEAR
            LEFT JOIN grant_gard_project_relation gpr
                ON p.APPLICATION_ID = gpr.application_id
            WHERE
                p.is_new = 1
                AND gpr.application_id IS NULL
        """
        cursor = None

        try:
            cursor = self.mysql.cursor(dictionary=True)
            cursor.execute(PROJECT_ID_BOUNDS_SQL)
            row = cursor.fetchone() or {}
            return row.get("min_id"), row.get("max_id")

        finally:
            if cursor is not None:
                cursor.close()
 

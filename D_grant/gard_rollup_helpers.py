"""
Shared GARD disease roll-up helpers for the legacy D_grant initializer scripts.

The direct grant matcher removes shorter disease names when they are contained
inside longer matched disease names. That keeps one matcher result concise, but
it also means a project matched to a specific disease such as "adult
glioblastoma" can miss the base disease relationship to "glioblastoma".

These helpers infer those base-disease rows before insert time. The rule is
intentionally phrase based:
    - exact processed source terms can roll up to a GARD primary name
    - contiguous whole-word phrases in a source primary name can roll up to a
      shorter GARD primary name
    - exclusion labels such as "astrocytoma excluding glioblastoma" are ignored
"""

import json
import re
from typing import Any, Dict, Iterable, List, Set, Tuple

from utils.tools import _normalize_tuple, _normalize_txt


GARD_TERM_SEPARATOR = "$$$"
NORMALIZED_WORD_PATTERN = re.compile(r"[a-z0-9]+")
ROLLUP_SOURCE_TYPE_PREFIX = "rollup"
MAX_SOURCE_TYPE_LENGTH = 45
MAX_RAW_RESULT_LENGTH = 4000
MIN_ROLLUP_BASE_NAME_CHARS = 5
EXCLUSION_CONTEXT_WORDS = {"exclude", "excluded", "excluding", "except", "without", "non", "not"}
EXCLUSION_CONTEXT_WINDOW = 3


def normalize_gard_rollup_phrase(value: Any) -> str:

    """Normalize a GARD disease phrase to lowercase ASCII whole-word text."""

    if value is None:
        return ""

    normalized_value = _normalize_txt(str(value)).lower()
    words = NORMALIZED_WORD_PATTERN.findall(normalized_value)
    return " ".join(words)


def iter_normalized_gard_terms(value: Any) -> Iterable[str]:

    """Yield normalized processed GARD terms from either a list or $$$ string."""

    if not value:
        return

    if isinstance(value, (list, tuple, set)):
        raw_terms = value

    else:
        raw_terms = str(value).split(GARD_TERM_SEPARATOR)

    for raw_term in raw_terms:
        term = normalize_gard_rollup_phrase(raw_term)

        if term:
            yield term


def generate_contiguous_phrases(words: Tuple[str, ...]) -> Iterable[str]:

    """Yield each contiguous whole-word phrase from a normalized disease name."""

    for start_index in range(len(words)):
        for end_index in range(start_index + 1, len(words) + 1):
            yield " ".join(words[start_index:end_index])


def has_exclusion_context(source_words: Tuple[str, ...], target_words: Tuple[str, ...]) -> bool:

    """Return True when a target phrase appears in an excluding/without context."""

    if not source_words or not target_words:
        return False

    target_length = len(target_words)

    for start_index in range(0, len(source_words) - target_length + 1):
        if source_words[start_index:start_index + target_length] != target_words:
            continue

        """
        Disease names such as "astrocytoma (excluding glioblastoma)" contain a
        target disease phrase textually, but they should not become roll-up
        relationships to that excluded disease. A short left-context window
        catches those negative labels without blocking normal modifier/base
        disease names such as "adult glioblastoma".
        """
        context_start_index = max(0, start_index - EXCLUSION_CONTEXT_WINDOW)
        context_words = set(source_words[context_start_index:start_index])

        if context_words & EXCLUSION_CONTEXT_WORDS:
            return True

    return False


def is_valid_rollup_target(source_gard: Dict[str, Any], target_gard: Dict[str, Any]) -> bool:

    """Return True when target_gard is a shorter/base disease for source_gard."""

    if source_gard["gard_id"] == target_gard["gard_id"]:
        return False

    if source_gard["normalized_name"] == target_gard["normalized_name"]:
        return False

    if len(target_gard["normalized_name"]) < MIN_ROLLUP_BASE_NAME_CHARS:
        return False

    return len(target_gard["words"]) < len(source_gard["words"])


def add_rollup_targets_for_phrase(pairs_by_source_name: Dict[str, Dict[Tuple[str, str], Dict[str, str]]], names_by_phrase: Dict[str, List[Dict[str, Any]]], source_gard: Dict[str, Any], phrase: str, rollup_rule: str) -> None:

    """Add all base-disease targets whose primary name equals this source phrase."""

    if not phrase:
        return

    for target_gard in names_by_phrase.get(phrase, []):
        if not is_valid_rollup_target(source_gard, target_gard):
            continue

        if has_exclusion_context(source_gard["words"], target_gard["words"]):
            continue

        source_pairs = pairs_by_source_name.setdefault(source_gard["name"], {})
        pair_key = (source_gard["gard_id"], target_gard["gard_id"])
        current_pair = source_pairs.get(pair_key)

        if current_pair and current_pair["rollup_rule"] == "target_name_in_source_terms":
            continue

        source_pairs[pair_key] = {
            "source_gard_id": source_gard["gard_id"],
            "source_gard_name": source_gard["name"],
            "target_gard_id": target_gard["gard_id"],
            "target_gard_name": target_gard["name"],
            "rollup_rule": rollup_rule,
        }


def build_gard_rollup_targets(gard_rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, str]]]:

    """Build source-name to base-disease roll-up targets from processed GARD rows."""

    normalized_gards: List[Dict[str, Any]] = []
    names_by_phrase: Dict[str, List[Dict[str, Any]]] = {}
    seen_rows: Set[Tuple[str, str]] = set()

    for row in gard_rows:
        gard_id = str(row.get("gardid") or "").strip()
        gard_name = str(row.get("name") or "").strip()
        normalized_name = normalize_gard_rollup_phrase(gard_name)

        if not gard_id or not gard_name or not normalized_name:
            continue

        row_key = (gard_id, normalized_name)

        if row_key in seen_rows:
            continue

        seen_rows.add(row_key)
        source_terms = set(iter_normalized_gard_terms(row.get("synonyms_sw")))
        source_terms.add(normalized_name)
        normalized_gard = {
            "gard_id": gard_id,
            "name": gard_name,
            "normalized_name": normalized_name,
            "words": tuple(normalized_name.split()),
            "source_terms": source_terms,
        }
        normalized_gards.append(normalized_gard)
        names_by_phrase.setdefault(normalized_name, []).append(normalized_gard)

    pairs_by_source_name: Dict[str, Dict[Tuple[str, str], Dict[str, str]]] = {}

    for source_gard in normalized_gards:
        """
        If a specific disease has a base disease's primary name as one exact
        processed search term, future project matches should create both the
        specific and base relationships.
        """
        for source_term in source_gard["source_terms"]:
            add_rollup_targets_for_phrase(
                pairs_by_source_name,
                names_by_phrase,
                source_gard,
                source_term,
                "target_name_in_source_terms",
            )

        """
        Primary-name containment catches modifier/base names even when the
        source synonym list is incomplete. Only contiguous whole-word phrases
        are considered, so partial-word matches do not create roll-up rules.
        """
        for source_name_phrase in generate_contiguous_phrases(source_gard["words"]):
            add_rollup_targets_for_phrase(
                pairs_by_source_name,
                names_by_phrase,
                source_gard,
                source_name_phrase,
                "source_name_contains_target_name",
            )

    return {
        source_name: sorted(pairs.values(), key=lambda pair: (pair["target_gard_id"], pair["source_gard_id"]))
        for source_name, pairs in pairs_by_source_name.items()
    }


def build_relationship_tuple(gard_id: Any, application_id: Any, gard_name: Any, source_type: Any, confidence_score: Any, semantic_similarity_value: Any, core_project_num: Any, raw_result: Any) -> Tuple[Any, ...]:

    """Build one normalized grant_gard_project_relation insert tuple."""

    return _normalize_tuple(
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


def build_rollup_source_type(source_type: Any) -> str:

    """Create a source_type value that marks an inferred base-disease row."""

    source_type_value = str(source_type or "").strip()

    if not source_type_value:
        return ROLLUP_SOURCE_TYPE_PREFIX

    return f"{ROLLUP_SOURCE_TYPE_PREFIX}:{source_type_value}"[:MAX_SOURCE_TYPE_LENGTH]


def build_rollup_raw_result(source_gard_id: Any, source_gard_name: Any, target_gard_id: Any, target_gard_name: Any, rollup_rule: Any) -> str:

    """Build compact trace metadata for an inferred roll-up relationship row."""

    raw_result = {
        "rollup_rule": rollup_rule,
        "source_gard_id": source_gard_id,
        "source_gard_name": source_gard_name,
        "target_gard_id": target_gard_id,
        "target_gard_name": target_gard_name,
    }
    return json.dumps(raw_result, sort_keys=True, default=str)[:MAX_RAW_RESULT_LENGTH]

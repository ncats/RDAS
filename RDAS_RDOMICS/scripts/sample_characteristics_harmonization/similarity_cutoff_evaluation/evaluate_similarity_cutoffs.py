#!/usr/bin/env python3
"""
Evaluate similarity-assignment cutoffs for sample-characteristic labels.

This script reproduces the deterministic rule-based stage and the
similarity-based stage from 4_llm_clustering_enhanced.py, then evaluates
candidate similarity cutoffs such as 0.70, 0.75, and 0.80.

Outputs:
  - best_similarity_matches.csv:
      Best reference match for every label remaining after rule matching.
  - assignments_cutoff_<cutoff>.csv:
      Labels assigned at each similarity cutoff.
  - cutoff_assignment_counts.csv:
      Number of labels and occurrences assigned at each cutoff.
  - manual_review_template.csv:
      A sampled review sheet with a blank manual_correct column.
  - cutoff_manual_accuracy.csv:
      Written only when a completed review file is provided.

Manual review workflow:
  1. Run this script to generate outputs.
  2. Fill manual_review_template.csv column manual_correct with 1/0,
     yes/no, true/false, correct/incorrect, or y/n.
  3. Rerun this script with --review-file path/to/completed_review.csv.
"""

from __future__ import annotations

import argparse
import math
import re
from collections import defaultdict
from difflib import SequenceMatcher
from pathlib import Path
from typing import Iterable

import pandas as pd


DEFAULT_CUTOFFS = [0.70, 0.75, 0.80]


# Copied from 4_llm_clustering_enhanced.py so this evaluation can run without
# importing the vLLM-dependent clustering script.
KEYWORD_MAPPINGS = {
    "External_sample_id": {
        "exact_matches": [
            "patient_id",
            "sample_id",
            "donor_id",
            "subject_id",
            "patient id",
            "sample id",
            "donor id",
            "subject id",
        ],
        "keywords": [
            "patient",
            "id",
            "sample",
            "donor",
            "subject",
            "barcode",
            "participant",
            "individual",
            "controlid",
            "patientid",
            "subjectid",
            "sampleid",
        ],
        "patterns": [r".*id$", r".*_id$", r"patient.*", r"subject.*", r"donor.*"],
    },
    "Biospecimen Organism": {
        "exact_matches": ["species", "organism", "strain"],
        "keywords": [
            "mouse",
            "human",
            "homo sapiens",
            "mus musculus",
            "host",
            "background strain",
            "mouse strain",
        ],
        "patterns": [r".*strain.*", r".*organism.*"],
    },
    "Biospecimen Type": {
        "exact_matches": [
            "tissue",
            "cell",
            "blood",
            "serum",
            "organ",
            "cell type",
            "celltype",
            "tissue type",
        ],
        "keywords": [
            "lymphocyte",
            "monocyte",
            "macrophage",
            "stem cell",
            "biopsy",
            "specimen",
            "sample type",
            "fluid",
            "brain tissue",
            "liver tissue",
        ],
        "patterns": [r".*tissue.*", r".*cell.*", r".*blood.*"],
    },
    "Biospecimen Age": {
        "exact_matches": ["age", "day", "week", "month", "year", "time"],
        "keywords": [
            "developmental stage",
            "passage",
            "age in",
            "donor age",
            "patient age",
            "time point",
            "timepoint",
        ],
        "patterns": [r".*age.*", r".*day.*", r".*time.*", r".*stage.*"],
    },
    "Biospecimen Sex": {
        "exact_matches": ["sex", "gender", "male", "female"],
        "keywords": ["donor sex", "patient gender", "cell sex", "animal sex"],
        "patterns": [r".*sex.*", r".*gender.*"],
    },
    "Biospecimen Race": {
        "exact_matches": ["race", "ethnicity"],
        "keywords": [
            "caucasian",
            "asian",
            "hispanic",
            "ethnicity",
            "donor race",
            "donor ethnicity",
        ],
        "patterns": [r".*race.*", r".*ethnicity.*", r".*ethnic.*"],
    },
    "Biospecimen Disease Condition": {
        "exact_matches": ["disease", "condition", "diagnosis", "cancer", "tumor", "infection"],
        "keywords": [
            "disease state",
            "disease status",
            "clinical diagnosis",
            "pathology",
            "tumor type",
            "cancer status",
            "clinical condition",
        ],
        "patterns": [
            r".*disease.*",
            r".*tumor.*",
            r".*cancer.*",
            r".*diagnosis.*",
            r".*condition.*",
        ],
    },
    "Treatment": {
        "exact_matches": [
            "treatment",
            "drug",
            "therapy",
            "medication",
            "group",
            "control",
            "placebo",
        ],
        "keywords": [
            "drug treatment",
            "therapy",
            "intervention",
            "compound",
            "chemical",
            "vaccine",
            "antibody treatment",
            "chemotherapy",
        ],
        "patterns": [r".*treatment.*", r".*drug.*", r".*therapy.*", r".*group.*"],
    },
    "Treatment Dosage Regimen": {
        "exact_matches": ["dose", "dosage", "concentration", "duration"],
        "keywords": [
            "treatment dose",
            "drug dose",
            "treatment duration",
            "protocol",
            "regimen",
            "drug concentration",
        ],
        "patterns": [r".*dose.*", r".*dosage.*", r".*concentration.*", r".*duration.*"],
    },
}


def parse_cutoffs(raw_cutoffs: Iterable[str]) -> list[float]:
    cutoffs = [float(value) for value in raw_cutoffs]
    invalid = [value for value in cutoffs if value <= 0 or value > 1]
    if invalid:
        raise ValueError(f"Cutoffs must be in (0, 1], got: {invalid}")
    return sorted(cutoffs)


def cutoff_label(cutoff: float) -> str:
    return f"{cutoff:.2f}".replace(".", "_")


def load_labels(labels_file: Path) -> list[tuple[str, int]]:
    df = pd.read_csv(labels_file)
    required_columns = {"name", "count"}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"{labels_file} is missing columns: {sorted(missing_columns)}")

    return [(str(row["name"]), int(row["count"])) for _, row in df.iterrows()]


def keyword_based_categorization(
    labels_with_counts: list[tuple[str, int]],
) -> tuple[dict[str, list[dict]], list[tuple[str, int]]]:
    """Reproduce the rule-based categorization from the clustering script."""
    keyword_categorizations = defaultdict(list)
    remaining_labels = []

    for label, count in labels_with_counts:
        label_lower = label.lower().strip()
        best_category = None
        confidence = "LOW"
        matched_term = None
        match_type = None

        for category, mapping in KEYWORD_MAPPINGS.items():
            for exact_match in mapping["exact_matches"]:
                exact_lower = exact_match.lower()
                if exact_lower == label_lower or exact_lower in label_lower.split():
                    best_category = category
                    confidence = "HIGH"
                    matched_term = exact_match
                    match_type = "exact_match"
                    break

            if best_category:
                break

            for keyword in mapping["keywords"]:
                if keyword.lower() in label_lower:
                    best_category = category
                    confidence = "MEDIUM"
                    matched_term = keyword
                    match_type = "keyword"
                    break

            if best_category:
                break

            for pattern in mapping["patterns"]:
                if re.search(pattern, label_lower):
                    if confidence != "MEDIUM":
                        best_category = category
                        confidence = "MEDIUM" if "id" in pattern or "age" in pattern else "LOW"
                        matched_term = pattern
                        match_type = "pattern"
                    break

        if best_category:
            keyword_categorizations[best_category].append(
                {
                    "label": label,
                    "count": count,
                    "matched_term": matched_term,
                    "match_type": match_type,
                    "confidence": confidence,
                }
            )
        else:
            remaining_labels.append((label, count))

    return dict(keyword_categorizations), remaining_labels


def calculate_similarity(text1: str, text2: str) -> float:
    return SequenceMatcher(None, text1.lower(), text2.lower()).ratio()


def build_reference_terms(keyword_results: dict[str, list[dict]]) -> dict[str, list[str]]:
    reference_terms = {}
    for category, results in keyword_results.items():
        reference_terms[category] = [result["label"].lower() for result in results]
    return reference_terms


def find_best_similarity_matches(
    remaining_labels: list[tuple[str, int]],
    reference_terms: dict[str, list[str]],
) -> pd.DataFrame:
    rows = []

    for label, count in remaining_labels:
        best_reference = None
        best_category = None
        best_score = 0.0

        for category, category_labels in reference_terms.items():
            for reference_label in category_labels:
                score = calculate_similarity(label, reference_label)
                if score > best_score:
                    best_reference = reference_label
                    best_category = category
                    best_score = score

        rows.append(
            {
                "label": label,
                "count": count,
                "best_reference_label": best_reference,
                "best_similarity_score": best_score,
                "assigned_category": best_category,
            }
        )

    return pd.DataFrame(rows)


def summarize_cutoffs(best_matches: pd.DataFrame, cutoffs: list[float]) -> pd.DataFrame:
    total_remaining = len(best_matches)
    total_remaining_occurrences = int(best_matches["count"].sum()) if total_remaining else 0
    rows = []

    for cutoff in cutoffs:
        assigned = best_matches[best_matches["best_similarity_score"] >= cutoff]
        rows.append(
            {
                "cutoff": f"{cutoff:.2f}",
                "labels_assigned": len(assigned),
                "label_assignment_rate_among_remaining": (
                    len(assigned) / total_remaining if total_remaining else math.nan
                ),
                "occurrences_assigned": int(assigned["count"].sum()) if len(assigned) else 0,
                "occurrence_assignment_rate_among_remaining": (
                    assigned["count"].sum() / total_remaining_occurrences
                    if total_remaining_occurrences
                    else math.nan
                ),
            }
        )

    return pd.DataFrame(rows)


def add_cutoff_membership_columns(best_matches: pd.DataFrame, cutoffs: list[float]) -> pd.DataFrame:
    output = best_matches.copy()
    for cutoff in cutoffs:
        output[f"assigned_at_{cutoff_label(cutoff)}"] = (
            output["best_similarity_score"] >= cutoff
        )
    return output


def score_band(score: float, cutoffs: list[float]) -> str:
    min_cutoff = min(cutoffs)
    middle_cutoffs = sorted(cutoffs)

    if score < min_cutoff:
        return f"<{min_cutoff:.2f}"

    for left, right in zip(middle_cutoffs, middle_cutoffs[1:]):
        if left <= score < right:
            return f"{left:.2f}_to_lt_{right:.2f}"

    return f">={max(cutoffs):.2f}"


def create_review_template(
    best_matches: pd.DataFrame,
    cutoffs: list[float],
    sample_per_band: int,
    random_seed: int,
) -> pd.DataFrame:
    eligible = best_matches[best_matches["best_similarity_score"] >= min(cutoffs)].copy()
    if eligible.empty:
        return eligible

    eligible["score_band"] = eligible["best_similarity_score"].apply(
        lambda value: score_band(float(value), cutoffs)
    )
    eligible["eligible_cutoffs"] = eligible["best_similarity_score"].apply(
        lambda value: ";".join(f"{cutoff:.2f}" for cutoff in cutoffs if value >= cutoff)
    )

    sampled_groups = []
    for _, group in eligible.groupby("score_band", sort=True):
        sample_n = min(sample_per_band, len(group))
        sampled_groups.append(
            group.sample(n=sample_n, random_state=random_seed).sort_values(
                ["best_similarity_score", "count"], ascending=[False, False]
            )
        )

    review_df = pd.concat(sampled_groups, ignore_index=True)
    review_df = review_df.sort_values(
        ["score_band", "best_similarity_score", "count"],
        ascending=[True, False, False],
    )
    review_df.insert(0, "review_id", range(1, len(review_df) + 1))
    review_df["manual_correct"] = ""
    review_df["manual_notes"] = ""

    return review_df[
        [
            "review_id",
            "score_band",
            "eligible_cutoffs",
            "label",
            "count",
            "best_reference_label",
            "best_similarity_score",
            "assigned_category",
            "manual_correct",
            "manual_notes",
        ]
    ]


def parse_manual_correct(value) -> bool | None:
    if pd.isna(value):
        return None

    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "t", "yes", "y", "correct", "c"}:
        return True
    if normalized in {"0", "false", "f", "no", "n", "incorrect", "wrong", "w"}:
        return False
    if normalized == "":
        return None

    raise ValueError(
        "manual_correct must be one of 1/0, yes/no, true/false, "
        f"correct/incorrect, or blank. Got: {value!r}"
    )


def summarize_manual_accuracy(review_file: Path, cutoffs: list[float]) -> pd.DataFrame:
    review_df = pd.read_csv(review_file)
    required_columns = {"best_similarity_score", "manual_correct"}
    missing_columns = required_columns - set(review_df.columns)
    if missing_columns:
        raise ValueError(f"{review_file} is missing columns: {sorted(missing_columns)}")

    review_df["manual_correct_bool"] = review_df["manual_correct"].apply(parse_manual_correct)
    reviewed = review_df[review_df["manual_correct_bool"].notna()].copy()

    rows = []
    for cutoff in cutoffs:
        reviewed_at_cutoff = reviewed[reviewed["best_similarity_score"] >= cutoff]
        reviewed_n = len(reviewed_at_cutoff)
        correct_n = int(reviewed_at_cutoff["manual_correct_bool"].sum()) if reviewed_n else 0
        rows.append(
            {
                "cutoff": f"{cutoff:.2f}",
                "reviewed_labels": reviewed_n,
                "reviewed_correct": correct_n,
                "manual_accuracy": correct_n / reviewed_n if reviewed_n else math.nan,
            }
        )

    return pd.DataFrame(rows)


def write_cutoff_assignment_files(
    best_matches: pd.DataFrame,
    cutoffs: list[float],
    output_dir: Path,
) -> None:
    for cutoff in cutoffs:
        assigned = best_matches[best_matches["best_similarity_score"] >= cutoff].copy()
        assigned = assigned.sort_values(
            ["count", "best_similarity_score"], ascending=[False, False]
        )
        assigned.to_csv(output_dir / f"assignments_cutoff_{cutoff_label(cutoff)}.csv", index=False)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Evaluate candidate cutoffs for similarity-based label assignment."
    )
    script_dir = Path(__file__).resolve().parent
    parser.add_argument(
        "--labels-file",
        type=Path,
        default=script_dir / "3_sample_characteristics_key_count_english_only.csv",
        help="CSV with columns name,count.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=script_dir / "similarity_cutoff_evaluation",
        help="Directory for evaluation outputs.",
    )
    parser.add_argument(
        "--cutoffs",
        nargs="+",
        default=[str(value) for value in DEFAULT_CUTOFFS],
        help="Similarity cutoffs to evaluate, e.g. 0.70 0.75 0.80.",
    )
    parser.add_argument(
        "--review-file",
        type=Path,
        default=None,
        help="Completed manual review CSV with manual_correct filled in.",
    )
    parser.add_argument(
        "--sample-per-band",
        type=int,
        default=50,
        help="Number of labels to sample per score band for manual review.",
    )
    parser.add_argument(
        "--random-seed",
        type=int,
        default=13,
        help="Random seed for reproducible manual-review sampling.",
    )
    args = parser.parse_args()

    if args.sample_per_band < 1:
        raise ValueError("--sample-per-band must be positive")

    cutoffs = parse_cutoffs(args.cutoffs)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    labels_with_counts = load_labels(args.labels_file)
    keyword_results, remaining_after_keywords = keyword_based_categorization(labels_with_counts)
    reference_terms = build_reference_terms(keyword_results)
    best_matches = find_best_similarity_matches(remaining_after_keywords, reference_terms)
    best_matches = add_cutoff_membership_columns(best_matches, cutoffs)
    best_matches = best_matches.sort_values(
        ["best_similarity_score", "count"], ascending=[False, False]
    )

    best_matches.to_csv(args.output_dir / "best_similarity_matches.csv", index=False)
    write_cutoff_assignment_files(best_matches, cutoffs, args.output_dir)

    cutoff_summary = summarize_cutoffs(best_matches, cutoffs)
    cutoff_summary.to_csv(args.output_dir / "cutoff_assignment_counts.csv", index=False)

    review_template = create_review_template(
        best_matches=best_matches,
        cutoffs=cutoffs,
        sample_per_band=args.sample_per_band,
        random_seed=args.random_seed,
    )
    review_template.to_csv(args.output_dir / "manual_review_template.csv", index=False)

    print("Similarity cutoff evaluation complete.")
    print(f"Labels loaded: {len(labels_with_counts)}")
    print(f"Labels assigned by rule-based step: {sum(len(v) for v in keyword_results.values())}")
    print(f"Labels remaining for similarity evaluation: {len(remaining_after_keywords)}")
    print("\nAssignment counts by cutoff:")
    print(cutoff_summary.to_string(index=False))
    print(f"\nOutputs written to: {args.output_dir}")
    print("Fill manual_review_template.csv column manual_correct, then rerun with --review-file.")

    if args.review_file:
        manual_summary = summarize_manual_accuracy(args.review_file, cutoffs)
        manual_summary.to_csv(args.output_dir / "cutoff_manual_accuracy.csv", index=False)
        print("\nManual accuracy from reviewed subset:")
        print(manual_summary.to_string(index=False))
        print(f"Manual accuracy written to: {args.output_dir / 'cutoff_manual_accuracy.csv'}")


if __name__ == "__main__":
    main()

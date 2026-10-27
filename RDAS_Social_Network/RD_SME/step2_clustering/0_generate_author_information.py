"""
Generate author_information.json by aggregating background (articles, grants,
clinical trials) for each author using associate relationships.

Defaults target the GARD:0006233 dataset under step1_data_extraction.

Output schema (per author):
{
  "author_id": <id>,
  "first_name": <str>,
  "last_name": <str>,
  "author_name": <str>,
  "full_abstract": [
    {"type": "Grant"|"Article"|"ClinicalTrial", "abstract_id": <str>, "title": <str>, "abstract": <str>},
    ...
  ],
  "GardId": ["GARD:0006233", ...]
}
"""

import argparse
import json
import os
from collections import defaultdict
from typing import Any, Dict, List, Tuple


def load_json_list(file_path: str) -> List[Dict[str, Any]]:
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"Expected a list in {file_path}")
    return data


def build_index(records: List[Dict[str, Any]], key_name: str) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for rec in records:
        key_val = rec.get(key_name)
        if key_val is None:
            continue
        index[str(key_val)] = rec
    return index


def build_author_to_associates(associations: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    mapping: Dict[str, List[str]] = defaultdict(list)
    for rel in associations:
        author_id = rel.get("author_id")
        associate_id = rel.get("associate_id")
        if author_id is None or associate_id is None:
            continue
        mapping[str(author_id)].append(str(associate_id))
    return mapping


def make_abstract_item(item_type: str, abstract_id: str, title: str, abstract: str) -> Dict[str, Any]:
    return {
        "type": item_type,
        "abstract_id": abstract_id,
        "title": title if title is not None else "",
        "abstract": abstract if abstract is not None else "",
    }


def aggregate_author_background(
    authors: List[Dict[str, Any]],
    author_to_associates: Dict[str, List[str]],
    articles_by_id: Dict[str, Dict[str, Any]],
    trials_by_id: Dict[str, Dict[str, Any]],
    grants_by_id: Dict[str, Dict[str, Any]],
    gard_id: str,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    for author in authors:
        author_id = author.get("author_id")
        first_name = author.get("first_name", "")
        last_name = author.get("last_name", "")
        author_name = (str(first_name).strip() + " " + str(last_name).strip()).strip()

        associates = author_to_associates.get(str(author_id), [])

        seen: set[Tuple[str, str]] = set()
        abstracts: List[Dict[str, Any]] = []

        for assoc_id in associates:
            # Article
            article = articles_by_id.get(assoc_id)
            if article is not None:
                key = ("Article", assoc_id)
                if key not in seen:
                    seen.add(key)
                    abstracts.append(
                        make_abstract_item(
                            "Article",
                            assoc_id,
                            article.get("title", ""),
                            article.get("abstract_Text", ""),
                        )
                    )
                continue

            # Clinical Trial
            trial = trials_by_id.get(assoc_id)
            if trial is not None:
                key = ("ClinicalTrial", assoc_id)
                if key not in seen:
                    seen.add(key)
                    abstracts.append(
                        make_abstract_item(
                            "ClinicalTrial",
                            assoc_id,
                            trial.get("briefTitle", ""),
                            trial.get("briefSummary", ""),
                        )
                    )
                continue

            # Grant
            grant = grants_by_id.get(assoc_id)
            if grant is not None:
                key = ("Grant", assoc_id)
                if key not in seen:
                    seen.add(key)
                    abstracts.append(
                        make_abstract_item(
                            "Grant",
                            assoc_id,
                            grant.get("title", ""),
                            grant.get("abstract", ""),
                        )
                    )

        results.append(
            {
                "author_id": author_id,
                "first_name": first_name,
                "last_name": last_name,
                "author_name": author_name,
                "full_abstract": abstracts,
                "GardId": [gard_id],
            }
        )

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate author_information.json for a GARD cohort")
    parser.add_argument(
        "--authors",
        type=str,
        default="step1_data_extraction/authors_GARD_0006233.json",
        help="Path to authors JSON file",
    )
    parser.add_argument(
        "--associations",
        type=str,
        default="step1_data_extraction/author_associate_relations_GARD_0006233.json",
        help="Path to author-associate relations JSON file",
    )
    parser.add_argument(
        "--articles",
        type=str,
        default="step1_data_extraction/articles_GARD_0006233.json",
        help="Path to articles JSON file",
    )
    parser.add_argument(
        "--trials",
        type=str,
        default="step1_data_extraction/clinical_trials_GARD_0006233.json",
        help="Path to clinical trials JSON file",
    )
    parser.add_argument(
        "--grants",
        type=str,
        default="step1_data_extraction/grants_GARD_0006233.json",
        help="Path to grants JSON file",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="step2_clustering/author_information.json",
        help="Path to write author_information.json",
    )
    parser.add_argument(
        "--gard-id",
        type=str,
        default="GARD:0006233",
        help="GARD identifier to attach to each author (as a single-item list)",
    )

    args = parser.parse_args()

    print("Loading data...")
    authors = load_json_list(args.authors)
    associations = load_json_list(args.associations)
    articles = load_json_list(args.articles)
    trials = load_json_list(args.trials)
    grants = load_json_list(args.grants)

    print(f"Loaded: {len(authors)} authors, {len(associations)} associations, {len(articles)} articles, {len(trials)} trials, {len(grants)} grants")

    author_to_associates = build_author_to_associates(associations)
    articles_by_id = build_index(articles, "pubmed_id")
    trials_by_id = build_index(trials, "nctid")
    grants_by_id = build_index(grants, "application_id")

    print("Aggregating background per author...")
    output_data = aggregate_author_background(
        authors,
        author_to_associates,
        articles_by_id,
        trials_by_id,
        grants_by_id,
        args.gard_id,
    )

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(output_data)} author records to {args.output}")


if __name__ == "__main__":
    main()

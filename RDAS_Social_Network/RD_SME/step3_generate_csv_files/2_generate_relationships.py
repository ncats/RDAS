import os
import json
import argparse
import pandas as pd
from typing import Any, Dict, List, Tuple, Set


def load_json_list(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict):
        return [data]
    if not isinstance(data, list):
        raise ValueError(f"Expected an object or list in {path}")
    return data


def detect_gard_id(gard_json_path: str, default_gard: str = "GARD:0006233") -> str:
    try:
        records = load_json_list(gard_json_path)
        if not records:
            return default_gard
        rec = records[0]
        for key in ("gard_id", "GardId", "id"):
            val = rec.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
        return default_gard
    except Exception:
        return default_gard


def write_csv(rows: List[Dict[str, Any]], output_path: str, columns: List[str]) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df = pd.DataFrame(rows, columns=columns)
    df.to_csv(output_path, index=False)
    print(f"Wrote {len(df)} rows -> {output_path}")


def build_id_sets(
    articles: List[Dict[str, Any]],
    grants: List[Dict[str, Any]],
    trials: List[Dict[str, Any]],
) -> Tuple[Set[str], Set[str], Set[str]]:
    article_ids = {str(r.get("pubmed_id")).strip() for r in articles if r.get("pubmed_id") not in (None, "", "nan")}
    grant_ids = {str(r.get("application_id")).strip() for r in grants if r.get("application_id") not in (None, "", "nan")}
    trial_ids = {str(r.get("nctid")).strip() for r in trials if r.get("nctid") not in (None, "", "nan")}
    return article_ids, grant_ids, trial_ids


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate relationship CSVs for Memgraph from step1 JSONs")
    parser.add_argument(
        "--gard-json",
        type=str,
        default="step1_data_extraction/gard_node_GARD_0006233.json",
        help="Path to GARD node JSON",
    )
    parser.add_argument(
        "--articles-json",
        type=str,
        default="step1_data_extraction/articles_GARD_0006233.json",
        help="Path to Articles JSON",
    )
    parser.add_argument(
        "--grants-json",
        type=str,
        default="step1_data_extraction/grants_GARD_0006233.json",
        help="Path to Grants JSON",
    )
    parser.add_argument(
        "--trials-json",
        type=str,
        default="step1_data_extraction/clinical_trials_GARD_0006233.json",
        help="Path to Clinical Trials JSON",
    )
    parser.add_argument(
        "--author-assoc-json",
        type=str,
        default="step1_data_extraction/author_associate_relations_GARD_0006233.json",
        help="Path to author_associate_relations JSON",
    )
    parser.add_argument(
        "--author-collab-json",
        type=str,
        default="step1_data_extraction/author_collaborations_GARD_0006233.json",
        help="Path to author_collaborations JSON (pairs of collaborating authors)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="csv_files",
        help="Directory to write relationship CSV files",
    )

    args = parser.parse_args()

    # Load data
    gard_id = detect_gard_id(args.gard_json, default_gard="GARD:0006233")
    print(f"Using GARD ID: {gard_id}")

    articles = load_json_list(args.articles_json)
    grants = load_json_list(args.grants_json)
    trials = load_json_list(args.trials_json)
    assoc = load_json_list(args.author_assoc_json)
    # Author collaborations may not exist for all runs
    try:
        collab = load_json_list(args.author_collab_json)
    except Exception:
        collab = []

    article_ids, grant_ids, trial_ids = build_id_sets(articles, grants, trials)
    print(f"Loaded IDs: articles={len(article_ids)}, grants={len(grant_ids)}, trials={len(trial_ids)}")

    # 1) GARD -> Article/Grant/Trial relationships
    gard_article_rows: List[Dict[str, Any]] = []
    for r in articles:
        pid = r.get("pubmed_id")
        if pid not in (None, "", "nan"):
            gard_article_rows.append({"gard_id": gard_id, "pubmed_id": str(pid).strip(), "relationship": "GARD_HAS_ARTICLE"})
    write_csv(
        gard_article_rows,
        os.path.join(args.output_dir, "gard_article_relationships.csv"),
        ["gard_id", "pubmed_id", "relationship"],
    )

    gard_grant_rows: List[Dict[str, Any]] = []
    for r in grants:
        gid = r.get("application_id")
        if gid not in (None, "", "nan"):
            gard_grant_rows.append({"gard_id": gard_id, "application_id": str(gid).strip(), "relationship": "GARD_HAS_GRANT"})
    write_csv(
        gard_grant_rows,
        os.path.join(args.output_dir, "gard_grant_relationships.csv"),
        ["gard_id", "application_id", "relationship"],
    )

    gard_trial_rows: List[Dict[str, Any]] = []
    for r in trials:
        tid = r.get("nctid")
        if tid not in (None, "", "nan"):
            gard_trial_rows.append({"gard_id": gard_id, "nctid": str(tid).strip(), "relationship": "GARD_HAS_CLINICAL_TRIAL"})
    write_csv(
        gard_trial_rows,
        os.path.join(args.output_dir, "gard_trial_relationships.csv"),
        ["gard_id", "nctid", "relationship"],
    )

    # 2) Author -> Article/Grant/Trial relationships via associations
    author_article_rows: List[Dict[str, Any]] = []
    author_grant_rows: List[Dict[str, Any]] = []
    author_trial_rows: List[Dict[str, Any]] = []

    unknown_assoc = 0
    for rel in assoc:
        a_id = rel.get("author_id")
        assoc_id = rel.get("associate_id")
        if a_id in (None, "", "nan") or assoc_id in (None, "", "nan"):
            continue
        a_id_str = str(a_id).strip()
        assoc_str = str(assoc_id).strip()
        if assoc_str in article_ids:
            author_article_rows.append({"author_id": a_id_str, "pubmed_id": assoc_str, "relationship": "AUTHORED_ARTICLE"})
        elif assoc_str in grant_ids:
            author_grant_rows.append({"author_id": a_id_str, "application_id": assoc_str, "relationship": "ASSOCIATED_WITH_GRANT"})
        elif assoc_str in trial_ids:
            author_trial_rows.append({"author_id": a_id_str, "nctid": assoc_str, "relationship": "ASSOCIATED_WITH_TRIAL"})
        else:
            unknown_assoc += 1
    if unknown_assoc:
        print(f"Associations with unknown associate_id: {unknown_assoc}")

    write_csv(
        author_article_rows,
        os.path.join(args.output_dir, "author_article_relationships.csv"),
        ["author_id", "pubmed_id", "relationship"],
    )
    write_csv(
        author_grant_rows,
        os.path.join(args.output_dir, "author_grant_relationships.csv"),
        ["author_id", "application_id", "relationship"],
    )
    write_csv(
        author_trial_rows,
        os.path.join(args.output_dir, "author_trial_relationships.csv"),
        ["author_id", "nctid", "relationship"],
    )

    # 3) Author <-> Author collaborations (undirected pairs, write once per pair)
    collab_rows: List[Dict[str, Any]] = []
    seen_pairs: Set[tuple] = set()
    for r in collab:
        a1 = r.get("author_id_1")
        a2 = r.get("author_id_2")
        if a1 in (None, "", "nan") or a2 in (None, "", "nan"):
            continue
        a1s = str(a1).strip()
        a2s = str(a2).strip()
        if not a1s or not a2s or a1s == a2s:
            continue
        key = tuple(sorted((a1s, a2s)))
        if key in seen_pairs:
            continue
        seen_pairs.add(key)
        collab_rows.append(
            {"author_id_1": key[0], "author_id_2": key[1], "relationship": "COLLABORATES_WITH"}
        )
    write_csv(
        collab_rows,
        os.path.join(args.output_dir, "author_collaborations_relationships.csv"),
        ["author_id_1", "author_id_2", "relationship"],
    )


if __name__ == "__main__":
    main()

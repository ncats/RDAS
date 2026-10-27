import argparse
import ast
import csv
import hashlib
import json
import os
import re
import unicodedata
from typing import Dict, List, Tuple

import pandas as pd


def parse_expertise_mapping(value: str) -> Dict[str, str]:
    if value is None:
        return {}
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return {}
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        pass
    try:
        obj = ast.literal_eval(text)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def extract_ordered_terms(mapping: Dict[str, str]) -> List[Tuple[int, str]]:
    items: List[Tuple[int, str]] = []
    for i in range(1, 4):
        key = f"expertise_{i}"
        val = mapping.get(key)
        if val is None:
            continue
        term = str(val).strip()
        if term:
            items.append((i, term))
    return items


def normalize_keyword(text: str) -> str:
    s = str(text).strip().lower()
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace("_", " ")
    s = s.replace("-", " ")
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def build_expertise_id(gard_id: str, norm: str) -> str:
    key = f"{gard_id}|{norm}"
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()
    return f"EXP_{digest}"


def is_nonempty(val) -> bool:
    if val is None:
        return False
    s = str(val).strip()
    return s not in ("", "nan", "None")


def clean_optional(val: str) -> str:
    s = str(val or "").strip()
    return "" if s.lower() in ("", "nan", "none") else s


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Post-process expertise outputs to create per-keyword nodes and relationships."
    )
    parser.add_argument(
        "--expertise-nodes",
        type=str,
        default="step2_clustering/expertise/expertise_node.csv",
        help="Path to expertise_node.csv from step2",
    )
    parser.add_argument(
        "--expertise-relationships",
        type=str,
        default="step2_clustering/expertise/expertise_relationships.csv",
        help="Path to expertise_relationships.csv from step2",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="csv_files",
        help="Directory to write keyword expertise outputs",
    )
    parser.add_argument(
        "--output-node",
        type=str,
        default="expertise_node_keyword.csv",
        help="Output filename for keyword expertise nodes (within output-dir)",
    )
    parser.add_argument(
        "--output-rel",
        type=str,
        default="expertise_relationships_keyword.csv",
        help="Output filename for keyword expertise relationships (within output-dir)",
    )
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    node_out = os.path.join(args.output_dir, args.output_node)
    rel_out = os.path.join(args.output_dir, args.output_rel)

    if not os.path.exists(args.expertise_nodes):
        raise FileNotFoundError(f"Missing expertise nodes file: {args.expertise_nodes}")
    if not os.path.exists(args.expertise_relationships):
        raise FileNotFoundError(f"Missing expertise relationships file: {args.expertise_relationships}")

    nodes_df = pd.read_csv(args.expertise_nodes, dtype=str)
    required_node_cols = {"entity_id", "gard_id", "expertise"}
    missing = required_node_cols - set(nodes_df.columns)
    if missing:
        raise ValueError(f"Missing required columns in {args.expertise_nodes}: {missing}")

    entity_expertise_map: Dict[str, List[Tuple[str, int, str, str]]] = {}
    keyword_nodes: Dict[str, Dict[str, str]] = {}
    skipped_entities = 0

    for _, row in nodes_df.iterrows():
        entity_id = str(row.get("entity_id") or "").strip()
        gard_id = clean_optional(row.get("gard_id"))
        cluster_label = clean_optional(row.get("cluster_label"))
        if not entity_id or not gard_id:
            skipped_entities += 1
            continue
        mapping = parse_expertise_mapping(row.get("expertise"))
        terms = extract_ordered_terms(mapping)
        if not terms:
            continue
        seen_norms = set()
        for rank, term in terms:
            norm = normalize_keyword(term)
            if not norm or norm in seen_norms:
                continue
            seen_norms.add(norm)
            expertise_id = build_expertise_id(gard_id, norm)
            key = f"{gard_id}|{norm}"
            if key not in keyword_nodes:
                keyword_nodes[key] = {
                    "expertise_id": expertise_id,
                    "expertise_keyword": term,
                    "expertise_keyword_norm": norm,
                    "gard_id": gard_id,
                }
            entity_expertise_map.setdefault(entity_id, []).append(
                (expertise_id, rank, gard_id, cluster_label)
            )

    node_rows = list(keyword_nodes.values())
    pd.DataFrame(node_rows).to_csv(node_out, index=False)
    print(f"Wrote {len(node_rows)} keyword expertise nodes -> {node_out}")
    if skipped_entities:
        print(f"Skipped {skipped_entities} rows missing entity_id or gard_id.")

    rel_df = pd.read_csv(args.expertise_relationships, dtype=str)
    required_rel_cols = {"entity_id", "author_id", "relationship_type"}
    missing = required_rel_cols - set(rel_df.columns)
    if missing:
        raise ValueError(f"Missing required columns in {args.expertise_relationships}: {missing}")

    fieldnames = [
        "author_id",
        "cluster_id",
        "gard_id",
        "cluster_label",
        "expertise_id",
        "expertise_rank",
        "relationship_type",
    ]

    rows_written = 0
    missing_entities = 0
    with open(rel_out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for _, r in rel_df.iterrows():
            entity_id = str(r.get("entity_id") or "").strip()
            author_id = str(r.get("author_id") or "").strip()
            if not entity_id or not author_id:
                continue
            rel_type = str(r.get("relationship_type") or "").strip()
            if entity_id not in entity_expertise_map:
                missing_entities += 1
                continue
            cluster_id = clean_optional(r.get("cluster_id"))
            gard_id = clean_optional(r.get("gard_id"))
            cluster_label = clean_optional(r.get("cluster_label"))
            for expertise_id, rank, map_gard_id, map_cluster_label in entity_expertise_map[entity_id]:
                writer.writerow(
                    {
                        "author_id": author_id,
                        "cluster_id": cluster_id,
                        "gard_id": gard_id or map_gard_id,
                        "cluster_label": cluster_label or map_cluster_label,
                        "expertise_id": expertise_id,
                        "expertise_rank": rank,
                        "relationship_type": rel_type,
                    }
                )
                rows_written += 1

    print(f"Wrote {rows_written} keyword expertise relationships -> {rel_out}")
    if missing_entities:
        print(f"Skipped {missing_entities} relationship rows with no expertise mapping.")


if __name__ == "__main__":
    main()

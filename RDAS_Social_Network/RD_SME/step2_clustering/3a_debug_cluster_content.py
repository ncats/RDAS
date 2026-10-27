"""
Debug: Inspect author abstract background for a specific GARD cluster

Outputs:
- summary CSV with per-author stats
- JSONL with per-author extracted texts (for deeper inspection)

Defaults target GARD:0006233 cluster 52 using step2 outputs and author_information.json.
"""

import os
import argparse
import json
import pandas as pd
from typing import Any, Dict, List


def _cluster_file_from_gard(input_dir: str, gard_id: str) -> str:
    safe = gard_id.replace(":", "_")
    return os.path.join(input_dir, f"clustered_{safe}.csv")


def _extract_abstract_content(full_abstract_list: Any) -> List[str]:
    if not isinstance(full_abstract_list, list):
        return []
    out: List[str] = []
    for item in full_abstract_list:
        if isinstance(item, dict):
            title = str(item.get('title', '')).strip()
            abstract = str(item.get('abstract', '')).strip()
            typ = item.get('type', 'Unknown')
            if title and abstract:
                out.append(f"[{typ}] {title}: {abstract}")
            elif title:
                out.append(f"[{typ}] {title}")
            elif abstract:
                out.append(f"[{typ}] {abstract}")
    return out


def load_author_map(author_file: str) -> Dict[str, Dict[str, Any]]:
    with open(author_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    amap: Dict[str, Dict[str, Any]] = {}
    for obj in data:
        a_id = obj.get('author_id')
        if a_id is None:
            continue
        amap[str(a_id)] = obj
    return amap


def debug_cluster(gard_id: str, cluster_label: int, input_dir: str, author_file: str, output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)
    clustered_file = _cluster_file_from_gard(input_dir, gard_id)
    if not os.path.exists(clustered_file):
        raise FileNotFoundError(f"Clustered file not found: {clustered_file}")

    df = pd.read_csv(clustered_file)
    group = df[df['cluster'] == cluster_label]
    if group.empty:
        raise ValueError(f"No rows for {gard_id} cluster {cluster_label} in {clustered_file}")

    author_map = load_author_map(author_file)

    rows: List[Dict[str, Any]] = []
    jsonl_path = os.path.join(
        output_dir,
        f"cluster_debug_texts_{gard_id.replace(':', '_')}_cluster_{cluster_label}.jsonl",
    )
    csv_path = os.path.join(
        output_dir,
        f"cluster_debug_summary_{gard_id.replace(':', '_')}_cluster_{cluster_label}.csv",
    )

    # Write JSONL stream
    with open(jsonl_path, 'w', encoding='utf-8') as jf:
        for _, r in group.iterrows():
            a_id = r['author_id']
            a_name = r.get('author_name', '') if 'author_name' in r else ''
            a_obj = author_map.get(str(a_id), {})
            full_abstract = a_obj.get('full_abstract', [])
            texts = _extract_abstract_content(full_abstract)
            num_items = len(full_abstract) if isinstance(full_abstract, list) else 0
            num_texts = len(texts)
            sample = texts[:2]
            rows.append({
                'author_id': a_id,
                'author_name': a_name,
                'num_full_abstract_items': num_items,
                'num_nonempty_texts': num_texts,
                'has_content': bool(num_texts > 0),
            })
            jf.write(json.dumps({
                'author_id': a_id,
                'author_name': a_name,
                'num_full_abstract_items': num_items,
                'texts': texts,
                'sample_texts': sample,
            }) + "\n")

    # Summary CSV
    pd.DataFrame(rows).to_csv(csv_path, index=False)

    print(f"Wrote JSONL to {jsonl_path}")
    print(f"Wrote summary CSV to {csv_path}")
    print(f"Authors in cluster: {len(rows)} | With content: {sum(1 for r in rows if r['has_content'])}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Debug author abstracts for a GARD cluster")
    parser.add_argument("--gard-id", type=str, default="GARD:0006233", help="GARD ID")
    parser.add_argument("--cluster-label", type=int, default=52, help="Cluster label to inspect")
    parser.add_argument(
        "--input-dir",
        type=str,
        default="step2_clustering/clustering",
        help="Directory containing clustered_*.csv",
    )
    parser.add_argument(
        "--author-file",
        type=str,
        default="step2_clustering/author_information.json",
        help="Path to author_information.json",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="step2_clustering/debug",
        help="Where to write debug outputs",
    )

    args = parser.parse_args()
    debug_cluster(
        gard_id=args.gard_id,
        cluster_label=args.cluster_label,
        input_dir=args.input_dir,
        author_file=args.author_file,
        output_dir=args.output_dir,
    )


if __name__ == "__main__":
    main()

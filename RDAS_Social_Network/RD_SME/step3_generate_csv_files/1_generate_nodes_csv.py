import os
import json
import argparse
import pandas as pd
from typing import Any, Dict, List


def load_json_list(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict):
        return [data]
    if not isinstance(data, list):
        raise ValueError(f"Expected an object or list in {path}")
    return data


def sanitize_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert any list/dict fields to JSON strings so CSV has clean scalar cells."""
    sanitized: List[Dict[str, Any]] = []
    for rec in records:
        out: Dict[str, Any] = {}
        for k, v in rec.items():
            if isinstance(v, (list, dict)):
                out[k] = json.dumps(v, ensure_ascii=False)
            else:
                out[k] = v
        sanitized.append(out)
    return sanitized


def write_csv(input_path: str, output_dir: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    base = os.path.basename(input_path)
    name_no_ext = base[:-5] if base.endswith(".json") else base
    output_path = os.path.join(output_dir, f"{name_no_ext}.csv")

    records = load_json_list(input_path)
    records = sanitize_records(records)
    df = pd.DataFrame(records)
    df.to_csv(output_path, index=False)
    print(f"Written {len(df)} rows -> {output_path}")
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert step1 node JSON files to CSV for Memgraph import")
    parser.add_argument(
        "--gard",
        type=str,
        default="step1_data_extraction/gard_node_GARD_0006233.json",
        help="Path to GARD node JSON",
    )
    parser.add_argument(
        "--grants",
        type=str,
        default="step1_data_extraction/grants_GARD_0006233.json",
        help="Path to Grants JSON",
    )
    parser.add_argument(
        "--articles",
        type=str,
        default="step1_data_extraction/articles_GARD_0006233.json",
        help="Path to Articles JSON",
    )
    parser.add_argument(
        "--trials",
        type=str,
        default="step1_data_extraction/clinical_trials_GARD_0006233.json",
        help="Path to Clinical Trials JSON",
    )
    parser.add_argument(
        "--authors",
        type=str,
        default="step1_data_extraction/authors_GARD_0006233.json",
        help="Path to Authors JSON",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="csv_files",
        help="Directory to write CSV files",
    )
    args = parser.parse_args()

    inputs = [
        ("GARD", args.gard),
        ("GRANTS", args.grants),
        ("ARTICLES", args.articles),
        ("TRIALS", args.trials),
        ("AUTHORS", args.authors),
    ]
    for label, path in inputs:
        if not os.path.exists(path):
            print(f"[WARN] Missing {label} JSON: {path}")
            continue
        write_csv(path, args.output_dir)


if __name__ == "__main__":
    main()

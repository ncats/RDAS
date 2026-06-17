#!/usr/bin/env python3
"""
Prepare Sample Properties by Subcategory

This script reads:
- the configured final subclustering file (subcategory -> labels mapping)
- sample_node.csv from the pipeline outputs (contains Sample_id and Sample_characteristics)

It outputs:
- sample_properties_by_subcategory.csv with columns:
  [Sample_id] + [one column per subcategory name], where each cell contains the
  value(s) from Sample_characteristics whose key belongs to that subcategory.

Notes:
- Keys in Sample_characteristics are matched case-insensitively against the
  'labels' lists from the subclustering file
- If multiple keys map to the same subcategory for a sample, their values are
  joined by " | "
- If duplicate subcategory names exist across main categories, they are
  disambiguated by appending " (main_category)" to the column name
- Spaces in subcategory column names are replaced with underscores for DB compatibility
"""

import pandas as pd
import ast
import os
import re
from typing import Dict, List, Tuple
from pathlib import Path
import sys

CURRENT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = CURRENT_DIR.parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from utils import ensure_parent_dir, load_paths


def normalize_label(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    # Replace underscores/hyphens with space
    s = s.replace("_", " ").replace("-", " ")
    # Collapse multiple spaces
    s = re.sub(r"\s+", " ", s)
    return s


def sanitize_key(name: str) -> str:
    """Replace spaces with underscores in property keys."""
    return str(name).replace(" ", "_")


def load_label_to_subcategory_map(subcluster_path: str) -> Tuple[Dict[str, str], Dict[str, str], List[str]]:
    """Load mapping from label -> subcategory; also main_category per subcategory; return full subcategory list.
    Returns:
      label_to_subcat: normalized_label -> subcategory
      subcat_to_main: subcategory -> main_category (first occurrence)
      subcat_list: ordered list of (possibly disambiguated and sanitized) subcategory column names
    """
    if not os.path.exists(subcluster_path):
        raise FileNotFoundError(f"Subcluster file not found: {subcluster_path}")

    df = pd.read_csv(subcluster_path)

    label_to_subcat: Dict[str, str] = {}
    subcat_to_main: Dict[str, str] = {}

    # Collect subcategories per main category; detect duplicates for disambiguation
    subcat_counts: Dict[str, int] = {}
    rows: List[Tuple[str, str, List[str]]] = []

    for _, row in df.iterrows():
        main_cat = str(row['main_category'])
        subcat = str(row['subcategory'])
        try:
            labels = ast.literal_eval(row['labels']) if isinstance(row['labels'], str) else []
        except Exception:
            labels = []
        rows.append((main_cat, subcat, labels))
        subcat_counts[subcat] = subcat_counts.get(subcat, 0) + 1

    # Build mapping; record main category for subcategory
    for main_cat, subcat, labels in rows:
        if subcat not in subcat_to_main:
            subcat_to_main[subcat] = main_cat
        for lab in labels:
            nlab = normalize_label(lab)
            if nlab and nlab not in label_to_subcat:
                label_to_subcat[nlab] = subcat

    # Build final subcategory column list; disambiguate duplicates by appending main category
    subcat_list: List[str] = []
    used_cols: Dict[str, int] = {}
    for main_cat, subcat, _ in rows:
        col_name = subcat
        if subcat_counts.get(subcat, 0) > 1:
            # Disambiguate
            col_name = f"{subcat} ({main_cat})"
        col_name = sanitize_key(col_name)
        if col_name not in used_cols:
            used_cols[col_name] = 1
            subcat_list.append(col_name)

    return label_to_subcat, subcat_to_main, subcat_list


def parse_sample_characteristics(cell: str) -> List[Tuple[str, str]]:
    """Parse a Sample_characteristics string into list of (key, value)."""
    if not isinstance(cell, str) or not cell.strip():
        return []
    parts = [p.strip() for p in cell.split(';') if p.strip()]
    kvs: List[Tuple[str, str]] = []
    for p in parts:
        if ':' in p:
            k, v = p.split(':', 1)
            k = k.strip()
            v = v.strip()
            if k:
                kvs.append((k, v))
    return kvs


def main():
    print("=== Preparing Sample Properties by Subcategory ===")
    paths = load_paths()
    subcluster_file = paths["sample_characteristics_subclustered_final"]
    sample_node_file = f"{paths['node_csv_files']}/sample_node.csv"
    output_file = paths["sample_properties_by_subcategory"]

    # Load mappings
    label_to_subcat, subcat_to_main, subcat_list = load_label_to_subcategory_map(subcluster_file)
    print(f"✓ Loaded {len(label_to_subcat)} labels mapped to {len(subcat_list)} subcategories")

    # Load sample_node.csv
    if not os.path.exists(sample_node_file):
        raise FileNotFoundError(f"Sample node file not found: {sample_node_file}")

    sdf = pd.read_csv(sample_node_file)
    if 'Sample_id' not in sdf.columns:
        raise ValueError("'Sample_id' column not found in sample_node.csv")
    if 'Sample_characteristics' not in sdf.columns:
        raise ValueError("'Sample_characteristics' column not found in sample_node.csv")

    # Prepare output rows
    output_rows: List[Dict[str, str]] = []

    # Build a mapping from original subcategory to sanitized column name (accounting for disambiguation)
    subcat_to_col: Dict[str, str] = {}
    subcluster_df = pd.read_csv(subcluster_file)
    # Precompute duplicates
    subcat_series = subcluster_df['subcategory'].astype(str)
    dup_mask = subcat_series.duplicated(keep=False)
    dup_names = set(subcat_series[dup_mask])

    for _, row in subcluster_df.iterrows():
        main_cat = str(row['main_category'])
        subcat = str(row['subcategory'])
        col_name = subcat
        if subcat in dup_names:
            col_name = f"{subcat} ({main_cat})"
        col_name = sanitize_key(col_name)
        if subcat not in subcat_to_col:
            subcat_to_col[subcat] = col_name

    for _, row in sdf.iterrows():
        sample_id = row['Sample_id']
        scell = row['Sample_characteristics']
        kvs = parse_sample_characteristics(scell)

        # Initialize all subcategory cells empty
        out_row: Dict[str, str] = {col: '' for col in subcat_list}
        out_row['Sample_id'] = sample_id

        # Aggregate values per subcategory
        agg: Dict[str, List[str]] = {}
        for k, v in kvs:
            nk = normalize_label(k)
            subcat = label_to_subcat.get(nk)
            if not subcat:
                continue
            col_name = subcat_to_col.get(subcat, sanitize_key(subcat))
            agg.setdefault(col_name, []).append(v)

        # Assign aggregated values
        for col_name, vals in agg.items():
            # Deduplicate while preserving order
            seen = set()
            deduped = []
            for val in vals:
                if val not in seen:
                    seen.add(val)
                    deduped.append(val)
            out_row[col_name] = ' | '.join(deduped)

        output_rows.append(out_row)

    # Create output DataFrame with columns order: Sample_id + subcat_list
    out_df = pd.DataFrame(output_rows)
    # Ensure all columns exist
    for col in ['Sample_id'] + subcat_list:
        if col not in out_df.columns:
            out_df[col] = ''
    out_df = out_df[['Sample_id'] + subcat_list]

    ensure_parent_dir(output_file)
    out_df.to_csv(output_file, index=False)
    print(f"✓ Wrote {len(out_df)} rows to {output_file}")


if __name__ == "__main__":
    main()

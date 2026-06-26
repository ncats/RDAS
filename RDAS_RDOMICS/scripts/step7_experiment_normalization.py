"""Normalize experiment properties using the configured mapping table."""

import argparse
import os
from pathlib import Path
import sys

import pandas as pd

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from utils import ensure_parent_dir, load_paths


def normalize_experiment_data(config_path: str | None = None) -> None:
    paths = load_paths(config_path)
    norm_file_path = paths["experiment_normalization_rules"]
    experiment_file_path = os.path.join(paths["node_csv_files"], "experiment_node.csv")
    normalized_experiment_file_path = paths["experiment_node_normalized"]

    norm_df = pd.read_csv(norm_file_path)
    exp_df = pd.read_csv(experiment_file_path)

    exp_df['Omics_type_norm'] = ''
    exp_df['Sequencing_type_norm'] = ''

    assay_type_column = "Assay_type_norm" if "Assay_type_norm" in norm_df.columns else "Sequencing_type_norm"
    norm_dict = {}
    for _, row in norm_df.iterrows():
        key = (str(row['Omics_type']), str(row['Sequencing_type']), str(row['Sequencing_library']))
        value = (row['Omics_type_norm'], row[assay_type_column])
        norm_dict[key] = value

    for idx, row in exp_df.iterrows():
        key = (str(row['Omics_type']), str(row['Sequencing_type']), str(row['Sequencing_library']))
        if key in norm_dict:
            omics_norm, seq_norm = norm_dict[key]
            exp_df.at[idx, 'Omics_type_norm'] = omics_norm
            exp_df.at[idx, 'Sequencing_type_norm'] = seq_norm

    ensure_parent_dir(normalized_experiment_file_path)
    exp_df.to_csv(normalized_experiment_file_path, index=False)
    print(f"Normalization complete. Updated {sum(exp_df['Omics_type_norm'] != '')} rows.")
    print(f"Total rows in experiment_node.csv: {len(exp_df)}")
    print(f"Rows with normalized Omics type: {sum(exp_df['Omics_type_norm'] != '')}")
    print(f"Rows with normalized Sequencing type: {sum(exp_df['Sequencing_type_norm'] != '')}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize experiment node properties.")
    parser.add_argument("--config", help="Optional path to a YAML config file.")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    normalize_experiment_data(args.config)

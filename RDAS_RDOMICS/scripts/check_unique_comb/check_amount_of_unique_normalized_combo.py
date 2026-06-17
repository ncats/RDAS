import pandas as pd
import numpy as np
from pathlib import Path
import sys

CURRENT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = CURRENT_DIR.parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from utils import load_paths

paths = load_paths()
df = pd.read_csv(paths['experiment_normalization_rules'])

print("=== Dataset Overview ===")
print(f"Total rows in dataset: {len(df)}")
print(f"Dataset shape: {df.shape}")
print()

print("=== Column Information ===")
print("Columns in the dataset:")
for col in df.columns:
    print(f"  - {col}")
print()

print("=== Unique Values Analysis ===")
print(f"Unique values in 'Omics_type_norm': {df['Omics_type_norm'].nunique()}")
print(f"Unique values in 'Sequencing_library_norm': {df['Sequencing_library_norm'].nunique()}")
print()

print("=== Unique Combinations ===")
# Get unique combinations of the two columns
unique_combinations = df[['Omics_type_norm', 'Sequencing_library_norm']].drop_duplicates()
print(f"Total unique combinations of 'Omics_type_norm' and 'Sequencing_library_norm': {len(unique_combinations)}")
print()

print("=== All Unique Combinations ===")
print("Omics_type_norm | Sequencing_library_norm")
print("-" * 50)
for idx, row in unique_combinations.iterrows():
    omics_type = row['Omics_type_norm']
    seq_lib = row['Sequencing_library_norm']
    # Handle NaN values
    if pd.isna(seq_lib):
        seq_lib = "NaN/Empty"
    print(f"{omics_type} | {seq_lib}")

print()
print("=== Frequency of Each Combination ===")
combination_counts = df.groupby(['Omics_type_norm', 'Sequencing_library_norm']).size().reset_index(name='count')
combination_counts = combination_counts.sort_values('count', ascending=False)
print("Omics_type_norm | Sequencing_library_norm | Count")
print("-" * 60)
for idx, row in combination_counts.iterrows():
    omics_type = row['Omics_type_norm']
    seq_lib = row['Sequencing_library_norm']
    count = row['count']
    # Handle NaN values
    if pd.isna(seq_lib):
        seq_lib = "NaN/Empty"
    print(f"{omics_type} | {seq_lib} | {count}")

print()
print("=== Summary Statistics ===")
print(f"Most common combination appears {combination_counts['count'].max()} times")
print(f"Least common combination appears {combination_counts['count'].min()} times")
print(f"Average frequency per combination: {combination_counts['count'].mean():.2f}")

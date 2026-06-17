import pandas as pd
from pathlib import Path
import sys

CURRENT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = CURRENT_DIR.parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from utils import load_paths

paths = load_paths()
df = pd.read_csv(f"{paths['node_csv_files']}/experiment_node.csv")

# Select the relevant columns
df_subset = df[['Omics_type', 'Sequencing_type', 'Sequencing_library']]

# Get unique combinations, including rows with NaN in either column
unique_combinations = df_subset.drop_duplicates()

# Sort (optional)
unique_combinations = unique_combinations.sort_values(by=['Omics_type', 'Sequencing_type', 'Sequencing_library'])

# Save to CSV
unique_combinations.to_csv("omics_sequencing_combinations2.csv", index=False)

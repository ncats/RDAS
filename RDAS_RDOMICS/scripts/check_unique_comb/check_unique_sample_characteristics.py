import pandas as pd
from collections import Counter
from pathlib import Path
import sys

CURRENT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = CURRENT_DIR.parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from utils import load_paths

paths = load_paths()
df = pd.read_csv(f"{paths['node_csv_files']}/sample_node.csv")

# Initialize a counter for keys
key_counter = Counter()

# Iterate through the 'Sample_characteristics' column
for entry in df['Sample_characteristics'].dropna():
    for item in entry.split(';'):
        item = item.strip()
        if ':' in item:
            key = item.split(':', 1)[0].strip()
            key_counter[key] += 1

# Convert to DataFrame
key_df = pd.DataFrame(key_counter.items(), columns=['name', 'count'])

# Sort by count descending (optional)
key_df = key_df.sort_values(by='count', ascending=False)

# Save to CSV
key_df.to_csv("sample_characteristics_key_count.csv", index=False)

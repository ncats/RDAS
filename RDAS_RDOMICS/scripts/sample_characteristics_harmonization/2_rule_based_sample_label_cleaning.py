import pandas as pd
import re
from pathlib import Path
import sys

CURRENT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = CURRENT_DIR.parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from utils import ensure_parent_dir, load_paths

def is_meaningful_label(label):
    """
    Check if a label is meaningful and worth keeping for clustering.
    Returns True if the label should be kept, False if it should be filtered out.
    """
    # Convert to string and strip whitespace
    label = str(label).strip()
    
    # Filter out empty or very short labels
    if len(label) < 2:
        return False
    
    # Filter out labels that are too long (likely to be descriptions rather than categories)
    if len(label) > 100:
        return False
    
    # Filter out labels with unmatched parentheses or brackets
    if label.count('(') != label.count(')') or label.count('[') != label.count(']'):
        return False
    
    # Filter out labels that are mostly numbers
    if re.match(r'^[\d\.\-\+\s]+$', label):
        return False
    
    # Filter out labels that are mostly special characters
    special_char_ratio = len(re.findall(r'[^a-zA-Z0-9\s]', label)) / len(label)
    if special_char_ratio > 0.5:
        return False
    
    # Filter out labels that look like file paths or URLs
    if '/' in label and len(label.split('/')) > 2:
        return False
    if 'http' in label.lower() or 'www.' in label.lower():
        return False
    
    # Filter out labels that are just punctuation or symbols
    if re.match(r'^[^\w\s]+$', label):
        return False
    
    # Filter out labels that start or end with special characters (except common ones)
    if re.match(r'^[^\w\s]', label) or re.search(r'[^\w\s]$', label):
        # Allow labels ending with common abbreviations like pH, %, etc.
        if not re.search(r'(ph|%|\w)$', label.lower()):
            return False
    
    # Filter out labels that look like gene constructs or technical identifiers
    # Examples: "Tg(-7.2sox10", "ST22 (H37Rv ideR"
    if re.search(r'^[A-Z]{1,5}\d+\s*\(', label) or re.search(r'Tg\(', label):
        return False
    
    # Filter out labels that are mostly uppercase and look like codes
    if len(label) > 5 and label.isupper() and re.search(r'\d', label):
        return False
    
    # Filter out labels with excessive underscores or dashes
    underscore_ratio = label.count('_') / len(label)
    dash_ratio = label.count('-') / len(label)
    if underscore_ratio > 0.3 or dash_ratio > 0.3:
        return False
    
    return True

def clean_sample_labels(input_file, output_file):
    """
    Clean the sample characteristics labels by removing non-meaningful terms.
    """
    # Load the data
    df = pd.read_csv(input_file)
    
    print(f"Original dataset: {len(df)} labels")
    
    # Apply filtering
    df['is_meaningful'] = df['name'].apply(is_meaningful_label)
    
    # Show some examples of filtered out labels
    filtered_out = df[~df['is_meaningful']]['name'].head(20).tolist()
    print(f"\nExamples of filtered out labels:")
    for label in filtered_out:
        print(f"  '{label}'")
    
    # Keep only meaningful labels
    cleaned_df = df[df['is_meaningful']].copy()
    cleaned_df = cleaned_df.drop('is_meaningful', axis=1)
    
    # Sort by count (descending) to see most common terms first
    cleaned_df = cleaned_df.sort_values('count', ascending=False)
    
    print(f"\nCleaned dataset: {len(cleaned_df)} labels")
    print(f"Removed: {len(df) - len(cleaned_df)} labels ({(len(df) - len(cleaned_df))/len(df)*100:.1f}%)")
    
    # Save cleaned data
    cleaned_df.to_csv(output_file, index=False)
    print(f"Cleaned data saved to: {output_file}")
    
    # Show top 20 most frequent cleaned labels
    print(f"\nTop 20 most frequent cleaned labels:")
    for i, (_, row) in enumerate(cleaned_df.head(20).iterrows()):
        print(f"{i+1:2d}. {row['name']} (count: {row['count']})")
    
    return cleaned_df

if __name__ == "__main__":
    paths = load_paths()
    input_file = paths["sample_characteristics_key_count_cleaned"]
    output_file = paths["sample_characteristics_key_count_rule_based"]
    ensure_parent_dir(output_file)
    
    cleaned_df = clean_sample_labels(input_file, output_file) 

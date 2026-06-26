import pandas as pd
import re
from collections import Counter
from pathlib import Path
import sys
from tqdm import tqdm

CURRENT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = CURRENT_DIR.parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from utils import ensure_parent_dir, load_paths

def clean_label(label):
    """
    Remove special characters from the beginning and end of a label.
    Keep only alphanumeric characters and spaces, plus some common scientific notation.
    """
    if not isinstance(label, str):
        return str(label)
    
    # Remove leading and trailing special characters
    # Keep letters, numbers, spaces, and some common scientific characters in the middle
    cleaned = label.strip()
    
    # Remove leading special characters (except alphanumeric and space)
    cleaned = re.sub(r'^[^\w\s]+', '', cleaned)
    
    # Remove trailing special characters (except alphanumeric and space)  
    cleaned = re.sub(r'[^\w\s]+$', '', cleaned)
    
    # Clean up any remaining unwanted characters but preserve important ones
    # Remove quotes, brackets, etc. but keep hyphens, underscores in the middle
    cleaned = re.sub(r'^["\'\[\]{}()<>]+', '', cleaned)
    cleaned = re.sub(r'["\'\[\]{}()<>]+$', '', cleaned)
    
    # Remove leading/trailing percentage, caret, etc.
    cleaned = re.sub(r'^[%^#@!&*+=|\\/:;,\.]+', '', cleaned)
    cleaned = re.sub(r'[%^#@!&*+=|\\/:;,\.]+$', '', cleaned)
    
    # Final trim
    cleaned = cleaned.strip()
    
    return cleaned

def clean_sample_characteristics():
    """
    Clean special characters from sample characteristics labels and regenerate files.
    """
    paths = load_paths()
    input_file = f"{paths['node_csv_files']}/sample_node.csv"
    output_file = paths["sample_characteristics_cleaned_sample_node"]
    
    print(f"Loading sample node data from: {input_file}")
    df = pd.read_csv(input_file)
    
    print(f"Original dataset: {len(df)} samples")
    
    # Track changes for reporting
    changes_made = 0
    examples_before_after = []
    
    # Process each row's Sample_characteristics with progress bar
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Processing samples"):
        if pd.isna(row['Sample_characteristics']):
            continue
            
        characteristics = row['Sample_characteristics']
        cleaned_pairs = []
        
        # Split by semicolon to get individual key:value pairs
        for item in characteristics.split(';'):
            item = item.strip()
            if ':' in item:
                # Split on first colon to separate key and value
                key, value = item.split(':', 1)
                original_key = key.strip()
                cleaned_key = clean_label(original_key)
                
                # Track examples of changes
                if original_key != cleaned_key and len(examples_before_after) < 20:
                    examples_before_after.append((original_key, cleaned_key))
                    changes_made += 1
                elif original_key != cleaned_key:
                    changes_made += 1
                
                # Reconstruct the key:value pair
                cleaned_pairs.append(f"{cleaned_key}:{value.strip()}")
            else:
                # If no colon, keep as is
                cleaned_pairs.append(item)
        
        # Update the row with cleaned characteristics
        df.loc[idx, 'Sample_characteristics'] = '; '.join(cleaned_pairs)
    
    # Save the cleaned sample node file
    ensure_parent_dir(output_file)
    df.to_csv(output_file, index=False)
    print(f"Cleaned sample node data saved to: {output_file}")
    
    # Show examples of changes made
    print(f"\nChanges made: {changes_made} labels cleaned")
    print(f"\nExamples of cleaning (before -> after):")
    for before, after in examples_before_after:
        print(f"  '{before}' -> '{after}'")
    
    return df

def generate_cleaned_key_counts(sample_df):
    """
    Generate key counts from the cleaned sample characteristics.
    """
    print("\nGenerating key counts from cleaned data...")
    
    # Initialize a counter for keys
    key_counter = Counter()
    
    # Iterate through the cleaned 'Sample_characteristics' column with progress bar
    for entry in tqdm(sample_df['Sample_characteristics'].dropna(), desc="Counting keys"):
        for item in entry.split(';'):
            item = item.strip()
            if ':' in item:
                key = item.split(':', 1)[0].strip()
                # Additional cleaning in case some special chars remain
                key = clean_label(key)
                if key:  # Only count non-empty keys
                    key_counter[key] += 1
    
    # Convert to DataFrame
    key_df = pd.DataFrame(key_counter.items(), columns=['name', 'count'])
    
    # Sort by count descending
    key_df = key_df.sort_values(by='count', ascending=False)
    
    paths = load_paths()
    output_file = paths["sample_characteristics_key_count_cleaned"]
    ensure_parent_dir(output_file)
    key_df.to_csv(output_file, index=False)
    
    print(f"Key counts saved to: {output_file}")
    print(f"Total unique keys: {len(key_df)}")
    
    # Show top 20 most frequent keys
    print(f"\nTop 20 most frequent keys after cleaning:")
    for i, (_, row) in enumerate(key_df.head(20).iterrows()):
        print(f"{i+1:2d}. {row['name']} (count: {row['count']})")
    
    return key_df

def main():
    """
    Main function to clean sample characteristics and regenerate key counts.
    """
    print("=== Cleaning Sample Characteristics Labels ===")
    
    # Step 1: Clean the sample characteristics in the sample_node.csv
    cleaned_sample_df = clean_sample_characteristics()
    
    # Step 2: Generate new key counts from cleaned data
    key_counts_df = generate_cleaned_key_counts(cleaned_sample_df)
    
    print("\n=== Cleaning Complete ===")
    print("Files generated:")
    print("1. sample_node_chars_removed.csv - Sample node data with cleaned characteristic labels")
    print("2. sample_characteristics_key_count_chars_removed.csv - Key frequency counts from cleaned labels")

if __name__ == "__main__":
    main() 

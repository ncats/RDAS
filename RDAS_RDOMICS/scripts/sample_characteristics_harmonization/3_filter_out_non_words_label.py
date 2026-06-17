"""
Sample Characteristics Label Filtering Script

This script filters sample characteristic labels to retain only those that contain
standard English words from the NLTK English words corpus.

PURPOSE:
--------
After cleaning special characters and applying rule-based filtering, many labels may still 
contain non-English words, random strings, or irrelevant terms. This script uses a strict
English-only approach to filter out all non-English terms, including scientific 
abbreviations and biomedical terminology.

METHODOLOGY:
-----------
1. English Dictionary Matching:
   - Uses NLTK's words corpus to identify standard English words
   - Handles common suffixes (plurals, -ing, -ed, etc.)
   - Recognizes compound words and variations

2. Compound Term Analysis:
   - For multi-word labels, analyzes each component separately
   - Keeps labels ONLY if ALL words are standard English words (100% requirement)
   - Handles various separators (spaces, hyphens, underscores, slashes, periods)
   - Rejects any label containing even one non-English word

INPUT:
------
- sample_characteristics_key_count_chars_removed_rule_based_cleaned.csv
  Contains cleaned sample characteristic labels with their frequency counts

OUTPUT:
-------
- 3_sample_characteristics_key_count_english_only.csv
  Filtered labels containing only standard English words
  
- 3removed_non_english_labels.txt
  List of labels that were filtered out for manual review

EXAMPLE LABELS KEPT:
-------------------
- "tissue" (English word)
- "cell type" (English words)
- "patient diagnosis" (English words)
- "sample name" (English words)
- "treatment group" (English words)

EXAMPLE LABELS REMOVED:
----------------------
- "DNA methylation" (DNA is not in English dictionary)
- "TP53 mutation" (TP53 is not in English dictionary)
- "covid status" (covid is not in English dictionary)
- Random character strings
- Non-English language terms
- Scientific abbreviations

DEPENDENCIES:
------------
- nltk (for English words corpus)
- pandas (for data processing)
- tqdm (for progress bars)

AUTHOR: Biomedical Data Processing Pipeline
DATE: 2024
"""

import pandas as pd
import nltk
from nltk.corpus import words
import re
from tqdm import tqdm
import string
import warnings
from pathlib import Path
import sys
warnings.filterwarnings('ignore')

CURRENT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = CURRENT_DIR.parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from utils import ensure_parent_dir, load_paths

# Download required NLTK data
try:
    nltk.data.find('corpora/words')
except LookupError:
    print("Downloading NLTK words corpus...")
    nltk.download('words')

def is_english_word(word):
    """
    Check if a word is in the English dictionary.
    """
    if not word or len(word) < 2:
        return False
    
    # Get NLTK English words
    english_words = set(words.words())
    
    # Add some common words that might not be in NLTK dictionary
    supplementary_words = {
        'ethnicity', 'clinical', 'info', 'timepoint', 'dataset', 'barcode',
        'id', 'type', 'state', 'part', 'point', 'version', 'batch', 'cell'
    }
    english_words.update(supplementary_words)
    
    # Convert to lowercase for comparison
    word_lower = word.lower()
    
    # Direct match in NLTK words corpus
    if word_lower in english_words:
        return True
    
    # Handle plurals (simple case)
    if word_lower.endswith('s') and word_lower[:-1] in english_words:
        return True
    
    # Handle common English suffixes
    common_suffixes = ['ing', 'ed', 'er', 'est', 'ly', 'tion', 'sion', 'ment', 'ness', 'able', 'ible']
    for suffix in common_suffixes:
        if word_lower.endswith(suffix):
            root = word_lower[:-len(suffix)]
            if len(root) > 2 and root in english_words:
                return True
    
    return False

def split_compound_words(text):
    """
    Split compound words and camelCase words into individual components.
    """
    # First handle camelCase (like "cellType" -> "cell Type")
    text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
    
    # Handle common compound patterns by inserting spaces
    # This is a simple approach for the most common patterns
    patterns = [
        (r'subject(id|ID)', r'subject \1'),
        (r'patient(id|ID)', r'patient \1'),
        (r'sample(id|ID)', r'sample \1'),
        (r'cell(type|Type)', r'cell \1'),
        (r'tissue(type|Type)', r'tissue \1'),
        (r'disease(state|State)', r'disease \1'),
        (r'organism(part|Part)', r'organism \1'),
        (r'time(point|Point)', r'time \1'),
        (r'data(type|Type)', r'data \1'),
        (r'bio(material|Material)', r'bio \1'),
        (r'lab(version|Version)', r'lab \1'),
        (r'treatment(short|Short)', r'treatment \1'),
        (r'control(id|ID)', r'control \1'),
        (r'plate(simple|Simple)', r'plate \1'),
        (r'software(version|Version)', r'software \1'),
        (r'sequencing(batch|Batch)', r'sequencing \1'),
        (r'flow(cell|Cell)', r'flow \1'),
    ]
    
    for pattern, replacement in patterns:
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    
    return text

def should_keep_label(label):
    """
    Determine if a label should be kept based on English words only.
    ALL words in the label must be standard English dictionary words.
    """
    if not label or not isinstance(label, str):
        return False
    
    # Clean the label
    label = label.strip()
    if len(label) < 2:
        return False
    
    # Split compound words first
    label = split_compound_words(label)
    
    # Split into words (handling spaces, hyphens, underscores, slashes, and periods)
    words_in_label = re.split(r'[\s\-_/\.]+', label.lower())
    words_in_label = [w.strip(string.punctuation) for w in words_in_label if w.strip(string.punctuation)]
    
    if not words_in_label:
        return False
    
    # ALL words must be English dictionary words
    for word in words_in_label:
        if not is_english_word(word):
            return False  # If any word is not English, reject the entire label
    
    # If we get here, all words were English
    return True

def filter_non_english_labels():
    """
    Filter out labels that don't contain only English words.
    """
    paths = load_paths()
    input_file = paths["sample_characteristics_key_count_rule_based"]
    output_file = paths["sample_characteristics_key_count_english_only"]
    
    print(f"Loading cleaned labels from: {input_file}")
    df = pd.read_csv(input_file)
    
    print(f"Original dataset: {len(df)} labels")
    
    # Filter labels
    filtered_labels = []
    removed_labels = []
    
    print("Filtering labels to keep only English dictionary words...")
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Processing labels"):
        label = row['name']
        if should_keep_label(label):
            filtered_labels.append(row)
        else:
            removed_labels.append(label)
    
    # Create filtered DataFrame
    filtered_df = pd.DataFrame(filtered_labels)
    
    # Sort by count descending
    if not filtered_df.empty:
        filtered_df = filtered_df.sort_values(by='count', ascending=False).reset_index(drop=True)
    
    # Save filtered results
    ensure_parent_dir(output_file)
    filtered_df.to_csv(output_file, index=False)
    
    print(f"\nFiltering complete!")
    print(f"Labels kept: {len(filtered_df)}")
    print(f"Labels removed: {len(removed_labels)}")
    print(f"Retention rate: {len(filtered_df)/len(df)*100:.1f}%")
    print(f"Filtered data saved to: {output_file}")
    
    # Show examples of removed labels
    print(f"\nExamples of removed labels (first 20):")
    for i, label in enumerate(removed_labels[:20]):
        print(f"  {i+1:2d}. '{label}'")
    
    if len(removed_labels) > 20:
        print(f"  ... and {len(removed_labels) - 20} more")
    
    # Show top 20 kept labels
    print(f"\nTop 20 most frequent kept labels:")
    for i, (_, row) in enumerate(filtered_df.head(20).iterrows()):
        print(f"  {i+1:2d}. {row['name']} (count: {row['count']})")
    
    return filtered_df, removed_labels

def save_removed_labels(removed_labels):
    """
    Save the list of removed labels for review.
    """
    output_file = load_paths()["sample_characteristics_removed_non_english"]
    
    ensure_parent_dir(output_file)
    with open(output_file, 'w') as f:
        f.write("Labels removed as non-English:\n")
        f.write("=" * 40 + "\n\n")
        for i, label in enumerate(removed_labels, 1):
            f.write(f"{i:4d}. {label}\n")
    
    print(f"Removed labels saved to: {output_file}")

def main():
    """
    Main function to filter out non-English labels.
    """
    print("=== Filtering Non-English Labels ===")
    print("Using English dictionary only - strict filtering...")
    
    # Filter the labels
    filtered_df, removed_labels = filter_non_english_labels()
    
    # Save removed labels for review
    save_removed_labels(removed_labels)
    
    print("\n=== Filtering Complete ===")
    print("Files generated:")
    print("1. 3_sample_characteristics_key_count_english_only.csv - English words only")
    print("2. 3removed_non_english_labels.txt - List of removed labels for review")

if __name__ == "__main__":
    main()

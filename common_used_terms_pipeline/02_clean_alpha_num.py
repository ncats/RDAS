"""
   Reads the input file tsv to clean out any synonyms that contain numbers and symbols, or that could be a gene name.
   Rows that are cleaned out will be written to the deleted file tsv. Everything kept will be in the output file tsv.
"""

import csv
import re

INPUT_FILE = "nltk_matched_terms.tsv"
OUTPUT_FILE = "clean_alpha_num_matched_terms.tsv"
DELETED_FILE = "deleted_alpha_num_matched_terms.tsv"

# Matches words that have numbers in them
LETTER_NUMBER = re.compile(r"\b(?=\w*[A-Za-z])(?=\w*\d)\w+(?:-\w+)?\b")

# Matches any 2+ letter words that have all uppercase letters that could be abbreviations
UPPERCASE = re.compile(r"\b[A-Z]{2,}\b")

# Matches alphanumeric chunks with hyphens (pattern for gene/protein names)
GENE_HYPHEN = re.compile(r"\b[A-Za-z0-9]+-[A-Za-z0-9]+\b")

# Helper method to search and mark the presence
def looks_like_gene_or_code(text):
    if LETTER_NUMBER.search(text):
        return True

    if GENE_HYPHEN.search(text):
        return True

    if UPPERCASE.search(text):
        return True

    return False


def main():
    # Open the input file to read
    with open(INPUT_FILE, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        fieldnames = reader.fieldnames
        rows = list(reader)

    # Lists for the rows kept and removed
    kept = []
    removed_rows = []

    # Analyze each row using helper method and group them
    for row in rows:
        synonym = row["Synonym"]

        if looks_like_gene_or_code(synonym):
            removed_rows.append(row)
            continue

        kept.append(row)

    # Open the output and delete files to write the keep and delete results.
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(kept)

    with open(DELETED_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(removed_rows)

    print(f"Original rows : {len(rows)}")
    print(f"Removed rows  : {len(removed_rows)}")
    print(f"Remaining rows: {len(kept)}")
    print(f"Output written to {OUTPUT_FILE}")
    print(f"Removed rows written to {DELETED_FILE}")


if __name__ == "__main__":
    main()
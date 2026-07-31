"""
    Reads the input file tsv to conditionally clean out repeating matches. 
    Rows that are cleaned out will be written to the deleted file tsv. Everything kept will be in the output file tsv
"""

import csv
import re
from collections import Counter

INPUT_FILE = "clean_alpha_num_matched_terms.tsv"
OUTPUT_FILE = "clean_repeating_matched_terms.tsv"
DELETED_FILE = "deleted_repeating_matched_terms.tsv"

# Helper method to normalize the text (lowercase and remove extra spacing)
def normalize(text):
    return (text or "").strip().lower()

# Helper method to see if the synonym has 2+ words
def has_two_or_more_words(text):
    return len(re.findall(r"\S+", text.strip())) >= 2


def main():
    # Open the input file to read
    with open(INPUT_FILE, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        fieldnames = reader.fieldnames
        rows = list(reader)

    if not fieldnames:
        raise ValueError("No headers found in the TSV file.")

    # Counts how many times each normalized matched term appears across all rows
    matched_term_counts = Counter(
        normalize(row.get("Matched Term", ""))
        for row in rows
    )

    # Counts how many times each normalized synonym appears across all rows
    synonym_counts = Counter(
        normalize(row.get("Synonym", ""))
        for row in rows
    )

    # Lists for the kept and removed rows
    filtered_rows = []
    removed_rows = []

    # Go through each row
    for row in rows:
        # Normalize the matched term and synonym
        matched_term = normalize(row.get("Matched Term", ""))
        synonym = normalize(row.get("Synonym", ""))

        remove = False

        # Rule 1:
        # Remove rows whose Matched Term occurs more than 5 times,
        # unless the Synonym is exactly the same as the Matched Term. 
        # This assumes that the matched term is likely to be a very 
        # generic, medical term that is not of concern.
        if matched_term_counts[matched_term] > 5:
            if synonym != matched_term:
                remove = True

        # Rule 2:
        # If the Synonym has 2+ words and appears 5+ times,
        # remove all rows with that synonym.
        # Applies the same idea as Rule 1, but for multi-word synonyms
        elif has_two_or_more_words(synonym) and synonym_counts[synonym] >= 5:
            remove = True

        # Appends the rows to the remove or keep lists accordingly. 
        if remove:
            removed_rows.append(row)
        else:
            filtered_rows.append(row)


    # Open the output and delete files to write the keep and delete results.
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(filtered_rows)

    with open(DELETED_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(removed_rows)

    print(f"Original rows: {len(rows)}")
    print(f"Remaining rows: {len(filtered_rows)}")
    print(f"Removed rows: {len(removed_rows)}")
    print(f"Output written to {OUTPUT_FILE}")
    print(f"Removed rows written to {DELETED_FILE}")


if __name__ == "__main__":
    main()
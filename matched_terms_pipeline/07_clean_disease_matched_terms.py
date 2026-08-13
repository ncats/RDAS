"""
    Removes rows in input file tsv where the synonym has the word disease or syndrome. 
    Output file tsv will keep all the remaining rows.
"""

import csv
import re

INPUT_FILE = "06_1_clean_medical_suffix_matched_terms.tsv"
OUTPUT_FILE = "07_1_clean_disease_matched_terms.tsv"

REMOVE_WORDS = {
    "disease",
    "syndrome",
}

# Helper method to normalize the text (lowercase and remove extra spacing)
def normalize(text):
    return (text or "").strip().lower()

# Helper method to split string into separate words. 
def get_words(text):
    """Return alphabetic words from a string."""
    return re.findall(r"[A-Za-z]+", normalize(text))


def main():
    # Open the input file to read
    with open(INPUT_FILE, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        fieldnames = reader.fieldnames
        rows = list(reader)

    # List to hold the rows to keep and counter for number of removed rows
    kept_rows = []
    removed = 0

    # Go through each row
    for row in rows:
        # Get the synonym of the row
        synonym = row["Synonym"]
        # Break apart the synonym into individual words
        words = get_words(synonym)

        # Remove if the synonym has 2 or more words
        # and one of those words is in REMOVE_WORDS
        if len(words) >= 2 and any(word in REMOVE_WORDS for word in words):
            removed += 1
            continue

        # Other rows are kept
        kept_rows.append(row)

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(kept_rows)

    print(f"Original rows : {len(rows)}")
    print(f"Removed rows  : {removed}")
    print(f"Remaining rows: {len(kept_rows)}")
    print(f"Output written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()

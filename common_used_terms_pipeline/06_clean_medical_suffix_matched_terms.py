"""
    Reads the input file tsv to remove rows that have medical suffixes in the synonym/matched term (less likely to be common terms). 
    Rows that are kept will be in output file tsv. 
"""

import csv

INPUT_FILE = "06_1_clean_scispacy_matched_terms.tsv"
OUTPUT_FILE = "06_2_clean_medical_suffix_matched_terms.tsv"

# List of typical medical suffixes. Avoids any that could be common term.
SUFFIXES = (
    "itis",
    "osis",
    "emia",
    "ectomy",
    "algia",
    "iasis",
    "oma",
    "cytic",
    "cyte",
    "lysis",
    "penia",
    "philia",
    "osus",
    "uria",
    "amic",
    "oria",
    "egia",
    "ylia",
    "diac",
    "esis",
    "lasia",
    "elia",
    "idia",
    "omia",
    "almia",
    "enia",
    "achia",
    "opia",
    "dema",
    "ditism",
    "ocele",
    "dystrophy",
    "esia",
    "rtrophy",
    "otrophy",
    "ctasia",
    "stula",
)

# Helper method to normalize the text (lowercase and remove extra spacing)
def normalize(text):
    return (text or "").strip().lower()


def main():
    # Opens the input file to read
    with open(INPUT_FILE, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        fieldnames = reader.fieldnames
        rows = list(reader)

    # List to hold rows to keep, and track number of rows removed.
    kept_rows = []
    removed = 0

    # Go through each row
    for row in rows:
        # Normalize synonym and matched term
        synonym = normalize(row["Synonym"])
        matched_term = normalize(row["Matched Term"])

        # Checks that the synonym ends in the suffix. If so, remove and then go to next row
        if synonym.endswith(SUFFIXES):
            removed += 1
            continue

        # Checks that the matched term ends in the suffix. If so, remove and then go to the next row.
        if matched_term.endswith(SUFFIXES):
            removed += 1
            continue

        # The row doesn't contain the suffix - could be a common term - so it is kept.
        kept_rows.append(row)

    # Open the output file to write the keep results.
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

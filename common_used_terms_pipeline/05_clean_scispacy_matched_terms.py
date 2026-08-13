"""
    Runs the matched terms in input file tsv through SciSpacy and checks if it is a biomedical entity. 
    Any that are not recognized get dropped. Those that are recognized will be kept in the output file tsv.
"""

import csv
import spacy

nlp = spacy.load("en_core_sci_sm")

INPUT_FILE = "04_1_clean_duplicate_matched_terms.tsv"
OUTPUT_FILE = "04_2_clean_scispacy_matched_terms.tsv"

def is_scispacy_term(term):
    doc = nlp(term)
    return len(doc.ents) > 0

# List to keep the rows
rows_to_keep = []

# Open input file to read
with open(INPUT_FILE, "r", newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter="\t")
    fieldnames = reader.fieldnames

    # Go through each row
    for row in reader:
        matched_term = row["Matched Term"].strip()

        # Keep row if SciSpaCy recognizes the matched term as a biomedical entity
        if is_scispacy_term(matched_term):
            rows_to_keep.append(row)
            continue

# Open the output file to write the results.
with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
    writer.writeheader()
    writer.writerows(rows_to_keep)

print(f"Finished. Kept {len(rows_to_keep)} rows.")

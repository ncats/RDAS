"""
    Reads the input TSV and write out a new TSV where each (GardID, Matched
    Term) pair appears at most once, regardless of which synonym it came
    from. The same matched term CAN still appear across different GardIDs;
    only repeats within the same GardID are removed.

    The first occurrence (in file order) of a given GardID/term pair is
    kept. Later duplicate rows for that same pair are dropped.
"""

import csv

# File config
INPUT_FILE = "04_1_clean_repeating_matched_terms.tsv"
OUTPUT_FILE = "04_2_clean_duplicate_matched_terms.tsv"

GARD_ID_COL = "GardID"
TERM_COL = "Matched Term"


def dedupe_terms_per_gard():
    # Lists to track unique matched_term rows, kept rows, and a count for the total number of rows
    seen_pairs = set()
    rows_kept = []
    total_rows = 0

    # Open the input file to read
    with open(INPUT_FILE, "r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile, delimiter="\t")
        fieldnames = reader.fieldnames

        # Analyze each row 
        for row in reader:
            total_rows += 1

            gard_id = row[GARD_ID_COL].strip()
            term = row[TERM_COL].strip()

            # Match up the gard_id and term for insertion purposes
            pair_key = (gard_id, term)

            # If the pair has shown up before 
            if pair_key in seen_pairs:
                # Skip if seen previously
                continue

            # Otherwise, add into the seen_pairs list
            seen_pairs.add(pair_key)
            # And add to the kept list
            rows_kept.append(row)

    # Open the output file to write the keep results in.
    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows_kept)

    total_dropped = total_rows - len(rows_kept)
    print(f"Total rows read: {total_rows}")
    print(f"Rows kept (unique GardID/term pairs): {len(rows_kept)}")
    print(f"Rows dropped (duplicate GardID/term pairs): {total_dropped}")
    print(f"Wrote deduplicated file to: {OUTPUT_FILE}")


def main():
    dedupe_terms_per_gard()


if __name__ == "__main__":
    raise SystemExit(main())

"""
    Looks through each row of the grant_gard_processed_names_sip table from rdas database, using NLTK to pick up any common terms in the Synonyms lists. 
    Stores each matched term in a row with the GardID, Name, Synonym, and Term that was matched. 
    Output is nltk_matched_terms.tsv file.
"""

import os
import sys
import csv
import re
import nltk
from nltk.corpus import words

# Adds parent directory to the path to import shared DB connection class
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from baseclass.conn import DBConnection as db

# Downloads NLTK words corpus - a list of common English words
nltk.download("words", quiet=True)

TABLE_NAME = "grant_gard_processed_names_sip"


def nltk_matching():
    # Try to establish MySQL connection
    conn = db().mysql_conn()
    if conn is None:
        raise ConnectionError("Unable to connect to MySQL.")

    # Use lowercase version of all English words in NLTK words corpus
    english_words = set(w.lower() for w in words.words())

    # Retrieve GardID, name, Synonyms for every row in the table
    select_sql = f"""
        SELECT GardID, name, Synonyms
        FROM `{TABLE_NAME}`
    """

    # Create tsv file column headers
    headers = ["GardID", "Name", "Synonym", "Matched Term"]

    read_cursor = None

    try:
        # Fetch all rows in one go. Returns the rows as dictionaries
        read_cursor = conn.cursor(buffered=True, dictionary=True)
        read_cursor.execute(select_sql)

        rows = read_cursor.fetchall()

        # Open the output tsv file to write into
        with open("nltk_matched_terms.tsv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter = "\t")
            writer.writerow(headers)

            # Process each row
            for row in rows:
                gard_id = row["GardID"]
                name = row["name"]
                # If Synonyms is NULL, get an empty string. Otherwise, Synonyms 
                synonyms_value = row.get("Synonyms") or ""

                # Separate the Synonyms list by $$$ to get the individual terms
                synonyms = [s.strip() for s in synonyms_value.split("$$$") if s.strip()]

                for synonym in synonyms:
                    # Only pick up words, no numbers or symbols
                    tokens = re.findall(r"[A-Za-z]+", synonym.lower())
                    # Skip synonyms with more than 2 words, as it is less likely to be a common term
                    if len(tokens) > 2:
                        continue

                    # Keep only words that appear in NLTK words corpus
                    matched_terms = [token for token in tokens if token in english_words]

                    # Write one row per matched term (A synonym could match multiple terms)
                    if matched_terms:
                        for matched_term in matched_terms:
                            writer.writerow([gard_id, name, synonym, matched_term])

        print("Done. Output written to nltk_matched_terms.tsv")

    except Exception as exc:
        print(f"Could not finish executing the process: {exc}")

    finally:
        if read_cursor is not None:
            read_cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()


def main():
    nltk_matching()


if __name__ == "__main__":
    raise SystemExit(main())

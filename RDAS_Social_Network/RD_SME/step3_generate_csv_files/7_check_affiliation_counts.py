#!/usr/bin/env python3
import argparse
import os
import pandas as pd

DEFAULT_AFFILIATION_CSV = "csv_files/affiliation_node.csv"


def has_location(row) -> bool:
    return any(str(row.get(k, "")).strip() for k in ("Aff_city", "Aff_state", "Aff_country", "Aff_FIPS"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Check counts of non-empty locations in affiliation_node.csv")
    parser.add_argument("--affiliation_csv", type=str, default=DEFAULT_AFFILIATION_CSV, help="Path to affiliation_node.csv")
    args = parser.parse_args()

    if not os.path.exists(args.affiliation_csv):
        print(f"❌ File not found: {args.affiliation_csv}")
        return 2

    df = pd.read_csv(args.affiliation_csv, dtype=str, keep_default_na=False)
    for col in ["author_id", "Aff_city", "Aff_state", "Aff_country", "Aff_FIPS"]:
        if col not in df.columns:
            df[col] = ""

    total_rows = len(df)
    total_authors = df["author_id"].nunique()
    nonempty_mask = df.apply(has_location, axis=1)
    nonempty_rows = int(nonempty_mask.sum())
    empty_rows = total_rows - nonempty_rows
    nonempty_authors = df.loc[nonempty_mask, "author_id"].nunique()

    print("Affiliation CSV stats")
    print("---------------------")
    print(f"Path:                 {args.affiliation_csv}")
    print(f"Total rows:           {total_rows}")
    print(f"Unique author_ids:    {total_authors}")
    print(f"Rows with location:   {nonempty_rows}")
    print(f"Rows without location:{empty_rows}")
    print(f"Authors with location:{nonempty_authors}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

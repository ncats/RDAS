import argparse
import pandas as pd
import os


def main() -> None:
    parser = argparse.ArgumentParser(description="Sum total authors from cluster_node.csv (num_authors column)")
    parser.add_argument(
        "--csv",
        type=str,
        default="step2_clustering/expertise/cluster_node.csv",
        help="Path to cluster_node.csv",
    )
    args = parser.parse_args()

    path = args.csv
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

    df = pd.read_csv(path)
    if "num_authors" not in df.columns:
        raise ValueError("Column 'num_authors' not found in the CSV.")

    num_authors_series = pd.to_numeric(df["num_authors"], errors="coerce").fillna(0)
    total_authors = int(num_authors_series.sum())
    num_clusters = len(df)

    print(f"File: {path}")
    print(f"Clusters counted: {num_clusters}")
    print(f"Total authors (sum of 'num_authors'): {total_authors}")


if __name__ == "__main__":
    main()

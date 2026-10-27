import os
import argparse
import pandas as pd


def _is_nonempty(val) -> bool:
    if pd.isna(val):
        return False
    s = str(val).strip()
    return s not in ("", "nan", "None")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate clustering relationships CSVs from expertise_relationships.csv")
    parser.add_argument(
        "--expertise-rel",
        type=str,
        default="step2_clustering/expertise/expertise_relationships.csv",
        help="Path to expertise_relationships.csv generated in step 3",
    )
    parser.add_argument(
        "--expertise-rel-keyword",
        type=str,
        default="",
        help="Optional path to expertise_relationships_keyword.csv for keyword-based relationships",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="csv_files",
        help="Directory to write relationship CSV files",
    )
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    df = pd.read_csv(args.expertise_rel)

    # Normalize columns expected: entity_id, cluster_id, gard_id, cluster_label, author_id, relationship_type
    required_cols = {"entity_id", "cluster_id", "author_id"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in {args.expertise_rel}: {missing}")

    # Cluster -> Author (AUTHOR_BELONGS_TO_CLUSTER)
    ca_rows = []
    for _, r in df.iterrows():
        cluster_id = r.get("cluster_id")
        author_id = r.get("author_id")
        if _is_nonempty(cluster_id) and _is_nonempty(author_id):
            ca_rows.append(
                {
                    "cluster_id": str(cluster_id).strip(),
                    "author_id": str(author_id).strip(),
                    "relationship": "AUTHOR_BELONGS_TO_CLUSTER",
                }
            )
    if ca_rows:
        ca_df = pd.DataFrame(ca_rows).drop_duplicates()
    else:
        ca_df = pd.DataFrame(columns=["cluster_id", "author_id", "relationship"])
    ca_path = os.path.join(args.output_dir, "cluster_author_relationships.csv")
    ca_df.to_csv(ca_path, index=False)
    print(f"Wrote {len(ca_df)} rows -> {ca_path}")

    # Cluster -> Expertise (CLUSTER_HAS_EXPERTISE)
    ce_rows = []
    for _, r in df.iterrows():
        cluster_id = r.get("cluster_id")
        entity_id = r.get("entity_id")
        # Only clusters (skip individuals: cluster_id empty)
        if _is_nonempty(cluster_id) and _is_nonempty(entity_id):
            ce_rows.append(
                {
                    "cluster_id": str(cluster_id).strip(),
                    "entity_id": str(entity_id).strip(),
                    "relationship": "CLUSTER_HAS_EXPERTISE",
                }
            )
    if ce_rows:
        ce_df = pd.DataFrame(ce_rows).drop_duplicates()
    else:
        ce_df = pd.DataFrame(columns=["cluster_id", "entity_id", "relationship"])
    ce_path = os.path.join(args.output_dir, "cluster_expertise_relationships.csv")
    ce_df.to_csv(ce_path, index=False)
    print(f"Wrote {len(ce_df)} rows -> {ce_path}")

    # Author -> Expertise (AUTHOR_HAS_EXPERTISE) - includes both clusters and individuals
    ae_rows = []
    for _, r in df.iterrows():
        author_id = r.get("author_id")
        entity_id = r.get("entity_id")
        if _is_nonempty(author_id) and _is_nonempty(entity_id):
            ae_rows.append(
                {
                    "author_id": str(author_id).strip(),
                    "entity_id": str(entity_id).strip(),
                    "relationship": "AUTHOR_HAS_EXPERTISE",
                }
            )
    if ae_rows:
        ae_df = pd.DataFrame(ae_rows).drop_duplicates()
    else:
        ae_df = pd.DataFrame(columns=["author_id", "entity_id", "relationship"])
    ae_path = os.path.join(args.output_dir, "author_expertise_relationships.csv")
    ae_df.to_csv(ae_path, index=False)
    print(f"Wrote {len(ae_df)} rows -> {ae_path}")

    # Optional keyword-based expertise relationships
    if args.expertise_rel_keyword:
        kw_path = args.expertise_rel_keyword
        if not os.path.exists(kw_path):
            raise FileNotFoundError(f"Keyword expertise relationships not found: {kw_path}")
        kw_df = pd.read_csv(kw_path, dtype=str)

        required_kw_cols = {"author_id", "expertise_id"}
        missing_kw = required_kw_cols - set(kw_df.columns)
        if missing_kw:
            raise ValueError(f"Missing required columns in {kw_path}: {missing_kw}")

        has_rank = "expertise_rank" in kw_df.columns
        has_rel_type = "relationship_type" in kw_df.columns

        # Cluster -> Expertise (keyword)
        ce_kw_rows = []
        for _, r in kw_df.iterrows():
            cluster_id = r.get("cluster_id")
            expertise_id = r.get("expertise_id")
            rel_type = str(r.get("relationship_type") or "").strip() if has_rel_type else ""
            if rel_type == "INDIVIDUAL_EXPERTISE":
                continue
            if _is_nonempty(cluster_id) and _is_nonempty(expertise_id):
                row = {
                    "cluster_id": str(cluster_id).strip(),
                    "expertise_id": str(expertise_id).strip(),
                    "relationship": "CLUSTER_HAS_EXPERTISE",
                }
                if has_rank:
                    row["expertise_rank"] = r.get("expertise_rank")
                ce_kw_rows.append(row)
        if ce_kw_rows:
            ce_kw_df = pd.DataFrame(ce_kw_rows)
            if has_rank:
                ce_kw_df["expertise_rank"] = pd.to_numeric(ce_kw_df["expertise_rank"], errors="coerce")
                ce_kw_df = (
                    ce_kw_df.groupby(["cluster_id", "expertise_id", "relationship"], as_index=False)
                    .agg({"expertise_rank": "min"})
                )
            else:
                ce_kw_df = ce_kw_df.drop_duplicates()
        else:
            ce_kw_df = pd.DataFrame(columns=["cluster_id", "expertise_id", "relationship"] + (["expertise_rank"] if has_rank else []))
        ce_kw_out = os.path.join(args.output_dir, "cluster_expertise_relationships_keyword.csv")
        ce_kw_df.to_csv(ce_kw_out, index=False)
        print(f"Wrote {len(ce_kw_df)} rows -> {ce_kw_out}")

        # Author -> Expertise (keyword)
        ae_kw_rows = []
        for _, r in kw_df.iterrows():
            author_id = r.get("author_id")
            expertise_id = r.get("expertise_id")
            if _is_nonempty(author_id) and _is_nonempty(expertise_id):
                row = {
                    "author_id": str(author_id).strip(),
                    "expertise_id": str(expertise_id).strip(),
                    "relationship": "AUTHOR_HAS_EXPERTISE",
                }
                if has_rank:
                    row["expertise_rank"] = r.get("expertise_rank")
                ae_kw_rows.append(row)
        if ae_kw_rows:
            ae_kw_df = pd.DataFrame(ae_kw_rows)
            if has_rank:
                ae_kw_df["expertise_rank"] = pd.to_numeric(ae_kw_df["expertise_rank"], errors="coerce")
                ae_kw_df = (
                    ae_kw_df.groupby(["author_id", "expertise_id", "relationship"], as_index=False)
                    .agg({"expertise_rank": "min"})
                )
            else:
                ae_kw_df = ae_kw_df.drop_duplicates()
        else:
            ae_kw_df = pd.DataFrame(columns=["author_id", "expertise_id", "relationship"] + (["expertise_rank"] if has_rank else []))
        ae_kw_out = os.path.join(args.output_dir, "author_expertise_relationships_keyword.csv")
        ae_kw_df.to_csv(ae_kw_out, index=False)
        print(f"Wrote {len(ae_kw_df)} rows -> {ae_kw_out}")


if __name__ == "__main__":
    main()

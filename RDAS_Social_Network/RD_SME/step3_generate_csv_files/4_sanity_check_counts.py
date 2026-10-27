import os
import argparse
import pandas as pd


def load_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        print(f"[WARN] Missing file: {path}")
        return pd.DataFrame()
    return pd.read_csv(path)


def count_and_print(label: str, df: pd.DataFrame) -> int:
    n = len(df)
    print(f"{label}: {n}")
    return n


def main() -> None:
    parser = argparse.ArgumentParser(description="Sanity check counts for nodes and relationship CSVs")
    parser.add_argument("--csv-dir", type=str, default="csv_files", help="Directory containing generated CSVs")
    # Node CSVs (from step1 JSON conversions)
    parser.add_argument("--gard-csv", type=str, default="gard_node_GARD_0006233.csv")
    parser.add_argument("--articles-csv", type=str, default="articles_GARD_0006233.csv")
    parser.add_argument("--grants-csv", type=str, default="grants_GARD_0006233.csv")
    parser.add_argument("--trials-csv", type=str, default="clinical_trials_GARD_0006233.csv")
    parser.add_argument("--authors-csv", type=str, default="authors_GARD_0006233.csv")
    # Relationship CSVs (from relationship generators)
    parser.add_argument("--gard-article-rel", type=str, default="gard_article_relationships.csv")
    parser.add_argument("--gard-grant-rel", type=str, default="gard_grant_relationships.csv")
    parser.add_argument("--gard-trial-rel", type=str, default="gard_trial_relationships.csv")
    parser.add_argument("--author-article-rel", type=str, default="author_article_relationships.csv")
    parser.add_argument("--author-grant-rel", type=str, default="author_grant_relationships.csv")
    parser.add_argument("--author-trial-rel", type=str, default="author_trial_relationships.csv")
    parser.add_argument("--author-collab-rel", type=str, default="author_collaborations_relationships.csv")
    # Optional clustering relationships
    parser.add_argument("--cluster-author-rel", type=str, default="cluster_author_relationships.csv")
    parser.add_argument("--cluster-expertise-rel", type=str, default="cluster_expertise_relationships.csv")
    parser.add_argument("--author-expertise-rel", type=str, default="author_expertise_relationships.csv")
    args = parser.parse_args()

    d = args.csv_dir
    # Load nodes
    gard_df = load_csv(os.path.join(d, args.gard_csv))
    art_df = load_csv(os.path.join(d, args.articles_csv))
    grant_df = load_csv(os.path.join(d, args.grants_csv))
    trial_df = load_csv(os.path.join(d, args.trials_csv))
    auth_df = load_csv(os.path.join(d, args.authors_csv))

    print("=== Node Counts ===")
    n_gard = count_and_print("Gard nodes", gard_df)
    n_articles = count_and_print("Article nodes", art_df)
    n_grants = count_and_print("Grant nodes", grant_df)
    n_trials = count_and_print("ClinicalTrial nodes", trial_df)
    n_authors = count_and_print("Author nodes", auth_df)

    # Load relationships
    gard_art_rel = load_csv(os.path.join(d, args.gard_article_rel))
    gard_grant_rel = load_csv(os.path.join(d, args.gard_grant_rel))
    gard_trial_rel = load_csv(os.path.join(d, args.gard_trial_rel))
    aa_rel = load_csv(os.path.join(d, args.author_article_rel))
    ag_rel = load_csv(os.path.join(d, args.author_grant_rel))
    at_rel = load_csv(os.path.join(d, args.author_trial_rel))
    ac_rel = load_csv(os.path.join(d, args.author_collab_rel))
    ca_rel = load_csv(os.path.join(d, args.cluster_author_rel))
    ce_rel = load_csv(os.path.join(d, args.cluster_expertise_rel))
    ae_rel = load_csv(os.path.join(d, args.author_expertise_rel))

    print("\n=== Relationship Counts ===")
    rn_gard_art = count_and_print("GARD_HAS_ARTICLE", gard_art_rel)
    rn_gard_grant = count_and_print("GARD_HAS_GRANT", gard_grant_rel)
    rn_gard_trial = count_and_print("GARD_HAS_CLINICAL_TRIAL", gard_trial_rel)
    rn_author_article = count_and_print("AUTHOR->ARTICLE (AUTHORED_ARTICLE)", aa_rel)
    rn_author_grant = count_and_print("AUTHOR->GRANT (ASSOCIATED_WITH_GRANT)", ag_rel)
    rn_author_trial = count_and_print("AUTHOR->TRIAL (ASSOCIATED_WITH_TRIAL)", at_rel)
    rn_author_collab = count_and_print("AUTHOR<->AUTHOR (COLLABORATES_WITH)", ac_rel)
    rn_cluster_author = count_and_print("CLUSTER->AUTHOR (AUTHOR_BELONGS_TO_CLUSTER)", ca_rel)
    rn_cluster_expertise = count_and_print("CLUSTER->EXPERTISE (CLUSTER_HAS_EXPERTISE)", ce_rel)
    rn_author_expertise = count_and_print("AUTHOR->EXPERTISE (AUTHOR_HAS_EXPERTISE)", ae_rel)

    # Checks: Since we have one GARD instance, gard-* relationship counts should equal node counts (unique IDs).
    print("\n=== Equality Checks (one GARD) ===")
    try:
        unique_pubmed = art_df["pubmed_id"].astype(str).str.strip().nunique()
        print(f"Articles unique pubmed_id: {unique_pubmed} vs GARD_HAS_ARTICLE rows: {rn_gard_art} -> {'OK' if unique_pubmed == rn_gard_art else 'MISMATCH'}")
    except Exception:
        print("Articles CSV missing pubmed_id for equality check.")
    try:
        unique_app = grant_df["application_id"].astype(str).str.strip().nunique()
        print(f"Grants unique application_id: {unique_app} vs GARD_HAS_GRANT rows: {rn_gard_grant} -> {'OK' if unique_app == rn_gard_grant else 'MISMATCH'}")
    except Exception:
        print("Grants CSV missing application_id for equality check.")
    try:
        unique_nctid = trial_df["nctid"].astype(str).str.strip().nunique()
        print(f"Trials unique nctid: {unique_nctid} vs GARD_HAS_CLINICAL_TRIAL rows: {rn_gard_trial} -> {'OK' if unique_nctid == rn_gard_trial else 'MISMATCH'}")
    except Exception:
        print("Trials CSV missing nctid for equality check.")

    # Referential integrity checks for author relationships
    print("\n=== Referential Integrity Checks ===")
    def nonempty_unique(series, name):
        try:
            return set(series.dropna().astype(str).str.strip().replace({"": None}).dropna().unique())
        except Exception:
            print(f"[WARN] Could not read {name}.")
            return set()

    article_ids = nonempty_unique(art_df.get("pubmed_id", pd.Series(dtype=str)), "Article pubmed_id")
    grant_ids = nonempty_unique(grant_df.get("application_id", pd.Series(dtype=str)), "Grant application_id")
    trial_ids = nonempty_unique(trial_df.get("nctid", pd.Series(dtype=str)), "Trial nctid")
    author_ids = nonempty_unique(auth_df.get("author_id", pd.Series(dtype=str)), "Author author_id")

    def check_pairs(rel_df, left_col, left_set, right_col, right_set, label):
        if rel_df.empty:
            print(f"{label}: 0 rows (skipped)")
            return
        left_missing = [(i, v) for i, v in enumerate(rel_df[left_col].astype(str).str.strip(), 1) if v not in left_set]
        right_missing = [(i, v) for i, v in enumerate(rel_df[right_col].astype(str).str.strip(), 1) if v not in right_set]
        print(f"{label}: missing left={len(left_missing)}, missing right={len(right_missing)}")
        if left_missing[:3]:
            print(f"  Examples missing left ({left_col}): {left_missing[:3]}")
        if right_missing[:3]:
            print(f"  Examples missing right ({right_col}): {right_missing[:3]}")

    check_pairs(aa_rel, "author_id", author_ids, "pubmed_id", article_ids, "AUTHOR->ARTICLE")
    check_pairs(ag_rel, "author_id", author_ids, "application_id", grant_ids, "AUTHOR->GRANT")
    check_pairs(at_rel, "author_id", author_ids, "nctid", trial_ids, "AUTHOR->TRIAL")

    # Collaborations sanity
    if not ac_rel.empty:
        dup_pairs = ac_rel.assign(p1=ac_rel["author_id_1"].astype(str).str.strip(), p2=ac_rel["author_id_2"].astype(str).str.strip())
        dup_pairs["key"] = dup_pairs.apply(lambda r: tuple(sorted((r["p1"], r["p2"]))), axis=1)
        dups = len(dup_pairs["key"]) - dup_pairs["key"].nunique()
        print(f"\nCollaborations duplicate pairs (should be 0): {dups}")

    print("\n=== Direction Guidance for Memgraph ===")
    print("Direction is set when loading CSVs into Memgraph via Cypher:")
    print("  MATCH (a:Author {author_id: $author_id}), (p:Article {pubmed_id: $pubmed_id})")
    print("  MERGE (a)-[:AUTHORED_ARTICLE]->(p);")
    print("Similarly for GARD relationships:")
    print("  MATCH (g:GARD {gard_id: $gard_id}), (p:Article {pubmed_id: $pubmed_id})")
    print("  MERGE (g)-[:GARD_HAS_ARTICLE]->(p);")
    print("You can verify after import with counts like:")
    print("  MATCH (:Author)-[:AUTHORED_ARTICLE]->(:Article) RETURN count(*);")
    print("  MATCH (:GARD)-[:GARD_HAS_ARTICLE]->(:Article) RETURN count(*);")


if __name__ == "__main__":
    main()

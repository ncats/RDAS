import os
import json
import ast
from typing import Dict, Optional, Tuple, List

import pandas as pd

OUTPUT_DIR = "data/processed/figures"
CLUSTERS_CSV = "data/external/researchers_per_cluster.csv"
EXPERTISE_CSV = "data/processed/csv_files/expertise_node.csv"
REL_CSV = "data/processed/csv_files/author_expertise_relationships.csv"
TARGET_CLUSTER_ID = "GARD_0006233_Cluster_2649"


def parse_expertise_mapping(value: str) -> Optional[dict]:
    """
    Parse the 'expertise' column payload which looks like a JSON object
    containing keys such as 'expertise_1', 'expertise_2', etc.
    """
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.lower() == "nan":
        return None
    # First try strict JSON
    try:
        return json.loads(text)
    except Exception:
        pass
    # Fall back to Python-literal parsing for slightly malformed JSON
    try:
        parsed = ast.literal_eval(text)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    return None


def extract_expertise_terms_for_cluster(cluster_id: str) -> Tuple[List[Tuple[str, int]], int]:
    """
    For the target cluster_id, extract all expertise_n terms with rank-based weights.
    Returns:
      - list of (term, weight)
      - researchers count for the cluster (for annotation)
    """
    # Load researchers per cluster to fetch researcher count and verify existence
    clusters = pd.read_csv(CLUSTERS_CSV)
    clusters["cluster_id_norm"] = clusters["cluster_id"].astype(str).str.casefold()
    cid_norm = cluster_id.casefold()
    clusters = clusters[clusters["cluster_id_norm"] == cid_norm].copy()
    if clusters.empty:
        raise ValueError(f"Target cluster_id not found: {cluster_id}")
    clusters["researchers"] = pd.to_numeric(clusters["researchers"], errors="coerce").fillna(0).astype(int)
    researchers_in_cluster = int(clusters["researchers"].iloc[0])

    # Load expertise and parse JSON for the specific entity_id
    expertise_df = pd.read_csv(EXPERTISE_CSV)
    if "entity_id" not in expertise_df.columns or "expertise" not in expertise_df.columns:
        raise ValueError("expertise_node.csv must include 'entity_id' and 'expertise' columns.")
    expertise_df["entity_id_norm"] = expertise_df["entity_id"].astype(str).str.casefold()
    row = expertise_df.loc[expertise_df["entity_id_norm"] == cid_norm]
    if row.empty:
        raise ValueError(f"No expertise entry found for cluster_id {cluster_id}")
    mapping = parse_expertise_mapping(row["expertise"].iloc[0])
    if not mapping:
        raise ValueError(f"Empty or invalid expertise mapping for cluster_id {cluster_id}")

    # Collect all expertise_i terms with descending weights by rank (expertise_1 > expertise_2 > ...)
    items: List[Tuple[str, int]] = []
    for key, value in mapping.items():
        if not key.startswith("expertise_"):
            continue
        term = (value or "").strip()
        if not term:
            continue
        try:
            rank = int(key.split("_", 1)[1])
        except Exception:
            rank = 99
        weight = max(1, 4 - rank)  # expertise_1:3, _2:2, _3:1, others:1
        items.append((term, weight))
    if not items:
        raise ValueError(f"No expertise terms found for cluster_id {cluster_id}")
    return items, researchers_in_cluster


def extract_individual_terms_for_cluster(cluster_id: str) -> List[Tuple[str, int]]:
    """
    Aggregate per-researcher expertise terms for all authors linked to the cluster.
    Uses author_expertise_relationships.csv to find member authors and
    expertise_node.csv rows where entity_type == 'individual' for those authors.
    """
    # Load relationships
    rel = pd.read_csv(REL_CSV, dtype=str)
    for col in ["author_id", "entity_id"]:
        if col not in rel.columns:
            raise ValueError("author_expertise_relationships.csv must include 'author_id' and 'entity_id'.")
    rel["entity_id_norm"] = rel["entity_id"].astype(str).str.casefold()
    cid_norm = cluster_id.casefold()
    members = rel.loc[rel["entity_id_norm"] == cid_norm, "author_id"].dropna().astype(str).unique().tolist()
    if not members:
        return []

    # Load expertise_node for individuals
    exp = pd.read_csv(EXPERTISE_CSV, dtype=str)
    if "entity_type" not in exp.columns or "entity_id" not in exp.columns or "expertise" not in exp.columns:
        raise ValueError("expertise_node.csv must include 'entity_type', 'entity_id', and 'expertise'.")
    exp["entity_type_norm"] = exp["entity_type"].astype(str).str.casefold()
    exp["entity_id_norm"] = exp["entity_id"].astype(str).str.casefold()
    exp_ind = exp[(exp["entity_type_norm"] == "individual") & (exp["entity_id"].isin(members))].copy()
    if exp_ind.empty:
        return []

    items: List[Tuple[str, int]] = []
    for _, row in exp_ind.iterrows():
        mapping = parse_expertise_mapping(row["expertise"])
        if not mapping:
            continue
        for key, value in mapping.items():
            if not key.startswith("expertise_"):
                continue
            term = (value or "").strip()
            if not term:
                continue
            try:
                rank = int(key.split("_", 1)[1])
            except Exception:
                rank = 99
            weight = max(1, 4 - rank)  # 3,2,1,...
            items.append((term, weight))
    return items


def build_dual_frequencies(terms_with_weights: List[Tuple[str, int]]) -> Tuple[Dict[str, float], Dict[str, float]]:
    """
    Build two frequency dicts:
      - individual view: original terms with their rank weights
      - grouped view: normalized terms (lowercased/punctuation stripped) aggregated
    """
    import re
    individual: Dict[str, float] = {}
    grouped: Dict[str, float] = {}
    for term, w in terms_with_weights:
        individual[term] = individual.get(term, 0.0) + float(w)
        norm = re.sub(r"[^a-z0-9]+", " ", term.lower()).strip()
        grouped[norm] = grouped.get(norm, 0.0) + float(w)
    # small boost to expertise_1 if present (already handled in weight)
    return individual, grouped


def ensure_wordcloud_available():
    try:
        import wordcloud  # noqa: F401
    except Exception:
        raise RuntimeError(
            "The 'wordcloud' package is required. Install with:\n"
            "  conda install -c conda-forge wordcloud\n"
            "or\n"
            "  pip install wordcloud"
        )


def generate_and_save_dual_clouds(cluster_id: str, individual_view: Dict[str, float], group_view: Dict[str, float], researchers_in_cluster: int) -> None:
    from wordcloud import WordCloud
    import matplotlib.pyplot as plt
    import seaborn as sns

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    sns.set_theme(style="white")

    # Prepare two wordclouds
    wc_kwargs = dict(
        width=1400,
        height=1000,
        background_color="white",
        prefer_horizontal=1.0,
        colormap="viridis",
        collocations=False,
        normalize_plurals=False,
        min_font_size=8,
        max_words=5000,
    )
    wc_ind = WordCloud(**wc_kwargs).generate_from_frequencies(individual_view)
    wc_grp = WordCloud(**wc_kwargs).generate_from_frequencies(group_view)

    title = f"Expertise for {cluster_id} (N={researchers_in_cluster} researchers)"

    # Two-panel figure
    fig, axes = plt.subplots(1, 2, figsize=(16, 8), dpi=200, constrained_layout=True)
    axes[0].imshow(wc_ind, interpolation="bilinear")
    axes[0].axis("off")
    axes[0].set_title("Individual Expertise (all researchers, rank-weighted)", fontsize=12)
    axes[1].imshow(wc_grp, interpolation="bilinear")
    axes[1].axis("off")
    axes[1].set_title("Cluster Expertise (group-level fields)", fontsize=12)
    fig.suptitle(title, fontsize=14)

    base = f"cluster_{cluster_id}_expertise_dual"
    out_png = os.path.join(OUTPUT_DIR, f"{base}.png")
    out_svg = os.path.join(OUTPUT_DIR, f"{base}.svg")
    fig.savefig(out_png, bbox_inches="tight")
    fig.savefig(out_svg, bbox_inches="tight")
    plt.close(fig)

    # Also export a simple CSV of frequencies for transparency
    ind_csv = os.path.join(OUTPUT_DIR, f"{base}_individual_freq.csv")
    grp_csv = os.path.join(OUTPUT_DIR, f"{base}_grouped_freq.csv")
    pd.DataFrame(sorted(individual_view.items(), key=lambda x: -x[1]), columns=["term", "weight"]).to_csv(ind_csv, index=False)
    pd.DataFrame(sorted(group_view.items(), key=lambda x: -x[1]), columns=["term", "weight"]).to_csv(grp_csv, index=False)

    print(f"Wrote:\n  {out_png}\n  {out_svg}\n  {ind_csv}\n  {grp_csv}")


def main() -> None:
    ensure_wordcloud_available()
    # Group-level (cluster) terms
    group_terms, researchers_in_cluster = extract_expertise_terms_for_cluster(TARGET_CLUSTER_ID)
    group_view, _ = build_dual_frequencies(group_terms)  # keep original terms for right panel
    # Individual aggregated terms
    individual_terms = extract_individual_terms_for_cluster(TARGET_CLUSTER_ID)
    if not individual_terms:
        # fall back to group-only if no individuals found
        individual_view, _ = build_dual_frequencies(group_terms)
    else:
        individual_view, _ = build_dual_frequencies(individual_terms)
    generate_and_save_dual_clouds(TARGET_CLUSTER_ID, individual_view, group_view, researchers_in_cluster)


if __name__ == "__main__":
    main()

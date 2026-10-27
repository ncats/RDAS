import os
import io
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

OUTPUT_DIR = "data/processed/figures"
# Inputs
CLUSTER_ID = "GARD_0006233_Cluster_2649"
REL_CSV = "data/processed/csv_files/author_expertise_relationships.csv"
EXPERTISE_CSV = "data/processed/csv_files/expertise_node.csv"
AFFILIATION_CSV = "data/processed/csv_files/affiliation_node.csv"
USCITIES_CSV_CANDIDATES = [
    "data/external/uscities.csv",
    "data/external/uscities.csv",
]
RESEARCHERS_PER_CLUSTER = "data/external/researchers_per_cluster.csv"


def parse_expertise_mapping(value: str) -> Dict[str, str]:
    import json, ast
    if value is None:
        return {}
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return {}
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        pass
    try:
        obj = ast.literal_eval(text)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def ensure_us_county_centroids() -> pd.DataFrame:
    cache_csv = os.path.join(os.path.dirname(OUTPUT_DIR), "us_county_centroids.csv")
    if os.path.exists(cache_csv):
        return pd.read_csv(cache_csv, dtype={"GEOID": str})
    # derive from uscities
    for path in USCITIES_CSV_CANDIDATES:
        if os.path.exists(path):
            tmp = pd.read_csv(path)
            cols = {c.lower(): c for c in tmp.columns}
            required = {"county_fips", "lat", "lng"}
            if required.issubset(set(cols.keys())):
                fips_col = cols["county_fips"]; lat_col = cols["lat"]; lon_col = cols["lng"]
                pop_col = cols.get("population")
                df = tmp[[fips_col, lat_col, lon_col] + ([pop_col] if pop_col else [])].copy()
                df = df.rename(columns={fips_col: "GEOID", lon_col: "lon", lat_col: "lat"})
                df["GEOID"] = df["GEOID"].astype(str).str.zfill(5)
                if pop_col:
                    df[pop_col] = pd.to_numeric(df[pop_col], errors="coerce").fillna(0.0)
                    grouped = df.groupby("GEOID", as_index=False).apply(
                        lambda g: pd.Series({
                            "lon": np.average(g["lon"], weights=g[pop_col]) if g[pop_col].sum() > 0 else g["lon"].mean(),
                            "lat": np.average(g["lat"], weights=g[pop_col]) if g[pop_col].sum() > 0 else g["lat"].mean(),
                        })
                    )
                    centroids = grouped.reset_index(drop=True)
                else:
                    centroids = df.groupby("GEOID", as_index=False)[["lon", "lat"]].mean()
                centroids.to_csv(cache_csv, index=False)
                return centroids
    raise RuntimeError("Could not load county centroids (need uscities.csv).")


def draw_state_outlines(ax: plt.Axes) -> None:
    # Download a small states geojson if not cached
    states_cache = os.path.join(os.path.dirname(OUTPUT_DIR), "us_states.geojson")
    if not os.path.exists(states_cache):
        try:
            import requests
            urls = [
                "https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json",
                "https://raw.githubusercontent.com/python-visualization/folium/master/examples/data/us-states.json",
            ]
            for url in urls:
                r = requests.get(url, timeout=30)
                if r.status_code == 200 and len(r.text) > 1000:
                    with open(states_cache, "w", encoding="utf-8") as f:
                        f.write(r.text)
                    break
        except Exception:
            pass
    # Draw outlines
    try:
        import json
        with open(states_cache, "r", encoding="utf-8") as f:
            data = json.load(f)
        exclude_ids = {"AK", "HI", "PR", "GU", "VI", "MP", "AS"}
        features: List[dict] = data.get("features", [])
        for feat in features:
            geom = (feat.get("geometry") or {})
            props = feat.get("properties") or {}
            st_id = (feat.get("id") or props.get("state_code") or props.get("STATE") or props.get("STATE_ABBR"))
            if isinstance(st_id, str) and st_id in exclude_ids:
                continue
            gtype = geom.get("type"); coords = geom.get("coordinates", [])
            if gtype == "Polygon":
                for ring in coords:
                    if not ring: continue
                    xs = [float(x) for x, y in ring]; ys = [float(y) for x, y in ring]
                    ax.plot(xs, ys, color="lightgray", linewidth=0.4, alpha=0.9, zorder=0.5)
            elif gtype == "MultiPolygon":
                for poly in coords:
                    for ring in poly:
                        if not ring: continue
                        xs = [float(x) for x, y in ring]; ys = [float(y) for x, y in ring]
                        ax.plot(xs, ys, color="lightgray", linewidth=0.4, alpha=0.9, zorder=0.5)
    except Exception:
        pass


def aggregate_individual_expertise(cluster_id: str) -> pd.DataFrame:
    rel = pd.read_csv(REL_CSV, dtype=str)
    rel["entity_id_norm"] = rel["entity_id"].astype(str).str.casefold()
    cid_norm = cluster_id.casefold()
    members = rel.loc[rel["entity_id_norm"] == cid_norm, "author_id"].dropna().astype(str).unique().tolist()
    if not members:
        return pd.DataFrame(columns=["term", "weight"])
    exp = pd.read_csv(EXPERTISE_CSV, dtype=str)
    exp["entity_type_norm"] = exp["entity_type"].astype(str).str.casefold()
    exp_ind = exp[(exp["entity_type_norm"] == "individual") & (exp["entity_id"].isin(members))].copy()
    items: List[Tuple[str, int]] = []
    for _, r in exp_ind.iterrows():
        m = parse_expertise_mapping(r["expertise"])
        if not m: continue
        for k, v in m.items():
            if not k.startswith("expertise_"): continue
            t = (v or "").strip()
            if not t: continue
            try: rank = int(k.split("_", 1)[1])
            except Exception: rank = 99
            w = max(1, 4 - rank)
            items.append((t, w))
    if not items:
        return pd.DataFrame(columns=["term", "weight"])
    df = pd.DataFrame(items, columns=["term", "weight"])
    agg = df.groupby("term", as_index=False)["weight"].sum().sort_values("weight", ascending=False)
    return agg


def map_members_to_fips(cluster_id: str) -> pd.DataFrame:
    rel = pd.read_csv(REL_CSV, dtype=str)
    rel["entity_id_norm"] = rel["entity_id"].astype(str).str.casefold()
    cid_norm = cluster_id.casefold()
    members = rel.loc[rel["entity_id_norm"] == cid_norm, "author_id"].dropna().astype(str).unique().tolist()
    if not members:
        return pd.DataFrame(columns=["fips", "count"])
    aff = pd.read_csv(AFFILIATION_CSV, dtype=str, usecols=["author_id", "Aff_FIPS"])
    aff["Aff_FIPS"] = aff["Aff_FIPS"].fillna("").astype(str).str.strip()
    aff = aff[aff["author_id"].isin(members)]
    aff = aff[aff["Aff_FIPS"].str.fullmatch(r"\d{5}", na=False)]
    if aff.empty:
        return pd.DataFrame(columns=["fips", "count"])
    counts = aff.groupby("Aff_FIPS").size().reset_index(name="count").rename(columns={"Aff_FIPS": "fips"})
    return counts.sort_values("count", ascending=False)


def get_cluster_metadata(cluster_id: str) -> Dict[str, str]:
    meta: Dict[str, str] = {"cluster_id": cluster_id}
    # size
    try:
        cl = pd.read_csv(RESEARCHERS_PER_CLUSTER)
        cl["cluster_id_norm"] = cl["cluster_id"].astype(str).str.casefold()
        row = cl.loc[cl["cluster_id_norm"] == cluster_id.casefold()]
        if not row.empty:
            meta["researchers"] = str(int(pd.to_numeric(row["researchers"], errors="coerce").fillna(0).iloc[0]))
    except Exception:
        meta["researchers"] = "NA"
    # top cluster expertise
    try:
        exp = pd.read_csv(EXPERTISE_CSV, dtype=str)
        row = exp.loc[(exp["entity_type"].astype(str).str.casefold() == "cluster") &
                      (exp["entity_id"].astype(str).str.casefold() == cluster_id.casefold())]
        if not row.empty:
            m = parse_expertise_mapping(row["expertise"].iloc[0])
            tops = [m.get(f"expertise_{i}") for i in range(1, 4)]
            meta["cluster_expertise_top3"] = ", ".join([t for t in tops if t])
    except Exception:
        pass
    return meta


def scale_sizes(values: pd.Series, vmin: float, vmax: float) -> pd.Series:
    if values.empty: return values
    lo, hi = float(values.min()), float(values.max())
    if lo == hi: return pd.Series(np.full(len(values), (vmin + vmax) / 2.0), index=values.index)
    return pd.Series(np.interp(values.astype(float), (lo, hi), (vmin, vmax)), index=values.index)


def build_figure(cluster_id: str) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    sns.set_theme(style="whitegrid")
    plt.rcParams["figure.dpi"] = 200

    # Data
    terms = aggregate_individual_expertise(cluster_id)
    fips_counts = map_members_to_fips(cluster_id)
    meta = get_cluster_metadata(cluster_id)
    centroids = ensure_us_county_centroids()
    coords = centroids.rename(columns={"GEOID": "fips"})
    coords = coords[coords["fips"].isin(fips_counts["fips"].unique() if not fips_counts.empty else [])]

    # Layout: 1x2 main blocks; right column split into bars + stats
    fig = plt.figure(figsize=(18, 9))
    gs = fig.add_gridspec(2, 2, width_ratios=[1.2, 1], height_ratios=[2, 1], hspace=0.25, wspace=0.15)
    ax_map = fig.add_subplot(gs[:, 0])
    ax_bar = fig.add_subplot(gs[0, 1])
    ax_stats = fig.add_subplot(gs[1, 1])
    for ax in [ax_map, ax_bar, ax_stats]:
        ax.set_facecolor("white")

    # Panel A: US map with members by county
    ax_map.set_xlim(-125, -66.5); ax_map.set_ylim(24, 49.5)
    ax_map.grid(alpha=0.2, linewidth=0.3)
    draw_state_outlines(ax_map)
    if not fips_counts.empty and not coords.empty:
        plot_df = fips_counts.merge(coords, on="fips", how="inner")
        plot_df["size"] = scale_sizes(plot_df["count"], 40, 900)
        sc = ax_map.scatter(plot_df["lon"], plot_df["lat"], s=plot_df["size"], c="#8b0000", alpha=0.85,
                            edgecolors="black", linewidths=0.4, zorder=3)
        # Label top 10 hotspots
        top_hot = plot_df.sort_values("count", ascending=False).head(10)
        for _, r in top_hot.iterrows():
            ax_map.text(r["lon"], r["lat"], str(int(r["count"])), fontsize=7.5, ha="center", va="center",
                        color="white", zorder=4)
    ax_map.set_title(f"Geographic footprint of {cluster_id} members")
    ax_map.set_xlabel("Longitude"); ax_map.set_ylabel("Latitude")

    # Panel B: Top expertise terms (individual aggregated)
    if not terms.empty:
        top_k = terms.head(15).iloc[::-1]  # reverse for horizontal bar
        ax_bar.barh(top_k["term"], top_k["weight"], color="#4c78a8", alpha=0.9)
        ax_bar.set_xlabel("Weighted frequency (expertise_1 > _2 > _3)")
        ax_bar.set_title("Top expertise among cluster members")
    else:
        ax_bar.text(0.5, 0.5, "No individual expertise found", ha="center", va="center")
        ax_bar.axis("off")

    # Panel C: Stats box
    ax_stats.axis("off")
    text_lines = [
        f"Cluster: {cluster_id}",
        f"Researchers: {meta.get('researchers', 'NA')}",
        f"Members with US FIPS: {0 if fips_counts.empty else int(fips_counts['count'].sum())}",
        f"Unique counties: {0 if fips_counts.empty else fips_counts.shape[0]}",
        f"Unique expertise terms (individual): {int(terms.shape[0])}",
        f"Cluster expertise (top-3): {meta.get('cluster_expertise_top3', 'NA')}",
    ]
    ax_stats.text(0.01, 0.95, "Cluster summary", fontsize=12, weight="bold", va="top")
    y = 0.85
    for line in text_lines:
        ax_stats.text(0.02, y, line, fontsize=10, va="top")
        y -= 0.12

    fig.suptitle("Cluster Expertise Case Study", fontsize=16)
    out_png = os.path.join(OUTPUT_DIR, f"case_study_{cluster_id}.png")
    out_svg = os.path.join(OUTPUT_DIR, f"case_study_{cluster_id}.svg")
    fig.savefig(out_png, bbox_inches="tight")
    fig.savefig(out_svg, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    build_figure(CLUSTER_ID)


if __name__ == "__main__":
    main()

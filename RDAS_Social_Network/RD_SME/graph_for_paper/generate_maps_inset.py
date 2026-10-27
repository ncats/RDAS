import os
import io
import math
from typing import Dict, Tuple, Optional

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from matplotlib.patches import Ellipse
from typing import List


# Input data (same as base script)
AFFILIATIONS_CSV = "data/processed/csv_files/affiliation_node.csv"
COLLABS_CSV = "data/processed/csv_files/author_collaborations_relationships.csv"

# Output directory
OUTPUT_DIR = "data/processed/figures"

# Plot controls (kept consistent with base map)
MIN_EDGE_WEIGHT_TO_DRAW = 3
MAX_EDGES_TO_DRAW = 5000
NODE_SIZE_MIN = 20
NODE_SIZE_MAX = 800
SELF_LOOP_BASE_RADIUS_KM = 50

# Inset controls
INSET_POS = [0.62, 0.60, 0.32, 0.32]  # [left, bottom, width, height] in figure coords
GRID_DEG = 2.5                        # grid size (deg) for selecting dense zero-collab region
INSET_PADDING_DEG = 0.75              # expand bbox around selected grid cell


def ensure_us_county_centroids() -> pd.DataFrame:
    """
    Return a DataFrame with columns: GEOID (5-digit county FIPS), lon, lat.
    Try cached CSV, else derive from local uscities.csv, else try downloading
    a lightweight centroids CSV. Same logic as base script.
    """
    cache_csv = os.path.join(os.path.dirname(OUTPUT_DIR), "us_county_centroids.csv")
    if os.path.exists(cache_csv):
        return pd.read_csv(cache_csv, dtype={"GEOID": str})

    # Attempt to derive county centroids from uscities.csv (weighted by population)
    candidate_paths = [
        "data/external/uscities.csv",
        "data/external/uscities.csv",
    ]
    for path in candidate_paths:
        if os.path.exists(path):
            tmp = pd.read_csv(path)
            cols = {c.lower(): c for c in tmp.columns}
            required = {"county_fips", "lat", "lng"}
            if required.issubset(set(cols.keys())):
                fips_col = cols["county_fips"]
                lat_col = cols["lat"]
                lon_col = cols["lng"]
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

    # Try lightweight public CSVs
    try:
        import requests  # type: ignore
        candidate_urls = [
            "https://raw.githubusercontent.com/datadesk/countyjson/master/data/centroids.csv",
            "https://raw.githubusercontent.com/kjhealy/us-county/master/data/centers.csv",
        ]
        for url in candidate_urls:
            try:
                resp = requests.get(url, timeout=30)
                if resp.status_code == 200 and len(resp.text) > 1000:
                    tmp = pd.read_csv(io.StringIO(resp.text))
                    cols = {c.lower(): c for c in tmp.columns}
                    if "fips" in cols and "lat" in cols and "lon" in cols:
                        centroids = tmp.rename(
                            columns={cols["fips"]: "GEOID", cols["lon"]: "lon", cols["lat"]: "lat"}
                        )[["GEOID", "lon", "lat"]].copy()
                        centroids["GEOID"] = centroids["GEOID"].astype(str).str.zfill(5)
                        centroids.to_csv(cache_csv, index=False)
                        return centroids
            except Exception:
                continue
    except Exception:
        pass

    raise RuntimeError(
        "Could not load county centroids. Provide 'us_county_centroids.csv' with GEOID,lon,lat."
    )


def load_data_with_zero_nodes() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      - nodes_all: all FIPS present in affiliations (with 'weight' = incident edge sum, 0 if none)
      - edges: aggregated edges [fips_u, fips_v, weight]
      - coords: centroids for FIPS present in nodes_all
      - degree: degree (count of incident edge entries) per FIPS
    """
    affiliations = pd.read_csv(
        AFFILIATIONS_CSV,
        dtype={"Aff_FIPS": str},
        usecols=["author_id", "Aff_FIPS"],
    )
    affiliations["Aff_FIPS"] = affiliations["Aff_FIPS"].astype(str).str.strip()
    affiliations = affiliations[affiliations["Aff_FIPS"].str.fullmatch(r"\d{5}", na=False)].copy()

    all_fips = affiliations[["Aff_FIPS"]].drop_duplicates().rename(columns={"Aff_FIPS": "fips"})

    collabs = pd.read_csv(
        COLLABS_CSV,
        dtype=str,
        usecols=["author_id_1", "author_id_2"],
    )
    author_to_fips = affiliations.dropna(subset=["Aff_FIPS"])[["author_id", "Aff_FIPS"]]
    author_to_fips = author_to_fips.drop_duplicates(subset=["author_id"])

    collabs = collabs.merge(
        author_to_fips.rename(columns={"Aff_FIPS": "fips_u", "author_id": "author_id_1"}),
        on="author_id_1",
        how="left",
    ).merge(
        author_to_fips.rename(columns={"Aff_FIPS": "fips_v", "author_id": "author_id_2"}),
        on="author_id_2",
        how="left",
    )
    collabs = collabs.dropna(subset=["fips_u", "fips_v"])
    collabs["fips_u"] = collabs["fips_u"].astype(str)
    collabs["fips_v"] = collabs["fips_v"].astype(str)

    edges = (
        collabs.groupby(["fips_u", "fips_v"])
        .size()
        .reset_index(name="weight")
        .sort_values("weight", ascending=False)
    )

    # Degree and weighted degree (incident sum)
    left_sum = edges.groupby("fips_u")["weight"].sum()
    right_sum = edges.groupby("fips_v")["weight"].sum()
    weighted_degree = left_sum.add(right_sum, fill_value=0.0)
    degree_count = (
        pd.concat([edges["fips_u"], edges["fips_v"]])
        .value_counts()
        .rename("degree")
        .astype(int)
    )

    nodes_all = all_fips.merge(weighted_degree.rename("weight"), how="left", left_on="fips", right_index=True)
    nodes_all["weight"] = nodes_all["weight"].fillna(0.0)
    nodes_all = nodes_all.merge(degree_count.rename("degree"), how="left", left_on="fips", right_index=True)
    nodes_all["degree"] = nodes_all["degree"].fillna(0).astype(int)

    centroids = ensure_us_county_centroids().rename(columns={"GEOID": "fips"})
    coords = centroids[centroids["fips"].isin(nodes_all["fips"].unique())].copy()
    return nodes_all, edges, coords, nodes_all[["fips", "degree"]].copy()


def scale_sizes(values: pd.Series, vmin: float, vmax: float) -> pd.Series:
    if values.empty:
        return values
    lo, hi = float(values.min()), float(values.max())
    if lo == hi:
        return pd.Series(np.full(len(values), (vmin + vmax) / 2.0), index=values.index)
    return pd.Series(np.interp(values.astype(float), (lo, hi), (vmin, vmax)), index=values.index)


def select_inset_bbox(zero_nodes_df: pd.DataFrame, coords: pd.DataFrame) -> Tuple[float, float, float, float]:
    """
    Pick a zoom bbox where there are many zero-degree nodes (no edges).
    Returns (lon_min, lon_max, lat_min, lat_max)
    """
    if zero_nodes_df.empty:
        # Fallback: Midwest-ish view
        return (-100.0, -85.0, 35.0, 45.0)
    zero_coords = zero_nodes_df.merge(coords, on="fips", how="inner")
    if zero_coords.empty:
        return (-100.0, -85.0, 35.0, 45.0)

    # Grid-based density over CONUS bounds
    lon_min, lon_max = -125, -66.5
    lat_min, lat_max = 24, 49.5
    lons = np.arange(lon_min, lon_max, GRID_DEG)
    lats = np.arange(lat_min, lat_max, GRID_DEG)

    best = None
    best_count = -1
    for i in range(len(lons) - 1):
        for j in range(len(lats) - 1):
            a, b = lons[i], lons[i + 1]
            c, d = lats[j], lats[j + 1]
            mask = (zero_coords["lon"] >= a) & (zero_coords["lon"] < b) & (zero_coords["lat"] >= c) & (zero_coords["lat"] < d)
            cnt = int(mask.sum())
            if cnt > best_count:
                best_count = cnt
                best = (a, b, c, d)
    if best is None:
        return (-100.0, -85.0, 35.0, 45.0)
    a, b, c, d = best
    return (a - INSET_PADDING_DEG, b + INSET_PADDING_DEG, c - INSET_PADDING_DEG, d + INSET_PADDING_DEG)


def draw_map_with_inset() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    nodes_all, edges, coords, degree = load_data_with_zero_nodes()

    # Main plotting set (nodes that have coordinates)
    nodes_plot = nodes_all.merge(coords, on="fips", how="inner")
    nodes_plot["size"] = scale_sizes(nodes_plot["weight"], NODE_SIZE_MIN, NODE_SIZE_MAX)

    # Edges to draw
    edges_plot = edges[edges["weight"] >= MIN_EDGE_WEIGHT_TO_DRAW].copy()
    edges_plot = edges_plot.head(MAX_EDGES_TO_DRAW)
    coords_map: Dict[str, Tuple[float, float]] = dict(zip(nodes_plot["fips"], zip(nodes_plot["lon"], nodes_plot["lat"])))

    # Figure and main axes
    fig, ax = plt.subplots(figsize=(16, 8))
    ax.set_xlim(-125, -66.5)
    ax.set_ylim(24, 49.5)
    ax.grid(alpha=0.2, linewidth=0.3)

    def _download_us_states_geojson(cache_path: str) -> None:
        try:
            import requests  # type: ignore
            urls = [
                "https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json",
                "https://raw.githubusercontent.com/python-visualization/folium/master/examples/data/us-states.json",
            ]
            for url in urls:
                try:
                    r = requests.get(url, timeout=30)
                    if r.status_code == 200 and len(r.text) > 1000:
                        with open(cache_path, "w", encoding="utf-8") as f:
                            f.write(r.text)
                        return
                except Exception:
                    continue
        except Exception:
            pass

    def draw_states_outlines(ax_target: plt.Axes) -> None:
        states_cache = os.path.join(os.path.dirname(OUTPUT_DIR), "us_states.geojson")
        if not os.path.exists(states_cache):
            _download_us_states_geojson(states_cache)
        if not os.path.exists(states_cache):
            return
        try:
            import json
            with open(states_cache, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return
        exclude_ids = {"AK", "HI", "PR", "GU", "VI", "MP", "AS"}
        features: List[dict] = data.get("features", [])
        for feat in features:
            geom = (feat.get("geometry") or {})
            props = feat.get("properties") or {}
            st_id = (feat.get("id") or props.get("state_code") or props.get("STATE") or props.get("STATE_ABBR"))
            if isinstance(st_id, str) and st_id in exclude_ids:
                continue
            gtype = geom.get("type")
            coords = geom.get("coordinates", [])
            if gtype == "Polygon":
                for ring in coords:
                    if not ring:
                        continue
                    xs = [float(x) for x, y in ring]
                    ys = [float(y) for x, y in ring]
                    ax_target.plot(xs, ys, color="lightgray", linewidth=0.4, alpha=0.9, zorder=0.5)
            elif gtype == "MultiPolygon":
                for poly in coords:
                    for ring in poly:
                        if not ring:
                            continue
                        xs = [float(x) for x, y in ring]
                        ys = [float(y) for x, y in ring]
                        ax_target.plot(xs, ys, color="lightgray", linewidth=0.4, alpha=0.9, zorder=0.5)

    # draw state outlines on main axes
    draw_states_outlines(ax)
    def points_to_data_dx(ax_obj: plt.Axes, lon: float, lat: float, points: float) -> float:
        fig = ax_obj.figure
        x_disp, y_disp = ax_obj.transData.transform((lon, lat))
        dx_pix = points * (fig.dpi / 72.0)
        lon2, _ = ax_obj.transData.inverted().transform((x_disp + dx_pix, y_disp))
        return float(abs(lon2 - lon))

    def points_to_data_dy(ax_obj: plt.Axes, lon: float, lat: float, points: float) -> float:
        fig = ax_obj.figure
        x_disp, y_disp = ax_obj.transData.transform((lon, lat))
        dy_pix = points * (fig.dpi / 72.0)
        _, lat2 = ax_obj.transData.inverted().transform((x_disp, y_disp + dy_pix))
        return float(abs(lat2 - lat))

    def draw_self_loop_circle(ax_obj: plt.Axes, lon: float, lat: float, marker_size_pts2: float, weight: float, se_min: float, se_max: float, alpha: float = 0.95, lw_scale: Tuple[float, float] = (1.0, 2.0)) -> None:
        r_node_pts = float(np.sqrt(max(marker_size_pts2, 1.0) / np.pi))
        loop_ratio = float(np.interp(weight, (se_min, se_max), (0.8, 1.3)))
        r_loop_pts = r_node_pts * loop_ratio
        gap_pts = 0.0
        rx_node = points_to_data_dx(ax_obj, lon, lat, r_node_pts)
        ry_node = points_to_data_dy(ax_obj, lon, lat, r_node_pts)
        rx_loop = points_to_data_dx(ax_obj, lon, lat, r_loop_pts)
        ry_loop = points_to_data_dy(ax_obj, lon, lat, r_loop_pts)
        dx_gap = points_to_data_dx(ax_obj, lon, lat, gap_pts)
        # Place loop to the LEFT of node
        cx = lon - (rx_node + dx_gap + rx_loop)
        cy = lat
        lw = float(np.interp(weight, (se_min, se_max), lw_scale))
        loop = Ellipse((cx, cy), width=2 * rx_loop, height=2 * ry_loop,
                       angle=0.0, facecolor="none", edgecolor="#f4a460",
                       linewidth=lw, alpha=alpha, zorder=4)
        ax_obj.add_patch(loop)

    # Draw inter-county edges
    for _, row in edges_plot.iterrows():
        u, v, w = row["fips_u"], row["fips_v"], float(row["weight"])
        if u == v or u not in coords_map or v not in coords_map:
            continue
        (lon1, lat1) = coords_map[u]
        (lon2, lat2) = coords_map[v]
        alpha = np.interp(w, (edges_plot["weight"].min(), edges_plot["weight"].max()), (0.05, 0.25))
        ax.plot([lon1, lon2], [lat1, lat2], color="#f4a460", linewidth=0.6, alpha=alpha, zorder=1)

    # Self-loops (as arcs touching the node circle)
    self_edges = edges[(edges["fips_u"] == edges["fips_v"])].copy()
    if not self_edges.empty:
        self_edges = self_edges.merge(nodes_plot[["fips", "lon", "lat", "size"]], left_on="fips_u", right_on="fips")
        se_min = float(self_edges["weight"].min())
        se_max = float(self_edges["weight"].max())
        for _, row in self_edges.iterrows():
            lon = float(row["lon"]); lat = float(row["lat"]); w = float(row["weight"]); s_pts2 = float(row["size"])
            draw_self_loop_circle(ax, lon, lat, s_pts2, w, se_min, se_max, alpha=0.9, lw_scale=(1.0, 2.0))

    # Nodes (degree 0 highlighted by thicker edge)
    zero_degree = set(nodes_all.loc[nodes_all["degree"] == 0, "fips"].tolist())
    edgecolors = np.where(nodes_plot["fips"].isin(zero_degree), "black", "black")
    linewidths = np.where(nodes_plot["fips"].isin(zero_degree), 0.9, 0.4)
    ax.scatter(
        nodes_plot["lon"],
        nodes_plot["lat"],
        s=nodes_plot["size"],
        c="#8b0000",
        alpha=0.85,
        edgecolors=edgecolors,
        linewidths=linewidths,
        zorder=3,
    )

    # Labels (FIPS)
    for _, r in nodes_plot.iterrows():
        ax.text(r["lon"], r["lat"], r["fips"], fontsize=6, ha="center", va="center", color="white", zorder=4)

    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title("US Collaboration Network (Inset: area with nodes lacking any collaboration edges)")

    # Compute inset bbox focusing on zero-degree nodes
    zero_nodes_df = nodes_all[nodes_all["degree"] == 0][["fips"]]
    inset_bbox = select_inset_bbox(zero_nodes_df, coords)
    x0, x1, y0, y1 = inset_bbox

    # Draw rectangle on main map indicating inset
    rect = Rectangle((x0, y0), x1 - x0, y1 - y0, linewidth=1.2, edgecolor="blue", facecolor="none", zorder=5)
    ax.add_patch(rect)

    # Inset axes
    ax_in = fig.add_axes(INSET_POS)
    ax_in.set_xlim(x0, x1)
    ax_in.set_ylim(y0, y1)
    ax_in.grid(alpha=0.2, linewidth=0.3)
    ax_in.set_xticks([])
    ax_in.set_yticks([])
    ax_in.set_title("Zoomed region (isolated nodes)", fontsize=9)
    # draw states outlines in inset too
    draw_states_outlines(ax_in)

    # Plot within inset
    # Edges in bbox
    def in_bbox(lon: float, lat: float) -> bool:
        return (x0 <= lon <= x1) and (y0 <= lat <= y1)

    for _, row in edges_plot.iterrows():
        u, v, w = row["fips_u"], row["fips_v"], float(row["weight"])
        if u == v or u not in coords_map or v not in coords_map:
            continue
        (lon1, lat1) = coords_map[u]; (lon2, lat2) = coords_map[v]
        if in_bbox(lon1, lat1) and in_bbox(lon2, lat2):
            alpha = np.interp(w, (edges_plot["weight"].min(), edges_plot["weight"].max()), (0.1, 0.35))
            ax_in.plot([lon1, lon2], [lat1, lat2], color="#f4a460", linewidth=0.8, alpha=alpha, zorder=1)

    # Self-loops in bbox (as arcs)
    if not self_edges.empty:
        for _, row in self_edges.iterrows():
            lon = float(row["lon"]); lat = float(row["lat"]); w = float(row["weight"]); s_pts2 = float(row["size"])
            if not in_bbox(lon, lat):
                continue
            draw_self_loop_circle(ax_in, lon, lat, s_pts2, w, se_min, se_max, alpha=0.95, lw_scale=(1.0, 2.0))

    nodes_in = nodes_plot[nodes_plot.apply(lambda r: in_bbox(r["lon"], r["lat"]), axis=1)].copy()
    edgecolors_in = np.where(nodes_in["fips"].isin(zero_degree), "black", "black")
    linewidths_in = np.where(nodes_in["fips"].isin(zero_degree), 1.0, 0.5)
    ax_in.scatter(
        nodes_in["lon"],
        nodes_in["lat"],
        s=nodes_in["size"],
        c="#8b0000",
        alpha=0.95,
        edgecolors=edgecolors_in,
        linewidths=linewidths_in,
        zorder=3,
    )
    for _, r in nodes_in.iterrows():
        ax_in.text(r["lon"], r["lat"], r["fips"], fontsize=6.5, ha="center", va="center", color="white", zorder=4)

    # Save
    out_png = os.path.join(OUTPUT_DIR, "us_collaboration_map_with_inset.png")
    out_svg = os.path.join(OUTPUT_DIR, "us_collaboration_map_with_inset.svg")
    plt.tight_layout()
    fig.savefig(out_png, dpi=300, bbox_inches="tight")
    fig.savefig(out_svg, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    draw_map_with_inset()


if __name__ == "__main__":
    main()

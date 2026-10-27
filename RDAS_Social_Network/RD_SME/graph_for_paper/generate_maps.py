import os
import io
import zipfile
import math
from typing import Dict, Tuple, Optional

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Ellipse
from typing import List


# Input data
AFFILIATIONS_CSV = "data/processed/csv_files/affiliation_node.csv"
COLLABS_CSV = "data/processed/csv_files/author_collaborations_relationships.csv"

# Output directory
OUTPUT_DIR = "data/processed/figures"

# Display controls
MIN_EDGE_WEIGHT_TO_DRAW = 3      # hide very weak edges for readability
MAX_EDGES_TO_DRAW = 5000         # safety cap
NODE_SIZE_MIN = 20
NODE_SIZE_MAX = 800
SELF_LOOP_BASE_RADIUS_KM = 50    # base size for self-loop circle


def ensure_us_county_centroids() -> pd.DataFrame:
    """
    Return a DataFrame with columns: GEOID (5-digit county FIPS), lon, lat.
    Strategy:
      1) Load from local cache CSV if present
      2) Build from local uscities.csv if available (weighted by population when present)
      3) Try downloading a lightweight public CSV of county centroids
      4) If unavailable, instruct the user to provide a centroid CSV
    """
    cache_csv = os.path.join(
        os.path.dirname(OUTPUT_DIR),
        "us_county_centroids.csv",
    )
    if os.path.exists(cache_csv):
        return pd.read_csv(cache_csv, dtype={"GEOID": str})

    # Attempt to derive county centroids from uscities.csv
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
                # Weighted average by population if available, else simple mean
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

    # Try downloading a small ready-made centroids CSV from known sources
    try:
        import requests  # type: ignore
        candidate_urls = [
            # LA Times county centroids (fips,lat,lon) - commonly available
            "https://raw.githubusercontent.com/datadesk/countyjson/master/data/centroids.csv",
            # Another possible source with columns fips,lon,lat (order may vary)
            "https://raw.githubusercontent.com/kjhealy/us-county/master/data/centers.csv",
        ]
        for url in candidate_urls:
            try:
                resp = requests.get(url, timeout=30)
                if resp.status_code == 200 and len(resp.text) > 1000:
                    tmp = pd.read_csv(io.StringIO(resp.text))
                    cols = {c.lower(): c for c in tmp.columns}
                    # Normalize column names
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
        "Could not load county centroids.\n"
        f"- Provide a CSV at {cache_csv} with columns: GEOID,lon,lat (lon/lat in WGS84), or\n"
        "- Ensure internet access to auto-download a centroids CSV."
    )


def haversine_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    r = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def load_and_aggregate() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      - nodes_df: index by FIPS with 'weight' = total collaborations involving that FIPS
      - edges_df: columns [fips_u, fips_v, weight], aggregated (including self-edges)
      - coords_df: columns [GEOID, lon, lat] for FIPS present in nodes_df
    """
    affiliations = pd.read_csv(
        AFFILIATIONS_CSV,
        dtype={"Aff_FIPS": str, "Aff_state": str, "Aff_country": str},
        usecols=["author_id", "Aff_city", "Aff_state", "Aff_country", "Aff_FIPS"],
    )
    # Keep only US counties with 5-digit FIPS
    affiliations["Aff_FIPS"] = affiliations["Aff_FIPS"].str.strip()
    affiliations = affiliations[
        affiliations["Aff_FIPS"].str.fullmatch(r"\d{5}", na=False)
    ].copy()

    author_to_fips = affiliations.dropna(subset=["Aff_FIPS"])[["author_id", "Aff_FIPS"]]
    author_to_fips = author_to_fips.drop_duplicates(subset=["author_id"])

    collabs = pd.read_csv(
        COLLABS_CSV,
        dtype=str,
        usecols=["author_id_1", "author_id_2"],
    )

    # Map authors to FIPS and filter valid pairs
    collabs = collabs.merge(
        author_to_fips.rename(columns={"Aff_FIPS": "fips_u", "author_id": "author_id_1"}),
        on="author_id_1",
        how="left",
    )
    collabs = collabs.merge(
        author_to_fips.rename(columns={"Aff_FIPS": "fips_v", "author_id": "author_id_2"}),
        on="author_id_2",
        how="left",
    )
    collabs = collabs.dropna(subset=["fips_u", "fips_v"])
    collabs["fips_u"] = collabs["fips_u"].astype(str)
    collabs["fips_v"] = collabs["fips_v"].astype(str)

    # Aggregate edge weights
    edges = (
        collabs.groupby(["fips_u", "fips_v"])
        .size()
        .reset_index(name="weight")
        .sort_values("weight", ascending=False)
    )

    # Node weights as sum of incident edges (including self-edges)
    left_sum = edges.groupby("fips_u")["weight"].sum()
    right_sum = edges.groupby("fips_v")["weight"].sum()
    node_weight = left_sum.add(right_sum, fill_value=0)
    nodes = node_weight.reset_index()
    nodes.columns = ["fips", "weight"]

    # Coordinates for nodes
    centroids = ensure_us_county_centroids()
    coords = centroids.rename(columns={"GEOID": "fips"})
    coords = coords[coords["fips"].isin(nodes["fips"].unique())].copy()
    return nodes, edges, coords


def scale_sizes(values: pd.Series, vmin: float, vmax: float) -> pd.Series:
    if values.empty:
        return values
    lo, hi = float(values.min()), float(values.max())
    if lo == hi:
        return pd.Series(np.full(len(values), (vmin + vmax) / 2.0), index=values.index)
    scaled = np.interp(values.astype(float), (lo, hi), (vmin, vmax))
    return pd.Series(scaled, index=values.index)


def try_add_us_basemap(ax) -> None:
    """
    Attempt to draw a US basemap. If cartopy is not available, no-op.
    """
    # Skip basemap to avoid heavy GIS dependencies in constrained environments.
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

def draw_states_outlines(ax: plt.Axes) -> None:
    """
    Draw simple state outlines using a cached/downloaded GeoJSON.
    """
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
                ax.plot(xs, ys, color="lightgray", linewidth=0.4, alpha=0.9, zorder=0.5)
        elif gtype == "MultiPolygon":
            for poly in coords:
                for ring in poly:
                    if not ring:
                        continue
                    xs = [float(x) for x, y in ring]
                    ys = [float(y) for x, y in ring]
                    ax.plot(xs, ys, color="lightgray", linewidth=0.4, alpha=0.9, zorder=0.5)


def draw_map(nodes: pd.DataFrame, edges: pd.DataFrame, coords: pd.DataFrame) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Merge coordinates onto nodes
    nodes_plot = nodes.merge(coords, on="fips", how="inner")
    if nodes_plot.empty:
        raise RuntimeError("No nodes with coordinates available to plot.")

    # Limit edges and filter by weight
    edges_plot = edges[edges["weight"] >= MIN_EDGE_WEIGHT_TO_DRAW].copy()
    edges_plot = edges_plot.head(MAX_EDGES_TO_DRAW)

    # Prepare node sizes
    nodes_plot["size"] = scale_sizes(
        nodes_plot["weight"], NODE_SIZE_MIN, NODE_SIZE_MAX
    )

    # Figure
    fig, ax = plt.subplots(figsize=(16, 8))
    try_add_us_basemap(ax)
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

    def draw_self_loop_circle(ax_obj: plt.Axes, lon: float, lat: float, marker_size_pts2: float, weight: float, se_min: float, se_max: float) -> None:
        # Node radius in points
        r_node_pts = float(np.sqrt(max(marker_size_pts2, 1.0) / np.pi))
        # Loop radius scales with weight relative to node radius
        loop_ratio = float(np.interp(weight, (se_min, se_max), (0.8, 1.3)))
        r_loop_pts = r_node_pts * loop_ratio
        gap_pts = 0.0  # no gap; tangent contact with node
        # Convert radii and gap to data deltas
        rx_node = points_to_data_dx(ax_obj, lon, lat, r_node_pts)
        ry_node = points_to_data_dy(ax_obj, lon, lat, r_node_pts)
        rx_loop = points_to_data_dx(ax_obj, lon, lat, r_loop_pts)
        ry_loop = points_to_data_dy(ax_obj, lon, lat, r_loop_pts)
        dx_gap = points_to_data_dx(ax_obj, lon, lat, gap_pts)
        # Place loop center to the LEFT, tangent to node
        cx = lon - (rx_node + dx_gap + rx_loop)
        cy = lat
        lw = float(np.interp(weight, (se_min, se_max), (0.9, 2.0)))
        loop = Ellipse((cx, cy), width=2 * rx_loop, height=2 * ry_loop,
                       angle=0.0, facecolor="none", edgecolor="#f4a460",
                       linewidth=lw, alpha=0.95, zorder=4)
        ax_obj.add_patch(loop)

    # Draw inter-county edges
    coords_map: Dict[str, Tuple[float, float]] = dict(
        zip(nodes_plot["fips"], zip(nodes_plot["lon"], nodes_plot["lat"]))
    )
    for _, row in edges_plot.iterrows():
        u = row["fips_u"]
        v = row["fips_v"]
        w = float(row["weight"])
        if u not in coords_map or v not in coords_map:
            continue
        (lon1, lat1) = coords_map[u]
        (lon2, lat2) = coords_map[v]
        if u == v:
            continue
        alpha = np.interp(w, (edges_plot["weight"].min(), edges_plot["weight"].max()), (0.05, 0.25))
        ax.plot([lon1, lon2], [lat1, lat2], color="#f4a460", linewidth=0.6, alpha=alpha, zorder=1)

    # Draw self-loops as tangent circles next to the node
    self_edges = edges[(edges["fips_u"] == edges["fips_v"])].copy()
    if not self_edges.empty:
        self_edges = self_edges.merge(nodes_plot[["fips", "lon", "lat", "size"]], left_on="fips_u", right_on="fips")
        se_min = float(self_edges["weight"].min())
        se_max = float(self_edges["weight"].max())
        for _, row in self_edges.iterrows():
            lon = float(row["lon"])
            lat = float(row["lat"])
            w = float(row["weight"])
            s_pts2 = float(row["size"])
            draw_self_loop_circle(ax, lon, lat, s_pts2, w, se_min, se_max)

    # Draw nodes
    sc = ax.scatter(
        nodes_plot["lon"],
        nodes_plot["lat"],
        s=nodes_plot["size"],
        c="#8b0000",
        alpha=0.8,
        edgecolors="black",
        linewidths=0.4,
        zorder=3,
    )

    # Label with FIPS
    for _, r in nodes_plot.iterrows():
        ax.text(r["lon"], r["lat"], r["fips"], fontsize=6, ha="center", va="center", color="white", zorder=4)

    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title("US Collaboration Network by County FIPS\nNode size ∝ collaboration intensity; self-loops = within-county collaborations")
    plt.tight_layout()

    out_png = os.path.join(OUTPUT_DIR, "us_collaboration_map.png")
    out_svg = os.path.join(OUTPUT_DIR, "us_collaboration_map.svg")
    fig.savefig(out_png, dpi=300, bbox_inches="tight")
    fig.savefig(out_svg, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    nodes, edges, coords = load_and_aggregate()
    # Keep coordinates only for nodes present
    coords = coords[coords["fips"].isin(nodes["fips"])].copy()
    draw_map(nodes, edges, coords)


if __name__ == "__main__":
    main()

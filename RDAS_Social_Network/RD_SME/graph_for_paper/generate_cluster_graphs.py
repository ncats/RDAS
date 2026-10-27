import os
from typing import Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


DATA_CSV = "data/external/researchers_per_cluster.csv"
OUTPUT_DIR = "data/processed/figures"


def compute_sizes(series: pd.Series, min_size: float = 100.0, max_size: float = 3000.0) -> pd.Series:
    """
    Map researcher counts to bubble areas used by matplotlib's scatter 's' parameter (points^2).
    The mapping is linear between the min and max of the series.
    """
    if series.empty:
        return series
    if series.nunique() == 1:
        return pd.Series(np.full(len(series), (min_size + max_size) / 2.0), index=series.index)
    values = series.to_numpy(dtype=float)
    vmin, vmax = float(values.min()), float(values.max())
    scaled = np.interp(values, (vmin, vmax), (min_size, max_size))
    return pd.Series(scaled, index=series.index)


def load_data(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    expected_cols = {"cluster_id", "researchers"}
    missing = expected_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in CSV: {missing}")
    df["researchers"] = pd.to_numeric(df["researchers"], errors="coerce")
    df = df.dropna(subset=["researchers"]).copy()
    df = df.sort_values("researchers", ascending=False).reset_index(drop=True)
    return df


def save_top_n_bubble(df: pd.DataFrame, output_dir: str, top_n: int = 30) -> Tuple[str, str]:
    top = df.head(top_n).copy()
    top["size"] = compute_sizes(top["researchers"])

    sns.set_theme(style="whitegrid", font_scale=1.2)
    plt.rcParams["figure.dpi"] = 200

    palette = sns.color_palette("tab20", n_colors=min(20, len(top)))
    color_map = {cid: palette[i % len(palette)] for i, cid in enumerate(top["cluster_id"])}

    fig, ax = plt.subplots(figsize=(max(10, top_n * 0.5), 6))
    x = np.arange(len(top))
    y = np.zeros(len(top))
    colors = [color_map[cid] for cid in top["cluster_id"]]
    ax.scatter(x, y, s=top["size"], c=colors, alpha=0.8, edgecolors="black", linewidths=0.5)

    for i, (cid, count) in enumerate(zip(top["cluster_id"], top["researchers"])):
        ax.text(x[i], y[i] + 0.02, str(int(count)), ha="center", va="bottom", fontsize=8)

    ax.set_xticks(x)
    short_labels = [cid.split("_")[-1] for cid in top["cluster_id"]]
    ax.set_xticklabels(short_labels, rotation=45, ha="right")
    ax.set_yticks([])
    ax.set_xlabel("Cluster ID (suffix)")
    ax.set_title(f"Top {top_n} Clusters by Researchers (bubble size ∝ researchers)")

    os.makedirs(output_dir, exist_ok=True)
    out_png = os.path.join(output_dir, f"clusters_bubble_top{top_n}.png")
    out_svg = os.path.join(output_dir, f"clusters_bubble_top{top_n}.svg")
    plt.tight_layout()
    fig.savefig(out_png, bbox_inches="tight")
    fig.savefig(out_svg, bbox_inches="tight")
    plt.close(fig)
    return out_png, out_svg


def save_all_bubble_strip(df: pd.DataFrame, output_dir: str) -> Tuple[str, str]:
    all_df = df.copy()
    all_df["size"] = compute_sizes(all_df["researchers"])

    sns.set_theme(style="whitegrid", font_scale=1.2)
    plt.rcParams["figure.dpi"] = 200

    fig, ax = plt.subplots(figsize=(16, 6))
    x = np.arange(len(all_df))
    y = np.zeros(len(all_df))
    norm = plt.Normalize(all_df["researchers"].min(), all_df["researchers"].max())
    cmap = plt.cm.viridis
    colors = cmap(norm(all_df["researchers"].to_numpy()))

    ax.scatter(x, y, s=all_df["size"], c=colors, alpha=0.35, edgecolors="none")
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_xlabel("Clusters (sorted by researchers)")
    ax.set_title("All Clusters Bubble Strip (color and size ∝ researchers)")

    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = plt.colorbar(sm, ax=ax, pad=0.02)
    cbar.set_label("Researchers")

    os.makedirs(output_dir, exist_ok=True)
    out_png = os.path.join(output_dir, "clusters_bubble_all.png")
    out_svg = os.path.join(output_dir, "clusters_bubble_all.svg")
    plt.tight_layout()
    fig.savefig(out_png, bbox_inches="tight")
    fig.savefig(out_svg, bbox_inches="tight")
    plt.close(fig)
    return out_png, out_svg


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df = load_data(DATA_CSV)
    save_top_n_bubble(df, OUTPUT_DIR, top_n=30)
    save_all_bubble_strip(df, OUTPUT_DIR)


if __name__ == "__main__":
    main()

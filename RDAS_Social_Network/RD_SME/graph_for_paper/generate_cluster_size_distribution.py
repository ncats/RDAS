import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

INPUT_CSV = "data/processed/csv_files/cluster_node.csv"
OUTPUT_DIR = "data/processed/figures"


def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "num_authors" not in df.columns:
        raise ValueError("Expected column 'num_authors' in cluster_node.csv")
    df["num_authors"] = pd.to_numeric(df["num_authors"], errors="coerce")
    df = df.dropna(subset=["num_authors"]).copy()
    df["num_authors"] = df["num_authors"].astype(int)
    return df


def freedman_diaconis_bins(x: np.ndarray) -> int:
    x = x[~np.isnan(x)]
    if x.size < 2:
        return 10
    q75, q25 = np.percentile(x, [75, 25])
    iqr = q75 - q25
    if iqr <= 0:
        return 10
    bin_width = 2 * iqr / np.cbrt(x.size)
    if bin_width <= 0:
        return 10
    bins = int(np.ceil((x.max() - x.min()) / bin_width))
    return max(bins, 10)


def plot_distribution(df: pd.DataFrame) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    sns.set_theme(style="whitegrid")
    plt.rcParams["figure.dpi"] = 200

    values = df["num_authors"].to_numpy()
    bins_fd = freedman_diaconis_bins(values)

    mean_v = float(np.mean(values))
    median_v = float(np.median(values))
    p90_v = float(np.percentile(values, 90))

    # Single figure with two panels: linear + log-x
    fig, axes = plt.subplots(1, 2, figsize=(14, 5), constrained_layout=True)

    # Linear scale histogram
    axes[0].hist(values, bins=bins_fd, color="#8b0000", alpha=0.8, edgecolor="black", linewidth=0.3)
    axes[0].axvline(median_v, color="black", linestyle="--", linewidth=1.0, label=f"Median = {int(median_v)}")
    axes[0].axvline(p90_v, color="gray", linestyle=":", linewidth=1.0, label=f"90th pct = {int(p90_v)}")
    axes[0].set_xlabel("Number of authors per cluster")
    axes[0].set_ylabel("Number of clusters")
    axes[0].set_title("Cluster size distribution (linear)")
    axes[0].legend(frameon=True)

    # Log-x histogram (avoid zeros)
    positive = values[values > 0]
    if positive.size > 0:
        axes[1].hist(positive, bins=bins_fd, color="#8b0000", alpha=0.8, edgecolor="black", linewidth=0.3)
        axes[1].set_xscale("log")
        axes[1].axvline(max(median_v, 1), color="black", linestyle="--", linewidth=1.0, label="Median")
        axes[1].axvline(max(p90_v, 1), color="gray", linestyle=":", linewidth=1.0, label="90th pct")
        axes[1].set_xlabel("Number of authors per cluster (log scale)")
        axes[1].set_ylabel("Number of clusters")
        axes[1].set_title("Cluster size distribution (log-x)")
        axes[1].legend(frameon=True)
    else:
        axes[1].text(0.5, 0.5, "No positive cluster sizes", ha="center", va="center")
        axes[1].axis("off")

    fig.suptitle("Cluster Size Distribution", fontsize=14)

    out_png = os.path.join(OUTPUT_DIR, "cluster_size_distribution.png")
    out_svg = os.path.join(OUTPUT_DIR, "cluster_size_distribution.svg")
    fig.savefig(out_png, bbox_inches="tight")
    fig.savefig(out_svg, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    df = load_data(INPUT_CSV)
    plot_distribution(df)


if __name__ == "__main__":
    main()

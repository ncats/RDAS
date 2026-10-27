#!/usr/bin/env python3
"""
Compare the current HDBSCAN clustering workflow against K-means on a random
sample of researcher embeddings for GARD:0006233.

Outputs are written only under step2_experiment_clustering/outputs.
"""

from __future__ import annotations

import argparse
import json
import os
import warnings
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import hdbscan
import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.metrics import davies_bouldin_score, silhouette_score


BASE_DIR = "step2_experiment_clustering"
DEFAULT_EMBEDDINGS_CSV = "data/processed/step2/embeddings/embeddings_GARD_0006233.csv"
DEFAULT_OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")


@dataclass
class MethodResult:
    method: str
    labels: np.ndarray
    n_clusters: int
    n_noise: int
    silhouette: float
    davies_bouldin: float
    selection_score: Optional[float]
    params: Dict[str, object]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate HDBSCAN vs K-means on a random sample of embeddings."
    )
    parser.add_argument("--embeddings-csv", default=DEFAULT_EMBEDDINGS_CSV)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--sample-size", type=int, default=1000)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--kmeans-min-k", type=int, default=2)
    parser.add_argument("--kmeans-max-k", type=int, default=12)
    return parser.parse_args()


def load_embeddings(path: str) -> Tuple[pd.DataFrame, List[str]]:
    df = pd.read_csv(path)
    feature_cols = [col for col in df.columns if col.startswith("feature_")]
    if not feature_cols:
        raise ValueError(f"No feature columns found in {path}")
    required_cols = {"author_id", "author_name"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing metadata columns in embeddings CSV: {sorted(missing)}")
    return df, feature_cols


def sample_embeddings(
    df: pd.DataFrame,
    feature_cols: Iterable[str],
    sample_size: int,
    random_state: int,
) -> Tuple[pd.DataFrame, np.ndarray]:
    if sample_size <= 1:
        raise ValueError("sample_size must be > 1")
    sample_n = min(sample_size, len(df))
    sampled = df.sample(n=sample_n, random_state=random_state).reset_index(drop=True).copy()
    sampled["sample_index"] = np.arange(len(sampled))
    X = sampled[list(feature_cols)].to_numpy(dtype=float)
    return sampled, X


def compute_clustering_metrics(X: np.ndarray, labels: np.ndarray) -> Tuple[float, float, int, int]:
    labels = np.asarray(labels)
    non_noise_mask = labels != -1
    n_noise = int((labels == -1).sum())
    unique_non_noise = sorted(set(labels[non_noise_mask].tolist()))
    n_clusters = len(unique_non_noise)

    if n_clusters < 2:
        return np.nan, np.nan, n_clusters, n_noise

    X_eval = X[non_noise_mask]
    labels_eval = labels[non_noise_mask]
    if len(X_eval) < 2:
        return np.nan, np.nan, n_clusters, n_noise

    silhouette = float(silhouette_score(X_eval, labels_eval, metric="euclidean"))
    davies_bouldin = float(davies_bouldin_score(X_eval, labels_eval))
    return silhouette, davies_bouldin, n_clusters, n_noise


def grid_search_hdbscan(X: np.ndarray) -> Tuple[Dict[str, object], float]:
    min_samples_values = [3, 5, 8]
    cluster_selection_epsilon_values = [0.10, 0.15, 0.20, 0.25]
    cluster_selection_method_values = ["eom", "leaf"]
    n_samples = len(X)
    min_cluster_size_values = sorted(set([5, 10, 15, max(8, int(0.01 * n_samples))]))

    best_score = float("-inf")
    best_params: Optional[Dict[str, object]] = None

    for min_samples in min_samples_values:
        for min_cluster_size in min_cluster_size_values:
            for epsilon in cluster_selection_epsilon_values:
                for method in cluster_selection_method_values:
                    params = {
                        "min_samples": min_samples,
                        "min_cluster_size": min_cluster_size,
                        "cluster_selection_epsilon": epsilon,
                        "cluster_selection_method": method,
                    }
                    try:
                        clusterer = hdbscan.HDBSCAN(
                            **params,
                            metric="euclidean",
                            core_dist_n_jobs=os.cpu_count(),
                        )
                        labels = clusterer.fit_predict(X)
                        unique_labels = set(labels.tolist())
                        n_clusters = len(unique_labels) - (1 if -1 in unique_labels else 0)
                        if n_clusters < 2:
                            continue

                        dbcv_score = float(hdbscan.validity.validity_index(X, labels, metric="euclidean"))
                        cluster_sizes = [int((labels == label).sum()) for label in unique_labels if label != -1]
                        largest_cluster_ratio = max(cluster_sizes) / len(X) if cluster_sizes else 1.0
                        cluster_balance = 1.0 - (np.std(cluster_sizes) / np.mean(cluster_sizes) / 2.0)
                        cluster_balance = max(0.0, min(1.0, float(cluster_balance)))
                        cluster_count_score = min(1.0, n_clusters / 20.0)

                        if largest_cluster_ratio > 0.40:
                            main_cluster_penalty = 0.0
                        elif largest_cluster_ratio > 0.30:
                            main_cluster_penalty = 0.3
                        elif largest_cluster_ratio > 0.20:
                            main_cluster_penalty = 0.7
                        else:
                            main_cluster_penalty = 1.0

                        combined = (
                            0.3 * dbcv_score
                            + 0.3 * cluster_count_score
                            + 0.3 * main_cluster_penalty
                            + 0.1 * cluster_balance
                        )
                        if combined > best_score:
                            best_score = combined
                            best_params = params
                    except Exception:
                        continue

    if best_params is None:
        best_params = {
            "min_samples": 5,
            "min_cluster_size": 10,
            "cluster_selection_epsilon": 0.20,
            "cluster_selection_method": "eom",
        }
        best_score = np.nan

    return best_params, float(best_score)


def run_hdbscan(X: np.ndarray) -> MethodResult:
    best_params, selection_score = grid_search_hdbscan(X)
    clusterer = hdbscan.HDBSCAN(
        **best_params,
        metric="euclidean",
        core_dist_n_jobs=os.cpu_count(),
    )
    labels = clusterer.fit_predict(X)
    silhouette, dbi, n_clusters, n_noise = compute_clustering_metrics(X, labels)
    return MethodResult(
        method="Current HDBSCAN",
        labels=labels,
        n_clusters=n_clusters,
        n_noise=n_noise,
        silhouette=silhouette,
        davies_bouldin=dbi,
        selection_score=selection_score,
        params=best_params,
    )


def choose_best_kmeans(X: np.ndarray, min_k: int, max_k: int, random_state: int) -> MethodResult:
    if min_k < 2:
        raise ValueError("kmeans_min_k must be >= 2")
    if max_k < min_k:
        raise ValueError("kmeans_max_k must be >= kmeans_min_k")

    best: Optional[MethodResult] = None

    for k in range(min_k, max_k + 1):
        model = KMeans(n_clusters=k, random_state=random_state, n_init="auto")
        labels = model.fit_predict(X)
        silhouette = float(silhouette_score(X, labels, metric="euclidean"))
        dbi = float(davies_bouldin_score(X, labels))
        result = MethodResult(
            method="KMeans",
            labels=labels,
            n_clusters=k,
            n_noise=0,
            silhouette=silhouette,
            davies_bouldin=dbi,
            selection_score=silhouette,
            params={"n_clusters": k, "random_state": random_state, "n_init": "auto"},
        )
        if best is None:
            best = result
            continue
        if (result.silhouette > best.silhouette) or (
            np.isclose(result.silhouette, best.silhouette) and result.davies_bouldin < best.davies_bouldin
        ):
            best = result

    if best is None:
        raise RuntimeError("KMeans search failed to produce any result")
    return best


def make_metrics_table(
    results: List[MethodResult],
    sample_size: int,
    random_state: int,
) -> pd.DataFrame:
    rows = []
    for result in results:
        rows.append(
            {
                "method": result.method,
                "sample_size": sample_size,
                "random_state": random_state,
                "num_clusters": result.n_clusters,
                "num_noise": result.n_noise,
                "noise_ratio": result.n_noise / sample_size,
                "silhouette_score": result.silhouette,
                "davies_bouldin_index": result.davies_bouldin,
                "model_selection_score": result.selection_score,
                "selected_params_json": json.dumps(result.params, sort_keys=True),
            }
        )
    return pd.DataFrame(rows)


def write_markdown_table(df: pd.DataFrame, path: str) -> None:
    render_df = df.copy()
    for col in ["noise_ratio", "silhouette_score", "davies_bouldin_index", "model_selection_score"]:
        render_df[col] = render_df[col].map(
            lambda x: "NA" if pd.isna(x) else f"{float(x):.4f}"
        )

    headers = render_df.columns.tolist()
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for _, row in render_df.iterrows():
        lines.append("| " + " | ".join(str(row[h]) for h in headers) + " |")
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def save_assignments(
    sampled_df: pd.DataFrame,
    hdbscan_result: MethodResult,
    kmeans_result: MethodResult,
    path: str,
) -> None:
    out = sampled_df[["sample_index", "author_id", "author_name"]].copy()
    out["hdbscan_label"] = hdbscan_result.labels
    out["kmeans_label"] = kmeans_result.labels
    out.to_csv(path, index=False)


def plot_metric_comparison(
    df: pd.DataFrame,
    sample_size: int,
    output_png: str,
    output_svg: str,
) -> None:
    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.8))

    order = df["method"].tolist()
    silhouette_vals = df["silhouette_score"].tolist()
    dbi_vals = df["davies_bouldin_index"].tolist()
    colors = ["#8b0000", "#1f77b4"]

    axes[0].bar(order, silhouette_vals, color=colors, alpha=0.85)
    axes[0].set_title("Silhouette Score")
    axes[0].set_ylabel("Higher is better")
    for idx, value in enumerate(silhouette_vals):
        axes[0].text(idx, value, f"{value:.3f}", ha="center", va="bottom", fontsize=9)

    axes[1].bar(order, dbi_vals, color=colors, alpha=0.85)
    axes[1].set_title("Davies-Bouldin Index")
    axes[1].set_ylabel("Lower is better")
    for idx, value in enumerate(dbi_vals):
        axes[1].text(idx, value, f"{value:.3f}", ha="center", va="bottom", fontsize=9)

    fig.suptitle(f"Clustering Evaluation on {sample_size:,} Randomly Sampled Researcher Embeddings")
    fig.tight_layout(rect=[0, 0, 1, 0.94])
    fig.savefig(output_png, dpi=300, bbox_inches="tight")
    fig.savefig(output_svg, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    os.makedirs(args.output_dir, exist_ok=True)
    warnings.filterwarnings(
        "ignore",
        message=".*force_all_finite.*ensure_all_finite.*",
        category=FutureWarning,
    )

    embeddings_df, feature_cols = load_embeddings(args.embeddings_csv)
    sampled_df, X = sample_embeddings(
        embeddings_df,
        feature_cols,
        sample_size=args.sample_size,
        random_state=args.random_state,
    )

    sample_csv = os.path.join(
        args.output_dir,
        f"sampled_embeddings_n{len(sampled_df)}_seed{args.random_state}.csv",
    )
    sampled_df.to_csv(sample_csv, index=False)

    hdbscan_result = run_hdbscan(X)
    kmeans_result = choose_best_kmeans(
        X,
        min_k=args.kmeans_min_k,
        max_k=args.kmeans_max_k,
        random_state=args.random_state,
    )

    metrics_df = make_metrics_table(
        [hdbscan_result, kmeans_result],
        sample_size=len(sampled_df),
        random_state=args.random_state,
    )
    metrics_csv = os.path.join(args.output_dir, "clustering_method_comparison.csv")
    metrics_md = os.path.join(args.output_dir, "clustering_method_comparison.md")
    metrics_df.to_csv(metrics_csv, index=False)
    write_markdown_table(metrics_df, metrics_md)

    assignments_csv = os.path.join(args.output_dir, "sample_cluster_assignments.csv")
    save_assignments(sampled_df, hdbscan_result, kmeans_result, assignments_csv)

    fig_png = os.path.join(args.output_dir, "clustering_method_comparison.png")
    fig_svg = os.path.join(args.output_dir, "clustering_method_comparison.svg")
    plot_metric_comparison(metrics_df, len(sampled_df), fig_png, fig_svg)

    summary_json = os.path.join(args.output_dir, "clustering_method_comparison_summary.json")
    with open(summary_json, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "sample_size": len(sampled_df),
                "random_state": args.random_state,
                "embeddings_csv": args.embeddings_csv,
                "outputs": {
                    "sample_csv": sample_csv,
                    "metrics_csv": metrics_csv,
                    "metrics_md": metrics_md,
                    "assignments_csv": assignments_csv,
                    "figure_png": fig_png,
                    "figure_svg": fig_svg,
                },
                "results": metrics_df.to_dict(orient="records"),
            },
            handle,
            indent=2,
        )

    print(f"Saved sample CSV: {sample_csv}")
    print(f"Saved metrics CSV: {metrics_csv}")
    print(f"Saved metrics Markdown: {metrics_md}")
    print(f"Saved assignments CSV: {assignments_csv}")
    print(f"Saved figure PNG: {fig_png}")
    print(f"Saved figure SVG: {fig_svg}")
    print(f"Saved summary JSON: {summary_json}")


if __name__ == "__main__":
    main()

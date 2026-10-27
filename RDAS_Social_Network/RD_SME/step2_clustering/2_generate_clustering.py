"""
Step 3 (current): Cluster authors per GARD group using HDBSCAN

This script consumes the step2 summary CSV (gard_embeddings_summary.csv), loads
each group's embeddings CSV, and performs HDBSCAN clustering with an optional
grid search that prioritizes diverse, balanced clusters (avoids one dominant cluster).

Defaults point to the embeddings produced by step 1_generate_embeddings.py.
"""

import os
import argparse
import pandas as pd
import numpy as np
from tqdm import tqdm
from sklearn.metrics import silhouette_score
from sklearn.metrics.pairwise import cosine_distances


def grid_search_hdbscan(X, chosen_metric, verbose=True, prefer_granular=False, main_cluster_threshold=0.3):
    """
    Grid search HDBSCAN parameters with a diversity-aware combined score.
    Returns (best_params, best_score).
    """
    import itertools
    import hdbscan

    min_samples_values = [3, 5, 8]
    cluster_selection_epsilon_values = [0.1, 0.15, 0.2, 0.25]
    cluster_selection_method_values = ["eom", "leaf"]

    best_score = -1
    best_params = None

    if verbose:
        print("  Starting HDBSCAN grid search...")
        print(f"    Testing {len(min_samples_values) * len(cluster_selection_epsilon_values) * len(cluster_selection_method_values)} combinations")

    for min_samples in min_samples_values:
        n_samples = len(X)
        min_cluster_size_values = sorted(list(set([
            5, 10, 15, max(8, int(0.01 * n_samples))
        ])))

        for min_cluster_size, epsilon, method in itertools.product(
            min_cluster_size_values, cluster_selection_epsilon_values, cluster_selection_method_values
        ):
            try:
                if chosen_metric == 'euclidean':
                    clusterer = hdbscan.HDBSCAN(
                        min_cluster_size=min_cluster_size,
                        min_samples=min_samples,
                        cluster_selection_epsilon=epsilon,
                        cluster_selection_method=method,
                        metric='euclidean'
                    )
                    labels = clusterer.fit_predict(X)
                else:
                    distances = cosine_distances(X)
                    clusterer = hdbscan.HDBSCAN(
                        min_cluster_size=min_cluster_size,
                        min_samples=min_samples,
                        cluster_selection_epsilon=epsilon,
                        cluster_selection_method=method,
                        metric='precomputed'
                    )
                    labels = clusterer.fit_predict(distances)

                if len(set(labels)) > 1:
                    dbcv_score = hdbscan.validity.validity_index(X, labels, metric=chosen_metric)
                    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
                    n_noise = list(labels).count(-1)
                    noise_ratio = n_noise / len(X)

                    cluster_sizes = [list(labels).count(i) for i in set(labels) if i != -1]
                    if cluster_sizes:
                        largest_cluster_ratio = max(cluster_sizes) / len(X)
                        severe_threshold = main_cluster_threshold + 0.1
                        moderate_threshold = main_cluster_threshold
                        light_threshold = max(0.0, main_cluster_threshold - 0.1)
                        if largest_cluster_ratio > severe_threshold:
                            main_cluster_penalty = 0.0
                        elif largest_cluster_ratio > moderate_threshold:
                            main_cluster_penalty = 0.3
                        elif largest_cluster_ratio > light_threshold:
                            main_cluster_penalty = 0.7
                        else:
                            main_cluster_penalty = 1.0
                        cluster_balance = 1.0 - (np.std(cluster_sizes) / np.mean(cluster_sizes) / 2.0)
                        cluster_balance = max(0.0, min(1.0, cluster_balance))
                    else:
                        largest_cluster_ratio = 0.0
                        main_cluster_penalty = 0.0
                        cluster_balance = 0.0

                    cluster_count_score = min(1.0, n_clusters / 20.0)
                    if prefer_granular:
                        combined_score = (0.2 * dbcv_score + 0.3 * cluster_count_score +
                                          0.4 * main_cluster_penalty + 0.1 * cluster_balance)
                    else:
                        combined_score = (0.3 * dbcv_score + 0.3 * cluster_count_score +
                                          0.3 * main_cluster_penalty + 0.1 * cluster_balance)

                    if combined_score > best_score:
                        best_score = combined_score
                        best_params = {
                            'min_samples': min_samples,
                            'min_cluster_size': min_cluster_size,
                            'cluster_selection_epsilon': epsilon,
                            'cluster_selection_method': method
                        }
                        if verbose:
                            print(f"    New best: DBCV={dbcv_score:.3f}, Combined={combined_score:.3f}, Clusters={n_clusters}, Noise={noise_ratio:.1%}")
                            print(f"             Main cluster: {largest_cluster_ratio:.1%}, Params: {best_params}")
            except Exception as e:
                if verbose:
                    print(f"    Failed params: min_samples={min_samples}, min_cluster_size={min_cluster_size}, epsilon={epsilon}, method={method}, error={e}")
                continue

    if verbose and best_params:
        print(f"  Grid search complete. Best combined score: {best_score:.3f}")
        print(f"  Best parameters: {best_params}")

    return best_params, best_score


def cluster_with_hdbscan(summary_file, output_path,
                         use_gpu=False, grid_search=True,
                         prefer_granular=False, main_cluster_threshold=0.3,
                         grid_sample_max_points=20000, grid_sample_fraction=None,
                         metrics_max_points=10000, random_state=42):
    """Apply HDBSCAN clustering to author embeddings for each GARD group"""
    print(f"Applying HDBSCAN clustering to embeddings from {summary_file}")

    os.makedirs(output_path, exist_ok=True)

    summary_df = pd.read_csv(summary_file)
    print(f"Loaded summary for {len(summary_df)} GARD groups")

    # Check for GPU support if requested
    if use_gpu:
        try:
            import cudf  # noqa: F401
            from cuml.cluster import HDBSCAN as cuHDBSCAN  # noqa: F401
            print("GPU acceleration enabled with RAPIDS cuML")
            gpu_available = True
        except Exception:
            print("Warning: GPU acceleration requested but RAPIDS not available. Falling back to CPU.")
            gpu_available = False
    else:
        gpu_available = False

    all_clusters = []

    for _, row in tqdm(summary_df.iterrows(), total=len(summary_df), desc="Clustering GARD groups"):
        gard_id = row['gard_id']
        embeddings_file = row['embeddings_file']
        try:
            print(f"Clustering {gard_id}")

            embeddings_df = pd.read_csv(embeddings_file)
            if len(embeddings_df) < 5:
                print(f"  Skipping {gard_id}: Not enough authors ({len(embeddings_df)})")
                continue

            feature_cols = [c for c in embeddings_df.columns if c.startswith('feature_')]
            X = embeddings_df[feature_cols].values
            chosen_metric = 'euclidean'  # step2 already applied L2/UMAP as configured

            # Sample for grid search to avoid O(N^2) costs on full set
            X_for_search = X
            if grid_search:
                n = len(X)
                if grid_sample_fraction is not None and 0 < grid_sample_fraction < 1.0:
                    k = max(1000, int(n * grid_sample_fraction))
                else:
                    k = min(n, int(grid_sample_max_points))
                if n > k:
                    rng = np.random.default_rng(seed=random_state)
                    idx = rng.choice(n, size=k, replace=False)
                    X_for_search = X[idx]
                    print(f"  Grid search sampling: {k}/{n} points")

            if grid_search:
                best_params, _ = grid_search_hdbscan(X_for_search, chosen_metric, verbose=True, prefer_granular=prefer_granular, main_cluster_threshold=main_cluster_threshold)
                if best_params is None:
                    best_params = {'min_samples': 5, 'min_cluster_size': 10, 'cluster_selection_epsilon': 0.2, 'cluster_selection_method': 'eom'}
            else:
                best_params = {'min_samples': 5, 'min_cluster_size': 10, 'cluster_selection_epsilon': 0.2, 'cluster_selection_method': 'eom'}

            # Final clustering (CPU path for stability)
            import hdbscan
            if chosen_metric == 'euclidean':
                # Use parallel core distance computation if available
                clusterer = hdbscan.HDBSCAN(**best_params, metric='euclidean', core_dist_n_jobs=os.cpu_count())
                labels = clusterer.fit_predict(X)
            else:
                distances = cosine_distances(X)
                clusterer = hdbscan.HDBSCAN(**best_params, metric='precomputed', core_dist_n_jobs=os.cpu_count())
                labels = clusterer.fit_predict(distances)

            embeddings_df['cluster'] = labels

            labels_list = labels.tolist()
            n_clusters = len(set(labels_list)) - (1 if -1 in labels_list else 0)
            n_noise = labels_list.count(-1)

            quality_scores = {'silhouette': None, 'dbcv': None}
            # Compute metrics only on manageable sample sizes
            n_total = len(X)
            eval_idx = None
            if n_total > metrics_max_points:
                rng = np.random.default_rng(seed=random_state)
                eval_idx = rng.choice(n_total, size=metrics_max_points, replace=False)
                X_eval = X[eval_idx]
                labels_eval = labels[eval_idx]
                print(f"  Metrics sampling: {metrics_max_points}/{n_total} points")
            else:
                X_eval = X
                labels_eval = labels
            try:
                if n_clusters > 1:
                    mask = labels_eval != -1
                    if np.any(mask):
                        quality_scores['silhouette'] = silhouette_score(X_eval[mask], labels_eval[mask], metric='euclidean')
            except Exception:
                pass
            try:
                import hdbscan as _hdb  # reuse for validity index
                quality_scores['dbcv'] = _hdb.validity.validity_index(X_eval, labels_eval, metric=chosen_metric)
            except Exception:
                pass

            print(f"  {gard_id}: {n_clusters} clusters, {n_noise} noise")

            clustered_file = f"{output_path}/clustered_{gard_id.replace(':', '_')}.csv"
            embeddings_df.to_csv(clustered_file, index=False)

            all_clusters.append({
                'gard_id': gard_id,
                'num_authors': len(embeddings_df),
                'num_clusters': n_clusters,
                'num_noise': n_noise,
                'silhouette_score': quality_scores['silhouette'],
                'dbcv_score': quality_scores['dbcv'],
                'clustered_file': clustered_file
            })

        except Exception as e:
            print(f"  Error clustering {gard_id}: {e}")

    clusters_summary_file = f"{output_path}/gard_clusters_summary.csv"
    clusters_summary_df = pd.DataFrame(all_clusters)
    clusters_summary_df.to_csv(clusters_summary_file, index=False)
    print(f"Saved summary of {len(clusters_summary_df)} clustered GARD groups")

    return clusters_summary_df, clusters_summary_file


def main():
    parser = argparse.ArgumentParser(description="Cluster authors per GARD group using HDBSCAN")
    parser.add_argument("--input", type=str, default="step2_clustering/embeddings/gard_embeddings_summary.csv", help="Path to embeddings summary CSV file")
    parser.add_argument("--output", type=str, default="step2_clustering/clustering", help="Output directory for clustering results")
    parser.add_argument("--gpu", action="store_true", help="Use GPU acceleration with RAPIDS cuML (fallback to CPU if unavailable)")
    parser.add_argument("--no-grid-search", action="store_true", help="Disable grid search for HDBSCAN parameters (use defaults)")
    parser.add_argument("--prefer-granular", action="store_true", help="Prefer more granular clustering (diversity-oriented scoring)")
    parser.add_argument("--main-cluster-threshold", type=float, default=0.3, help="Max allowed size for the largest cluster (fraction of total)")
    parser.add_argument("--grid-sample-max-points", type=int, default=20000, help="Max points used for grid search sampling (default: 20000)")
    parser.add_argument("--grid-sample-fraction", type=float, default=None, help="If set (0-1), sample this fraction for grid search")
    parser.add_argument("--metrics-max-points", type=int, default=10000, help="Max points used when computing quality metrics (default: 10000)")
    parser.add_argument("--random-state", type=int, default=42, help="Random seed for sampling")

    args = parser.parse_args()

    cluster_with_hdbscan(
        summary_file=args.input,
        output_path=args.output,
        use_gpu=args.gpu,
        grid_search=not args.no_grid_search,
        prefer_granular=args.prefer_granular,
        main_cluster_threshold=args.main_cluster_threshold,
        grid_sample_max_points=args.grid_sample_max_points,
        grid_sample_fraction=args.grid_sample_fraction,
        metrics_max_points=args.metrics_max_points,
        random_state=args.random_state
    )


if __name__ == "__main__":
    main()

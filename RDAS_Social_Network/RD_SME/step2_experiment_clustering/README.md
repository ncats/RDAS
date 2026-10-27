# Clustering method comparison

This code compares HDBSCAN with K-means on 1,000 sampled researcher embeddings.
The retained `clustering_method_comparison.csv` and `.md` contain aggregate
metrics only. Researcher-level sampled embeddings and assignments are omitted.

From the repository root, with approved embeddings available, run:

```bash
python step2_experiment_clustering/compare_hdbscan_vs_kmeans.py \
  --embeddings-csv data/processed/step2/embeddings/embeddings_GARD_0006233.csv \
  --output-dir data/processed/experiment --sample-size 1000 --random-state 42
```

The script selects HDBSCAN settings using a diversity-aware score. It selects
K-means `k` by silhouette score, with Davies-Bouldin as a tie breaker.
HDBSCAN label `-1` is treated as an individual researcher in the downstream
pipeline; these researchers are excluded from HDBSCAN cluster-quality metrics.
K-means assigns all researchers to clusters, so its metrics cover all 1,000.
Read scores together with the individual-researcher count and cluster count.
The generated `data/processed/experiment/` directory is Git-ignored.

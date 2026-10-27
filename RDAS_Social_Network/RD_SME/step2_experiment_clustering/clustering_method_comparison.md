| method | sample_size | random_state | num_clusters | num_noise | noise_ratio | silhouette_score | davies_bouldin_index | model_selection_score | selected_params_json |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Current HDBSCAN | 1000 | 42 | 35 | 669 | 0.6690 | 0.5004 | 0.6591 | 0.7166 | {"cluster_selection_epsilon": 0.1, "cluster_selection_method": "leaf", "min_cluster_size": 5, "min_samples": 3} |
| KMeans | 1000 | 42 | 2 | 0 | 0.0000 | 0.4021 | 3.3787 | 0.4021 | {"n_clusters": 2, "n_init": "auto", "random_state": 42} |

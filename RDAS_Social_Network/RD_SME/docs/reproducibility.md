# Reproducibility notes

Run commands from the repository root. `scripts/run_pipeline.sh` executes:

1. Five SQL extraction scripts, writing JSON to `data/processed/step1/`.
2. Author background aggregation and embedding generation.
3. HDBSCAN clustering and LLM expertise generation. Label `-1` is processed
   as an individual researcher, outside the clusters.
4. Node and relationship CSV generation, keyword expertise post-processing,
   and count checks, writing to `data/processed/csv_files/`.

The launcher does not import base graph nodes or generate paper maps.
The figure scripts are included in `graph_for_paper/`, with local paths, but
they require the approved CSV and geographic inputs listed below.
The optional affiliation location script also requires the external city data
and a spaCy `en_core_web_sm` model. Set `USCITIES_CSV` to run that optional
step in the launcher.

## Models and software

The embedding code names `all-MiniLM-L6-v2` and
`dmis-lab/biobert-base-cased-v1.1`. Expertise generation uses a local
Llama-3.1-70B-Instruct model via vLLM. `requirements-pipeline.txt` and
`requirements-llm.txt` provide pinned reference package versions observed in
the project environments. They have not yet been validated as one clean
installation. The original model snapshot commit hashes are not recorded in
this folder; record them before claiming an exact reproduction of the paper.
GPU/CUDA requirements depend on the selected machine and model deployment.

## Paper figures

After supplying approved `data/processed/csv_files/` inputs, install
`requirements-figures.txt`. The geographical and case-study scripts also
expect an approved `data/external/uscities.csv`. The cluster bubble and word
cloud scripts expect an approved
`data/external/researchers_per_cluster.csv` with `cluster_id,researchers`
columns. Map scripts can fetch county centroids and state outlines from
external sources if local caches are absent; check provenance before relying
on those downloads. The Graphviz diagram also needs the Graphviz `dot`
executable. Figure outputs go to `data/processed/figures/`.

```bash
python graph_for_paper/generate_affiliation_summary_table.py
python graph_for_paper/generate_cluster_size_distribution.py
python graph_for_paper/generate_cluster_graphs.py
python graph_for_paper/generate_word_cloud_for_cluster.py
python graph_for_paper/generate_cluster_expertise_case_study.py
python graph_for_paper/generate_maps.py
python graph_for_paper/generate_maps_inset.py
python graph_for_paper/generate_geo_pipeline_graph.py
```

These commands invoke the figure scripts. No figure was regenerated from
private data as part of this release candidate.

## Clustering method comparison

Given approved researcher embeddings, run:

```bash
python step2_experiment_clustering/compare_hdbscan_vs_kmeans.py \
  --embeddings-csv data/processed/step2/embeddings/embeddings_GARD_0006233.csv \
  --output-dir data/processed/experiment \
  --sample-size 1000 --random-state 42
```

The retained [aggregate table](../step2_experiment_clustering/clustering_method_comparison.md)
reports 35 HDBSCAN clusters and 669 individual researchers in the sample.
Silhouette and Davies-Bouldin values for HDBSCAN use only clustered
researchers; K-means values use all 1,000 sampled researchers. The methods
also select settings using different objectives. Interpret the table with
those differences visible.

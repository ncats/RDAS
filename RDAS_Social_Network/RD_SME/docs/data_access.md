# Data access and publication scope

The extraction scripts query an institutional SQL database for GARD, article,
clinical-trial, grant, and researcher records. Access to that database is not
provided by this code release. The source and output fields are documented in
`data_extraction_schema.md` and `csv_output_schema.md`.

Generated JSON, graph CSVs, embeddings, cluster assignments, author names,
ORCIDs, and affiliations are excluded from this repository. The records in
`examples/synthetic/` are invented solely to exercise the code. The retained
method-comparison table has aggregate metrics only.

Affiliation geocoding requires a separately obtained `uscities.csv` file.
Place an approved copy at `data/external/uscities.csv` or pass
`--uscities-csv`. Verify the source license and attribution before distributing
that dataset. The historical county shapefile and map assets are not included.
Figure code is included separately and writes to `data/processed/figures/`.

Use only source data and model snapshots for which you have access and
redistribution rights. Decide separately whether paper figures that label
researchers or institutions can be released.

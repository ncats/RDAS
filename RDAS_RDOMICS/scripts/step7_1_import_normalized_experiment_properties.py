"""Import normalized experiment properties into the graph database."""

import argparse
import os
from pathlib import Path
import sys

import pandas as pd
from neo4j import GraphDatabase

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from utils import load_paths


def import_normalized_properties(config_path: str | None = None) -> None:
    neo4j_uri = os.environ.get("NEO4J_URI")
    neo4j_user = os.environ.get("NEO4J_USER")
    neo4j_password = os.environ.get("NEO4J_PASSWORD")
    if not all([neo4j_uri, neo4j_user, neo4j_password]):
        raise ValueError("Set NEO4J_URI, NEO4J_USER, and NEO4J_PASSWORD before running this step.")

    paths = load_paths(config_path)
    experiment_file_path = paths["experiment_node_normalized"]

    print("Reading experiment data from CSV file...")
    exp_df = pd.read_csv(experiment_file_path)

    print("Connecting to Memgraph database...")
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    batch_size = 500
    total_updated = 0

    with driver.session() as session:
        for i in range(0, len(exp_df), batch_size):
            batch = exp_df.iloc[i:i+batch_size]
            batch_updated = 0

            for _, row in batch.iterrows():
                experiment_id = row['Experiment_id']
                omics_type_norm = row['Omics_type_norm'] if pd.notna(row['Omics_type_norm']) else ""
                sequencing_type_norm = row['Sequencing_type_norm'] if pd.notna(row['Sequencing_type_norm']) else ""

                if not omics_type_norm and not sequencing_type_norm:
                    continue

                query = """
                MATCH (e:Experiment {Experiment_id: $experiment_id})
                SET e.Omics_type_norm = $omics_type_norm,
                    e.Sequencing_type_norm = $sequencing_type_norm
                RETURN count(e) as updated
                """

                result = session.run(
                    query,
                    experiment_id=experiment_id,
                    omics_type_norm=omics_type_norm,
                    sequencing_type_norm=sequencing_type_norm
                )
                batch_updated += result.single()["updated"]

            total_updated += batch_updated
            print(f"Processed batch {i//batch_size + 1}, updated {batch_updated} nodes")

    driver.close()
    print(f"Import complete. Updated {total_updated} Experiment nodes with normalized properties.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import normalized experiment properties.")
    parser.add_argument("--config", help="Optional path to a YAML config file.")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    import_normalized_properties(args.config)

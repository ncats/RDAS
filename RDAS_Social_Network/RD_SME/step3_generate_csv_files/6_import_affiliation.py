#!/usr/bin/env python3
"""
Import Location nodes and RESEARCHER_LOCATION relationships into Memgraph/Neo4j.

Behavior:
- Reads affiliation_node.csv (author_id, Aff_city, Aff_state, Aff_country, Aff_FIPS)
- Skips rows where all location fields are empty (only author_id present)
- MERGE (loc:Location {author_id}) and SET location properties
- MATCH (r:Researcher {author_id}) and MERGE (r)-[:RESEARCHER_LOCATION]->(loc)
"""

import argparse
import os
from typing import Dict

import pandas as pd
from tqdm import tqdm
from neo4j import GraphDatabase

# Connection settings (mirrors process_clustering/step5 example)
NEO4J_URI = os.getenv("GRAPH_URI", "")
NEO4J_USER = os.getenv("GRAPH_USER", "")
NEO4J_PASSWORD = os.getenv("GRAPH_PASSWORD", "")

DEFAULT_AFFILIATION_CSV = "csv_files/affiliation_node.csv"


def get_driver():
    if not all((NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)):
        raise RuntimeError("Set GRAPH_URI, GRAPH_USER, and GRAPH_PASSWORD before importing")
    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASSWORD),
        max_transaction_retry_time=60,
        connection_timeout=30,
        max_connection_lifetime=3600,
    )
    # Quick connectivity check
    with driver.session() as session:
        session.run("RETURN 1").single()
    return driver


def row_has_location(props: Dict[str, str]) -> bool:
    return any(
        bool(str(props.get(k, "")).strip())
        for k in ("Aff_city", "Aff_state", "Aff_country", "Aff_FIPS")
    )


def import_affiliations(driver, csv_path: str, batch_size: int) -> None:
    total_rows = max(0, sum(1 for _ in open(csv_path, "r", encoding="utf-8")) - 1)
    pbar = tqdm(total=total_rows, desc="Affiliations", unit="rows")

    # Process in chunks
    for chunk in pd.read_csv(csv_path, chunksize=batch_size, dtype=str, keep_default_na=False):
        # Normalize column names
        required_cols = ["author_id", "Aff_city", "Aff_state", "Aff_country", "Aff_FIPS"]
        for c in required_cols:
            if c not in chunk.columns:
                chunk[c] = ""

        with driver.session() as session:
            with session.begin_transaction() as tx:
                for _, row in chunk.iterrows():
                    author_id = str(row.get("author_id", "")).strip()
                    if not author_id:
                        continue
                    props = {
                        "author_id": author_id,
                        "Aff_city": str(row.get("Aff_city", "")).strip(),
                        "Aff_state": str(row.get("Aff_state", "")).strip(),
                        "Aff_country": str(row.get("Aff_country", "")).strip(),
                        "Aff_FIPS": str(row.get("Aff_FIPS", "")).strip(),
                    }
                    # Skip if no location info present
                    if not row_has_location(props):
                        continue

                    # Upsert Location node and relationship
                    query = (
                        "MERGE (loc:Location {author_id: $author_id}) "
                        "SET loc.Aff_city = $Aff_city, "
                        "    loc.Aff_state = $Aff_state, "
                        "    loc.Aff_country = $Aff_country, "
                        "    loc.Aff_FIPS = $Aff_FIPS "
                        "WITH loc "
                        "MATCH (r:Researcher {author_id: $author_id}) "
                        "MERGE (r)-[:RESEARCHER_LOCATION]->(loc)"
                    )
                    tx.run(query, **props)
        pbar.update(len(chunk))
    pbar.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import Location nodes and RESEARCHER_LOCATION relationships.")
    parser.add_argument(
        "--affiliation_csv",
        type=str,
        default=DEFAULT_AFFILIATION_CSV,
        help="Path to affiliation_node.csv",
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=1000,
        help="Batch size for processing CSV",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not os.path.exists(args.affiliation_csv):
        print(f"❌ Missing input CSV: {args.affiliation_csv}")
        return 2

    try:
        driver = get_driver()
        print("✅ Connected to Neo4j/Memgraph")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return 1

    try:
        print(f"Importing from: {args.affiliation_csv}")
        import_affiliations(driver, args.affiliation_csv, args.batch_size)
        print("✅ Import completed.")
    except Exception as e:
        print(f"❌ Import failed: {e}")
        driver.close()
        return 1

    driver.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

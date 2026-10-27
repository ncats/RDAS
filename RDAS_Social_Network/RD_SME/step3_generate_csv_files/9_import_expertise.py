#!/usr/bin/env python3
"""
Import keyword-level Expertise nodes and relationships into Memgraph/Neo4j.

Behavior:
- Reads expertise_node_keyword.csv for Expertise nodes
- Reads author_expertise_relationships_keyword.csv for Researcher->Expertise
- Reads cluster_expertise_relationships_keyword.csv for Cluster->Expertise
- Uses relationship types RESEARCHER_HAS_EXPERTISE and CLUSTER_HAS_EXPERTISE
- Sets expertise_rank on relationships when present
"""

import argparse
import os
from typing import Optional

import pandas as pd
from tqdm import tqdm
from neo4j import GraphDatabase

# Connection settings (mirrors 6_import_affiliation.py)
NEO4J_URI = os.getenv("GRAPH_URI", "")
NEO4J_USER = os.getenv("GRAPH_USER", "")
NEO4J_PASSWORD = os.getenv("GRAPH_PASSWORD", "")

DEFAULT_EXPERTISE_NODE_CSV = "csv_files/expertise_node_keyword.csv"
DEFAULT_AUTHOR_REL_CSV = "csv_files/author_expertise_relationships_keyword.csv"
DEFAULT_CLUSTER_REL_CSV = "csv_files/cluster_expertise_relationships_keyword.csv"


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
    with driver.session() as session:
        session.run("RETURN 1").single()
    return driver


def _count_rows(path: str) -> int:
    return max(0, sum(1 for _ in open(path, "r", encoding="utf-8")) - 1)


def _parse_rank(value) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in ("nan", "none"):
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def import_expertise_nodes(driver, csv_path: str, batch_size: int) -> None:
    total_rows = _count_rows(csv_path)
    pbar = tqdm(total=total_rows, desc="Expertise nodes", unit="rows", ascii=True)

    for chunk in pd.read_csv(csv_path, chunksize=batch_size, dtype=str, keep_default_na=False):
        required_cols = ["expertise_id", "expertise_keyword", "expertise_keyword_norm", "gard_id"]
        for c in required_cols:
            if c not in chunk.columns:
                chunk[c] = ""

        with driver.session() as session:
            with session.begin_transaction() as tx:
                for _, row in chunk.iterrows():
                    expertise_id = str(row.get("expertise_id", "")).strip()
                    if not expertise_id:
                        continue
                    props = {
                        "expertise_id": expertise_id,
                        "expertise_keyword": str(row.get("expertise_keyword", "")).strip(),
                        "expertise_keyword_norm": str(row.get("expertise_keyword_norm", "")).strip(),
                        "gard_id": str(row.get("gard_id", "")).strip(),
                    }
                    query = (
                        "MERGE (e:Expertise {expertise_id: $expertise_id}) "
                        "SET e.expertise_keyword = $expertise_keyword, "
                        "    e.expertise_keyword_norm = $expertise_keyword_norm, "
                        "    e.gard_id = $gard_id"
                    )
                    tx.run(query, **props)
        pbar.update(len(chunk))
    pbar.close()


def import_researcher_expertise(driver, csv_path: str, batch_size: int) -> None:
    total_rows = _count_rows(csv_path)
    pbar = tqdm(total=total_rows, desc="Researcher expertise", unit="rows", ascii=True)

    for chunk in pd.read_csv(csv_path, chunksize=batch_size, dtype=str, keep_default_na=False):
        required_cols = ["author_id", "expertise_id"]
        for c in required_cols:
            if c not in chunk.columns:
                chunk[c] = ""
        has_rank = "expertise_rank" in chunk.columns

        with driver.session() as session:
            with session.begin_transaction() as tx:
                for _, row in chunk.iterrows():
                    author_id = str(row.get("author_id", "")).strip()
                    expertise_id = str(row.get("expertise_id", "")).strip()
                    if not author_id or not expertise_id:
                        continue
                    rank = _parse_rank(row.get("expertise_rank")) if has_rank else None
                    if rank is None:
                        query = (
                            "MATCH (r:Researcher {author_id: $author_id}) "
                            "MATCH (e:Expertise {expertise_id: $expertise_id}) "
                            "MERGE (r)-[:RESEARCHER_HAS_EXPERTISE]->(e)"
                        )
                        tx.run(query, author_id=author_id, expertise_id=expertise_id)
                    else:
                        query = (
                            "MATCH (r:Researcher {author_id: $author_id}) "
                            "MATCH (e:Expertise {expertise_id: $expertise_id}) "
                            "MERGE (r)-[rel:RESEARCHER_HAS_EXPERTISE]->(e) "
                            "SET rel.expertise_rank = $expertise_rank"
                        )
                        tx.run(
                            query,
                            author_id=author_id,
                            expertise_id=expertise_id,
                            expertise_rank=rank,
                        )
        pbar.update(len(chunk))
    pbar.close()


def import_cluster_expertise(driver, csv_path: str, batch_size: int) -> None:
    total_rows = _count_rows(csv_path)
    pbar = tqdm(total=total_rows, desc="Cluster expertise", unit="rows", ascii=True)

    for chunk in pd.read_csv(csv_path, chunksize=batch_size, dtype=str, keep_default_na=False):
        required_cols = ["cluster_id", "expertise_id"]
        for c in required_cols:
            if c not in chunk.columns:
                chunk[c] = ""
        has_rank = "expertise_rank" in chunk.columns

        with driver.session() as session:
            with session.begin_transaction() as tx:
                for _, row in chunk.iterrows():
                    cluster_id = str(row.get("cluster_id", "")).strip()
                    expertise_id = str(row.get("expertise_id", "")).strip()
                    if not cluster_id or not expertise_id:
                        continue
                    rank = _parse_rank(row.get("expertise_rank")) if has_rank else None
                    if rank is None:
                        query = (
                            "MATCH (c:Cluster {cluster_id: $cluster_id}) "
                            "MATCH (e:Expertise {expertise_id: $expertise_id}) "
                            "MERGE (c)-[:CLUSTER_HAS_EXPERTISE]->(e)"
                        )
                        tx.run(query, cluster_id=cluster_id, expertise_id=expertise_id)
                    else:
                        query = (
                            "MATCH (c:Cluster {cluster_id: $cluster_id}) "
                            "MATCH (e:Expertise {expertise_id: $expertise_id}) "
                            "MERGE (c)-[rel:CLUSTER_HAS_EXPERTISE]->(e) "
                            "SET rel.expertise_rank = $expertise_rank"
                        )
                        tx.run(
                            query,
                            cluster_id=cluster_id,
                            expertise_id=expertise_id,
                            expertise_rank=rank,
                        )
        pbar.update(len(chunk))
    pbar.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import Expertise nodes and Researcher/Cluster relationships."
    )
    parser.add_argument(
        "--expertise_nodes",
        type=str,
        default=DEFAULT_EXPERTISE_NODE_CSV,
        help="Path to expertise_node_keyword.csv",
    )
    parser.add_argument(
        "--author_relationships",
        type=str,
        default=DEFAULT_AUTHOR_REL_CSV,
        help="Path to author_expertise_relationships_keyword.csv",
    )
    parser.add_argument(
        "--cluster_relationships",
        type=str,
        default=DEFAULT_CLUSTER_REL_CSV,
        help="Path to cluster_expertise_relationships_keyword.csv",
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

    missing = [p for p in (args.expertise_nodes, args.author_relationships, args.cluster_relationships) if not os.path.exists(p)]
    if missing:
        print("ERROR: Missing input files:")
        for path in missing:
            print(f"  - {path}")
        return 2

    try:
        driver = get_driver()
        print("Connected to Neo4j/Memgraph")
    except Exception as exc:
        print(f"ERROR: Connection failed: {exc}")
        return 1

    try:
        print(f"Importing expertise nodes from: {args.expertise_nodes}")
        import_expertise_nodes(driver, args.expertise_nodes, args.batch_size)
        print(f"Importing researcher expertise from: {args.author_relationships}")
        import_researcher_expertise(driver, args.author_relationships, args.batch_size)
        print(f"Importing cluster expertise from: {args.cluster_relationships}")
        import_cluster_expertise(driver, args.cluster_relationships, args.batch_size)
        print("Import completed.")
    except Exception as exc:
        print(f"ERROR: Import failed: {exc}")
        driver.close()
        return 1

    driver.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

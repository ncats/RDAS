import json
import os
import sys
from typing import Any, Dict, List, Optional

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase

"""
Create MeshTerm nodes for new publication articles.

For each new row in publication_article (is_new = 1), parse
source_json.meshHeadingList.meshHeading[].descriptorName, create MeshTerm nodes,
and link each MeshTerm to the matching Article with has_mesh_term.
"""

# Reference: C_publication/initializer/mesh_term.py


class NewPublicationMeshTermGraphTask(PipelineBase):
    """Create MeshTerm nodes and Article relationships for new publications."""

    BATCH_SIZE = 50

    # Each chunk represents one Article with zero or more MeSH descriptors.
    # UNWIND expands those descriptors into individual MeshTerm nodes and links.
    BATCH_CREATE = '''
        UNWIND $chunks AS chunk
        MATCH (p:Article {pubmedId: chunk.pubmedId})

        UNWIND chunk.meshTerms AS meshTerm
        MERGE (m:MeshTerm {meshTerm: meshTerm})
        MERGE (m)-[:has_mesh_term]->(p)
    '''

    # MeSH headings live inside source_json, so only rows with publication JSON
    # can produce MeshTerm graph updates.
    FETCH_NEW_ARTICLES_QUERY = '''
        SELECT
            pubmed_id, source_json
        FROM publication_article
        WHERE is_new = 1
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        self.logger.info("NewPublicationMeshTermGraphTask does not use find_new_data().")


    # implement
    def process_new_data(self) -> None:
        """Read new publications, extract MeSH terms, and batch graph updates."""

        fetch_cursor = None
        count = 0
        batch_num = 0

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(self.FETCH_NEW_ARTICLES_QUERY)

            while True:
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    self.logger.info("No more rows to fetch.")
                    break

                batch_num += 1
                self.logger.info(f"--- batch# = {batch_num} ---")

                chunks = []

                for row in rows:
                    # Convert the staged article row into the shape required by
                    # BATCH_CREATE; rows with no MeSH terms are skipped.
                    mesh_chunk = self._create_mesh_term_chunk(row)

                    if mesh_chunk is None:
                        continue

                    chunks.append(mesh_chunk)

                if not chunks:
                    self.logger.info("No valid MeshTerm mappings to insert into Memgraph.")
                    continue

                try:
                    self.memgraph.execute(self.BATCH_CREATE, {"chunks": chunks})

                    count += len(chunks)
                    mesh_term_count = sum(len(item["meshTerms"]) for item in chunks)
                    self.logger.info(
                        f"Submitted {len(chunks)} Article MeshTerm chunks to Memgraph. "
                        f"#meshTerms = {mesh_term_count}. Total = {count}"
                    )

                except Exception as e:
                    self.logger.error(f"Error executing MeshTerm batch create: {e}")

        except Exception as e:
            self.logger.error(f"Error creating MeshTerm nodes in Memgraph: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def _create_mesh_term_chunk(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Build one Article-to-MeSH chunk from a staged publication row."""

        try:
            pubmed_id = int(row["pubmed_id"])
        except (TypeError, ValueError) as e:
            self.logger.error(f"Invalid pubmed_id found: {row.get('pubmed_id')}. Error: {e}")
            return None

        mesh_terms = self.get_mesh_terms_list(pubmed_id, row.get("source_json"))

        if not mesh_terms:
            return None

        return {
            "pubmedId": pubmed_id,
            "meshTerms": mesh_terms,
        }


    def get_mesh_terms_list(self, pubmed_id: int, source_json: Any) -> List[str]:
        """Parse source_json and return unique descriptor names for one article."""

        try:
            source_obj = json.loads(source_json or "{}")
        except (json.JSONDecodeError, TypeError) as e:
            self.logger.error(f"Error parsing source_json for pubmed_id={pubmed_id}: {e}")
            return []

        # PubMed may return one meshHeading object or a list of them depending
        # on the article, so normalize to a list before iterating.
        mesh_heading_list = (source_obj.get("meshHeadingList") or {}).get("meshHeading", [])

        if not mesh_heading_list:
            return []

        if not isinstance(mesh_heading_list, list):
            mesh_heading_list = [mesh_heading_list]

        mesh_terms = []

        for heading in mesh_heading_list:
            if not isinstance(heading, dict):
                continue

            descriptor_name = heading.get("descriptorName", "")

            if descriptor_name:
                mesh_terms.append(str(descriptor_name))

        # Deduplicate terms within the article so the Cypher batch stays small.
        return sorted(set(mesh_terms))

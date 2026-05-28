import os
import sys
from typing import Any, Dict, List

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _clean, _make_hash_key, _to_int, _to_stripped_string

"""
Create Substance nodes for new publication articles.

Task publication_6 extracts chemical substances from publication_article
into publication_substance. This graph task reads substance rows linked to
publication_article where is_new = 1, creates Substance nodes, and links
the matching Article nodes with:

    (Article)-[:has_substance]->(Substance)
"""

# Reference: C_publication/initializer/substance.py


class NewPublicationSubstanceGraphTask(PipelineBase):
    """Create Substance nodes and link them to new Article nodes."""

    BATCH_SIZE = 50

    # One Substance chunk can point to multiple Article nodes. MERGE keeps the
    # Substance node stable, while ON MATCH only fills missing name/registry data.
    BATCH_CREATE = '''
        UNWIND $substances AS subs
        MERGE (s:Substance {_composite_key: subs._composite_key})
        ON CREATE SET
            s.name = subs.substanceName,
            s.registryNumber = subs.registryNumber
        ON MATCH SET
            s.name = CASE
                WHEN (
                    subs.substanceName IS NOT NULL
                    AND subs.substanceName <> ''
                    AND (s.name IS NULL OR s.name = '')
                )
                THEN subs.substanceName
                ELSE s.name
            END,
            s.registryNumber = CASE
                WHEN (
                    subs.registryNumber IS NOT NULL
                    AND subs.registryNumber <> ''
                    AND (s.registryNumber IS NULL OR s.registryNumber = '')
                )
                THEN subs.registryNumber
                ELSE s.registryNumber
            END

        WITH s, subs
        UNWIND subs.pubmedIdList AS pubmedId
        MATCH (a:Article {pubmedId: pubmedId})
        MERGE (a)-[:has_substance]->(s)
    '''

    # Start from distinct substance hashes so all article links for the same substance are grouped before writing to Memgraph.
    FETCH_NEW_HASH_IDS_QUERY = '''
        SELECT DISTINCT ps.hash_id
        FROM publication_substance AS ps
        INNER JOIN publication_article AS pa
            ON pa.pubmed_id = ps.pubmed_id
        WHERE pa.is_new = 1
        AND ps.hash_id IS NOT NULL
        AND ps.hash_id <> ''
        ORDER BY ps.hash_id
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        self.logger.info("NewPublicationSubstanceGraphTask does not use find_new_data().")


    # implement
    def process_new_data(self) -> None:
        """Read new substance hashes, gather their articles, and update the graph."""

        hash_cursor = None
        data_cursor = None
        count = 0
        batch_num = 0

        try:
            hash_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            data_cursor = self.mysql.cursor(dictionary=True, buffered=True)

            hash_cursor.execute(self.FETCH_NEW_HASH_IDS_QUERY)

            while True:
                rows = hash_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    self.logger.info("No more rows to fetch.")
                    break

                batch_num += 1
                self.logger.info(f"--- batch# = {batch_num} ---")

                hash_ids = [
                    row["hash_id"]
                    for row in rows
                    if row.get("hash_id")
                ]

                if not hash_ids:
                    self.logger.info("No valid substance hash IDs found in this batch.")
                    continue

                # Load the full substance rows for this hash batch and collapse
                # them into graph chunks keyed by one composite Substance ID.
                substances = self._get_substance_chunks(data_cursor, hash_ids)

                if not substances:
                    self.logger.info("No valid Substance nodes to insert into Memgraph.")
                    continue

                try:
                    self.memgraph.execute(self.BATCH_CREATE, {"substances": substances})

                    count += len(substances)
                    relation_count = sum(len(substance["pubmedIdList"]) for substance in substances)
                    self.logger.info(
                        f"Submitted {len(substances)} Substance nodes to Memgraph. "
                        f"#Article relationships = {relation_count}. Total = {count}"
                    )

                except Exception as e:
                    self.logger.error(f"Error executing Substance batch create: {e}")

        except Exception as e:
            self.logger.error(f"Error creating Substance nodes in Memgraph: {e}")

        finally:
            if hash_cursor:
                hash_cursor.close()

            if data_cursor:
                data_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def _get_substance_chunks(self, cursor, hash_ids: List[str]) -> List[Dict[str, Any]]:
        """Build Substance chunks with all new Article links for each hash ID."""

        placeholders = ",".join(["%s"] * len(hash_ids))

        # Rejoin to publication_article so the graph update only uses substance
        # rows tied to new publications.
        substance_query = f'''
            SELECT DISTINCT 
                ps.pubmed_id, ps.substance_name, ps.registry_number, ps.hash_id
            FROM publication_substance AS ps
            INNER JOIN publication_article AS pa
                ON pa.pubmed_id = ps.pubmed_id
            WHERE pa.is_new = 1
            AND ps.hash_id IN ({placeholders})
            ORDER BY ps.hash_id, ps.pubmed_id
        '''

        cursor.execute(substance_query, hash_ids)
        rows = cursor.fetchall()

        rows_by_hash_id = {}

        for row in rows:
            rows_by_hash_id.setdefault(row.get("hash_id"), []).append(row)

        # Preserve the original hash order from the driving query while
        # gathering the Article IDs and best available Substance properties.
        substances = []

        for hash_id in hash_ids:
            pubmed_id_list = []
            substance_name = None
            registry_number = None

            for row in rows_by_hash_id.get(hash_id, []):
                raw_pubmed_id = row.get("pubmed_id")
                pubmed_id = _to_int(raw_pubmed_id)

                if pubmed_id is not None:
                    pubmed_id_list.append(pubmed_id)
                else:
                    self.logger.error(f"Invalid pubmed_id found: {raw_pubmed_id}.")

                row_substance_name = _to_stripped_string(row.get("substance_name"))
                row_registry_number = _to_stripped_string(row.get("registry_number"))
                row_registry_number = None if row_registry_number == "0" else row_registry_number

                # Use the first non-empty values seen for the shared Substance.
                substance_name = substance_name or row_substance_name
                registry_number = registry_number or row_registry_number

            if not pubmed_id_list or not (substance_name or registry_number):
                continue

            substances.append({
                "pubmedIdList": sorted(set(pubmed_id_list)),
                "substanceName": substance_name or "",
                "registryNumber": registry_number or "",
                "_composite_key": self._make_composite_key(substance_name, registry_number),
            })

            self.logger.info(
                f"hash_id={hash_id}, registry_number={registry_number}, "
                f"substance_name={substance_name}, #pubmed_id={len(set(pubmed_id_list))}"
            )

        return substances


    def _make_composite_key(self, substance_name: Any, registry_number: Any) -> str:
        """Create the Substance identity hash, preferring registry number."""

        if registry_number:
            composite_str = str(registry_number).lower()
        else:
            composite_str = _clean(substance_name).lower()

        composite_str = "".join(composite_str.split())
        return _make_hash_key(composite_str)

import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Set

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _clean, _make_hash_key, _parse_json_list, _time_hms, _to_float

"""
Sync new organization location rows from MySQL back to Memgraph.

1. Reference: G_update/update_organization_location_db_step_3_graph.py.
2. Read organization_location rows where is_new = 1.
3. Update matching Organization nodes with ROR metadata.
4. Create Location nodes when location data exists.
5. Create (Organization)-[:has_location]->(Location) relationships.
6. Merge duplicate Organization nodes by real ror_id.
7. Merge duplicate Location nodes by _idx_key.
"""

# Reference: G_update/update_organization_location_db_step_3_graph.py

class OrganizationLocationGraphSyncTask(PipelineBase):
    """Apply newly staged organization_location rows to the Memgraph graph."""

    BATCH_SIZE = 200
    TABLE_NAME = "organization_location"

    # Relationship types are discovered from the graph and inserted into Cypher
    # as literal tokens, so validate them before constructing merge queries.
    RELATIONSHIP_TYPE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
    
    # Step 1 stores found and not-found ROR lookups in organization_location.
    # This task only consumes rows marked is_new=1 for the current alert run.
    FETCH_NEW_ORGANIZATION_LOCATIONS_QUERY = f'''
        SELECT
            ror_id,
            org_name,
            original_name_in_graph_db,
            original_name_in_graph_db_idx_key,
            website,
            city,
            state,
            country,
            country_code,
            lat,
            lng,
            geonames_id,
            established,
            status,
            types,
            full_data,
            is_new
        FROM {TABLE_NAME}
        WHERE is_new = 1
    '''

    # A chunk with location data updates the Organization and links it to a
    # Location node. A chunk without location data marks the Organization as
    # not found so it will not be repeatedly searched.
    BATCH_UPDATE_ORGANIZATIONS = '''
        UNWIND $chunks AS chunk

        MERGE (org:Organization {_idx_key: chunk.orgIdxKey})

        FOREACH (_ IN CASE WHEN chunk.hasLocation THEN [1] ELSE [] END |
            SET org.displayName = chunk.displayName,
                org.ror_id = chunk.rorId,
                org.types = chunk.types,
                org.website = chunk.website

            MERGE (loc:Location {_idx_key: chunk.locationIdxKey})
            ON CREATE SET
                loc.facility = '',
                loc.address = '',
                loc.zip = '',
                loc.city = chunk.city,
                loc.state = chunk.state,
                loc.country = chunk.country,
                loc.countryCode = chunk.countryCode

            MERGE (org)-[:has_location]->(loc)
        )

        FOREACH (_ IN CASE WHEN NOT chunk.hasLocation THEN [1] ELSE [] END |
            SET org.ror_id = 'N/A'
        )
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("OrganizationLocationGraphSyncTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Read staged location rows and sync Organization/Location graph data."""

        fetch_cursor = None
        total_updated = 0
        batch_num = 0
        start_time = time.time()

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(self.FETCH_NEW_ORGANIZATION_LOCATIONS_QUERY)

            while True:
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    self.logger.info("No more new organization_location rows to fetch.")
                    break

                batch_num += 1
                batch_start = time.time()

                # Convert raw MySQL rows into the exact payload shape expected
                # by the Cypher batch update.
                chunks = self.create_organization_location_chunks(rows)

                if not chunks:
                    self.logger.info(f"Batch #{batch_num}: no valid Organization location chunks.")
                    continue

                try:
                    self.memgraph.execute(self.BATCH_UPDATE_ORGANIZATIONS, {"chunks": chunks})

                    with_location_count = sum(1 for chunk in chunks if chunk["hasLocation"])
                    without_location_count = len(chunks) - with_location_count
                    total_updated += len(chunks)

                    hours, minutes, seconds = _time_hms(time.time() - batch_start)
                    self.logger.info(
                        f"Batch #{batch_num}: updated {len(chunks)} Organization nodes. "
                        f"With location={with_location_count}, without location={without_location_count}. "
                        f"Total updated={total_updated}. "
                        f"Time={hours} hours, {minutes} minutes, {seconds} seconds."
                    )

                except Exception as e:
                    self.logger.error(f"Error syncing organization location batch #{batch_num} to Memgraph: {e}")

            merged_organizations = self.merge_duplicate_organizations()
            merged_locations = self.merge_duplicate_locations()

            total_hours, total_minutes, total_seconds = _time_hms(time.time() - start_time)
            self.logger.info(
                f"Completed OrganizationLocationGraphSyncTask. Total updated={total_updated}. "
                f"Merged organizations={merged_organizations}. "
                f"Merged locations={merged_locations}. "
                f"Time={total_hours} hours, {total_minutes} minutes, {total_seconds} seconds."
            )

        except Exception as e:
            self.logger.error(f"OrganizationLocationGraphSyncTask failed: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def create_organization_location_chunks(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:

        '''
        Convert organization_location rows into the Cypher payload shape.
        Rows without org_name are treated as ROR not-found rows and only mark
        the matching Organization node with ror_id = 'N/A'.
        '''
        chunks = []

        for row in rows:
            chunk = self.create_organization_location_chunk(row)

            if chunk:
                chunks.append(chunk)

        return chunks


    def create_organization_location_chunk(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create one Organization sync chunk from an organization_location row."""

        org_idx_key = row.get("original_name_in_graph_db_idx_key")

        if not org_idx_key:
            self.logger.info("Skipping organization_location row without original_name_in_graph_db_idx_key.")
            return None

        display_name = _clean(row.get("org_name", ""))

        if not display_name:
            # Rows without org_name represent ROR not-found lookups from step 1.
            return {
                "orgIdxKey": org_idx_key,
                "hasLocation": False
            }

        lat = _to_float(row.get("lat"))
        lng = _to_float(row.get("lng"))
        types = _parse_json_list(row.get("types"))

        # Prefer coordinates for Location identity. If coordinates are missing,
        # fall back to the organization name so a stable key is still produced.
        if lat is not None and lng is not None:
            location_idx_key = _make_hash_key(f"{lat}{lng}")
        else:
            location_idx_key = _make_hash_key(display_name)

        return {
            "displayName": display_name,
            "rorId": _clean(row.get("ror_id", "N/A")),
            "types": types,
            "website": _clean(row.get("website", "")),
            "city": _clean(row.get("city", "")),
            "country": _clean(row.get("country", "")),
            "countryCode": _clean(row.get("country_code", "")),
            "state": _clean(row.get("state", "")),
            "locationIdxKey": location_idx_key,
            "orgIdxKey": org_idx_key,
            "hasLocation": True
        }


    def merge_duplicate_organizations(self) -> int:
        """Merge Organization nodes that now share the same real ROR id."""

        where_clause = "n.ror_id IS NOT NULL AND n.ror_id <> '' AND n.ror_id <> 'N/A'"
        return self.merge_duplicate_nodes_by_property("Organization", "ror_id", where_clause)


    def merge_duplicate_locations(self) -> int:
        """Merge duplicate Location nodes that share the same stable _idx_key."""

        where_clause = "n._idx_key IS NOT NULL AND n._idx_key <> ''"
        return self.merge_duplicate_nodes_by_property("Location", "_idx_key", where_clause)


    def merge_duplicate_nodes_by_property(self, label: str, property_name: str, where_clause: str) -> int:
        """
        Merge duplicate nodes one duplicate group at a time.

        Cypher cannot parameterize labels, properties, or relationship types, so
        this method is only called with constants owned by this task.
        """

        merged_count = 0

        while True:
            duplicate_group = self.fetch_one_duplicate_group(label, property_name, where_clause)

            if not duplicate_group:
                break

            merge_key = duplicate_group["merge_key"]
            node_ids = duplicate_group["node_ids"]

            if len(node_ids) < 2:
                break

            keeper_id = min(node_ids)
            duplicate_ids = [node_id for node_id in node_ids if node_id != keeper_id]

            self.logger.info(
                f"Merging {len(duplicate_ids)} duplicate {label} nodes for "
                f"{property_name}={merge_key}; keeper_id={keeper_id}."
            )

            for duplicate_id in duplicate_ids:
                self.merge_one_duplicate_node(label, keeper_id, duplicate_id)
                merged_count += 1

        self.logger.info(f"Merged {merged_count} duplicate {label} nodes.")
        return merged_count


    def fetch_one_duplicate_group(self, label: str, property_name: str, where_clause: str) -> Optional[Dict[str, Any]]:
        """Return one duplicate group for a label/property pair."""

        query = f"""
            MATCH (n:{label})
            WHERE {where_clause}
            WITH n
            ORDER BY id(n)
            WITH n.{property_name} AS merge_key, collect(id(n)) AS node_ids, count(n) AS node_count
            WHERE node_count > 1
            RETURN merge_key, node_ids, node_count
            LIMIT 1
        """

        rows = list(self.memgraph.execute_and_fetch(query))

        if not rows:
            return None

        return rows[0]


    def merge_one_duplicate_node(self, label: str, keeper_id: int, duplicate_id: int) -> None:
        """
        Rewire all relationships from a duplicate node to the keeper, then delete it.

        This avoids depending on optional graph refactor procedures. Relationship
        types are copied one type at a time because Cypher relationship types
        must be literal tokens, not parameters.
        """

        relationship_types = self.fetch_relationship_types(duplicate_id)

        for relationship_type in relationship_types:
            if not self.RELATIONSHIP_TYPE_RE.match(relationship_type):
                raise ValueError(f"Unsafe relationship type from graph: {relationship_type}")

            self.rewire_outgoing_relationships(label, keeper_id, duplicate_id, relationship_type)
            self.rewire_incoming_relationships(label, keeper_id, duplicate_id, relationship_type)

        self.delete_duplicate_node(label, duplicate_id)


    def fetch_relationship_types(self, node_id: int) -> Set[str]:
        """Fetch all incoming and outgoing relationship types for a node."""

        query = """
            MATCH (n)-[r]-()
            WHERE id(n) = $node_id
            RETURN DISTINCT type(r) AS relationship_type
        """

        rows = self.memgraph.execute_and_fetch(query, {"node_id": node_id})
        return {row["relationship_type"] for row in rows if row.get("relationship_type")}


    def rewire_outgoing_relationships(self, label: str, keeper_id: int, duplicate_id: int, relationship_type: str) -> None:
        """Copy duplicate outgoing relationships to the keeper node."""

        query = f"""
            MATCH (keeper:{label}), (duplicate:{label})
            WHERE id(keeper) = $keeper_id AND id(duplicate) = $duplicate_id
            MATCH (duplicate)-[r:{relationship_type}]->(target)
            WHERE id(target) <> $keeper_id AND id(target) <> $duplicate_id
            MERGE (keeper)-[new_r:{relationship_type}]->(target)
            SET new_r += properties(r)
            DELETE r
        """

        self.memgraph.execute(query, {"keeper_id": keeper_id, "duplicate_id": duplicate_id})


    def rewire_incoming_relationships(self, label: str, keeper_id: int, duplicate_id: int, relationship_type: str) -> None:
        """Copy duplicate incoming relationships to the keeper node."""

        query = f"""
            MATCH (keeper:{label}), (duplicate:{label})
            WHERE id(keeper) = $keeper_id AND id(duplicate) = $duplicate_id
            MATCH (source)-[r:{relationship_type}]->(duplicate)
            WHERE id(source) <> $keeper_id AND id(source) <> $duplicate_id
            MERGE (source)-[new_r:{relationship_type}]->(keeper)
            SET new_r += properties(r)
            DELETE r
        """

        self.memgraph.execute(query, {"keeper_id": keeper_id, "duplicate_id": duplicate_id})


    def delete_duplicate_node(self, label: str, duplicate_id: int) -> None:
        """Delete the duplicate node after its relationships have been rewired."""

        query = f"""
            MATCH (duplicate:{label})
            WHERE id(duplicate) = $duplicate_id
            DETACH DELETE duplicate
        """

        self.memgraph.execute(query, {"duplicate_id": duplicate_id})

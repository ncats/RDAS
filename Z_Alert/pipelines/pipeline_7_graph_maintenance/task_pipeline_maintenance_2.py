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
    DUPLICATE_KEY_BATCH_SIZE = 500
    DUPLICATE_MERGE_BATCH_SIZE = 500
    DUPLICATE_KEY_BATCH_SIZE_ENV_VAR = "ORGANIZATION_LOCATION_DUPLICATE_KEY_BATCH_SIZE"
    DUPLICATE_MERGE_BATCH_SIZE_ENV_VAR = "ORGANIZATION_LOCATION_DUPLICATE_MERGE_BATCH_SIZE"
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
        self.duplicate_key_batch_size = self._resolve_positive_int_setting(
            self.DUPLICATE_KEY_BATCH_SIZE_ENV_VAR,
            self.DUPLICATE_KEY_BATCH_SIZE,
        )
        self.duplicate_merge_batch_size = self._resolve_positive_int_setting(
            self.DUPLICATE_MERGE_BATCH_SIZE_ENV_VAR,
            self.DUPLICATE_MERGE_BATCH_SIZE,
        )
        self.relationship_type_cache: Dict[str, List[str]] = {}
        self.logger.info(
            "OrganizationLocationGraphSyncTask configured with "
            f"batch_size={self.BATCH_SIZE}, "
            f"duplicate_key_batch_size={self.duplicate_key_batch_size}, "
            f"duplicate_merge_batch_size={self.duplicate_merge_batch_size}."
        )


    def _resolve_positive_int_setting(self, env_var_name: str, default_value: int) -> int:
        """Resolve a positive integer setting from an environment variable."""

        '''
        Duplicate merging is write-heavy in Memgraph, so the best batch size can
        differ between machines and graph sizes. Keep conservative defaults in
        code, but allow operations to tune the merge checks without another
        source edit or redeploy.
        '''
        env_value = os.getenv(env_var_name)

        if not env_value:
            return default_value

        try:
            parsed_value = int(env_value)
        except ValueError:
            self.logger.info(f"{env_var_name}={env_value} is not an integer; using default {default_value}.")
            return default_value

        if parsed_value <= 0:
            self.logger.info(f"{env_var_name}={env_value} must be greater than 0; using default {default_value}.")
            return default_value

        return parsed_value


    def find_new_data(self, gard_node) -> None:
        self.logger.info("OrganizationLocationGraphSyncTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Read staged location rows and sync Organization/Location graph data."""

        fetch_cursor = None
        total_updated = 0
        batch_num = 0
        start_time = time.time()
        updated_ror_ids = set()
        updated_location_idx_keys = set()

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

                    for chunk in chunks:
                        if not chunk["hasLocation"]:
                            continue

                        ror_id = chunk.get("rorId")
                        location_idx_key = chunk.get("locationIdxKey")

                        if ror_id and ror_id != "N/A":
                            updated_ror_ids.add(ror_id)

                        if location_idx_key:
                            updated_location_idx_keys.add(location_idx_key)

                    hours, minutes, seconds = _time_hms(time.time() - batch_start)
                    self.logger.info(
                        f"Batch #{batch_num}: updated {len(chunks)} Organization nodes. "
                        f"With location={with_location_count}, without location={without_location_count}. "
                        f"Total updated={total_updated}. "
                        f"Time={hours} hours, {minutes} minutes, {seconds} seconds."
                    )

                except Exception as e:
                    self.logger.error(f"Error syncing organization location batch #{batch_num} to Memgraph: {e}")

            merged_organizations = 0
            merged_locations = 0

            if total_updated > 0:
                merged_organizations = self.merge_duplicate_organizations(updated_ror_ids)
                merged_locations = self.merge_duplicate_locations(updated_location_idx_keys)
            else:
                self.logger.info("No Organization location updates were applied; skipping duplicate merges.")

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


    def merge_duplicate_organizations(self, ror_ids: Optional[Set[str]] = None) -> int:
        """Merge Organization nodes that now share the same real ROR id."""

        if ror_ids is not None:
            return self.merge_duplicate_nodes_for_keys("Organization", "ror_id", ror_ids)

        where_clause = "n.ror_id IS NOT NULL AND n.ror_id <> '' AND n.ror_id <> 'N/A'"
        return self.merge_duplicate_nodes_by_property("Organization", "ror_id", where_clause)


    def merge_duplicate_locations(self, location_idx_keys: Optional[Set[str]] = None) -> int:
        """Merge duplicate Location nodes that share the same stable _idx_key."""

        if location_idx_keys is not None:
            return self.merge_duplicate_nodes_for_keys("Location", "_idx_key", location_idx_keys)

        where_clause = "n._idx_key IS NOT NULL AND n._idx_key <> ''"
        return self.merge_duplicate_nodes_by_property("Location", "_idx_key", where_clause)


    def merge_duplicate_nodes_for_keys(self, label: str, property_name: str, merge_keys: Set[str]) -> int:
        """
        Merge duplicate nodes only for keys touched by current organization_location rows.

        This keeps normal alert maintenance incremental. A full graph-wide scan
        is still available through merge_duplicate_nodes_by_property for manual
        cleanup, but the scheduled path only needs keys from is_new rows.
        """

        filtered_merge_keys = sorted(merge_key for merge_key in merge_keys if merge_key)

        if not filtered_merge_keys:
            self.logger.info(f"No updated {label}.{property_name} keys to check; skipping duplicate merge.")
            return 0

        merged_count = 0
        key_batch_count = 0

        for start in range(0, len(filtered_merge_keys), self.duplicate_key_batch_size):
            key_batch_count += 1
            merge_key_batch = filtered_merge_keys[start:start + self.duplicate_key_batch_size]
            duplicate_fetch_start = time.time()
            duplicate_groups = self.fetch_duplicate_groups_for_keys(label, property_name, merge_key_batch)
            fetch_hours, fetch_minutes, fetch_seconds = _time_hms(time.time() - duplicate_fetch_start)

            self.logger.info(
                f"Duplicate {label} touched key batch #{key_batch_count}: "
                f"merge_keys={len(merge_key_batch)}, duplicate_groups={len(duplicate_groups)}, "
                f"last_{property_name}={merge_key_batch[-1]}, "
                f"duplicate_merge_batch_size={self.duplicate_merge_batch_size}, "
                f"duplicate_group_fetch_time={fetch_hours} hours, {fetch_minutes} minutes, {fetch_seconds} seconds."
            )

            if not duplicate_groups:
                continue

            relationship_types = self.get_relationship_types_for_label(label)
            batch_merge_start = time.time()
            batch_merged_count = 0

            for duplicate_group in duplicate_groups:
                merge_key = duplicate_group["merge_key"]
                node_ids = [int(node_id) for node_id in duplicate_group["node_ids"]]

                if len(node_ids) < 2:
                    continue

                keeper_id = min(node_ids)
                duplicate_ids = [node_id for node_id in node_ids if node_id != keeper_id]

                self.logger.info(
                    f"Batch merging {len(duplicate_ids)} duplicate {label} nodes for "
                    f"{property_name}={merge_key}; keeper_id={keeper_id}."
                )

                duplicate_merged_count = self.merge_duplicate_group(label, keeper_id, duplicate_ids, relationship_types)
                batch_merged_count += duplicate_merged_count
                merged_count += duplicate_merged_count

            merge_hours, merge_minutes, merge_seconds = _time_hms(time.time() - batch_merge_start)
            self.logger.info(
                f"Duplicate {label} touched key batch #{key_batch_count} merge complete: "
                f"merged_nodes={batch_merged_count}, "
                f"merge_time={merge_hours} hours, {merge_minutes} minutes, {merge_seconds} seconds."
            )

        self.logger.info(f"Merged {merged_count} duplicate {label} nodes for {len(filtered_merge_keys)} updated keys.")
        return merged_count


    def merge_duplicate_nodes_by_property(self, label: str, property_name: str, where_clause: str) -> int:
        """
        Merge duplicate nodes by walking merge keys in bounded batches.

        Cypher cannot parameterize labels, properties, or relationship types, so
        this method is only called with constants owned by this task.
        """

        merged_count = 0
        key_batch_count = 0
        last_merge_key = None

        while True:
            merge_keys = self.fetch_duplicate_merge_key_batch(label, property_name, where_clause, last_merge_key)

            if not merge_keys:
                break

            key_batch_count += 1
            last_merge_key = merge_keys[-1]
            duplicate_fetch_start = time.time()
            duplicate_groups = self.fetch_duplicate_groups_for_keys(label, property_name, merge_keys)
            fetch_hours, fetch_minutes, fetch_seconds = _time_hms(time.time() - duplicate_fetch_start)

            self.logger.info(
                f"Duplicate {label} key batch #{key_batch_count}: "
                f"merge_keys={len(merge_keys)}, duplicate_groups={len(duplicate_groups)}, "
                f"last_{property_name}={last_merge_key}, "
                f"duplicate_merge_batch_size={self.duplicate_merge_batch_size}, "
                f"duplicate_group_fetch_time={fetch_hours} hours, {fetch_minutes} minutes, {fetch_seconds} seconds."
            )

            if not duplicate_groups:
                continue

            relationship_types = self.get_relationship_types_for_label(label)
            batch_merge_start = time.time()
            batch_merged_count = 0

            for duplicate_group in duplicate_groups:
                merge_key = duplicate_group["merge_key"]
                node_ids = [int(node_id) for node_id in duplicate_group["node_ids"]]

                if len(node_ids) < 2:
                    continue

                keeper_id = min(node_ids)
                duplicate_ids = [node_id for node_id in node_ids if node_id != keeper_id]

                self.logger.info(
                    f"Batch merging {len(duplicate_ids)} duplicate {label} nodes for "
                    f"{property_name}={merge_key}; keeper_id={keeper_id}."
                )

                duplicate_merged_count = self.merge_duplicate_group(label, keeper_id, duplicate_ids, relationship_types)
                batch_merged_count += duplicate_merged_count
                merged_count += duplicate_merged_count

            merge_hours, merge_minutes, merge_seconds = _time_hms(time.time() - batch_merge_start)
            self.logger.info(
                f"Duplicate {label} key batch #{key_batch_count} merge complete: "
                f"merged_nodes={batch_merged_count}, "
                f"merge_time={merge_hours} hours, {merge_minutes} minutes, {merge_seconds} seconds."
            )

            if len(merge_keys) < self.duplicate_key_batch_size:
                break

        self.logger.info(f"Merged {merged_count} duplicate {label} nodes.")
        return merged_count


    def fetch_duplicate_merge_key_batch(self, label: str, property_name: str, where_clause: str, last_merge_key: Optional[str]) -> List[str]:
        """Return the next bounded page of merge keys for duplicate discovery."""

        query = f"""
            MATCH (n:{label})
            WHERE {where_clause}
              AND ($lastMergeKey IS NULL OR n.{property_name} > $lastMergeKey)
            WITH DISTINCT n.{property_name} AS merge_key
            ORDER BY merge_key
            LIMIT $limit
            RETURN merge_key
        """

        rows = self.memgraph.execute_and_fetch(query, {
            "lastMergeKey": last_merge_key,
            "limit": self.duplicate_key_batch_size,
        })
        return [row["merge_key"] for row in rows if row.get("merge_key")]


    def fetch_duplicate_groups_for_keys(self, label: str, property_name: str, merge_keys: List[str]) -> List[Dict[str, Any]]:
        """Return duplicate groups only for the current bounded key page."""

        if not merge_keys:
            return []

        query = f"""
            MATCH (n:{label})
            WHERE n.{property_name} IN $mergeKeys
            WITH n.{property_name} AS merge_key, collect(id(n)) AS node_ids, count(n) AS node_count
            WHERE node_count > 1
            RETURN merge_key, node_ids, node_count
            ORDER BY node_count DESC
        """

        return list(self.memgraph.execute_and_fetch(query, {"mergeKeys": merge_keys}))


    def merge_duplicate_group(self, label: str, keeper_id: int, duplicate_ids: List[int], relationship_types: Optional[List[str]] = None) -> int:
        """
        Rewire relationships from duplicate nodes to the keeper, then delete them.

        This avoids depending on optional graph refactor procedures. Relationship
        types are copied one type at a time because Cypher relationship types
        must be literal tokens, not parameters. Within each duplicate group,
        node ids are processed in chunks to keep each Memgraph transaction
        bounded while still avoiding one transaction per duplicate node.
        """

        merged_count = 0
        total_duplicate_count = len(duplicate_ids)

        if relationship_types is None:
            relationship_types = self.get_relationship_types_for_label(label)

        for start in range(0, total_duplicate_count, self.duplicate_merge_batch_size):
            batch_start = time.time()
            batch_duplicate_ids = duplicate_ids[start:start + self.duplicate_merge_batch_size]
            outgoing_rewired_count = 0
            incoming_rewired_count = 0
            relationship_rewire_start = time.time()

            for relationship_type in relationship_types:
                if not self.RELATIONSHIP_TYPE_RE.match(relationship_type):
                    raise ValueError(f"Unsafe relationship type from graph: {relationship_type}")

                outgoing_rewired_count += self.rewire_outgoing_relationships_for_nodes(label, keeper_id, batch_duplicate_ids, relationship_type)
                incoming_rewired_count += self.rewire_incoming_relationships_for_nodes(label, keeper_id, batch_duplicate_ids, relationship_type)

            relationship_rewire_hours, relationship_rewire_minutes, relationship_rewire_seconds = _time_hms(time.time() - relationship_rewire_start)
            delete_start = time.time()
            delete_result = self.delete_duplicate_nodes_after_rewire(label, batch_duplicate_ids)
            deleted_count = delete_result["deleted_count"]
            delete_mode = delete_result["delete_mode"]
            delete_hours, delete_minutes, delete_seconds = _time_hms(time.time() - delete_start)
            batch_hours, batch_minutes, batch_seconds = _time_hms(time.time() - batch_start)
            merged_count += deleted_count

            self.logger.info(
                f"Merged duplicate {label} batch "
                f"{start + 1}-{start + len(batch_duplicate_ids)} of {total_duplicate_count}; "
                f"deleted_nodes={deleted_count}, relationship_types={len(relationship_types)}, "
                f"outgoing_relationships={outgoing_rewired_count}, incoming_relationships={incoming_rewired_count}, "
                f"relationship_type_source=label_cache, "
                f"relationship_rewire_time={relationship_rewire_hours} hours, {relationship_rewire_minutes} minutes, {relationship_rewire_seconds} seconds, "
                f"delete_mode={delete_mode}, "
                f"delete_time={delete_hours} hours, {delete_minutes} minutes, {delete_seconds} seconds, "
                f"batch_time={batch_hours} hours, {batch_minutes} minutes, {batch_seconds} seconds."
            )

        return merged_count


    def get_relationship_types_for_label(self, label: str) -> List[str]:
        """Return cached relationship types connected to the given node label."""

        cached_relationship_types = self.relationship_type_cache.get(label)

        if cached_relationship_types is not None:
            return cached_relationship_types

        '''
        The old merge path discovered relationship types for every duplicate
        group. The live logs showed that discovery alone cost about 4-5 seconds
        per group, even when the group only had one relationship type. Fetching
        the label-level type list once preserves the same copy/delete behavior
        while removing hundreds of repeated read queries during large merges.
        '''
        relationship_type_start = time.time()
        relationship_types = sorted(self.fetch_relationship_types_for_label(label))
        relationship_type_hours, relationship_type_minutes, relationship_type_seconds = _time_hms(time.time() - relationship_type_start)
        self.relationship_type_cache[label] = relationship_types

        self.logger.info(
            f"Cached {len(relationship_types)} relationship types for {label}: "
            f"relationship_types={relationship_types}. "
            f"relationship_type_cache_fetch_time={relationship_type_hours} hours, "
            f"{relationship_type_minutes} minutes, {relationship_type_seconds} seconds."
        )

        return relationship_types


    def fetch_relationship_types_for_label(self, label: str) -> Set[str]:
        """Fetch all relationship types connected to nodes with the given label."""

        query = f"""
            MATCH (n:{label})-[r]-()
            RETURN DISTINCT type(r) AS relationship_type
            ORDER BY relationship_type
        """

        rows = self.memgraph.execute_and_fetch(query)
        return {row["relationship_type"] for row in rows if row.get("relationship_type")}


    def delete_duplicate_nodes_after_rewire(self, label: str, duplicate_ids: List[int]) -> Dict[str, Any]:
        """Delete duplicate nodes, falling back to DETACH DELETE only when needed."""

        if not duplicate_ids:
            return {
                "deleted_count": 0,
                "delete_mode": "skip_empty"
            }

        '''
        Rewire queries delete the normal external relationships before this
        point, so a plain DELETE should usually be enough and avoids Memgraph
        doing another relationship-detach pass. Keep the DETACH DELETE fallback
        for rare groups that still have internal duplicate-to-duplicate or
        duplicate-to-keeper relationships after rewiring.
        '''
        try:
            return {
                "deleted_count": self.delete_duplicate_nodes_without_detach(label, duplicate_ids),
                "delete_mode": "delete"
            }
        except Exception as e:
            self.logger.info(
                f"Plain DELETE failed for {len(duplicate_ids)} duplicate {label} nodes; "
                f"falling back to DETACH DELETE. Error: {e}"
            )

            return {
                "deleted_count": self.detach_delete_duplicate_nodes(label, duplicate_ids),
                "delete_mode": "detach_delete"
            }


    def delete_duplicate_nodes_without_detach(self, label: str, duplicate_ids: List[int]) -> int:
        """Delete duplicate nodes when rewiring already removed their relationships."""

        if not duplicate_ids:
            return 0

        query = f"""
            MATCH (duplicate:{label})
            WHERE id(duplicate) IN $duplicate_ids
            WITH collect(duplicate) AS duplicates, count(duplicate) AS deleted_count
            FOREACH (duplicate IN duplicates | DELETE duplicate)
            RETURN deleted_count
        """

        rows = list(self.memgraph.execute_and_fetch(query, {"duplicate_ids": duplicate_ids}))
        return int(rows[0].get("deleted_count") or 0) if rows else 0


    def fetch_relationship_types_for_nodes(self, node_ids: List[int]) -> Set[str]:
        """Fetch all incoming and outgoing relationship types for duplicate nodes."""

        if not node_ids:
            return set()

        query = """
            MATCH (n)-[r]-()
            WHERE id(n) IN $node_ids
            RETURN DISTINCT type(r) AS relationship_type
        """

        rows = self.memgraph.execute_and_fetch(query, {"node_ids": node_ids})
        return {row["relationship_type"] for row in rows if row.get("relationship_type")}


    def rewire_outgoing_relationships_for_nodes(self, label: str, keeper_id: int, duplicate_ids: List[int], relationship_type: str) -> int:
        """Copy duplicate outgoing relationships to the keeper node in one batch."""

        query = f"""
            MATCH (keeper:{label})
            WHERE id(keeper) = $keeper_id
            UNWIND $duplicate_ids AS duplicate_id
            MATCH (duplicate:{label})
            WHERE id(duplicate) = duplicate_id
            MATCH (duplicate)-[r:{relationship_type}]->(target)
            WHERE id(target) <> $keeper_id
              AND NOT id(target) IN $duplicate_ids
            MERGE (keeper)-[new_r:{relationship_type}]->(target)
            SET new_r += properties(r)
            DELETE r
            RETURN count(*) AS rewired_count
        """

        rows = list(self.memgraph.execute_and_fetch(query, {"keeper_id": keeper_id, "duplicate_ids": duplicate_ids}))
        return int(rows[0].get("rewired_count") or 0) if rows else 0


    def rewire_incoming_relationships_for_nodes(self, label: str, keeper_id: int, duplicate_ids: List[int], relationship_type: str) -> int:
        """Copy duplicate incoming relationships to the keeper node in one batch."""

        query = f"""
            MATCH (keeper:{label})
            WHERE id(keeper) = $keeper_id
            UNWIND $duplicate_ids AS duplicate_id
            MATCH (duplicate:{label})
            WHERE id(duplicate) = duplicate_id
            MATCH (source)-[r:{relationship_type}]->(duplicate)
            WHERE id(source) <> $keeper_id
              AND NOT id(source) IN $duplicate_ids
            MERGE (source)-[new_r:{relationship_type}]->(keeper)
            SET new_r += properties(r)
            DELETE r
            RETURN count(*) AS rewired_count
        """

        rows = list(self.memgraph.execute_and_fetch(query, {"keeper_id": keeper_id, "duplicate_ids": duplicate_ids}))
        return int(rows[0].get("rewired_count") or 0) if rows else 0


    def delete_duplicate_nodes(self, label: str, duplicate_ids: List[int]) -> int:
        """Delete duplicate nodes and any remaining relationships."""

        return self.detach_delete_duplicate_nodes(label, duplicate_ids)


    def detach_delete_duplicate_nodes(self, label: str, duplicate_ids: List[int]) -> int:
        """Delete duplicate nodes and any remaining relationships."""

        if not duplicate_ids:
            return 0

        query = f"""
            MATCH (duplicate:{label})
            WHERE id(duplicate) IN $duplicate_ids
            WITH collect(duplicate) AS duplicates, count(duplicate) AS deleted_count
            FOREACH (duplicate IN duplicates | DETACH DELETE duplicate)
            RETURN deleted_count
        """

        rows = list(self.memgraph.execute_and_fetch(query, {"duplicate_ids": duplicate_ids}))
        return int(rows[0].get("deleted_count") or 0) if rows else 0

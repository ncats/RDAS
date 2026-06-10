import os
import re
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Set

from dotenv import load_dotenv

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

load_dotenv(os.path.abspath(os.path.join(_dir, "..", ".env")))

from baseclass.conn import DBConnection as db
from utils.applogger import AppLogger
from utils.tools import _clean, _make_hash_key, _parse_json_list, _time_hms, _to_float, _to_int

# Show duplicate nodes count with the same ror_id
''' 
MATCH (o:Organization)
WHERE o.ror_id IS NOT NULL
  AND o.ror_id <> ''
  AND o.ror_id <> 'N/A'
WITH o.ror_id AS ror_id, count(o) AS node_count
WHERE node_count > 1
RETURN ror_id, node_count
ORDER BY node_count DESC;
'''

'''
when ror_id = 'N/A' and org_name is null:

name: not changed. This task does not set org.name at all.
displayName: not changed, because no update chunk is created.
'''
class OrganizationLocationGraphUpdateTask:
    """
    Sync completed ROR organization_location rows back into Memgraph.

    1. Reads MySQL organization_location rows with real ROR ids first. Rows are
       scanned newest-first by MySQL id, so the first row seen for each
       original_name_in_graph_db_idx_key is the newest real-ROR row for that
       graph Organization key.
    2. Updates matching graph Organization nodes by Organization._idx_key. This
       avoids scanning large batches of graph Organization nodes that have no
       matching real-ROR MySQL row.
    3. Updates only pending graph Organization nodes. Existing graph nodes that
       already have a real ror_id are left unchanged, which prevents this sync
       from overwriting previously confirmed ROR data. The update writes
       displayName, ror_id, ROR types, website, established, status, and the
       updatedFromOrganizationLocation marker.
    4. Creates or updates Location nodes only when usable ROR location data
       exists. The Location._idx_key is derived from lat/lng when available,
       then geonames_id, then a city/state/country/country_code fallback. The
       Organization is connected with :has_location.
    5. Merges duplicate Organization nodes that now share the same real ror_id.
       The lowest Memgraph internal node id is kept, relationships from duplicate
       nodes are rewired to the keeper, and duplicate nodes are deleted.
    6. Merges duplicate Location nodes by stable _idx_key using the same
       relationship-rewire process. This consolidates locations created from the
       same ROR location payload across multiple Organization updates.
    """

    BATCH_SIZE = 2000
    TABLE_NAME = "organization_location"

    # Relationship types come from the graph, but they still need to be inserted
    # into Cypher as literals. Keep a strict identifier check before doing that.
    RELATIONSHIP_TYPE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

    BATCH_UPDATE_ORGANIZATIONS = """
        UNWIND $chunks AS chunk

        MATCH (org:Organization {_idx_key: chunk.orgIdxKey})
        WHERE org.ror_id IS NULL OR org.ror_id = '' OR org.ror_id = 'N/A'

        SET org.displayName = chunk.displayName,
            org.ror_id = chunk.rorId,
            org.types = chunk.types,
            org.website = chunk.website,
            org.established = chunk.established,
            org.status = chunk.status,
            org.updatedFromOrganizationLocation = true

        FOREACH (_ IN CASE WHEN chunk.hasLocation THEN [1] ELSE [] END |
            MERGE (loc:Location {_idx_key: chunk.locationIdxKey})
            ON CREATE SET
                loc.facility = '',
                loc.address = '',
                loc.zip = ''
            SET loc.city = chunk.city,
                loc.state = chunk.state,
                loc.country = chunk.country,
                loc.countryCode = chunk.countryCode,
                loc.geonamesId = chunk.geonamesId,
                loc.lat = chunk.lat,
                loc.lng = chunk.lng

            MERGE (org)-[:has_location]->(loc)
        )

        RETURN count(org) AS updated_count
    """

    def __init__(self):
        
        self.mysql = db().mysql_conn()
        self.memgraph = db().memgraph_conn()

        self.log_dir = os.path.expanduser(os.getenv("ALERT_LOG_DIR", "logs"))
        os.makedirs(self.log_dir, exist_ok=True)

        class_name = type(self).__name__
        self.log_file = f"{self.log_dir}/alert-{class_name}.log"
        self.logger = AppLogger(class_name, self.log_file).get_logger()
        self.logger.info(f'\n\n{"*" * 20} The {class_name} is initialized. {"*" * 20}\n')


    def close(self) -> None:
        """Close MySQL and logger resources owned by this task."""

        if self.mysql is not None and self.mysql.is_connected():
            self.mysql.close()

        self.mysql = None

        if hasattr(self, "logger") and self.logger is not None:
            for handler in list(self.logger.handlers):
                handler.flush()
                handler.close()
                self.logger.removeHandler(handler)

            self.logger = None


    def update(self) -> None:
        """Run the full graph update and duplicate merge workflow."""

        start_time = time.time()

        try:
            updated_count = self.sync_real_ror_rows_to_graph()
            merged_organizations = self.merge_duplicate_organizations()
            merged_locations = self.merge_duplicate_locations()

            hours, minutes, seconds = _time_hms(time.time() - start_time)
            self.logger.info(
                "Completed OrganizationLocationGraphUpdateTask. "
                f"updated_organizations={updated_count}, "
                f"merged_organizations={merged_organizations}, "
                f"merged_locations={merged_locations}, "
                f"time={hours} hours, {minutes} minutes, {seconds} seconds."
            )

        except Exception as e:
            self.logger.error(f"OrganizationLocationGraphUpdateTask failed: {e}")

        finally:
            self.close()


    def sync_real_ror_rows_to_graph(self) -> int:
        """
        Read real-ROR MySQL rows first, then update matching graph nodes.

        is_new is deliberately not used here. The source-of-truth filters are:
        MySQL has a usable real ror_id, and the graph Organization with the
        same original_name_in_graph_db_idx_key still has missing/empty/N/A
        ror_id.

        The old implementation scanned pending graph Organization keys first.
        That can waste a lot of time when most pending graph keys have no real
        ROR row in MySQL. This implementation scans MySQL rows newest-first and
        only sends real-ROR candidates to Memgraph. Before running the update
        query, it asks Memgraph which candidate keys are still pending so
        already-updated or missing graph nodes can be skipped quickly.
        """

        total_updated = 0
        total_mysql_rows = 0
        total_unique_keys = 0
        batch_num = 0
        last_id = None
        seen_org_idx_keys = set()

        while True:
            rows = self.fetch_mysql_real_ror_rows_before_id(last_id)

            if not rows:
                self.logger.info(
                    "No more MySQL organization_location rows with real ROR ids to sync. "
                    f"mysql_rows_read={total_mysql_rows}, unique_org_keys={total_unique_keys}."
                )
                break

            batch_num += 1
            last_id = rows[-1]["id"]
            total_mysql_rows += len(rows)

            latest_rows = []

            for row in rows:
                org_idx_key = row.get("original_name_in_graph_db_idx_key")

                if not org_idx_key or org_idx_key in seen_org_idx_keys:
                    continue

                seen_org_idx_keys.add(org_idx_key)
                latest_rows.append(row)

            total_unique_keys += len(latest_rows)

            chunks = self.create_organization_location_chunks(latest_rows)

            if not chunks:
                self.logger.info(
                    f"Batch #{batch_num}: mysql_rows={len(rows)}, "
                    f"newest_unique_keys={len(latest_rows)}, no valid graph update chunks."
                )
                continue

            pending_org_idx_keys = self.fetch_pending_graph_org_keys_for_chunks(chunks)

            if not pending_org_idx_keys:
                self.logger.info(
                    f"Batch #{batch_num}: mysql_rows={len(rows)}, newest_unique_keys={len(latest_rows)}, "
                    f"chunks={len(chunks)}, pending_graph_keys=0, skipped graph update."
                )
                continue

            pending_chunks = [
                chunk
                for chunk in chunks
                if chunk["orgIdxKey"] in pending_org_idx_keys
            ]

            updated_count = self.execute_graph_update(pending_chunks)
            total_updated += updated_count

            with_location = sum(1 for chunk in pending_chunks if chunk["hasLocation"])
            self.logger.info(
                f"Batch #{batch_num}: mysql_rows={len(rows)}, newest_unique_keys={len(latest_rows)}, "
                f"chunks={len(chunks)}, pending_graph_keys={len(pending_org_idx_keys)}, "
                f"updated_graph_nodes={updated_count}, "
                f"with_location={with_location}, total_updated={total_updated}."
            )

        return total_updated


    def fetch_pending_graph_org_keys_for_chunks(self, chunks: List[Dict[str, Any]]) -> Set[str]:
        """
        Return Organization keys from this batch that still need ROR data.

        The MySQL-first scan can encounter many real-ROR rows for graph nodes
        that are already updated, or for keys that do not exist in Memgraph.
        This precheck avoids running the heavier update/Location merge query
        when no candidate key is actually pending in the graph.
        """

        org_idx_keys = sorted({
            chunk["orgIdxKey"]
            for chunk in chunks
            if chunk.get("orgIdxKey")
        })

        if not org_idx_keys:
            return set()

        query = """
            UNWIND $orgIdxKeys AS orgIdxKey
            MATCH (org:Organization {_idx_key: orgIdxKey})
            WHERE org.ror_id IS NULL OR org.ror_id = '' OR org.ror_id = 'N/A'
            RETURN DISTINCT org._idx_key AS org_idx_key
        """

        rows = self.memgraph.execute_and_fetch(query, {"orgIdxKeys": org_idx_keys})

        return {
            row["org_idx_key"]
            for row in rows
            if row.get("org_idx_key")
        }


    def fetch_mysql_real_ror_rows_before_id(self, last_id: Optional[int]) -> List[Dict[str, Any]]:
        """
        Fetch real-ROR organization_location rows newest-first.

        Because rows are ordered by id DESC, the first row encountered for an
        original_name_in_graph_db_idx_key is the newest real-ROR row for that
        Organization key. sync_real_ror_rows_to_graph keeps a set of keys that
        have already been seen and ignores older rows for the same key.
        """

        id_filter = ""
        params: List[Any] = []

        if last_id is not None:
            id_filter = "AND id < %s"
            params.append(last_id)

        query = f"""
            SELECT
                id,
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
                full_data
            FROM {self.TABLE_NAME}
            WHERE original_name_in_graph_db_idx_key IS NOT NULL
              AND original_name_in_graph_db_idx_key <> ''
              AND ror_id IS NOT NULL
              AND TRIM(ror_id) <> ''
              AND UPPER(TRIM(ror_id)) <> 'N/A'
              {id_filter}
            ORDER BY id DESC
            LIMIT %s
        """

        params.append(self.BATCH_SIZE)

        cursor = None

        try:
            cursor = self.mysql.cursor(dictionary=True, buffered=True)
            cursor.execute(query, tuple(params))
            return cursor.fetchall()

        finally:
            if cursor:
                cursor.close()


    def create_organization_location_chunks(self, rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert MySQL rows into Cypher payload dictionaries."""

        chunks = []

        for row in rows:
            chunk = self.create_organization_location_chunk(row)

            if chunk:
                chunks.append(chunk)

        return chunks


    def create_organization_location_chunk(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create one graph update chunk from one organization_location row."""

        org_idx_key = row.get("original_name_in_graph_db_idx_key")

        if not org_idx_key:
            self.logger.info(f"Skipping organization_location id={row.get('id')} without graph idx key.")
            return None

        ror_id = _clean(row.get("ror_id", ""))

        if not ror_id or ror_id.upper() == "N/A":
            return None

        display_name = _clean(row.get("org_name", "")) or _clean(row.get("original_name_in_graph_db", ""))
        lat = _to_float(row.get("lat"))
        lng = _to_float(row.get("lng"))
        geonames_id = _to_int(row.get("geonames_id"))

        has_location = any([
            row.get("city"),
            row.get("state"),
            row.get("country"),
            row.get("country_code"),
            geonames_id is not None,
            lat is not None,
            lng is not None,
        ])

        location_idx_key = None
        if has_location:
            if lat is not None and lng is not None:
                location_idx_key = _make_hash_key(f"{lat}{lng}")
            elif geonames_id is not None:
                location_idx_key = _make_hash_key(f"geonames:{geonames_id}")
            else:
                location_idx_key = _make_hash_key(
                    "|".join([
                        _clean(row.get("city", "")),
                        _clean(row.get("state", "")),
                        _clean(row.get("country", "")),
                        _clean(row.get("country_code", "")),
                    ])
                )

        return {
            "displayName": display_name,
            "rorId": ror_id,
            "types": _parse_json_list(row.get("types")),
            "website": _clean(row.get("website", "")),
            "established": _clean(row.get("established", "")),
            "status": _clean(row.get("status", "")),
            "city": _clean(row.get("city", "")),
            "state": _clean(row.get("state", "")),
            "country": _clean(row.get("country", "")),
            "countryCode": _clean(row.get("country_code", "")),
            "geonamesId": geonames_id,
            "lat": lat,
            "lng": lng,
            "locationIdxKey": location_idx_key,
            "orgIdxKey": org_idx_key,
            "hasLocation": bool(has_location and location_idx_key),
        }


    def execute_graph_update(self, chunks: List[Dict[str, Any]]) -> int:
        """Send one Organization/Location update batch to Memgraph."""

        rows = list(self.memgraph.execute_and_fetch(self.BATCH_UPDATE_ORGANIZATIONS, {"chunks": chunks}))

        if not rows:
            return 0

        return int(rows[0].get("updated_count") or 0)


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
        this method is only called with constants owned by this script.
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


if __name__ == "__main__":
    OrganizationLocationGraphUpdateTask().update()

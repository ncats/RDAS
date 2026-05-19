import json
import os
import sys
import time
from typing import Any, Dict, List, Optional

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _clean, _make_hash_key, _time_hms

"""
Sync new organization location rows from MySQL back to Memgraph.

This task is the alert-pipeline version of:
G_update/organization_location_step_2_updater.py

It reads organization_location rows where is_new = 1, updates matching
Organization nodes with ROR metadata, creates Location nodes when location data
exists, and creates:

    (Organization)-[:has_location]->(Location)
"""


class OrganizationLocationGraphSyncTask(PipelineBase):
    """Apply newly staged organization_location rows to the Memgraph graph."""

    BATCH_SIZE = 200
    TABLE_NAME = "organization_location"
    
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

            total_hours, total_minutes, total_seconds = _time_hms(time.time() - start_time)
            self.logger.info(
                f"Completed OrganizationLocationGraphSyncTask. Total updated={total_updated}. "
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

        lat = self._to_float(row.get("lat"))
        lng = self._to_float(row.get("lng"))
        types = self._parse_types(row.get("types"))

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


    def _parse_types(self, value: Any) -> List[Any]:
        """Parse ROR organization types from JSON or pass through list values."""

        if value is None:
            return []

        try:
            parsed = json.loads(value) if isinstance(value, str) else value
        except json.JSONDecodeError:
            return []

        if isinstance(parsed, list):
            return parsed

        return [parsed]


    def _to_float(self, value: Any):
        """Convert nullable MySQL coordinate values to floats."""

        if value is None or value == "":
            return None

        try:
            return float(value)
        except (TypeError, ValueError):
            return None

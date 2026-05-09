import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Tuple

import requests

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _time_hms

"""
Fetch ROR organization location data for Organization nodes.
 
It finds Organization nodes whose ror_id is still empty, skips organizations
already stored in MySQL organization_location, queries the ROR API for the
remaining organizations, saves found and not-found rows into MySQL with
organization_location.is_new = 1, and marks not-found Organization nodes with
ror_id = 'N/A'.
"""

# Reference: G_update/updater/organization_location_step_1_finder_multiple.py


def _clean_org_name(text: Any) -> Any:
    """
    Sanitize organization names before sending them to the ROR affiliation API.
    """
    if not text:
        return text

    text = str(text)
    text = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', '', text)
    text = re.sub(r'[^\x20-\x7E\u00A0-\uFFFF]', '', text)
    text = re.sub(r'\s+', ' ', text)

    return text.strip()


def _to_float(value: Any):
    """Convert nullable API values to floats for location coordinates."""
    if value is None or value == "":
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any):
    """Convert nullable API values to integers for geonames IDs."""
    if value is None or value == "":
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def lookup_ror_static(org_name: str, idx_key: str, timeout: int = 20):
    """
    Query the ROR API for one organization.

    Returns a tuple matching the organization_location insert statement, or
    None when ROR has no chosen organization result.
    """
    cleaned_org_name = _clean_org_name(org_name)

    if not cleaned_org_name or not idx_key:
        return None

    ror_organizations_api = os.getenv("ROR_ORGANIZATIONS_API")
    if not ror_organizations_api:
        return None

    try:
        response = requests.get(
            ror_organizations_api,
            params={"affiliation": cleaned_org_name},
            timeout=timeout,
        )
        response.raise_for_status()
        data = response.json()

        items = data.get("items") or []

        if data.get("number_of_results") == 0 or not items:
            return None

        first_item = items[0]

        if not first_item.get("chosen"):
            return None

        org = first_item.get("organization") or {}

        # Pull only the fields the downstream organization_location table needs.
        ror_id = org.get("id")
        names = org.get("names") or []
        org_display_name = next(
            (
                name.get("value")
                for name in names
                if isinstance(name, dict) and "ror_display" in name.get("types", [])
            ),
            None
        )

        locations = org.get("locations") or []
        first_location = locations[0] if locations else {}
        location_details = first_location.get("geonames_details") or {}

        geonames_id = _to_int(first_location.get("geonames_id"))
        established = org.get("established")
        status = org.get("status")
        types = json.dumps(org.get("types", []))

        website = next(
            (
                link.get("value", "").strip()[:500]
                for link in org.get("links", [])
                if isinstance(link, dict) and link.get("type") == "website" and link.get("value")
            ),
            None
        )

        city = location_details.get("name")
        country = location_details.get("country_name")
        country_code = location_details.get("country_code")
        state = location_details.get("country_subdivision_name")
        lat = _to_float(location_details.get("lat"))
        lng = _to_float(location_details.get("lng"))
        full_data = json.dumps(org)

        return (
            ror_id,
            str(org_name)[:500],
            org_display_name,
            geonames_id,
            established,
            status,
            types,
            website,
            city,
            country,
            country_code,
            state,
            lat,
            lng,
            full_data,
            idx_key,
        )

    except requests.RequestException:
        return None
    except (KeyError, ValueError, TypeError, json.JSONDecodeError):
        return None


class OrganizationLocationRorLookupTask(PipelineBase):
    """Fetch missing Organization ROR/location data and stage it in MySQL."""

    TABLE_NAME = "organization_location"
    BATCH_SIZE = 100
    MAX_WORKERS = 20
    ROR_TIMEOUT = 20

    # If ROR has no match, mark the Organization node so future runs do not keep
    # querying the API for the same graph record.
    MARK_NOT_FOUND_IN_GRAPH = '''
        UNWIND $chunks AS idx_key
        MATCH (org:Organization {_idx_key: idx_key})
        SET org.ror_id = 'N/A'
    '''

    # Found rows are upserted and marked is_new=1 so the next follow-up task can
    # update Organization nodes from these staged location records.
    INSERT_FOUND_ORGANIZATION_SQL = f'''
        INSERT INTO {TABLE_NAME}
        (
            ror_id, original_name_in_graph_db, org_name, geonames_id, established,
            status, types, website, city, country,
            country_code, state, lat, lng, full_data,
            original_name_in_graph_db_idx_key,
            is_new
        )
        VALUES (%s, %s, %s, %s, %s,  %s, %s, %s, %s, %s,  %s, %s, %s, %s, %s,  %s, 1)
        ON DUPLICATE KEY UPDATE
            ror_id = VALUES(ror_id),
            original_name_in_graph_db = VALUES(original_name_in_graph_db),
            org_name = VALUES(org_name),
            geonames_id = VALUES(geonames_id),
            established = VALUES(established),
            status = VALUES(status),
            types = VALUES(types),
            website = VALUES(website),
            city = VALUES(city),
            country = VALUES(country),
            country_code = VALUES(country_code),
            state = VALUES(state),
            lat = VALUES(lat),
            lng = VALUES(lng),
            full_data = VALUES(full_data),
            is_new = 1
    '''

    # Not-found rows are also stored. This records that a lookup was attempted
    # and lets downstream steps work from the same is_new flag.
    INSERT_NOT_FOUND_ORGANIZATION_SQL = f'''
        INSERT INTO {TABLE_NAME}
        (original_name_in_graph_db, original_name_in_graph_db_idx_key, is_new)
        VALUES (%s, %s, 1)
        ON DUPLICATE KEY UPDATE
            original_name_in_graph_db = VALUES(original_name_in_graph_db),
            is_new = 1
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("OrganizationLocationRorLookupTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Find Organization nodes needing ROR data and stage lookup results."""

        total_processed = 0
        total_found = 0
        total_not_found = 0
        total_already_in_db = 0
        start_time = time.time()

        try:
            for batch_num, batch in self.organization_generator(self.BATCH_SIZE):
                batch_start = time.time()

                if not batch:
                    self.logger.info(f"Batch #{batch_num}: empty batch, skipped.")
                    continue

                # Only call ROR for graph organizations that are not already in
                # organization_location; existing rows are re-marked as new.
                batch_idx_keys = [org["_idx_key"] for org in batch if org.get("_idx_key")]
                idx_keys_to_process = set(self.find_idx_keys_not_in_db(batch_idx_keys))
                existing_idx_keys = [
                    idx_key
                    for idx_key in batch_idx_keys
                    if idx_key not in idx_keys_to_process
                ]
                marked_existing_count = self.mark_existing_organization_locations_as_new(existing_idx_keys)

                org_list_to_process = [
                    org
                    for org in batch
                    if org.get("_idx_key") in idx_keys_to_process and org.get("name")
                ]

                already_in_db_count = len(batch_idx_keys) - len(idx_keys_to_process)
                total_already_in_db += already_in_db_count

                self.logger.info(
                    f"Batch #{batch_num}: graph organizations={len(batch)}, "
                    f"already in DB={already_in_db_count}, "
                    f"marked existing is_new=1 rows={marked_existing_count}, "
                    f"to query in ROR={len(org_list_to_process)}."
                )

                if not org_list_to_process:
                    continue

                # Split the API results into found and not-found lists so both
                # outcomes are saved and the graph can suppress future misses.
                found_orgs, not_found_orgs, not_found_idx_keys = self.lookup_organizations(org_list_to_process)

                saved_found_count = self.save_found_organizations(found_orgs)
                saved_not_found_count = self.save_not_found_organizations(not_found_orgs)

                if not_found_idx_keys:
                    self.mark_not_found_in_graph(not_found_idx_keys)

                batch_processed = saved_found_count + saved_not_found_count
                total_processed += batch_processed
                total_found += saved_found_count
                total_not_found += saved_not_found_count

                hours, minutes, seconds = _time_hms(time.time() - batch_start)
                self.logger.info(
                    f"Batch #{batch_num} complete: found={saved_found_count}, "
                    f"not_found={saved_not_found_count}, processed={batch_processed}, "
                    f"time={hours} hours, {minutes} minutes, {seconds} seconds."
                )

            total_hours, total_minutes, total_seconds = _time_hms(time.time() - start_time)
            self.logger.info(
                "Completed OrganizationLocationRorLookupTask: "
                f"processed={total_processed}, found={total_found}, "
                f"not_found={total_not_found}, already_in_db={total_already_in_db}, "
                f"time={total_hours} hours, {total_minutes} minutes, {total_seconds} seconds."
            )

        except Exception as e:
            self.logger.error(f"OrganizationLocationRorLookupTask failed: {e}")

        finally:
            ''' Explicitly close all db connections. '''
            self.close()


    def organization_generator(self, batch_size: int = 100):

        '''
        Yield Organization nodes that still need ROR lookup.
        Pagination uses _idx_key instead of SKIP so marking not-found nodes as
        N/A does not change the remaining page offsets while the task is running.
        '''
        last_idx_key = None
        batch_num = 1

        while True:
            if last_idx_key is None:
                query = '''
                    MATCH (o:Organization)
                    WHERE o.ror_id = ''
                    AND o._idx_key IS NOT NULL
                    RETURN o.name AS name, o._idx_key AS _idx_key
                    ORDER BY o._idx_key
                    LIMIT $limit
                '''
                params = {"limit": batch_size}
            else:
                query = '''
                    MATCH (o:Organization)
                    WHERE o.ror_id = ''
                    AND o._idx_key IS NOT NULL
                    AND o._idx_key > $lastIdxKey
                    RETURN o.name AS name, o._idx_key AS _idx_key
                    ORDER BY o._idx_key
                    LIMIT $limit
                '''
                params = {"lastIdxKey": last_idx_key, "limit": batch_size}

            rows = list(self.memgraph.execute_and_fetch(query, params))

            if not rows:
                break

            last_idx_key = rows[-1].get("_idx_key")
            yield batch_num, rows
            batch_num += 1


    def find_idx_keys_not_in_db(self, idx_keys: List[str]) -> List[str]:

        '''
        Skip organizations already stored in organization_location so ROR is not
        called repeatedly for the same graph node.
        '''
        if not idx_keys:
            return []

        placeholders = ",".join(["%s"] * len(idx_keys))
        query = f'''
            SELECT original_name_in_graph_db_idx_key
            FROM {self.TABLE_NAME}
            WHERE original_name_in_graph_db_idx_key IN ({placeholders})
        '''

        cursor = None

        try:
            cursor = self.mysql.cursor(buffered=True, dictionary=True)
            cursor.execute(query, tuple(idx_keys))
            rows = cursor.fetchall()
            found_keys = {row["original_name_in_graph_db_idx_key"] for row in rows}

            return [idx_key for idx_key in idx_keys if idx_key not in found_keys]

        except Exception as e:
            self.logger.error(f"Error checking existing organization_location rows: {e}")
            return idx_keys

        finally:
            if cursor:
                cursor.close()


    def mark_existing_organization_locations_as_new(self, idx_keys: List[str]) -> int:

        '''
        Existing organization_location rows may still need to be consumed by
        later alert steps when the matching graph Organization has ror_id = ''.
        Mark them as is_new = 1 so downstream tasks can filter on that flag.
        '''
        if not idx_keys:
            return 0

        placeholders = ",".join(["%s"] * len(idx_keys))
        update_sql = f'''
            UPDATE {self.TABLE_NAME}
            SET is_new = 1
            WHERE original_name_in_graph_db_idx_key IN ({placeholders})
        '''

        cursor = None

        try:
            cursor = self.mysql.cursor(buffered=True)
            cursor.execute(update_sql, tuple(idx_keys))
            self.mysql.commit()

            return cursor.rowcount

        except Exception as e:
            self.logger.error(f"Error marking existing organization_location rows as is_new=1: {e}")

            if self.mysql:
                self.mysql.rollback()

            return 0

        finally:
            if cursor:
                cursor.close()


    def lookup_organizations(self, orgs: List[Dict[str, Any]]) -> Tuple[List[Tuple[Any, ...]], List[Tuple[str, str]], List[str]]:

        '''
        Run ROR lookups in parallel. ROR calls are network I/O, so threads are a
        simpler fit than multiprocessing inside the alert runner.
        '''
        found_orgs = []
        not_found_orgs = []
        not_found_idx_keys = []

        if not orgs:
            return found_orgs, not_found_orgs, not_found_idx_keys

        max_workers = min(self.MAX_WORKERS, len(orgs))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_org = {
                executor.submit(
                    lookup_ror_static,
                    org.get("name"),
                    org.get("_idx_key"),
                    self.ROR_TIMEOUT,
                ): org
                for org in orgs
            }

            for future in as_completed(future_to_org):
                org = future_to_org[future]
                org_name = org.get("name") or ""
                idx_key = org.get("_idx_key")

                try:
                    result = future.result()
                except Exception as e:
                    self.logger.error(f"ROR lookup failed for organization={org_name}, idx_key={idx_key}: {e}")
                    result = None

                if result is not None:
                    found_orgs.append(result)
                    self.logger.info(f"ROR found: {org_name}")
                else:
                    not_found_idx_keys.append(idx_key)
                    not_found_orgs.append((str(org_name)[:500], idx_key))
                    self.logger.info(f"ROR not found: {org_name}")

        return found_orgs, not_found_orgs, not_found_idx_keys


    def save_found_organizations(self, org_info_tuple_list: List[Tuple[Any, ...]]) -> int:
        """Save successful ROR lookup rows into organization_location."""

        if not org_info_tuple_list:
            return 0

        cursor = None

        try:
            cursor = self.mysql.cursor(buffered=True)
            cursor.executemany(self.INSERT_FOUND_ORGANIZATION_SQL, org_info_tuple_list)
            self.mysql.commit()
            self.logger.info(f"Saved {cursor.rowcount} found organizations into {self.TABLE_NAME}.")

            return len(org_info_tuple_list)

        except Exception as e:
            self.logger.error(f"Error saving found organization_location rows: {e}")

            if self.mysql:
                self.mysql.rollback()

            return 0

        finally:
            if cursor:
                cursor.close()


    def save_not_found_organizations(self, not_found_list: List[Tuple[str, str]]) -> int:
        """Save failed ROR lookups so they are not repeatedly queried."""

        if not not_found_list:
            return 0

        cursor = None

        try:
            cursor = self.mysql.cursor(buffered=True)
            cursor.executemany(self.INSERT_NOT_FOUND_ORGANIZATION_SQL, not_found_list)
            self.mysql.commit()
            self.logger.info(f"Saved {cursor.rowcount} not-found organizations into {self.TABLE_NAME}.")

            return len(not_found_list)

        except Exception as e:
            self.logger.error(f"Error saving not-found organization_location rows: {e}")

            if self.mysql:
                self.mysql.rollback()

            return 0

        finally:
            if cursor:
                cursor.close()


    def mark_not_found_in_graph(self, idx_keys: List[str]) -> None:
        """Mark Organization nodes with no ROR match as ror_id='N/A'."""

        if not idx_keys:
            return

        try:
            self.memgraph.execute(self.MARK_NOT_FOUND_IN_GRAPH, {"chunks": idx_keys})
            self.logger.info(f"Marked {len(idx_keys)} Organization nodes as ror_id='N/A'.")

        except Exception as e:
            self.logger.error(f"Error marking not-found Organization nodes in Memgraph: {e}")

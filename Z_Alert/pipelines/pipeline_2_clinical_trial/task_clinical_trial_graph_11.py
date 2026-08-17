import os
import sys
import json
from typing import Any, Dict, List

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "../..")),
    os.path.abspath(os.path.join(_dir, "../../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _make_hash_key, _remove_parentheses

"""
Create or refresh Organization and Location nodes for new or changed clinical trials.
"""
# Reference: B_clinical_trial/initializer/organization_location.py


class NewClinicalTrialOrganizationLocationGraphTask(PipelineBase):
    """
    Create or refresh Organization and Location nodes for newly imported or
    changed clinical trials.

    ClinicalTrials.gov stores the responsible organization under
    identificationModule and trial sites under contactsLocationsModule. This
    task mirrors the bulk initializer's node keys and relationships for rows
    that are new in the alert pipeline.
    """

    BATCH_SIZE = 100

    '''
    FOREACH creates the Organization only when the current study JSON has an
    organization, while still allowing Location nodes to be created when a trial
    has locations but no organization. Existing ROR enrichment fields are
    preserved by refreshing only source-owned properties after MERGE.
    '''
    BATCH_CREATE = '''
        UNWIND $chunks AS chunk
        MATCH (ct: ClinicalTrial {nctId: chunk.nctId})

        FOREACH (_ IN CASE WHEN chunk.hasOrganization THEN [1] ELSE [] END |
            MERGE (org: Organization {_idx_key: chunk.org_idx_key})
            ON CREATE SET
                org.displayName = '',
                org.ror_id = '',
                org.website = '',
                org.types = []
            SET org.name = chunk.org_name

            MERGE (ct)-[:has_associated_organization]->(org)
        )

        WITH chunk, ct
        UNWIND chunk.locations AS location
        MERGE (loc: Location {_idx_key: location.loc_idx_key})
        ON CREATE SET
            loc.facility = location.facility,
            loc.address = location.address,
            loc.city = location.city,
            loc.state = location.state,
            loc.country = location.country,
            loc.zip = location.zip,
            loc.countryCode = ''
        SET
            loc.facility = location.facility,
            loc.address = location.address,
            loc.city = location.city,
            loc.state = location.state,
            loc.country = location.country,
            loc.zip = location.zip

        MERGE (ct)-[:has_trial_location]->(loc)
    '''

    '''
    Remove old ClinicalTrial -> Organization relationships when a changed study
    now has no organization or points to a different organization. Organization
    nodes are shared and may contain ROR enrichment, so only the relationship is
    deleted.
    '''
    BATCH_DELETE_STALE_ORGANIZATION_RELATIONSHIPS = '''
        UNWIND $chunks AS chunk
        MATCH (ct: ClinicalTrial {nctId: chunk.nctId})
        OPTIONAL MATCH (ct)-[old_rel:has_associated_organization]->(old:Organization)
        WHERE old_rel IS NOT NULL
        AND (
            NOT chunk.hasOrganization
            OR old._idx_key IS NULL
            OR old._idx_key <> chunk.org_idx_key
        )
        DELETE old_rel
    '''

    '''
    Remove old ClinicalTrial -> Location relationships when changed study JSON no
    longer includes a previous site. This is what lets changed trials add more
    Location links while dropping only locations that disappeared from the latest
    NCTID JSON.
    '''
    BATCH_DELETE_STALE_LOCATION_RELATIONSHIPS = '''
        UNWIND $chunks AS chunk
        MATCH (ct: ClinicalTrial {nctId: chunk.nctId})
        WITH ct, [location IN chunk.locations | location.loc_idx_key] AS current_location_keys
        OPTIONAL MATCH (ct)-[old_rel:has_trial_location]->(old:Location)
        WHERE old_rel IS NOT NULL
        AND (
            old._idx_key IS NULL
            OR NOT old._idx_key IN current_location_keys
        )
        DELETE old_rel
    '''

    '''
    If a changed trial now has neither organization nor locations, no create
    chunk exists. These queries still clear stale relationships for those NCTIDs.
    '''
    BATCH_DELETE_ALL_ORGANIZATION_RELATIONSHIPS = '''
        UNWIND $nctids AS nctid
        MATCH (ct: ClinicalTrial {nctId: nctid})-[old_rel:has_associated_organization]->(:Organization)
        DELETE old_rel
    '''

    BATCH_DELETE_ALL_LOCATION_RELATIONSHIPS = '''
        UNWIND $nctids AS nctid
        MATCH (ct: ClinicalTrial {nctId: nctid})-[old_rel:has_trial_location]->(:Location)
        DELETE old_rel
    '''

    FETCH_NEW_CLINICAL_QUERY = '''
        SELECT id, nctid, studies
        FROM clinical_trial_unique
        WHERE nctid IS NOT NULL
        AND is_new = 1
    '''

    def __init__(self):

        """Initialize MySQL and Memgraph connections for organization/location loading."""

        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:

        raise NotImplementedError("NewClinicalTrialOrganizationLocationGraphTask does not implement find_new_data().")


    # implement
    def process_new_data(self) -> None:

        """Fetch changed trial JSON and write Organization/Location graph chunks."""

        total_chunks = 0
        total_organizations = 0
        total_locations = 0
        empty_organization_location_nctids = []
        batch_num = 0
        fetch_cursor = None

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(self.FETCH_NEW_CLINICAL_QUERY)

            while True:
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    self.logger.info("No more rows to fetch.")
                    break

                batch_num += 1
                self.logger.info(f'--- batch# = {batch_num} ---')

                chunks = []

                for row in rows:
                    nctid = row.get('nctid')
                    if not nctid:
                        continue

                    try:
                        study = json.loads(row.get('studies') or '{}')
                    except (json.JSONDecodeError, TypeError) as e:
                        self.logger.error(f"Invalid JSON for nctId {nctid}: {e}")
                        continue

                    organization_location_chunk = self._create_organization_location_chunk(nctid, study)
                    if organization_location_chunk:
                        chunks.append(organization_location_chunk)
                    else:
                        empty_organization_location_nctids.append(nctid)

                if chunks:
                    self.memgraph.execute(self.BATCH_CREATE, {"chunks": chunks})
                    self.memgraph.execute(self.BATCH_DELETE_STALE_ORGANIZATION_RELATIONSHIPS, {"chunks": chunks})
                    self.memgraph.execute(self.BATCH_DELETE_STALE_LOCATION_RELATIONSHIPS, {"chunks": chunks})

                    organization_count = sum(1 for chunk in chunks if chunk["hasOrganization"])
                    location_count = sum(len(chunk["locations"]) for chunk in chunks)

                    total_chunks += len(chunks)
                    total_organizations += organization_count
                    total_locations += location_count
                    self.logger.info(
                        f'Upserted {organization_count} organization mappings and '
                        f'{location_count} location mappings in memgraph. '
                        f'Total chunks/organizations/locations = '
                        f'{total_chunks}/{total_organizations}/{total_locations}'
                    )
                else:
                    self.logger.info('No valid organizations or locations to upsert into memgraph.')

            if empty_organization_location_nctids:
                self.memgraph.execute(self.BATCH_DELETE_ALL_ORGANIZATION_RELATIONSHIPS, {"nctids": empty_organization_location_nctids})
                self.memgraph.execute(self.BATCH_DELETE_ALL_LOCATION_RELATIONSHIPS, {"nctids": empty_organization_location_nctids})
                self.logger.info(f'Removed stale organization/location mappings for {len(empty_organization_location_nctids)} trials with no current organizations or locations.')

        except Exception as e:
            self.logger.error(f"Error executing organization/location graph task: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def _create_organization_location_chunk(self, nctid: str, study: Dict[str, Any]) -> Dict[str, Any]:

        """Convert one study payload into a Cypher chunk for org/site loading."""

        if not isinstance(study, dict):
            return {}

        protocol = study.get('protocolSection', {})
        if not isinstance(protocol, dict):
            return {}

        locations = self._extract_locations(protocol)
        organization = self._extract_organization_name(protocol)

        if not organization and not locations:
            return {}

        locs = self._create_location_chunks(nctid, locations, organization)
        if not organization and not locs:
            return {}

        return {
            "nctId": nctid,
            "hasOrganization": bool(organization),
            "org_idx_key": _make_hash_key(organization) if organization else None,
            "org_name": organization,
            "locations": locs
        }


    def _extract_organization_name(self, protocol: Dict[str, Any]) -> str:

        """Read and normalize the organization fullName used by the initializer."""

        identification_module = protocol.get('identificationModule', {})
        if not isinstance(identification_module, dict):
            return ''

        organization = identification_module.get('organization', {})
        if not isinstance(organization, dict):
            return ''

        full_name = organization.get('fullName')
        if not full_name:
            return ''

        return _remove_parentheses(str(full_name))


    def _extract_locations(self, protocol: Dict[str, Any]) -> List[Dict[str, Any]]:

        """Read trial location records from contactsLocationsModule."""

        contacts_locations_module = protocol.get('contactsLocationsModule', {})
        if not isinstance(contacts_locations_module, dict):
            return []

        locations = contacts_locations_module.get('locations', [])
        return locations if isinstance(locations, list) else []


    def _create_location_chunks(self, nctid: str, locations: List[Dict[str, Any]], organization: str) -> List[Dict[str, Any]]:

        """Build Location chunk dictionaries with initializer-compatible keys."""

        locs = []
        org_idx_key = _make_hash_key(organization) if organization else None

        for loc in locations:
            if not isinstance(loc, dict):
                continue

            loc_idx_key = None
            facility = loc.get('facility', None)
            if facility:
                loc_idx_key = _make_hash_key(facility)

            ''' geoPoint as index key is more precise than the facility. '''
            geo_point = loc.get('geoPoint', None)
            if isinstance(geo_point, dict):
                lat = geo_point.get('lat')
                lon = geo_point.get('lon')
                if lat is not None and lon is not None:
                    loc_idx_key = _make_hash_key(str(lat) + '' + str(lon))

            if not loc_idx_key:
                continue

            locs.append(
                {
                    "nctId": nctid,
                    "facility": facility,
                    "address": "",
                    "city": loc.get('city', ''),
                    "state": loc.get('state', ''),
                    "country": loc.get('country', ''),
                    "zip": loc.get('zip', ''),
                    "loc_idx_key": loc_idx_key,
                    "org_idx_key": org_idx_key
                }
            )

        return locs

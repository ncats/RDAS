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
Create Organization and Location nodes for new clinical trials.
"""
# Reference: B_clinical_trial/initializer/organization_location.py


class NewClinicalTrialOrganizationLocationGraphTask(PipelineBase):
    """
    Create Organization and Location nodes for newly imported clinical trials.

    ClinicalTrials.gov stores the responsible organization under
    identificationModule and trial sites under contactsLocationsModule. This
    task mirrors the bulk initializer's node keys and relationships for rows
    that are new in the alert pipeline.
    """

    BATCH_SIZE = 100 

    # FOREACH: it allowed “create org only when present, but still create locations.”
    BATCH_CREATE = '''
        UNWIND $chunks AS chunk
        MATCH (ct: ClinicalTrial {nctId: chunk.nctId})
        
        FOREACH (_ IN CASE WHEN chunk.hasOrganization THEN [1] ELSE [] END |
            MERGE (org: Organization {_idx_key: chunk.org_idx_key})
            ON CREATE SET
                org.name = chunk.org_name,
                org.displayName = '',
                org.ror_id = '',
                org.website = '',
                org.types = []

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

        MERGE (ct)-[:has_trial_location]->(loc)
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
        """Fetch new trial JSON and write Organization/Location graph chunks."""

        total_chunks = 0
        total_organizations = 0
        total_locations = 0
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

                if chunks:
                    self.memgraph.execute(self.BATCH_CREATE, {"chunks": chunks})

                    organization_count = sum(1 for chunk in chunks if chunk["hasOrganization"])
                    location_count = sum(len(chunk["locations"]) for chunk in chunks)

                    total_chunks += len(chunks)
                    total_organizations += organization_count
                    total_locations += location_count
                    self.logger.info(
                        f'Created {organization_count} organization mappings and '
                        f'{location_count} location mappings in memgraph. '
                        f'Total chunks/organizations/locations = '
                        f'{total_chunks}/{total_organizations}/{total_locations}'
                    )
                else:
                    self.logger.info('No valid organizations or locations to insert into memgraph.')

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


    def _create_location_chunks(
        self,
        nctid: str,
        locations: List[Dict[str, Any]],
        organization: str
    ) -> List[Dict[str, Any]]:
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

            # geoPoint as index key is more precise than the facility.
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

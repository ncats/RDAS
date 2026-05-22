import os
import sys
import json
import time
from typing import Any, Dict, List

import requests
import urllib3
from dotenv import load_dotenv

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

load_dotenv(os.path.abspath(os.path.join(_dir, "..", ".env")))

from baseclass.conn import DBConnection as db
from utils.applogger import AppLogger
from utils.tools import _time_hms
 
# *** Update the existing data in organization_location table ***

class RORFindOrganizationTask:

    BATCH_SIZE = 100
    TABLE_NAME = "organization_location"   
    PROCESSED_FLAG = "llama3.1_org_name_extracted"

    LOOKUP_ERROR = object()

    def __init__(self):

        self.mysql = db().mysql_conn() 
        self.log_dir = os.path.expanduser(os.getenv("ALERT_LOG_DIR", "logs"))
        os.makedirs(self.log_dir, exist_ok=True)

        class_name = type(self).__name__
        self.log_file = f"{self.log_dir}/alert-{class_name}.log"
        self.logger = AppLogger(class_name, self.log_file).get_logger()
        self.logger.info(f'\n\n{"*" * 20} The {class_name} is initialized. {"*" * 20}\n')

        self.ror_ssl_verify = self.get_ror_ssl_verify_setting()
  

    def update(self) -> None:

        '''
        These counters summarize the whole run. "found" means ROR returned a
        chosen organization and the row received ROR/location fields.
        "not_found" means ROR returned no chosen match and the row was marked
        ror_id = 'N/A'. "errors" means an API or parsing problem happened, so
        the row is intentionally left with ror_id IS NULL for a later retry.
        '''
        total_found = 0
        total_not_found = 0
        total_errors = 0
        batch_num = 0
        start_time = time.time()

        try:  
            while True:
                '''
                Step 1:
                Fetch one small MySQL batch. The WHERE clause only selects
                records that already have a model_extracted_name from step 1,
                have not yet received ROR data, and are safe to send to the ROR
                affiliation endpoint.
                '''
                rows = self.fetch_organization_batch()

                if not rows:
                    self.logger.info('No more organizations to process')
                    break

                batch_num += 1
                found_vals = []
                not_found_ids = []
                error_count = 0

                self.logger.info(f'\n\n------ batch #: {batch_num} ------\n')

                '''
                Step 2:
                Process each row in the batch through the ROR API. The lookup
                method returns one of three values:

                - tuple: ROR found a chosen organization; save the returned data.
                - None: ROR did not find a chosen organization; mark as N/A.
                - LOOKUP_ERROR: network/parsing error; leave unmodified for retry.
                '''
                idx = 0
                for row in rows:

                    id = row['id']
                    model_extracted_name = row['model_extracted_name']
                    idx += 1
                    
                    val = self.lookup_ror_static(id, model_extracted_name)

                    if val is self.LOOKUP_ERROR:
                        error_count += 1

                        self.logger.warning(
                            f"#{idx}, [id]={id}, [model_extracted_name]={model_extracted_name}, "
                            f"[found_name]= N/A"
                        )
                        continue

                    if val is None:
                        not_found_ids.append(id)
                        continue

                    org_display_name = val[1]
                    self.logger.info(
                        f"#{idx}, [id]={id}, [model_extracted_name]={model_extracted_name}, "
                        f"[found_name]={org_display_name}"
                    )

                    found_vals.append(val)

                '''
                Step 3:
                Save successful ROR matches in one executemany call. This writes
                the ROR id, display name, website, location, coordinates, and
                full API response back to organization_location.
                '''
                found_count = self.update_found_organization_location_data(found_vals)

                '''
                Step 4:
                Mark no-match rows as ror_id = 'N/A'. This is important because
                otherwise the same rows would continue to match ror_id IS NULL
                and be sent to ROR on every loop.
                '''
                not_found_count = self.mark_not_found_organization_locations(not_found_ids)

                '''
                Step 5:
                Update run-level counters after both database writes finish.
                Lookup errors are counted but not saved, so those rows can be
                retried in a future run.
                '''
                total_found += found_count
                total_not_found += not_found_count
                total_errors += error_count

                self.logger.info(
                    f"Batch #{batch_num}: fetched={len(rows)}, found={found_count}, "
                    f"not_found={not_found_count}, lookup_errors={error_count}."
                )

                '''
                Step 6:
                If nothing was updated, stop the loop. This avoids repeatedly
                fetching the same batch when every row hit a retryable ROR/API
                error.
                '''
                if found_count == 0 and not_found_count == 0:
                    self.logger.error(
                        f"Batch #{batch_num}: no rows were updated. "
                        "Stopping to avoid repeatedly fetching the same rows."
                    )
                    break

            '''
            Step 7:
            Log the final totals and elapsed time after all eligible rows have
            been processed or the loop stops because of retryable failures.
            '''
            hours, minutes, seconds = _time_hms(time.time() - start_time)
            self.logger.info(
                f"Completed RORFindOrganizationTask. found={total_found}, "
                f"not_found={total_not_found}, lookup_errors={total_errors}, "
                f"time={hours} hours, {minutes} minutes, {seconds} seconds."
            )

        except Exception as e:
            self.logger.error(f"RORFindOrganizationTask failed: {e}")
        finally:
            '''
            Step 8:
            Always close the MySQL connection and logger handlers so the script
            can exit cleanly and log files are flushed.
            '''
            if self.mysql is not None and self.mysql.is_connected():
                print(f"Closing MySQL connection...")
                self.mysql.close()

            self.mysql = None

            print('MySQL connection closed')

            if hasattr(self, "logger") and self.logger is not None:
                for handler in list(self.logger.handlers):
                    handler.flush()
                    handler.close()
                    self.logger.removeHandler(handler)

                self.logger = None
 

    def update_found_organization_location_data(self, vals: List[tuple]) -> int: 

        '''
        vals contains one tuple per ROR match. Each tuple is already ordered to
        match the UPDATE placeholders below, ending with organization_location.id.
        '''
        if not vals:
            return 0
               
        '''
        A successful ROR match should make this row available to the next graph
        sync step. is_new = 1 marks it as a newly updated MySQL row that should
        be pushed into Memgraph.
        '''
        sql = '''
            UPDATE organization_location
            SET 
                ror_id = %s,
                org_name = %s,
                geonames_id = %s,
                established = %s,
                status = %s,
                types = %s,
                website = %s,
                city = %s,
                country = %s,
                country_code = %s,
                state = %s,
                lat = %s,
                lng = %s,
                full_data = %s,
                is_new = 1
            WHERE id = %s 
        '''

        cursor = None

        try:
            '''
            Use executemany so the entire found portion of the batch is written
            with one cursor operation instead of one UPDATE per organization.
            '''
            cursor = self.mysql.cursor(buffered=True)
            cursor.executemany(sql, vals) 
            self.mysql.commit()

            return cursor.rowcount

        except Exception as e: 
            self.logger.error(f"Error updating found organization_location rows: {e}")

            if self.mysql:
                self.mysql.rollback()

            return 0

        finally:
            if cursor:
                cursor.close()


    def mark_not_found_organization_locations(self, ids: List[int]) -> int:
        """Mark rows with no chosen ROR match so they are not fetched forever."""

        '''
        ids contains organization_location.id values that ROR looked up
        successfully but did not match to a chosen organization.
        '''
        if not ids:
            return 0

        '''
        ror_id = 'N/A' is a terminal value for this lookup stage. It means the
        request completed, but ROR did not provide a chosen organization. This
        prevents repeated calls for the same model_extracted_name.
        '''
        sql = '''
            UPDATE organization_location
            SET ror_id = 'N/A'
            WHERE id = %s
        '''

        cursor = None

        try:
            '''
            Convert the id list to one-column tuples because executemany expects
            a sequence of parameter sequences.
            '''
            cursor = self.mysql.cursor(buffered=True)
            cursor.executemany(sql, [(id,) for id in ids])
            self.mysql.commit()

            return cursor.rowcount

        except Exception as e:
            self.logger.error(f"Error marking not-found organization_location rows: {e}")

            if self.mysql:
                self.mysql.rollback()

            return 0

        finally:
            if cursor:
                cursor.close()

            
    def lookup_ror_static(self, id: int, model_extracted_name: str, timeout: int = 20):

        '''
        Normalize the Llama-extracted name before sending it to ROR. Empty names
        are treated as no-match, not as API errors.
        '''
        cleaned_org_name = self.clean_org_name(model_extracted_name)

        if not cleaned_org_name:
            return None

        try:
            '''
            Query the ROR affiliation endpoint. ROR_ORGANIZATIONS_API can be
            configured in .env, otherwise the public v2 organizations endpoint
            is used.
            '''
            url = os.getenv("ROR_ORGANIZATIONS_API", "https://api.ror.org/v2/organizations")
            r = requests.get(
                url,
                params={"affiliation": cleaned_org_name},
                timeout=timeout,
                verify=self.ror_ssl_verify,
            )
            r.raise_for_status()
            data = r.json()

            '''
            ROR can return results that are not "chosen". Only chosen matches
            are trusted enough to update organization_location with a ROR id.
            '''
            items = data.get("items") or []

            # Check if API returned valid results
            if data.get('number_of_results') == 0 or not items or not items[0].get('chosen'):
                return None
                
            first_item = items[0]
            org = first_item['organization']

            '''
            Extract the canonical ROR organization fields used downstream by
            the graph sync step.
            '''
            # Extract core organization data
            ror_id = org['id']
            names = org['names']
            org_display_name = next(
                (name['value'] for name in names if 'ror_display' in name.get('types', [])), 
                None
            )

            # Extract location data
            locations = org.get("locations") or []
            location_details = locations[0].get("geonames_details") if locations else {}
            geonames_id = locations[0].get("geonames_id") if locations else None

            # Extract geographic coordinates
            lat = location_details.get("lat") if location_details else None
            lng = location_details.get("lng") if location_details else None

            # Prepare database fields 
            geonames_id_int = int(geonames_id) if geonames_id else None
            established = org.get("established")
            status = org.get("status")
            types = json.dumps(org.get("types", []))

            # Extract website (max 500 chars to match DB column)
            website = next(
                (link.get("value", "").strip()[:500] 
                for link in org.get("links", []) 
                if link.get("type") == "website" and link.get("value")), 
                None
            )
            
            # Extract location details
            city = location_details.get("name") if location_details else None
            country = location_details.get("country_name") if location_details else None
            country_code = location_details.get("country_code") if location_details else None
            state = location_details.get("country_subdivision_name") if location_details else None
            lat_float = float(lat) if lat else None
            lng_float = float(lng) if lng else None

            '''
            Store the complete ROR organization object as JSON so the row keeps
            a trace of the source data used for this update.
            '''
            # Store complete API response for reference
            full_data = json.dumps(org)

            '''
            Return values in exactly the same order as
            update_found_organization_location_data() expects.
            '''
            return (
                ror_id, org_display_name, geonames_id_int, established, 
                status, types, website, city, country, 
                country_code, state, lat_float, lng_float, full_data,
                id
            )
                
        except requests.RequestException as e:
            self.logger.error(f"ROR request failed for id={id}, name={cleaned_org_name}: {e}")
            return self.LOOKUP_ERROR

        except (KeyError, ValueError, json.JSONDecodeError) as e:
            self.logger.error(f"ROR response parsing failed for id={id}, name={cleaned_org_name}: {e}")
            return self.LOOKUP_ERROR

        except Exception as e:
            self.logger.error(f"Unexpected ROR lookup failure for id={id}, name={cleaned_org_name}: {e}")
            return self.LOOKUP_ERROR


    def clean_org_name(self, value):
        """Clean the model-extracted organization name before sending to ROR."""

        '''
        Keep this cleanup conservative. Step 1 already uses Llama to identify
        the organization name, so here we only trim and normalize whitespace
        before using it as the ROR affiliation query.
        '''
        if not value:
            return ""

        return " ".join(str(value).strip().split())


    def get_ror_ssl_verify_setting(self):
        """
        Decide how requests should verify ROR HTTPS certificates.

        Preferred fix for self-signed certificate errors:
        set ROR_CA_BUNDLE=/path/to/corporate-or-local-ca.pem in .env.

        Temporary local workaround:
        set ROR_VERIFY_SSL=false in .env. This disables certificate
        verification for ROR requests only and should not be used as the
        permanent production setting.
        """

        ca_bundle = (
            os.getenv("ROR_CA_BUNDLE")
            or os.getenv("REQUESTS_CA_BUNDLE")
            or os.getenv("CURL_CA_BUNDLE")
        )

        if ca_bundle:
            ca_bundle = os.path.expanduser(ca_bundle)
            self.logger.info(f"Using custom CA bundle for ROR requests: {ca_bundle}")
            return ca_bundle

        verify_ssl = os.getenv("ROR_VERIFY_SSL", "true").strip().lower()

        if verify_ssl in {"0", "false", "no", "off"}:
            self.logger.warning("ROR SSL certificate verification is disabled by ROR_VERIFY_SSL.")
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            return False

        return True
        


    def fetch_organization_batch(self) -> List[Dict[str, Any]]: 

        '''
        Fetch only rows prepared by step 1. The ORDER BY id + LIMIT pattern keeps
        each pass small and deterministic. Rows are removed from future batches
        when this step writes either a real ror_id or ror_id = 'N/A'.
        '''
        fetch_query = f'''
            SELECT id, model_extracted_name 
            FROM {self.TABLE_NAME}
            WHERE ror_id IS NULL
            AND processed = %s
            AND model_extracted_name IS NOT NULL
            AND model_extracted_name <> ''
            ORDER BY id
            LIMIT %s
        ''' 
        cursor = None

        try:
            cursor = self.mysql.cursor(dictionary=True, buffered=True)
            cursor.execute(fetch_query, (self.PROCESSED_FLAG, self.BATCH_SIZE))
            return cursor.fetchall()

        finally:
            if cursor:
                cursor.close()



if __name__ == "__main__":

    RORFindOrganizationTask().update()

 

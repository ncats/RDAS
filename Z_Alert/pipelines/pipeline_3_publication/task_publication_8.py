import os
import sys
import json

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.publication_worker import PublicationWorker

"""
Find publication articles referenced by newly retrieved OMIM entries.

The publication pipeline first discovers new GARD/OMIM mappings and downloads
the OMIM entry JSON into publication_omim. This task turns those new OMIM
entries into PubMed work:

1. Read OMIM IDs that were added in publication_gard_omim_mapping.
2. Fetch matching publication_omim.entry_json rows where is_new = 1.
3. Parse entryList[].entry.referenceList[].reference.pubmedID values.
4. Save OMIM/PubMed pairs in publication_omim_pubmed_mapping.
5. Download only PubMed articles that are not already in publication_article.
"""

# Reference: Z_Alert/pipelines/pipeline_2_clinical_trial/task_clinical_trial_4.py
# Reference: C_publication/init_6_publication-retrieve-omim.py

class NewOmimPublicationArticleImportTask(PipelineBase):

    BATCH_SIZE = 50

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=False)
        self.publication_worker = PublicationWorker()


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        self.logger.info("NewOmimPublicationArticleImportTask does not use find_new_data().")


    # implement
    def process_new_data(self) -> None:

        try:
            '''
            Step 1: publication_gard_omim_mapping is the source of new OMIM IDs
            for this alert run. These IDs were created by the OMIM mapping task.
            '''
            new_omim_id_list = self.get_unique_new_omim_id_list()

            if not new_omim_id_list:
                self.logger.info("No new OMIM IDs found in publication_gard_omim_mapping.")
                return

            '''
            Step 2: Only process OMIM entries that were actually downloaded into
            publication_omim and are still marked is_new = 1.
            '''
            omim_rows = self.get_new_omim_entry_json(new_omim_id_list)

            if not omim_rows:
                self.logger.info("No new OMIM entry_json rows found in publication_omim.")
                return

            '''
            Step 3: Parse the OMIM JSON shape:
            omim.entryList[].entry.referenceList[].reference.pubmedID.
            Keep both an OMIM -> PMID mapping dictionary and a flattened PMID set.
            '''
            omim_pubmedids_dict = {}
            all_pubmed_ids = set()

            for row in omim_rows:
                omim_id = row.get('omim_id')
                pubmed_id_list = self._extract_pubmed_ids_from_entry_json(omim_id, row.get('entry_json'))

                if not pubmed_id_list:
                    self.logger.info(f"No PubMed IDs found in OMIM entry_json for omim_id={omim_id}.")
                    continue

                omim_pubmedids_dict[omim_id] = pubmed_id_list
                all_pubmed_ids.update(pubmed_id_list)

            if not all_pubmed_ids:
                self.logger.info("No PubMed IDs found from new OMIM entry_json rows.")
                return

            '''
            Step 4: Persist OMIM/PubMed relationships first so the source
            provenance remains available even when an article already exists.
            '''
            self._save_omim_pubmed_mappings(omim_pubmedids_dict)

            '''
            Step 5: Import only the missing article metadata into
            publication_article. Later publication tasks will classify and graph
            these rows by using publication_article.is_new = 1.
            '''
            self._retrieve_missing_publication_articles(all_pubmed_ids)

        except Exception as e:
            self.logger.error(f"Error retrieving OMIM publication articles: {e}")

        finally:

            ''' Explicitly close all db connections. '''
            self.close()



    def get_new_omim_entry_json(self, omim_id_list):

        '''
        Fetch the OMIM JSON payloads for the supplied OMIM IDs.
        The IN clause is built with placeholders and processed in batches so a
        large alert run does not create an oversized SQL statement.
        '''
        if not omim_id_list:
            return []

        ''' Normalize input and drop blank/None values before building SQL. '''
        omim_ids = [
            omim_id
            for omim_id in omim_id_list
            if omim_id is not None and str(omim_id).strip()
        ]

        if not omim_ids:
            return []

        fetch_cursor = None
        rows = []

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)

            for batch in self._iter_batches(omim_ids, self.BATCH_SIZE):
                ''' Build one placeholder per OMIM ID in this batch. '''
                placeholders = ','.join(['%s'] * len(batch))

                fetch_new_omim_entry_json_query = f'''
                    SELECT omim_id, entry_json
                    FROM publication_omim
                    WHERE is_new = 1
                    AND omim_id IN ({placeholders})
                '''

                fetch_cursor.execute(fetch_new_omim_entry_json_query, batch)
                rows.extend(fetch_cursor.fetchall())

            self.logger.info(f"Fetched {len(rows)} new OMIM entry_json rows from publication_omim.")
            return rows

        except Exception as e:
            self.logger.error(f"Error fetching new OMIM entry_json rows: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

        return []


    def _extract_pubmed_ids_from_entry_json(self, omim_id, entry_json):

        '''
        Extract PubMed IDs from one publication_omim.entry_json value.
        OMIM usually returns entryList and referenceList as lists, but the helper
        accepts dicts too so one oddly shaped response does not break the task.
        '''
        if not entry_json:
            return []

        try:
            omim_obj = json.loads(entry_json) if isinstance(entry_json, str) else entry_json
        except (json.JSONDecodeError, TypeError) as e:
            self.logger.error(f"Error parsing OMIM entry_json for omim_id={omim_id}: {e}")
            return []

        entry_list = (omim_obj.get('omim') or {}).get('entryList', [])

        if isinstance(entry_list, dict):
            entry_list = [entry_list]

        if not isinstance(entry_list, list):
            return []

        ''' Use a set so duplicate references inside the same OMIM entry collapse. '''
        pubmed_ids = set()

        for entry_item in entry_list:
            if not isinstance(entry_item, dict):
                continue

            entry = entry_item.get('entry') or {}
            reference_list = entry.get('referenceList', [])

            if isinstance(reference_list, dict):
                reference_list = [reference_list]

            if not isinstance(reference_list, list):
                continue

            for item in reference_list:
                if not isinstance(item, dict):
                    continue

                reference = item.get('reference') or {}
                pubmed_id = reference.get('pubmedID')

                if pubmed_id is None:
                    continue

                pubmed_id = str(pubmed_id).strip()

                ''' OMIM PubMed IDs should be numeric; skip non-PMID strings safely. '''
                if pubmed_id.isdigit():
                    pubmed_ids.add(pubmed_id)

        ''' Sort numerically for stable logs and deterministic processing order. '''
        return sorted(pubmed_ids, key=int)


    def _save_omim_pubmed_mappings(self, omim_pubmedids_dict):

        '''
        Save OMIM/PubMed provenance rows without creating duplicates.
        The table has no unique key in the schema, so use INSERT ... SELECT ...
        WHERE NOT EXISTS for idempotent reruns.
        '''
        if not omim_pubmedids_dict:
            return

        insert_sql = '''
            INSERT INTO publication_omim_pubmed_mapping (omim_id, pubmed_id)
            SELECT %s, %s
            WHERE NOT EXISTS (
                SELECT 1
                FROM publication_omim_pubmed_mapping
                WHERE omim_id = %s
                AND pubmed_id = %s
            )
        '''

        values = []

        ''' executemany needs both insert values and duplicate-check values. '''
        for omim_id, pubmed_id_list in omim_pubmedids_dict.items():
            for pubmed_id in pubmed_id_list:
                values.append((omim_id, pubmed_id, omim_id, pubmed_id))

        if not values:
            return

        cursor = None

        try:
            cursor = self.mysql.cursor()

            for batch in self._iter_batches(values, self.BATCH_SIZE):
                ''' Commit each batch so a long OMIM import does not hold one huge transaction. '''
                cursor.executemany(insert_sql, batch)
                self.mysql.commit()
                self.logger.info(f"Inserted {cursor.rowcount} OMIM/PubMed mappings.")

        except Exception as e:
            self.logger.error(f"Error inserting OMIM/PubMed mappings: {e}")
            self.mysql.rollback()

        finally:
            if cursor:
                cursor.close()


    def _retrieve_missing_publication_articles(self, pubmed_ids):

        '''
        Download article metadata only for PMIDs missing from publication_article.
        The inserted rows are marked is_new = 1 so downstream alert publication
        tasks can process only the current run's article set.
        '''
        pubmed_ids = sorted(
            {str(pubmed_id) for pubmed_id in pubmed_ids if str(pubmed_id).isdigit()},
            key=int
        )

        if not pubmed_ids:
            return

        ''' Avoid duplicate inserts into publication_article. '''
        existing_pubmed_ids = self._get_existing_pubmed_ids(pubmed_ids)

        missing_pubmed_ids = [
            pubmed_id
            for pubmed_id in pubmed_ids
            if pubmed_id not in existing_pubmed_ids
        ]

        self.logger.info(
            f"Found {len(pubmed_ids)} OMIM PubMed IDs; "
            f"{len(existing_pubmed_ids)} already exist and {len(missing_pubmed_ids)} need import."
        )

        if not missing_pubmed_ids:
            return

        insert_article_sql = '''
            INSERT INTO publication_article (
                pubmed_id, doi, title, abstract_text, affiliation,
                first_publication_date, publication_year, cited_by_count, is_open_access, in_EPMC,
                in_PMC, has_PDF, pub_type, source_json, is_new)
            SELECT %s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s, 1
            WHERE NOT EXISTS (
                SELECT 1
                FROM publication_article
                WHERE pubmed_id = %s
            )
        '''
        insert_cursor = None
        inserted_count = 0

        try:
            insert_cursor = self.mysql.cursor(buffered=True)

            for pubmed_id in missing_pubmed_ids:
                ''' Reuse the shared Europe PMC downloader used by publication discovery. '''
                article_val = self.publication_worker.download_by_pmid(pubmed_id)

                if not article_val:
                    self.logger.error(f"Unable to download OMIM PubMed article: pubmed_id={pubmed_id}")
                    continue

                insert_cursor.execute(insert_article_sql, (*article_val, pubmed_id))
                self.mysql.commit()

                inserted_count += 1
                self.logger.info(f"Inserted OMIM PubMed article into publication_article: pubmed_id={pubmed_id}")

            self.logger.info(f"Inserted {inserted_count} OMIM PubMed articles into publication_article.")

        except Exception as e:
            self.logger.error(f"Error importing OMIM PubMed articles: {e}")
            self.mysql.rollback()

        finally:
            if insert_cursor:
                insert_cursor.close()


    def _get_existing_pubmed_ids(self, pubmed_ids):

        '''
        Return PMIDs that already exist in publication_article. This prevents
        duplicate article downloads and duplicate publication_article rows.
        '''
        existing_pubmed_ids = set()
        cursor = None

        try:
            cursor = self.mysql.cursor()

            for batch in self._iter_batches(pubmed_ids, self.BATCH_SIZE):
                ''' Check each batch against publication_article. '''
                placeholders = ','.join(['%s'] * len(batch))
                query = f'''
                    SELECT pubmed_id
                    FROM publication_article
                    WHERE pubmed_id IN ({placeholders})
                '''

                cursor.execute(query, batch)
                existing_pubmed_ids.update(
                    str(row[0])
                    for row in cursor.fetchall()
                    if row[0] is not None
                )

        except Exception as e:
            self.logger.error(f"Error checking existing OMIM PubMed articles: {e}")

        finally:
            if cursor:
                cursor.close()

        return existing_pubmed_ids


    def _iter_batches(self, values, batch_size):

        ''' Yield fixed-size chunks for SQL IN lists and executemany calls. '''
        values = list(values)

        for start in range(0, len(values), batch_size):
            yield values[start:start + batch_size]




    def get_unique_new_omim_id_list(self):

        '''
        Find OMIM IDs introduced by the current alert run.
        The entry JSON is fetched separately from publication_omim so this method
        stays focused on the GARD-to-OMIM mapping source.
        '''
        fetch_new_omim_id_list_query = '''
            SELECT DISTINCT omim_id 
            FROM publication_gard_omim_mapping
            WHERE is_new = 1
            AND omim_id IS NOT NULL
        '''

        fetch_cursor = None

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(fetch_new_omim_id_list_query)
            rows = fetch_cursor.fetchall()

            return [row['omim_id'] for row in rows]
        
        except Exception as e:
            self.logger.error(f'{e}')
        finally:
            if fetch_cursor:
                fetch_cursor.close()

        return []

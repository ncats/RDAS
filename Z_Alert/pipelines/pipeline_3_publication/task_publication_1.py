import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterator, List, Sequence
from collections import OrderedDict
import requests
import time
import json
import re

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from dotenv import load_dotenv
load_dotenv()

from pipelines.pipeline_base import PipelineBase
from utils.publication_worker import PublicationWorker
from utils.tools import _resolve_worker_count

"""
Find and store newly published PubMed articles for updated GARD diseases.

For each GARD node, this pipeline searches PubMed with the node's filtered
disease names between the node's last update date and today. 

It saves each PubMed search response in publication_gard_pubmed, downloads
article metadata for PMIDs that are NOT already in publication_article,
stores those article rows in publication_article, and records the
GARD/search-term/PMID relationship in publication_gard_searchterm_pubmed_mapping.
"""
# Reference: C_publication/init_1_publication_gard_pubmed-id.py
# Reference: C_publication/init_2_1_publication-gard-searchterm-pubmed-mapping.py
# Reference: C_publication/init_3_1_publication-article-by-pubmed-id.py

# The pubmed_id in publication_article should be unique for alert imports.
'''
# check uniqueness of pubmed_id in publication_article

SELECT distinct pubmed_id, count(*) as ct
FROM rdas_db.publication_article
WHERE pubmed_id IS NOT NULL
GROUP BY pubmed_id
HAVING COUNT(*) > 1
limit 10;
'''

class NewPublicationDiscoveryTask(PipelineBase):

    PUBLICATION_INSERT_BATCH_SIZE = 100


    def __init__(self):

        super().__init__(init_mysql=True, init_memgraph=False)

        self.api_key = os.getenv("NCBI_KEY") 
        self.pubmed_esearch_api = os.getenv("PUBMED_ESEARCH_API")
        self.publication_worker = PublicationWorker()


    # Not implemented
    def process_new_data(self) -> None:
        raise NotImplementedError("NewPublicationDiscoveryTask does not implement process_new_data().")
   

    def find_new_data(self, gard_node) -> None:
        
        gard_id = gard_node['gardId']
        names = gard_node['filtered_names']
        last_update_date = gard_node.get("updated")
        today = date.today()

        for name in names:

            '''
            Process each complete date slice before fetching the next one, so
            a broad search never holds every matching PMID in memory at once.
            '''
            for pubmed_ids in self.retrieve_pubmed_ids(gard_id, name, last_update_date, today):
                if pubmed_ids:
                    self.find_new_publications(gard_id, name, pubmed_ids)
    


    # Step 1
    def retrieve_pubmed_ids(self, gard_id, search_term, mindate=None, maxdate=None) -> Iterator[List[str]]:

        """Yield complete PubMed ESearch ID lists, one date slice at a time.

        ``count`` is the number of matches in PubMed; ``idlist`` contains only
        the IDs returned by this request. A broad search can report many more
        matches than IDs because PubMed caps the returned list. For example,
        the GARD:0000016 search for ``coma`` from 1971-01-01 to 2026-10-19
        reported 41,918 matches but returned only 9,999 IDs. That response is
        incomplete and must not be saved as though it contained every PMID.

        Split incomplete searches by entry date until each slice has
        ``count == len(idlist)``. Save each complete response separately, then
        yield its IDs so the caller can import articles and mappings before
        the next slice is fetched. The returned iterator is consumed by
        ``find_new_data``; no large combined PMID list is built.
        """

        if not self.pubmed_esearch_api:
            raise RuntimeError("PUBMED_ESEARCH_API is not configured.")

        search_term = search_term.lower()  # Store the same lowercase term in every slice.
        '''
        Both runners supply full dates: the regular Alert runner uses MySQL
        dates and the GARD update runner uses an ISO date string. Converting
        their string forms to YYYY-MM-DD also discards any time component.
        A missing lower bound starts at 1970-01-01; a missing upper bound uses
        today. Reject a reversed range before calling PubMed.
        '''
        start_date = date.fromisoformat(str(mindate or date(1970, 1, 1))[:10])
        end_date = date.fromisoformat(str(maxdate or date.today())[:10])
        if start_date > end_date:
            raise ValueError(f"Invalid PubMed date range for {gard_id}: {start_date} > {end_date}.")

        '''
        Keep the existing exact Title/Abstract search. Requests encodes its
        spaces and punctuation as URL parameters, and EDAT makes the original
        Date - Entry filter explicit. The stack begins with the whole requested
        window; only oversized windows are added back as smaller slices.
        '''
        normalized_search_term = re.sub(r"\s+", " ", search_term)
        term_search_query = f'"{normalized_search_term}"[Title/Abstract:~0]'
        pending_ranges = [(start_date, end_date)]

        while pending_ranges:
            range_start, range_end = pending_ranges.pop()
            '''
            Request at most 9,999 IDs because PubMed capped the observed
            10,000-ID request at 9,999. The response still reports the full
            count, which lets us detect a truncated idlist. Include the API
            key only when configured, so an unset key is not sent as text.
            '''
            params = {
                "db": "pubmed",
                "term": term_search_query,
                "datetype": "edat",
                "mindate": range_start.isoformat(),
                "maxdate": range_end.isoformat(),
                "retmode": "json",
                "retmax": 9999,
            }
            if self.api_key:
                params["api_key"] = self.api_key

            '''
            Retry temporary connection failures and timeouts for this same
            slice. HTTP errors and malformed responses fail immediately.
            Never turn an error into an empty PMID list: that would allow the
            runner to advance the GARD update date despite missing results.
            '''
            for attempt in range(10):
                try:
                    response = requests.get(self.pubmed_esearch_api, params=params, timeout=30)
                    response.raise_for_status()
                    obj = response.json()
                    result = obj["esearchresult"]
                    count = int(result["count"])
                    retmax = int(result["retmax"])
                    retstart = int(result["retstart"])
                    pubmed_ids = result["idlist"]
                    '''
                    This task never requests a nonzero offset. A mismatched
                    offset or impossible count indicates an invalid response.
                    '''
                    if not isinstance(pubmed_ids, list) or retstart != 0 or count < len(pubmed_ids):
                        raise ValueError("PubMed ESearch returned an inconsistent ID list or offset.")
                    break
                except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
                    if attempt == 9:
                        self.logger.exception(
                            f"PubMed ESearch failed for {gard_id}: {search_term}, {range_start} to {range_end}."
                        )
                        raise
                    time.sleep(1)
                except (requests.exceptions.RequestException, KeyError, TypeError, ValueError):
                    self.logger.exception(
                        f"PubMed ESearch failed for {gard_id}: {search_term}, {range_start} to {range_end}."
                    )
                    raise

            '''
            Compare count with the actual number of IDs, not with retmax.
            For the 41,918-match coma example, 9,999 returned IDs cause this
            branch to divide the date range instead of saving a partial row.
            '''
            if count > len(pubmed_ids):
                if range_start == range_end:
                    '''
                    A single day cannot be split by date any further. Stop
                    explicitly so the missing IDs are visible and retryable.
                    '''
                    raise RuntimeError(
                        f"PubMed returned only {len(pubmed_ids)} of {count} PMIDs for "
                        f"{gard_id}: {search_term} on {range_start}; date splitting cannot recover the rest."
                    )

                '''
                The left slice ends on midpoint; the right starts the next
                day. Those inclusive windows cover the parent exactly once.
                Since this is a last-in-first-out stack, pushing the right
                slice first makes the earlier dates run first. Repeat the
                split if either child still exceeds PubMed's result limit.
                '''
                midpoint = range_start + (range_end - range_start) // 2
                self.logger.info(
                    f"Splitting PubMed search for {gard_id}: {search_term}, "
                    f"{range_start} to {range_end}; returned {len(pubmed_ids)} of {count} PMIDs."
                )
                pending_ranges.append((midpoint + timedelta(days=1), range_end))
                pending_ranges.append((range_start, midpoint))
                continue

            '''
            Only complete responses reach this point. Store the exact date
            slice in year_range so sibling slices have different keys and an
            older broad, truncated row does not block this backfill. Preserve
            the response JSON for later inspection and mapping repair.
            '''
            year_range = f"{range_start}-{range_end}"
            querytranslation = result.get("querytranslation", "")
            warnings = result.get("warninglist") or {}
            phrasesignored = warnings.get("phrasesignored") or []
            quotedphrasesnotfound = warnings.get("quotedphrasesnotfound") or []
            insert_sql = '''
                INSERT INTO publication_gard_pubmed (
                    gard_id, year_range, search_term, total, retmax, retstart,
                    pubmed_ids, query_translation, phrases_ignored,
                    quoted_phrases_not_found, ref_json
                )
                SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM publication_gard_pubmed
                    WHERE gard_id = %s AND year_range = %s AND search_term = %s
                )
            '''
            values = (
                gard_id, year_range, search_term, count, retmax, retstart,
                ",".join(pubmed_ids), querytranslation, ",".join(phrasesignored),
                ",".join(quotedphrasesnotfound), json.dumps(obj),
                gard_id, year_range, search_term,
            )
            cursor = self.mysql.cursor()
            try:
                '''
                Record even a complete zero-match response. The NOT EXISTS
                check avoids another raw row for the same GARD ID, term, and
                date slice when discovery is retried after a later failure.
                '''
                cursor.execute(insert_sql, values)
                inserted = cursor.rowcount
                self.mysql.commit()
            except Exception:
                self.mysql.rollback()
                self.logger.exception(
                    f"Unable to save publication_gard_pubmed for {gard_id}: {search_term}."
                )
                raise
            finally:
                cursor.close()

            if inserted:
                self.logger.info(
                    f"Saved publication_gard_pubmed for {gard_id}: {search_term}, "
                    f"year_range={year_range}, total={count}, returned={len(pubmed_ids)}."
                )

            '''
            Yield on reruns too: an existing raw row does not prove its
            article and GARD/term/PMID mapping imports finished earlier.
            '''
            yield pubmed_ids


    # Step 2
    def find_new_publications(self, gard_id, search_term, pubmed_ids):

        '''
        PubMed can return thousands of IDs for one complete date slice. Keep
        the input order while removing duplicate IDs, then limit each MySQL
        transaction and in-memory download set to a small chunk. A previous
        slice may already have inserted an Article or this mapping, so check
        both tables before downloading or preparing rows.
        '''
        unique_pubmed_ids = list(dict.fromkeys(str(pubmed_id) for pubmed_id in pubmed_ids if pubmed_id))
        if not unique_pubmed_ids:
            return

        worker_count = _resolve_worker_count(
            4, env_var_name="PUBLICATION_DOWNLOAD_MAX_WORKERS", logger=self.logger
        )
        check_cursor = self.mysql.cursor()
        article_cursor = self.mysql.cursor()
        mapping_cursor = self.mysql.cursor()
        new_articles_added = 0
        existing_articles_seen = 0
        mappings_added = 0
        failed_downloads = 0

        try:
            '''
            Europe PMC downloads spend most of their time waiting on HTTP, so
            a bounded thread pool overlaps those waits. Workers only return
            article tuples; all MySQL reads, writes, and commits stay in this
            parent thread. Reuse one pool across chunks in this date slice.
            '''
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                for start in range(0, len(unique_pubmed_ids), self.PUBLICATION_INSERT_BATCH_SIZE):
                    batch_ids = unique_pubmed_ids[start:start + self.PUBLICATION_INSERT_BATCH_SIZE]
                    placeholders = ",".join(["%s"] * len(batch_ids))

                    check_cursor.execute(
                        f"SELECT pubmed_id FROM publication_article WHERE pubmed_id IN ({placeholders})",
                        batch_ids,
                    )
                    existing_article_ids = {str(row[0]) for row in check_cursor.fetchall()}
                    check_cursor.execute(
                        f'''SELECT pubmed_id FROM publication_gard_searchterm_pubmed_mapping
                            WHERE gard_id = %s AND search_term = %s AND pubmed_id IN ({placeholders})''',
                        [gard_id, search_term, *batch_ids],
                    )
                    existing_mapping_ids = {str(row[0]) for row in check_cursor.fetchall()}
                    missing_article_ids = [pubmed_id for pubmed_id in batch_ids if pubmed_id not in existing_article_ids]
                    existing_articles_seen += len(existing_article_ids)

                    '''
                    End the read transaction before waiting for network calls.
                    The Article table's unique PMID key protects its insert
                    if another writer saves that PMID during the download.
                    Mapping rows have no unique key, so the mapping precheck
                    above skips tuples already present before this download.
                    '''
                    self.mysql.commit()

                    article_rows = []
                    eligible_mapping_ids = set(existing_article_ids)
                    for pubmed_id, article_val in zip(
                        missing_article_ids,
                        executor.map(self.publication_worker.download_by_pmid, missing_article_ids),
                    ):
                        if article_val is None:
                            failed_downloads += 1
                            self.logger.warning(
                                f"GARD ID: {gard_id}, Search term: {search_term} - Unable to download PubMed ID: {pubmed_id}."
                            )
                            continue
                        article_rows.append((*article_val, 1))
                        eligible_mapping_ids.add(pubmed_id)

                    '''
                    An Article may already belong to another GARD disease;
                    its new mapping must still be inserted. A failed download
                    of a missing Article does not create an orphan mapping.
                    '''
                    mapping_pubmed_ids = [
                        pubmed_id
                        for pubmed_id in batch_ids
                        if pubmed_id in eligible_mapping_ids and pubmed_id not in existing_mapping_ids
                    ]

                    try:
                        if article_rows:
                            article_placeholders = ",".join(["(" + ",".join(["%s"] * 15) + ")"] * len(article_rows))
                            article_cursor.execute(
                                f'''INSERT INTO publication_article (
                                    pubmed_id, doi, title, abstract_text, affiliation,
                                    first_publication_date, publication_year, cited_by_count,
                                    is_open_access, in_EPMC, in_PMC, has_PDF, pub_type,
                                    source_json, is_new
                                ) VALUES {article_placeholders}
                                ON DUPLICATE KEY UPDATE id = id''',
                                [value for row in article_rows for value in row],
                            )
                        inserted_articles = article_cursor.rowcount if article_rows else 0

                        if mapping_pubmed_ids:
                            '''
                            Recheck the mapping table in the bulk INSERT itself.
                            A download can take long enough for another task
                            to save a tuple after the earlier batch precheck.
                            This preserves the old per-row NOT EXISTS behavior
                            without requiring a unique mapping key. If the live
                            database also has that key, the no-op duplicate
                            handler covers a concurrent insert after this check.
                            '''
                            incoming_ids_sql = " UNION ALL ".join(
                                ["SELECT %s AS pubmed_id"] + ["SELECT %s"] * (len(mapping_pubmed_ids) - 1)
                            )
                            mapping_cursor.execute(
                                f'''INSERT INTO publication_gard_searchterm_pubmed_mapping
                                    (gard_id, search_term, pubmed_id)
                                    SELECT %s, %s, incoming.pubmed_id
                                    FROM ({incoming_ids_sql}) AS incoming
                                    WHERE NOT EXISTS (
                                        SELECT 1 FROM publication_gard_searchterm_pubmed_mapping AS existing
                                        WHERE existing.gard_id = %s
                                          AND existing.search_term = %s
                                          AND existing.pubmed_id = incoming.pubmed_id
                                    )
                                    ON DUPLICATE KEY UPDATE id = id''',
                                [gard_id, search_term, *mapping_pubmed_ids, gard_id, search_term],
                            )
                        inserted_mappings = mapping_cursor.rowcount if mapping_pubmed_ids else 0

                        '''
                        Article rows and their mappings commit together. If
                        either INSERT fails, only this chunk rolls back; a
                        rerun will still yield IDs from the saved raw search.
                        '''
                        self.mysql.commit()
                    except Exception:
                        self.mysql.rollback()
                        raise

                    new_articles_added += inserted_articles
                    mappings_added += inserted_mappings
                    self.logger.info(
                        f"Publication discovery batch for GARD ID: {gard_id}, Search term: {search_term}; "
                        f"ids={len(batch_ids)}, new_articles_added={inserted_articles}, "
                        f"mappings_added={inserted_mappings}."
                    )

            self.logger.info(
                f"Publication discovery summary for GARD ID: {gard_id}, Search term: {search_term}; "
                f"new_articles_added={new_articles_added}, existing_articles_seen={existing_articles_seen}, "
                f"mappings_added={mappings_added}, failed_downloads={failed_downloads}."
            )

        except Exception:
            self.mysql.rollback()
            self.logger.exception(f"Publication import failed for {gard_id}: {search_term}.")
            raise

        finally:
            check_cursor.close()
            article_cursor.close()
            mapping_cursor.close()

import json
import os
import sys
from typing import Any, Dict, Optional

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase

"""
Update Article nodes in Memgraph with extra attributes from publication_article.

For each new publication row (is_new = 1), parse source_json and update the matching Memgraph Article node with:
- fullTextUrls from source_json.fullTextUrlList.fullTextUrl[].url
- issue from source_json.journalInfo.issue
- volume from source_json.journalInfo.volume
"""

# Reference: C_publication/initializer/article_attrs.py


class NewPublicationArticleNodeAttrsUpdateTask(PipelineBase):
    """
    Update extra Article node attributes from staged publication metadata.

    Article nodes are created first with core fields. This task parses
    source_json from publication_article rows marked is_new = 1 and fills in
    attributes that come from nested publication metadata.
    """

    BATCH_SIZE = 200

    # This task updates existing Article nodes only; it does not create Article
    # nodes if the PubMed ID is missing from Memgraph.
    BATCH_UPDATE = '''
        UNWIND $chunks AS chunk
        MATCH (a:Article {pubmedId: chunk.pubmedId})
        SET a.fullTextUrls = chunk.fullTextUrls,
            a.issue = chunk.issue,
            a.volume = chunk.volume
    '''

    # source_json is required because full-text URLs and journal issue/volume
    # are nested inside the original publication payload.
    FETCH_NEW_ARTICLES_QUERY = '''
        SELECT
            pubmed_id,
            source_json
        FROM publication_article
        WHERE is_new = 1
    '''

    def __init__(self):
        """Initialize MySQL and Memgraph connections for Article attribute updates."""

        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        self.logger.info("PublicationArticleNodeAttributesUpdateTask does not use find_new_data().")


    # implement
    def process_new_data(self) -> None:
        """Fetch staged article JSON and update extra Article properties in batches."""

        fetch_cursor = None
        count = 0
        batch_num = 0

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(self.FETCH_NEW_ARTICLES_QUERY)

            while True:
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    self.logger.info("No more rows to fetch.")
                    break

                batch_num += 1
                self.logger.info(f"--- batch# = {batch_num} ---")

                chunks = []

                for row in rows:
                    # Convert one staged article row into the compact update
                    # payload expected by BATCH_UPDATE.
                    article_attrs = self._create_article_attrs(row)

                    if article_attrs is None:
                        continue

                    chunks.append(article_attrs)

                if not chunks:
                    self.logger.info("No valid Article extra attributes to update in Memgraph.")
                    continue

                try:
                    self.memgraph.execute(self.BATCH_UPDATE, {"chunks": chunks})

                    count += len(chunks)
                    total_urls = sum(len(item["fullTextUrls"]) for item in chunks)
                    self.logger.info(
                        f"Updated {len(chunks)} Article extra-attribute chunks in Memgraph. "
                        f"#fullTextUrls = {total_urls}. Total = {count}"
                    )

                except Exception as e:
                    self.logger.error(f"Error executing Article extra-attribute batch update: {e}")

        except Exception as e:
            self.logger.error(f"Error updating Article extra attributes in Memgraph: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def _create_article_attrs(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse source_json and return extra Article attributes for one PMID."""

        try:
            pubmed_id = int(row["pubmed_id"])
        except (TypeError, ValueError) as e:
            self.logger.error(f"Invalid pubmed_id found: {row.get('pubmed_id')}. Error: {e}")
            return None

        try:
            source_obj = json.loads(row.get("source_json") or "{}")
        except (json.JSONDecodeError, TypeError) as e:
            self.logger.error(f"Error parsing source_json for pubmed_id={pubmed_id}: {e}")
            return None

        full_text_url_list = source_obj.get("fullTextUrlList") or {}
        journal_info = source_obj.get("journalInfo") or {}

        # Skip rows that do not contain any of the nested fields this task owns.
        if not full_text_url_list and not journal_info:
            return None

        try:
            full_text_urls = full_text_url_list.get("fullTextUrl", []) or []
            urls = [item.get("url") for item in full_text_urls if item.get("url")]

            return {
                "pubmedId": pubmed_id,
                "fullTextUrls": urls,
                "issue": journal_info.get("issue", ""),
                "volume": journal_info.get("volume", ""),
            }

        except Exception as e:
            self.logger.error(f"Error building Article extra attributes for pubmed_id={pubmed_id}: {e}")
            return None

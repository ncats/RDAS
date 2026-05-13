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
from utils.tools import _normalize_keywords

"""
Create Keyword nodes for new publication articles.

For each new row in publication_article (is_new = 1), parse
source_json.keywordList.keyword, create Keyword nodes, and link the matching
Article node to each Keyword with has_keyword.
"""

# Reference: C_publication/initializer/keyword.py


class NewPublicationKeywordGraphTask(PipelineBase):
    """
    Create Keyword nodes and Article/Keyword relationships for new articles.

    Keyword data comes from nested source_json metadata. This task normalizes
    those keyword values and links them to the Article node identified by PubMed ID.
    """

    BATCH_SIZE = 200

    # Keyword nodes are reused by keyword text, so multiple articles can point
    # to the same normalized Keyword node.
    BATCH_CREATE = '''
        UNWIND $chunks AS chunk
        MATCH (a:Article {pubmedId: chunk.pubmedId})
        WITH a, chunk
        UNWIND chunk.keywords AS kw
        MERGE (k:Keyword {keyword: kw})
        MERGE (a)-[:has_keyword]->(k)
    '''

    # source_json is required because keywordList is nested in the publication
    # source payload.
    FETCH_NEW_ARTICLES_QUERY = '''
        SELECT
            pubmed_id, source_json
        FROM publication_article
        WHERE is_new = 1
    '''

    def __init__(self):
        """Initialize MySQL and Memgraph connections for keyword graph loading."""

        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        self.logger.info("NewPublicationKeywordGraphTask does not use find_new_data().")


    # implement
    def process_new_data(self) -> None:
        """Fetch staged article JSON and write Keyword mappings in batches."""

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
                    # Build one article-to-keywords chunk from the nested source_json keyword payload.
                    keyword_chunk = self._create_keyword_chunk(row)

                    if keyword_chunk is None:
                        continue

                    chunks.append(keyword_chunk)

                if not chunks:
                    self.logger.info("No valid Keyword mappings to insert into Memgraph.")
                    continue

                try:
                    self.memgraph.execute(self.BATCH_CREATE, {"chunks": chunks})

                    count += len(chunks)
                    keyword_count = sum(len(item["keywords"]) for item in chunks)
                    self.logger.info(
                        f"Submitted {len(chunks)} Article keyword chunks to Memgraph. "
                        f"#keywords = {keyword_count}. Total = {count}"
                    )

                except Exception as e:
                    self.logger.error(f"Error executing Keyword batch create: {e}")

        except Exception as e:
            self.logger.error(f"Error creating Keyword nodes in Memgraph: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def _create_keyword_chunk(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse and normalize keywords for one staged article row."""

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

        keyword_list = source_obj.get("keywordList") or {}

        if not keyword_list:
            return None

        try:
            # _normalize_keywords handles source payload variations such as a
            # single keyword string versus a list of keyword values.
            keywords = _normalize_keywords(keyword_list.get("keyword", []))
        except Exception as e:
            self.logger.error(f"Error processing keywords for pubmed_id={pubmed_id}: {e}")
            return None

        if not keywords:
            return None

        return {
            "pubmedId": pubmed_id,
            "keywords": keywords,
        }

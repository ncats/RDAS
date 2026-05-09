import os
import sys
from typing import Any, Dict, Optional

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _as_bool, _empty_if_none

"""
Insert new Article nodes into Memgraph from update_publication_article.
"""

# Reference: C_publication/initializer/article.py


class NewPublicationArticleGraphTask(PipelineBase):
    """
    Create Article nodes in Memgraph for newly staged publications.

    update_publication_article contains the current alert run's article rows.
    This task converts those rows into Article node properties and creates nodes
    keyed by PubMed ID.
    """

    BATCH_SIZE = 300

    '''
    Create Article nodes only when the PubMed ID does not already exist.
    Existing Article nodes are left unchanged.
    '''
    BATCH_CREATE = '''
        UNWIND $chunks AS chunk
        MERGE (a:Article {pubmedId: chunk.pubmedId})
        ON CREATE SET
            a.doi = chunk.doi,
            a.title = chunk.title,
            a.abstractText = chunk.abstractText,
            a.firstPublicationDate = chunk.firstPublicationDate,
            a.publicationYear = chunk.publicationYear,
            a.citationCount = chunk.citationCount,
            a.isOpenAccess = chunk.isOpenAccess,
            a.inEPMC = chunk.inEPMC,
            a.inPMC = chunk.inPMC,
            a.isEpidemiologicalStudy = chunk.isEpidemiologicalStudy,
            a.isNaturalHistoryStudy = chunk.isNaturalHistoryStudy,
            a.hasPDF = chunk.hasPDF,
            a.pubType = chunk.pubType,
            a.dateCreatedByRDAS = chunk.dateCreatedByRDAS,
            a.lastUpdatedDateByRDAS = chunk.lastUpdatedDateByRDAS,
            a.fullTextUrls = chunk.fullTextUrls,
            a.issue = chunk.issue,
            a.volume = chunk.volume,
            a.isGeneReview = false
    '''

    # Load only current-run article rows that can be keyed in Memgraph by PubMed ID.
    FETCH_NEW_ARTICLES_QUERY = '''
        SELECT 
            pubmed_id, doi, title, abstract_text, first_publication_date,
            publication_year, cited_by_count,
            is_open_access, in_EPMC, in_PMC, is_EPI, is_NHS,
            has_PDF, pub_type
        FROM update_publication_article
        WHERE is_new = 1
        AND pubmed_id IS NOT NULL
    '''

    def __init__(self):
        """Initialize MySQL and Memgraph connections for Article node loading."""

        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        raise NotImplementedError("NewPublicationArticleGraphTask does not implement find_new_data().")


    # implement
    def process_new_data(self) -> None:
        """Fetch staged publication rows and write Article nodes in batches."""

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
                    # Convert MySQL column names/types into the Article node
                    # property names expected by the graph schema.
                    article_node = self._create_article_node(row)

                    if article_node is None:
                        continue

                    chunks.append(article_node)

                if not chunks:
                    self.logger.info("No valid Article nodes to insert into Memgraph.")
                    continue

                try:
                    self.memgraph.execute(self.BATCH_CREATE, {"chunks": chunks})

                    count += len(chunks)
                    self.logger.info(f"Submitted {len(chunks)} Article nodes to Memgraph. Total = {count}")

                except Exception as e:
                    self.logger.error(f"Error executing Article node batch create: {e}")

        except Exception as e:
            self.logger.error(f"Error creating Article nodes in Memgraph: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def _create_article_node(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Build one Article node property dictionary from a database row."""

        try:
            pubmed_id = int(row["pubmed_id"])
        except (TypeError, ValueError) as e:
            self.logger.error(f"Invalid pubmed_id found: {row.get('pubmed_id')}. Error: {e}")
            return None

        return {
            "pubmedId": pubmed_id,
            "doi": _empty_if_none(row.get("doi")),
            "title": _empty_if_none(row.get("title")),
            "abstractText": _empty_if_none(row.get("abstract_text")),
            "firstPublicationDate": _empty_if_none(row.get("first_publication_date")),
            "publicationYear": row.get("publication_year"),
            "citationCount": row.get("cited_by_count"),
            "isOpenAccess": _as_bool(row.get("is_open_access")),
            "inEPMC": _as_bool(row.get("in_EPMC")),
            "inPMC": _as_bool(row.get("in_PMC")),
            "isEpidemiologicalStudy": _as_bool(row.get("is_EPI")),
            "isNaturalHistoryStudy": _as_bool(row.get("is_NHS")),
            "hasPDF": _as_bool(row.get("has_PDF")),
            "pubType": _empty_if_none(row.get("pub_type")),
            "dateCreatedByRDAS": self.formatted_today,
            "lastUpdatedDateByRDAS": self.formatted_today,

            # These are populated by the article attributes workflow later.
            "fullTextUrls": [],
            "issue": "",
            "volume": "",
        }

import json
import os
import sys
from typing import Any, Dict, List, Optional

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _as_list, _make_hash_key, _to_string

"""
Create EpidemiologyAnnotation nodes for new publication articles.

For each new row in update_publication_article (is_new = 1), read epi_extract,
create a unique EpidemiologyAnnotation node, and link it to the matching Article
node with has_epidemiological_annotation.
"""

# Reference: C_publication/initializer/epidemiology.py


class NewPublicationEpidemiologyGraphTask(PipelineBase):
    """
    Load extracted epidemiology facts from staged articles into Memgraph.

    EPI extraction produces JSON fields such as rates, locations, and dates.
    This task turns those fields into EpidemiologyAnnotation nodes and links
    them to the Article nodes they came from.
    """

    BATCH_SIZE = 200

    # EpidemiologyAnnotation nodes are keyed by a hash of the extracted fields
    # so the same annotation payload can be reused across reruns.
    BATCH_CREATE = '''
        UNWIND $chunks AS chunk
        MATCH (a:Article {pubmedId: chunk.pubmedId})
        MERGE (n:EpidemiologyAnnotation {_composite_key: chunk._composite_key})
        ON CREATE SET
            n.epidemiologyType = chunk.epidemiologyType,
            n.epidemiologyRate = chunk.epidemiologyRate,
            n.studyDate = chunk.date,
            n.studyLocation = chunk.location,
            n.ethnicity = chunk.ethnicity,
            n.sex = chunk.sex,
            n.dateCreatedByRDAS = chunk.dateCreatedByRDAS,
            n.lastUpdatedByRDAS = chunk.lastUpdatedByRDAS
        MERGE (a)-[:has_epidemiological_annotation {epidemiology_probability: chunk.epiProbability}]->(n)
    '''

    # is_EPI has appeared as both numeric and string/boolean-like values, so the
    # filter accepts the known truthy forms.
    FETCH_NEW_EPI_ARTICLES_QUERY = '''
        SELECT
            pubmed_id,
            epi_probability,
            epi_extract
        FROM update_publication_article
        WHERE is_new = 1
        AND pubmed_id IS NOT NULL
        AND epi_extract IS NOT NULL
        AND (
            is_EPI = 1
            OR is_EPI = '1'
            OR LOWER(is_EPI) = 'true'
            OR LOWER(is_EPI) = 'yes'
            OR LOWER(is_EPI) = 'y'
        )
    '''

    def __init__(self):
        """Initialize MySQL and Memgraph connections for epidemiology graph loading."""

        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        self.logger.info("NewPublicationEpidemiologyGraphTask does not use find_new_data().")


    # implement
    def process_new_data(self) -> None:
        """Fetch staged EPI rows and write EpidemiologyAnnotation mappings."""

        fetch_cursor = None
        count = 0
        batch_num = 0

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(self.FETCH_NEW_EPI_ARTICLES_QUERY)

            while True:
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    self.logger.info("No more rows to fetch.")
                    break

                batch_num += 1
                self.logger.info(f"--- batch# = {batch_num} ---")

                chunks = []

                for row in rows:
                    # Convert one staged article's epi_extract JSON into the
                    # chunk shape expected by BATCH_CREATE.
                    epi_node = self._create_epidemiology_annotation(row)

                    if epi_node is None:
                        continue

                    chunks.append(epi_node)

                if not chunks:
                    self.logger.info("No valid EpidemiologyAnnotation nodes to insert into Memgraph.")
                    continue

                try:
                    self.memgraph.execute(self.BATCH_CREATE, {"chunks": chunks})

                    count += len(chunks)
                    self.logger.info(
                        f"Submitted {len(chunks)} EpidemiologyAnnotation mappings to Memgraph. Total = {count}"
                    )

                except Exception as e:
                    self.logger.error(f"Error executing EpidemiologyAnnotation batch create: {e}")

        except Exception as e:
            self.logger.error(f"Error creating EpidemiologyAnnotation nodes in Memgraph: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def _create_epidemiology_annotation(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse one epi_extract payload into graph-ready annotation properties."""

        try:
            pubmed_id = int(row["pubmed_id"])
        except (TypeError, ValueError) as e:
            self.logger.error(f"Invalid pubmed_id found: {row.get('pubmed_id')}. Error: {e}")
            return None

        epi_extract = row.get("epi_extract")
        if not epi_extract:
            return None

        try:
            epi_obj = json.loads(epi_extract)
        except (json.JSONDecodeError, TypeError) as e:
            self.logger.error(f"Error parsing epi_extract for pubmed_id={pubmed_id}: {e}")
            return None

        # Each EPI field is normalized to a list so Cypher receives stable
        # property types regardless of whether extraction returned one or many values.
        epidemiology_type = _as_list(epi_obj.get("EPI"))
        epidemiology_rate = _as_list(epi_obj.get("STAT"))
        study_date = _as_list(epi_obj.get("DATE"))
        study_location = _as_list(epi_obj.get("LOC"))
        ethnicity = _as_list(epi_obj.get("ETHN"))
        sex = _as_list(epi_obj.get("SEX"))

        composite_key = self._make_composite_key(
            epidemiology_type,
            epidemiology_rate,
            study_date,
            study_location,
            ethnicity,
            sex,
        )

        return {
            "pubmedId": pubmed_id,
            "epiProbability": _to_string(row.get("epi_probability")),
            "epidemiologyType": epidemiology_type,
            "epidemiologyRate": epidemiology_rate,
            "date": study_date,
            "location": study_location,
            "ethnicity": ethnicity,
            "sex": sex,
            "_composite_key": composite_key,
            "dateCreatedByRDAS": self.formatted_today,
            "lastUpdatedByRDAS": self.formatted_today,
        }


    def _make_composite_key(self, *values: List[str]) -> str:
        """Build a stable hash key from all extracted epidemiology fields."""

        composite_key_str = "_".join(
            "_".join(sorted(value_list))
            for value_list in values
        )
        composite_key_str = "_".join(composite_key_str.split())

        return _make_hash_key(composite_key_str)

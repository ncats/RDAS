import json
import os
import sys
from typing import Any, Dict, List

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _make_hash_key, _to_string

"""
Create PubtatorAnnotation nodes for newly staged publication articles.

Task publication_5 retrieves and parses PubTator data into
publication_pubtator_parsed. This graph task reads those parsed annotations
only for rows whose PubMed IDs are in update_publication_article with is_new = 1,
then creates:

    (Article)-[:has_pubtator_annotation]->(PubtatorAnnotation)

Duplicate PubTator rows are merged by PubMed ID, annotation identifier,
annotation type, and relation type, matching the initializer behavior.
"""
# Reference: alert/pipelines/pipeline_3/task_publication_5.py
# Reference: C_publication/initializer/pubtator.py


class PublicationGraphTask_9(PipelineBase):

    BATCH_SIZE = 1000

    BATCH_CREATE = '''
        WITH $chunks AS chunks
        WHERE chunks IS NOT NULL AND size(chunks) > 0

        UNWIND chunks AS chunk
        MATCH (a:Article {pubmedId: chunk.pubmedId})

        WITH a, chunk
        WHERE chunk.pubtators IS NOT NULL AND size(chunk.pubtators) > 0

        UNWIND chunk.pubtators AS pt

        MERGE (p:PubtatorAnnotation {_composite_key: pt._composite_key})
        ON CREATE SET
            p.annotation = pt.annotation,
            p.annotationType = pt.annotationType,
            p.annotationIdentifier = pt.annotationIdentifier,
            p.dateCreatedByRDAS = pt.dateCreatedByRDAS,
            p.lastUpdatedByRDAS = pt.lastUpdatedByRDAS

        MERGE (a)-[:has_pubtator_annotation {type: pt.relation_type}]->(p)
    '''

    FETCH_NEW_PUBTATOR_QUERY = '''
        SELECT DISTINCT
            ppp.id,
            ppp.pubmed_id,
            ppp.infons_identifier,
            ppp.infons_type,
            ppp.infons_text,
            ppp.relation_type
        FROM publication_pubtator_parsed AS ppp
        INNER JOIN update_publication_article AS upa
            ON upa.pubmed_id = ppp.pubmed_id
        WHERE upa.is_new = 1
        AND ppp.pubmed_id IS NOT NULL
        ORDER BY ppp.pubmed_id, ppp.id
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        self.logger.info("PublicationGraphTask_9 does not use find_new_data().")


    # implement
    def process_new_data(self) -> None:

        fetch_cursor = None
        count = 0
        batch_num = 0
        carryover_rows = []

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(self.FETCH_NEW_PUBTATOR_QUERY)

            while True:
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    self.logger.info("No more rows to fetch.")
                    break

                batch_num += 1
                self.logger.info(f"--- batch# = {batch_num} ---")

                rows = carryover_rows + rows
                last_pubmed_id = rows[-1].get("pubmed_id")
                complete_rows = [
                    row for row in rows
                    if row.get("pubmed_id") != last_pubmed_id
                ]
                carryover_rows = [
                    row for row in rows
                    if row.get("pubmed_id") == last_pubmed_id
                ]

                chunks = self._create_pubtator_chunks(complete_rows)

                if complete_rows and not chunks:
                    self.logger.info("No valid PubtatorAnnotation chunks to insert into Memgraph.")
                    continue

                count = self._submit_chunks(chunks, count)

            if carryover_rows:
                chunks = self._create_pubtator_chunks(carryover_rows)
                count = self._submit_chunks(chunks, count)

        except Exception as e:
            self.logger.error(f"Error creating PubtatorAnnotation nodes in Memgraph: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def _submit_chunks(self, chunks: List[Dict[str, Any]], count: int) -> int:

        if not chunks:
            return count

        try:
            #self.memgraph.execute(self.BATCH_CREATE, {"chunks": chunks})

            count += len(chunks)
            annotation_count = sum(len(chunk["pubtators"]) for chunk in chunks)
            self.logger.info(
                f"Submitted {len(chunks)} Article PubTator chunks to Memgraph. "
                f"#annotations = {annotation_count}. Total = {count}"
            )

        except Exception as e:
            self.logger.error(f"Error executing PubtatorAnnotation batch create: {e}")

        return count


    def _create_pubtator_chunks(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:

        pubmed_id_ann_dict = {}

        for row in rows:
            try:
                pubmed_id = int(row["pubmed_id"])
            except (TypeError, ValueError) as e:
                self.logger.error(f"Invalid pubmed_id found: {row.get('pubmed_id')}. Error: {e}")
                continue

            pubmed_id_ann_dict[pubmed_id] = pubmed_id_ann_dict.get(pubmed_id, [])

            infons_identifier = row.get("infons_identifier")
            infons_identifier = "" if (not infons_identifier or infons_identifier == "-") else str(infons_identifier)

            pubmed_id_ann_dict[pubmed_id].append({
                "annotation": _to_string(row.get("infons_text")),
                "annotationType": _to_string(row.get("infons_type")),
                "relation_type": self._parse_relation_type(row.get("relation_type"), pubmed_id),
                "annotationIdentifier": infons_identifier,
            })

        merged_annotations = self.merge_annotations(pubmed_id_ann_dict)

        return [
            {"pubmedId": pubmed_id, "pubtators": pubtators}
            for pubmed_id, pubtators in merged_annotations.items()
            if pubtators
        ]


    def merge_annotations(self, val_dict: Dict[int, List[Dict[str, Any]]]) -> Dict[int, List[Dict[str, Any]]]:

        result_dict = {}

        for pubmed_id, items in val_dict.items():
            temp_obj = {}

            for item in items:
                relation_type_tuple = tuple(sorted(item["relation_type"]))
                compose_key = (
                    item["annotationIdentifier"],
                    item["annotationType"],
                    relation_type_tuple,
                )

                if compose_key not in temp_obj:
                    temp_obj[compose_key] = set()

                temp_obj[compose_key].add(item["annotation"])

            result_dict[pubmed_id] = []

            for compose_key, annotations in temp_obj.items():
                annotation_identifier, annotation_type, relation_type_tuple = compose_key

                composite_key_str = (
                    f"{annotation_identifier}_"
                    f"{'_'.join(sorted(annotations))}_"
                    f"{'_'.join(relation_type_tuple)}"
                )
                composite_key_str = "_".join(composite_key_str.split())

                result_dict[pubmed_id].append({
                    "annotationIdentifier": annotation_identifier,
                    "annotation": list(annotations),
                    "annotationType": annotation_type,
                    "relation_type": list(relation_type_tuple),
                    "_composite_key": _make_hash_key(composite_key_str),
                    "lastUpdatedByRDAS": self.formatted_today,
                    "dateCreatedByRDAS": self.formatted_today,
                })

        return result_dict


    def _parse_relation_type(self, value: Any, pubmed_id: int) -> List[str]:

        if value is None or value == "":
            return []

        try:
            relation_type = json.loads(value)
        except (json.JSONDecodeError, TypeError) as e:
            self.logger.error(f"Error parsing relation_type for pubmed_id={pubmed_id}: {e}")
            return []

        if relation_type is None:
            return []

        if not isinstance(relation_type, list):
            relation_type = [relation_type]

        return sorted({
            str(item)
            for item in relation_type
            if item is not None and str(item).strip()
        })

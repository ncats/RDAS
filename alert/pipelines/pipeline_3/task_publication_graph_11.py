import json
import os
import re
import sys
from typing import Any, Dict, List

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _make_hash_key

'''
Create OMIMRef nodes and Article/OMIMRef mappings for newly retrieved OMIM entries.

This task reads publication_omim rows where is_new = 1. For each OMIM entry_json,
it finds which OMIM text sections cite each reference number, matches those
references to PubMed IDs, creates consolidated OMIMRef nodes, and links matching
Article nodes with:

    (Article)-[:has_omim_ref]->(OMIMRef)
'''

#Reference: C_publication/initializer/omimref.py


class NewPublicationOmimRefGraphTask(PipelineBase):
    """Create OMIMRef graph records from newly retrieved OMIM entry JSON."""

    BATCH_SIZE = 20

    '''
    Create one OMIMRef node per unique OMIM ID and section-list combination.
    A single PubMed article can be cited by multiple sections, so sections are
    consolidated before graph creation and included in the composite key.
    '''
    '''
    Example chunks produced by this alert task.
    The original initializer uses target_refs/formatted_today; this task keeps
    the same payload shape with targetRefs/formattedToday for the Cypher query.

    [
        {
            "pubmedId": 10071185,
            "targetRefs": [
                {
                    "omimId": 274270,
                    "omimName": "DIHYDROPYRIMIDINE DEHYDROGENASE DEFICIENCY; DPYDD",
                    "omimSections": [
                        "Description",
                        "Biochemical Features",
                        "Molecular Genetics"
                    ],
                    "_composite_key": "generated-by-_make_hash_key"
                }
            ],
            "formattedToday": "2025-12-18"
        },
        {
            "pubmedId": 19296131,
            "targetRefs": [
                {
                    "omimId": 274270,
                    "omimName": "DIHYDROPYRIMIDINE DEHYDROGENASE DEFICIENCY; DPYDD",
                    "omimSections": [
                        "Molecular Genetics"
                    ],
                    "_composite_key": "generated-by-_make_hash_key"
                }
            ],
            "formattedToday": "2025-12-18"
        }
    ]
    '''
    BATCH_CREATE = '''
        UNWIND $chunks AS chunk
        MATCH (a:Article {pubmedId: chunk.pubmedId})
        UNWIND chunk.targetRefs AS ref
        MERGE (o:OMIMRef {_composite_key: ref._composite_key})
        ON CREATE SET
            o.omimId = ref.omimId,
            o.omimName = ref.omimName,
            o.omimSections = ref.omimSections,
            o.dateCreatedByRDAS = chunk.formattedToday,
            o.lastUpdatedByRDAS = chunk.formattedToday
        MERGE (a)-[:has_omim_ref]->(o)
    '''

    '''
    Only OMIM entries retrieved during the current alert run should be processed.
    '''
    FETCH_NEW_OMIM_QUERY = '''
        SELECT id, omim_id, entry_json
        FROM publication_omim
        WHERE is_new = 1
        AND omim_id IS NOT NULL
        AND entry_json IS NOT NULL
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("NewPublicationOmimRefGraphTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Parse new OMIM entries, map PubMed references, and submit graph chunks."""

        fetch_cursor = None
        count = 0
        batch_num = 0

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(self.FETCH_NEW_OMIM_QUERY)

            while True:
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    self.logger.info("No more rows to fetch.")
                    break

                batch_num += 1
                self.logger.info(f"--- batch# = {batch_num} ---")

                chunks = []

                for row in rows:
                    # Convert the raw OMIM JSON into normalized text sections
                    # and reference rows before building Article graph chunks.
                    result_obj = self.parse_entry_json(
                        row.get("id"),
                        row.get("omim_id"),
                        row.get("entry_json")
                    )

                    pubmed_omimref_chunks = self.get_pubmed_id_and_omimrefs_mapping_list(result_obj)

                    if pubmed_omimref_chunks:
                        chunks.extend(pubmed_omimref_chunks)

                if not chunks:
                    self.logger.info("No valid OMIMRef mappings to insert into Memgraph.")
                    continue

                try:
                    
                    #self.memgraph.execute(self.BATCH_CREATE, {"chunks": chunks})

                    count += len(chunks)
                    ref_count = sum(len(chunk["targetRefs"]) for chunk in chunks)

                    self.logger.info(
                        f"Submitted {len(chunks)} Article OMIMRef chunks to Memgraph. "
                        f"#OMIMRefs = {ref_count}. Total = {count}"
                    )

                except Exception as e:
                    self.logger.error(f"Error executing OMIMRef batch create: {e}")

        except Exception as e:
            self.logger.error(f"Error creating OMIMRef nodes in Memgraph: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def get_pubmed_id_and_omimrefs_mapping_list(self, result_obj: Dict[str, Any]) -> List[Dict[str, Any]]:

        '''
        Build the Article-to-OMIMRef chunks expected by BATCH_CREATE.
        Each OMIM text section stores the reference numbers it cites. Each
        reference stores the PubMed ID. This method joins those two structures
        through referenceNumber.
        '''
        pubmed_omimrefs_mapping_list = []
        text_section_obj_list = result_obj.get("textSectionList", [])
        reference_obj_list = result_obj.get("referenceList", [])

        omimref_obj_list = []

        # First collect every OMIM section and the reference numbers cited in
        # that section. The reference numbers are matched to PubMed IDs below.
        for obj in text_section_obj_list:
            omimref_obj_list.append({
                "omimId": result_obj.get("omim_id", ""),
                "omimName": result_obj.get("title", ""),
                "omimSection": obj.get("title", ""),
                "refNums": obj.get("refNums", [])
            })

        for obj in reference_obj_list:
            ref_num = obj.get("referenceNumber")

            if ref_num is None:
                continue

            pubmed_id = obj.get("pubmedID")

            if not pubmed_id:
                continue

            target_refs = self.get_omimrefs_by_ref_num(omimref_obj_list, ref_num)

            if not target_refs:
                continue

            # Each output chunk represents one Article and all OMIMRef nodes
            # that should be linked to it.
            pubmed_omimrefs_mapping_list.append({
                "pubmedId": self._to_int(pubmed_id),
                "targetRefs": self.transform_target_refs(target_refs),
                "formattedToday": self.formatted_today
            })

        return [
            chunk
            for chunk in pubmed_omimrefs_mapping_list
            if chunk["pubmedId"] is not None and chunk["targetRefs"]
        ]


    def get_omimrefs_by_ref_num(self, omimref_obj_list: List[Dict[str, Any]], ref_num: Any) -> List[Dict[str, Any]]:

        '''
        Return all OMIM section objects that cite the given reference number.
        Reference numbers are compared as strings because OMIM text citations
        are parsed from strings such as {8:Gerber et al., 2016}.
        '''
        target_refs = []

        for obj in omimref_obj_list:
            if str(ref_num) in obj.get("refNums", []):
                target_refs.append(obj)

        return target_refs


    def transform_target_refs(self, target_refs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:

        '''
        Consolidate multiple section-level references into one OMIMRef object per
        OMIM ID. The sorted omimSections list is part of the composite key so
        different section combinations remain distinct nodes.

        Example input target_refs:
        {
            "pubmedId": 10071185,
            "targetRefs": [
                {
                    "omimId": 274270,
                    "omimName": "DIHYDROPYRIMIDINE DEHYDROGENASE DEFICIENCY; DPYDD",
                    "omimSection": "Description",
                    "refNums": ["18", "7"]
                },
                {
                    "omimId": 274270,
                    "omimName": "DIHYDROPYRIMIDINE DEHYDROGENASE DEFICIENCY; DPYDD",
                    "omimSection": "Biochemical Features",
                    "refNums": ["2", "20", "18", "4"]
                },
                {
                    "omimId": 274270,
                    "omimName": "DIHYDROPYRIMIDINE DEHYDROGENASE DEFICIENCY; DPYDD",
                    "omimSection": "Molecular Genetics",
                    "refNums": ["20", "17", "16", "10", "18"]
                }
            ],
            "formattedToday": "2025-12-18"
        }

        Example output targetRefs:
        {
            "pubmedId": 10071185,
            "targetRefs": [
                {
                    "omimId": 274270,
                    "omimName": "DIHYDROPYRIMIDINE DEHYDROGENASE DEFICIENCY; DPYDD",
                    "omimSections": [
                        "Description",
                        "Biochemical Features",
                        "Molecular Genetics"
                    ],
                    "_composite_key": "generated-by-_make_hash_key"
                }
            ],
            "formattedToday": "2025-12-18"
        }
        '''
        omim_dict = {}

        for ref in target_refs:
            omim_id = ref.get("omimId")
            omim_name = ref.get("omimName", "")
            omim_section = ref.get("omimSection", "")

            if not omim_id:
                continue

            if omim_id not in omim_dict:
                omim_dict[omim_id] = {
                    "omimId": omim_id,
                    "omimName": omim_name,
                    "omimSections": []
                }

            if omim_section and omim_section not in omim_dict[omim_id]["omimSections"]:
                omim_dict[omim_id]["omimSections"].append(omim_section)

        # The section list participates in the key, so the same OMIM entry can
        # produce distinct OMIMRef nodes for different cited-section sets.
        for omim_id in omim_dict:
            omim_dict[omim_id]["omimSections"].sort()

            composite_key_str = f"{omim_id}{'-'.join(omim_dict[omim_id]['omimSections'])}"
            composite_key_str = "".join(composite_key_str.lower().split())

            omim_dict[omim_id]["_composite_key"] = _make_hash_key(composite_key_str)

        return list(omim_dict.values())


    def parse_entry_json(self, row_id: Any, omim_id: Any, entry_json: Any) -> Dict[str, Any]:

        '''
        Parse the publication_omim.entry_json structure.
        textSectionContent contains OMIM reference markers such as
        {8:Gerber et al., 2016}. The numeric part is later matched to
        referenceList[].reference.referenceNumber.

        Example publication_omim.entry_json:
        {
            "omim": {
                "version": "1.0",
                "entryList": [
                {
                    "entry": {
                    "prefix": "#",
                    "mimNumber": 200110,
                    "status": "live",
                    "titles": {
                        "preferredTitle": "ABLEPHARON-MACROSTOMIA SYNDROME; AMS"
                    },
                    "textSectionList": [
                        {
                        "textSection": {
                            "textSectionName": "text",
                            "textSectionTitle": "Text",
                            "textSectionContent": "A number sign (#) is used with this entry because of evidence that ablepharon-macrostomia syndrome (AMS) is caused by heterozygous mutation in the TWIST2 gene ({607556}) on chromosome 2q37."
                        }
                        },
                        {
                        "textSection": {
                            "textSectionName": "description",
                            "textSectionTitle": "Description",
                            "textSectionContent": "Ablepharon-macrostomia syndrome (AMS) is a congenital ectodermal dysplasia characterized by absent eyelids, macrostomia, microtia, redundant skin, sparse hair, dysmorphic nose and ears, variable abnormalities of the nipples, genitalia, fingers, and hands, largely normal intellectual and motor development, and poor growth (summary by {7:Marchegiani et al., 2015})."
                        }
                        },
                        {
                        "textSection": {
                            "textSectionName": "inheritance",
                            "textSectionTitle": "Inheritance",
                            "textSectionContent": "{13:Rohena et al. (2011)} reported AMS in a newborn female and her 22-year-old father and suggested autosomal dominant inheritance of the disorder. {7:Marchegiani et al. (2015)} confirmed autosomal dominant inheritance of the AMS in this family.\n\nPossible autosomal recessive inheritance was proposed because of a postulated relationship to the disorder in monozygotic twins from a consanguineous marriage: one twin had bilateral cryptophthalmos and the other had cryptophthalmos on the left and ablepharon on the right ({1:Azevedo et al., 1973})."
                        }
                        },
                        {
                        "textSection": {
                            "textSectionName": "cytogenetics",
                            "textSectionTitle": "Cytogenetics",
                            "textSectionContent": "{11:Pellegrino et al. (1996)} described a male infant with ablepharon-macrostomia syndrome and a complex rearrangement and partial deletion of chromosome 18; the final karyotype, based on molecular cytogenetic analysis, was 46,XY,-18,+[del(18)(q21.3q23),inv(18)(q12.3q21.2)]. The authors stated that this was the first AMS patient to be reported with an abnormal karyotype, and that he lacked the typical features of the 18q deletion syndrome ({601808}). {11:Pellegrino et al. (1996)} suggested that the gene(s) for ablepharon-macrostomia syndrome might lie on chromosome 18 in the region of this patient's deletion or inversion breakpoints."
                        }
                        }
                    ],
                    "referenceList": [
                        {
                        "reference": {
                            "mimNumber": 200110,
                            "referenceNumber": 1,
                            "authors": "Azevedo, E. S., Biondi, J., Ramalho, L. M.",
                            "title": "Cryptophthalmos in two families from Bahia, Brazil.",
                            "source": "J. Med. Genet. 10: 389-392, 1973.",
                            "pubmedID": 4774831,
                            "doi": "10.1136/jmg.10.4.389"
                        }
                        },
                        {
                        "reference": {
                            "mimNumber": 200110,
                            "referenceNumber": 2,
                            "authors": "Brancati, F., Mingarelli, R., Sarkozy, A., Dallapiccola, B.",
                            "title": "Ablepharon-macrostomia syndrome in a 46-year-old woman.",
                            "source": "Am. J. Med. Genet. 127A: 96-98, 2004.",
                            "pubmedID": 15103726,
                            "doi": "10.1002/ajmg.a.20658"
                        }
                        },
                        {
                        "reference": {
                            "mimNumber": 200110,
                            "referenceNumber": 5,
                            "authors": "Hornblass, A., Reifler, D. M.",
                            "title": "Ablepharon macrostomia syndrome.",
                            "source": "Am. J. Ophthal. 99: 552-556, 1985.",
                            "pubmedID": 4003491,
                            "doi": "10.1016/s0002-9394(14)77956-5"
                        }
                        },
                        {
                        "reference": {
                            "mimNumber": 200110,
                            "referenceNumber": 10,
                            "authors": "McCarthy, G. T., West, C. M.",
                            "title": "Ablepheron (sic) macrostomia syndrome.",
                            "source": "Dev. Med. Child Neurol. 19: 659-672, 1977.",
                            "pubmedID": 913905,
                            "doi": "10.1111/j.1469-8749.1977.tb07999.x"
                        }
                        },
                        {
                        "reference": {
                            "mimNumber": 200110,
                            "referenceNumber": 11,
                            "authors": "Pellegrino, J. E., Schnur, R. E., Boghosian-Sell, L., Strathdee, G., Overhauser, J., Spinner, N. B., Stump, T., Grace, K., Zackai, E. H.",
                            "title": "Ablepharon macrostomia syndrome with associated cutis laxa: possible localization to 18q.",
                            "source": "Hum. Genet. 97: 532-536, 1996.",
                            "pubmedID": 8834257,
                            "doi": "10.1007/BF02267081"
                        }
                        },
                        {
                        "reference": {
                            "mimNumber": 200110,
                            "referenceNumber": 14,
                            "authors": "Stevens, C. A., Sargent, L. A.",
                            "title": "Ablepharon-macrostomia syndrome.",
                            "source": "Am. J. Med. Genet. 107: 30-37, 2002.",
                            "pubmedID": 11807864,
                            "doi": "10.1002/ajmg.10123"
                        }
                        }
                    ]
                    }
                }
                ]
            }
        }
        '''
        result_obj = {
            "id": row_id,
            "omim_id": omim_id,
            "title": "",
            "textSectionList": [],
            "referenceList": []
        }

        if not entry_json:
            return result_obj

        try:
            omim_obj = json.loads(entry_json) if isinstance(entry_json, str) else entry_json
        except (json.JSONDecodeError, TypeError) as e:
            self.logger.error(f"Error parsing OMIM entry_json for id={row_id}, omim_id={omim_id}: {e}")
            return result_obj

        entry_list = (omim_obj.get("omim") or {}).get("entryList", [])

        if isinstance(entry_list, dict):
            entry_list = [entry_list]

        if not isinstance(entry_list, list) or not entry_list:
            return result_obj

        # OMIM responses are expected to contain one entry for this task; use
        # the first entry and normalize the pieces needed by later methods.
        first_entry_item = entry_list[0] if isinstance(entry_list[0], dict) else {}
        entry = (first_entry_item.get("entry") or {}) if isinstance(first_entry_item, dict) else {}

        titles = entry.get("titles") or {}
        result_obj["title"] = titles.get("preferredTitle", "")

        result_obj["textSectionList"] = self.extract_text_section_refs(entry)
        result_obj["referenceList"] = self.extract_reference_list(entry)

        return result_obj


    def extract_text_section_refs(self, entry: Dict[str, Any]) -> List[Dict[str, Any]]:

        '''
        Extract OMIM text section titles and cited reference numbers.
        The regex keeps only the numeric reference number before the colon.
        '''
        pattern = r"{(\d+):"
        text_section_list = entry.get("textSectionList", [])

        if isinstance(text_section_list, dict):
            text_section_list = [text_section_list]

        if not isinstance(text_section_list, list):
            return []

        sections = []

        for item in text_section_list:
            if not isinstance(item, dict):
                continue

            text_section = item.get("textSection") or {}
            text_section_content = text_section.get("textSectionContent", "")
            # Reference markers look like {8:Gerber et al., 2016}; keep only
            # the numeric reference number so it can join to referenceList.
            ref_nums = re.findall(pattern, text_section_content)

            sections.append({
                "title": text_section.get("textSectionTitle", ""),
                "refNums": sorted(set(ref_nums), key=int)
            })

        return sections


    def extract_reference_list(self, entry: Dict[str, Any]) -> List[Dict[str, Any]]:

        '''
        Flatten entry.referenceList[].reference into a simple list of reference
        dictionaries. Missing or malformed reference items are skipped.
        '''
        reference_list = entry.get("referenceList", [])

        if isinstance(reference_list, dict):
            reference_list = [reference_list]

        if not isinstance(reference_list, list):
            return []

        references = []

        for item in reference_list:
            if not isinstance(item, dict):
                continue

            reference = item.get("reference")

            if isinstance(reference, dict):
                references.append(reference)

        return references


    def _to_int(self, value: Any):
        """Convert PubMed IDs from OMIM references to integers."""

        try:
            return int(value)
        except (TypeError, ValueError) as e:
            self.logger.error(f"Invalid pubmed_id found: {value}. Error: {e}")
            return None

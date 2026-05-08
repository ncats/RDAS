import os
import re
import sys
from collections import defaultdict
from typing import Any, Dict, List, Optional

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _clean, _make_hash_key, _remove_parentheses

"""
Create Agent nodes and relationships for newly staged people.

It only reads person_of_all_sources rows where is_new = 1, groups them by
rdas_group_id, creates/updates Agent nodes, and creates relationships from
ClinicalTrial, Project, and Article nodes to those Agent nodes.
"""

#F_person/initializer/agent.py


class NewPersonAgentGraphTask(PipelineBase):

    BATCH_SIZE = 500
    PERSON_TABLE = "person_of_all_sources"
    PUBLICATION = "Publication"
    GRANT_PROJECT = "GrantProject"
    CLINICAL_TRIAL = "ClinicalTrial"

    BATCH_CREATE = f'''
        UNWIND $chunks AS chunk

        MERGE (a: Agent {{_idx_key: chunk._idx_key}})
        ON CREATE SET
            a.fullName = chunk.fullName,
            a.firstName = chunk.firstName,
            a.lastName = chunk.lastName,
            a.orc_id = chunk.orc_id,
            a.pi_id = chunk.pi_id,
            a.contactEmail = chunk.contactEmail,
            a.dateCreatedByRDAS = chunk.formattedToday
        SET
            a.lastUpdatedByRDAS = chunk.formattedToday

        WITH a, chunk
        UNWIND chunk.relations AS relation

        CALL {{
            WITH a, relation
            WHERE relation.source = '{CLINICAL_TRIAL}' AND relation.relationType = 'has_investigator'
            MERGE (ct: ClinicalTrial {{nctId: relation.nctId}})
            MERGE (ct)-[:has_investigator]->(a)
        }}

        CALL {{
            WITH a, relation
            WHERE relation.source = '{CLINICAL_TRIAL}' AND relation.relationType = 'has_contact'
            MERGE (ct: ClinicalTrial {{nctId: relation.nctId}})
            MERGE (ct)-[:has_contact]->(a)
        }}

        CALL {{
            WITH a, relation
            WHERE relation.source = '{GRANT_PROJECT}' AND relation.relationType = 'has_investigator'
            MERGE (p: Project {{applicationId: relation.applicationId}})
            MERGE (p)-[:has_investigator]->(a)
        }}

        CALL {{
            WITH a, relation
            WHERE relation.source = '{GRANT_PROJECT}' AND relation.relationType = 'has_contact'
            MERGE (p: Project {{applicationId: relation.applicationId}})
            MERGE (p)-[:has_contact]->(a)
        }}

        CALL {{
            WITH a, relation
            WHERE relation.source = '{PUBLICATION}'
            MERGE (t: Article {{pubmedId: relation.pubmedId}})
            MERGE (t)-[:has_author]->(a)
        }}

        WITH a, chunk
        UNWIND chunk.organizations AS org
        MERGE (o: Organization {{_idx_key: org._idx_key}})
        ON CREATE SET
            o.name = org.name
        MERGE (a)-[:has_affiliation]->(o)
    '''

    FETCH_NEW_PERSON_QUERY = f'''
        SELECT
            id,
            associate_id,
            associate_type,
            source,
            first_name,
            last_name,
            affiliation,
            orcid,
            email,
            rdas_group_id,
            PI_id,
            role
        FROM {PERSON_TABLE}
        WHERE is_new = 1
        AND rdas_group_id IS NOT NULL
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("NewPersonAgentGraphTask does not use find_new_data().")


    def process_new_data(self) -> None:

        fetch_cursor = None
        total_agents = 0
        total_people = 0
        batch_num = 0

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(self.FETCH_NEW_PERSON_QUERY)

            while True:
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    self.logger.info("No more new person rows to fetch.")
                    break

                batch_num += 1
                chunks = self.create_agent_chunks(rows)

                if not chunks:
                    self.logger.info(f"Batch #{batch_num}: no valid Agent chunks to insert into Memgraph.")
                    continue

                try:
                    self.memgraph.execute(self.BATCH_CREATE, {"chunks": chunks})

                    total_agents += len(chunks)
                    total_people += len(rows)

                    self.logger.info(
                        f"Batch #{batch_num}: submitted {len(chunks)} Agent chunks "
                        f"from {len(rows)} person rows. Total Agents={total_agents}; "
                        f"total person rows read={total_people}."
                    )

                except Exception as e:
                    self.logger.error(f"Error executing Agent graph batch #{batch_num}: {e}")

            self.logger.info(
                f"Completed NewPersonAgentGraphTask. Total Agents={total_agents}; "
                f"total person rows read={total_people}."
            )

        except Exception as e:
            self.logger.error(f"NewPersonAgentGraphTask failed: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def create_agent_chunks(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:

        grouped_by_rdas_group_id = defaultdict(list)

        for row in rows:
            rdas_group_id = row.get("rdas_group_id")

            if not rdas_group_id:
                continue

            grouped_by_rdas_group_id[rdas_group_id].append(row)

        chunks = []

        for rdas_group_id, person_list in grouped_by_rdas_group_id.items():
            chunk = self.create_agent_chunk(rdas_group_id, person_list)

            if chunk:
                chunks.append(chunk)

        return chunks


    def create_agent_chunk(self, rdas_group_id: Any, person_list: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:

        relations = []
        relation_keys = set()
        email_set = set()
        affiliation_set = set()
        first_name = ""
        last_name = ""
        full_name = ""
        orc_id = ""
        pi_id = ""

        for person in person_list:
            original_first_name = person.get("first_name")
            original_last_name = person.get("last_name")

            if not original_first_name or not original_last_name:
                continue

            normalized_last_name = self.normalize_last_name(original_last_name)

            if not normalized_last_name:
                self.logger.info(f"Skipping invalid last_name={original_last_name}")
                continue

            first_name = str(original_first_name).strip().title()
            last_name = normalized_last_name.strip().title()
            full_name = f"{first_name} {last_name}"
            orc_id = _clean(person.get("orcid"))
            pi_id = _clean(person.get("PI_id"))

            relation = self.create_relation(person)

            if relation:
                relation_key = tuple(sorted(relation.items()))

                if relation_key not in relation_keys:
                    relation_keys.add(relation_key)
                    relations.append(relation)

            email = _clean(person.get("email"))

            if email:
                email_set.add(email)

            affiliation = _clean(person.get("affiliation"))

            if affiliation:
                affiliation_set.add(affiliation)

        if not full_name:
            return None

        organizations = [
            {
                "name": org,
                "_idx_key": _make_hash_key(_remove_parentheses(org))
            }
            for org in sorted(affiliation_set)
            if org and org.strip()
        ]

        return {
            "_idx_key": _make_hash_key(rdas_group_id),
            "fullName": full_name,
            "firstName": first_name,
            "lastName": last_name,
            "orc_id": orc_id,
            "pi_id": pi_id,
            "contactEmail": sorted(email_set),
            "organizations": organizations,
            "relations": relations,
            "formattedToday": self.formatted_today
        }


    def create_relation(self, person: Dict[str, Any]) -> Optional[Dict[str, Any]]:

        associate_id = _clean(person.get("associate_id"))
        associate_type = _clean(person.get("associate_type"))
        source = _clean(person.get("source"))
        role = _clean(person.get("role"))

        if not associate_id or not source:
            return None

        if source == self.CLINICAL_TRIAL:
            relation_type = "has_investigator" if associate_type == "PI" else "has_contact"

            return {
                "nctId": associate_id,
                "relationType": relation_type,
                "source": self.CLINICAL_TRIAL
            }

        if source == self.GRANT_PROJECT:
            relation_type = "has_contact" if role == "contact" else "has_investigator"

            return {
                "applicationId": associate_id,
                "relationType": relation_type,
                "source": self.GRANT_PROJECT
            }

        if source == self.PUBLICATION:
            pubmed_id = self._to_int(associate_id)

            if pubmed_id is None:
                return None

            return {
                "pubmedId": pubmed_id,
                "relationType": "has_author",
                "source": self.PUBLICATION
            }

        return None


    def normalize_last_name(self, last_name: Any) -> Optional[str]:

        '''
        Normalize last names using the same rules as PersonWorker.normalize_last_name().
        '''
        if not last_name or not isinstance(last_name, str):
            return None

        last_name = last_name.strip()

        if not last_name:
            return None

        if re.match(r"^#", last_name):
            return None

        if re.match(r"^[\(\)]|[\)']$", last_name):
            return None

        if re.match(r"^(-|\.|\.Null)$", last_name):
            return None

        if re.match(r"^\d", last_name):
            return None

        if re.match(r"^[?<>@\[\]{}]", last_name):
            return None

        match = re.match(r"^'([ntsNTS])\s+(.+)$", last_name)
        if match:
            prefix = match.group(1).lower()
            remainder = match.group(2).strip()
            return f"'{prefix} {remainder}" if remainder else None

        match = re.match(r"^'([ntsNTS])([A-Za-z-].*)$", last_name)
        if match:
            prefix = match.group(1).lower()
            remainder = match.group(2).strip()
            return f"'{prefix} {remainder}" if remainder else None

        last_name = re.sub(r"^'", "", last_name)
        last_name = re.sub(r"^-", "", last_name)
        last_name = last_name.strip()

        return last_name or None


    def _to_int(self, value: Any) -> Optional[int]:

        try:
            return int(value)
        except (TypeError, ValueError):
            self.logger.error(f"Invalid pubmed_id found for Agent relationship: {value}")
            return None

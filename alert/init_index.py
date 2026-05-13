import os
import sys
from ast import literal_eval
from typing import List, Tuple

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(_dir),
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase


"""
Create Memgraph indexes used by RDAS graph loaders.

The configs below are based on the current Memgraph indexes exported with
SHOW INDEX INFO, plus the missing lookup indexes needed by incremental alert
tasks and initializer Cypher.

Keep the indexed properties from initializer create_indexes(...) and
_create_index(...) calls, such as F_person/initializer/agent.py:

    self.create_indexes('Agent', ['firstName', 'lastName', '_idx_key'])

Do not add indexes for nodes that are only CREATE'd without a persisted lookup
property, such as StudyDesign, IndividualPatientData, and PrimaryOutcome in
the bulk clinical-trial initializers.
"""

LABEL_INDEX_CONFIG = [
    "Article",
    "CoreProject",
    "Disease",
]

INDEX_CONFIG = [
    # GARD/Disease base graph
    {"Disease": ["gardId", "gardName", "synonyms", "countArticles", "countGenes", "countPhenotypes", "countProjects", "countTrials"]},
    {"GARD": ["gardId"]},
    {"Phenotype": ["hpoId"]},
    {"Gene": ["geneIdentifier"]},

    # Clinical trial graph
    {"Annotation": ["umlsCui"]},
    {"ClinicalTrial": ["nctId", "NCTId"]},
    {"Condition": ["condition"]},
    {"Drug": ["rxnormID"]},
    {"Intervention": ["interventionName", "interventionType", "_composite_key", "_intervention_name_key"]},
    {"Participant": ["nctId"]},
    {"PrimaryOutcome": ["_composite_key"]},

    # Publication graph
    {"Article": ["pubmedId", "publicationYear", "title", "isEpidemiologicalStudy", "isNaturalHistoryStudy"]},
    {"EpidemiologyAnnotation": ["epidemiologyType", "studyLocation", "ethnicity", "_composite_key"]},
    {"Journal": ["issn", "essn", "nlmid", "title"]},
    {"Keyword": ["keyword"]},
    {"MeshTerm": ["meshTerm"]},
    {"OMIMRef": ["omimId", "omimSections", "_composite_key"]},
    {"PubtatorAnnotation": ["annotationIdentifier", "annotationType", "annotation", "_composite_key"]},
    {"Substance": ["registryNumber", "name", "_composite_key"]},

    # Grant graph
    {"ClinicalStudy": ["nctId"]},
    {"CoreProject": ["coreProjectNumber", "applicationId"]},
    {"Patent": ["patentId"]},
    {"Project": ["applicationId", "application_id", "fundingYear"]},

    # Person
    {"Agent": ["firstName", "lastName", "_idx_key", "name"]},

    # Fllow up
    {"Location": ["facility", "_idx_key"]},
    {"Organization": ["ror_id", "name", "_idx_key"]},
]

TEXT_INDEX_CONFIG = [
    {
        "name": "name_and_synonyms",
        "label": "Disease",
        "properties": ["gardName", "synonyms"],
    },
]


def iter_label_index_config():
    """Yield label-only indexes."""

    for label in LABEL_INDEX_CONFIG:
        yield label


def iter_index_config():
    """Yield one node label and its configured label-property indexes."""

    for item in INDEX_CONFIG:
        for node_name, properties in item.items():
            yield node_name, properties


def iter_text_index_config():
    """Yield configured text indexes."""

    for item in TEXT_INDEX_CONFIG:
        yield item["name"], item["label"], item["properties"]


class MemgraphIndexInitializationTask(PipelineBase):
    """Create all configured Memgraph indexes."""

    def __init__(self):
        super().__init__(init_mysql=False, init_memgraph=True)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("MemgraphIndexInitializationTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Create/check every configured Memgraph index."""

        created_total = 0
        skipped_total = 0
        error_total = 0

        try:
            self.logger.info("Starting Memgraph index initialization.")

            for label in iter_label_index_config():
                self.logger.info(f"Creating/checking label index for {label}")

                if self._is_label_index_exists(label):
                    skipped_total += 1
                    self.logger.info(f"Label index already exists: :{label}")
                elif self._create_label_index(label):
                    created_total += 1
                else:
                    error_total += 1

            for node_name, properties in iter_index_config():
                self.logger.info(f"Creating/checking indexes for {node_name}: {properties}")

                created, skipped, errors = self.create_indexes(node_name, properties)

                created_total += created
                skipped_total += skipped
                error_total += errors

            for name, label, properties in iter_text_index_config():
                self.logger.info(f"Creating/checking text index {name} for {label}: {properties}")

                if self._is_text_index_exists(name, label, properties):
                    skipped_total += 1
                    self.logger.info(f"Text index already exists: {name} ON :{label}({', '.join(properties)})")
                elif self._create_text_index(name, label, properties):
                    created_total += 1
                else:
                    error_total += 1

            self.logger.info(
                "Finished Memgraph index initialization. "
                f"created={created_total}, already_exists={skipped_total}, errors={error_total}"
            )

        except Exception as e:
            self.logger.error(f"Unexpected error while creating Memgraph indexes: {e}")

        finally:
            self.close()


    def create_indexes(self, label: str, fields: List[str]) -> Tuple[int, int, int]:
        """Create indexes for one node label, matching InitBase.create_indexes style."""

        created = 0
        skipped = 0
        errors = 0

        for field in fields:
            if self._is_index_field_exists(label, field):

                skipped += 1
                self.logger.info(f"Index already exists: :{label}({field})")
                continue

            if self._create_index(label, field):
                created += 1
            else:
                errors += 1

        return created, skipped, errors


    def _create_label_index(self, label: str) -> bool:
        """Create one Memgraph label-only index."""

        command = f"CREATE INDEX ON :{label};"

        try:
            self.memgraph.execute(command)
            self.logger.info(f"Created label index: {command}")
            return True

        except Exception as e:
            self.logger.error(f"Error creating label index {command}: {e}")
            return False


    def _create_index(self, label: str, field: str) -> bool:
        """Create one Memgraph label-property index."""

        command = f"CREATE INDEX ON :{label}({field});"

        try:
            self.memgraph.execute(command)
            self.logger.info(f"Created index: {command}")
            return True

        except Exception as e:
            self.logger.error(f"Error creating index {command}: {e}")
            return False


    def _create_text_index(self, name: str, label: str, fields: List[str]) -> bool:
        """Create one Memgraph text index."""

        properties = ", ".join(fields)
        command = f"CREATE TEXT INDEX {name} ON :{label}({properties});"

        try:
            self.memgraph.execute(command)
            self.logger.info(f"Created text index: {command}")
            return True

        except Exception as e:
            self.logger.error(f"Error creating text index {command}: {e}")
            return False


    @staticmethod
    def _property_list(value) -> List[str]:
        """Normalize SHOW INDEX INFO property values across Memgraph versions."""

        if value is None or value == "":
            return []

        if isinstance(value, list):
            return value

        if isinstance(value, tuple):
            return list(value)

        if isinstance(value, str):
            text = value.strip()

            if not text:
                return []

            if text.startswith("[") and text.endswith("]"):
                try:
                    parsed = literal_eval(text)
                    return parsed if isinstance(parsed, list) else [text]
                except (SyntaxError, ValueError):
                    return [text]

            return [text]

        return [str(value)]


    @staticmethod
    def _index_type(row) -> str:
        """Return the Memgraph index type column regardless of driver naming."""

        return str(row.get("index type") or row.get("type") or row.get("index_type") or "")


    def _is_label_index_exists(self, label_name: str) -> bool:
        """Return True if Memgraph already has the label-only index."""

        rows = self.memgraph.execute_and_fetch("SHOW INDEX INFO")

        for row in rows:
            if row.get("label") != label_name:
                continue

            index_type = self._index_type(row)
            properties = self._property_list(row.get("property"))

            if index_type == "label" or not properties:
                return True

        return False


    def _is_index_field_exists(self, label_name: str, field: str) -> bool:
        """Return True if Memgraph already has the label-property index."""
        rows = self.memgraph.execute_and_fetch("SHOW INDEX INFO")

        for row in rows:
            if row.get("label") != label_name:
                continue

            index_type = self._index_type(row)
            properties = self._property_list(row.get("property"))

            if index_type and index_type != "label+property":
                continue

            if properties == [field]:
                return True

        return False


    def _is_text_index_exists(self, name: str, label_name: str, fields: List[str]) -> bool:
        """Return True if Memgraph already has the named label text index."""

        rows = self.memgraph.execute_and_fetch("SHOW INDEX INFO")

        for row in rows:
            if row.get("label") != label_name:
                continue

            index_type = self._index_type(row)
            properties = self._property_list(row.get("property"))

            if "label_text" not in index_type:
                continue

            if name in index_type or properties == fields:
                return True

        return False


def main() -> None:
    task = MemgraphIndexInitializationTask()
    task.process_new_data()


if __name__ == "__main__":
    main()

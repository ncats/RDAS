import os
import sys

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(_dir),
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_0_setup.memgraph_index_utils import create_indexes, create_label_index, create_text_index, is_label_index_exists, is_text_index_exists
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

                if is_label_index_exists(self.memgraph, label):
                    skipped_total += 1
                    self.logger.info(f"Label index already exists: :{label}")
                elif create_label_index(self.memgraph, self.logger, label):
                    created_total += 1
                else:
                    error_total += 1

            for node_name, properties in iter_index_config():
                self.logger.info(f"Creating/checking indexes for {node_name}: {properties}")

                created, skipped, errors = create_indexes(self.memgraph, self.logger, node_name, properties)

                created_total += created
                skipped_total += skipped
                error_total += errors

            for name, label, properties in iter_text_index_config():
                self.logger.info(f"Creating/checking text index {name} for {label}: {properties}")

                if is_text_index_exists(self.memgraph, name, label, properties):
                    skipped_total += 1
                    self.logger.info(f"Text index already exists: {name} ON :{label}({', '.join(properties)})")
                elif create_text_index(self.memgraph, self.logger, name, label, properties):
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

def main() -> None:

    task = MemgraphIndexInitializationTask()
    task.process_new_data()


if __name__ == "__main__":
    main()

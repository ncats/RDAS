"""
Generate UMLS annotations for NIH RePORTER project abstracts.

Original setup notes kept here because this script depends on both sciSpaCy
models and a project-level work table.

Installation check and model loading:
    pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.3/en_core_sci_lg-0.5.3.tar.gz
    pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.3/en_ner_bionlp13cg_md-0.5.3.tar.gz
    pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.3/en_ner_bc5cdr_md-0.5.3.tar.gz

Step one:
    SELECT CONCAT(GROUP_CONCAT('p.', COLUMN_NAME ORDER BY ORDINAL_POSITION SEPARATOR ', ')) AS columns
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'rdas_db'
      AND TABLE_NAME = 'grant_project_annotation';

    CREATE TABLE rdas_db.grant_gard_project_relation_unique_application_id (
        id SERIAL PRIMARY KEY,
        application_id INT UNIQUE
    );

    INSERT INTO rdas_db.grant_gard_project_relation_unique_application_id (application_id)
    SELECT DISTINCT application_id
    FROM rdas_db.grant_gard_project_relation
    ORDER BY application_id;

    The current script also expects this processing flag on the work table:
        ALTER TABLE rdas_db.grant_gard_project_relation_unique_application_id
        ADD COLUMN project_annotation_processed TINYINT NULL;

Step three: create a table grant_project_annotation_unique, identified by CONCEPT_ID.
    CREATE INDEX idx_grant_project_annotation_concept_id
    ON rdas_db.grant_project_annotation (concept_id);

    CREATE TABLE rdas_db.grant_project_annotation_unique AS
        WITH RankedRows AS (
            SELECT
                concept_id,
                umls_concept,
                umls_cui,
                semantic_types,
                semantic_type_names,
                aliases,
                definition,
                ROW_NUMBER() OVER (PARTITION BY concept_id ORDER BY concept_id) AS rn
            FROM rdas_db.grant_project_annotation
        )
        SELECT
            concept_id,
            umls_concept,
            umls_cui,
            semantic_types,
            semantic_type_names,
            aliases,
            definition
        FROM RankedRows
        WHERE rn = 1;

TODO: Filter out rows/concept_id by semantic_type_names after confirming the
      desired semantic type rules with Qian Zhu.
"""

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import mysql.connector

# Add the project root to the Python path so this file can be run directly:
# python D_grant/init_11_Project_annotation_generator.py
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.append(str(PROJECT_ROOT))

from baseclass.conn import DBConnection as db
from utils.tools import _id_range_generator, _normalize_txt, _val, ask_to_continue


MODEL_NAMES = (
    "en_ner_bionlp13cg_md",
    "en_ner_bc5cdr_md",
)

# Keep the ID range small because each row can contain a long grant abstract and
# each abstract is processed by two sciSpaCy models. Increase this only after
# checking memory use and MySQL transaction time on the target machine.
DEFAULT_ID_STEP = 1
DEFAULT_RANGE_BATCH_SIZE = 10

# spaCy batches text documents inside each ID range. A conservative batch keeps
# memory stable for long abstracts while still avoiding one-document-at-a-time
# overhead.
DEFAULT_NLP_BATCH_SIZE = 8

PROCESSED_FLAG = 1

# Do not disable tok2vec here. The NER components in these sciSpaCy models depend
# on their upstream vector component. The helper below disables only components
# that are present and not needed for entity recognition/linking.
PIPE_COMPONENTS_TO_DISABLE = (
    "tagger",
    "parser",
    "attribute_ruler",
    "lemmatizer",
)

ANNOTATION_INSERT_QUERY = """
    INSERT INTO grant_project_annotation (
        application_id,
        concept_id,
        score,
        umls_concept,
        umls_cui,
        semantic_types,
        semantic_type_names,
        aliases,
        definition
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

ABSTRACT_QUERY = """
    SELECT
        gpru.application_id,
        ga.abstract_text
    FROM grant_gard_project_relation_unique_application_id AS gpru
    INNER JOIN grant_abstract AS ga
        ON ga.APPLICATION_ID = gpru.application_id
    WHERE gpru.id BETWEEN %s AND %s
      AND gpru.project_annotation_processed IS NULL
      AND ga.abstract_text IS NOT NULL
"""

PENDING_BOUNDS_QUERY = """
    SELECT
        MIN(id) AS min_id,
        MAX(id) AS max_id
    FROM grant_gard_project_relation_unique_application_id
    WHERE project_annotation_processed IS NULL
"""

MARK_RANGE_PROCESSED_QUERY = """
    UPDATE grant_gard_project_relation_unique_application_id
    SET project_annotation_processed = %s
    WHERE id BETWEEN %s AND %s
      AND project_annotation_processed IS NULL
"""


@dataclass(frozen=True)
class ModelResource:
    name: str
    nlp: Any
    linker: Any
    semantic_type_tree: Any


def load_models() -> Tuple[ModelResource, ...]:
    """Load sciSpaCy NER models and attach the UMLS linker to each pipeline."""
    try:
        import spacy
        import scispacy  # noqa: F401 - importing registers sciSpaCy components.
        from scispacy.linking import EntityLinker  # noqa: F401 - registers scispacy_linker.

    except ImportError as exc:
        raise RuntimeError(
            "Unable to import spaCy/scispaCy. Install scispaCy and the model "
            "packages listed in the module comments."
        ) from exc

    models: List[ModelResource] = []

    for model_name in MODEL_NAMES:
        try:
            nlp = spacy.load(model_name)

        except OSError as exc:
            raise RuntimeError(
                f"Error loading sciSpaCy model {model_name!r}. Install the model "
                "with the pip commands listed in the module comments."
            ) from exc

        # Disable pipeline components that are not needed for annotation. This
        # keeps NER and UMLS linking intact while avoiding unnecessary work.
        for pipe_name in PIPE_COMPONENTS_TO_DISABLE:
            if pipe_name in nlp.pipe_names:
                nlp.disable_pipe(pipe_name)

        try:
            # Remember to add linkers to these if you want linking. If a model
            # was already loaded with the linker, reuse it instead of adding a
            # duplicate.
            if "scispacy_linker" in nlp.pipe_names:
                linker = nlp.get_pipe("scispacy_linker")
            else:
                linker = nlp.add_pipe("scispacy_linker", config={"linker_name": "umls"})

        except ValueError as exc:
            raise RuntimeError(f"Pipeline configuration error for {model_name!r}: {exc}") from exc

        models.append(
            ModelResource(
                name=model_name,
                nlp=nlp,
                linker=linker,
                semantic_type_tree=linker.kb.semantic_type_tree,
            )
        )
        print(f"Loaded model and UMLS linker: {model_name}")

    return tuple(models)


def get_pending_id_bounds(conn: Any) -> Optional[Tuple[int, int]]:
    """Return the min/max pending work-table IDs, or None when no work remains."""
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute(PENDING_BOUNDS_QUERY)
        row = cursor.fetchone()

    finally:
        cursor.close()

    if not row or row["min_id"] is None or row["max_id"] is None:
        return None

    return int(row["min_id"]), int(row["max_id"])


def fetch_abstracts_for_range(conn: Any, start_id: int, end_id: int) -> List[Dict[str, Any]]:
    """Fetch unprocessed abstracts for one ID range using parameterized SQL."""
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute(ABSTRACT_QUERY, (start_id, end_id))
        rows = cursor.fetchall()

    finally:
        cursor.close()

    return rows


def _semantic_type_names(kb_entity: Any, semantic_type_tree: Any) -> List[str]:
    """Convert UMLS semantic type abbreviations into readable names."""
    names = []

    for abbr in kb_entity.types:
        try:
            node = semantic_type_tree.get_node_from_id(abbr)
            names.append(node.full_name)

        except KeyError:
            names.append(f"{abbr} (Name not found)")

    return names


def process_abstract_text(model: ModelResource, application_ids: Sequence[int], abstract_texts: Sequence[str], range_label: str) -> Optional[List[Dict[str, Any]]]:
    """Run one sciSpaCy model over project abstracts and return annotation rows."""
    processed_annotations: List[Dict[str, Any]] = []

    try:
        for index, doc in enumerate(model.nlp.pipe(abstract_texts, batch_size=DEFAULT_NLP_BATCH_SIZE)):
            application_id = application_ids[index]
            print(f"  {model.name}: processing application_id={application_id}")

            for ent in doc.ents:
                if not getattr(ent._, "kb_ents", None):
                    continue

                # Taking the first linked entity as the primary UMLS concept.
                concept_id, score = ent._.kb_ents[0]

                try:
                    kb_entity = model.linker.kb.cui_to_entity[concept_id]

                except KeyError:
                    print(f"  Warning: concept_id={concept_id!r} not found for entity {ent.text!r}.")
                    continue

                semantic_type_names = _semantic_type_names(kb_entity, model.semantic_type_tree)

                processed_annotations.append(
                    {
                        "application_id": application_id,
                        # "entity_label": _val(ent.label_),
                        "concept_id": concept_id,
                        "score": f"{score:.4f}",
                        "umls_concept": _val(kb_entity.canonical_name),
                        "umls_cui": kb_entity.concept_id,
                        "semantic_types": ",".join(kb_entity.types),
                        "semantic_type_names": ",".join(_normalize_txt(name) for name in semantic_type_names),
                        "aliases": ",".join(_normalize_txt(alias) for alias in kb_entity.aliases),
                        "definition": _normalize_txt(kb_entity.definition) if kb_entity.definition else "",
                    }
                )

    except Exception as exc:
        print(f"Error during NLP processing for range {range_label} with {model.name}: {exc}")
        return None

    if not processed_annotations:
        print(f"  {model.name}: no annotations generated for range {range_label}.")

    return processed_annotations


def remove_duplicate_annotations(annotations: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Remove duplicate annotations for the same application_id/concept_id pair.

    The two sciSpaCy processes may generate the same concept_id for the same
    application_id with different scores. Keep the annotation with the highest
    score and skip rows where the score cannot be compared.
    """
    unique_annotations: Dict[Tuple[Any, Any], Dict[str, Any]] = {}

    for annotation in annotations:
        key = (annotation["application_id"], annotation["concept_id"])

        try:
            current_score = float(annotation["score"])

        except (TypeError, ValueError):
            print(
                "Warning: unable to compare annotation score "
                f"{annotation.get('score')!r} for application_id={annotation.get('application_id')}, "
                f"concept_id={annotation.get('concept_id')}. Skipping this row."
            )
            continue

        existing_annotation = unique_annotations.get(key)
        if existing_annotation is None:
            unique_annotations[key] = annotation
            continue

        try:
            existing_score = float(existing_annotation["score"])

        except (TypeError, ValueError):
            unique_annotations[key] = annotation
            continue

        if current_score > existing_score:
            unique_annotations[key] = annotation

    return list(unique_annotations.values())


def save_processed_annotations_to_db(processed_annotations: Sequence[Dict[str, Any]], conn: Any) -> int:
    """Insert annotation rows; the caller commits after the processed flag update."""
    if not processed_annotations:
        return 0

    data_to_insert = [
        (
            ann["application_id"],
            ann["concept_id"],
            ann["score"],
            ann["umls_concept"],
            ann["umls_cui"],
            ann["semantic_types"],
            ann["semantic_type_names"],
            ann["aliases"],
            ann["definition"],
        )
        for ann in processed_annotations
    ]

    cursor = conn.cursor()

    try:
        cursor.executemany(ANNOTATION_INSERT_QUERY, data_to_insert)

    finally:
        cursor.close()

    return len(data_to_insert)


def mark_id_range_processed(conn: Any, start_id: int, end_id: int) -> int:
    """
    Mark the source work-table rows as processed.

    This happens after annotation generation and insertion. Rows without an
    abstract are also marked processed so later runs do not rescan the same gap.
    """
    cursor = conn.cursor()

    try:
        cursor.execute(MARK_RANGE_PROCESSED_QUERY, (PROCESSED_FLAG, start_id, end_id))
        updated_count = cursor.rowcount

    finally:
        cursor.close()

    return updated_count


def process_by_range(conn: Any, start_id: int, end_id: int, models: Sequence[ModelResource]) -> Optional[Dict[str, int]]:
    """Fetch, annotate, de-duplicate, and insert annotations for one ID range."""
    range_label = f"[{start_id}-{end_id}]"

    try:
        rows = fetch_abstracts_for_range(conn, start_id, end_id)

        if not rows:
            print(f"Skip or no new abstracts found for range {range_label}.")
            return {
                "abstract_count": 0,
                "generated_count": 0,
                "deduplicated_count": 0,
                "inserted_count": 0,
            }

        application_ids = [row["application_id"] for row in rows]
        abstract_texts = [row["abstract_text"] for row in rows]

        annotations: List[Dict[str, Any]] = []
        for model in models:
            model_annotations = process_abstract_text(model, application_ids, abstract_texts, range_label)
            if model_annotations is None:
                print(f"Skipping processed-flag update for range {range_label} because NLP processing failed.")
                return None

            annotations.extend(model_annotations)
            print(f"{model.name} generated {len(model_annotations)} annotations for range {range_label}.")

        print(f"Total generated before de-duplication for range {range_label}: {len(annotations)}")

        processed_annotations = remove_duplicate_annotations(annotations)
        print(f"After removing duplicates for range {range_label}: {len(processed_annotations)} annotations")

        inserted_count = save_processed_annotations_to_db(processed_annotations, conn)

        return {
            "abstract_count": len(rows),
            "generated_count": len(annotations),
            "deduplicated_count": len(processed_annotations),
            "inserted_count": inserted_count,
        }

    except mysql.connector.Error as exc:
        conn.rollback()
        print(f"Database error for range {range_label}: {exc}")
        return None


def main() -> int:
    ok = ask_to_continue("*** Generate the Annotation data for Grant.Project ? ***")
    if not ok:
        print("Exit.")
        return 0

    try:
        models = load_models()

    except RuntimeError as exc:
        print(exc)
        return 1

    conn = db().mysql_conn()

    if conn is None:
        print("Unable to connect to MySQL.")
        return 1

    total_ranges = 0
    failed_ranges = 0
    total_abstracts = 0
    total_generated = 0
    total_inserted = 0

    try:
        bounds = get_pending_id_bounds(conn)
        if bounds is None:
            print("No pending project annotation rows found.")
            return 0

        min_id, max_id = bounds
        print(f"Pending work-table ID range: [{min_id}-{max_id}]")

        # Generate ID ranges from live database bounds instead of a hard-coded
        # max_id. This makes the script safer after new projects are loaded.
        for start_id, end_id in _id_range_generator(min_id, max_id, DEFAULT_ID_STEP, DEFAULT_RANGE_BATCH_SIZE):
            total_ranges += 1
            print(f"\n{'=' * 12} Processing ID range [{start_id}-{end_id}] {'=' * 12}")

            summary = process_by_range(conn, start_id, end_id, models)
            if summary is None:
                failed_ranges += 1
                continue

            updated_count = mark_id_range_processed(conn, start_id, end_id)
            conn.commit()

            total_abstracts += summary["abstract_count"]
            total_generated += summary["generated_count"]
            total_inserted += summary["inserted_count"]

            print(
                f"Range [{start_id}-{end_id}] complete: "
                f"abstracts={summary['abstract_count']}, "
                f"generated={summary['generated_count']}, "
                f"inserted={summary['inserted_count']}, "
                f"marked_processed={updated_count}"
            )

    except mysql.connector.Error as exc:
        conn.rollback()
        print(f"MySQL error: {exc}")
        return 1

    finally:
        conn.close()

    print(f"\n{'=' * 12} All Done {'=' * 12}")
    print(
        f"Ranges={total_ranges}, failed_ranges={failed_ranges}, "
        f"abstracts={total_abstracts}, generated={total_generated}, inserted={total_inserted}"
    )

    return 1 if failed_ranges else 0


if __name__ == "__main__":
    raise SystemExit(main())

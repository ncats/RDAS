"""
Generate UMLS annotations for new NIH RePORTER grant project abstracts.

This alert-pipeline task is based on
`D_grant/init_11_Project_annotation_generator.py`.

The task annotates only new grant projects (`grant_project.is_new = 1`) that
already have GARD-project relationship rows. That keeps this step aligned with
the alert pipeline: task_grant_10 first finds the new projects related to GARD
diseases, and this task then generates UMLS concepts for those projects'
abstracts.

Processing flow:
    1. Add any missing new-project application IDs from
       `grant_gard_project_relation` into the work table
       `grant_gard_project_relation_unique_application_id`, and mark the
       current new-project work rows with `is_new = 1`.
    2. Select pending work-table rows where `project_annotation_processed` is
       NULL and the matching `grant_project` row has `is_new = 1`.
    3. Fetch the matching `grant_abstract` text using `APPLICATION_ID` plus
       fiscal year, so duplicate abstract application IDs from different years
       do not cross-match.
    4. Run each abstract through the sciSpaCy NER models and UMLS linker.
    5. De-duplicate annotations by `(application_id, concept_id)`, keeping the
       highest model score when both models produce the same concept.
    6. Insert only annotation rows that do not already exist in
       `grant_project_annotation`, marking inserted/current rows with
       `is_new = 1`.
    7. Mark the processed work-table ID range only after annotation generation
       and insertion finish successfully.

Required inputs:
    `grant_project`
        Supplies `APPLICATION_ID`, `FY`, and `is_new`.
    `grant_abstract`
        Supplies `ABSTRACT_TEXT`, joined by `APPLICATION_ID` and fiscal year.
    `grant_gard_project_relation`
        Supplies application IDs for grant projects with GARD relationships.
    `grant_gard_project_relation_unique_application_id`
        Work table used to track project annotation processing status.
    `grant_project_annotation`
        Output table for UMLS annotations.

sciSpaCy model requirements preserved from the initializer:
    pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.3/en_core_sci_lg-0.5.3.tar.gz
    pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.3/en_ner_bionlp13cg_md-0.5.3.tar.gz
    pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.3/en_ner_bc5cdr_md-0.5.3.tar.gz

The UMLS linker may also need to download or load its knowledge base the first
time the task runs in a fresh environment.
"""

# Reference: D_grant/init_11_Project_annotation_generator.py

import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from pipelines.pipeline_4_grant.grant_base import GrantPipelineBase
from utils.tools import _normalize_txt, _time_hms, _val


MODEL_NAMES = (
    "en_ner_bionlp13cg_md",
    "en_ner_bc5cdr_md",
)

DEFAULT_ID_STEP = 1
DEFAULT_RANGE_BATCH_SIZE = 10
DEFAULT_NLP_BATCH_SIZE = 8
DEFAULT_WORK_INSERT_BATCH_SIZE = 1000
PROCESSED_FLAG = 1

PIPE_COMPONENTS_TO_DISABLE = (
    "tagger",
    "parser",
    "attribute_ruler",
    "lemmatizer",
)

PENDING_NEW_APPLICATION_IDS_SQL = """
    SELECT DISTINCT
        gpr.application_id
    FROM grant_gard_project_relation AS gpr
    INNER JOIN grant_project AS p
        ON p.APPLICATION_ID = gpr.application_id
        AND p.is_new = 1
    LEFT JOIN grant_gard_project_relation_unique_application_id AS existing
        ON existing.application_id = gpr.application_id
    WHERE
        gpr.application_id IS NOT NULL
        AND existing.application_id IS NULL
    ORDER BY gpr.application_id
"""

WORK_TABLE_INSERT_SQL = """
    INSERT INTO grant_gard_project_relation_unique_application_id (
        application_id,
        is_new
    )
    VALUES (%s, 1)
"""

MARK_CURRENT_WORK_ROWS_NEW_SQL = """
    UPDATE grant_gard_project_relation_unique_application_id AS gpru
    INNER JOIN grant_project AS p
        ON p.APPLICATION_ID = gpru.application_id
        AND p.is_new = 1
    INNER JOIN grant_gard_project_relation AS gpr
        ON gpr.application_id = gpru.application_id
    SET gpru.is_new = 1
    WHERE COALESCE(gpru.is_new, 0) <> 1
"""

PENDING_BOUNDS_SQL = """
    SELECT
        MIN(gpru.id) AS min_id,
        MAX(gpru.id) AS max_id
    FROM grant_gard_project_relation_unique_application_id AS gpru
    INNER JOIN grant_project AS p
        ON p.APPLICATION_ID = gpru.application_id
        AND p.is_new = 1
    WHERE gpru.project_annotation_processed IS NULL
"""

ABSTRACT_SELECT_SQL = """
    SELECT DISTINCT
        gpru.id,
        gpru.application_id,
        ga.ABSTRACT_TEXT AS abstract_text
    FROM grant_gard_project_relation_unique_application_id AS gpru
    INNER JOIN grant_project AS p
        ON p.APPLICATION_ID = gpru.application_id
        AND p.is_new = 1
    INNER JOIN grant_abstract AS ga
        ON ga.APPLICATION_ID = p.APPLICATION_ID
        AND ga.YEAR = p.FY
    WHERE
        gpru.id BETWEEN %s AND %s
        AND gpru.project_annotation_processed IS NULL
        AND ga.ABSTRACT_TEXT IS NOT NULL
"""

ANNOTATION_INSERT_IF_MISSING_SQL = """
    INSERT INTO grant_project_annotation (
        application_id,
        concept_id,
        score,
        umls_concept,
        umls_cui,
        semantic_types,
        semantic_type_names,
        aliases,
        definition,
        is_new
    )
    SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, 1
    WHERE NOT EXISTS (
        SELECT 1
        FROM grant_project_annotation AS existing
        WHERE
            existing.application_id <=> %s
            AND existing.concept_id <=> %s
        LIMIT 1
    )
"""

MARK_RANGE_ANNOTATIONS_NEW_SQL = """
    UPDATE grant_project_annotation AS pa
    INNER JOIN grant_gard_project_relation_unique_application_id AS gpru
        ON gpru.application_id = pa.application_id
    INNER JOIN grant_project AS p
        ON p.APPLICATION_ID = gpru.application_id
        AND p.is_new = 1
    SET pa.is_new = 1
    WHERE
        gpru.id BETWEEN %s AND %s
        AND COALESCE(pa.is_new, 0) <> 1
"""

MARK_RANGE_PROCESSED_SQL = """
    UPDATE grant_gard_project_relation_unique_application_id AS gpru
    INNER JOIN grant_project AS p
        ON p.APPLICATION_ID = gpru.application_id
        AND p.is_new = 1
    SET gpru.project_annotation_processed = %s
    WHERE
        gpru.id BETWEEN %s AND %s
        AND gpru.project_annotation_processed IS NULL
"""


@dataclass(frozen=True)
class ModelResource:
    """Loaded sciSpaCy model plus the UMLS linker resources needed for rows."""

    name: str
    nlp: Any
    linker: Any
    semantic_type_tree: Any


class GrantProjectAnnotationTask(GrantPipelineBase):
    """Generate UMLS annotations for new grant-project abstracts."""

    def __init__(self, id_step: int = DEFAULT_ID_STEP, range_batch_size: int = DEFAULT_RANGE_BATCH_SIZE, nlp_batch_size: int = DEFAULT_NLP_BATCH_SIZE, work_insert_batch_size: int = DEFAULT_WORK_INSERT_BATCH_SIZE):
        super().__init__(init_mysql=True, init_memgraph=False)
        self.id_step = id_step
        self.range_batch_size = range_batch_size
        self.nlp_batch_size = nlp_batch_size
        self.work_insert_batch_size = work_insert_batch_size


    def find_new_data(self, gard_node) -> None:
        self.logger.info("GrantProjectAnnotationTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Load models once, annotate pending new projects by ID range, and log a summary."""

        from utils.tools import _id_range_generator

        start_time = time.time()
        summary = {
            "work_rows_inserted": 0,
            "ranges_seen": 0,
            "ranges_failed": 0,
            "abstracts_seen": 0,
            "annotations_generated": 0,
            "annotations_deduplicated": 0,
            "annotations_inserted": 0,
            "annotations_marked_new": 0,
            "work_rows_marked_processed": 0,
            "work_rows_marked_new": 0,
        }

        try:
            if not self._validate_runtime_options():
                return

            if self.mysql is None:
                self.logger.error("Unable to create MySQL connection.")
                return

            # The initializer expected this work table to be populated manually.
            # In the alert pipeline, task_grant_10 may have just inserted new
            # relationship rows, so sync missing new application IDs here.
            summary["work_rows_inserted"] = self._sync_new_work_table_rows()
            summary["work_rows_marked_new"] = self._mark_current_work_rows_new()
            self.mysql.commit()

            models = self._load_models()

            if not models:
                self.logger.error("No sciSpaCy models were loaded. Project annotation task will stop.")
                return

            bounds = self._get_pending_id_bounds()

            if bounds is None:
                self.logger.info(f"No pending new project annotation rows found. Summary={summary}")
                return

            min_id, max_id = bounds
            self.logger.info(f"Pending new project annotation work-table ID range: [{min_id}-{max_id}]")

            for start_id, end_id in _id_range_generator(min_id, max_id, self.id_step, self.range_batch_size):
                summary["ranges_seen"] += 1
                range_label = f"[{start_id}-{end_id}]"
                self.logger.info(f"Processing project annotation range {range_label}.")

                try:
                    range_summary = self._process_id_range(start_id, end_id, models)

                    if range_summary is None:
                        summary["ranges_failed"] += 1
                        self.mysql.rollback()
                        continue

                    marked_new_count = self._mark_range_annotations_new(start_id, end_id)
                    marked_count = self._mark_id_range_processed(start_id, end_id)
                    self.mysql.commit()

                    summary["abstracts_seen"] += range_summary["abstract_count"]
                    summary["annotations_generated"] += range_summary["generated_count"]
                    summary["annotations_deduplicated"] += range_summary["deduplicated_count"]
                    summary["annotations_inserted"] += range_summary["inserted_count"]
                    summary["annotations_marked_new"] += marked_new_count
                    summary["work_rows_marked_processed"] += marked_count

                    self.logger.info(
                        f"Completed range {range_label}: "
                        f"abstracts={range_summary['abstract_count']}, "
                        f"generated={range_summary['generated_count']}, "
                        f"deduplicated={range_summary['deduplicated_count']}, "
                        f"inserted={range_summary['inserted_count']}, "
                        f"marked_new={marked_new_count}, "
                        f"marked_processed={marked_count}"
                    )

                except Exception:
                    summary["ranges_failed"] += 1
                    self.mysql.rollback()
                    self.logger.exception(f"Project annotation range {range_label} failed. Continuing with next range.")
                    continue

            self.logger.info(f"Completed project annotation processing. Summary={summary}")

        except Exception:
            self.logger.exception(f"GrantProjectAnnotationTask failed. Summary={summary}")
            return

        finally:
            hours, minutes, seconds = _time_hms(time.time() - start_time)
            self.logger.info(f"Total time elapsed: {hours} hours, {minutes} minutes, {seconds} seconds")
            self.close()


    def _validate_runtime_options(self) -> bool:
        """Validate range and batch settings before the shared ID generator runs."""

        if self.id_step <= 0:
            self.logger.error("id_step must be greater than 0")
            return False

        if self.range_batch_size <= 0:
            self.logger.error("range_batch_size must be greater than 0")
            return False

        if self.nlp_batch_size <= 0:
            self.logger.error("nlp_batch_size must be greater than 0")
            return False

        if self.work_insert_batch_size <= 0:
            self.logger.error("work_insert_batch_size must be greater than 0")
            return False

        return True


    def _sync_new_work_table_rows(self) -> int:
        """
        Add missing new-project application IDs into the annotation work table.

        The work table's `id` column is AUTO_INCREMENT, so this method inserts
        only `application_id` and lets MySQL assign the row ID.
        """

        read_cursor = None
        insert_cursor = None
        inserted_count = 0

        try:
            # Use a buffered cursor because this method reads pending
            # application IDs and inserts work-table rows on the same MySQL
            # connection. Without buffering, mysql-connector can report
            # "Unread result found" when the insert cursor executes before the
            # SELECT cursor has consumed every row from the server.
            read_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            insert_cursor = self.mysql.cursor()

            read_cursor.execute(PENDING_NEW_APPLICATION_IDS_SQL)

            while True:
                rows = read_cursor.fetchmany(self.work_insert_batch_size)

                if not rows:
                    break

                insert_values = []

                for row in rows:
                    application_id = row.get("application_id")

                    if application_id is None:
                        continue

                    insert_values.append((application_id,))

                if not insert_values:
                    continue

                insert_cursor.executemany(WORK_TABLE_INSERT_SQL, insert_values)
                inserted_count += insert_cursor.rowcount if insert_cursor.rowcount and insert_cursor.rowcount > 0 else len(insert_values)

            self.logger.info(f"Synced {inserted_count} new application ID row(s) into the project annotation work table.")
            return inserted_count

        finally:
            if insert_cursor is not None:
                insert_cursor.close()

            if read_cursor is not None:
                read_cursor.close()


    def _mark_current_work_rows_new(self) -> int:
        """Mark work-table rows linked to current new projects as `is_new = 1`."""

        cursor = None

        try:
            cursor = self.mysql.cursor()
            cursor.execute(MARK_CURRENT_WORK_ROWS_NEW_SQL)

            if cursor.rowcount and cursor.rowcount > 0:
                return cursor.rowcount

            return 0

        finally:
            if cursor is not None:
                cursor.close()


    def _load_models(self) -> Tuple[ModelResource, ...]:
        """Load sciSpaCy NER models and attach the UMLS linker to each model."""

        try:
            import spacy
            import scispacy  # noqa: F401 - importing registers sciSpaCy components.
            from scispacy.linking import EntityLinker  # noqa: F401 - registers scispacy_linker.

        except ImportError:
            self.logger.exception(
                "Unable to import spaCy/scispaCy. Install scispaCy and the model "
                "packages listed in the module comments."
            )
            return ()

        models: List[ModelResource] = []

        for model_name in MODEL_NAMES:
            try:
                nlp = spacy.load(model_name)

                # Keep NER and the UMLS linker intact. Disable only components
                # that are not needed for annotation and are present in the model.
                for pipe_name in PIPE_COMPONENTS_TO_DISABLE:
                    if pipe_name in nlp.pipe_names:
                        nlp.disable_pipe(pipe_name)

                if "scispacy_linker" in nlp.pipe_names:
                    linker = nlp.get_pipe("scispacy_linker")
                else:
                    linker = nlp.add_pipe("scispacy_linker", config={"linker_name": "umls"})

                models.append(
                    ModelResource(
                        name=model_name,
                        nlp=nlp,
                        linker=linker,
                        semantic_type_tree=linker.kb.semantic_type_tree,
                    )
                )
                self.logger.info(f"Loaded sciSpaCy model and UMLS linker: {model_name}")

            except Exception:
                self.logger.exception(f"Unable to load sciSpaCy model or UMLS linker: {model_name}")
                return ()

        return tuple(models)


    def _get_pending_id_bounds(self) -> Optional[Tuple[int, int]]:
        """Return min/max pending work-table IDs for new projects, or None."""

        cursor = None

        try:
            cursor = self.mysql.cursor(dictionary=True)
            cursor.execute(PENDING_BOUNDS_SQL)
            row = cursor.fetchone() or {}

        finally:
            if cursor is not None:
                cursor.close()

        if row.get("min_id") is None or row.get("max_id") is None:
            return None

        return int(row["min_id"]), int(row["max_id"])


    def _fetch_abstracts_for_range(self, start_id: int, end_id: int) -> List[Dict[str, Any]]:
        """Fetch new-project abstracts for one work-table ID range."""

        cursor = None

        try:
            cursor = self.mysql.cursor(dictionary=True)
            cursor.execute(ABSTRACT_SELECT_SQL, (start_id, end_id))
            return cursor.fetchall()

        finally:
            if cursor is not None:
                cursor.close()


    def _process_id_range(self, start_id: int, end_id: int, models: Sequence[ModelResource]) -> Optional[Dict[str, int]]:
        """Fetch, annotate, de-duplicate, and insert annotations for one range."""

        range_label = f"[{start_id}-{end_id}]"
        rows = self._fetch_abstracts_for_range(start_id, end_id)

        if not rows:
            self.logger.info(f"No new abstracts found for project annotation range {range_label}.")
            return {
                "abstract_count": 0,
                "generated_count": 0,
                "deduplicated_count": 0,
                "inserted_count": 0,
            }

        application_ids = [row["application_id"] for row in rows]
        abstract_texts = [row.get("abstract_text") or "" for row in rows]
        annotations: List[Dict[str, Any]] = []

        for model in models:
            model_annotations = self._process_abstract_text(model, application_ids, abstract_texts, range_label)

            if model_annotations is None:
                self.logger.error(f"Skipping processed-flag update for range {range_label} because NLP processing failed.")
                return None

            annotations.extend(model_annotations)
            self.logger.info(f"{model.name} generated {len(model_annotations)} annotation row(s) for range {range_label}.")

        deduplicated_annotations = self._remove_duplicate_annotations(annotations)
        inserted_count = self._save_annotations_to_db(deduplicated_annotations)

        return {
            "abstract_count": len(rows),
            "generated_count": len(annotations),
            "deduplicated_count": len(deduplicated_annotations),
            "inserted_count": inserted_count,
        }


    def _process_abstract_text(self, model: ModelResource, application_ids: Sequence[int], abstract_texts: Sequence[str], range_label: str) -> Optional[List[Dict[str, Any]]]:
        """Run one sciSpaCy model over abstracts and build insert-ready rows."""

        processed_annotations: List[Dict[str, Any]] = []

        try:
            for index, doc in enumerate(model.nlp.pipe(abstract_texts, batch_size=self.nlp_batch_size)):
                application_id = application_ids[index]
                self.logger.info(f"{model.name}: processing application_id={application_id}")

                for ent in doc.ents:
                    if not getattr(ent._, "kb_ents", None):
                        continue

                    # sciSpaCy ranks linked UMLS concepts for each entity. Keep
                    # the top concept, matching the initializer's behavior.
                    concept_id, score = ent._.kb_ents[0]

                    try:
                        kb_entity = model.linker.kb.cui_to_entity[concept_id]

                    except KeyError:
                        self.logger.warning(f"{model.name}: concept_id={concept_id!r} not found for entity={ent.text!r}.")
                        continue

                    semantic_type_names = self._semantic_type_names(kb_entity, model.semantic_type_tree)

                    processed_annotations.append(
                        {
                            "application_id": application_id,
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

        except Exception:
            self.logger.exception(f"NLP processing failed for range {range_label} with model={model.name}.")
            return None

        return processed_annotations


    def _semantic_type_names(self, kb_entity: Any, semantic_type_tree: Any) -> List[str]:
        """Convert UMLS semantic type abbreviations into readable names."""

        names = []

        for abbr in kb_entity.types:
            try:
                node = semantic_type_tree.get_node_from_id(abbr)
                names.append(node.full_name)

            except KeyError:
                names.append(f"{abbr} (Name not found)")

        return names


    def _remove_duplicate_annotations(self, annotations: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Keep the highest-scoring row for each application/concept pair."""

        unique_annotations: Dict[Tuple[Any, Any], Dict[str, Any]] = {}

        for annotation in annotations:
            key = (annotation["application_id"], annotation["concept_id"])

            try:
                current_score = float(annotation["score"])

            except (TypeError, ValueError):
                self.logger.warning(
                    "Unable to compare annotation score "
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


    def _save_annotations_to_db(self, processed_annotations: Sequence[Dict[str, Any]]) -> int:
        """Insert de-duplicated annotations, skipping existing app/concept rows."""

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
                ann["application_id"],
                ann["concept_id"],
            )
            for ann in processed_annotations
        ]
        cursor = None

        try:
            cursor = self.mysql.cursor()
            cursor.executemany(ANNOTATION_INSERT_IF_MISSING_SQL, data_to_insert)

            if cursor.rowcount and cursor.rowcount > 0:
                return cursor.rowcount

            return 0

        finally:
            if cursor is not None:
                cursor.close()


    def _mark_id_range_processed(self, start_id: int, end_id: int) -> int:
        """Mark one completed new-project work-table range as processed."""

        cursor = None

        try:
            cursor = self.mysql.cursor()
            cursor.execute(MARK_RANGE_PROCESSED_SQL, (PROCESSED_FLAG, start_id, end_id))

            if cursor.rowcount and cursor.rowcount > 0:
                return cursor.rowcount

            return 0

        finally:
            if cursor is not None:
                cursor.close()


    def _mark_range_annotations_new(self, start_id: int, end_id: int) -> int:
        """Mark existing annotation rows in a completed new-project range as new."""

        cursor = None

        try:
            cursor = self.mysql.cursor()
            cursor.execute(MARK_RANGE_ANNOTATIONS_NEW_SQL, (start_id, end_id))

            if cursor.rowcount and cursor.rowcount > 0:
                return cursor.rowcount

            return 0

        finally:
            if cursor is not None:
                cursor.close()


if __name__ == "__main__":

    task = GrantProjectAnnotationTask()
    task.process_new_data()

import os
import sys
import json
import spacy
import scispacy
from scispacy.linking import EntityLinker

'''
Installation Check and Model Loading
Ensure you've installed all necessary scispaCy models:
pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.3/en_core_sci_lg-0.5.3.tar.gz
pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.3/en_ner_bionlp13cg_md-0.5.3.tar.gz
pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.3/en_ner_bc5cdr_md-0.5.3.tar.gz
'''

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "../..")),
    os.path.abspath(os.path.join(_dir, "../../..")),
])

from utils.tools import _val, _normalize_txt, _clean
from pipelines.pipeline_base import PipelineBase

"""
Generate the Annotation data for NEW Clinical Trail
"""

# Reference: B_clinical_trial/init_9_clinical_trail_annotation_generator.py

class NewClinicalTrialAnnotationTask(PipelineBase):

    DEFAULT_NLP_PIPE_BATCH_SIZE = 1
    DEFAULT_NLP_TEXT_CHUNK_SIZE = 12000

    def __init__(self):

        super().__init__(init_mysql=True, init_memgraph=False)
        self.nlp_pipe_batch_size = self.parse_positive_int_setting(os.getenv("CLINICAL_TRIAL_NLP_PIPE_BATCH_SIZE"), self.DEFAULT_NLP_PIPE_BATCH_SIZE, "CLINICAL_TRIAL_NLP_PIPE_BATCH_SIZE")
        self.nlp_text_chunk_size = self.parse_positive_int_setting(os.getenv("CLINICAL_TRIAL_NLP_TEXT_CHUNK_SIZE"), self.DEFAULT_NLP_TEXT_CHUNK_SIZE, "CLINICAL_TRIAL_NLP_TEXT_CHUNK_SIZE")


    # Not implemented
    def find_new_data(self, gard_node) -> None:

        raise NotImplementedError("NewClinicalTrialAnnotationTask does not implement find_new_data().")


    def parse_positive_int_setting(self, value, default_value, setting_name):

        """Parse a positive integer environment setting, falling back to a safe default."""

        if value is None or str(value).strip() == "":
            return default_value

        try:
            parsed_value = int(value)

            if parsed_value > 0:
                return parsed_value

        except (TypeError, ValueError):
            pass

        self.logger.warning(f"Invalid {setting_name}={value}; using default {default_value}.")
        return default_value


    def load_models(self):

        """
        Load the scispaCy NER models used to annotate clinical-trial text.

        The BioNLP model extracts broad biomedical entities, while the BC5CDR
        model focuses on chemical and disease entities. Each model gets a UMLS
        linker so later steps can map detected spans to UMLS concepts and
        semantic type metadata.
        """
        try:
            '''
            Load the BioNLP NER model with non-NER pipeline components disabled
            because this pipeline only needs entity spans and UMLS linking.
            '''
            nlp_bionlp = spacy.load("en_ner_bionlp13cg_md", disable=["tok2vec", "tagger", "parser", "attribute_ruler", "lemmatizer"])
            self.logger.info("Model 'en_ner_bionlp13cg_md'")

            ''' Add the UMLS linker to normalize BioNLP entities to UMLS concepts. '''
            linker_bionlp_component = nlp_bionlp.add_pipe("scispacy_linker", config={"linker_name": "umls"})
            self.logger.info("The linker for 'en_ner_bionlp13cg_md'")


            ''' Load the BC5CDR model to capture disease and chemical mentions. '''
            nlp_bc5cdr = spacy.load("en_ner_bc5cdr_md", disable=["tok2vec", "tagger", "parser", "attribute_ruler", "lemmatizer"])
            self.logger.info("Model 'en_ner_bc5cdr_md'")

            '''
            Add a separate UMLS linker for BC5CDR entities so each model can
            expose its own knowledge base and semantic type tree.
            '''
            linker_bc5cdr_component = nlp_bc5cdr.add_pipe("scispacy_linker", config={"linker_name": "umls"})
            self.logger.info("The linker for 'en_ner_bc5cdr_md'")

            '''
            semantic_type_tree = linker.kb.semantic_type_tree
            self.logger.info("UMLS Semantic Type Tree loaded successfully.")
            '''

            '''
            Return both NLP objects and their linker components for annotation
            generation and semantic type lookup in process_new_data().
            '''
            return nlp_bionlp, linker_bionlp_component, nlp_bc5cdr, linker_bc5cdr_component

        except OSError as e:
            self.logger.error(f"Error loading scispaCy models: {e}")
            self.logger.error("Please ensure all models are installed correctly using the pip commands provided in the comments.")
            return None, None, None, None
        
        except ValueError as e:
            self.logger.error(f"Pipeline configuration error: {e}")
            self.logger.error("This might be due to incorrect component names or order.")
            return None, None, None, None


    # implement
    def process_new_data(self) -> None:
        
        fetch_cursor = None

        try:
            ''' Step 1. Load the scispaCy NER models and UMLS linkers used for annotation. '''
            nlp_bionlp, bionlp_linker, nlp_bc5cdr, bc5cdrlinker = self.load_models()

            if not all([nlp_bionlp, bionlp_linker, nlp_bc5cdr, bc5cdrlinker]):
                self.logger.error("Skipping clinical trial annotation generation because one or more scispaCy models failed to load.")
            else:
                ''' Step 2. Cache each linker's semantic type tree for later UMLS type lookup. '''
                bionlp_semantic_type_tree = bionlp_linker.kb.semantic_type_tree
                bc5cdr_semantic_type_tree = bc5cdrlinker.kb.semantic_type_tree

                ''' Step 3. Select newly added clinical trials that should be annotated. '''
                query = '''
                    SELECT nctid, studies
                    FROM clinical_trial_unique
                    WHERE is_new = 1
                '''

                batch_num = 0
                batch_size = 100

                ''' Step 4. Stream matching trial rows in batches to avoid loading all studies at once. '''
                fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
                fetch_cursor.execute(query)

                while True:
                    rows = fetch_cursor.fetchmany(batch_size)

                    if not rows:
                        self.logger.info(f"No more rows to fetch.")
                        break

                    batch_num += 1
                    self.logger.info(f'\n--- batch# = {batch_num} ---')

                    try:
                        batch_nctids = []
                        seen_batch_nctids = set()
                        nctid_list = []
                        description_list = []

                        ''' Step 5. Extract the NCT ID and trial description text from each study JSON. '''
                        for row in rows:
                            nctid = row['nctid']
                            studies = row['studies']

                            if nctid and nctid not in seen_batch_nctids:
                                seen_batch_nctids.add(nctid)
                                batch_nctids.append(nctid)

                            if studies:
                                obj = json.loads(studies)
                                descriptionModule = obj.get('protocolSection', {}).get('descriptionModule')

                                if descriptionModule:
                                    '''
                                    This line handles the primary/fallback logic.
                                    It tries to get 'detailedDescription', and if
                                    that's None (or not found), it falls back to
                                    'briefSummary'.
                                    '''
                                    description = descriptionModule.get('detailedDescription') or descriptionModule.get('briefSummary')

                                    if description:
                                        nctid_list.append(nctid)
                                        description_list.append(description)

                        if len(nctid_list) <= 0:
                            '''
                            A changed study can legitimately lose its description
                            text. In that case, clear the current-run annotation
                            flags for the batch so downstream graph cleanup removes
                            annotations that no longer come from the latest JSON.
                            '''
                            self.save_processed_annotations_to_db([], batch_nctids)
                            continue

                        ''' Step 6. Generate biomedical annotations with the BioNLP model. '''
                        processed_annotations_1 = self.process_description_text(nlp_bionlp, bionlp_linker, bionlp_semantic_type_tree, nctid_list, description_list)
                        self.logger.info(f'en_ner_bionlp13cg_md generated: {len(processed_annotations_1)} annotations')

                        ''' Step 7. Generate disease and chemical annotations with the BC5CDR model. '''
                        processed_annotations_2 = self.process_description_text(nlp_bc5cdr, bc5cdrlinker, bc5cdr_semantic_type_tree, nctid_list, description_list)
                        print(f'en_ner_bc5cdr_md generated: {len(processed_annotations_2)} annotations')

                        ''' Step 8. Merge annotations produced by both models. '''
                        processed_annotations = processed_annotations_1 + processed_annotations_2
                        self.logger.info(f'Total generated: {len(processed_annotations)} annotations')

                        '''
                        Step 9. Remove duplicate NCT ID/concept ID pairs, keeping the
                        annotation with the highest linker score.
                        '''
                        processed_annotations = self.remove_duplicate_annotations(processed_annotations)
                        self.logger.info(f'After removing duplicates: {len(processed_annotations)} annotations')

                        '''
                        Step 10. Save the processed annotations to the database.
                        The save method first clears prior is_new flags for these
                        NCT IDs so graph step 10 only sees annotations from the
                        latest fetched study JSON.
                        '''
                        self.save_processed_annotations_to_db(processed_annotations, batch_nctids)

                        for ann in processed_annotations:
                            print(ann)

                    except Exception as err:
                        self.logger.error(f"Error processing clinical trial annotation batch#{batch_num}: {err}")
                        continue

        except Exception as err:
            self.logger.error(f"Error: {err}")

        finally:
            ''' Step 10. Close the cursor and database connections after processing finishes. '''
            if fetch_cursor:
                fetch_cursor.close()

            self.close()



    ''' Step 10. Save the processed annotations to the database. '''
    def save_processed_annotations_to_db(self, processed_annotations, current_nctids):

        """
        Replace current annotation markers for the changed clinical trials.

        clinical_trial_annotation keeps historical rows, so this method does not
        delete previous annotations. Instead, it clears is_new for the NCT IDs in
        the current batch and inserts the freshly generated annotations as
        is_new=1. The reset and insert run in one transaction so a failed insert
        does not leave the downstream graph task with an empty current set.
        """

        if not current_nctids:
            return

        current_nctids = list(dict.fromkeys(current_nctids))
        processed_annotations = processed_annotations or []
        reset_placeholders = ", ".join(["%s"] * len(current_nctids))
        insert_cursor = None

        try:
            insert_cursor = self.mysql.cursor()

            reset_query = f"""
                UPDATE clinical_trial_annotation
                SET is_new = 0
                WHERE is_new = 1
                AND nctid IN ({reset_placeholders})
            """

            insert_cursor.execute(reset_query, tuple(current_nctids))
            reset_count = max(insert_cursor.rowcount, 0)

            insert_query = """
                INSERT INTO clinical_trial_annotation (
                    nctid, concept_id, score, umls_concept,
                    umls_cui, semantic_types, semantic_type_names, aliases, definition, is_new
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
            """

            data_to_insert = [
                (
                    ann['nctid'], ann['concept_id'], ann['score'], ann['umls_concept'],
                    ann['umls_cui'], ann['semantic_types'], ann['semantic_type_names'],
                    ann['aliases'], ann['definition']
                )
                for ann in processed_annotations
            ]

            if data_to_insert:
                insert_cursor.executemany(insert_query, data_to_insert)

            self.mysql.commit()

            self.logger.info(f'Reset {reset_count} old current annotations and inserted {len(processed_annotations)} refreshed annotations')

        except Exception as e:
            self.logger.error(f"Error refreshing annotations: {e}")
            self.mysql.rollback()

        finally:
            if insert_cursor:
                insert_cursor.close()



    def remove_duplicate_annotations(self, annotations_list):

        """
        Removes duplicate annotations from a list of dictionaries based on 'nctid'
        and 'concept_id', keeping the entry with the highest 'score'.
        Args:
            annotations_list (list): A list of dictionaries, where each dictionaryrepresents an annotation.
        Returns:
            list: A new list containing only the unique annotations.
        """

        '''
        A dictionary to store unique annotations
        Key: (nctid, concept_id)
        Value: The annotation dictionary with the highest score
        '''
        unique_annotations = {}

        for annotation in annotations_list:

            nctid = annotation['nctid']
            concept_id = annotation['concept_id']
            score_str = annotation['score']

            ''' Convert score to float for comparison '''
            try:
                current_score = float(score_str)
            except ValueError:
                self.logger.info(f"Warning: Could not convert score '{score_str}' to float for nctid {nctid}, concept_id {concept_id}. Skipping this annotation for score comparison.")
                continue # Skip this annotation if score is invalid

            key = (nctid, concept_id)

            if key not in unique_annotations:
                ''' If this combination is new, add it '''
                unique_annotations[key] = annotation
            else:
                '''If this combination already exists, compare scores '''
                existing_annotation = unique_annotations[key]
                try:
                    existing_score = float(existing_annotation['score'])
                except ValueError:
                    ''' If existing score is invalid, and current is valid, replace it '''
                    if current_score is not None:
                        unique_annotations[key] = annotation
                    continue

                if current_score > existing_score:
                    ''' If the current annotation has a higher score, replace the existing one '''
                    unique_annotations[key] = annotation

        ''' Convert the dictionary values back to a list '''
        return list(unique_annotations.values())



    def process_description_text(self, nlp, linker, semantic_type_tree, nctid_list, description_list):

        processed_annotations = []

        for i, description in enumerate(description_list):
            current_app_id = nctid_list[i]
            description_chunks = self.split_description_text(description)

            if not description_chunks:
                continue

            if len(description_chunks) > 1:
                self.logger.info(
                    f"Split nctid={current_app_id} description into {len(description_chunks)} NLP chunks "
                    f"using chunk_size={self.nlp_text_chunk_size}."
                )

            for chunk_index, description_chunk in enumerate(description_chunks, 1):
                self.logger.info(f"Processing nctid: {current_app_id}, chunk {chunk_index}/{len(description_chunks)}")

                try:
                    docs = nlp.pipe(
                        [description_chunk],
                        batch_size=self.nlp_pipe_batch_size,
                        disable=["parser", "attribute_ruler", "lemmatizer"],
                    )

                    for doc in docs:
                        for ent in doc.ents:

                            if hasattr(ent._, 'kb_ents') and ent._.kb_ents:
                                # Taking the first linked entity as the primary
                                concept_id, score = ent._.kb_ents[0]
                                try:
                                    kb_entity = linker.kb.cui_to_entity[concept_id]

                                    semantic_type_names = []

                                    for abbr in kb_entity.types:
                                        try:
                                            node = semantic_type_tree.get_node_from_id(abbr)
                                            semantic_type_names.append(node.full_name)
                                        except KeyError:
                                            semantic_type_names.append(f"{abbr} (Name not found)")
                                            continue

                                    processed_annotations.append( {
                                        'nctid': current_app_id,
                                        #'entity_label': _val(ent.label_),
                                        'concept_id': concept_id,
                                        'score': f'{score:.4f}',
                                        'umls_concept': _val(kb_entity.canonical_name),
                                        'umls_cui': kb_entity.concept_id,
                                        'semantic_types': ','.join(kb_entity.types),
                                        'semantic_type_names': ','.join(_normalize_txt(name) for name in semantic_type_names),
                                        'aliases': ','.join(_normalize_txt(alias) for alias in kb_entity.aliases),
                                        'definition': _normalize_txt(kb_entity.definition) if kb_entity.definition else ''
                                    })

                                except KeyError as e:
                                    self.logger.error(e)
                                    self.logger.info(f"Warning: Concept ID '{concept_id}' not found for '{ent.text}'.")
                                    continue

                except Exception as e:
                    model_name = nlp.meta.get("name", "unknown") if hasattr(nlp, "meta") else "unknown"
                    self.logger.error(
                        f"Error during NLP processing: model={model_name}, nctid={current_app_id}, "
                        f"chunk={chunk_index}/{len(description_chunks)}, text_length={len(description or '')}, "
                        f"chunk_length={len(description_chunk)}, batch_size={self.nlp_pipe_batch_size}, "
                        f"chunk_size={self.nlp_text_chunk_size}: {e}",
                        exc_info=True,
                    )
                    continue

        if not processed_annotations:
            self.logger.info(f'No new annotations generated')
            return []

        return processed_annotations


    def split_description_text(self, description):

        """Split long clinical trial descriptions so sciSpaCy does not process one huge document."""

        description_text = str(description or "").strip()

        if not description_text:
            return []

        if len(description_text) <= self.nlp_text_chunk_size:
            return [description_text]

        chunks = []
        start_index = 0

        while start_index < len(description_text):
            end_index = min(start_index + self.nlp_text_chunk_size, len(description_text))

            if end_index < len(description_text):
                split_index = description_text.rfind(" ", start_index, end_index)

                if split_index > start_index:
                    end_index = split_index

            chunk = description_text[start_index:end_index].strip()

            if chunk:
                chunks.append(chunk)

            start_index = end_index

            while start_index < len(description_text) and description_text[start_index].isspace():
                start_index += 1

        return chunks

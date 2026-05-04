import os
import sys
import json
import spacy
import scispacy
from scispacy.linking import EntityLinker

# --- Installation Check and Model Loading ---
# Ensure you've installed all necessary scispaCy models:
# pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.3/en_core_sci_lg-0.5.3.tar.gz
# pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.3/en_ner_bionlp13cg_md-0.5.3.tar.gz
# pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.3/en_ner_bc5cdr_md-0.5.3.tar.gz

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

class ClinicalTrialTask_6(PipelineBase):

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=False)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        raise NotImplementedError("ClinicalTrialTask_2 does not implement find_new_data().")


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

            #semantic_type_tree = linker.kb.semantic_type_tree
            #self.logger.info("UMLS Semantic Type Tree loaded successfully.")

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
                        nctid_list = []
                        description_list = []

                        ''' Step 5. Extract the NCT ID and trial description text from each study JSON. '''
                        for row in rows:
                            nctid = row['nctid']
                            studies = row['studies']

                            if studies:
                                obj = json.loads(studies)
                                descriptionModule = obj.get('protocolSection', {}).get('descriptionModule')

                                if descriptionModule:
                                    # This line handles the primary/fallback logic.
                                    # It tries to get 'detailedDescription', and if that's None (or not found), it falls back to 'briefSummary'.
                                    description = descriptionModule.get('detailedDescription') or descriptionModule.get('briefSummary')

                                    if description:
                                        nctid_list.append(nctid)
                                        description_list.append(description)

                        if len(nctid_list) <= 0:
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

                        ''' Step 10. Save the processed annotations to the database. '''
                        self.save_processed_annotations_to_db(processed_annotations)

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
    def save_processed_annotations_to_db(self, processed_annotations):

        if not processed_annotations:
            return

        insert_cursor = None

        try:
            insert_cursor = self.mysql.cursor()

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

            insert_cursor.executemany(insert_query, data_to_insert)
            self.mysql.commit()

            self.logger.info(f'Inserted: {len(processed_annotations)} annotations')

        except Exception as e:
            self.logger.error(f"Error inserting annotations: {e}")
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
        try:
            for i, doc in enumerate(nlp.pipe(description_list, disable=["parser", "attribute_ruler", "lemmatizer"])):

                current_app_id = nctid_list[i]
                self.logger.info(f"Processing nctid: {current_app_id}")

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

                if not processed_annotations:
                    self.logger.info(f'No new annotations generated')
                    return []

        except Exception as e:
            self.logger.error(f'Error during NLP processing: {e}')
            return []

        return processed_annotations

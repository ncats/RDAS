import os
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import spacy
import scispacy
from scispacy.linking import EntityLinker

# --- Installation Check and Model Loading ---
# Ensure you've installed all necessary scispaCy models:
# pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.3/en_core_sci_lg-0.5.3.tar.gz
# pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.3/en_ner_bionlp13cg_md-0.5.3.tar.gz
# pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.3/en_ner_bc5cdr_md-0.5.3.tar.gz
 
from utils.conn import DBConnection as db
from utils.tools import ask_to_continue, _val, _normalize_txt, _id_range_generator, _append_to_file
import mysql.connector # Make sure this import is present at the top of your file

from colorama import init, Fore, Style
init()
 
############################################################################################################## 
# Step one:
'''
SELECT CONCAT(GROUP_CONCAT('p.', COLUMN_NAME ORDER BY ORDINAL_POSITION SEPARATOR ', '))  as columns
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'rdas_db' 
AND TABLE_NAME = 'grant_project_annotation'
'''

'''
CREATE TABLE rdas_db.grant_gard_project_relation_unique_application_id (
    id SERIAL PRIMARY KEY,
    application_id INT UNIQUE
);

INSERT INTO rdas_db.grant_gard_project_relation_unique_application_id (application_id)
SELECT DISTINCT application_id
FROM rdas_db.grant_gard_project_relation
ORDER BY application_id;
''' 
# End of step one


############################################################################################################## 
# Step tow:

def load_models():
    try: 
        
        nlp_bionlp = spacy.load("en_ner_bionlp13cg_md", disable=["tok2vec", "tagger", "parser", "attribute_ruler", "lemmatizer"])
        success_loaded("Model 'en_ner_bionlp13cg_md'")

        # Remember to add linkers to these if you want linking 
        linker_bionlp_component = nlp_bionlp.add_pipe("scispacy_linker", config={"linker_name": "umls"})
        success_loaded("The linker for 'en_ner_bionlp13cg_md'")
        

        nlp_bc5cdr = spacy.load("en_ner_bc5cdr_md", disable=["tok2vec", "tagger", "parser", "attribute_ruler", "lemmatizer"])
        success_loaded("Model 'en_ner_bc5cdr_md'")        
       
        linker_bc5cdr_component = nlp_bc5cdr.add_pipe("scispacy_linker", config={"linker_name": "umls"})
        success_loaded("The linker for 'en_ner_bc5cdr_md'")
        
        #semantic_type_tree = linker.kb.semantic_type_tree
        #print(Fore.GREEN+ "UMLS Semantic Type Tree loaded successfully."+ Style.RESET_ALL)

        # Return the nlp objects AND their respective linker components
        return nlp_bionlp, linker_bionlp_component, nlp_bc5cdr, linker_bc5cdr_component
 
    except OSError as e:
        print(Fore.RED+ f"Error loading scispaCy models: {e}"+ Style.RESET_ALL)
        print(Fore.RED+ "Please ensure all models are installed correctly using the pip commands provided in the comments."+ Style.RESET_ALL)
        exit()
    except ValueError as e:
        print(Fore.RED+ f"Pipeline configuration error: {e}"+ Style.RESET_ALL)
        print(Fore.RED+ "This might be due to incorrect component names or order."+ Style.RESET_ALL)
        exit()
    

def remove_duplicate_annotations(annotations_list):
    """
    Removes duplicate annotations from a list of dictionaries based on 'application_id'
    and 'concept_id', keeping the entry with the highest 'score'.
    Args:
        annotations_list (list): A list of dictionaries, where each dictionaryrepresents an annotation.
    Returns:
        list: A new list containing only the unique annotations.
    """
    unique_annotations = {} # A dictionary to store unique annotations
                            # Key: (application_id, concept_id)
                            # Value: The annotation dictionary with the highest score

    for annotation in annotations_list:
        app_id = annotation['application_id']
        concept_id = annotation['concept_id']
        score_str = annotation['score']

        # Convert score to float for comparison
        try:
            current_score = float(score_str)
        except ValueError:
            print(f"Warning: Could not convert score '{score_str}' to float for app_id {app_id}, concept_id {concept_id}. Skipping this annotation for score comparison.")
            continue # Skip this annotation if score is invalid

        key = (app_id, concept_id)

        if key not in unique_annotations:
            # If this combination is new, add it
            unique_annotations[key] = annotation
        else:
            # If this combination already exists, compare scores
            existing_annotation = unique_annotations[key]
            try:
                existing_score = float(existing_annotation['score'])
            except ValueError:
                # If existing score is invalid, and current is valid, replace it
                if current_score is not None:
                    unique_annotations[key] = annotation
                continue

            if current_score > existing_score:
                # If the current annotation has a higher score, replace the existing one
                unique_annotations[key] = annotation

    # Convert the dictionary values back to a list
    return list(unique_annotations.values())



def success_loaded(message):
    print(Fore.BLUE+ f'{message} loaded successfully.\n'+ Style.RESET_ALL)


def save_processed_annotations_to_db(processed_annotations, conn):
    
    insert_cursor = conn.cursor(buffered=True) 

    if processed_annotations:
       
        insert_query = """
            INSERT INTO grant_project_annotation (
                application_id,    concept_id, score, umls_concept, 
                umls_cui, semantic_types, semantic_type_names, aliases, definition
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        # Ensure data is in the correct ORDER for the %s placeholders
        data_to_insert = [
            (ann['application_id'],  ann['concept_id'], ann['score'], ann['umls_concept'], 
             ann['umls_cui'], ann['semantic_types'], ann['semantic_type_names'], ann['aliases'], ann['definition'])
            for ann in processed_annotations
        ]
  
        try:
            insert_cursor.executemany(insert_query, data_to_insert)
            conn.commit() # Commit changes after successful batch insert

            print(f"{Fore.BLUE}  Stored {len(processed_annotations)} annotations for range [{start_id}-{end_id}].{Style.RESET_ALL}")
        except mysql.connector.Error as err:
            print(f"{Fore.RED}Database INSERT error for range [{start_id}-{end_id}]: {err}{Style.RESET_ALL}")
            conn.rollback() # Rollback on error

        if insert_cursor:
            insert_cursor.close()

          
def process_by_range(conn, start_id, end_id, nlp_bionlp, nlp_bc5cdr, bionlp_linker, bc5cdrlinker, bionlp_semantic_type_tree, bc5cdr_semantic_type_tree) :

    dict_cursor = conn.cursor(dictionary=True, buffered=True)

    # Use table grant_gard_project_relation_unique_application_id
    try:
        query = f'''
            SELECT gpru.application_id, ga.abstract_text
            FROM grant_gard_project_relation_unique_application_id gpru
            LEFT JOIN grant_project_annotation gpa
                ON gpru.application_id = gpa.application_id
            INNER JOIN grant_abstract ga
                ON gpru.application_id = ga.APPLICATION_ID
            WHERE (gpru.id BETWEEN {start_id} AND {end_id})  
            AND gpa.application_id IS NULL
            AND ga.abstract_text IS NOT NULL 
        '''

        query = f'''
            SELECT gpru.application_id, ga.abstract_text
            FROM grant_gard_project_relation_unique_application_id gpru 
            INNER JOIN grant_abstract ga
                ON gpru.application_id = ga.APPLICATION_ID
            WHERE (gpru.id BETWEEN {start_id} AND {end_id}) 
                AND gpru.project_annotation_processed is NULL
                AND ga.abstract_text IS NOT NULL 
        '''

        dict_cursor.execute(query)
        rows = dict_cursor.fetchall() # Fetch all results immediately

        if not rows:
            print(f'{Fore.YELLOW}Skip or No new abstracts found for range [{start_id}-{end_id}].{Style.RESET_ALL}')
            return
 
        application_ids = [row['application_id'] for row in rows]
        abstract_texts = [row['abstract_text'] for row in rows] # Now 'abstract_text' key should work

        processed_annotations_1 = process_abstract_text(nlp_bionlp, bionlp_linker, bionlp_semantic_type_tree, application_ids, abstract_texts)
        print(f'en_ner_bionlp13cg_md generated: {len(processed_annotations_1)} annotations')

        processed_annotations_2 = process_abstract_text(nlp_bc5cdr, bc5cdrlinker, bc5cdr_semantic_type_tree, application_ids, abstract_texts)
        print(f'en_ner_bc5cdr_md generated: {len(processed_annotations_2)} annotations')

        processed_annotations = processed_annotations_1 + processed_annotations_2
        print(f'Total generated: {len(processed_annotations)} annotations')

        '''
        The two processes may generate the enities with same concept_id (with application_id) but different score, remove the duplicates with the lower score.
        '''
        processed_annotations = remove_duplicate_annotations(processed_annotations)
        print(f'After removing duplicates: {len(processed_annotations)} annotations')
          
    except mysql.connector.Error as err:
        print(f"{Fore.RED}Database query error for range [{start_id}-{end_id}]: {err}{Style.RESET_ALL}")
        return None
 
    if dict_cursor:
        dict_cursor.close()
 
    return processed_annotations



def process_abstract_text(nlp, linker, semantic_type_tree, application_ids, abstract_texts):

    processed_annotations = []
    try:
        for i, doc in enumerate(nlp.pipe(abstract_texts, disable=["parser", "attribute_ruler", "lemmatizer"])):
            
            current_app_id = application_ids[i] 
            print(f"  Processing application_id: {current_app_id}")

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
                            'application_id': current_app_id,
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
                        print(e)
                        print(f"{Fore.YELLOW}Warning: Concept ID '{concept_id}' not found for '{ent.text}'.{Style.RESET_ALL}")
                        continue

            if not processed_annotations:
                print(f"{Fore.MAGENTA}  No new annotations generated for range [{start_id}-{end_id}].{Style.RESET_ALL}")
                return []
            
    except Exception as e:
        print(e)
        print(f"{Fore.RED}Error during NLP processing for range [{start_id}-{end_id}]: {e}{Style.RESET_ALL}")  
        return []
        
    return processed_annotations

        
# Step two starts here
if __name__ == '__main__':
    
    ok = ask_to_continue(f'*** Generate the Annotation data for Grant.Project ? *** ')
    if not ok:
        sys.exit(Fore.RED + '\n'+ '-'*50 +  '  Exit  ' + '-'*50 + '\n'+ Style.RESET_ALL)
 
    nlp_bionlp, bionlp_linker, nlp_bc5cdr, bc5cdrlinker = load_models()
  
    bionlp_semantic_type_tree = bionlp_linker.kb.semantic_type_tree # Access semantic type tree
    bc5cdr_semantic_type_tree = bc5cdrlinker.kb.semantic_type_tree
 
    #SELECT min(id), max(id)  FROM rdas_db.grant_gard_project_relation_unique_application_id;
    min_id = 1
    max_id = 388186
    
    step = 1
    batch_size = 10  
    
    # Generate ID ranges
    id_ranges = list(_id_range_generator(min_id, max_id, step, batch_size))
  
    with db().mysql_conn() as conn: # Using 'with' for connection management
        # It's generally safer to create a new cursor for each main operation
        # or at least ensure the cursor is fully consumed.
        # Using dictionary=True for easier access to columns by name.
        # buffered=True is essential for fetching all results into memory.
        
        update_cursor = conn.cursor(buffered=True)

        for start_id, end_id in id_ranges:

            print(f'\n{Fore.CYAN}Processing ID range: [{start_id}-{end_id}]{Style.RESET_ALL}')

            processed_annotations = process_by_range(conn, start_id, end_id, nlp_bionlp, nlp_bc5cdr, bionlp_linker, bc5cdrlinker, bionlp_semantic_type_tree, bc5cdr_semantic_type_tree) 

            if processed_annotations:
                save_processed_annotations_to_db(processed_annotations, conn)

            # Mark as already processed
            update_cursor.execute(f"UPDATE grant_gard_project_relation_unique_application_id SET project_annotation_processed = 1 WHERE id BETWEEN {start_id} AND {end_id}")
            conn.commit()


        if update_cursor:
            update_cursor.close()

    if conn:
        conn.close()

    # Connections and cursors will be closed automatically by 'with' statement
    print(f'{Fore.BLUE}\n'+'=**='*15 + ' All Done  '+'=**='*15 + '\n'+ Style.RESET_ALL)
 
    sys.exit()



############################################################################################################## 
# Step Three: create a table grant_project_annotation_unique -- identified by CONCEPT_ID

'''
-- Create index on concept_id to optimize the query

CREATE INDEX idx_grant_project_annotation_concept_id ON rdas_db.grant_project_annotation (concept_id);
'''

'''
-- Create the new table with the first row per concept_id

CREATE TABLE rdas_db.grant_project_annotation_unique AS

    WITH RankedRows AS (
        SELECT  
            concept_id,  umls_concept,  umls_cui,  semantic_types,  semantic_type_names,  aliases,  definition,
            ROW_NUMBER() OVER (PARTITION BY concept_id ORDER BY concept_id) AS rn
        FROM rdas_db.grant_project_annotation
    )

    SELECT 
        concept_id,  umls_concept, umls_cui,  semantic_types,  semantic_type_names,  aliases, definition
    FROM RankedRows
    WHERE rn = 1;
'''
# ??? filter out some rows/concept_id by semantic_type_names ??? Ask Qian Zhu
# End of step three 


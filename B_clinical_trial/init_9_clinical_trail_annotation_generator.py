import os
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import spacy
import scispacy
from scispacy.linking import EntityLinker

# --- Installation Check and Model Loading ---
# Ensure you've installed all necessary scispaCy models:
# pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.3/en_core_sci_lg-0.5.3.tar.gz
# pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.3/en_ner_bionlp13cg_md-0.5.3.tar.gz
# pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.3/en_ner_bc5cdr_md-0.5.3.tar.gz
 
from baseclass.conn import DBConnection as db
from utils.tools import ask_to_continue, _val, _normalize_txt, _id_range_generator, _append_to_file
import mysql.connector # Make sure this import is present at the top of your file

from colorama import init, Fore, Style
init()
 

def success_loaded(message):
    print(Fore.BLUE+ f'{message} loaded successfully.\n'+ Style.RESET_ALL)

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
    Removes duplicate annotations from a list of dictionaries based on 'nctid'
    and 'concept_id', keeping the entry with the highest 'score'.
    Args:
        annotations_list (list): A list of dictionaries, where each dictionaryrepresents an annotation.
    Returns:
        list: A new list containing only the unique annotations.
    """
    unique_annotations = {} # A dictionary to store unique annotations
                            # Key: (nctid, concept_id)
                            # Value: The annotation dictionary with the highest score

    for annotation in annotations_list:
        nctid = annotation['nctid']
        concept_id = annotation['concept_id']
        score_str = annotation['score']

        # Convert score to float for comparison
        try:
            current_score = float(score_str)
        except ValueError:
            print(f"Warning: Could not convert score '{score_str}' to float for nctid {nctid}, concept_id {concept_id}. Skipping this annotation for score comparison.")
            continue # Skip this annotation if score is invalid

        key = (nctid, concept_id)

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

 


def save_processed_annotations_to_db(processed_annotations, conn):
    
    insert_cursor = conn.cursor(buffered=True) 

    if processed_annotations:
       
        insert_query = """
            INSERT INTO clinical_trial_annotation (
                nctid,  concept_id, score, umls_concept, 
                umls_cui, semantic_types, semantic_type_names, aliases, definition
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        # Ensure data is in the correct ORDER for the %s placeholders
        data_to_insert = [
            (ann['nctid'],  ann['concept_id'], ann['score'], ann['umls_concept'], 
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

          
def process_data_by_range(conn, start_id, end_id, nlp_bionlp, nlp_bc5cdr, bionlp_linker, bc5cdrlinker, bionlp_semantic_type_tree, bc5cdr_semantic_type_tree) :

    dict_cursor = conn.cursor(dictionary=True, buffered=True)

    # Use table clinical_trial_unique
    try: 

        query = f'''
            SELECT nctid, studies
            FROM clinical_trial_unique
            WHERE id BETWEEN {start_id} AND {end_id} 
            AND (annotation_processed is null or annotation_processed !={PROCESSED_FLAG})
        '''

        dict_cursor.execute(query)
        rows = dict_cursor.fetchall() # Fetch all results immediately

        if not rows:
            print(f'{Fore.YELLOW}Already processed for range [{start_id}-{end_id}].{Style.RESET_ALL}')
            return
 
        nctid_list = []
        description_list = []
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

        if len(nctid_list) == 0:
            print(f'{Fore.YELLOW}No description found for range [{start_id}-{end_id}].{Style.RESET_ALL}')
            return

        processed_annotations_1 = process_description_text(nlp_bionlp, bionlp_linker, bionlp_semantic_type_tree, nctid_list, description_list)
        print(f'en_ner_bionlp13cg_md generated: {len(processed_annotations_1)} annotations')

        processed_annotations_2 = process_description_text(nlp_bc5cdr, bc5cdrlinker, bc5cdr_semantic_type_tree, nctid_list, description_list)
        print(f'en_ner_bc5cdr_md generated: {len(processed_annotations_2)} annotations')

        processed_annotations = processed_annotations_1 + processed_annotations_2
        print(f'Total generated: {len(processed_annotations)} annotations')

        '''
        The two processes may generate the enities with same concept_id (with nctid) but different score, remove the duplicates with the lower score.
        '''
        processed_annotations = remove_duplicate_annotations(processed_annotations)
        print(f'After removing duplicates: {len(processed_annotations)} annotations')
       
    except mysql.connector.Error as err:
        print(f"{Fore.RED}Database query error for range [{start_id}-{end_id}]: {err}{Style.RESET_ALL}")
        return None
 
    if dict_cursor:
        dict_cursor.close()
 
    return processed_annotations



def process_description_text(nlp, linker, semantic_type_tree, nctid_list, description_list):

    processed_annotations = []
    try:
        for i, doc in enumerate(nlp.pipe(description_list, disable=["parser", "attribute_ruler", "lemmatizer"])):
            
            current_app_id = nctid_list[i] 
            print(f"  Processing nctid: {current_app_id}")

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

        

PROCESSED_FLAG = 2

# Step two starts here
if __name__ == '__main__':
    
    ok = ask_to_continue(f'*** Generate the Annotation data for Clinical Trail ? *** ')
    if not ok:
        sys.exit(Fore.RED + '\n'+ '-'*50 +  '  Exit  ' + '-'*50 + '\n'+ Style.RESET_ALL)
  
    nlp_bionlp, bionlp_linker, nlp_bc5cdr, bc5cdrlinker = load_models()
     
    bionlp_semantic_type_tree = bionlp_linker.kb.semantic_type_tree # Access semantic type tree
    bc5cdr_semantic_type_tree = bc5cdrlinker.kb.semantic_type_tree
     
    #SELECT min(id), max(id)  FROM rdas_db.clinical_trial_unique;
    min_id = 378093 #3
    max_id = 378099+10
    
    step = 1
    batch_size = 30 
    
    # Generate ID ranges
    id_ranges = list(_id_range_generator(min_id, max_id, step, batch_size))
    
    '''
    Using 'with' for connection management
    It's generally safer to create a new cursor for each main operation or at least ensure the cursor is fully consumed.
    Using dictionary=True for easier access to columns by name. buffered=True is essential for fetching all results into memory.
    '''
    with db().mysql_conn() as conn: 
        
        update_cursor = conn.cursor(buffered=True)

        for start_id, end_id in id_ranges:

            print(f'\n{Fore.CYAN}Processing ID range: [{start_id}-{end_id}]{Style.RESET_ALL}')

            processed_annotations = process_data_by_range(conn, start_id, end_id, nlp_bionlp, nlp_bc5cdr, bionlp_linker, bc5cdrlinker, bionlp_semantic_type_tree, bc5cdr_semantic_type_tree) 
             
            if processed_annotations:
                #
                for ann in processed_annotations:
                    print(ann)

                save_processed_annotations_to_db(processed_annotations, conn)

            # Mark as already processed
            update_cursor.execute(f"UPDATE clinical_trial_unique SET annotation_processed = {PROCESSED_FLAG} WHERE id BETWEEN {start_id} AND {end_id}")
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


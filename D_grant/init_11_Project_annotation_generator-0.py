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


from baseclass.conn import DBConnection as db
from utils.tools import ask_to_continue, _val, _normalize_txt, _id_range_generator, _append_to_file
import mysql.connector # Make sure this import is present at the top of your file

from colorama import init, Fore, Style
init()
 
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

##############################################################################################################
# Deprecated
##############################################################################################################

def load_models():
    try:
        # Load the base scispaCy model
        nlp = spacy.load("en_core_sci_lg")
        print(Fore.BLUE+ "Model 'en_core_sci_lg' loaded successfully.\n"+ Style.RESET_ALL)
        print(f"Initial pipeline components: {nlp.pipe_names}"+ Style.RESET_ALL)

        # --- Step 1: Ensure the EntityLinker is added FIRST ---
        # Determine the actual name of the linker component once added to the pipeline
        linker_component_name = "scispacy_linker" # scispacy's factory name

        if linker_component_name not in nlp.pipe_names:
            linker = nlp.add_pipe(linker_component_name, config={"linker_name": "umls"})
            print(Fore.BLUE+ f"ScispaCy EntityLinker added to the pipeline with UMLS knowledge base (as '{linker_component_name}')."+ Style.RESET_ALL)
        else:
            linker = nlp.get_pipe(linker_component_name)
            print(Fore.LIGHTRED_EX+ f"ScispaCy EntityLinker ('{linker_component_name}') already in pipeline."+ Style.RESET_ALL)

        # --- Step 2: Load specialized NER models and add them to the pipeline ---
        # Load the specialized NER models, disabling their full pipelines
        ner_bionlp = spacy.load("en_ner_bionlp13cg_md", disable=["tok2vec", "tagger", "parser", "attribute_ruler", "lemmatizer"])
        print(Fore.BLUE+ "Model 'en_ner_bionlp13cg_md' loaded successfully.\n"+ Style.RESET_ALL)

        ner_bc5cdr = spacy.load("en_ner_bc5cdr_md", disable=["tok2vec", "tagger", "parser", "attribute_ruler", "lemmatizer"])
        print(Fore.BLUE+ "Model 'en_ner_bc5cdr_md' loaded successfully.\n"+ Style.RESET_ALL)

        # Add the specialized NER components.
        # Use the actual name of the linker component for correct positioning.
        nlp.add_pipe("ner", source=ner_bionlp, before=linker_component_name, name="bionlp_ner")
        nlp.add_pipe("ner", source=ner_bc5cdr, before=linker_component_name, name="bc5cdr_ner")

        print(Fore.GREEN+ f"Final pipeline components: {nlp.pipe_names}"+ Style.RESET_ALL)

        # --- Get the UMLS Semantic Type Tree from the linker ---
        # The linker has a reference to the loaded UMLS knowledge base,
        # which in turn has access to the semantic type tree.
        # Note: This file (semantictypes.tsv) is typically part of your scispacy installation
        # and is used to build the semantic type tree.

        #semantic_type_tree = linker.kb.semantic_type_tree
        #print(Fore.GREEN+ "UMLS Semantic Type Tree loaded successfully."+ Style.RESET_ALL)

    except OSError as e:
        print(Fore.RED+ f"Error loading scispaCy models: {e}"+ Style.RESET_ALL)
        print(Fore.RED+ "Please ensure all models are installed correctly using the pip commands provided in the comments."+ Style.RESET_ALL)
        exit()
    except ValueError as e:
        print(Fore.RED+ f"Pipeline configuration error: {e}"+ Style.RESET_ALL)
        print(Fore.RED+ "This might be due to incorrect component names or order."+ Style.RESET_ALL)
        exit()
        
    return nlp

 


def save_processed_annotations_to_db(processed_annotations, mysql):
    
    insert_cursor = mysql.cursor(buffered=True) 

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
            mysql.commit() # Commit changes after successful batch insert

            print(f"{Fore.BLUE}  Stored {len(processed_annotations)} annotations for range [{start_id}-{end_id}].{Style.RESET_ALL}")
        except mysql.connector.Error as err:
            print(f"{Fore.RED}Database INSERT error for range [{start_id}-{end_id}]: {err}{Style.RESET_ALL}")
            mysql.rollback() # Rollback on error

        if insert_cursor:
            insert_cursor.close()

          
def process_by_range(start_id, end_id, nlp, linker, semantic_type_tree):

    dict_cursor = mysql.cursor(dictionary=True, buffered=True)

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

        dict_cursor.execute(query)
        rows = dict_cursor.fetchall() # Fetch all results immediately

        if not rows:
            print(f'{Fore.YELLOW}Skip or No new abstracts found for range [{start_id}-{end_id}].{Style.RESET_ALL}')
            return
 
        application_ids = [row['application_id'] for row in rows]
        abstract_texts = [row['abstract_text'] for row in rows] # Now 'abstract_text' key should work

        processed_annotations = []

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

                    except KeyError:
                        print(f"{Fore.YELLOW}Warning: Concept ID '{concept_id}' not found for '{ent.text}'.{Style.RESET_ALL}")
                        continue

        if not processed_annotations:
            print(f"{Fore.MAGENTA}  No new annotations generated for range [{start_id}-{end_id}].{Style.RESET_ALL}")
            return None

    except mysql.connector.Error as err:
        print(f"{Fore.RED}Database query error for range [{start_id}-{end_id}]: {err}{Style.RESET_ALL}")
        return None
    except Exception as e:
        print(f"{Fore.RED}Error during NLP processing for range [{start_id}-{end_id}]: {e}{Style.RESET_ALL}")  
        return None
 
    if dict_cursor:
        dict_cursor.close()
 
    return processed_annotations


##############################################################################################################
# Deprecated
##############################################################################################################
        
if __name__ == '__main__':
    
    ok = ask_to_continue(f'*** Generate the Annotation data for Grant.Project ? *** ')
    if not ok:
        sys.exit(Fore.RED + '\n'+ '-'*50 +  '  Exit  ' + '-'*50 + '\n'+ Style.RESET_ALL)
 
    nlp = load_models()
    linker = nlp.get_pipe("scispacy_linker") # Get reference to the linker in this process
    semantic_type_tree = linker.kb.semantic_type_tree # Access semantic type tree


    min_id = 1
    max_id = 388186
    step = 1
    batch_size = 10  
    
    # Generate ID ranges
    id_ranges = list(_id_range_generator(min_id, max_id, step, batch_size))
  
    with db().mysql_conn() as mysql: # Using 'with' for connection management
        # It's generally safer to create a new cursor for each main operation
        # or at least ensure the cursor is fully consumed.
        # Using dictionary=True for easier access to columns by name.
        # buffered=True is essential for fetching all results into memory.
        
        for start_id, end_id in id_ranges:

            print(f'\n{Fore.CYAN}Processing ID range: [{start_id}-{end_id}]{Style.RESET_ALL}')

            processed_annotations = process_by_range(start_id, end_id, nlp, linker, semantic_type_tree)

            if processed_annotations:
                save_processed_annotations_to_db(processed_annotations, mysql)


    if mysql:
        mysql.close()

    # Connections and cursors will be closed automatically by 'with' statement
    print(f'{Fore.BLUE}\n'+'=**='*15 + ' All Done  '+'=**='*15 + '\n'+ Style.RESET_ALL)
 
    sys.exit()
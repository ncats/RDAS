import requests
import spacy
import re
import os
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.conn import DBConnection as db 
from utils.tools import ask_to_continue

nlp = spacy.load("en_core_web_sm")
api_key = '506f8603-e6ea-48c1-8a70-9fb26f7a7f48'
semantic_type_list = ['Disease or Syndrome', 'Neoplastic Process', 'Mental or Behavioral Dysfunction',
                  'Injury or Poisoning',  'Congenital Abnormality', 'Acquired Abnormality', 'Environmental Factor',
                  'Organism', 'Physiologic Function']



publication_article = 'publication_article'
publication_gard_searchterm_pubmed_mapping = 'publication_gard_searchterm_pubmed_mapping'

# Manually set is_abbreviation column --------------------------------------------------------------------------------
f'''
    UPDATE rdas_db.{publication_gard_searchterm_pubmed_mapping} pgs
    JOIN rdas_db.gard g 
        ON pgs.search_term = g.Label
    SET pgs.is_abbreviation = 1
    WHERE g.Label_Predicate_Mapping LIKE 'ABBRE%';
'''
#---------------------------------------------------------------------------------------------------------------------


f"""
    Update the {publication_gard_searchterm_pubmed_mapping} table, set the is_valid=1, if is_abbrevation=1 and search_term is a VALID abbreviation.
"""
class ArticleFalsPositiveValidator:

    """ This validated results will be reflected in publication - GARD relationship """

    def __init__(self):
        pass
        

    def find_first_sentence(self, text, abbreviation):
        # Create a pattern that will match both 'FLNMS' and any variant like 'F-LNMs' or 'F-LNMS'
        pattern = re.escape(abbreviation)  # Escape the abbreviation to handle special characters

        pattern = pattern.replace('FLNMS', r'F[-]LNMs')  # Make it flexible to match 'F-LNMs' and 'FLNMS'

        # Split the text into sentences
        sentences = re.split(r'(?<=\.|\!|\?)\s+', text)
        # Loop through the sentences and check if the abbreviation is present in each
        for sentence in sentences:
            if re.search(pattern, sentence):  # Use regex to find the abbreviation (accounting for variations)
                return sentence.strip()  # Return the first sentence that contains the abbreviation
            
        return None
        #return f"The abbreviation '{abbreviation}' was not found in any sentence."


    def find_full_name_of_abbreviation(self, text, abbreviation):
        # Create a pattern to find the full name of the abbreviation in the format "Full Name (Abbreviation)"
        # This will match patterns where the abbreviation is inside parentheses, considering "i.e." usage
        pattern = re.compile(r'([A-Za-z\s\-]+)\s*\(([^)]+)\)\s*(?:i\.e\.\s*([^;]+))?')
        # Search for the pattern in the text
        match = re.search(pattern, text)
        if match:
            # Return the full name of the abbreviation
            return match.group(1).strip()  # Full name (group 1)
        else:
            return None
            #return f"The abbreviation '{abbreviation}' was not found in the text."
        

    def extract_noun_phrases(self, sentence):
        # Process the sentence using spaCy
        doc = nlp(sentence)
        # Extract noun phrases
        noun_phrases = [chunk.text for chunk in doc.noun_chunks]
        return noun_phrases


    def get_cui(self, term):

        search_url = f"https://uts-ws.nlm.nih.gov/rest/search/current?string={term}&searchType=exact&apiKey={api_key}"
        response = requests.get(search_url)
        if response.status_code == 200:
            data = response.json()
            # Check if there are any results and get the first CUI
            if data.get("result") and data["result"].get("results"):
                # Return the CUI of the first result
                cui = data["result"]["results"][0]["ui"]
            #  print(f"CUI for '{term}': {cui}")
                return cui
            else:
                return None
                #raise Exception(f"No results found for the term: {term}")
        else:
            print((f"Error searching for term: {response.status_code}, {response.text}"))
            return None
            #raise Exception(f"Error searching for term: {response.status_code}, {response.text}")
        

    # Step 3: Function to get semantic types for a given CUI
    def get_semantic_types(self, cui):

        # Use the semantictypes API to get semantic types for the CUI
        semantic_type_url = f"https://uts-ws.nlm.nih.gov/rest/content/current/CUI/{cui}?apiKey={api_key}"
        # Send the GET request to retrieve semantic types for the CUI
        response = requests.get(semantic_type_url)

        if response.status_code == 200:

            data = response.json()
            # Access the 'semanticTypes' list inside 'result'
            if 'semanticTypes' in data.get("result", {}):
                semantic_types = data["result"]["semanticTypes"]
                # Extract and return only the 'name' of each semantic type
                semantic_type_names = [semantic_type['name'] for semantic_type in semantic_types]
                return semantic_type_names
            else:
                return None
                #raise Exception(f"No semantic types found for CUI: {cui}")
        else:
            print(f"Error retrieving semantic types: {response.status_code}, {response.text}")
            #raise Exception(f"Error retrieving semantic types: {response.status_code}, {response.text}")
            return None
    

    def is_a_disease_by_Spacy(self, term):
        # Process the text using SpaCy's NLP pipeline
        doc = nlp(term)

        # Step 1: Check for disease-related entities in the recognized entities 
        for ent in doc.ents:
            print(f'ent = {ent}')
            # Check if the entity type is medical or disease-related
            if ent.label_ in ["DISEASE", "DISORDER", "SYMPTOM", "MEDICAL_CONDITION", "PATHOLOGY"]:
                print(f"Entity '{ent.text}' is recognized as a disease-related entity: {ent.label_}.")
                return True # is a disease
            
        return False


 
    def verify(self, searching_term, abstract):
        if not abstract:
            return False
        
        first_sent = self.find_first_sentence(abstract.lower(), searching_term)

        if not first_sent: 
            return False

        print("first_sent:", first_sent)

        result = self.find_full_name_of_abbreviation(first_sent, searching_term)
        print("full_name_of_abbreviation:", result)
            
        if not result: 
            return False
            
        noun_phrases = self.extract_noun_phrases(result)
        if not noun_phrases  or len(noun_phrases) <= 0:
            return False
        
        # The last noun phrase
        last_noun_phrase = noun_phrases[-1]
        print("Last Noun Phrase:", last_noun_phrase)

        # Step 1: Get the CUI for the term
        cui = self.get_cui(last_noun_phrase)
        print(f'cui = {cui}')
        if cui:
            # Step 2: Get the semantic types for the CUI
            semantic_types = self.get_semantic_types(cui)
            print(f'semantic_types = {semantic_types}')
            if semantic_types:            
                for semantic_type in semantic_types:
                    if semantic_type in semantic_type_list:
                        return True
        else:
            is_disease = self.is_a_disease_by_Spacy(last_noun_phrase)
            return is_disease
    
        

if __name__ == "__main__": 
     
    # Check
    f''' SELECT * FROM rdas_db.{publication_gard_searchterm_pubmed_mapping} where is_abbreviation =1; '''


    ok = ask_to_continue('Validate the false postive? (Is a valid abbreviation?)')
    if not ok:
        sys.exit('------Stopped ------')
 

    fetch_conn = db().mysql_conn()
    update_conn = db().mysql_conn()
    update_cursor = update_conn.cursor(buffered=True)
    fetch_cursor = fetch_conn.cursor(dictionary=True, buffered=True)
    
    validator = ArticleFalsPositiveValidator()
 

    select_query = f'''
        SELECT gsp.id, gsp.gard_id, gsp.search_term, gsp.pubmed_id, a.abstract_text 
        FROM  
            rdas_db.{publication_gard_searchterm_pubmed_mapping} gsp, 
            rdas_db.{publication_article} a 
        WHERE  (gsp.is_abbreviation = 1 and gsp.is_valid is null)
        AND gsp.pubmed_id=a.pubmed_id
    '''

    update_is_valid_sql = f"update rdas_db.{publication_gard_searchterm_pubmed_mapping} set is_valid=%s where id=%s"
 
    fetch_cursor.execute(select_query)

    total = 0
    batch_num = 0

    while True:
        try:               
            rows = fetch_cursor.fetchmany(100)
            
            if not rows:
                print('\n\n---------------- All finished, no more data ----------------\n\n')
                break

            batch_num += 1
            is_valid_tuple_list = [] 
            print(f'Batch#: {batch_num}')

            for row in rows:
                id = row['id']
                gard_id = row['gard_id']
                search_term = row['search_term']
                pubmed_id = row['pubmed_id']
                abstract = row['abstract_text']

                is_valid = validator.verify(search_term, abstract)
                is_valid_tuple_list.append((1 if is_valid else 0, id))

                total += 1
                print(f'total = {total}')

            # update  
            try:
                if len(is_valid_tuple_list) <= 0:
                    continue 

                
                update_cursor.executemany(update_is_valid_sql, is_valid_tuple_list)
                update_conn.commit()

                print(update_cursor.rowcount, "record(s) affected")

            except Exception as e:
                print(e)
                break

        except Exception as e: 
            print(e)
            break



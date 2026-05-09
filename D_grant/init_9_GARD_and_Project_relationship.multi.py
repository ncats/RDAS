import os
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import re
import nltk
print(f'nltk version: {nltk.__version__}')
print(f'nltk.data.path: {nltk.data.path}')
# Manually install punkt and stopwords in /Users/zhaot3/nltk_data
# cd /Users/zhaot3/nltk_data
# wget --no-check-certificate https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages/tokenizers/punkt.zip
# wget --no-check-certificate https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages/corpora/stopwords.zip
# wget --no-check-certificate https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages/corpora/english_wordnet.zip
'''
nltk.download('wordnet')
nltk.download('punkt')
nltk.download('stopwords')
'''
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize

from sentence_transformers import SentenceTransformer, util
from transformers import AutoTokenizer, AutoModel
import torch

import spacy
nlp = spacy.load("en_core_web_sm")

from dotenv import load_dotenv
load_dotenv()

from colorama import init, Fore, Style
# Initialize colorama for Windows compatibility
init()

from multiprocessing import Pool, Manager, Value
from functools import partial
import itertools

import ast
import math
from baseclass.conn import DBConnection as db
from utils.tools import ask_to_continue, _val, _normalize_tuple, _stem_text, _remove_stop_words, _id_range_generator, _append_to_file

"""
    *Core Purpose *
    The primary goal of this script is to identify and quantify the relevance of various GARD diseases to NIH grant projects. 
    It does this by performing sophisticated text analysis on different parts of grant applications (project titles, public health relevance statements, 
    and abstract texts) to find mentions of GARD disease names and their synonyms.
"""

# 1. Find and see the duplicate APPLICATION_ID in grant_abstract
'''
    SELECT application_id FROM rdas_db.grant_abstract GROUP BY application_id HAVING COUNT(1) > 1
    SELECT * FROM rdas_db.grant_abstract  where APPLICATION_ID in (7916889, 10200508, 10224557, 10410101, 10711865, 10817330, 10991546, 10993253 ) order by APPLICATION_ID;
'''

# 2. Create indexes on application_id and year on 'grant_abstract', as well as on 'grant_project'
'''
    ALTER TABLE `rdas_db`.`grant_abstract` 
    ADD INDEX `idx_grant_abstract_year` (`YEAR` ASC) VISIBLE,
    ADD INDEX `idx_grant_abstract_app_id` (`APPLICATION_ID` ASC) VISIBLE,
    ADD INDEX `idx_grant_abstract_app_id_yr` (`APPLICATION_ID` ASC, `YEAR` ASC) VISIBLE;
'''

# 3. No duplicate APPLICATION_ID in grant_project. Use 'p.FY=a.YEAR' to de-duplicate APPLICATION_ID in grant_abstract
'''
SELECT p.APPLICATION_ID, p.FY, p.PROJECT_TITLE, p.PHR, a.ABSTRACT_TEXT 
FROM rdas_db.grant_project p, rdas_db.grant_abstract a
where p.APPLICATION_ID=a.APPLICATION_ID and p.FY=a.YEAR
'''

# 4. Check results
''' 
select gard_id, count(gard_id) FROM rdas_db.grant_gard_project_relation group by gard_id order by count(gard_id) desc;
select application_id, count(application_id) FROM rdas_db.grant_gard_project_relation group by application_id order by count(application_id) desc;


select application_id,gard_id, count(*) FROM rdas_db.grant_gard_project_relation group by application_id,gard_id order by count(*) desc;
select application_id,gard_id, count(*) FROM rdas_db.grant_gard_project_relation group by application_id,gard_id having count(*)>1 order by count(*) desc;
'''

word_pattern = re.compile(r'\b\w+\b')

hf_token = os.getenv("HUGGINGFACE_TOKEN")

# ---------------------------------------------------------------------------------------------------------------------------------------------------------

def split_sentence(sentence):    
    words = word_pattern.findall(sentence)# Use the pre-compiled pattern for splitting
    return words

def word_matching(text, word):
   for i in  split_sentence(word):
     if i not in text:
        return False
   return True

def get_gard_title(text, list_chcek):

    if list_chcek in ['Synonyms_stem','Synonyms_sw_stem','Synonyms_stem_bow','Synonyms_sw_stem_bow']: 
        text1=_stem_text(text.lower())
    elif list_chcek in [ 'Synonyms_sw_nltk']  :          
        text1=_remove_stop_words(text.lower())
    else:                                                  
        text1=text.lower()

    #print(text1)
    text2=split_sentence(text1)
    #print(text2)
    
    out=dict()
    list_chcek = list_chcek.lower()
    for gard in gard_processed_names:

        gardName = gard['name'] 
        gard_names_to_check = gard[list_chcek]
        
        for _name in gard_names_to_check:
            if _name in text1 and word_matching(text2, _name):
                count = text2.count(_name) if len(_name.split()) == 1 else text1.count(_name)

                out[gardName] = [out[gardName][0] + count] if gardName in out else [count] 
                
    return out


def get_gard_title_stem_exact(text):
    exact_matching = get_gard_title(text, 'Synonyms_sw_bow') or {}
    stemming_check = get_gard_title(text, 'Synonyms_sw_stem_bow') or {}
    combined_dict = {**exact_matching, **stemming_check}  # Merge dictionaries
    #print(f'combined_dict\n{combined_dict}')

    # Remove keys that are part of another key
    keys_to_remove = {key1 for key1 in combined_dict for key2 in combined_dict if key1 != key2 and key1 in key2}
    combined_dict = {key: 1 for key in combined_dict if key not in keys_to_remove}

    return combined_dict or None


def is_about_term(input_text, target_term):
    # Load ClinicalBERT model and tokenizer
    model_name = "emilyalsentzer/Bio_ClinicalBERT"
    # ClinicalBERT: "emilyalsentzer/Bio_ClinicalBERT"
    tokenizer = AutoTokenizer.from_pretrained(model_name, token=hf_token)
    model = AutoModel.from_pretrained(model_name, token=hf_token)
    # Tokenize input text and target term
    input_tokens = tokenizer(input_text, return_tensors="pt", padding=True, truncation=True, max_length=512)
    term_tokens = tokenizer(target_term, return_tensors="pt", padding=True, truncation=True, max_length=512)
    # Get embeddings from ClinicalBERT
    with torch.no_grad():
        input_embedding = model(**input_tokens).last_hidden_state.mean(dim=1)
        term_embedding = model(**term_tokens).last_hidden_state.mean(dim=1)
    # Calculate cosine similarity between text and term
    similarity = util.pytorch_cos_sim(input_embedding, term_embedding)
    # Define a threshold for similarity
    similarity_threshold = 0.7
    # Return True if similarity is above the threshold, indicating the text is about the term
    return similarity.item() #> similarity_threshold

 
def normalize(x):
   if x < 20:
       return math.log(x) / math.log(20)
   else:
    return 1
   
def normalize_combined_dictionary(input_text,title_,dict1, dict2, dict3, dict4,min_, max_,type):
    if    type =='title':      factor=20
    elif  type =='statement':  factor=2
    else: factor=1
    dict1 = {key: value * 5 for key, value in dict1.items()}
    # Make the values of the second dictionary two times
    dict2 = {key: value * 7 for key, value in dict2.items()}
    dict3 = {key: value * 3 for key, value in dict3.items()}
    # Combine all dictionaries
    combined_dict = {key: dict1.get(key, 0) + dict2.get(key, 0) + dict3.get(key, 0) + dict4.get(key, 0) for key in set(dict1) | set(dict2) | set(dict3) | set(dict4)}
    # Normalize the values of the combined dictionary
    total_frequency = sum(combined_dict.values())
    # Check if total_frequency is zero to avoid division by zero
    if total_frequency == 0:
        return {}
    normalized_dict = {key: value   for key, value in combined_dict.items()}
    result_dict = {}
    for key, value in normalized_dict.items():
        #if  is_about_term(input_text.lower(), key) >=0.5:
        #sen_has_gard=get_sen(input_text.lower(), key,title_)

        #!!! No SourceDescription in the new code, just let defin=key
        #defin=get_def(key)
        defin = key
        try:
          #result_dict[key] = [20 if  type =='title' else 1+(factor*value //2), is_about_term(sen_has_gard,  defin), is_about_term(input_text.lower(),  defin), sen_has_gard]
          result_dict[key] = [normalize(20 if  type =='title' else 1+(factor*value //2)),  is_about_term(input_text.lower(),  defin)]

        except:
          try:
              #result_dict[key] = [20 if  type =='title' else 1+ (factor*value //2), is_about_term(sen_has_gard[:2000],  defin[:2000]), is_about_term(input_text.lower()[:2000],  defin[:2000]), sen_has_gard]
              result_dict[key] = [normalize(20 if  type =='title' else 1+ (factor*value //2)), is_about_term(input_text.lower()[:2000],  defin[:2000])]
          except:
              try:
                  result_dict[key] = [normalize(20 if  type =='title' else 1+ (factor*value //2)) ,  is_about_term(input_text.lower()[:1500],  defin[:1500])]
                  #result_dict[key] = [20 if  type =='title' else 1+ (factor*value //2) , is_about_term(sen_has_gard[:1500],  defin[:1500]), is_about_term(input_text.lower()[:1500],  defin[:1500]), sen_has_gard]

              except:
                  #result_dict[key] = [20 if  type =='title' else 1+ (factor*value //2) , is_about_term(sen_has_gard[:1000],   defin[:1000]), is_about_term(input_text.lower()[:500],  defin[:1000]), sen_has_gard]
                  result_dict[key] = [normalize(20 if  type =='title' else 1+ (factor*value //2)) , is_about_term(input_text.lower()[:500],  defin[:1000])]

    return result_dict


# Function to determine verb tense
def get_verb_tense(verb):
    if "VBD" in verb.tag_:
        return "past"
    elif ("MD" in verb.tag_ and "will" in verb.lemma_.lower()) or ('aim' in verb.lemma_.lower() ) :
        return "future"
    elif "VBP" in verb.tag_ or "VBZ" in verb.tag_:
        return "present"
    else:
        return "unknown"

# Function to determine if a sentence is negated
def is_sentence_negated(sentence):
    for token in sentence:
        if token.dep_ == "neg":
            return True
    return False


def check_sen(text):
  # Process the text
  doc = nlp(text)
  # Iterate over sentences in the document
  first_sentence = ''
  priority, future_positive, present_positive, positive='','','',''

  for i, sent in enumerate(doc.sents, 1):
    # Initialize a set to store unique tenses in the sentence
    sentence_tenses = set()
    # Iterate over tokens in the sentence
    for token in sent:
        # Check if the token is a verb
        if token.pos == spacy.symbols.VERB or token.pos == spacy.symbols.AUX:
            # Check the tense of the verb
            tense = get_verb_tense(token)
            sentence_tenses.add(tense)

    # Determine the overall tense of the sentence
    if is_sentence_negated(sent)==False and  ("past" not in sentence_tenses):
        if i == 1:    first_sentence = sent.text
        #positive+=sent.text
        elif  ("the goal of" in sent.text.lower()) or ("aim" in sent.text.lower()):
            priority+=sent.text
        elif "future" in sentence_tenses:
           future_positive+=sent.text
        elif "present" in sentence_tenses and is_sentence_negated(sent)==False:
           present_positive+=sent.text
        if i == 1:    first_sentence = sent.text


  return first_sentence,  priority, future_positive, present_positive


def stem_text(text):
    # Initialize the Porter Stemmer
    stemmer = PorterStemmer()
    # Remove punctuation
    text_without_punctuation = re.sub(r'[^\w\s]', '', text)
    # Tokenize the text into words
    words = word_tokenize(text_without_punctuation)
    # Perform stemming on each word
    stemmed_words = [stemmer.stem(word) for word in words]
    # Join the stemmed words back into a single string
    stemmed_text = ' '.join(stemmed_words)
    return stemmed_text


def remove_stop_words(text):
    stop_words = set(stopwords.words('english'))
    words = word_tokenize(text)
    filtered_words = [word for word in words if word.lower() not in stop_words]
    return ' '.join(filtered_words)


def get_gard_abstract(text, list_chcek):
 
  if list_chcek in ['Synonyms_stem','Synonyms_sw_stem','Synonyms_stem_bow','Synonyms_sw_stem_bow']: 
      text1=stem_text(text.lower())
  elif list_chcek in [ 'Synonyms_sw_nltk']  :          
      text1=remove_stop_words(text.lower())
  else:                                                  
      text1=text.lower()
  text2=split_sentence(text1)

  out=dict()
  list_chcek = list_chcek.lower()
  
  for gard in gard_processed_names:
    gardName = gard['name'] 
    gard_names_to_check = gard[list_chcek]
    
    for _name in gard_names_to_check:
        if _name in text1 and word_matching(text2, _name):

            #count = text2.count(_name) if len(_name.split()) == 1 else text1.count(_name)
            #out[gardName] = [out[gardName][0] + count] if gardName in out else [count] 

            if len(_name.split()) == 1:
                count = text2.count(_name)
            else:
                count = text1.count(_name)
 
            if gardName in out:
                out[gardName] = [out[gardName][0] + count]
            else:
                out[gardName] = [count]
                 
  if out== {}: return None,None
  return out , ''#sen

def _sum_and_update(target_dict, source_dict):
    for key, value in source_dict.items():
        target_dict[key] = target_dict.get(key, 0) + sum(value)


def combine_dictionaries_count(dict1, dict2): 
    combined_dict = {}
    _sum_and_update(combined_dict, dict1)
    _sum_and_update(combined_dict, dict2)
    return combined_dict


 # Remove keys that are part of another key
def modified_dict(combined_dict):
    keys_to_remove = set()
    for key1 in combined_dict:
        for key2 in combined_dict:
            if key1 != key2 and (key1 in key2) and (combined_dict[key1] <= combined_dict[key2]):
                keys_to_remove.add(key1)
    for key in keys_to_remove:
        del combined_dict[key]
        
    return combined_dict


def get_gard_abstract_stem_exact(text):
  
  if text and isinstance(text, str):
    exact_matching, exact_matching_sen=get_gard_abstract(text, 'Synonyms_sw')
    #print(exact_matching)
    stemming_chcek, Stemming_chcek_sen=get_gard_abstract(text, 'Synonyms_sw_stem')
    #print(Stemming_chcek)
    if exact_matching is None: exact_matching = {}
    if stemming_chcek is None: stemming_chcek = {}
    #if exact_matching_sen is None:exact_matching_sen = {}
    #if Stemming_chcek_sen is None:Stemming_chcek_sen = {}

    combined_dict = combine_dictionaries_count(exact_matching,stemming_chcek)
    #combined_dict_sen= combine_dictionaries_sent(exact_matching_sen,Stemming_chcek_sen)
    # Remove keys that are part of another key
    combined_dict=modified_dict(combined_dict)#,combined_dict_sen)
    if combined_dict=={}:return {}
    return combined_dict
  return {}


def get_GARD_with_processed_names():

    def safe_split(value, delimiter='$$$'):
        return value.split(delimiter) if value is not None else []

    mysql = db().mysql_conn()
    dict_cursor = mysql.cursor(dictionary=True, buffered=True)

    query = ''' select gardid, name, synonyms, synonyms_sw, synonyms_sw_bow, synonyms_sw_stem, synonyms_sw_stem_bow from grant_gard_processed_names '''
    dict_cursor.execute(query)
    rows = dict_cursor.fetchall()

    processed_rows = [
        {
            **row,
            'synonyms': safe_split(row['synonyms']),
            'synonyms_sw': safe_split(row['synonyms_sw']),
            'synonyms_sw_bow': safe_split(row['synonyms_sw_bow']),
            'synonyms_sw_stem': safe_split(row['synonyms_sw_stem']),
            'synonyms_sw_stem_bow': safe_split(row['synonyms_sw_stem_bow'])
        }
        for row in rows
    ]

    dict_cursor.close()
    mysql.close()

    return processed_rows

# Global ---
gard_processed_names = get_GARD_with_processed_names()

def get_GARD_id_by_name(gard_name):
    for gard in gard_processed_names:
        if gard['name'] == gard_name:
            return gard['gardid']
    return None


def process_text_and_normalize(text, project_title, weight_start, weight_end, source_type):
    """
    Processes a given text, extracts information, normalizes it, and returns the result dictionary.
    Args:
        text (str): The text to process (e.g., public_health_relevance_statement or abstract_text).
        project_title (str): The title of the project.
        weight_start (float): The starting weight for normalization.
        weight_end (float): The ending weight for normalization.
        source_type (str): The type of the text source (e.g., 'statement').
    Returns:
        dict: The normalized dictionary if processing was successful and the dictionary is not empty,otherwise None.
    """
    if text and not text.isspace():

        first_sentence, priority, future_positive, present_positive = check_sen(text)

        name1 = get_gard_abstract_stem_exact(first_sentence)
        name2 = get_gard_abstract_stem_exact(priority)
        name3 = get_gard_abstract_stem_exact(future_positive)
        name4 = get_gard_abstract_stem_exact(present_positive)

        result_dict = normalize_combined_dictionary(text, project_title, name1, name2, name3, name4, weight_start, weight_end, source_type)

        if result_dict and result_dict != {}:
            return result_dict
    return None



def project_gard_relationship(project_title, public_health_relevance_statement, abstract_text):

    if all(isinstance(arg, str) for arg in [project_title, public_health_relevance_statement, abstract_text]):
        return None, ''

    #print('# 1. Processing project_title')
    if project_title and not project_title.isspace():

        name_dict = get_gard_title_stem_exact(project_title) 
        #print(f'name_dict\n{name_dict}')
        
        if name_dict:
            if abstract_text:
                result_dict = normalize_combined_dictionary(abstract_text, project_title, name_dict, {},{},{}, 1, 1, 'title')
            else: 
                result_dict =  normalize_combined_dictionary(project_title, project_title, name_dict, {},{},{}, 1, 1, 'title')
            #print(f'result_dict\n{result_dict}')

            return result_dict, 'title'
        
    
    #print('# 2. Processing public_health_relevance_statement')
    if public_health_relevance_statement and not public_health_relevance_statement.isspace():
        result = process_text_and_normalize(public_health_relevance_statement, project_title, 0.7, 0.9, 'statement')
        #print(f'{result}')

        if result:
            return result, 'statement'


    #print('# 3. Processing abstract_text')
    if abstract_text and not abstract_text.isspace():
        result = process_text_and_normalize(abstract_text, project_title, 0, 0.7, 'abstract')
        #print(f'{result}')

        if result:
            return result, 'abstract'
    
    return None, ''


LOG_FILE_PATH = 'logs/grant_GARD_Project_relation_process.log'

def process_id_range(id_range, batch_size=100):

    """Process a single ID range (start_id, end_id) in a separate process."""
    # Initialize MySQL connection for this process
    mysql = db().mysql_conn()
    dict_cursor = mysql.cursor(dictionary=True, buffered=True)
    insert_cursor = mysql.cursor(buffered=True)

    start_id, end_id = id_range

    # SQL query for the ID range
    query = f'''
        SELECT 
            p.id,  p.APPLICATION_ID,  p.FY,   p.PROJECT_TITLE,  p.PHR,  p.core_project_num,
            a.ABSTRACT_TEXT 
        FROM 
            rdas_db.grant_project p
            INNER JOIN rdas_db.grant_abstract a 
                ON p.APPLICATION_ID = a.APPLICATION_ID   AND p.FY = a.YEAR
            LEFT JOIN rdas_db.grant_gard_project_relation gpr 
                ON p.APPLICATION_ID = gpr.APPLICATION_ID
        WHERE 
            (p.id BETWEEN {start_id} AND {end_id}) 
            AND gpr.APPLICATION_ID IS NULL
    '''

    # Insert query
    insert = '''
        INSERT INTO grant_gard_project_relation (gard_id, application_id, gard_name, source_type, confidence_score, semantic_similarity, core_project_num, raw_result) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    '''

    dict_cursor.execute(query)
    rows = dict_cursor.fetchall()
 
    list_of_tuple = []

    for row in rows: 

        id = row['id']
        application_id = row['APPLICATION_ID']

        message = f'[{start_id}-{end_id}]:  id = {id}, application_id = {application_id}'
        print(message)

        _append_to_file(LOG_FILE_PATH, f'{message}')

        phr = row['PHR']
        project_title = row['PROJECT_TITLE']
        abstract_text = row['ABSTRACT_TEXT']

        result_dict, source_type = project_gard_relationship(project_title, phr, abstract_text)

        if result_dict:
            print(f'\tapplication_id: {application_id}, source_type: {source_type}')
            print(f'\t{result_dict}')

            for key, value in result_dict.items():
                gard_name = key
                confidence_score = value[0]
                semantic_similarity = value[1]
                core_project_num = _val(row['core_project_num'])
                raw_result = str(result_dict)
                gard_id = get_GARD_id_by_name(gard_name)

                list_of_tuple.append((gard_id, application_id, gard_name, source_type, confidence_score, semantic_similarity, core_project_num, raw_result))

    # Insert any remaining tuples
    if list_of_tuple:
        insert_cursor.executemany(insert, list_of_tuple)
        mysql.commit()

    # Clean up
    dict_cursor.close()
    insert_cursor.close()
    mysql.close()




if __name__ == '__main__':
 
    ok = ask_to_continue(f'*** Find relateionships between GARD and Grant Project? *** ')
    if not ok:
        sys.exit(Fore.RED + '\n------------------------ Stopped ------------------------\n'+ Style.RESET_ALL)
     
    min_id = 0 #2300000 #2110465 #2066200 #1774252#1670052#1584253 #765554 #730494 #72401 #0
    max_id = 2875061
    step = 1
    batch_size = 2000 #200
    num_processes = 20 #10  # Adjust based on your CPU cores and database capacity


    # Generate ID ranges
    id_ranges = list(_id_range_generator(min_id, max_id, step, batch_size))

    process_func = partial(process_id_range, batch_size=batch_size)

    # Process ID ranges in parallel
    with Pool(processes=num_processes) as pool:
        pool.map(process_func, id_ranges)

 
    print(Fore.BLUE + f'\n=**=**=**=**=**=**=**=**=**=**=**=**=**=**=**= All Done  =**=**=**=**=**=**=**=**=**=**=**=**=**=**=**=\n'+ Style.RESET_ALL)
   
    sys.exit()
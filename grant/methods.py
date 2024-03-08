import sys
import os
import sysvars
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import update_grant
import pandas as pd
from AlertCypher import AlertCypher
import requests
import json
import nltk
nltk.download('punkt')
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
import re
import ast
from itertools import permutations
from nltk.corpus import stopwords
nltk.download('stopwords')
import spacy
import csv
import pubmed.methods as rdas
from datetime import datetime
from sentence_transformers import SentenceTransformer, util
from transformers import AutoTokenizer, AutoModel
import torch
import glob

def start(db, restart_raw=False, restart_processed=False):
    update_grant.main(db, restart_raw=restart_raw, restart_processed=restart_processed)

def download_nih_data(restart_raw=False):
    current_year = int(datetime.today().year)

    print('Downloading NIH Exporter files')

    # Clinical Studies
    if restart_raw:
        os.remove(f'{sysvars.gnt_files_path}raw/clinical_studies/clinical_studies.csv')
    if not os.path.exists(f'{sysvars.gnt_files_path}raw/clinical_studies/clinical_studies.csv'):
        command = f'curl -L -X GET https://reporter.nih.gov/exporter/clinicalstudies/download -o {sysvars.gnt_files_path}raw/clinical_studies/clinical_studies.csv'
        os.system(command)
    else:
        print('Clinical Studies file already downloaded... bypassing')

    # Patents
    if restart_raw:
        os.remove(f'{sysvars.gnt_files_path}raw/patents/patents.csv')
    if not os.path.exists(f'{sysvars.gnt_files_path}raw/patents/patents.csv'):
        command = f'curl -L -X GET https://reporter.nih.gov/exporter/patents/download -o {sysvars.gnt_files_path}raw/patents/patents.csv'
        os.system(command)
    else:
        print('Patents file already downloaded... bypassing')

    types = ['abstracts','linktables','projects','publications']
    for type in types:
        if type == 'linktables':
            file_dir = 'link_tables'
        else:
            file_dir = type

        if restart_raw:
            cur_path_files = os.listdir(f'{sysvars.gnt_files_path}raw/{file_dir}/')
            for item in cur_path_files:
                if item.endswith(".csv"):
                    os.remove(os.path.join(f'{sysvars.gnt_files_path}raw/{file_dir}/', item))

        if len(os.listdir(f'{sysvars.gnt_files_path}raw/{file_dir}/')) == 1:
            for i in range(1985,current_year+1):
                command = f'curl -L -X GET https://reporter.nih.gov/exporter/{type}/download/{i} -o {sysvars.base_path}grant/src/raw/{file_dir}/{type}{i}.zip'
                os.system(command)
                command = f'unzip {sysvars.gnt_files_path}raw/{file_dir}/{type}{i}.zip -d {sysvars.base_path}grant/src/raw/{file_dir}'
                os.system(command)
                command = f'rm {sysvars.gnt_files_path}raw/{file_dir}/{type}{i}.zip'
                os.system(command)
        else:
            print(f'Files exist in {file_dir} folder... bypassing')
                
    # Copies over 1985-1999 project funding files stored in the grant/src/raw/ folder to the grant/src/raw/projects folder because it can not be downloaded with CURL
    if os.path.exists(f'{sysvars.gnt_files_path}raw/RePORTER_PRJFUNDING_C_FY1985_FY1999/'):
        funding_files = os.listdir(f'{sysvars.gnt_files_path}raw/RePORTER_PRJFUNDING_C_FY1985_FY1999/')
        for fund_file in funding_files:
            command = f'cp {sysvars.gnt_files_path}raw/RePORTER_PRJFUNDING_C_FY1985_FY1999/{fund_file} {sysvars.gnt_files_path}raw/projects/{fund_file}'
            os.system(command)
    else:
        print('Project funding files cannot be downloaded automatically, please visit the project tab on https://reporter.nih.gov/exporter and download the FY 1985-1999 Updated funding and DUNS information into the RDAS/grant/src/raw/projects directory')

def get_project_data (appl_id):
    url = 'https://api.reporter.nih.gov/v2/projects/search'
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    # Define parameters
    total_data = []
    limit = 1
    num_requests = 1 // limit

    data = {
        "criteria": {"appl_ids":[int(f'{appl_id}')]},
        "sort_field": "project_start_date",
        "sort_order": "desc"
    }

    # Send POST request
    response = requests.post(url, headers=headers, json=data)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse and append the response JSON data to the total_data list
        result_data = response.json()
        return result_data
        
    else:
        # Print an error message if the request was not successful
        print(f"Error: {response.status_code}, {response.text}")
        return
    
def clear_processed_files (restart_processed=False):
    if restart_processed:
        files = glob.glob(f'{sysvars.gnt_files_path}processed/**/*.csv', recursive=True)
        for f in files:
            print(f)
            os.remove(f)

    print('All files in processed folder removed')

def update_dictionary(dictionary):
    updated_dict = {}
    for key, value in dictionary.items():
        new_key = Gard[Gard['GardName'] == key]['GardId'].tolist()
        if new_key:
            new_key = new_key[0].replace('"', '')
            updated_dict[new_key] = value
        else:
            updated_dict[key] = value
    return updated_dict

def remove_similar_strings(df):
    for i in df.index:
        if i % 2000 ==0 : print(i)
        for j in df.index:
            if i != j:
                string_a = df['GardName'][i]
                list_b = df['Synonyms'][j]
                for item in list_b:  # Using [:] for iterating a copy of the list
                    if item == string_a:
                        list_b.remove(item)
    return df

def extract_words_from_json_string(input_string):
    try:
        # Use ast.literal_eval to safely convert the string to a list
        result_list = ast.literal_eval(input_string)
        if isinstance(result_list, list):
            return result_list
        else:
            raise ValueError("Input is not a string representation of a list.")
    except (ValueError, SyntaxError) as e:
        print(f"Error converting string to list: {e}")
        return None

def len_chcek(row):
        return [w for w in row if (len(w) >4) or (w == "sars") ]


#Gard = pd.read_csv('/content/Gard_V1.csv')
#######################          BOW       ########################################################################

def generate_term_orders(terms):
    words = terms.split()
    if len(words) ==2:
      all_permutations = list(permutations(words))
      orders = [' '.join(permutation) for permutation in all_permutations]
      return orders
    else: return [terms]

def generate_term_orders_list_of_sords(words):
    X=[]
    for i in words:
      X+=generate_term_orders(i)
    return X

########################      Removing stop words  #########################################################
def process_row(row):
    words = row.split()
    if len(words) > 2 :
        words = [word.lower()  for word in words if word.lower() not in ['syndrome','syndromes', 'disease','diseases']]
    return ' '.join(words)

def process_row_list(row):
    return [process_row(w) for w in row]

def remove_stop_words(text):
    stop_words = set(stopwords.words('english'))
    words = word_tokenize(text)
    filtered_words = [word for word in words if word.lower() not in stop_words]
    return ' '.join(filtered_words)

def process_row_list_2(row):
    return [remove_stop_words(w) if (remove_stop_words(w) != '' and len(w.split()) > 2) else w for w in row]

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

def stem_text_list(row):
      return [stem_text(w) for w in row if len(stem_text(w)) >2 ]

def extract_words_from_json_string2(input_string):
    try:
        # Use ast.literal_eval to safely convert the string to a list
        result_list = ast.literal_eval(input_string)
        if isinstance(result_list, list):
            return result_list
        else:
            raise ValueError("Input is not a string representation of a list.")
    except (ValueError, SyntaxError) as e:
        print(f"Error converting string to list: {e}")
        return None

def GardNamePreprocessor(Gard):
   Gard['GardName'] = Gard['GardName'].apply(lambda x: str(x).replace('"', '').lower())
   Gard['Synonyms'] = Gard['Synonyms'].apply(lambda x: extract_words_from_json_string(str(x).lower()))
   Gard = remove_similar_strings(Gard)
   Gard['Synonyms'] = Gard['Synonyms'].apply(lambda x: extract_words_from_json_string(str(x)))
   Gard['Synonyms'] =Gard['GardName'].apply(lambda x: [x])+Gard['Synonyms']
   #Gard['Synonyms_bow']=Gard['Synonyms'].apply(lambda x: generate_term_orders_list_of_sords(x) )
   Gard['Synonyms_sw'] = Gard['Synonyms'].apply(lambda x: process_row_list(x)) #.apply(lambda x: process_row_list(x))
   Gard['Synonyms_sw_bow']=Gard['Synonyms_sw'].apply(lambda x: generate_term_orders_list_of_sords(x) )
   Gard['Synonyms_sw_bow']=Gard['Synonyms_sw_bow'].apply(lambda x: list(set(len_chcek(x))) )
   #Gard['Synonyms_sw_nltk'] = Gard['Synonyms_sw'].apply(lambda x: process_row_list_2(x))
   #Gard['Synonyms_sw_nltk']=Gard['Synonyms_sw_nltk']+Gard['Synonyms_sw']
   #Gard['Synonyms_sw_nltk'] = Gard['Synonyms_sw_nltk'].apply(lambda x: list(set(x)))
   #Gard['Synonyms_stem'] = Gard['Synonyms'].apply(lambda x: stem_text_list(x))
   #Gard['Synonyms_stem_bow']=Gard['Synonyms_stem'].apply(lambda x: generate_term_orders_list_of_sords(x) )
   Gard['Synonyms_sw_stem'] = Gard['Synonyms_sw'].apply(lambda x: stem_text_list(x))
   Gard['Synonyms_sw_stem_bow']=Gard['Synonyms_sw_stem'].apply(lambda x: generate_term_orders_list_of_sords(x) )
   Gard['Synonyms_sw_stem'] = Gard['Synonyms_sw_stem'].apply(lambda x:list(set(len_chcek(x))) )
   Gard['Synonyms_sw_stem_bow']=Gard['Synonyms_sw_stem_bow'].apply(lambda x: list(set(len_chcek(x))) )
   Gard['Synonyms_sw'] = Gard['Synonyms_sw_stem'].apply(lambda x: list(set(len_chcek(x))) )

   Excluding_list = ['GARD:{:07d}'.format(int(gard_id.split(':')[1])) for gard_id in sysvars.gard_preprocessor_exclude]
   Gard['GardId'] = Gard['GardId'].str.strip('"')
   Gard = Gard[~Gard['GardId'].isin(Excluding_list)]

   return Gard

def download_gard_data_from_db ():
    db = AlertCypher(sysvars.gard_db)
    in_progress = db.getConf('UPDATE_PROGRESS', 'grant_in_progress')

    if not in_progress == 'True':
        return None

    if not os.path.exists(f'{sysvars.base_path}grant/src/processed/all_gards_processed.csv'):
        response = db.run('MATCH (x:GARD) RETURN x.GardId as GardId, x.GardName as GardName, x.Synonyms as Synonyms').data()

        myFile = open(f'{sysvars.base_path}grant/src/raw/all_gards.csv', 'w')
        writer = csv.writer(myFile)
        writer.writerow(['GardId', 'GardName', 'Synonyms'])
        for dictionary in response:
            writer.writerow(dictionary.values())
        myFile.close()
        df = pd.read_csv(f'{sysvars.base_path}grant/src/raw/all_gards.csv')

        df = GardNamePreprocessor(df)
        df.to_csv(f'{sysvars.base_path}grant/src/processed/all_gards_processed.csv')

    else:
        df = pd.read_csv(f'{sysvars.base_path}grant/src/processed/all_gards_processed.csv')
        df['Synonyms_sw'] = df['Synonyms_sw'].apply(lambda x: extract_words_from_json_string2(str(x).lower()))
        df['Synonyms_sw_bow'] = df['Synonyms_sw_bow'].apply(lambda x: extract_words_from_json_string2(str(x).lower()))
        df['Synonyms_sw_stem'] = df['Synonyms_sw_stem'].apply(lambda x: extract_words_from_json_string2(str(x).lower()))
        df['Synonyms_sw_stem_bow'] = df['Synonyms_sw_stem_bow'].apply(lambda x: extract_words_from_json_string2(str(x).lower()))

    return df

# Global Objects for Processing

Gard = download_gard_data_from_db()

'''
if not os.path.exists(f'{sysvars.base_path}grant/src/processed/all_gards_processed.csv'):
    pass
    Gard = download_gard_data_from_db()
else:
    Gard = pd.read_csv(f'{sysvars.base_path}grant/src/processed/all_gards_processed.csv')
    Gard['Synonyms_sw'] = Gard['Synonyms_sw'].apply(lambda x: extract_words_from_json_string2(str(x).lower()))
    Gard['Synonyms_sw_bow'] = Gard['Synonyms_sw_bow'].apply(lambda x: extract_words_from_json_string2(str(x).lower()))
    Gard['Synonyms_sw_stem'] = Gard['Synonyms_sw_stem'].apply(lambda x: extract_words_from_json_string2(str(x).lower()))
    Gard['Synonyms_sw_stem_bow'] = Gard['Synonyms_sw_stem_bow'].apply(lambda x: extract_words_from_json_string2(str(x).lower()))

nlp = spacy.load("en_core_web_sm")
'''

def is_about_term(input_text, target_term):
    # Load ClinicalBERT model and tokenizer
    model_name = "emilyalsentzer/Bio_ClinicalBERT"
    # ClinicalBERT: "emilyalsentzer/Bio_ClinicalBERT"
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name)
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

# Read the CSV file into a Pandas DataFrame
#Gard = pd.read_csv('/content/exporttttt.csv')

# Sample text
#text = "The goal of tis project was ird. This aim is not to go the first sentence. This is not the second sentence? And this is the third sentence."
#check_sen(text)

def split_sentence(sentence):
    # Use regular expression to split words without including punctuation
    words = re.findall(r'\b\w+\b', sentence)
    return words
def word_matching(text,word):
   for i in  split_sentence(word):
     if i not in text:
        return False
   return True

def get_gard_title(text, list_chcek):
  if list_chcek in ['Synonyms_stem','Synonyms_sw_stem','Synonyms_stem_bow','Synonyms_sw_stem_bow']: text1=stem_text(text.lower())
  elif list_chcek in [ 'Synonyms_sw_nltk'] : text1=remove_stop_words(text.lower())
  else: text1=text.lower()

  text2=split_sentence(text1)
  out=dict()
  
  for i in Gard.index:
    if Gard[list_chcek][i] != []:
      for j in Gard[list_chcek][i]:
        #if not j in ['-','',' ']:
         if j in text1 and word_matching(text2,j)==True:
           if Gard['GardName'][i] in out:
                if len(j.split()) ==1:   out[Gard['GardName'][i]][0]+=text2.count(j)
                else: out[Gard['GardName'][i]][0]+=text1.count(j)
           else:
                if len(j.split()) ==1:out[Gard['GardName'][i]]=[text2.count(j)]
                else:  out[Gard['GardName'][i]]=[text1.count(j)]
  if out== {}: return None
  return out

def get_gard_title_stem_exact(text):
    exact_matching=get_gard_title(text, 'Synonyms_sw_bow')
    #print(exact_matching)
    Stemming_chcek=get_gard_title(text, 'Synonyms_sw_stem_bow')
    #print(Stemming_chcek)
    if exact_matching is None:
        exact_matching = {}
    if Stemming_chcek is None:
        Stemming_chcek = {}
    combined_dict = {}
    combined_dict.update(exact_matching)
    combined_dict.update(Stemming_chcek)
    # Remove keys that are part of another key
    keys_to_remove = set()
    for key1 in combined_dict:
        for key2 in combined_dict:
            if key1 != key2 and key1 in key2:
                keys_to_remove.add(key1)
    for key in keys_to_remove:
        del combined_dict[key]
    if combined_dict=={}:return None
    for key1 in combined_dict:
        combined_dict[key1]=1
    return combined_dict

# Load spaCy model with sentencizer component

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
  Priority,Future_positive,present_positive,positive='','','',''
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
           Priority+=sent.text
        elif "future" in sentence_tenses:
           Future_positive+=sent.text
        elif "present" in sentence_tenses and is_sentence_negated(sent)==False:
           present_positive+=sent.text
        if i == 1:    first_sentence = sent.text
  return first_sentence,Priority,Future_positive,present_positive


def get_sentence_with_word(paragraph, target_word):
    if not isinstance(paragraph, str):
        return ''

    # Define characters indicating the start of a new sentence
    new_sentence_chars = ['-', ':', ';', '1)', '2)', '3)', '4)', '5)', '6)', '7)', '8)']

    # Split the paragraph into sentences using provided characters
    for char in new_sentence_chars:
        paragraph = paragraph.replace(char, '.')

    # Split the paragraph into sentences using standard punctuation
    sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?|\!)\s', paragraph)

    # Check for the target word in each sentence
    sen=''
    for sentence in sentences:
        if target_word.lower() in sentence.lower():
            sen+= sentence
    return sen


def stem_text_finding(text):
    # Initialize the Porter Stemmer
    stemmer = PorterStemmer()
    # Remove punctuation
    text_without_punctuation = text
    # Tokenize the text into words
    words = word_tokenize(text_without_punctuation)
    # Perform stemming on each word
    stemmed_words = [stemmer.stem(word) for word in words]
    # Join the stemmed words back into a single string
    stemmed_text = ' '.join(stemmed_words)
    return stemmed_text

def split_sentence(sentence):
    # Use regular expression to split words without including punctuation
    words = re.findall(r'\b\w+\b', sentence)
    return words
def word_matching(text,word):
   for i in  split_sentence(word):
     if i not in text:
        return False
   return True

def get_gard_abstract(text, list_chcek):
  #text=check_sen(text)
  if list_chcek in ['Synonyms_stem','Synonyms_sw_stem','Synonyms_stem_bow','Synonyms_sw_stem_bow']: text1=stem_text(text.lower())
  elif list_chcek in ['Synonyms_sw_nltk'] : text1=remove_stop_words(text.lower())
  else: text1=text.lower()

  text2=split_sentence(text1)
  out=dict()
  sen=dict()
  for i in Gard.index:
    if Gard[list_chcek][i] != []:
      for j in  Gard[list_chcek][i]:
         if j in text1 and word_matching(text2,j)==True:
           if Gard['GardName'][i] in out:
                if len(j.split()) ==1:
                   out[Gard['GardName'][i]][0]+=text2.count(j)
                   if list_chcek in ['Synonyms_stem','Synonyms_sw_stem','Synonyms_stem_bow','Synonyms_sw_stem_bow']:sen[Gard['GardName'][i]] += get_sentence_with_word(stem_text_finding(text.lower()), j)
                   else:    sen[Gard['GardName'][i]] += get_sentence_with_word(text1, j)
                else:
                   out[Gard['GardName'][i]][0]+=text1.count(j)
                   if list_chcek in ['Synonyms_stem','Synonyms_sw_stem','Synonyms_stem_bow','Synonyms_sw_stem_bow']:sen[Gard['GardName'][i]] += get_sentence_with_word(stem_text_finding(text.lower()), j)
                   else:    sen[Gard['GardName'][i]] += get_sentence_with_word(text1, j)
           else:
                if len(j.split()) ==1:
                     out[Gard['GardName'][i]]=[text2.count(j)]
                     if list_chcek in ['Synonyms_stem','Synonyms_sw_stem','Synonyms_stem_bow','Synonyms_sw_stem_bow']:sen[Gard['GardName'][i]] = get_sentence_with_word(stem_text_finding(text.lower()), j)
                     else:    sen[Gard['GardName'][i]] = get_sentence_with_word(text1, j)
                else:
                     out[Gard['GardName'][i]]=[text1.count(j)]
                     if list_chcek in ['Synonyms_stem','Synonyms_sw_stem','Synonyms_stem_bow','Synonyms_sw_stem_bow']:sen[Gard['GardName'][i]] = get_sentence_with_word(stem_text_finding(text.lower()), j)
                     else:    sen[Gard['GardName'][i]] = get_sentence_with_word(text1, j)
  if out== {}: return None,None
  return out,sen

def remove_stop_words(text):
    stop_words = set(stopwords.words('english'))
    words = word_tokenize(text)
    filtered_words = [word for word in words if word.lower() not in stop_words]
    return ' '.join(filtered_words)

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

def combine_dictionaries_count(dict1, dict2):
    combined_dict = {}
    # Update combined_dict with values from dict1
    for key, value in dict1.items():
        combined_dict[key] = combined_dict.get(key, 0) + sum(value)
    # Update combined_dict with values from dict2
    for key, value in dict2.items():
        combined_dict[key] = combined_dict.get(key, 0) + sum(value)
    return combined_dict

def combine_dictionaries_sent(dict1, dict2):
    combined_dict = {}
    # Update combined_dict with values from dict1
    for key, value in dict1.items():
        if key in combined_dict:
            combined_dict[key] += value
        else:
            combined_dict[key] = value
    # Update combined_dict with values from dict2
    for key, value in dict2.items():
        if key in combined_dict:
            combined_dict[key] += value
        else:
            combined_dict[key] = value
    return combined_dict

def modified_dict(combined_dict,combined_dict_sen):
    keys_to_remove = set()
    for key1 in combined_dict:
        for key2 in combined_dict:
          #try:
            if key1 != key2 and (key1 in key2) and (combined_dict[key1] <= combined_dict[key2]) and (combined_dict_sen[key1] in combined_dict_sen[key2]):
                keys_to_remove.add(key1)
          #except:
          #  pass
    for key in keys_to_remove:
        del combined_dict[key]
        del combined_dict_sen[key]
    return combined_dict


def get_gard_abstract_stem_exact(text):
  if text and isinstance(text, str):
    exact_matching, exact_matching_sen=get_gard_abstract(text, 'Synonyms_sw')
    #print(exact_matching)
    Stemming_chcek, Stemming_chcek_sen=get_gard_abstract(text, 'Synonyms_sw_stem')
    #print(Stemming_chcek)
    if exact_matching is None:exact_matching = {}
    if Stemming_chcek is None:Stemming_chcek = {}
    if exact_matching_sen is None:exact_matching_sen = {}
    if Stemming_chcek_sen is None:Stemming_chcek_sen = {}

    combined_dict    = combine_dictionaries_count(exact_matching,Stemming_chcek)
    combined_dict_sen= combine_dictionaries_sent(exact_matching_sen,Stemming_chcek_sen)
    # Remove keys that are part of another key
    combined_dict=modified_dict(combined_dict,combined_dict_sen)
    if combined_dict=={}:return {}
    return combined_dict
  return {}


def normalize_combined_dictionary(input_text,dict1, dict2, dict3, dict4,min_, max_):
    dict1 = {key: value * 4 for key, value in dict1.items()}
    # Make the values of the second dictionary two times
    dict2 = {key: value * 3 for key, value in dict2.items()}
    dict3 = {key: value * 2 for key, value in dict3.items()}
    # Combine all dictionaries
    combined_dict = {key: dict1.get(key, 0) + dict2.get(key, 0) + dict3.get(key, 0) + dict4.get(key, 0) for key in set(dict1) | set(dict2) | set(dict3) | set(dict4)}
    # Normalize the values of the combined dictionary
    total_frequency = sum(combined_dict.values())
    # Check if total_frequency is zero to avoid division by zero
    if total_frequency == 0:
        return {}
    normalized_dict = {key: min_ + (max_ - min_) * (value / total_frequency) for key, value in combined_dict.items()}
    result_dict = {}
    for key, value in normalized_dict.items():
    #if  is_about_term(input_text.lower(), key) >=0.7:
        result_dict[key] = [value, is_about_term(input_text.lower(), key)]
    return result_dict


def gard_id(title_, Public_health_relevance_statement, abstract_, nlp):
    if not isinstance(title_, str) and not isinstance(Public_health_relevance_statement, str) and not isinstance(abstract_, str):
        return ''  # Return default values when no string input is provided
    if title_ and isinstance(title_, str):
        name = get_gard_title_stem_exact(title_)
        if name: return name
    if Public_health_relevance_statement and isinstance(Public_health_relevance_statement, str):
        A, B, C,D = check_sen(Public_health_relevance_statement, nlp)
        name1 = get_gard_abstract_stem_exact(A)
        name2 = get_gard_abstract_stem_exact(B)
        name3 = get_gard_abstract_stem_exact(C)
        name4 = get_gard_abstract_stem_exact(D)
        name=normalize_combined_dictionary(Public_health_relevance_statement,name1,name2,name3,name4,0.7,0.9)
        if name and (name !={}): return name
    if abstract_ and isinstance(abstract_, str):
        A, B, C , D = check_sen(abstract_, nlp)
        name1 = get_gard_abstract_stem_exact(A)
        name2 = get_gard_abstract_stem_exact(B)
        name3 = get_gard_abstract_stem_exact(C)
        name4 = get_gard_abstract_stem_exact(D)
        name=normalize_combined_dictionary(abstract_,name1,name2,name3,name4,0,0.7)
        if name and (name !={}): return name

def GardNameExtractor(project_title,phr_text,abstract_text, nlp):
  #Abstract1['Gard_name']=Abstract1.apply(lambda x: gard_id(x['project_title'],x['phr_text'],x['abstract_text']), axis=1)
  gard_ids = gard_id(project_title,phr_text,abstract_text, nlp)
  if gard_ids:
    return update_dictionary(gard_ids)
  else:
    return None
  
def create_gard_nodes(db):
    print('Populating database with GARD Nodes')
    gard_db = AlertCypher(f'{sysvars.gard_db}')
    check = db.run('MATCH (x:GARD) RETURN x LIMIT 1').single()

    # Return if Gard nodes already created
    if check:
        print('Gard nodes already populated... bypassing')
        return

    response = gard_db.run('MATCH (x:GARD) RETURN x.GardId as GardId, x.GardName as GardName, x.Synonyms as Synonyms').data()

    for res in response:
        gid = res['GardId']
        name = res['GardName']
        syns = res['Synonyms']

        db.run(f'MERGE (x:GARD{{GardId:\"{gid}\", GardName:\"{name}\", Synonyms: {syns}}}) RETURN TRUE')

def create_project_node (db, gard_id, project_data, today):
    # Get node specific data from entry
    abstract = project_data.get('abstract_text')
    phr = project_data.get('phr_text')
    title = project_data.get('project_title')
    application_id = project_data.get('appl_id')
    application_type = project_data.get('project_num_split').get('appl_type_code')
    funding_year = project_data.get('fiscal_year')
    pref_terms_unparsed = project_data.get('pref_terms')
    terms_unparsed = project_data.get('terms')
    total_cost = project_data.get('agency_ic_fundings')[0].get('total_cost')

    # Convert float total_cost to integer
    if total_cost:
        total_cost = round(total_cost)
    else:
        total_cost = None

    # Parse out both of the term lists
    if terms_unparsed:
        terms = terms_unparsed.replace('<','').split('>')
        terms = terms[:len(terms)-1] # Removes empty string at end of list
    else:
        terms = None

    if pref_terms_unparsed:
        pref_terms = pref_terms_unparsed.split(';')
    else:
        pref_terms = None

    query = """
    MATCH (x:GARD) WHERE x.GardId = $gard_id 
    MERGE (y:Project {abstract: $abstract,
    phr_text: $phr,
    title: $title,
    application_id: $application_id,
    application_type: $application_type,
    funding_year: $funding_year,
    terms: $terms,
    pref_terms: $pref_terms,
    total_cost: $total_cost})
    MERGE (x)-[:RESEARCHED_BY]->(y)
    RETURN ID(y) as node_id
    """

    response = db.run(query, args={
        "gard_id":gard_id,
        "abstract":abstract if abstract else '', 
        "phr":phr if phr else '', 
        "title":title if title else '', 
        "application_id":application_id if application_id else '', 
        "application_type":application_type if application_type else '', 
        "funding_year":funding_year if funding_year else '',
        "terms":terms if terms else [],
        "pref_terms":pref_terms if pref_terms else [],
        "total_cost":total_cost if total_cost else '',
        "rdascreated": today,
        "rdasupdated": today
    })
    return response.single()['node_id']

def create_investigator_node (db, project_node_id, project_data):
    investigators = project_data.get('principal_investigators')
    organization = project_data.get('organization')
    org_name = organization.get('org_name')
    org_state = organization.get('org_state')
    org_city = organization.get('org_city')
    org_country = organization.get('org_country')

    for investigator in investigators:
        profile_id = investigator.get('profile_id')
        first_name = investigator.get('first_name')
        middle_name = investigator.get('middle_name')
        last_name = investigator.get('last_name')
        full_name = investigator.get('full_name')
        title = investigator.get('title')
        is_contact_pi = investigator.get('is_contact_pi')

    query = """
    MATCH (x:Project) WHERE ID(x) = $project_node_id 
    MERGE (y:PrincipalInvestigator {org_name: $org_name,
    org_state: $org_state,
    org_city: $org_city,
    org_country: $org_country,
    profile_id: $profile_id,
    first_name: $first_name,
    middle_name: $middle_name,
    last_name: $last_name,
    full_name: $full_name,
    title: $title,
    is_contact_pi: $is_contact_pi})
    MERGE (y)-[:INVESTIGATED]->(x)
    RETURN ID(y) as node_id
    """

    response = db.run(query, args={
        "project_node_id": project_node_id,
        "org_name": org_name if org_name else '',
        "org_state":org_state if org_state else '',
        "org_city":org_city if org_city else '',
        "org_country":org_country if org_country else '',
        "profile_id":profile_id if profile_id else '',
        "first_name":first_name if first_name else '',
        "middle_name":middle_name if middle_name else '',
        "last_name":last_name if last_name else '',
        "full_name":full_name if full_name else '',
        "title":title if title else '',
        "is_contact_pi":is_contact_pi if is_contact_pi else '',
    })
    return response.single()['node_id']

def create_core_project_node (db, project_node_id, project_data):
    core_project_num = project_data.get('core_project_num')

    query = """
    MATCH (x:Project) WHERE ID(x) = $project_node_id 
    MERGE (y:CoreProject {core_project_num: $core_project_num})
    MERGE (x)-[:UNDER_CORE]->(y)
    RETURN ID(y) as node_id,
    y.core_project_num as core_project_num
    """

    response = db.run(query, args={
        "project_node_id": project_node_id,
        "core_project_num": core_project_num
    }).single().data()
    return [response['node_id'], response['core_project_num']]

def create_agent_nodes (db, core_project_node_id, project_data):
    agency = project_data.get('agency_ic_admin')
    agent_name = agency.get('name')
    agent_abbr = agency.get('abbreviation')
    agent_code = agency.get('code')

    query = """
    MATCH (x:CoreProject) WHERE ID(x) = $core_project_node_id 
    MERGE (y:Agent {name: $agent_name,
    abbreviation: $agent_abbr,
    code: $agent_code})
    MERGE (x)-[:FUNDED_BY]->(y)
    RETURN ID(y) as node_id
    """

    response = db.run(query, args={
        "core_project_node_id": core_project_node_id,
        "agent_name": agent_name,
        "agent_abbr": agent_abbr if agent_abbr else '',
        "agent_code": agent_code if agent_code else ''
    }).single().data()
    return response['node_id']

def download_publication_data (core_project_num):
    core_project_num = 'R01GM100283'
    url = 'https://api.reporter.nih.gov/v2/publications/search'

    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    data = {
        "criteria": {
            "core_project_nums": [f"{core_project_num}"]
        },
        "limit": 500,
        "offset": 0,
        "sort_field": "core_project_nums",
        "sort_order": "desc"
    }

    # Send POST request
    response = requests.post(url, headers=headers, json=data)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse and work with the response JSON data
        result_data = response.json()
        return result_data['results']

    else:
        # Print an error message if the request was not successful
        print(f"Error: {response.status_code}, {response.text}")

def create_journal_nodes (db, publication_node_id, article_data):
    journal_name = article_data.get('journalInfo').get('journal').get('title')

    query = """
    MATCH (x:Publication) WHERE ID(x) = $publication_node_id 
    MERGE (y:Journal {title: $name})
    MERGE (x)-[:PUBLISHED_IN]->(y)
    RETURN ID(y) as node_id
    """

    response = db.run(query, args={
        "publication_node_id": publication_node_id,
        "name": journal_name
    }).single().data()
    return response['node_id']


def create_publication_nodes(db, core_project_node_id, publication_data):
    pmid_list = list()
    for publication in publication_data:
        pmid = publication.get('pmid')
        pmid_list.append(str(pmid))

    article_data = rdas.fetch_abstracts(pmid_list)

    if not article_data:
        return

    article_data = article_data[0]['resultList']['result']

    for pub in article_data:
        pmid = pub.get('pmid')
        title = pub.get('title')
        language = pub.get('language')
        publication_date = pub.get('firstPublicationDate')
        publication_year = str(datetime.strptime(publication_date, '%Y-%m-%d').year)
        authors = [i.get('fullName') for i in pub.get('authorList').get('author')]
        pmc_id = pub.get('fullTextIdList').get('fullTextId')[0].replace('PMC','')

        query = """
        MATCH (x:CoreProject) WHERE ID(x) = $core_project_node_id 
        MERGE (y:Publication {pmid: $pmid,
        pmc_id: $pmc_id,
        title: $title,
        language: $language,
        authors: $authors,
        date: $date,
        publicationYear: $publication_year})
        MERGE (x)-[:PUBLISHED]->(y)
        RETURN ID(y) as node_id
        """

        response = db.run(query, args={
            "core_project_node_id": core_project_node_id,
            "pmid": pmid,
            "pmc_id": pmc_id if pmc_id else '',
            "title": title if title else '',
            "language": language if language else '',
            "authors": authors if authors else '',
            "date": publication_date if publication_date else '',
            "publication_year": publication_year if publication_year else ''
        }).single().data()
    
        create_journal_nodes (db, response['node_id'], pub)

def create_clinical_study_nodes (db, core_project_num, core_project_node_id):
    df = pd.read_csv(f'{sysvars.base_path}grant_2024/src/raw/clinical_studies.csv', index_col=False)
    studies = df.loc[df['Core Project Number'] == core_project_num]
    print(studies)
    length = len(studies.index)
    print(length)
    if length > 0:
        for idx in range(length):
            row = studies.iloc[idx]
            gov_id = row['ClinicalTrials.gov ID']
            title = row['Study']
            status = row['Study Status']

            query = """
            MATCH (x:CoreProject) WHERE ID(x) = $core_project_node_id 
            MERGE (y:ClinicalStudies {gov_id: $gov_id,
            title: $title,
            status: $status})
            MERGE (x)-[:STUDIED]->(y)
            RETURN ID(y) as node_id
            """

            response = db.run(query, args={
                "core_project_node_id": core_project_node_id,
                "gov_id": gov_id,
                "title": title if title else '',
                "status": status if status else ''
            }).single().data()
            print('Created Clinical Study')

def create_patent_nodes (db, core_project_num, core_project_node_id):
    df = pd.read_csv(f'{sysvars.base_path}grant_2024/src/raw/patents.csv', index_col=False)
    print(core_project_num)
    patents = df.loc[df['PROJECT_ID'] == core_project_num]
    print(patents)
    length = len(patents.index)
    print(length)
    if length > 0:
        for idx in range(length):
            row = patents.iloc[idx]
            org_name = row['PATENT_ORG_NAME']
            pid = row['PATENT_ID']
            title = row['PATENT_TITLE']

            query = """
            MATCH (x:CoreProject) WHERE ID(x) = $core_project_node_id 
            MERGE (y:Patent {org_name: $org_name,
            id: $pid,
            title: $title})
            MERGE (x)-[:PATENTED]->(y)
            RETURN ID(y) as node_id
            """

            response = db.run(query, args={
                "core_project_node_id": core_project_node_id,
                "pid": pid,
                "org_name": org_name if org_name else '',
                "title": title if title else ''
            }).single().data()
            print('Created Patent')

def convert_csv_data ():
    df = pd.read_csv(f'{sysvars.base_path}grant_2024/src/raw/abstracts/abstracts1985.csv', index_col=False, encoding = "UTF-8")
    print(df)
    exit()
    

# Creates the data model for a single project
def create_data_model (db, gard_matches, project_data, today):
    for gard_id, gard_name in gard_matches.items():
        project_node_id = create_project_node (db, gard_id, project_data, today)

    core_project_node_id, core_project_num = create_core_project_node(db, project_node_id, project_data)
    create_agent_nodes(db, core_project_node_id, project_data)

    publication_data = download_publication_data(core_project_num)
    create_publication_nodes(db, core_project_node_id, publication_data)

    create_investigator_node(db, project_node_id, project_data)

    create_clinical_study_nodes(db, core_project_num, core_project_node_id)

    create_patent_nodes(db, core_project_num, core_project_node_id)

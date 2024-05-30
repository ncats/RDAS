import pandas as pd
import numpy as np
import unicodedata
import re
import spacy
from scispacy.abbreviation import AbbreviationDetector
import nltk
nltk.download('words')
nltk.download('punkt')
from nltk.tokenize import sent_tokenize
from nltk.corpus import words


# Encoding of text
ENCODING = 'latin1'

# Avoid these roman numerals detected by scispaCy as abbreviation
ROMAN_NUMERALS = ['ii','iii','iv','vi','vii']

# List of variations of 'aim 1' found in abstracts
SEARCH_AIM1 = ['aim1', 'aim 1', 'Aim1', 'Aim 1', 'AIM1', 'AIM 1', 'first aim', 'First aim']



# Load scispaCy model and add the abbreviation detector to the pipeline
nlp = spacy.load("en_core_sci_lg", exclude=["parser", "ner"])
nlp.add_pipe("abbreviation_detector")


def prepare_phr_aim(projects_file, abstracts_file):
    '''
    Select the Public Health Relevance (PHR) or 
    the aim section of the abstract for annotation.
    Returns a dataframe with application ID and text for annotation
    '''
    
    # Merge projects and abstracts files
    text = pd.read_csv(projects_file, encoding = ENCODING, dtype={'APPLICATION_ID':int, 'PROJECT_TITLE':str, 'PHR':str})
    abstracts = pd.read_csv(abstracts_file, encoding = ENCODING, dtype={'APPLICATION_ID':int, 'ABSTRACT_TEXT':str})
    text = pd.merge(left=text, right=abstracts, how='left', left_on='APPLICATION_ID', right_on='APPLICATION_ID')

    # Fill missing PROJECT_TITLE with a space
    text['PROJECT_TITLE'] =  text['PROJECT_TITLE'].fillna(' ')
    text['ABSTRACT_TEXT'] =  text['ABSTRACT_TEXT'].fillna(' ')

    # Use only PHR with more than 20 words, else use abstract
    phr_mask = text['PHR'].apply(lambda text: True if len(str(text).split()) > 20 else False)
    text['ABSTRACT_TEXT'] = np.where(phr_mask, text['PHR'], text['ABSTRACT_TEXT'])
    text['SOURCE'] = np.where(phr_mask, 'PHR', 'ABSTRACT')
    text = text[['APPLICATION_ID', 'PROJECT_TITLE', 'ABSTRACT_TEXT', 'SOURCE']]
    text.sort_values(by=['APPLICATION_ID'], inplace=True)
    text.reset_index(drop=True, inplace=True)

    # Normalize unicode characters and remove extra whitespaces
    text['ABSTRACT_TEXT'] = text['ABSTRACT_TEXT'].apply(clean_text)
    
    # Replace abbreviations
    docs = list(nlp.pipe(text['ABSTRACT_TEXT']))

    for idx, doc in enumerate(docs):
        if len(doc._.abbreviations) > 0:
            token_text = [token.text for token in doc]

            # Replace abbreviated token text with its long form
            for abrv in doc._.abbreviations:
                if abrv.text.islower():  
                    # Check if abbreviation is not an english word or roman numerals
                    if (abrv.text not in words.words()) and (abrv.text not in ROMAN_NUMERALS):
                        token_text[abrv.start] = str(abrv._.long_form)
#                        print(f'abbrev: {abrv.text}       long_form: {abrv._.long_form}')
#                    else:
#                        print(f'NOT REPLACED.... abbrev: {abrv.text}       long_form: {abrv._.long_form}')
                else:
                    # Check if abbreviation is in plural form
                    if (abrv.text[-1] == 's' and abrv.text[:-1].isupper()) and \
                       (str(abrv._.long_form)[-1] == 's'):
                        # Replace the token with the abbreviation in plural form
                        token_text[abrv.start] = str(abrv._.long_form)
#                        print(f'abbrev: {abrv.text}       long_form: {abrv._.long_form}')

                        # Find if other tokens have the abbreviation in singular form
                        for token_idx, token in enumerate(token_text):
                            if token == abrv.text[:-1]:
                                token_text[token_idx] = str(abrv._.long_form)[:-1]
#                                print(f'abbrev: {abrv.text[:-1]}       long_form: {str(abrv._.long_form)[:-1]}')
                    else:
                        token_text[abrv.start] = str(abrv._.long_form)
#                        print(f'abbrev: {abrv.text}       long_form: {abrv._.long_form}')
            
            text.loc[idx, 'ABSTRACT_TEXT'] = ' '.join(token_text)

    # Extract aim section of the abstract
    text['ABSTRACT_TEXT'], text['SOURCE'] = extract_aims(text['ABSTRACT_TEXT'], text['SOURCE'])
    
    text['ABSTRACT_TEXT'] = text['PROJECT_TITLE'] + ' ' + text['ABSTRACT_TEXT']
    text = text[['APPLICATION_ID', 'ABSTRACT_TEXT', 'SOURCE']]

    return text


def clean_text(text):
    '''
    Clean up the unicode characters and extra whitespaces
    '''

    # Remove unicode characters
    text = unicodedata.normalize('NFC', text)
    
    # Remove extra whitespaces
    text = re.sub(' +', ' ', text)
    
    return text


def extract_aims(text, source):
    '''
    Extract the aim section from the text
    '''
    for i in range(len(text)):
        if source[i] == 'ABSTRACT':
            text_sent = sent_tokenize(text[i])
            
            # search for the aim section 
            for idx in range(len(text_sent)):
                if any(ele in text_sent[idx] for ele in SEARCH_AIM1):
                    text[i] = ' '.join(text_sent[idx:])
                    source[i] = 'AIM'
                    break
    
    return text, source

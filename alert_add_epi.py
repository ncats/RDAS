import argparse
import requests
import xml.etree.ElementTree as ET
import pickle
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3' 
import tensorflow as tf
import nltk
from nltk.corpus import stopwords
import spacy
import numpy as np
from tensorflow.keras.preprocessing.sequence import pad_sequences

import json
from neo4j import GraphDatabase, basic_auth
from datetime import date
import time
import logging, logging.config
import itertools
import requests
import jmespath
import re

STOPWORDS = ''
max_length = 300
trunc_type = 'post'
padding_type = 'post'

def get_all_abstracts():
  #Returns list of abstracts in current neo4j
  print('get all abstracts ...')
  with GraphDatabase.driver("bolt://localhost:7687", encrypted=False) as driver:
    with driver.session() as session:
      cypher_query = '''
      match p = (n:Article) where n.abstractText <> "" return id(n) as id, n.abstractText as abstract
      '''
      nodes = session.run(cypher_query, parameters={})
      results = nodes.data()
    #return [r['pubmed_id'] for r in results]
    return results
    
def add_epi(tx, isEpi, article_id):
  query = '''
  MATCH (a:Article) WHERE id(a) = $article_id
  SET a.isEpi = $isEpi
  '''
  
  tx.run(query, parameters={
    "article_id":article_id,
    "isEpi":isEpi,
  })


def setup():
    nltk.download('stopwords')
    nltk.download('punkt')
    STOPWORDS = set(stopwords.words('english'))

# Standardize the abstract by replacing all named entities with their entity label.
# Eg. 3 patients reported at a clinic in England --> CARDINAL patients reported at a clinic in GPE
# expects the spaCy model en_core_web_lg as input
def standardizeAbstract(abstract, nlp):
    doc = nlp(abstract)
    newAbstract = abstract
    for e in reversed(doc.ents):
        if e.label_ in {'PERCENT','CARDINAL','GPE','LOC','DATE','TIME','QUANTITY','ORDINAL'}:
            start = e.start_char
            end = start + len(e.text)
            newAbstract = newAbstract[:start] + e.label_ + newAbstract[end:]
    return newAbstract

# Same as above but replaces biomedical named entities from scispaCy models
# Expects as input en_ner_bc5cdr_md and en_ner_bionlp13cg_md
def standardizeSciTerms(abstract, nlpSci, nlpSci2):
    doc = nlpSci(abstract)
    newAbstract = abstract
    for e in reversed(doc.ents):
        start = e.start_char
        end = start + len(e.text)
        newAbstract = newAbstract[:start] + e.label_ + newAbstract[end:]
        
    doc = nlpSci2(newAbstract)
    for e in reversed(doc.ents):
        start = e.start_char
        end = start + len(e.text)
        newAbstract = newAbstract[:start] + e.label_ + newAbstract[end:]
    return newAbstract

def process_abstract(abstract, new_tokenizer, new_model, nlpSci, nlpSci2, nlp):
    # remove stopwords
    for word in STOPWORDS:
        token = ' ' + word + ' '
        abstract = abstract.replace(token, ' ')
        abstract = abstract.replace(' ', ' ')
    abstract_standard = [standardizeAbstract(standardizeSciTerms(abstract, nlpSci, nlpSci2), nlp)]
    sequence = new_tokenizer.texts_to_sequences(abstract_standard)
    padded = pad_sequences(sequence, maxlen=max_length, padding=padding_type, truncating=trunc_type)
    
    y_pred1 = new_model.predict(padded) # generate prediction
    y_pred = np.argmax(y_pred1, axis=1) # get binary prediction
    
    prob = y_pred1[0][1]
    if y_pred == 1:
        isEpi = True
    else:
        isEpi = False

    return prob, isEpi

# Generate predictions for a PubMed Abstract
# nlp: en_core_web_lg
# nlpSci: en_ner_bc5cdr_md
# nlpSci2: en_ner_bionlp13cg_md
# Defaults to load my_model_orphanet_final, the most up-to-date version of the classification model,
# but can also be run on any other tf.keras model
def getPredictions(model='my_model_orphanet_final'):
    print('setup......')
    setup()
    print('Loading 3 NLP models...')
    nlp = spacy.load('en_core_web_lg')
    print('Core model loaded.')
    nlpSci = spacy.load("en_ner_bc5cdr_md")
    print('Disease and chemical model loaded.')
    nlpSci2 = spacy.load('en_ner_bionlp13cg_md')
    print('All models loaded.')
    
    # load the tokenizer
    handle = open('tokenizer.pickle', 'rb')
    new_tokenizer = pickle.load(handle)
    handle.close()
    
    new_model = tf.keras.models.load_model('saved_model/'+model) # load the model
    
    # preprocess abstract
    records = get_all_abstracts()
    print("number of articles: ", len(records))
    with GraphDatabase.driver("bolt://localhost:7687", encrypted=False) as driver:
        with driver.session() as session:
            tx = session.begin_transaction()
            count = 0
            for r in records:
                count = count + 1
                # from terminal breaking point
                #if count < 543000:
                #    continue
                prob, isEpi = process_abstract(r['abstract'], new_tokenizer, new_model, nlpSci, nlpSci2, nlp)
                #print(count, r['pubmed_id'], prob, isEpi)
                add_epi(tx, isEpi, r['id'])
                
                if count % 1000 == 0:
                    print('commit', count)
                    tx.commit()
                    tx = session.begin_transaction()
            tx.commit()       

if __name__ == '__main__':
    getPredictions()


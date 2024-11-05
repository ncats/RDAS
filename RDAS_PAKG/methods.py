import os
import sys
import asyncio
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import json
from neo4j import GraphDatabase, basic_auth
import configparser
from datetime import datetime, date
from AlertCypher import AlertCypher
from dateutil.relativedelta import relativedelta
from collections import OrderedDict
import time
import logging
import itertools
import requests
import jmespath
import re
import ast
import sysvars
import pandas as pd
import string
from transformers import AutoTokenizer, AutoModelForTokenClassification
from transformers import pipeline
from nltk.corpus import words as nltk_words
# Setup NLTK for english word parsing for synonym filtering
wordset = set(nltk_words.words())



def get_gard_list():
  """
    Returns a list of GARD diseases from the GARD database.

    This function executes a Cypher query to retrieve information about GARD diseases from the Neo4j database.

    Returns:
    :return: Ordered dictionary containing information about GARD diseases.
             The dictionary is sorted based on the GARD IDs.
  """

  # Connect to the GARD database using the AlertCypher class
  GARDdb = AlertCypher(sysvars.gard_db)

  # Define the Cypher query to retrieve GARD diseases
  cypher_query = 'match (m:GARD) return m'

  # Execute the Cypher query and retrieve nodes
  nodes = GARDdb.run(cypher_query)
  results = nodes.data()

  # Create an ordered dictionary to store GARD disease information
  myData = {}

  # Process each result and populate the dictionary
  for res in results:
    gard_id = res['m']["GardId"]
    disease = {}
    disease['gard_id'] = gard_id
    disease['name'] = res['m']["GardName"]
    disease['type'] = res['m']["DisorderType"]
    disease['classification'] = res['m']["ClassificationLevel"] if res['m']["ClassificationLevel"] is not None else ''
    disease['synonyms'] = res['m']["Synonyms"] if res['m']["Synonyms"] is not None else ''
    disease['OMIM'] = res['m']['OMIM'] if 'OMIM' in res['m'] else None
    myData[gard_id] = disease

  # Return the ordered dictionary sorted based on GARD IDs
  return OrderedDict(sorted(myData.items()))




def get_gard_omim_mapping(db):
  """
    Retrieves mapping information between GARD diseases and OMIM identifiers from the specified graph database.

    This function performs a Cypher query to the graph database to find GARD diseases that are linked to OMIM identifiers.

    Parameters:
    :param db: Neo4j database connection object.

    Returns:
    :return: A list of dictionaries containing mapping information between GARD diseases and OMIM identifiers.
             Each dictionary includes the GARD ID, GARD name, match type, OMIM ID, and OMIM name.
  """

  # Get the list of GARD diseases
  




def find_OMIM_articles(OMIMNumber):
  """
    Finds articles related to a specific OMIM entry using the OMIM API.

    This function sends a POST request to the OMIM API to retrieve information about articles related to a specific OMIM entry.

    Parameters:
    :param db: Neo4j database connection object.
    :param OMIMNumber: The OMIM number for which articles are to be retrieved.

    Returns:
    :return: JSON response containing information about articles related to the specified OMIM entry.
  """

  # Set parameters for the OMIM API request and send a POST request to the API
  params = {'mimNumber': OMIMNumber, 'include':"all", 'format': 'json', 'apiKey': os.environ['OMIM_KEY']}
  
  # Return the JSON response containing information about articles related to the specified OMIM entry
  try:
    query = f"https://api.omim.org/api/entry?mimNumber={params['mimNumber']}&include={params['include']}&format={params['format']}&apiKey={params['apiKey']}"
    return_data = requests.get(query)
    return_data = return_data.json()
  except Exception as e:
    print('Retrying find OMIM after 1 day to reset API limit', e)
    time.sleep(87000) # Sleep for a day to reset API limit
    return_data = requests.post(f"https://api.omim.org/api/entry?mimNumber={params['mimNumber']}&include={params['include']}&format={params['format']}&apiKey={params['apiKey']}")
    return_data = return_data.json()

  return return_data




def get_article_in_section(omim_reference):
  """
    Extracts information about articles referenced in specific sections of an OMIM entry.

    This function parses the provided OMIM reference data and identifies articles referenced in different sections
    of the entry.

    Parameters:
    :param omim_reference: OMIM reference data, typically obtained from the OMIM API.

    Returns:
    :return: A dictionary containing PubMed IDs and the corresponding sections in which they are referenced.
             The keys are PubMed IDs, and the values are lists of section names.
  """
  all_omim_data_list = list()

  # Extract text sections from the OMIM reference data
  all_entries = jmespath.search("omim.entryList",omim_reference)
  print(f'Total OMIM numbers in GARD ID:: {len(all_entries)}')
  for entry in all_entries:
    all_omim_data = {'OMIM':"", 'non-pubmed':[], 'pubmed':{}}

    omim_num = jmespath.search("entry.mimNumber",entry)
    all_omim_data['OMIM'] = omim_num

    prefTitle = jmespath.search("entry.titles.preferredTitle",entry)
    textSections = jmespath.search("entry.textSectionList[*].textSection",entry)

    # Create a dictionary to store references by section
    references = {}

    # Iterate over text sections and extract references
    for t in textSections:
      refs = re.findall("({[0-9]*?:.*?})",t['textSectionContent'])
      if refs:
        sectionReferenced = set()
        for ref in refs:
          splitRef= ref[1:].split(":")
          sectionReferenced.add(splitRef[0])
        references[t['textSectionName']] = sectionReferenced

    # Extract reference numbers and PubMed IDs from the OMIM reference data
    refNumbers = jmespath.search("entry.referenceList[*].reference.[referenceNumber,pubmedID,title,authors,doi,source]",entry)

    # Create a dictionary to store PubMed IDs and corresponding sections
    articleString = {}

    # Check if there are no reference numbers
    if refNumbers is None:
      return all_omim_data_list
    
    # Iterate over reference numbers and PubMed IDs
    for refNumber,pmid,title,authors,doi,source in refNumbers:
      # Identify sections referencing the current PubMed ID
      tsections = []
      for idx, sectionName in enumerate(references):
        if references[sectionName].intersection(set([str(refNumber)])):
          tsections.append(sectionName)

      # Skip references without PubMed IDs
      if pmid:
        # Update the dictionary with PubMed ID and corresponding sections
        if tsections:
          articleString[str(pmid)] = tsections
        else:
          articleString[str(pmid)] = ['See Also']

        all_omim_data['pubmed'] = {'title':prefTitle, 'sections':articleString}

      else:
        current_non_pubmed_list = all_omim_data['non-pubmed']
        current_non_pubmed_list.append({'refNumber':refNumber, 'title':title, 'authors':{'fullName':authors}, 'doi':doi, 'sections':tsections})
        all_omim_data['non-pubmed'] = current_non_pubmed_list

      # Creates a list like [{'OMIM':'1020201','pubmed':{...}, 'non-pubmed':{...}}]
    all_omim_data_list.append(all_omim_data)
      
  # Return the dictionary containing PubMed IDs and corresponding sections
  return all_omim_data_list




def get_article_id(pubmed_id, driver):
  """
    Retrieves the Neo4j database ID of an article given its PubMed ID.

    This function performs a Cypher query to find the Neo4j database ID of an article with the specified PubMed ID.

    Parameters:
    :param pubmed_id: PubMed ID of the article.
    :param driver: Neo4j driver object for database connection.

    Returns:
    :return: The Neo4j database ID of the article if found, otherwise None.
  """

  # Initialize article_id as None
  article_id = None

  # Execute a Cypher query to find the Neo4j database ID of the article
  result = driver.run("MATCH(a:Article {pubmed_id:$pmid}) return ID(a) as id", args = {'pmid':pubmed_id})
  record = result.single()
  
  # Check if a record was found and retrieve the article ID
  if record:
    article_id = record["id"]
    
  # Return the article ID or None if not found
  return article_id




def get_disease_id(gard_id, driver):
  """
    Retrieves the Neo4j database ID of a GARD disease given its GARD ID.

    This function performs a Cypher query to find the Neo4j database ID of a GARD disease with the specified GARD ID.

    Parameters:
    :param gard_id: GARD ID of the disease.
    :param driver: Neo4j driver object for database connection.

    Returns:
    :return: The Neo4j database ID of the GARD disease if found, otherwise None.
  """

  # Initialize id as None
  id = None

  # Execute a Cypher query to find the Neo4j database ID of the GARD disease
  result = driver.run("MATCH(a:GARD {GardId:$gard_id}) return ID(a) as id", args = {'gard_id':gard_id})
  record = result.single()

  # Check if a record was found and retrieve the disease ID
  if record:
    id = record["id"]

  # Return the disease ID or None if not found
  return id



def create_omim_article_no_pubmed(db, omim_data, gard_id, search_source, maxdate):
    # Excludes OMIM articles named Personal Communication, since their data is incaccessible
    if omim_data['title'] == 'Personal Communication.':
      return None

    create_article_query = '''
    MATCH (d:GARD) WHERE d.GardId=$gard_id
    MERGE (n:Article {doi: $doi, title: $title})
    ON CREATE SET
      n.doi = $doi, 
      n.title = $title,
      n.isEpi = $epi,
      n.epi_processed = $epi_proc,
      n.DateCreatedRDAS = $now,
      n.LastUpdatedRDAS = $now,
      n.ReferenceOrigin = [\'''' + search_source + '''\']
    MERGE (d)-[r:MENTIONED_IN]->(n)
    RETURN ID(n)
    '''

    params={
      "gard_id":gard_id,
      "doi":omim_data['doi'] if 'doi' in omim_data and omim_data['doi'] else '',
      "title":omim_data['title'] if 'title' in omim_data and omim_data['title'] else '',
      "epi": False,
      "epi_proc": False,
      "now": datetime.strptime(maxdate,"%Y/%m/%d").strftime("%m/%d/%y"), #"isEpi": False
      }
    
    # Execute the Cypher query and retrieve the ID of the created Article node
    response = db.run(create_article_query, args=params).single().value()
    print('+', end='', flush=True)
    return response




def mask_name(name, nlp):
    name = name.rstrip(string.punctuation)
    name = name.title()
    entities = nlp(name)
    person_entities = [entity['word'] for entity in entities if entity['entity'] == 'B-PER' or entity['entity'] == 'I-PER']
    if person_entities == []:
        reversed_name = ' '.join(name.split()[::-1])
        entities = nlp(reversed_name)
        person_entities = [entity['word'] for entity in entities if entity['entity'] == 'B-PER' or entity['entity'] == 'I-PER']
    masked_name = ' '.join(person_entities) if person_entities else None
    masked_name = masked_name.replace(' ##', '') if masked_name is not None else None
    if masked_name != None:
             name=name.replace('"', '').replace("'", '')
             #masked_name=', '.join([i.strip() for i in name.split(',') if len(i.strip())>=4 ])
    return masked_name



    
def save_omim_articles(db, today):
  """
    Retrieves and saves OMIM-related articles in the Neo4j database.

    This function iterates over GARD diseases, retrieves OMIM-related articles, and updates the database.

    Parameters:
    :param db: Neo4j database connection object.
    :param today: Today's date in the format "%Y/%m/%d".

    Returns:
    None
  """

  # Get OMIM API key from environment variables
  omim_api_key = os.environ['OMIM_KEY']

  # Check if OMIM API key is available
  if len(omim_api_key) == 0:
    return

  # Get the list of GARD diseases
  results = get_gard_list()

  # Define search source
  search_source = 'OMIM'

  # Iterate over GARD diseases
  current_step = db.getConf('UPDATE_PROGRESS', 'pubmed_omim_article_progress')
  print(current_step, type(current_step))
  if not current_step == '':
    current_step = int(current_step)
  else:
    current_step = 0
  for idx, gard_id in enumerate(results):
    if idx < current_step:
      continue

    print(idx, 'UPDATING OMIM: ' + gard_id)
    try:
      omim_ids = results[gard_id]["OMIM"]
      omim_ids = ",".join(omim_ids)
    except Exception as e:
      print(omim_ids, f'No OMIM IDs Available for {gard_id}')
      db.setConf('UPDATE_PROGRESS', 'pubmed_omim_article_progress', str(idx))
      continue
    
    # Skip if no OMIM IDs are available for the current GARD disease
    if not omim_ids:
      db.setConf('UPDATE_PROGRESS', 'pubmed_omim_article_progress', str(idx))
      continue

    # Retrieve OMIM articles using the OMIM API
    try:
      omim_json = find_OMIM_articles(omim_ids)
    except Exception as e:
      logging.error(f' Exception when search omim_id {omim_ids}: error {e}')
      continue

    # Extract sections from OMIM articles
    omim_data_list = get_article_in_section(omim_json)
    for omim_data in omim_data_list:
      omim = omim_data['OMIM']
      if len(omim_data['pubmed']) > 0:
        prefTitle = omim_data['pubmed']['title']
        sections = omim_data['pubmed']['sections']

        logging.info(f'sections: {sections}')

        # Create a copy of the sections dictionary
        new_sections = dict(sections)
      
        # Iterate over the PubMed IDs in sections
        for pubmed_id in sections:
          # Get the Neo4j database ID of the article
          article_id = get_article_ID(pubmed_id, db)

          # Check if the article already exists in the database
          if (article_id):
            logging.info(f'PubMed evidence article already exists: {pubmed_id}, {article_id}')
            # Save OMIM article relation in the database
            save_omim_article_relation(article_id, prefTitle, omim, sections[pubmed_id], db, today)
            # Remove the PubMed ID from the copy of sections
            new_sections.pop(pubmed_id)

          else:
            logging.info(f'PubMed evidence article NOT exists: {pubmed_id}, {article_id}')

        logging.info(f'sections after loop: {new_sections}' )

        # Save remaining OMIM articles to the database
        save_omim_remaining_articles(gard_id, prefTitle, omim, new_sections, search_source, db, today) 

      # If the OMIM reference has no PubMed ID, create an article node with just the information supplied by OMIM
      if len(omim_data['non-pubmed']) > 0: 
        for omim_data_index in omim_data['non-pubmed']:
          sections = omim_data_index['sections']
          title = omim_data_index['title']
          article_id = create_omim_article_no_pubmed(db, omim_data_index, gard_id, search_source, today)
          if article_id:
            create_authors(db, omim_data_index['authors'], article_id, omim=True)
            save_omim_article_relation(article_id, title, omim, sections, db, today)

    db.setConf('UPDATE_PROGRESS', 'pubmed_omim_article_progress', str(idx))

           

      
def save_omim_article_relation(article_id, prefTitle, omim_id, sections, driver, today):
  """
    Saves the relationship between an article and an OMIM entry in the Neo4j database.

    This function creates a relationship between an article and an OMIM entry in the Neo4j database.

    Parameters:
    :param article_id: Neo4j database ID of the article.
    :param omim_id: OMIM ID associated with the article.
    :param sections: Sections in which the article references the OMIM entry.
    :param driver: Neo4j driver object for database connection.
    :param today: Today's date in the format "%Y/%m/%d".

    Returns:
    None
  """

  rdascreated = datetime.strptime(today,"%Y/%m/%d").strftime("%m/%d/%y")
  rdasupdated = datetime.strptime(today,"%Y/%m/%d").strftime("%m/%d/%y")

  # Check if reference source already in ReferenceOrigin
  ref_check = driver.run(f'MATCH (a:Article) WHERE ID(a) = {article_id} RETURN a.ReferenceOrigin as ref').data()[0]['ref']
  
  if sections:
    if 'OMIM' in ref_check:
      print(f'omim if::: {sections}')
      query = f'''
      MATCH (a:Article) WHERE ID(a) = {article_id}
      MERGE (p:OMIMRef {{omimId: {omim_id}, omimName: \"{prefTitle}\", omimSections: {sections}}}) SET p.DateCreatedRDAS = \"{rdascreated}\" SET p.LastUpdatedRDAS = \"{rdasupdated}\"
      MERGE (a) - [r:HAS_OMIM_REF] -> (p)
      '''
    else:
      print(f'omim else::: {sections}')
    # Define the Cypher query for creating the relationship
      query = f'''
      MATCH (a:Article) WHERE ID(a) = {article_id}
      SET a.ReferenceOrigin = ['OMIM'] + a.ReferenceOrigin
      MERGE (p:OMIMRef {{omimId: {omim_id}, omimName: \"{prefTitle}\", omimSections: {sections}}}) SET p.DateCreatedRDAS = \"{rdascreated}\" SET p.LastUpdatedRDAS = \"{rdasupdated}\"
      MERGE (a) - [r:HAS_OMIM_REF] -> (p)
      '''
  else:
    print(f'sections else::: {sections}')
    query = f'''
      MATCH (a:Article) WHERE ID(a) = {article_id}
      MERGE (p:OMIMRef {{omimId: {omim_id}, omimName: \"{prefTitle}\", omimSections: {sections}}}) SET p.DateCreatedRDAS = \"{rdascreated}\" SET p.LastUpdatedRDAS = \"{rdasupdated}\"
      MERGE (a) - [r:HAS_OMIM_REF] -> (p)
      '''

  # Execute the Cypher query
  driver.run(query)



      
def save_omim_remaining_articles(gard_id, prefTitle, omim_id, sections, search_source, driver, today):
  """
    Saves remaining OMIM-related articles and their relationships in the Neo4j database.

    This function saves articles and their relationships with an OMIM entry in the Neo4j database.

    Parameters:
    :param gard_id: GARD ID of the disease.
    :param omim_id: OMIM ID associated with the articles.
    :param sections: Sections in which the articles reference the OMIM entry.
    :param search_source: Source of the search (e.g., 'omim_evidence').
    :param driver: Neo4j driver object for database connection.
    :param today: Today's date in the format "%Y/%m/%d".

    Returns:
    None
  """

  # Get the list of PubMed IDs from the sections
  pubmed_ids = list(sections.keys())

  # Get the Neo4j database ID of the disease
  disease_id = get_disease_ID(gard_id, driver)
  logging.info(f'pubmed_ids: {pubmed_ids}')
  
  # Save articles and related information
  save_articles(disease_id, pubmed_ids, search_source, driver, today)

  # Iterate over PubMed IDs in sections
  for pubmed_id in sections:
    # Get the Neo4j database ID of the article
    article_id = get_article_ID(pubmed_id, driver)

    # Check if the article ID is available
    if (article_id):
      # Save OMIM article relation in the database
      save_omim_article_relation(article_id, prefTitle, omim_id, sections[pubmed_id], driver, today)
    else:
      logging.error(f'Something wrong with adding omim article relation: {pubmed_id}, {article_id}')            




def find_articles(keyword, mindate, maxdate, batch=list(), cnt=0, recurse=False, finished=False):
  """
  NOTE: A maximum of 9999 articles are retrieved during an update, a batch of queries will need to be ran to get the full number of articles during an update
  """
  """
  Search for articles in the PubMed database using the NCBI API.

  Parameters:
  - keyword (str): The search keyword or terms, separated by semicolons if multiple.
  - mindate (str): The minimum publication date for filtering the search results (format: YYYY/MM/DD).
  - maxdate (str): The maximum publication date for filtering the search results (format: YYYY/MM/DD).

  Returns:
  dict: A dictionary containing the response from the NCBI API.

  Note:
  - This function may wait for 15 seconds and retry the API request if the API limit is reached.
  - Ensure the 'NCBI_KEY' environment variable is set with the NCBI API key.

  Example:
  >> response = find_articles("cancer;therapy", "2022/01/01", "2022/12/31")
  >> print(response)
  {'esearchresult': {'count': '123', 'idlist': ['123456', '789012'], ...}}
  """
  if not recurse and not finished:
    # Clears the ID list between new diseases searched
    batch = list()

  # Initialize variables
  url = str()
  term_search_query = str()
  api_key = os.environ['NCBI_KEY']
  keyword = keyword.replace('-',' ').replace('\"','')
  tokens = keyword.split(';')
  tokens = [i.strip() for i in tokens]
  mindate_obj = datetime.strptime(mindate,'%Y/%m/%d')
  maxdate_obj = datetime.strptime(maxdate,'%Y/%m/%d')

  # Construct the search query
  if len(tokens) > 1:
    term_search_query += '('
    for idx,token in enumerate(tokens):
      token_word_length = len(token.split(' '))
      if token_word_length == 1:
        if idx == len(tokens) - 1:
          term_search_query += f'(\"{token}\"[Title/Abstract])'
        else:
          term_search_query += f'(\"{token}\"[Title/Abstract])+AND+'
      else:
        if idx == len(tokens) - 1:
          term_search_query += f'\"{token}\"[Title/Abstract:~1]'
        else:
          term_search_query += f'\"{token}\"[Title/Abstract:~1]+AND+'
    term_search_query += ')'
  else:
    term_search_query = f'\"{keyword}\"[Title/Abstract:~1]'
  
  # Construct the API request URL
  print(term_search_query)
  url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term={term_search_query}&mindate={mindate}&maxdate={maxdate}&retmode=json&retmax=10000&api_key={api_key}" #retmax=10000

  # Make the API request
  response = requests.post(url).json()
  try:
    articles_found = int(response['esearchresult']['count'])
  except:
    articles_found = 0

  if articles_found == 0:
    return list()

  if articles_found > 9999:
    maxdate_obj = maxdate_obj - relativedelta(years=1)
    maxdate = maxdate_obj.strftime("%Y/%m/%d")
    mindate = mindate_obj.strftime("%Y/%m/%d")
    cnt+=1
    
    return find_articles(keyword, mindate, maxdate, batch=batch, cnt=cnt, recurse=True)
  else:
    mindate_obj = maxdate_obj
    maxdate_obj = maxdate_obj + relativedelta(years=cnt)
    maxdate = maxdate_obj.strftime("%Y/%m/%d")
    mindate = mindate_obj.strftime("%Y/%m/%d")
    batch.extend(response['esearchresult']['idlist'])
    
    # Stop condition for recursive calls
    if mindate == maxdate:
      return batch
    else:
      return find_articles(keyword, mindate, maxdate, batch=batch, cnt=0, recurse=True, finished=True)




def fetch_abstracts(pubmedIDs): 
  """
  Fetch abstracts for articles using PubMed IDs.

  Parameters:
  - pubmedIDs (list): A list of PubMed IDs for which abstracts will be retrieved.

  Returns:
  list: A list of responses, where each response contains abstract information for a batch of PubMed IDs.

  Note:
  - PubMed IDs are processed in batches due to limitations on the amount of data that can be retrieved at once.

  Example:
  >> pubmed_ids = ['123456', '789012', '345678']
  >> responses = fetch_abstracts(pubmed_ids)
  >> print(responses)
  [{'resultList': {'result': [{'abstractText': 'This is the abstract for article 1.'}, ...]}, ...}]
  """
  
  # Initialize variables
  responses = list()

  # Split PubMed IDs into batches of 1000 for processing
  batches = [pubmedIDs[i * 1000:(i + 1) * 1000] for i in range((len(pubmedIDs) + 1000 - 1) // 1000 )]
  
  # Process each batch of PubMed IDs
  for batch in batches:
    # Construct the query for the batch
    ids = ' OR ext_id:'.join(batch)
    ids = 'ext_id:' +ids
    
    # Define the URL and parameters for the Europe PMC API
    url = "https://www.ebi.ac.uk/europepmc/webservices/rest/searchPOST"
    params = {"query": ids, "pageSize": "1000", "resultType": "core", "format": "json"}
    head = {'Content-type': 'application/x-www-form-urlencoded'}
    try:
      # Make the API request to fetch abstracts
      response = requests.post(url=url, data=params, headers=head).json()
      responses.append(response)
      
    except Exception as e:
      print('Cannot append to responses var in fetch_abstract', e)

  return responses



  
def fetch_pubtator_annotations(pubmedIDs,retry=0):
  """
    Fetch annotations from PubTator for a given PubMed ID.

    Parameters:
    - pubmedId (str): The PubMed ID for which annotations will be retrieved.

    Returns:
    dict or None: A dictionary containing PubTator annotations in BioC JSON format,
                 or None if the annotations are not available or an error occurs.

    Example:
    >> pubmed_id = '123456'
    >> annotations = fetch_pubtator_annotations(pubmed_id)
    >> print(annotations)
    {'documents': [{'infons': {}, 'passages': [...], 'annotations': [...], ...}]}
  """
  # Splits pubmedIDs into batches of < 100 due to API limit
  batches = [pubmedIDs[i * 99:(i + 1) * 99] for i in range((len(pubmedIDs) + 99 - 1) // 99 )]
  
  for batch_num, batch in enumerate(batches):
    try:
      print('BATCH NUM::', str(batch_num))

      str_batch = ",".join(batch)
    # Construct the PubTator API URL for the given PubMed ID
      pubtatorUrl = "https://www.ncbi.nlm.nih.gov/research/pubtator-api/publications/export/biocjson?pmids=" + str_batch
      
      # Make a GET request to fetch PubTator annotations
      r = requests.get(pubtatorUrl)
      time.sleep(0.34) #limits to 3 queries a second aka API limit

      # Check if the response is sucessful and not empty
      if (not r or r is None or r ==''):
        print(f'fetch_pubtator_annotations: api response empty or not successful')
        retry += 1
        print('RETRY QUERY:', retry)
        if retry < 6:
          time.sleep(1) #wait 1 second
          fetch_pubtator_annotations(pubmedIDs,retry=retry)
        else:
          yield None
          
      else:
        yield r.json()

    except TimeoutError as e:
      #Retry after a short delay if a timeout error occurs
      print(e)
      continue

    except ValueError as e:
      # Return None if theres an issue parsing the response as JSON
      print(e)
      continue



    
def fetch_pmc_fulltext_xml(pmcId):
  """
    Fetch the full-text XML content from PMC (PubMed Central) for a given PMC ID.

    Parameters:
    - pmcId (str): The PMC ID for which full-text XML content will be retrieved.

    Returns:
    requests.Response: A Response object containing the full-text XML content,
                      which can be accessed using the `.text` attribute.

    Example:
    >> pmc_id = 'PMC123456'
    >> xml_response = fetch_pmc_fulltext_xml(pmc_id)
    >> print(xml_response.text)
    '<?xml version="1.0" encoding="UTF-8"?>\n<...> Full-text XML content ... </...>'
  """

  # Retrieve the NCBI API key from the environment variables
  api_key = os.environ['NCBI_KEY']

  # Construct the PMC API URL for fetching full-text XML
  pmcUrl = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&api_key={api_key}&id=" + pmcId

  # Make a GET request to fetch the full-text XML content
  return requests.get(pmcUrl)




def fetch_pmc_fulltext_json(pubmedId):
  """
    Fetch the full-text JSON content from PMC (PubMed Central) for a given PubMed ID.

    Parameters:
    - pubmedId (str): The PubMed ID for which full-text JSON content will be retrieved.

    Returns:
    dict: A dictionary containing the full-text JSON content.

    Example:
    >> pubmed_id = '123456'
    >> json_content = fetch_pmc_fulltext_json(pubmed_id)
    >> print(json_content)
    {'collection': {'date': '2023-01-01', 'source': 'PubMed Central', 'documents': [...], ...}}
  """

  # Construct the PMC API URL for fetching full-text JSON 
  pmcUrl = "https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi/BioC_json/" + pubmedId + "/unicode"

  # Make a GET request to fetch the full-text JSON content
  return requests.urlopen(pmcUrl).json()




def create_disease(session, gard_id, rd):
  """
    Create or update a disease node in the Neo4j graph database with GARD (Genetic and Rare Diseases) information.

    Parameters:
    - session (neo4j.Session): The Neo4j database session.
    - gard_id (str): The GARD ID of the disease.
    - rd (dict): A dictionary containing information about the disease, including:
      - 'gard_id' (str): GARD ID (same as parameter gard_id).
      - 'name' (str): The name of the disease.
      - 'type' (str): The type or category of the disease.
      - 'classification' (str): The classification of the disease.
      - 'synonyms' (list): A list of synonyms for the disease.

    Returns:
    int or None: The unique identifier (ID) of the created or updated disease node in the Neo4j database.
                Returns None if an error occurs during database interaction.

    Example:
    >> session = neo4j_driver.session()
    >> gard_id = 'C12345'
    >> disease_info = {
    ...     'gard_id': 'C12345',
    ...     'name': 'Example Disease',
    ...     'type': 'Genetic',
    ...     'classification': 'Rare Disease',
    ...     'synonyms': ['Disease X', 'Rare Genetic Condition']
    ... }
    >> node_id = create_disease(session, gard_id, disease_info)
    >> print(node_id)
    123
  """

  query = '''
  MERGE (d:GARD {GardId:$gard_id}) 
  ON CREATE SET
    d.GardId = $gard_id,
    d.GardName = $name,
    d.Classification = $classification, 
    d.Synonyms = $synonyms,
    d.Type = $type
  RETURN ID(d)
  '''
  params = {
    "gard_id":gard_id,
    "gard_id":rd['gard_id'],
    "name":rd['name'], 
    "type":rd['type'],
    "classification":rd['classification'],
    "synonyms":rd['synonyms']
  }

  # Execute the Cypher query and retrieve the ID of the created or updated node
  return session.run(query, args=params).single().value()


def get_nhsExtract(texts, url=f"{sysvars.nhsapi_url}v1/predict"):
  try:
    nhs_info = dict(requests.post(url, json={'texts': texts}, verify=False).json())
    return nhs_info['predictions'][0]

  except Exception as e:
    logging.error(f'Exception during get_nhsExtract. texts: {texts}, error: {e}')
    return 0


def create_article(tx, abstractDataRel, disease_node, search_source, maxdate):
  """
    Create an Article node in the Neo4j graph database and establish a MENTIONED_IN relationship with a GARD node.

    Parameters:
    - tx (neo4j.Transaction): The Neo4j database transaction.
    - abstractDataRel (dict): A dictionary containing information about the article, including PubMed ID, title, abstract, etc.
    - disease_node (int): The unique identifier of the GARD node to which the article is mentioned.
    - search_source (str): The source of the search that led to the discovery of this article.
    - maxdate (str): The maximum date for filtering the search results.

    Returns:
    int or None: The unique identifier (ID) of the created Article node.
                Returns None if an error occurs during database interaction.

    Example:
    >> session = neo4j_driver.session()
    >> abstract_data = {'pmid': '123456', 'title': 'Example Article', 'abstractText': 'This is an abstract.', ...}
    >> gard_node_id = 456
    >> source = 'PubMed'
    >> max_date = '2023/01/01'
    >> article_id = create_article(session.write_transaction, abstract_data, gard_node_id, source, max_date)
    >> print(article_id)
    789
  """

  # Creating an article with search_source as 'OMIM' implies that there is no pubmed source available therefore ReferenceOrigin would just be ['OMIM']
  create_article_query = '''
  MATCH (d:GARD) WHERE ID(d)=$id
  MERGE (n:Article {pubmed_id:$pubmed_id})
  ON CREATE SET
    n.pubmed_id = $pubmed_id,
    n.doi = $doi, 
    n.title = $title, 
    n.abstractText = $abstractText, 
    n.affiliation = $affiliation, 
    n.firstPublicationDate = $firstPublicationDate,
    n.publicationYear = $year,
    n.citedByCount = $citedByCount,
    n.isOpenAccess = $isOpenAccess, 
    n.inEPMC = $inEPMC, 
    n.inPMC = $inPMC, 
    n.hasPDF = $hasPDF, 
    n.source = $source,
    n.pubType = $pubtype,
    n.isEpi = $epi,
    n.epi_processed = $epi_proc,
    n.DateCreatedRDAS = $now,
    n.LastUpdatedRDAS = $now,
    n.isNHS = $isNHS,
    n.ReferenceOrigin = [\'''' + search_source + '''\']
  MERGE (d)-[r:MENTIONED_IN]->(n)
  RETURN ID(n)
  '''

  # Gather NHS API data
  title = abstractDataRel['title']
  abstract = abstractDataRel['abstractText']
  isNHS = False

  if title and abstract:
      results = get_nhsExtract([title + abstract])

      if results == 1:
        print('isNHS')
        isNHS = True
      else:
        isNHS = False

  elif title and not abstract:
      results = get_nhsExtract([title])

      if results == 1:
        print('isNHS')
        isNHS = True
      else:
        isNHS = False

  elif abstract and not title:
      results = get_nhsExtract([abstract])

      if results == 1:
        print('isNHS')
        isNHS = True
      else:
        isNHS = False

  params={
    "id":disease_node,
    "pubmed_id":abstractDataRel['pmid'] if 'pmid' in abstractDataRel else '',
    "source":abstractDataRel['source'] if 'source' in abstractDataRel else '',
    "doi":abstractDataRel['doi'] if 'doi' in abstractDataRel else '',
    "title":abstractDataRel['title'] if 'title' in abstractDataRel else '',
    "abstractText":abstractDataRel['abstractText'] if 'abstractText' in abstractDataRel else '',
    "affiliation":abstractDataRel['affiliation'] if 'affiliation' in abstractDataRel else '',
    "firstPublicationDate":abstractDataRel['firstPublicationDate'] if 'firstPublicationDate' in abstractDataRel else '',
    "year":str(datetime.strptime(abstractDataRel['firstPublicationDate'], '%Y-%m-%d').year) if 'firstPublicationDate' in abstractDataRel else '',
    "isOpenAccess": True if 'isOpenAccess' in abstractDataRel else False,
    "inEPMC": True if 'inEPMC' in abstractDataRel else False,
    "inPMC":True if 'inPMC' in abstractDataRel else False,
    "hasPDF":True if 'hasPDF' in abstractDataRel else False,
    "epi": False,
    "epi_proc": False,
    "pubtype":abstractDataRel['pubTypeList']['pubType'] if 'pubTypeList' in abstractDataRel else '',
    "now": datetime.strptime(maxdate,"%Y/%m/%d").strftime("%m/%d/%y"), #"isEpi": False
    "citedByCount":int(abstractDataRel['citedByCount']) if 'citedByCount' in abstractDataRel else 0,
    "isNHS":isNHS
    }
  
  # Execute the Cypher query and retrieve the ID of the created Article node
  response = tx.run(create_article_query, args=params).single().value()
  print('+', end='', flush=True)
  return response




def create_authors(tx, abstractDataRel, article_node, omim=False):
  """
    Create Author nodes and WROTE relationships in the Neo4j graph database based on the provided article information.

    Parameters:
    - tx (neo4j.Transaction): The Neo4j database transaction.
    - abstractDataRel (dict): A dictionary containing information about the article, including author details.
    - article_node (int): The unique identifier of the Article node to which the authors will be related.

    Returns:
    None

    Example:
    >> session = neo4j_driver.session()
    >> abstract_data = {'authorList': {'author': [{'fullName': 'John Doe'}, {'fullName': 'Jane Smith'}]}, ...}
    >> article_id = 789
    >> create_authors(session.write_transaction, abstract_data, article_id)
  """

  if omim:
    create_author_query = '''
      MATCH (a:Article) WHERE ID(a) = $article_id
      MERGE (p:Author {fullName:$fullName, firstName:$firstName, lastName:$lastName})
      MERGE (p) - [r:WROTE] -> (a)
      '''
    tx.run(create_author_query, args={
      "article_id":article_node,
      "fullName": abstractDataRel['fullName'] if 'fullName' in abstractDataRel else '',
      "firstName": abstractDataRel['firstName'] if 'firstName' in abstractDataRel else '',
      "lastName": abstractDataRel['lastName'] if 'lastName' in abstractDataRel else ''
    })
  else:
    create_author_query = '''
      MATCH (a:Article) WHERE ID(a) = $article_id
      MERGE (p:Author {fullName:$fullName, firstName:$firstName, lastName:$lastName, affiliation:$affiliation, orc_id:$orc_id})
      MERGE (p) - [r:WROTE] -> (a)
      '''
    
    for author in abstractDataRel['authorList']['author']:
      affiliation = None
      auth_val = None
      if 'collectiveName' in author:
          continue

      if not 'authorAffiliationDetailsList' and 'authorId' in author:
          continue

      if 'authorAffiliationDetailsList' in author:
          affiliation_data = author['authorAffiliationDetailsList']['authorAffiliation']
          affiliation = [aff['affiliation'] for aff in affiliation_data]

      if 'authorId' in author:
          author_id_info = author['authorId']
          auth_type = author_id_info['type']
          if auth_type == 'ORCID':
              auth_val = author_id_info['value']
      try:
        tx.run(create_author_query, args={
          "article_id":article_node,
          "fullName": author['fullName'] if 'fullName' in author else '',
          "firstName": author['firstName'] if 'firstName' in author else '',
          "lastName": author['lastName'] if 'lastName' in author else '',
          "affiliation": affiliation if affiliation else '',
          "orc_id": auth_val if auth_val else ''
        })
      except Exception as e:
        print(e)




def create_journal(tx, abstractDataRel, article_node):
  """
    Create Journal and JournalVolume nodes, and establish APPEARS_IN and CONTENT_OF relationships in the Neo4j graph database.

    Parameters:
    - tx (neo4j.Transaction): The Neo4j database transaction.
    - abstractDataRel (dict): A dictionary containing information about the article, including journal details.
    - article_node (int): The unique identifier of the Article node to which the journal information will be related.

    Returns:
    None

    Example:
    >> session = neo4j_driver.session()
    >> abstract_data = {'issue': '1', 'volume': '10', 'dateOfPublication': '2023-01-01', 'journal': {'title': 'Example Journal', 'issn': '1234-5678'}, ...}
    >> article_id = 789
    >> create_journal(session.write_transaction, abstract_data, article_id)
  """

  create_journal_query = '''
  MATCH (a:Article) WHERE ID(a) = $article_id
  MERGE (j:Journal{title:$title,medlineAbbreviation:$medlineAbbreviation,essn:$essn,issn:$issn,nlmid:$nlmid})
  MERGE (ji:JournalVolume{issue:$issue, volume:$volume, journalIssueId:$journalIssueId,
    dateOfPublication:$dateOfPublication, monthOfPublication:$monthOfPublication,yearOfPublication:$yearOfPublication,
    printPublicationDate:$printPublicationDate})
  MERGE (a)-[:APPEARS_IN]->(ji)
  MERGE (ji)-[:CONTENT_OF]->(j)
  '''
  tx.run(create_journal_query, args={
    "article_id":article_node,
    "issue": abstractDataRel['issue'] if 'issue' in abstractDataRel else '',
    "volume": abstractDataRel['volume'] if 'volume' in abstractDataRel else '',
    "journalIssueId": abstractDataRel['journalIssueId'] if 'journalIssueId' in abstractDataRel else '',
    "dateOfPublication": abstractDataRel['dateOfPublication'] if 'dateOfPublication' in abstractDataRel else '',
    "monthOfPublication": abstractDataRel['monthOfPublication'] if 'monthOfPublication' in abstractDataRel else '',
    "yearOfPublication": abstractDataRel['yearOfPublication'] if 'yearOfPublication' in abstractDataRel else '',
    "printPublicationDate": abstractDataRel['printPublicationDate'] if 'printPublicationDate' in abstractDataRel else '',
    "title": abstractDataRel['journal']['title'] if 'title' in abstractDataRel['journal'] else '',
    "medlineAbbreviation": abstractDataRel['journal']['medlineAbbreviation'] if 'medlineAbbreviation' in abstractDataRel['journal'] else '',
    "essn": abstractDataRel['journal']['essn'] if 'essn' in abstractDataRel['journal'] else '',
    "issn": abstractDataRel['journal']['issn'] if 'issn' in abstractDataRel['journal'] else '',
    "nlmid": abstractDataRel['journal']['nlmid'] if 'nlmid' in abstractDataRel['journal'] else ''
  })




def create_keywords(tx, abstractDataRel, article_node):
  """
    Create Keyword nodes and KEYWORD_FOR relationships in the Neo4j graph database based on the provided article information.

    Parameters:
    - tx (neo4j.Transaction): The Neo4j database transaction.
    - abstractDataRel (dict): A dictionary containing information about the article, including keywords.
    - article_node (int): The unique identifier of the Article node to which the keywords will be related.

    Returns:
    None

    Example:
    >> session = neo4j_driver.session()
    >> abstract_data = {'keywords': ['Keyword1', 'Keyword2', 'Keyword3'], ...}
    >> article_id = 789
    >> create_keywords(session.write_transaction, abstract_data, article_id)
  """

  create_keyword_query = '''
  MATCH (a:Article) WHERE ID(a) = $article_id
  MERGE (k:Keyword {keyword:$keyword}) 
  MERGE (k)- [r:KEYWORD_FOR] -> (a)
  '''
  # Some articles have all the keywords in one field, therefore we must convert the text to a list if needed
  for keyword_field in abstractDataRel:
    if keyword_field:
      #keyword_field_list = [x.strip() for x in keyword_field.split(', ')]
      for keyword in keyword_field:
        keyword = keyword.lower()
        tx.run(create_keyword_query, args={
          "article_id":article_node,      
          "keyword": keyword
        })




def get_isEpi(text, url=f"{sysvars.epiapi_url}postEpiClassifyText/"):
  """
    Check if the given text corresponds to an epidemiology article using an external API.

    Parameters:
    - text (str): The text content to be analyzed.
    - url (str): The URL of the external API endpoint for epidemiology classification.
                 Defaults to the URL specified in the sysvars module.

    Returns:
    bool: True if the article is classified as epidemiology, False otherwise.

    Example:
    >> article_text = "This is an epidemiology article."
    >> is_epidemiology = get_isEpi(article_text)
    >> print(is_epidemiology)
    True
  """

  try:
    # Send a POST request to the external API for epidemiology classification
    response = requests.post(url, json={'text': text})
    print(response.status_code)
    print(response)

    # Parse the JSON response
    response = response.json()
    print(response)

    # Check if 'isEpi' is present in the response
    if 'IsEpi' in response:
        return {'isEpi':response['IsEpi'], 'probability':response['EPI_PROB']}
    else:
        return {'isEpi':response['IsEpi'], 'probability':None}

  except Exception as e:
    logging.error(f'Exception during get_isEpi 2. text: {text}, error: {e}')




def get_epiExtract(text, url=f"{sysvars.epiapi_url}postEpiExtractText/"):
  """
    Extract epidemiological information from the given text using an external API.

    Parameters:
    - text (str): The text content to be analyzed.
    - url (str): The URL of the external API endpoint for epidemiological information extraction.
                 Defaults to the URL specified in the sysvars module.

    Returns:
    dict: A dictionary containing epidemiological information extracted from the text.

    Example:
    >> article_text = "This is an epidemiology article."
    >> epi_info = get_epiExtract(article_text)
    >> print(epi_info)
    {'DATE': ['1989'], 'LOC': ['Uruguay', 'Brazil'], 'STAT': ['1 in 10000', 1/83423], ...}
  """

  try:
    # Send a POST request to the external API for epidemiological information extraction
    epi_info = dict(requests.post(url, json={'text': text,'extract_diseases':False}, verify=False).json())
    return epi_info

  except Exception as e:
    logging.error(f'Exception during get_isEpi 1. text: {text}, error: {e}')
    return None




def create_epidemiology(tx, abstractDataRel, article_node, today):
  """
    Add isEpi to the Article node and create an EpidemiologyAnnotation node if the article is classified as epidemiology.

    Parameters:
    - tx (neo4j.Transaction): The Neo4j database transaction.
    - abstractDataRel (dict): A dictionary containing information about the article, including title and abstract.
    - article_node (int): The unique identifier of the Article node.
    - today (str): The current date in the format 'YYYY/MM/DD'.

    Returns:
    None

    Example:
    >> session = neo4j_driver.session()
    >> abstract_data = {'title': 'Example Article', 'abstractText': 'This is an abstract.', ...}
    >> article_id = 789
    >> current_date = '2023/01/01'
    >> create_epidemiology(session.write_transaction, abstract_data, article_id, current_date)
  """

  text = abstractDataRel['title'] + ' ' + abstractDataRel['abstractText']
  
  # Check if the article is classified as epidemiology
  
  epi_data = get_isEpi(text)
  if not epi_data:
    return

  if epi_data['isEpi']:
    print(epi_data['isEpi'])
    epi_info = get_epiExtract(text)
    print('getEpi Info: ', str(epi_info), article_node)

    # Check if any values in epi_info are not empty
    if epi_info and sum([1 for x in epi_info.values() if x]) > 0:
      try:
        create_epidemiology_query = '''
          MATCH (a:Article) WHERE ID(a) = $article_id
          SET a.isEpi = $isEpi
          SET a.epi_processed = TRUE
          MERGE (n:EpidemiologyAnnotation {isEpi:$isEpi, epidemiology_type:$epidemiology_type, epidemiology_rate:$epidemiology_rate, date:$date, location:$location, sex:$sex, ethnicity:$ethnicity, DateCreatedRDAS:$rdascreated, LastUpdatedRDAS:$rdasupdated})
          MERGE (n) -[r:EPIDEMIOLOGY_ANNOTATION_FOR {epidemiology_probability:$epi_prob}]-> (a)
          '''
        tx.run(create_epidemiology_query, args={
          "article_id":article_node,
          "isEpi": epi_data['isEpi'],
          "epi_prob": epi_data['probability'],
          "epidemiology_type":epi_info['EPI'] if epi_info['EPI'] else [], 
          "epidemiology_rate":epi_info['STAT'] if epi_info['STAT'] else [], 
          "date":epi_info['DATE'] if epi_info['DATE'] else [], 
          "location":epi_info['LOC'] if epi_info['LOC'] else [], 
          "sex":epi_info['SEX'] if epi_info['SEX'] else [], 
          "ethnicity":epi_info['ETHN'] if epi_info['ETHN'] else [],
          "rdascreated": datetime.strptime(today,"%Y/%m/%d").strftime("%m/%d/%y"),
          "rdasupdated": datetime.strptime(today,"%Y/%m/%d").strftime("%m/%d/%y")
        })
      except Exception as e:
        logging.error(f'Exception during tx.run(create_epidemiology_query) where isEpi is True.')
        raise e
      
    # If all the epi_info values are empty, set is_Epi to false
    else:
      try:
        create_epidemiology_query = '''
            MATCH (a:Article) WHERE ID(a) = $article_id
            SET a.isEpi = $isEpi
            SET a.epi_processed = TRUE
            '''
        tx.run(create_epidemiology_query, args={
          "article_id":article_node,
          "isEpi": epi_data['isEpi']
        })

      except Exception as e:
        logging.error(f'Exception during tx.run(create_epidemiology_query) where isEpi is True, but no epi_info.')
        raise e

  # Update the Article node with isEpi information
  else:
    create_epidemiology_query = '''
        MATCH (a:Article) WHERE ID(a) = $article_id
        SET a.isEpi = $isEpi
        SET a.epi_processed = TRUE
        '''
    try:
      tx.run(create_epidemiology_query, args={"article_id":article_node, 'isEpi': epi_data['isEpi']})
    except Exception as e:
      logging.error(f'Exception during tx.run(create_epidemiology_query) where isEpi is False.')
      raise e




def create_fullTextUrls(tx, abstractDataRel, article_node):
  """
    Create FullTextUrl nodes and associate them with the specified Article node in the Neo4j database.

    Parameters:
    - tx (neo4j.Transaction): The Neo4j database transaction.
    - abstractDataRel (dict): A dictionary containing information about the article, including full-text URLs.
    - article_node (int): The unique identifier of the Article node.

    Returns:
    None

    Example:
    >> session = neo4j_driver.session()
    >> abstract_data = {'fullTextUrls': [{'availability': 'Open Access', 'url': 'http://example.com/fulltext'}], ...}
    >> article_id = 789
    >> create_fullTextUrls(session.write_transaction, abstract_data, article_id)
  """

  create_fullTextUrls_query = '''
  MATCH (a:Article) WHERE ID(a) = $article_id
  MERGE (u:FullTextUrl {availability:$availability, availabilityCode:$availabilityCode, documentStyle:$documentStyle,site:$site,url:$url})
  MERGE (u) - [r:CONTENT_FOR] -> (a)
  '''
  for fullTextUrl in abstractDataRel:
    tx.run(create_fullTextUrls_query, args={
      "article_id":article_node,
      "availability": fullTextUrl['availability'] if 'availability' in fullTextUrl else '',
      "availabilityCode": fullTextUrl['availabilityCode'] if 'availabilityCode' in fullTextUrl else '',
      "documentStyle": fullTextUrl['documentStyle'] if 'documentStyle' in fullTextUrl else '',
      "site": fullTextUrl['site'] if 'site' in fullTextUrl else '',
      "url": fullTextUrl['url'] if 'url' in fullTextUrl else ''
    })




def create_meshHeadings(tx, abstractDataRel, article_node):
  """
    Create MeshTerm and MeshQualifier nodes and associate them with the specified Article node in the Neo4j database.

    Parameters:
    - tx (neo4j.Transaction): The Neo4j database transaction.
    - abstractDataRel (dict): A dictionary containing information about the article, including MeshHeadings.
    - article_node (int): The unique identifier of the Article node.

    Returns:
    None

    Example:
    >> session = neo4j_driver.session()
    >> abstract_data = {'meshHeadings': [{'descriptorName': 'Disease', 'majorTopic_YN': 'Y', 'meshQualifierList': ...}], ...}
    >> article_id = 789
    >> create_meshHeadings(session.write_transaction, abstract_data, article_id)
  """

  create_meshHeadings_query = '''
  MATCH (a:Article) WHERE ID(a) = $article_id
  MERGE (m:MeshTerm {isMajorTopic:$isMajorTopic, descriptorName:$descriptorName}) 
  MERGE (m) - [r:MESH_TERM_FOR] -> (a)
  RETURN ID(m)
  '''

  create_meshQualifiers_query = '''
  MATCH (m:MeshTerm) WHERE ID(m) = $meshHeading_id
  MERGE (mq:MeshQualifier {abbreviation:$abbreviation, qualifierName:$qualifierName, isMajorTopic:$isMajorTopic}) 
  MERGE (mq) - [r:MESH_QUALIFIER_FOR] -> (m)
  '''

  for meshHeading in abstractDataRel:
    if 'majorTopic_YN' in meshHeading:
      # Convert the MeSH majorTopic_YN property to a boolean isMajorTopic
      isMajorTopic = True if meshHeading['majorTopic_YN'] == 'Y' else False # Default to False if majorTopic_YN is not present

    parameters={
      "article_id":article_node,
      "isMajorTopic": isMajorTopic,
      "descriptorName": meshHeading['descriptorName'] if 'descriptorName' in meshHeading else ''
    }

    # Create MeshTerm and get its unique identifier
    txout = tx.run(create_meshHeadings_query, args=parameters).single()

    if (txout):
      meshHeadingId = txout.value()
      if ('meshQualifierList' in meshHeading and 'meshQualifier' in meshHeading['meshQualifierList']):
        for meshQualifier in meshHeading['meshQualifierList']['meshQualifier']:
          if 'majorTopic_YN' in meshQualifier:
            # Convert MeSH majorTopic_YN property to a boolean isMajorTopic
            isMajorTopic = True if meshQualifier['majorTopic_YN'] == 'Y' else False

          # Create MeshQualifier and associate it with MeshTerm
          tx.run(create_meshQualifiers_query, args={
          "meshHeading_id":meshHeadingId,
          "abbreviation": meshQualifier['abbreviation'] if 'abbreviation' in meshQualifier else '',
          "qualifierName": meshQualifier['qualifierName'] if 'qualifierName' in meshQualifier else '',
          "isMajorTopic": isMajorTopic,
          })




def create_chemicals(tx, abstractDataRel, article_node):
  """
    Create Substance nodes and associate them with the specified Article node in the Neo4j database.

    Parameters:
    - tx (neo4j.Transaction): The Neo4j database transaction.
    - abstractDataRel (dict): A dictionary containing information about the article, including chemicals.
    - article_node (int): The unique identifier of the Article node.

    Returns:
    None

    Example:
    >> session = neo4j_driver.session()
    >> abstract_data = {'chemicals': [{'name': 'Chemical A', 'registryNumber': '12345-67-8'}, ...], ...}
    >> article_id = 789
    >> create_chemicals(session.write_transaction, abstract_data, article_id)
  """

  create_chemicals_query = '''
  MATCH (a:Article) WHERE ID(a) = $article_id
  MERGE (u:Substance {name:$name, registryNumber:$registryNumber}) MERGE (u)-[r:SUBSTANCE_ANNOTATED_BY_PUBMED]->(a)
  '''

  for chemical in abstractDataRel:
    tx.run(create_chemicals_query, args={
      "article_id":article_node,
      "name": chemical['name'].lower() if 'name' in chemical else '',
      "registryNumber": chemical['registryNumber'] if 'registryNumber' in chemical else '',
    })




def create_annotations(tx, pubtatorData, article_node, today):
  """
    Create PubtatorAnnotation nodes and associate them with the specified Article node in the Neo4j database.

    Parameters:
    - tx (neo4j.Transaction): The Neo4j database transaction.
    - pubtatorData (dict): A dictionary containing PubTator annotations for the article.
    - article_node (int): The unique identifier of the Article node.
    - today (datetime.date): The current date.

    Returns:
    None

    Example:
    >> session = neo4j_driver.session()
    >> pubtator_data = {'passages': [{'infons': {...}, 'annotations': [...]}, ...]}
    >> article_id = 123
    >> current_date = datetime.date.today()
    >> create_annotations(session.write_transaction, pubtator_data, article_id, current_date)
  """

  if pubtatorData:
    for passage in pubtatorData['passages']:
      type = passage['infons']['type'] if 'type' in passage['infons'] else ''

      for annotation in passage['annotations']:
        parameters={
          "article_id":article_node,
          "type":type,
          "infons_identifier": annotation['infons']['identifier'] if ('identifier' in annotation['infons'] and annotation['infons']['identifier'])  else '',
          "infons_type": annotation['infons']['type'] if ('type' in annotation['infons'] and annotation['infons']['type']) else '',
          "text": annotation['text'] if 'text' in annotation else '',
          "rdascreated": datetime.strptime(today, "%Y/%m/%d").strftime("%m/%d/%y"),
          "rdasupdated": datetime.strptime(today, "%Y/%m/%d").strftime("%m/%d/%y")
        }

        # Split the 'text' field into a list if it contains commas
        temp = parameters['text']
        if len(temp) > 0:
          try:
            temp = temp.split(",")
          except:
            pass
        parameters['text'] = [x.lower() for x in temp] #lowercases all elements in list

        # Check for other connected pubtator annotation relationships and identify the type sources ('title', 'abstract', or 'title and abstract')
        # Ex. List of Values; if value is 'title and abstract' then it will be ['title','abstract']
        check = tx.run('MATCH (pa:PubtatorAnnotation {{ text: {text}, infons_type: \'{infons_type}\', infons_identifier: \'{infons_identifier}\' }})-[r:ANNOTATION_FOR]->(a:Article) WHERE ID(a) = {article_id} RETURN DISTINCT r.type as rel_type, ID(r) as rel_id'
                      .format(text=parameters['text'],
                      article_id=parameters['article_id'],
                      infons_type=parameters['infons_type'],
                      infons_identifier=parameters['infons_identifier'],
                      type=parameters['type'])).data()
        
        if len(check) > 0:
          existing_type = check[0]['rel_type'] # is a list
          incoming_type = parameters['type']
          existing_id = check[0]['rel_id']
          
          if existing_type == ['Abstract'] and incoming_type == 'abstract':
            continue
          if existing_type == ['Title'] and incoming_type == 'title':
            continue
          if existing_type == ['Title', 'Abstract'] and incoming_type == 'title and abstract':
            continue
          if existing_type == ['Title', 'Abstract'] and incoming_type == 'title':
            continue
          if existing_type == ['Title', 'Abstract'] and incoming_type == 'abstract':
            continue

          if existing_type == ['Title'] and incoming_type == 'abstract':
            parameters['type'] = ['Title', 'Abstract']
          elif existing_type == ['Abstract'] and incoming_type == 'title':
            parameters['type'] = ['Title', 'Abstract']

          tx.run('MATCH ()-[r:ANNOTATION_FOR]->() WHERE ID(r) = {existing_id} SET r.type = {new_type}'.format(existing_id=existing_id, new_type=parameters['type']))
          continue

        else:
          type_temp = parameters['type']
          if type_temp == 'title and abstract':
            parameters['type'] = ['Title', 'Abstract']
          elif type_temp == 'title':
            parameters['type'] = ['Title']
          elif type_temp == 'abstract':
            parameters['type'] = ['Abstract']
          

        # Develop Neo4j Query to Populate Annotations (New Node Only)
        create_annotations_query = '''
          MATCH (a:Article) WHERE ID(a) = {article_id}
          MERGE (pa:PubtatorAnnotation {{ text: {text}, infons_type: \'{infons_type}\', infons_identifier: \'{infons_identifier}\' }})
          ON CREATE
            SET pa.infons_identifier = \'{infons_identifier}\'
            SET pa.DateCreatedRDAS = \'{rdascreated}\'
            SET pa.LastUpdatedRDAS = \'{rdasupdated}\'
            SET pa.text = {text}
            SET pa.infons_type = \'{infons_type}\'
          MERGE (pa)-[r:ANNOTATION_FOR {{ type: {type} }} ]-> (a)
          '''.format(text=parameters['text'],
                    article_id=parameters['article_id'],
                    infons_type=parameters['infons_type'],
                    rdasupdated=parameters['rdasupdated'],
                    rdascreated=parameters['rdascreated'],
                    infons_identifier=parameters['infons_identifier'],
                    type=parameters['type'])
        
        # Execute the Cypher query to create PubtatorAnnotation nodes and associate them with the Article node
        txout = tx.run(create_annotations_query)




def create_disease_article_relation(tx, disease_node, article_node):
  """
    Create a relationship between a Disease (GARD) node and an Article node in the Neo4j database.

    Parameters:
    - tx (neo4j.Transaction): The Neo4j database transaction.
    - disease_node (int): The unique identifier of the Disease (GARD) node.
    - article_node (int): The unique identifier of the Article node.

    Returns:
    None

    Example:
    >> session = neo4j_driver.session()
    >> disease_id = 456
    >> article_id = 789
    >> create_disease_article_relation(session.write_transaction, disease_id, article_id)
  """

  # Cypher query to create the relationship
  query = '''
  MATCH (a: Article) WHERE ID(a) = $article_id
  MATCH (d: GARD) WHERE ID(d) = $disease_id
  MERGE (d)-[:MENTIONED_IN]->(a)
  '''

  # Run the query with parameters
  tx.run(query, parameters={
    "article_id":article_node,
    "disease_id":disease_node,
  })




def save_disease_article_relation(disease_node, article_node, session):
  """
    Save a relationship between a Disease (GARD) node and an Article node in the Neo4j database.

    Parameters:
    - disease_node (int): The unique identifier of the Disease (GARD) node.
    - article_node (int): The unique identifier of the Article node.
    - session (neo4j.Session): The Neo4j database session.

    Returns:
    None

    Example:
    >> neo4j_session = neo4j_driver.session()
    >> disease_id = 456
    >> article_id = 789
    >> save_disease_article_relation(disease_id, article_id, neo4j_session)
  """

  # Start a new transaction
  tx = session.begin_transaction()
  logging.info(f'Create Disease - Article relation')

  # Call the function to create the relationship
  create_disease_article_relation(tx, disease_node, article_node)

  # Commit the transaction
  tx.commit()


def save_all_additional_nodes(session, abstract, article_node):
  # Create MeshHeadings nodes and relationships
    if ('meshHeadingList' in abstract and 
    'meshHeading' in abstract['meshHeadingList']):
      logging.info(f'Invoking create_meshHeading')
      create_meshHeadings(session, abstract['meshHeadingList']['meshHeading'], article_node)

    # Create Authors nodes and relationships
    if ('authorList' in abstract and 
    'author' in abstract['authorList']):
      logging.info(f'Invoking create_authors')
      create_authors(session, abstract, article_node)
    
    # Create Journal nodes and relationships
    if ('journalInfo' in abstract):
      logging.info(f'Invoking create_journal')
      create_journal(session, abstract['journalInfo'], article_node)
    
    # Create Keywords nodes and relationships
    if ('keywordList' in abstract and 
    'keyword' in abstract['keywordList']):
      logging.info(f'Invoking create_keywords')
      create_keywords(session, abstract['keywordList']['keyword'], article_node)

    # Create FullTextUrls nodes and relationships
    if ('fullTextUrlList' in abstract and 
    'fullTextUrl' in abstract['fullTextUrlList']):
      logging.info(f'Invoking create_fullTextUrls')
      create_fullTextUrls(session, abstract['fullTextUrlList']['fullTextUrl'], article_node)
    
    # Create Chemicals nodes and relationships
    if ('chemicalList' in abstract and 
    'chemical' in abstract['chemicalList']):
      logging.info(f'Invoking create_chemical')
      create_chemicals(session, abstract['chemicalList']['chemical'], article_node)

      
def save_all(abstract, disease_node, pubmedID, search_source, session, maxdate):
    """
    Save various components of an article, including creating Article, MeshHeadings, Authors, Journal, Keywords,
    FullTextUrls, and Chemicals nodes and relationships in the Neo4j database.

    Parameters:
    - abstract (dict): The abstract data containing information about the article.
    - disease_node (int): The unique identifier of the Disease (GARD) node.
    - pubmedID (str): The PubMed ID of the article.
    - search_source (str): The source used for searching the article.
    - session (neo4j.Session): The Neo4j database session.
    - maxdate (str): The maximum date for the article.

    Returns:
    None

    Example:
    >> neo4j_session = neo4j_driver.session()
    >> abstract_data = {...}  # Replace with actual abstract data
    >> disease_id = 456
    >> pubmed_id = "12345678"
    >> source = "PubMed"
    >> max_date = "2023-11-20"
    >> save_all(abstract_data, disease_id, pubmed_id, source, neo4j_session, max_date)
    """

    logging.info(f'Invoking create_article')

    # Create an Article node and get its identifier
    article_node = create_article(session, abstract, disease_node, search_source, maxdate)
    save_all_additional_nodes(session, abstract, article_node)





def save_articles(disease_node, pubmed_ids, search_source, session, maxdate):
  """
    Fetches abstracts for a list of PubMed IDs, and saves relevant information in the Neo4j database.
    Connects articles to the specified disease (GARD) node and creates or updates various nodes and relationships.

    Parameters:
    - disease_node (int): The unique identifier of the Disease (GARD) node.
    - pubmed_ids (list): List of PubMed IDs for which abstracts will be fetched and processed.
    - search_source (str): The source used for searching the articles.
    - session (neo4j.Session): The Neo4j database session.
    - maxdate (str): The maximum date for the articles.

    Returns:
    None

    Example:
    >> neo4j_session = neo4j_driver.session()
    >> disease_id = 456
    >> pubmed_ids_list = ["12345678", "98765432"]
    >> source = "PubMed"
    >> max_date = "2023-11-20"
    >> save_articles(disease_id, pubmed_ids_list, source, neo4j_session, max_date)
  """

  # Fetch abstracts for the given PubMed IDs
  all_abstracts = fetch_abstracts(pubmed_ids)

  # If no abstracts are fetched, return early
  if all_abstracts == None:
    return

  # Iterate through the fetched abstracts
  for abstracts in all_abstracts:
    if ('resultList' in abstracts and 'result' in abstracts['resultList'] and len(abstracts['resultList']['result']) > 0):
      try:
        # Iterate through the results in the fetched abstracts
        for result in abstracts['resultList']['result']:
          pubmedID = result['id'] if 'id' in result else None
          logging.info(f'pubmedID: {pubmedID}')
        
          # Skip to the next iteration if PubMed ID is None or title = Personal Communication.
          if pubmedID is None:
            print('.', end='', flush=True)
            continue
          else:
            # Check if the article already exists in the database
            res = session.run("match(a:Article{pubmed_id:$pmid}) return ID(a) as id", args = {'pmid':pubmedID})
            record = res.single()

            # Makes a connection to the GARD disease if the article already exists in the database
            if (record):
              session.run("MATCH (a:Article{pubmed_id:$pmid}) MATCH (g:GARD) WHERE ID(g) = $gard MERGE (g)-[:MENTIONED_IN]->(a)", args = {'pmid':pubmedID, 'gard':disease_node})
              print(f'o', end="", flush=True)
            else:
              try:
                # Call the function to create/update nodes and relationships for the article
                save_all(result, disease_node, pubmedID, search_source, session, maxdate)
              except Exception as e:
                logging.error(f" Exception when calling save_all, error: {e}")
              
      except Exception as e:
        logging.error(f" Exception when iterating abstracts['resultList']['result'], result: {result}, error: {e}")  

    else:
      # No data was returned from fetch_abstracts, skips adding them to the database
      print(f'No data from function fetch_abstracts was returned for PubMed IDs: ', pubmed_ids)



def filter_existing(db, gard_id, pmids):
  """
    Filters out existing articles in the Neo4j database for a specific Disease (GARD) node.

    Parameters:
    - db (neo4j.Session): The Neo4j database session.
    - gard_id (str): The unique identifier of the Disease (GARD) node.
    - pmids (list): List of PubMed IDs to be filtered.

    Returns:
    list or None: A list of PubMed IDs that are not present in the Neo4j database for the specified Disease (GARD) node.
                  Returns None if all PubMed IDs are already in the database.

    Example:
    >> neo4j_session = neo4j_driver.session()
    >> disease_id = "C123456"
    >> pubmed_ids_list = ["12345678", "98765432"]
    >> filtered_pmids = filter_existing(neo4j_session, disease_id, pubmed_ids_list)
    >> print(filtered_pmids)
  """

  # Check if there are any articles in the Neo4j database
  check_query = f'MATCH (x:GARD)--(y:Article) WHERE x.GardId = \"{gard_id}\" WITH count(y) AS cnt RETURN cnt'
  original_article_count = db.run(check_query).data()[0]['cnt']

  # If there are no existing articles, return the original list of PubMed IDs
  if original_article_count == 0:
    return pmids

  # Query to check which PubMed IDs already exist for the specified Disease (GARD) node
  query = f'MATCH (x:GARD)--(y:Article) WHERE x.GardId = \"{gard_id}\" AND y.pubmed_id IN {pmids} RETURN y.pubmed_id'
  response = db.run(query).data()

  # if there are PubMed IDs already in the database, filter them out from the PubMed API query response of PMIDs
  if len(response) > 0:
    for id in response:
      if id['y.pubmed_id'] in pmids:
        pmids.remove(id['y.pubmed_id'])
    # If all PubMed IDs are already in the database, return None
    if len(pmids) == 0:
      pmids = None   
  return pmids

def is_under_char_threshold(syn):
    if len(syn.split()) == 1:
        if len(syn) < 5:
            print('WORD UNDER CHAR LIMIT::', syn)
            return True
        else:
            return False
    else:
        return False

def is_english(syn):
    tokens = syn.lower().split()
    if len(tokens) == 1:
        if tokens[0] in wordset:
            print('ENGLISH WORD FOUND::', syn)
            return True
        else:
            return False
    else:
        return False

def is_acronym(words):
    """
    Checks if a word is an acronym.

    Args:
        word (str): The word to be checked.

    Returns:
        bool: True if the word is an acronym, False otherwise.

    Example:
        result = is_acronym("NASA")
        print(result)  # Output: True
    """
    if len(words.split()) > 1: return False

    for word in words.split():
        # Check if the word follows the pattern of an acronym
        if bool(re.match(r'\w*[A-Z]\w*', word[:len(word)-1])) and (word[len(word)-1].isupper() or word[len(word)-1].isnumeric()): # aGG2
            print('ACRONYM REMOVED::', words)
            return True
    return False

def filter_synonyms(syns):
    """
    Filter out synonyms that contain spaces.

    Parameters:
    - synonyms (list): List of synonyms to be filtered.

    Returns:
    list: List of synonyms that do not contain spaces.

    Example:
    >> synonyms_list = ["apple", "orange", "banana peel", "grape"]
    >> filtered_synonyms = filter_synonyms(synonyms_list)
    >> print(filtered_synonyms)
    ['apple', 'orange', 'grape']
    """
    gardsyns_eng = [syn for syn in syns if is_english(syn)]
    gardsyns_char_threshold = [syn for syn in syns if is_under_char_threshold(syn)]
    filtered_syns = [x for x in syns if not x in gardsyns_eng]
    filtered_syns = [x for x in filtered_syns if not x in gardsyns_char_threshold]

    # Return the filtered list of synonyms
    return filtered_syns




def save_disease_articles(db, mindate, maxdate, today):
      """
      --The main function of the PubMed database pipeline--

      Retrieve and save article data for Genetic and Rare Diseases (GARD) within the specified date range.

      This function interacts with a Neo4j database to store information about articles related to GARD diseases. It performs the following tasks:
      1. Retrieves a list of GARD diseases using the get_gard_list function.
      2. Iterates over each GARD disease to gather information, including synonyms.
      3. Searches for articles related to each disease using the find_articles function.
      4. Checks if the articles already exist in the Neo4j database based on PubMed IDs.
      5. Saves the disease node in the database using the create_disease function if not already present.
      6. Saves new articles related to the disease using the save_articles function.
      7. Fetches abstracts for the articles using the fetch_abstracts function.
      8. Updates the Neo4j database with the retrieved information.

      Parameters:
      :param db: Neo4j database connection object.
      :param mindate: Minimum date (in the format "%Y/%m/%d") for searching articles.
      :param maxdate: Maximum date (in the format "%Y/%m/%d") for searching articles.

      Example:
      save_disease_articles(my_database_connection, "2023/01/01", "2023/11/30")

      Note:
      - Ensure that the necessary helper functions (get_gard_list, filter_synonyms, find_articles, create_disease,
        filter_existing, save_articles, fetch_abstracts) are defined and available in the same code environment.

      - The function prints informative messages to the console during execution and logs any encountered exceptions.

      Returns:
      None
      """

      # Step 1: Retrieve GARD diseases list
      results = get_gard_list()
      search_source = 'Pubmed'
      date_db_now = datetime.strptime(maxdate,"%Y/%m/%d").strftime("%m/%d/%y")

      in_progress = db.getConf('UPDATE_PROGRESS', 'pubmed_in_progress')
      if in_progress == 'True':
        current_step = db.getConf('UPDATE_PROGRESS', 'pubmed_disease_article_progress')
        print(current_step, type(current_step))
        if not current_step == '':
            current_step = int(current_step)
        else:
            current_step = 0
      else:
        current_step = 0
      
      # Iterate over GARD diseases
      for idx, gard_id in enumerate(results):
        print('-------------------------------------------------------\n')
        #if not gard_id == 'GARD:0002491':
        #  continue
        if idx < current_step:
          continue

        # Step 2: Check the count of articles related to the current disease in the database
        check_query = f'MATCH (x:GARD)--(y:Article) WHERE x.GardId = \"{gard_id}\" WITH count(y) AS cnt RETURN cnt'
        db_article_count = int(db.run(check_query).data()[0]['cnt'])
        print('ORIGINAL ARTICLE COUNT IN DB: ', db_article_count, gard_id)

        if gard_id == None:
          continue
        no = 0
        rd = results[gard_id]
        
        # Step 3: Generate search terms and find articles for each term
        searchterms = filter_synonyms(rd['synonyms'])
        searchterms.extend([rd['name']])
       
        for searchterm in searchterms:
          #pubmedIDs = None
          try:
            pubmedIDs = find_articles(searchterm,mindate,maxdate)
            article_count = len(pubmedIDs)
          except Exception as e:
            print(pubmedIDs)
            logging.error(f'Exception when finding articles: {e}')
            continue

          print(idx, gard_id, "Articles:", article_count, rd["name"]+'['+searchterm+']')
          disease_node = create_disease(db, gard_id, rd)

          # Step 5: Filter existing articles in the database
          try:
            pubmed_ids = filter_existing(db, gard_id, pubmedIDs)
            if pubmed_ids:
              print('ARTICLES TO BE ADDED IN UPDATE: ', len(pubmed_ids))
              save_articles(disease_node, pubmed_ids, search_source, db, today)
              
              # Labels the article node with the term (name or synonym) of the selected GARD disease that was used to query the PubMed API for that article
              for article in pubmed_ids:
                db.run(f'MATCH (x:GARD)-[r:MENTIONED_IN]->(y:Article) WHERE y.pubmed_id = \"{article}\" AND x.GardId = \"{gard_id}\" SET r.MatchedTermRDAS = \"{searchterm}\"')

            else:
              print('All articles already in database')
              continue
          except Exception as e:
            logging.error(f'Exception when finding articles for disease {gard_id}, error1: {e}')
            continue

        db.setConf('UPDATE_PROGRESS', 'pubmed_disease_article_progress', str(idx))

        """
          # REPEAT WORK??
          # Step 6: Fetch abstracts for the articles and update the database
          try:
            all_abstracts = fetch_abstracts(pubmed_ids)
            if all_abstracts == None:
              continue
            
            for abstracts in all_abstracts:
              if ('resultList' in abstracts and 'result' in abstracts['resultList'] and len(abstracts['resultList']['result']) > 0):
                for result in abstracts['resultList']['result']:
                  pubmedID = result['id'] if 'id' in result else None
                
                  if pubmedID is None:
                    print('.', end='', flush=True)
                    continue

                  # Step 7: Check if the article already exists in the database
                  res = db.run("match(a:Article{pubmed_id:$pmid}) set a.pubmed_evidence=TRUE return ID(a)", args={"pmid":pubmedID})
                  alist = list(res)
                  matching_articles = len(alist)

                  if (matching_articles > 0):
                    pass
                  else:
                    # Step 8: Save the article details to the database
                    save_all(result, disease_node, pubmedID, search_source, db, maxdate)

          except Exception as e:
            logging.error(f'Exception when finding articles for disease {gard_id}, error2: {e}')
            continue
        """




def gather_pubtator(db, today):
  """
    Gather Pubtator annotations for articles that do not have associated annotations in the Neo4j database.

    This function retrieves articles from the database that lack Pubtator annotations, fetches the annotations using
    the fetch_pubtator_annotations function, and creates corresponding PubtatorAnnotation nodes in the database using
    the create_annotations function.

    Parameters:
    :param db: Neo4j database connection object.
    :param today: Today's date in the format "%Y-%m-%d".

    Example:
    gather_pubtator(my_database_connection, "2023-11-30")

    Note:
    - Ensure that the necessary helper functions (fetch_pubtator_annotations, create_annotations) are defined and
      available in the same code environment.

    - The function logs a warning for any exceptions encountered during the process.

    Returns:
    None
  """
  in_progress = db.getConf('UPDATE_PROGRESS', 'pubmed_in_progress')
  if in_progress == 'True':
    current_step = db.getConf('UPDATE_PROGRESS', 'pubmed_pubtator_article_progress')
    if not current_step == '':
      current_step = int(current_step)
    else:
      current_step = 0
  else:
    current_step = 0

  # Retrieve articles without Pubtator annotations
  #res = db.run('MATCH (x:Article) WHERE NOT (x)--(:PubtatorAnnotation) AND x.pubmed_id IS NOT NULL AND x.hasPubtatorAnnotation IS NULL RETURN x.pubmed_id AS pmid, ID(x) AS id').data()
  #print(len(res))

  res = db.run('MATCH (x:Article) WHERE x.pubmed_id IS NOT NULL AND x.hasPubtatorAnnotation IS NULL RETURN x.pubmed_id AS pmid, ID(x) AS id').data()
  print(len(res))

  # Set OMIM only articles to hasPubtatorAnnotation = False since they dont have pubmed_id's
  db.run('MATCH (x:Article) WHERE x.pubmed_id IS NULL AND x.hasPubtatorAnnotation IS NULL SET x.hasPubtatorAnnotation = FALSE')
  
  # Iterate over the articles and fetch Pubtator annotations
  res = res[current_step:]
  pmids = [r['pmid'] for r in res]
  pmid_to_id = {r['pmid']:r['id'] for r in res}

  try:
    # Fetch Pubtator annotations for the article
    for batch in fetch_pubtator_annotations(pmids):
      if not batch:
        continue
      
      annos = batch['PubTator3']

      for anno in annos:
        cur_pmid = str(anno['pmid'])
        article_id = pmid_to_id[cur_pmid]

        if anno:
          # Create PubtatorAnnotation nodes in the database
          print('ARTICLE_ID::', article_id, 'CURRENT_STEP::', current_step)
          
          create_annotations(db, anno, article_id, today)
          db.run(f'MATCH (a:Article) WHERE ID(a) = {article_id} SET a.hasPubtatorAnnotation = TRUE')
        else:
          db.run(f'MATCH (a:Article) WHERE ID(a) = {article_id} SET a.hasPubtatorAnnotation = FALSE')

        current_step += 1
        db.setConf('UPDATE_PROGRESS', 'pubmed_pubtator_article_progress', str(current_step))
      

  except Exception as e:
    #logging.warning(f'\nException creating annotations for article {pmid}:  {e}')
    print('error in gather_pubtator')




def gather_epi(db, today):
  """
    Gather epidemiology annotations for articles with non-empty titles and abstracts that do not have associated
    EpidemiologyAnnotation nodes in the Neo4j database.

    This function retrieves articles from the database that meet the criteria, creates a dictionary with abstract and title
    information, and creates corresponding EpidemiologyAnnotation nodes in the database using the create_epidemiology function.

    Parameters:
    :param db: Neo4j database connection object.
    :param today: Today's date in the format "%Y-%m-%d".

    Example:
    gather_epi(my_database_connection, "2023-11-30")

    Note:
    - Ensure that the necessary helper function (create_epidemiology) is defined and available in the same code environment.

    Returns:
    None
  """

  # Retrieve articles with non-empty titles and abstracts that havent already been processed by the API (epi_processed = FALSE)
  res = db.run('MATCH (x:Article) WHERE NOT (x.abstractText = \"\" OR x.title = \"\") AND (x.epi_processed = FALSE AND x.isEpi = FALSE) RETURN x.abstractText AS abstract, x.title AS title, ID(x) AS id').data() #TEST DateCreatedRDAS needs to be removed after
  # MATCH (x:Article) WHERE NOT (x.abstractText = \"\" OR x.title = \"\") AND ((x.epi_processed = TRUE AND x.isEpi = TRUE) AND NOT exists((x)--(:EpidemiologyAnnotation))) OR NOT (x.abstractText = \"\" OR x.title = \"\") AND (x.epi_processed = FALSE AND x.isEpi = FALSE) RETURN x.abstractText AS abstract, x.title AS title, ID(x) AS id
  print(len(res))  

  # Iterate over articles and create EpidemiologyAnnotation nodes
  for idx,r in enumerate(res):
    print(idx)

    abstract = r['abstract']
    title = r['title']
    abstractDataRel = {'abstractText':abstract,'title':title}
    ID = r['id']

    # Create EpidemiologyAnnotation nodes in the database
    create_epidemiology(db, abstractDataRel, ID, today)




def download_genereview_articles():
  if not os.path.exists(f'{sysvars.base_path}pubmed/src/genereviews_pmid.txt'):
    command = f'curl -L -X GET https://ftp.ncbi.nih.gov/pub/GeneReviews/GRtitle_shortname_NBKid.txt -o {sysvars.pm_files_path}genereviews_pmid.txt'
    os.system(command)




def generate_missing_genereviews(response, review_list, df):
  not_mapped = [i['pmid'] for i in response]

  missing = list(set(review_list)-set(not_mapped))
  missing = [int(i) for i in missing]
  df = df[df['PMID'].isin(missing)]

  df.to_csv(f'{sysvars.pm_files_path}genereviews_pmid_missing.csv')




def label_genereview(db):
  download_genereview_articles()

  df = pd.read_csv(f'{sysvars.pm_files_path}genereviews_pmid.txt', encoding='ISO-8859-1', sep='\t')
  review_list = df['PMID'].tolist()
  review_list = [str(i) for i in review_list]
  
  # Labels all genereview articles
  query = 'MATCH (x:Article) WHERE x.pubmed_id IN $pmid_list SET x.is_genereview = TRUE RETURN x.pubmed_id as pmid'
  args = {'pmid_list': review_list}

  response = db.run(query, args=args).data()
  generate_missing_genereviews(response, review_list, df)

  print('Genereview Articles Labeled')

  # Labels all other articles as not a gene review article
  query = 'MATCH (x:Article) WHERE NOT x.pubmed_id IN $pmid_list SET x.is_genereview = FALSE'
  args = {'pmid_list': review_list}

  db.run(query, args=args)



def retrieve_articles(db, last_update, updating_to, today):
  """
    Gets articles from multiple different sources (PubMed, NCATS databases, OMIM) within a 50-year rolling window or since
    the last script execution.

    This function serves as an entry point for retrieving articles from various sources and updating the database with
    relevant information. It utilizes several helper functions such as save_disease_articles, gather_epi, gather_pubtator,
    and save_omim_articles.

    Parameters:
    :param db: Neo4j database connection object.
    :param last_update: Date of the last script execution in the format "%Y-%m-%d".
    :param today: Today's date in the format "%Y-%m-%d".

    Example:
    retrieve_articles(my_database_connection, "2023-01-01", "2023-11-30")

    Note:
    - Ensure that the necessary helper functions (save_disease_articles, gather_epi, gather_pubtator,
      save_omim_articles) are defined and available in the same code environment.

    Returns:
    None
  """

  # Gets config update progress values
  current_step = db.getConf('UPDATE_PROGRESS', 'pubmed_current_step')
  in_progress = db.getConf('UPDATE_PROGRESS', 'pubmed_in_progress')

  # Retrieve articles related to GARD diseases from PubMed and update the database
  if current_step == 'save_articles' or in_progress == 'False':
    print('Populating PubMed Articles')

    # Set the update progress to in progress
    db.setConf('UPDATE_PROGRESS', 'pubmed_in_progress', 'True')
    db.setConf('UPDATE_PROGRESS', 'pubmed_current_step', 'save_articles')

    save_disease_articles(db, last_update, updating_to, today) #TEST, remove comment keep code
    db.setConf('UPDATE_PROGRESS', 'pubmed_current_step', 'save_omim')
  else:
    print('Update in progress... bypassing save_articles')

  # Save OMIM articles and update the database
  current_step = db.getConf('UPDATE_PROGRESS', 'pubmed_current_step')
  if current_step == 'save_omim': 
    print('Populating OMIM Articles and Information')
    save_omim_articles(db, today) #TEST, remove comment keep code
    db.setConf('UPDATE_PROGRESS', 'pubmed_current_step', 'save_epi')
  else:
    print('Update in progress... bypassing save_omim')
  
  # Gather epidemiology annotations for articles with non-empty titles and abstracts
  current_step = db.getConf('UPDATE_PROGRESS', 'pubmed_current_step')
  if current_step == 'save_epi':
    print('Populating Epidemiology Information')
    gather_epi(db, today) #TEST, remove comment keep code
    db.setConf('UPDATE_PROGRESS', 'pubmed_current_step', 'save_pubtator')
  else:
    print('Update in progress... bypassing save_epi')

  # Gather Pubtator annotations for articles that do not have associated annotations
  current_step = db.getConf('UPDATE_PROGRESS', 'pubmed_current_step')
  if current_step == 'save_pubtator': 
    print('Populating Pubtator Information')
    gather_pubtator(db, today)
    db.setConf('UPDATE_PROGRESS', 'pubmed_current_step', 'save_gene')
  else:
    print('Update in progress... bypassing save_pubtator')

  # Label genereview articles
  current_step = db.getConf('UPDATE_PROGRESS', 'pubmed_current_step')
  if current_step == 'save_gene':
    print('Labeling GeneReview Articles')
    label_genereview(db)

    # End of the pipeline, resets the config in_progress values
    db.setConf('UPDATE_PROGRESS', 'pubmed_current_step', '')
    db.setConf('UPDATE_PROGRESS', 'pubmed_disease_article_progress', '')
    db.setConf('UPDATE_PROGRESS', 'pubmed_omim_article_progress', '')
    db.setConf('UPDATE_PROGRESS', 'pubmed_pubtator_article_progress', '')
    db.setConf('UPDATE_PROGRESS', 'pubmed_in_progress', 'False')

  else:
    print('Update in progress... bypassing save_gene')

  




def retrieve_specific_article(pmid):
  """
    Retrieves a specific article from PubMed using the provided PubMed ID (PMID).

    This function sends a POST request to the PubMed E-Utilities API to fetch information about a specific article based
    on its PubMed ID (PMID).

    Parameters:
    :param pmid: PubMed ID (PMID) of the article to be retrieved.

    Example:
    retrieve_specific_article("12345678")

    Note:
    - The function requires an API key from NCBI, which should be set in the 'NCBI_KEY' environment variable.

    Returns:
    :return: JSON response containing information about the specified article from PubMed.
  """

  # Get NCBI API key from environment variables
  api_key = os.environ['NCBI_KEY']

  # Construct the PubMed E-Utilities API URL for retrieving the specific article
  url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/epost.fcgi?db=pubmed&id={pmid}&retmode=json&retmax=10000&api_key={api_key}"

  # Send a POST request to the API and return the response
  response = requests.post(url).json()
  return response 




def update_missing_abstracts(db, today):
  """
    Searches for missing abstracts of articles with missing abstract text in the Neo4j database.
    If a missing abstract is found, it fetches the abstract from PubMed and updates the database.

    Parameters:
    :param db: Neo4j database connection object.
    :param today: Today's date in the format "%Y/%m/%d".

    Example:
    update_missing_abstracts(my_database_connection, "2023/11/30")

    Returns:
    None
  """

  print('Searching for missing abstracts of articles with missing abstract')

  # Set the maximum search date as one year before the current date
  maxsearchdate = datetime.strptime(today,"%Y/%m/%d") - relativedelta(years=1)
  today = datetime.strptime(today,"%Y/%m/%d")

  # Query the Neo4j database to retrieve articles with missing abstracts
  query = 'MATCH (x:Article) WHERE x.abstractText = \"\" RETURN x.pubmed_id, x.firstPublicationDate, x.title, ID(x)'
  response = db.run(query).data()

  length = len(response)
  print(length)

  # Iterate over articles with missing abstracts
  for idx,res in enumerate(response):
    pmid = res['x.pubmed_id']
    title = res['x.title']
    pubdate = datetime.strptime(res['x.firstPublicationDate'], "%Y-%m-%d")
    article_node = res['ID(x)']

    # Check if the articles publication date is within the last year
    if pubdate < maxsearchdate:
      continue

    # Fetch abstract from PubMed
    article = fetch_abstracts([pmid])
    time.sleep(0.34)

    try:
      article = article[0]['resultList']['result'][0]
      if 'abstractText' in article:
        print(f'New Abstract Found for Article: {pmid}')

        # Update Neo4j database with the new abstract
        new_abstract = article['abstractText'].replace("\"","")

        today_str = today.strftime("%m/%d/%y")
        query = f"MATCH (x:Article) WHERE ID(x) = {article_node} SET x.abstractText = \"{new_abstract}\" SET x.LastUpdatedRDAS = \"{today_str}\" RETURN true"
        db.run(query)

        # Create epidemiology annotation for the new abstract
        abstractDataRel = {'abstractText': new_abstract,'title': title}
        create_epidemiology(db, abstractDataRel, article_node, today)

    except Exception as e:
      continue

    print(str(idx) + '/' + str(length))

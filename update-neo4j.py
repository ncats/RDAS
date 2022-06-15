#Richard: change how to use MERGE - only include key in MERGE and left other properties to ON CREATE SET

import json
from neo4j import GraphDatabase, basic_auth
import configparser
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import time
import logging
import itertools
import requests
import jmespath
import re

# Loads in variables from config.ini file
config = configparser.ConfigParser()
config.read("config.ini")

user = config.get("credentials","neo4j_username")
password = config.get("credentials","neo4j_password")
neo4j_uri = config.get("credentials","neo4j_uri")
disease_uri = config.get("credentials","disease_uri")
omim_api_key = config.get("credentials","omim_api_key")
database_last_run = config.get("database","database_last_run")

try:
  from_disease = int(config.get("script","from_disease"))
except ValueError as e:
  from_disease = 0
try:
  to_disease = int(config.get("script","to_disease"))
except ValueError as e:
  to_disease = None

def get_gard_list():
  #Returns list of the GARD Diseases
  with GraphDatabase.driver(disease_uri) as driver:
    with driver.session() as session:
      #cypher_query = '''
      #match p = (d:DATA)-[]-(m:S_GARD) where d.gard_id in ['GARD:0001358', 'GARD:0001360', 'GARD:0001362'] return  d, m
      #'''
      cypher_query = '''
      match p = (d:DATA)-[]-(m:S_GARD) return  d, m
      '''
      nodes = session.run(cypher_query, parameters={})
      results = nodes.data()
      #results = [record for record in nodes.data()]
      # Access the Node using the key 'd' and 'm' (defined in the return statement):
      myData = {}
      for res in results:
        gard_id = res['d'].get("gard_id")
        disease = {}
        disease['gard_id'] = gard_id
        disease['name'] = res['d'].get("name")
        disease['is_rare'] = True if res['d'].get("is_rare") is not None else False
        disease['categories'] = res['d'].get("categories") if res['d'].get("categories") is not None else ''
        disease['all_names'] = res['m'].get("N_Name") if res['m'].get("N_Name") is not None else ''
        disease['synonyms'] = res['d'].get("synonyms") if res['d'].get("synonyms") is not None else ''
        disease['all_ids'] = res['m'].get("I_CODE") if res['m'].get("I_CODE") is not None else ''
        myData[gard_id] = disease
  return myData

#Richard: return 4720 records
def get_gard_omim_mapping():
  with GraphDatabase.driver(disease_uri) as driver:
    with driver.session() as session:
      cypher_query = '''
      MATCH (o:S_ORDO_ORPHANET)-[:R_exactMatch|:R_equivalentClass]-(m:S_MONDO)-[:R_exactMatch|:R_equivalentClass]-(n:S_GARD)<-[:PAYLOAD]-(d:DATA)
      WHERE d.is_rare=true and d.gard_id in ['GARD:0001358', 'GARD:0001360', 'GARD:0001362']
      WITH o,n,m,d
      MATCH (o)-[e:R_exactMatch|R_closeMatch]-(k:S_OMIM)<-[:PAYLOAD]-(h)
      RETURN DISTINCT d.gard_id as gard_id,d.name as name, e.name as match_type,h.notation as omim_id,h.label as omim_name
      ORDER BY gard_id
      '''
      nodes = session.run(cypher_query, parameters={})
      return [record for record in nodes.data()]

#get OMIM json by OMIM number
def find_OMIM_articles(OMIMNumber):
  params = {'mimNumber': OMIMNumber, 'include':"all", 'format': 'json', 'apiKey': omim_api_key}
  return requests.post("https://api.omim.org/api/entry?%s", data=params).json()

#Parse OMIM json and return a map: pubmed_id -> OMIM section   
def get_article_in_section(omim_reference):
  #print(omim_reference)
  textSections = jmespath.search("omim.entryList[0].entry.textSectionList[*].textSection",omim_reference)
  references = {}
  for t in textSections:
    #refs = re.findall("({.*?}|{.*?:.*?})",t['textSectionContent'])
    refs = re.findall("({[0-9]*?:.*?})",t['textSectionContent'])
    if refs:
      #print(refs)
      sectionReferenced = set()
      for ref in refs:
        splitRef= ref[1:].split(":")
        sectionReferenced.add(splitRef[0])
      references[t['textSectionName']] = sectionReferenced
  #print(references)
  refNumbers = jmespath.search("omim.entryList[0].entry.referenceList[*].reference.[referenceNumber,pubmedID]",omim_reference)
  #print(refNumbers)
  articleString = {}
  #Richard: there are case like this: MOVED TO 144750, so the returned refNumbers is None
  if refNumbers is None:
    #print(f'No reference for this omim:\n{omim_reference}')
    return articleString
  for refNumber,pmid in refNumbers:
    if not pmid: continue #Richard: some reference has not pubmed id
    tsections = []
    #tsections.add('OMIM:reference')
    for idx, sectionName in enumerate(references):
      #Richard: if the two sets have common elements
      if references[sectionName].intersection(set([str(refNumber)])):
        tsections.append(sectionName)
    if tsections:
      articleString[str(pmid)] = tsections  #Richard: pmid is int, covert to string for consistance
    else:
      articleString[str(pmid)] = ['See Also']
  return articleString

def get_article_id(pubmed_id, driver):
  article_id = None
  with driver.session() as session:
    #pubmed_id = '11977179'
    result = session.run("MATCH(a:Article {pubmed_id:$pmid}) return id(a) as id", pmid = pubmed_id)
    #logging.info(f'result: {result}')
    record = result.single()
    #logging.info(f'record: {record}')
    if record:
      article_id = record["id"]
  return article_id

def get_disease_id(gard_id, driver):
  id = None
  with driver.session() as session:
    result = session.run("MATCH(a:Disease {gard_id:$gard_id}) return id(a) as id", gard_id = gard_id)
    record = result.single()
    #logging.info(f'record: {record}')
    if record:
      id = record["id"]
  return id
    
def save_omim_articles(mindate, maxdate):
  if len(omim_api_key) == 0:
    return

  results = get_gard_omim_mapping()
  results = itertools.islice(results, from_disease, to_disease)
   #results = dict(itertools.islice(results, 0, None))
  search_source = 'omim_evidence'
  with GraphDatabase.driver(neo4j_uri, auth=(user,password)) as driver:
    #with driver.session() as session:
    for no, rd in enumerate(results):  
      gard_id = rd["gard_id"]
      omim_id = rd["omim_id"]
      #logging.info(omim_id)
      omim_json = None
      try:
        omim_json = find_OMIM_articles(omim_id.split(":")[1])
      except Exception as e:
        logging.error(f' Exception when search omim_id {omim_id}: error {e}')
        continue        
      sections = get_article_in_section(omim_json)
      logging.info(f'sections: {sections}')
      #For each pubmed_id, we need to first find (pubmed_id) exist:
      #if exist, add property key "refInOMIM" (need add value true?) for the article, 
      #    then, create OMIM node with property: omimId, omimName, omimSections,
      #    then, create relationship (Article) - [:HAS_OMIM_REF] -> (OMIM)
      #if not exist, need to save this article by call:
      #    save_all(result, disease_node, pubmedID, search_source, session)
      new_sections = dict(sections)
      for pubmed_id in sections:
        #print(f'{no}\t{gard_id}\t{omim_id}\t{pubmed_id}\t{sections[pubmed_id]}\t{rd["name"]}\t{rd["omim_name"]}')
        article_id = get_article_id(pubmed_id, driver)
        #logging.info(f'article_id: {article_id}')
        if (article_id):
          logging.info(f'PubMed evidence article already exists: {pubmed_id}, {article_id}')
          #need add this function
          save_omim_article_relation(article_id, omim_id, rd["omim_name"], sections[pubmed_id], driver)
          new_sections.pop(pubmed_id)
        else:
          logging.info(f'PubMed evidence article NOT exists: {pubmed_id}, {article_id}')    
      logging.info(f'sections after loop: {new_sections}' )   
      #now add all pubmed articles in new_sections
      save_omim_remaining_articles(gard_id, rd, new_sections, search_source, driver)      
          
def save_omim_article_relation(article_id, omim_id, omim_name, sections, driver):
  query = '''
  MATCH (a:Article) WHERE id(a) = $article_id
  SET a.refInOMIM = TRUE
  MERGE (p:OMIMRef {omimId:$omim_id, omimName:$omim_name, omimSections:$sections})
  MERGE (a) - [r:HAS_OMIM_REF] -> (p)
  '''
  with driver.session() as session:
    session.run(query, parameters={
      "article_id":article_id,
      "omim_id":omim_id,
      "omim_name":omim_name,
      "sections":sections
    })          
          
def save_omim_remaining_articles(gard_id, rd, sections, search_source, driver):
  pubmed_ids = list(sections.keys())
  disease_id = get_disease_id(gard_id, driver)
  logging.info(f'pubmed_ids: {pubmed_ids}')
  #Save article and related information first
  save_articles(disease_id, pubmed_ids, search_source, driver)
  #Add (OMIM) node and relationship for these articles
  for pubmed_id in sections:
    #print(f'{no}\t{gard_id}\t{omim_id}\t{pubmed_id}\t{sections[pubmed_id]}\t{rd["name"]}\t{rd["omim_name"]}')
    article_id = get_article_id(pubmed_id, driver)
    #logging.info(f'article_id: {article_id}')
    if (article_id):
      save_omim_article_relation(article_id, rd["omim_id"], rd["omim_name"], sections[pubmed_id], driver)
    else:
      logging.error(f'Something wrong with adding omim article relation: {pubmed_id}, {article_id}')            
          

        
def create_indexes():
  with GraphDatabase.driver(neo4j_uri, auth=(user,password)) as driver:
    with driver.session() as session:
      cypher_query = [
      "CREATE INDEX IF NOT EXISTS FOR (n:Disease) ON (n.gard_id)",
      "CREATE INDEX IF NOT EXISTS FOR (n:Article) ON (n.pubmed_id)",
      "CREATE INDEX IF NOT EXISTS FOR (n:MeshTerm) ON (n.isMajorTopic, n.descriptorName)",
      "CREATE INDEX IF NOT EXISTS FOR (n:MeshQualifier) ON (n.abbreviation, n.qualifierName, n.isMajorTopic)",
      "CREATE INDEX IF NOT EXISTS FOR (n:Journal) ON (n.title, n.medlineAbbreviation, n.essn, n.issn, n.nlmid)",
      "CREATE INDEX IF NOT EXISTS FOR (n:JournalVolume) ON (n.issue, n.volume, n.journalIssueId, n.dateOfPublication, n.monthOfPublication, n.yearOfPublication,n.printPublicationDate)",
      "CREATE INDEX IF NOT EXISTS FOR (n:Keyword) ON (n.keyword)",
      "CREATE INDEX IF NOT EXISTS FOR (n:FullTextUrl) ON (n.availability, n.availabilityCode, n.documentStyle, n.site, n.url)",
      "CREATE INDEX IF NOT EXISTS FOR (n:Author) ON (n.fullName, n.firstName, n.lastName)",
      "CREATE INDEX IF NOT EXISTS FOR (n:PubtatorAnnotation) ON (n.type, n.infons_identifier, n.infons_type, n.text)",
      "CREATE INDEX IF NOT EXISTS FOR (n:OMIMRef) ON (n.omimId, n.omimName, n.omimSections)"
      ]
      for c in cypher_query:
        session.run(c, parameters={})

def find_articles(keyword, mindate, maxdate):
  #fetch articles and return a map
  params = {'db': 'pubmed', 'term': keyword, 'mindate':mindate, 'maxdate':maxdate, 'retmode': 'json', 'retmax':"1000"}
  return requests.post("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?%s", data=params).json()

def fetch_abstracts(pubmedIDs): 
  #fetch abstract for an article  
  url = "https://www.ebi.ac.uk/europepmc/webservices/rest/searchPOST"
  params = {'query': pubmedIDs, 'pageSize': '1000', 'resultType': 'core', 'format': 'json'}
  head = {'Content-Type': 'application/x-www-form-urlencoded'}
  response = requests.post(url, data=params, headers = head).json()
  return response
  
def fetch_pubtator_annotations(pubmedId):
  #fetch annotations from pubtator
    pubtatorUrl = "https://www.ncbi.nlm.nih.gov/research/pubtator-api/publications/export/biocjson?pmids=" + pubmedId
    r = requests.get(pubtatorUrl)
    if (not r or r is None or r ==''):
      logging.error(f'Can not find PubTator for: {pubmedId}')
      return None
    else:
      return r.json()

def fetch_pmc_fulltext_xml(pmcId):
  #fetch full text xml from pmc
  pmcUrl = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id=" + pmcId
  return requests.get(pmcUrl)

def fetch_pmc_fulltext_json(pubmedId):
  #fetch full text json from pmc
  #pmcUrl = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id=" + pmcId
  pmcUrl = "https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi/BioC_json/" + pubmedId + "/unicode" 
  #This may be a urllib command, not sure
  return requests.urlopen(pmcUrl).json()

def create_disease(session, gard_id, rd):
  query = '''
  MERGE (d:Disease {gard_id:$gard_id}) 
  ON CREATE SET
    d.gard_id = $gard_id,
    d.name = $name, 
    d.is_rare = $is_rare, 
    d.categories = $categories, 
    d.all_names = $all_names, 
    d.synonyms = $synonyms, 
    d.all_ids = $all_ids
  RETURN id(d)
  '''
  params = {
    "gard_id":gard_id,
    #"gard_id":rd['gard_id'],
    "name":rd['name'], 
    "is_rare":rd['is_rare'], 
    "categories":rd['categories'], 
    "all_names":rd['all_names'], 
    "synonyms":rd['synonyms'], 
    "all_ids":rd['all_ids']
  }

  return session.run(query, **params).single().value()

def create_article(tx, abstractDataRel, disease_node, search_source):
  #logging.info(f'{abstractDataRel}')
  #logging.info(f'{disease_node}')
  #logging.info(f'{search_source}')
  #Richard: remove: authorString:$authorString,
  #Richard: move source as an article property, instead of a relation and another node
  create_article_query = '''
  MATCH (d:Disease) WHERE id(d)=$id
  MERGE (n:Article {pubmed_id:$pubmed_id})
  ON CREATE SET
    n.pubmed_id = $pubmed_id,
    n.doi = $doi, 
    n.title = $title, 
    n.abstractText = $abstractText, 
    n.affiliation = $affiliation, 
    n.firstPublicationDate = $firstPublicationDate, 
    n.citedByCount = $citedByCount,
    n.isOpenAccess = $isOpenAccess, 
    n.inEPMC = $inEPMC, 
    n.inPMC = $inPMC, 
    n.hasPDF = $hasPDF, 
    n.source = $source, 

    n.''' + search_source + ''' = true
  MERGE (d)-[r:MENTIONED_IN]->(n)
  RETURN id(n)
  '''
  #these variables are used in get_isEpi() and as a Article node property
  #title = abstractDataRel['title'] if 'title' in abstractDataRel else ''
  #abstract = abstractDataRel['abstractText'] if 'abstractText' in abstractDataRel else ''

  params={
    "id":disease_node,
    "pubmed_id":abstractDataRel['pmid'] if 'pmid' in abstractDataRel else '',
    "source":abstractDataRel['source'] if 'source' in abstractDataRel else '',
    "doi":abstractDataRel['doi'] if 'doi' in abstractDataRel else '',
    "title":abstractDataRel['title'] if 'title' in abstractDataRel else '',
    "abstractText":abstractDataRel['abstractText'] if 'abstractText' in abstractDataRel else '',
    #"authorString":abstractDataRel['authorString'] if 'authorString' in abstractDataRel else '',
    "affiliation":abstractDataRel['affiliation'] if 'affiliation' in abstractDataRel else '',
    "firstPublicationDate":abstractDataRel['firstPublicationDate'] if 'firstPublicationDate' in abstractDataRel else '',
    "isOpenAccess": True if 'isOpenAccess' in abstractDataRel else False,
    "inEPMC": True if 'inEPMC' in abstractDataRel else False,
    "inPMC":True if 'inPMC' in abstractDataRel else False,
    "hasPDF":True if 'hasPDF' in abstractDataRel else False,
    #"isEpi": get_isEpi(title, abstract),
    "citedByCount":int(abstractDataRel['citedByCount']) if 'citedByCount' in abstractDataRel else 0,
    }
  return tx.run(create_article_query, **params).single().value()

#Richard: change node from "Person" to "Author"
#Richard: remove affiliation because most items don't have it
#Richard: why include disease_node here?  Do we really need [:STUDIES] relationship? We should only care Article, so remove Disease
def create_authors(tx, abstractDataRel, article_node):
  create_author_query = '''
  MATCH (a:Article) WHERE id(a) = $article_id
  MERGE (p:Author {fullName:$fullName, firstName:$firstName, lastName:$lastName})
  MERGE (p) - [r:WROTE] -> (a)
  '''
  for author in abstractDataRel['authorList']['author']:
    tx.run(create_author_query, parameters={
      "article_id":article_node,
      "fullName": author['fullName'] if 'fullName' in author else '',
      "firstName": author['firstName'] if 'firstName' in author else '',
      "lastName": author['lastName'] if 'lastName' in author else ''
    })

#Richard: change JournalInfo to Volume
def create_journal(tx, abstractDataRel, article_node):
  create_journal_query = '''
  MATCH (a:Article) WHERE id(a) = $article_id
  MERGE (j:Journal{title:$title,medlineAbbreviation:$medlineAbbreviation,essn:$essn,issn:$issn,nlmid:$nlmid})
  MERGE (ji:JournalVolume{issue:$issue, volume:$volume, journalIssueId:$journalIssueId,
    dateOfPublication:$dateOfPublication, monthOfPublication:$monthOfPublication,yearOfPublication:$yearOfPublication,
    printPublicationDate:$printPublicationDate})
  MERGE (a)-[:APPEARS_IN]->(ji)
  MERGE (ji)-[:CONTENT_OF]->(j)
  '''
  tx.run(create_journal_query, parameters={
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

#Richard: remove (d:Disease) from MATCH
def create_keywords(tx, abstractDataRel, article_node):
  create_keyword_query = '''
  MATCH (a:Article) WHERE id(a) = $article_id
  MERGE (k:Keyword {keyword:$keyword}) 
  MERGE (k)- [r:KEYWORD_FOR] -> (a)
  '''
  
  for keyword in abstractDataRel:
    if keyword:
      tx.run(create_keyword_query, parameters={
        "article_id":article_node,      
        "keyword": keyword
      })

#This classifies if an article is epidemiological or not.
def get_isEpi(text, url="https://rdip2.ncats.io:8000/postEpiClassifyText/"):
  #check if the article is an Epi article using API
  try:
    response = requests.post(url, json={'text': text}).json()
    return response['IsEpi']
  except Exception as e:
    logging.error(f'Exception during get_isEpi. text: {text}, error: {e}')
    raise e
  

def get_epiExtract(text, url="https://rdip2.ncats.io:8000/postEpiExtractText"):
  #Returns a dictionary of the form 
  '''
  {DATE:['1989'],
  LOC:['Uruguay', 'Brazil'],
  STAT:['1 in 10000',1/83423]
  ...}
  '''
  try:
    epi_info = dict(requests.post("https://rdip2.ncats.io:8000/postEpiExtractText", json={'text': text,'extract_diseases':False}).json())
    return epi_info
  except Exception as e:
    logging.error(f'Exception during get_isEpi. text: {text}, error: {e}')
    raise e

# This function adds isEpi to the article node. If it is an Epi article, it adds a new EpidemiologyAnnotation node
# isEpi is null when there is no abstract to review.
def create_epidemiology(tx, abstractDataRel, article_node):
  text = abstractDataRel['title'] + ' ' + abstractDataRel['abstractText']
  isEpi = get_isEpi(text)
  
  if isEpi:
    epi_info = get_epiExtract(text)
    #This checks if each of the values in the epi_info dictionary is empty. If they all are empty, then the node is not added.
    if sum([1 for x in epi_info.values() if x]) > 0:
      try:
        create_epidemiology_query = '''
          MATCH (a:Article) WHERE id(a) = $article_id
          SET a.isEpi = $isEpi
          MERGE (n:EpidemiologyAnnotation {isEpi:$isEpi, epidemiology_type:$epidemiology_type, epidemiology_rate:$epidemiology_rate, date:$date, location:$location, sex:$sex, ethnicity:$ethnicity}) 
          MERGE (n) -[r:EPIDEMIOLOGY_ANNOTATION_FOR]-> (a)
          '''
        tx.run(create_epidemiology_query, parameters={
          "article_id":article_node,
          "isEpi": isEpi,
          "epidemiology_type":epi_info['EPI'] if epi_info['EPI'] else [], 
          "epidemiology_rate":epi_info['STAT'] if epi_info['STAT'] else [], 
          "date":epi_info['DATE'] if epi_info['DATE'] else [], 
          "location":epi_info['LOC'] if epi_info['LOC'] else [], 
          "sex":epi_info['SEX'] if epi_info['SEX'] else [], 
          "ethnicity":epi_info['ETHN'] if epi_info['ETHN'] else [],
        })
      except Exception as e:
        logging.error(f'Exception during tx.run(create_epidemiology_query) where isEpi is True.')
        raise e
  else:
    create_epidemiology_query = '''
        MATCH (a:Article) WHERE id(a) = $article_id
        SET a.isEpi = $isEpi'''
    try:
      tx.run(create_epidemiology_query, parameters={"article_id":article_node, 'isEpi': isEpi,})
    except Exception as e:
      logging.error(f'Exception during tx.run(create_epidemiology_query) where isEpi is False.')
      raise e

def create_fullTextUrls(tx, abstractDataRel, article_node):
  create_fullTextUrls_query = '''
  MATCH (a:Article) WHERE id(a) = $article_id
  MERGE (u:FullTextUrl {availability:$availability, availabilityCode:$availabilityCode, documentStyle:$documentStyle,site:$site,url:$url})
  MERGE (u) - [r:CONTENT_FOR] -> (a)
  '''
  for fullTextUrl in abstractDataRel:
    tx.run(create_fullTextUrls_query, parameters={
      "article_id":article_node,
      "availability": fullTextUrl['availability'] if 'availability' in fullTextUrl else '',
      "availabilityCode": fullTextUrl['availabilityCode'] if 'availabilityCode' in fullTextUrl else '',
      "documentStyle": fullTextUrl['documentStyle'] if 'documentStyle' in fullTextUrl else '',
      "site": fullTextUrl['site'] if 'site' in fullTextUrl else '',
      "url": fullTextUrl['url'] if 'url' in fullTextUrl else ''
    })

def create_meshHeadings(tx, abstractDataRel, article_node):
  create_meshHeadings_query = '''
  MATCH (a:Article) WHERE id(a) = $article_id
  MERGE (m:MeshTerm {isMajorTopic:$isMajorTopic, descriptorName:$descriptorName}) 
  MERGE (m) - [r:MESH_TERM_FOR] -> (a)
  RETURN id(m)
  '''
  create_meshQualifiers_query = '''
  MATCH (m:MeshTerm) WHERE id(m) = $meshHeading_id
  MERGE (mq:MeshQualifier {abbreviation:$abbreviation, qualifierName:$qualifierName, isMajorTopic:$isMajorTopic}) 
  MERGE (mq) - [r:MESH_QUALIFIER_FOR] -> (m)
  '''
  for meshHeading in abstractDataRel:
    if 'majorTopic_YN' in meshHeading:
      #This converts the MeSH majorTopic_YN property to a boolean isMajorTopic
      isMajorTopic = True if meshHeading['majorTopic_YN'] == 'Y' else False # The other option is 'N
    parameters={
      "article_id":article_node,
      "isMajorTopic": isMajorTopic,
      "descriptorName": meshHeading['descriptorName'] if 'descriptorName' in meshHeading else ''
    }
    txout = tx.run(create_meshHeadings_query,**parameters).single()
    if (txout):
      meshHeadingId = txout.value()
      if ('meshQualifierList' in meshHeading and 'meshQualifier' in meshHeading['meshQualifierList']):
        for meshQualifier in meshHeading['meshQualifierList']['meshQualifier']:
          #This converts the MeSH majorTopic_YN property to a boolean isMajorTopic
          if 'majorTopic_YN' in meshQualifier:
            isMajorTopic = True if meshQualifier['majorTopic_YN'] == 'Y' else False # The other option is 'N
          tx.run(create_meshQualifiers_query, parameters={
          "meshHeading_id":meshHeadingId,
          "abbreviation": meshQualifier['abbreviation'] if 'abbreviation' in meshQualifier else '',
          "qualifierName": meshQualifier['qualifierName'] if 'qualifierName' in meshQualifier else '',
          "isMajorTopic": isMajorTopic,
          })

#Richard: change node name from Chemical to Substance
def create_chemicals(tx, abstractDataRel, article_node):
  create_chemicals_query = '''
  MATCH (a:Article) WHERE id(a) = $article_id
  MERGE (u:Substance {name:$name, registryNumber:$registryNumber}) - [r:SUBSTANCE_ANNOTATED_BY_PUBMED] -> (a)
  '''
  for chemical in abstractDataRel:
    tx.run(create_chemicals_query, parameters={
      "article_id":article_node,
      "name": chemical['name'] if 'name' in chemical else '',
      "registryNumber": chemical['registryNumber'] if 'registryNumber' in chemical else '',
    })

#Richard: title and abstract already include in Article, so no need to be in PubtatorPassage again, do remove "text"
#Richard: combine (PubtatorPassage) to (PubtatorAnnotations) because it has only one property: type
#Richard: remove locations_offset, locations_length
#Richard: change PubtatorAnnotations to PubtatorAnnotation
def create_annotations(tx, pubtatorData, article_node):
  create_annotations_query = '''
  MATCH(a:Article) WHERE id(a) = $article_id
  MERGE (pa:PubtatorAnnotation {
    type:$type,
    infons_identifier:$infons_identifier, 
    infons_type:$infons_type, 
    text:$text
  })
  MERGE (pa)- [r:ANNOTATION_FOR] -> (a)
  '''

  for passage in pubtatorData['passages']:
    type = passage['infons']['type'] if 'type' in passage['infons'] else ''
    for annotation in passage['annotations']:
      parameters={
        "article_id":article_node,
        "type":type,
        "infons_identifier": annotation['infons']['identifier'] if ('identifier' in annotation['infons'] and annotation['infons']['identifier'])  else '',
        "infons_type": annotation['infons']['type'] if ('type' in annotation['infons'] and annotation['infons']['type']) else '',
        "text": annotation['text'] if 'text' in annotation else '',
      }
      txout = tx.run(create_annotations_query, **parameters)

def create_disease_article_relation(tx, disease_node, article_node):
  query = '''
  MATCH (a: Article) WHERE id(a) = $article_id
  MATCH (d: Disease) WHERE id(d) = $disease_id
  MERGE (d)-[:MENTIONED_IN]->(a)
  '''
  tx.run(query, parameters={
    "article_id":article_node,
    "disease_id":disease_node,
  })
  
def save_disease_article_relation(disease_node, article_node, session):
  tx = session.begin_transaction()
  logging.info(f'Create Disease - Article relation')
  create_disease_article_relation(tx, disease_node, article_node)
  tx.commit()
      
def save_all(abstract, disease_node, pubmedID, search_source, session):
    tx = session.begin_transaction()
    #print(f'abstract: {abstract}')

    logging.info(f'Invoking create_article')
    article_node = create_article(tx, abstract, disease_node, search_source)
    if ('meshHeadingList' in abstract and 
    'meshHeading' in abstract['meshHeadingList']):
      logging.info(f'Invoking create_meshHeading')
      create_meshHeadings(tx, abstract['meshHeadingList']['meshHeading'], article_node)

    if ('authorList' in abstract and 
    'author' in abstract['authorList']):
      logging.info(f'Invoking create_authors')
      create_authors(tx, abstract, article_node)
    
    if ('journalInfo' in abstract):
      logging.info(f'Invoking create_journal')
      create_journal(tx, abstract['journalInfo'], article_node)
    
    if ('keywordList' in abstract and 
    'keyword' in abstract['keywordList']):
      logging.info(f'Invoking create_keywords')
      create_keywords(tx, abstract['keywordList']['keyword'], article_node)
    
    # This function adds isEpi (bool) to the article node and if True, adds EpidemiologyAnnotation node with relation Epidemiology_Annotation_For to Article node.
    # isEpi is null when there is no abstract to review.
    logging.info(f'Invoking create_epidemiology')
    if ('abstractText' in abstract and 'title' in abstract):
      create_epidemiology(tx, abstract, article_node)
    
    if ('fullTextUrlList' in abstract and 
    'fullTextUrl' in abstract['fullTextUrlList']):
      logging.info(f'Invoking create_fullTextUrls')
      create_fullTextUrls(tx, abstract['fullTextUrlList']['fullTextUrl'], article_node)
    
    if ('chemicalList' in abstract and 
    'chemical' in abstract['chemicalList']):
      logging.info(f'Invoking create_chemical')
      create_chemicals(tx, abstract['chemicalList']['chemical'], article_node)

    tx.commit()
    
    #Richard: add keywork, new way
    #Richard: this method has no effect to performance - it's still slow!
    '''
    if ('keywordList' in abstract and 'keyword' in abstract['keywordList']):
      logging.info(f'Invoking create_keywords')
      #create_keywords(tx, abstract['keywordList']['keyword'], article_node)
      session.write_transaction(create_keywords, abstract['keywordList']['keyword'], article_node) 
    '''
    
    #begin another transaction save article annotations
    tx = session.begin_transaction()
    logging.info(f'Invoking create annotations')
    annos = ''
    try:
      annos = fetch_pubtator_annotations(pubmedID)
      create_annotations(tx, annos, article_node)
    except Exception as e:
      logging.warning(f'\nException creating annotations for article {pubmedID}:  {e}')
    finally:
      tx.commit()

def save_articles(disease_node, pubmed_ids, search_source, driver):
  ids = ' OR ext_id:'.join(pubmed_ids)
  ids = 'ext_id:' +ids
  #logging.info(ids)

  #logging.warning(f'Fetch pubmed abstracts for ({ids})')
  abstracts = fetch_abstracts(ids)
  #logging.info(abstracts)
  if ('resultList' in abstracts and 'result' in abstracts['resultList'] and len(abstracts['resultList']['result']) > 0):
    try: 
      for result in abstracts['resultList']['result']:
        pubmedID = result['id'] if 'id' in result else None
        logging.info(f'pubmedID: {pubmedID}')
        if pubmedID is None:
          print('N', end='', flush=True)
          print(f'\n{result}\n')
          continue
        else:
          print('.', end='', flush=True)
        with driver.session() as session:
          res = session.run("match(a:Article{pubmed_id:$pmid}) return id(a) as id", pmid = pubmedID)
          record = res.single()
          if (record):
            article_node = record["id"]
            logging.info(f'PubMed evidence article already exists: {pubmedID}, {article_node}')
            with driver.session() as session:
              try:
                save_disease_article_relation(disease_node, article_node, session)
              except Exception as e:
                logging.error(f" Exception when calling save_disease_article_relation, error: {e}")
          else:
            with driver.session() as session:
              try:
                save_all(result, disease_node, pubmedID, search_source, session)
              except Exception as e:
                logging.error(f" Exception when calling save_all, error: {e}")
    except Exception as e:
      #print('disease_node',disease_node, 'pubmed_ids',pubmed_ids, 'search_source',search_source,sep='\n')
      logging.error(f" Exception when iterating abstracts['resultList']['result'], result: {result}, error: {e}")  

def save_disease_articles(mindate, maxdate):
  #Find and save article data for GARD diseases between from mindate to maxdate
  with GraphDatabase.driver(neo4j_uri, auth=(user,password)) as driver:
      #with driver.session() as session:
      #Richard: move the session to each query because a single session like this will slow the program - why?
        results = get_gard_list()
        results = dict(itertools.islice(results.items(), from_disease, to_disease))
        search_source = 'pubmed_evidence'
        for idx, gard_id in enumerate(results):
          if gard_id == None:
            continue  
          #time.sleep(0.02)
          no = 0
          rd = results[gard_id]
          #logging.info(rd["name"])
          logging.warning(f'{idx}: Invoking find_articles({rd["name"]},{mindate},{maxdate})')
          try:
            pubmedIDs = find_articles(rd["name"],mindate,maxdate)
            no = pubmedIDs['esearchresult']['count']
          except Exception as e:
            logging.error(f'Exception when finding articles: {e}')
            continue
            
          print(idx, gard_id, "Articles:", no, rd["name"])
          logging.debug('Invoking create_gard_disease')
          with driver.session() as session:
            disease_node = create_disease(session, gard_id, rd)
          if not 'esearchresult' in pubmedIDs:
            logging.error(f'\nError find article for this disease: {gard_id}, {rd["name"]}')
            continue
          # Richard: combine all the IDs to one query string

          #logging.info(pubmedIDs['esearchresult']['idlist'])
          pubmed_ids = pubmedIDs['esearchresult']['idlist']
          try:
            save_articles(disease_node, pubmed_ids, search_source, driver)
          except Exception as e:
            #print('disease_node',disease_node, 'pubmed_ids',pubmed_ids, 'search_source',search_source,sep='\n')
            logging.error(f'Exception when finding articles for disease {gard_id}, error: {e}')
            continue
          
          ids = ' OR ext_id:'.join(pubmedIDs['esearchresult']['idlist'])
          #logging.info(ids)
          ids = 'ext_id:' +ids
          #logging.info(ids)
          logging.info(f"Total articles find: {pubmedIDs['esearchresult']['count']}")

          try:
            #logging.warning(f'Fetch pubmed abstracts for ({ids})')
            #time.sleep(0.5)
            abstracts = fetch_abstracts(ids)
            #logging.info(abstracts)
            if ('resultList' in abstracts and 'result' in abstracts['resultList'] and len(abstracts['resultList']['result']) > 0):
              for result in abstracts['resultList']['result']:
                #time.sleep(0.02)
                pubmedID = result['id'] if 'id' in result else None
                if pubmedID is None:
                  print('N', end='', flush=True)
                  print(f'\n{result}\n')
                  continue
                else:
                  print('.', end='', flush=True)
                with driver.session() as session:
                  res = session.run("match(a:Article{pubmed_id:$pmid}) set a.pubmed_evidence=TRUE return id(a)", parameters={"pmid":pubmedID})
                logging.info(f'res: {res}')
                alist = list(res)
                matching_articles = len(alist)
                logging.info(f'matching_articles: {matching_articles}')
                if (matching_articles > 0):
                  article_node = alist[0].value()
                  logging.info(f'PubMed evidence article already exists: {pubmedID}, {article_node}')
                  with driver.session() as session:
                    save_disease_article_relation(disease_node, article_node, session)
                else:
                  with driver.session() as session:
                    save_all(result, disease_node, pubmedID, search_source, session)
              print()
          except Exception as e:
            logging.error(f'Exception when finding articles for disease {gard_id}, error: {e}')
            continue
          


def retrieve_articles():
  """Gets articles from multiple different sources (PubMed,NCATS databases,OMIM) within a 50 year rolling window or since last script execution"""
  today = datetime.now()
  if len(database_last_run) == 0:
    mindate = today - relativedelta(years=50)
    mindate = mindate.strftime(r"%Y/%m/%d")
  else:
    mindate = database_last_run
  today = today.strftime(r"%Y/%m/%d")
  
  logging.info(f'Started save_new_artilcles. Mindate: {mindate}  Maxdate: {today}')
  
  save_disease_articles(mindate, today)
  save_omim_articles(mindate, today)

def main():
  logging.basicConfig(filename='update_neo4j.log', filemode='w', level=logging.ERROR, format='%(asctime)s %(levelname)s:%(message)s')
  print('Started')
  #create_indexes()
  retrieve_articles()
  print('Finished')

if __name__ == '__main__':
  main()

  config.set("database","database_last_run",datetime.now().strftime(r"%Y/%m/%d"))
  with open("config.ini","w") as conf:
    config.write(conf)

#Richard: change how to use MERGE - only include key in MERGE and left other properties to ON CREATE SET
import os
import sys
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
#from firestore_base.ses import trigger_email

today = datetime.now()
today = today.strftime("%Y/%m/%d")
config = configparser.ConfigParser()
# Used to be "https://rdip2.ncats.io:8000"
epiapi_url = "https://rdip2.ncats.io"
#epiapi_url = "http://127.0.0.1:8000"
config.read("config.ini")

def get_gard_list(db):
  #Returns list of the GARD Diseases
  GARDdb = AlertCypher("gard")
  cypher_query = 'match (m:GARD) return m'
  nodes = GARDdb.run(cypher_query)
  results = nodes.data()

  myData = {}
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
  return OrderedDict(sorted(myData.items()))

def get_gard_omim_mapping(db):
  results = get_gard_list(db)
  gardlist = list()
  for res in results:
    gardlist.append(results[res]['gard_id'])

  with GraphDatabase.driver(os.environ['DISEASE_URI']) as driver:
    with driver.session() as session:
      cypher_query = '''
      MATCH (o:S_ORDO_ORPHANET)-[:R_exactMatch|:R_equivalentClass]-(m:S_MONDO)-[:R_exactMatch|:R_equivalentClass]-(n:S_GARD)<-[:PAYLOAD]-(d:DATA)
      WHERE d.is_rare=true and d.gard_id in $glist
      WITH o,n,m,d
      MATCH (o)-[e:R_exactMatch|R_closeMatch]-(k:S_OMIM)<-[:PAYLOAD]-(h)
      RETURN DISTINCT d.gard_id as gard_id,d.name as name, e.name as match_type,h.notation as omim_id,h.label as omim_name
      ORDER BY gard_id
      '''
      nodes = session.run(cypher_query, parameters={'glist':gardlist})
      omim = [record for record in nodes.data()]
      return omim

#get OMIM json by OMIM number
def find_OMIM_articles(db, OMIMNumber):
  params = {'mimNumber': OMIMNumber, 'include':"all", 'format': 'json', 'apiKey': os.environ['OMIM_KEY']}
  return requests.post("https://api.omim.org/api/entry?%s", data=params).json()

#Parse OMIM json and return a map: pubmed_id -> OMIM section   
def get_article_in_section(omim_reference):
  # if no pmid in reference, dont run pubtator but still include it, else run pubtator on pmid
  textSections = jmespath.search("omim.entryList[0].entry.textSectionList[*].textSection",omim_reference)
  references = {}
  for t in textSections:
    refs = re.findall("({[0-9]*?:.*?})",t['textSectionContent'])
    if refs:
      sectionReferenced = set()
      for ref in refs:
        splitRef= ref[1:].split(":")
        sectionReferenced.add(splitRef[0])
      references[t['textSectionName']] = sectionReferenced

  refNumbers = jmespath.search("omim.entryList[0].entry.referenceList[*].reference.[referenceNumber,pubmedID]",omim_reference)
  articleString = {}
  if refNumbers is None:
    return articleString
  
  for refNumber,pmid in refNumbers:
    if not pmid: continue
    
    tsections = []
    for idx, sectionName in enumerate(references):
      if references[sectionName].intersection(set([str(refNumber)])):
        tsections.append(sectionName)
        
    if tsections:
      articleString[str(pmid)] = tsections
    else:
      articleString[str(pmid)] = ['See Also']
      
  return articleString

def get_article_id(pubmed_id, driver):
  article_id = None
  result = driver.run("MATCH(a:Article {pubmed_id:$pmid}) return id(a) as id", args = {'pmid':pubmed_id})
  record = result.single()
  
  if record:
    article_id = record["id"]
    
  return article_id

def get_disease_id(gard_id, driver):
  id = None
  result = driver.run("MATCH(a:Disease {gard_id:$gard_id}) return id(a) as id", args = {'gard_id':gard_id})
  record = result.single()
  #logging.info(f'record: {record}')
  if record:
    id = record["id"]
  return id
    
def save_omim_articles(db, mindate, maxdate):
  omim_api_key = os.environ['OMIM_KEY']
  if len(omim_api_key) == 0:
    return

  results = get_gard_list(db)

  search_source = 'omim_evidence'
  for no, gard_id in enumerate(results):
    print('UPDATING OMIM: ' + gard_id)
    try:
      omim_ids = results[gard_id]["OMIM"]
    except Exception as e:
      print(e)
      continue
    
    if not omim_ids:
      continue
    for omim in omim_ids:    
      omim_json = None

      try:
        omim_json = find_OMIM_articles(db, omim)
      except Exception as e:
        logging.error(f' Exception when search omim_id {omim}: error {e}')
        continue
        
      sections = get_article_in_section(omim_json)
      logging.info(f'sections: {sections}')
      new_sections = dict(sections)
    
      for pubmed_id in sections:
        article_id = get_article_id(pubmed_id, db)
        if (article_id):
          logging.info(f'PubMed evidence article already exists: {pubmed_id}, {article_id}')
          save_omim_article_relation(article_id, omim, sections[pubmed_id], db)
          print(pubmed_id)
          print(omim)
          print(article_id)
          new_sections.pop(pubmed_id)

        else:
          logging.info(f'PubMed evidence article NOT exists: {pubmed_id}, {article_id}')

    logging.info(f'sections after loop: {new_sections}' )   
    save_omim_remaining_articles(gard_id, omim, new_sections, search_source, db)      
    
      
def save_omim_article_relation(article_id, omim_id, sections, driver):
  query = '''
  MATCH (a:Article) WHERE id(a) = $article_id
  SET a.refInOMIM = TRUE
  MERGE (p:OMIMRef {omimId:$omim_id, omimSections:$sections})
  MERGE (a) - [r:HAS_OMIM_REF] -> (p)
  '''
  driver.run(query, args={
    "article_id":article_id,
    "omim_id":omim_id,
    "sections":sections
  })          
          
def save_omim_remaining_articles(gard_id, omim_id, sections, search_source, driver):
  pubmed_ids = list(sections.keys())
  disease_id = get_disease_id(gard_id, driver)
  logging.info(f'pubmed_ids: {pubmed_ids}')
  
  #Save article and related information first
  save_articles(disease_id, pubmed_ids, search_source, driver)
  for pubmed_id in sections:
    article_id = get_article_id(pubmed_id, driver)
    if (article_id):
      save_omim_article_relation(article_id, omim_id, sections[pubmed_id], driver)
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
  url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=\"{keyword}\"[~1]&mindate={mindate}&maxdate={maxdate}&retmode=json&retmax=10000"
  response = requests.post(url).json()
  return response

def fetch_abstracts(pubmedIDs): 
  #fetch abstract for an article
  responses = list()
  batches = [pubmedIDs[i * 1000:(i + 1) * 1000] for i in range((len(pubmedIDs) + 1000 - 1) // 1000 )]
  
  for batch in batches:
    ids = ' OR ext_id:'.join(batch)
    ids = 'ext_id:' +ids
    
    url = "https://www.ebi.ac.uk/europepmc/webservices/rest/searchPOST"
    params = {"query": ids, "pageSize": "1000", "resultType": "core", "format": "json"}
    head = {'Content-type': 'application/x-www-form-urlencoded'}
    try:
      response = requests.post(url=url, data=params, headers=head).json()
      responses.append(response)
      
    except Exception as e:
      print('error in fetch abstracts')
      print(e)
      responses = None

  return responses
  
def fetch_pubtator_annotations(pubmedId):
  #fetch annotations from pubtator
    try:
      pubtatorUrl = "https://www.ncbi.nlm.nih.gov/research/pubtator-api/publications/export/biocjson?pmids=" + pubmedId
    
      r = requests.get(pubtatorUrl)
      if (not r or r is None or r ==''):
        logging.error(f'Can not find PubTator for: {pubmedId}')
        return None
      else:
        return r.json()
    except TimeoutError as e:
      time.sleep(1)
      fetch_pubtator_annotations(pubmedId)
    except ValueError as e:
      return None
    
def fetch_pmc_fulltext_xml(pmcId):
  #fetch full text xml from pmc
  pmcUrl = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id=" + pmcId
  return requests.get(pmcUrl)

def fetch_pmc_fulltext_json(pubmedId):
  #fetch full text json from pmc
  pmcUrl = "https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi/BioC_json/" + pubmedId + "/unicode" 
  return requests.urlopen(pmcUrl).json()

def create_disease(session, gard_id, rd):
  query = '''
  MERGE (d:Disease {gard_id:$gard_id}) 
  ON CREATE SET
    d.gard_id = $gard_id,
    d.name = $name,
    d.classification = $classification, 
    d.synonyms = $synonyms,
    d.type = $type
  RETURN id(d)
  '''
  params = {
    "gard_id":gard_id,
    "gard_id":rd['gard_id'],
    "name":rd['name'], 
    "type":rd['type'],
    "classification":rd['classification'],
    "synonyms":rd['synonyms']
  }

  return session.run(query, args=params).single().value()

def create_article(tx, abstractDataRel, disease_node, search_source):
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
    n.isEpi = $isEpi, 
    n.inPMC = $inPMC, 
    n.hasPDF = $hasPDF, 
    n.source = $source,
    n.pubType = $pubtype,
    n.DateCreatedRDAS = $now,
    n.''' + search_source + ''' = true
  MERGE (d)-[r:MENTIONED_IN]->(n)
  RETURN id(n)
  '''

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
    "pubtype":abstractDataRel['pubTypeList']['pubType'] if 'pubTypeList' in abstractDataRel else '',
    "now": date.today().strftime("%m/%d/%y"),
    "isEpi": False, #Defaults to False, changes to True if there is Epi data in article later on
    "citedByCount":int(abstractDataRel['citedByCount']) if 'citedByCount' in abstractDataRel else 0,
    }
  
  response = tx.run(create_article_query, args=params).single().value()
  return tx.run(create_article_query, args=params).single().value()

def create_authors(tx, abstractDataRel, article_node):
  create_author_query = '''
  MATCH (a:Article) WHERE id(a) = $article_id
  MERGE (p:Author {fullName:$fullName, firstName:$firstName, lastName:$lastName})
  MERGE (p) - [r:WROTE] -> (a)
  '''
  for author in abstractDataRel['authorList']['author']:
    tx.run(create_author_query, args={
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

#Richard: remove (d:Disease) from MATCH
def create_keywords(tx, abstractDataRel, article_node):
  create_keyword_query = '''
  MATCH (a:Article) WHERE id(a) = $article_id
  MERGE (k:Keyword {keyword:$keyword}) 
  MERGE (k)- [r:KEYWORD_FOR] -> (a)
  '''
  
  for keyword in abstractDataRel:
    if keyword:
      tx.run(create_keyword_query, args={
        "article_id":article_node,      
        "keyword": keyword
      })

#This classifies if an article is epidemiological or not.
def get_isEpi(text, url=f"{epiapi_url}/api/postEpiClassifyText/"):
  #check if the article is an Epi article using API
  try:
    response = requests.post(url, json={'text': text})
    response = response.json()
    return response['IsEpi']
  except Exception as e:
    logging.error(f'Exception during get_isEpi. text: {text}, error: {e}')
    raise e
  

def get_epiExtract(text, url=f"{epiapi_url}/api/postEpiExtractText"):
  #Returns a dictionary of the form 
  '''
  {DATE:['1989'],
  LOC:['Uruguay', 'Brazil'],
  STAT:['1 in 10000',1/83423]
  ...}
  '''
  try:
    epi_info = dict(requests.post(url, json={'text': text,'extract_diseases':False}).json())
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
        tx.run(create_epidemiology_query, args={
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

  create_epidemiology_query = '''
      MATCH (a:Article) WHERE id(a) = $article_id
      SET a.isEpi = $isEpi'''
  try:
    tx.run(create_epidemiology_query, args={"article_id":article_node, 'isEpi': isEpi})
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
    tx.run(create_fullTextUrls_query, args={
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
    txout = tx.run(create_meshHeadings_query, args=parameters).single()
    if (txout):
      meshHeadingId = txout.value()
      if ('meshQualifierList' in meshHeading and 'meshQualifier' in meshHeading['meshQualifierList']):
        for meshQualifier in meshHeading['meshQualifierList']['meshQualifier']:
          #This converts the MeSH majorTopic_YN property to a boolean isMajorTopic
          if 'majorTopic_YN' in meshQualifier:
            isMajorTopic = True if meshQualifier['majorTopic_YN'] == 'Y' else False # The other option is 'N
          tx.run(create_meshQualifiers_query, args={
          "meshHeading_id":meshHeadingId,
          "abbreviation": meshQualifier['abbreviation'] if 'abbreviation' in meshQualifier else '',
          "qualifierName": meshQualifier['qualifierName'] if 'qualifierName' in meshQualifier else '',
          "isMajorTopic": isMajorTopic,
          })

def create_chemicals(tx, abstractDataRel, article_node):
  create_chemicals_query = '''
  MATCH (a:Article) WHERE id(a) = $article_id
  MERGE (u:Substance {name:$name, registryNumber:$registryNumber}) - [r:SUBSTANCE_ANNOTATED_BY_PUBMED] -> (a)
  '''
  for chemical in abstractDataRel:
    tx.run(create_chemicals_query, args={
      "article_id":article_node,
      "name": chemical['name'] if 'name' in chemical else '',
      "registryNumber": chemical['registryNumber'] if 'registryNumber' in chemical else '',
    })

def create_annotations(tx, pubtatorData, article_node):
  if pubtatorData:
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
        temp = parameters['text']
        if len(temp) > 0:
          try:
            temp = temp.split(",")
          except:
            pass
        parameters['text'] = temp
        txout = tx.run(create_annotations_query, args=parameters)

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
    logging.info(f'Invoking create_article')
    article_node = create_article(session, abstract, disease_node, search_source)
    if ('meshHeadingList' in abstract and 
    'meshHeading' in abstract['meshHeadingList']):
      logging.info(f'Invoking create_meshHeading')
      create_meshHeadings(session, abstract['meshHeadingList']['meshHeading'], article_node)

    if ('authorList' in abstract and 
    'author' in abstract['authorList']):
      logging.info(f'Invoking create_authors')
      create_authors(session, abstract, article_node)
    
    if ('journalInfo' in abstract):
      logging.info(f'Invoking create_journal')
      create_journal(session, abstract['journalInfo'], article_node)
    
    if ('keywordList' in abstract and 
    'keyword' in abstract['keywordList']):
      logging.info(f'Invoking create_keywords')
      create_keywords(session, abstract['keywordList']['keyword'], article_node)
    
    logging.info(f'Invoking create_epidemiology')
    if ('abstractText' in abstract and 'title' in abstract):
      create_epidemiology(session, abstract, article_node)

    if ('fullTextUrlList' in abstract and 
    'fullTextUrl' in abstract['fullTextUrlList']):
      logging.info(f'Invoking create_fullTextUrls')
      create_fullTextUrls(session, abstract['fullTextUrlList']['fullTextUrl'], article_node)
    
    if ('chemicalList' in abstract and 
    'chemical' in abstract['chemicalList']):
      logging.info(f'Invoking create_chemical')
      create_chemicals(session, abstract['chemicalList']['chemical'], article_node)
    
    #begin another transaction save article annotations
    logging.info(f'Invoking create annotations')
    annos = ''
    try:
      annos = fetch_pubtator_annotations(pubmedID)
      if annos:
        create_annotations(session, annos, article_node)
    except Exception as e:
      logging.warning(f'\nException creating annotations for article {pubmedID}:  {e}')

def save_articles(disease_node, pubmed_ids, search_source, session):
  all_abstracts = fetch_abstracts(pubmed_ids)

  if all_abstracts == None:
    return

  for abstracts in all_abstracts:
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
            res = session.run("match(a:Article{pubmed_id:$pmid}) return id(a) as id", args = {'pmid':pubmedID})
            record = res.single()
            if (record):
              continue # place update function for abstractText?
              # add fetch pubtator function?
            else:
              try:
                save_all(result, disease_node, pubmedID, search_source, session)
              except Exception as e:
                logging.error(f" Exception when calling save_all, error: {e}")
              
      except Exception as e:
        #print('disease_node',disease_node, 'pubmed_ids',pubmed_ids, 'search_source',search_source,sep='\n')
        logging.error(f" Exception when iterating abstracts['resultList']['result'], result: {result}, error: {e}")  

def filter_existing(db, gard_id, pmids):
  query = fr'MATCH (x:Disease)--(y:Article) WHERE x.gard_id = "{gard_id}" AND y.pubmed_id IN {pmids} RETURN y.pubmed_id'
  response = db.run(query).data()
  if len(response) > 0:
    for id in response:
      pmids.remove(id['y.pubmed_id'])
    if len(pmids) == 0:
      pmids = None   
  return pmids

def filter_synonyms(syns):
    filtered = list()
    for syn in syns:
        if ' ' in syn:
            filtered.append(syn)
    return filtered

def save_disease_articles(db, mindate, maxdate):
  #Find and save article data for GARD diseases between from mindate to maxdate
      results = get_gard_list(db)
      search_source = 'pubmed_evidence'
      progress = db.getConf('DATABASE', 'pubmed_progress')
      
      if progress == '':
        progress = 0
      else:
        progress = int(progress)
        
      for idx, gard_id in enumerate(results):
        if idx < progress:
          continue
        if gard_id == None:
          continue

        no = 0
        rd = results[gard_id] #'GARD:0007893'
        
        searchterms = filter_synonyms(rd['synonyms'])
        searchterms.extend([rd['name']])
        
        for searchterm in searchterms:
          try:
            pubmedIDs = find_articles(searchterm,mindate,maxdate) #Use names AND synonyms (only include syns with more than 1 word, if more than 1 word then count if they have more than 5 characters???)
          except Exception as e:
            logging.error(f'Exception when finding articles: {e}')
            continue

          try:
            no = pubmedIDs['esearchresult']['count']
          except:
            no = 0

          print(idx, gard_id, "Articles:", no, rd["name"]+'['+searchterm+']')
          disease_node = create_disease(db, gard_id, rd)
        
          if not 'esearchresult' in pubmedIDs:
            print('no esearchresult in pubmedIDs')
            continue

          try:
            pubmed_ids = filter_existing(db, gard_id, pubmedIDs['esearchresult']['idlist'])
            # handle existing IDs and update them 
            if pubmed_ids:
              save_articles(disease_node, pubmed_ids, search_source, db)
            else:
              print('All articles already in database')
              continue
          except Exception as e:
            logging.error(f'Exception when finding articles for disease {gard_id}, error1: {e}')
            continue

          try:
            all_abstracts = fetch_abstracts(pubmed_ids)
            if all_abstracts == None:
              continue
            
            for abstracts in all_abstracts:
              if ('resultList' in abstracts and 'result' in abstracts['resultList'] and len(abstracts['resultList']['result']) > 0):
                for result in abstracts['resultList']['result']:
                  pubmedID = result['id'] if 'id' in result else None
                
                  if pubmedID is None:
                    print('N', end='', flush=True)
                    continue
                  else:
                    print('.', end='', flush=True)

                  res = db.run("match(a:Article{pubmed_id:$pmid}) set a.pubmed_evidence=TRUE return id(a)", args={"pmid":pubmedID})
                  alist = list(res)
                  matching_articles = len(alist)

                  if (matching_articles > 0):
                    pass
                  else:
                    save_all(result, disease_node, pubmedID, search_source, db)
                print()
                db.setConf('DATABASE', 'pubmed_progress', str(idx))
          except Exception as e:
            logging.error(f'Exception when finding articles for disease {gard_id}, error2: {e}')
            continue
          


def retrieve_articles(db, last_update):
  """
  Gets articles from multiple different sources (PubMed,NCATS databases,OMIM) within a 50 year rolling window or since last script execution
  """
  save_disease_articles(db, last_update, today)
  save_omim_articles(db, last_update, today)

def main(db, update=False):
  '''
  Routes script to either create database from scratch or update. Articles are retrieved from 50 years prior to the current date if creating from scratch, or
  from the last update date to current date if update=True
  '''
  if update == True:
    last_update = db.getConf('DATABASE','pubmed_update')
    last_update = datetime.strptime(last_update, "%m/%d/%y")
  else:
    last_update = datetime.strptime(today, "%Y/%m/%d") - relativedelta(years=50)

  last_update = last_update.strftime("%Y/%m/%d")
  retrieve_articles(db, last_update)
  
  if update:
    pass
    #trigger_email("pubmed")
  else:
    db.setConf('DATABASE', 'pubmed_finished', 'True')
    db.setConf('DATABASE', 'pubmed_progress', '')

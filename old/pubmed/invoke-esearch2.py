import json
from neo4j import GraphDatabase, basic_auth
from datetime import date
import time
import logging, logging.config
import itertools
import requests

def get_gard_list():
  #Returns list of the GARD Diseases
  with GraphDatabase.driver("bolt://disease.ncats.io:80") as driver:
    with driver.session() as session:
      cypher_query = '''
      match p = (d:DATA)-[]-(m:S_GARD) where d.gard_id is not null and d.is_rare = True return  d, m
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
        disease['is_rare'] = res['d'].get("is_rare") if res['d'].get("is_rare") is not None else ''
        disease['categories'] = res['d'].get("categories") if res['d'].get("categories") is not None else ''
        disease['all_names'] = res['m'].get("N_Name") if res['m'].get("N_Name") is not None else ''
        disease['synonyms'] = res['d'].get("synonyms") if res['d'].get("synonyms") is not None else ''
        disease['all_ids'] = res['m'].get("I_CODE") if res['m'].get("I_CODE") is not None else ''
        myData[gard_id] = disease
    return myData

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
      logging.error(f'Can not find pubtator for: {pubmedId}')
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
  return request.urlopen(pmcUrl).json()

def create_disease(session, gard_id, rd):
   return session.run("CREATE (d:Disease {gard_id:$gard_id, name:$name, is_rare:$is_rare, categories:$categories, all_names:$all_names, synonyms:$synonyms, all_ids:$all_ids}) RETURN id(d)", gard_id=rd['gard_id'], name=rd['name'], is_rare=rd['is_rare'], categories=rd['categories'], all_names=rd['all_names'], synonyms=rd['synonyms'], all_ids=rd['all_ids']).single().value()

def create_article(tx, abstractDataRel, disease_node, search_source):
  #Richard: remove: authorString:$authorString,
  #Richard: move source as an anticle property, instead of a relation and another node
  create_article_query = '''
  MATCH (d:Disease) WHERE id(d)=$id
  CREATE (n:Article {pubmed_id:$pubmed_id, doi:$doi, title:$title, abstractText:$abstractText, 
    affiliation:$affiliation, firstPublicationDate:$firstPublicationDate, citedByCount:$citedByCount,
    isOpenAccess:$isOpenAccess, inEPMC:$inEPMC, inPMC:$inPMC, hasPDF:$hasPDF, source:$source, ''' + search_source + ''':true})
  CREATE (d)-[r:MENTIONED_IN]->(n)
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
    "isOpenAccess":abstractDataRel['isOpenAccess'] if 'isOpenAccess' in abstractDataRel else '',
    "inEPMC":abstractDataRel['inEPMC'] if 'inEPMC' in abstractDataRel else '',
    "inPMC":abstractDataRel['inPMC'] if 'inPMC' in abstractDataRel else '',
    "hasPDF":abstractDataRel['hasPDF'] if 'hasPDF' in abstractDataRel else '',
    "citedByCount":abstractDataRel['citedByCount'] if 'citedByCount' in abstractDataRel else ''
    }
  return tx.run(create_article_query,**params ).single().value()

#Richard: change node from "Person" to "Author"
def create_authors(tx, abstractDataRel, article_node, disease_node):
  create_author_query = '''
  MATCH (a:Article) <- [:MENTIONED_IN] - (d:Disease) WHERE id(a) = $article_id and id(d) = $disease_node
  CREATE (p:Author {fullName:$fullName, firstName:$firstName, lastName:$lastName, affiliation:$affiliation})
  CREATE (p) - [r:WROTE] -> (a)
  CREATE (p)-[s:STUDIES]->(d)
  '''
  for author in abstractDataRel['authorList']['author']:
    logging.info(abstractDataRel['authorList'])
    logging.info(author)
    logging.info(article_node)
    logging.info(disease_node)
    tx.run(create_author_query, parameters={
      "article_id":article_node,
      "disease_node":disease_node,
      "fullName": author['fullName'] if 'fullName' in author else '',
      "firstName": author['firstName'] if 'firstName' in author else '',
      "lastName": author['lastName'] if 'lastName' in author else '',
      "affiliation": author['affiliation'] if 'affiliation' in author else ''
    })

def create_journal(tx, abstractDataRel, article_node):
  create_journal_query = '''
  MATCH (a:Article) WHERE id(a) = $article_id
  CREATE (j:Journal{title:$title,medlineAbbreviation:$medlineAbbreviation,essn:$essn,issn:$issn,nlmid:$nlmid})
  CREATE (ji:JournalInfo{issue:$issue, volume:$volume, journalIssueId:$journalIssueId,
    dateOfPublication:$dateOfPublication, monthOfPublication:$monthOfPublication,yearOfPublication:$yearOfPublication,
    printPublicationDate:$printPublicationDate})
  CREATE (a)-[:APPEARS_IN]->(ji)
  CREATE (ji)-[:CONTENT_OF]->(j)
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

def create_keywords(tx, abstractDataRel, article_node):
  create_keyword_query = '''
  MATCH (a:Article), (d:Disease) WHERE id(a) = $article_id
  CREATE (k:Keyword {keyword:$keyword}) 
  CREATE (k)- [r:for] -> (a)
  '''
  for keyword in abstractDataRel:
    if keyword:
      tx.run(create_keyword_query, parameters={
        "article_id":article_node,      
        "keyword": keyword
      })

def create_fullTextUrls(tx, abstractDataRel, article_node):
  create_fullTextUrls_query = '''
  MATCH (a:Article) WHERE id(a) = $article_id
  CREATE (u:fullTextUrl {availability:$availability, availabilityCode:$availabilityCode, documentStyle:$documentStyle,site:$site,url:$url})
  CREATE (u) - [r:CONTENT_FOR] -> (a)
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
  CREATE (m:MeshTerm {majorTopic_YN:$majorTopic_YN, descriptorName:$descriptorName}) 
  CREATE (m) - [r:MESH_TERM_FOR] -> (a)
  RETURN id(m)
  '''
  create_meshQualifiers_query = '''
  MATCH (m:MeshTerm) WHERE id(m) = $meshHeading_id
  CREATE (mq:MeshQualifier {abbreviation:$abbreviation, qualifierName:$qualifierName,majorTopic_YN:$majorTopic_YN}) 
  CREATE (mq) - [r:MESH_QUALIFIER_FOR] -> (m)
  '''
  for meshHeading in abstractDataRel:
    parameters={
      "article_id":article_node,
      "majorTopic_YN": meshHeading['majorTopic_YN'] if 'majorTopic_YN' in meshHeading else '',
      "descriptorName": meshHeading['descriptorName'] if 'descriptorName' in meshHeading else ''
    }
    txout = tx.run(create_meshHeadings_query,**parameters).single()
    if (txout):
      meshHeadingId = txout.value()
      if ('meshQualifierList' in meshHeading and 'meshQualifier' in meshHeading['meshQualifierList']):
        for meshQualifier in meshHeading['meshQualifierList']['meshQualifier']:
          tx.run(create_meshQualifiers_query, parameters={
          "meshHeading_id":meshHeadingId,
          "abbreviation": meshQualifier['abbreviation'] if 'abbreviation' in meshQualifier else '',
          "qualifierName": meshQualifier['qualifierName'] if 'qualifierName' in meshQualifier else '',
          "majorTopic_YN": meshQualifier['majorTopic_YN'] if 'majorTopic_YN' in meshQualifier else '',
          })


def create_chemicals(tx, abstractDataRel, article_node):
  create_chemicals_query = '''
  MATCH (a:Article) WHERE id(a) = $article_id
  CREATE (u:Chemical {name:$name, registryNumber:$registryNumber}) - [r:CHEMICAL_FOR] -> (a)
  '''
  for chemical in abstractDataRel:
    tx.run(create_chemicals_query, parameters={
      "article_id":article_node,
      "name": chemical['name'] if 'name' in chemical else '',
      "registryNumber": chemical['registryNumber'] if 'registryNumber' in chemical else '',
    })

def create_annotations(tx, pubtatorData, article_node):
  create_passages_query = '''
  MATCH(a:Article) WHERE id(a) = $article_id
  CREATE (p:PubtatorPassage {type:$type, text:$text}) - [r:passage_for] -> (a)
  RETURN id(p)
  '''
  create_annotations_query = '''
  MATCH(pp:PubtatorPassage) WHERE id(pp) = $pubtator_passage_id
  CREATE (pa:PubtatorAnnotations {infons_identifier:$infons_identifier, infons_type:$infons_type, text:$text, locations_offset:$locations_offset, locations_length:$locations_length}) - [r:annotations_for] -> (pp)
  '''
  for passage in pubtatorData['passages']:
    txout = tx.run(create_passages_query, parameters={
      "article_id":article_node,
      "type": passage['infons']['type'] if 'type' in passage['infons'] else '',
      "text": passage['text'] if 'text' in passage else '',
    }).single()
    passageId = txout.value()
    for annotation in passage['annotations']:
      parameters={
        "pubtator_passage_id":passageId,
        "infons_identifier": annotation['infons']['identifier'] if ('identifier' in annotation['infons'] and annotation['infons']['identifier'])  else '',
        "infons_type": annotation['infons']['type'] if ('type' in annotation['infons'] and annotation['infons']['type']) else '',
        "text": annotation['text'] if 'text' in annotation else '',
        "locations_offset": annotation['locations'][0]['offset'] if 'offset' in annotation['locations'][0] else '',
        "locations_length": annotation['locations'][0]['length'] if 'length' in annotation['locations'][0] else '',
      }
      txout = tx.run(create_annotations_query, **parameters)

def create_disease_article_relation(tx, disease_node, article_node):
  query = '''
  MATCH (a: Article) WHERE id(a) = $article_id
  MATCH (d: Disease) WHERE id(d) = $disease_id
  CREATE (d)-[:MENTIONED_IN]->(a)
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

    logging.info(f'Invoking create_article')
    article_node = create_article(tx, abstract, disease_node, search_source)
    
    if ('meshHeadingList' in abstract and 
    'meshHeading' in abstract['meshHeadingList']):
      logging.info(f'Invoking create_meshHeading')
      create_meshHeadings(tx, abstract['meshHeadingList']['meshHeading'], article_node)

    if ('authorList' in abstract and 
    'author' in abstract['authorList']):
      logging.info(f'Invoking create_authors')
      create_authors(tx, abstract, article_node, disease_node)
    
    if ('journalInfo' in abstract):
      logging.info(f'Invoking create_journal')
      create_journal(tx, abstract['journalInfo'], article_node)

    if ('keywordList' in abstract and 
    'keyword' in abstract['keywordList']):
      logging.info(f'Invoking create_keywords')
      create_keywords(tx, abstract['keywordList']['keyword'], article_node)
    
    if ('fullTextUrlList' in abstract and 
    'fullTextUrl' in abstract['fullTextUrlList']):
      logging.info(f'Invoking create_fullTextUrls')
      create_fullTextUrls(tx, abstract['fullTextUrlList']['fullTextUrl'], article_node)

    if ('chemicalList' in abstract and 
    'chemical' in abstract['chemicalList']):
      logging.info(f'Invoking create_chemical')
      create_chemicals(tx, abstract['chemicalList']['chemical'], article_node)

    tx.commit()
    
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
         
def save_pubmed_articles(mindate, maxdate):
  #Find and save article data for GARD diseases between from mindate to maxdate
  with GraphDatabase.driver("bolt://localhost:7687", encrypted=False) as driver:
      with driver.session() as session:
        results = get_gard_list()
        results = dict(itertools.islice(results.items(),0,None))
        search_source = 'pubmed_evidence'
        for idx, gard_id in enumerate(results):  
          #time.sleep(0.34)
          no = 0
          rd = results[gard_id]
          logging.info(rd["name"])
          logging.warning(f'{idx}: Invoking find_articles({rd["name"]},{mindate},{maxdate})')
          try:
            pubmedIDs = find_articles(rd["name"],mindate,maxdate)
            no = pubmedIDs['esearchresult']['count']
          except Exception as e:
            logging.error(f'Exception when finding articles: {e}')
            continue
            
          print(idx, gard_id, "Articles:", no, rd["name"])
          logging.debug('Invoking create_gard_disease')
          
          disease_node = create_disease(session, gard_id, rd)
          if not 'esearchresult' in pubmedIDs:
            logging.error(f'\nError find article for this disease: {gard_id}, {rd["name"]}')
            continue
          # Richard: combine all the IDs to one query string

          #logging.info(pubmedIDs['esearchresult']['idlist'])
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
                pubmedID = result['id'] if 'id' in result else None
                if pubmedID is None:
                  print('N', end='', flush=True)
                  print(f'\n{result}\n')
                  continue
                else:
                  print('.', end='', flush=True)
                res = session.run("match(a:Article{pubmed_id:$pmid}) set a.pubmed_evidence=TRUE return id(a)", parameters={"pmid":pubmedID})
                logging.info(f'res: {res}')
                alist = list(res)
                matching_articles = len(alist)
                logging.info(f'matching_articles: {matching_articles}')
                if (matching_articles > 0):
                  article_node = alist[0].value()
                  logging.info(f'PubMed evidence article already exists: {pubmedID}, {article_node}')
                  save_disease_article_relation(disease_node, article_node, session)
                else:
                  save_all(result, disease_node, pubmedID, search_source, session)
              print()
          except Exception as e:
            logging.error(f'Exception when finding articles for disease {gard_id}, error: {e}')
            continue

def save_initial_articles():
  """Write initial article data for the GARD Diseases"""
  mindate = '1900/01/01'
  today = '2021/08/31'
  #today = date.today().strftime("%Y/%m/%d")
  logging.info(f'Started save_new_artilcles. Mindate: {mindate}  Maxdate: {today}')
  save_pubmed_articles(mindate, today)
    
def save_new_articles():
  yesterday = (date.today() - datetime.timedelta(days=1)).strftime("%Y/%m/%d")
  today = date.today().strftime("%Y/%m/%d")
  logging.info(f'Started save_new_artilcles. Mindate: {yesterday}  Maxdate: {today}')

  save_articles(yesterday, today)

def main():
  logging.basicConfig(level=logging.ERROR, format='%(asctime)s %(levelname)s:%(message)s')
  print('Started')
  save_initial_articles()
  print('Finished')


if __name__ == '__main__':
  main()

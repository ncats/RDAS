from AlertCypher import AlertCypher
from collections import OrderedDict
import requests
import time

def get_gard_list():
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

def find_articles(keyword, mindate, maxdate):
    #fetch articles and return a map
    url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=\"{keyword}\"[~1]&mindate={mindate}&maxdate={maxdate}&retmode=json&retmax=10000"
    response = requests.post(url).json()
    time.sleep(0.5)
    return response

def filter_synonyms(syns):
    filtered = list()
    for syn in syns:
        if ' ' in syn:
            filtered.append(syn)
    return filtered

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

def create_disease(db, gard_id, rd):
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

  return db.run(query, args=params).single().value()


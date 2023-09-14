from alert.new.AlertCypher import AlertCypher
from dateutil.relativedelta import relativedelta
from collections import OrderedDict
import requests
from datetime import datetime,date
import json
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
    disease['synonyms'] = res['m']["Synonyms"] if res['m']["Synonyms"] is not None else ''
    myData[gard_id] = disease
  return OrderedDict(sorted(myData.items()))

def filter_synonyms(syns):
    filtered = list()
    for syn in syns:
        if ' ' in syn:
            filtered.append(syn)
    return filtered

def compare_queries(gard_id,term):
  #old_ids = list()
  new_ids = list()

  old_ids = db.run('MATCH (y:GARD)--(x:Article) WHERE y.GardId = \"{gard_id}\" RETURN x.pubmed_id'.format(gard_id=gard_id)).data()
  old_ids = [i['x.pubmed_id'] for i in old_ids]
  #print(old_ids)

  #old = old_query.format(term=term,mindate=mindate,maxdate=maxdate)
  new = new_query.format(term=term,mindate=mindate,maxdate=maxdate)

  #old_response = requests.post(old).json()
  new_response = requests.post(new).json()

  #if 'esearchresult' in old_response:
    #if 'idlist' in old_response['esearchresult']:
      #old_ids = old_response['esearchresult']['idlist']
  
  if 'esearchresult' in new_response:
    if 'idlist' in new_response['esearchresult']:
      new_ids = new_response['esearchresult']['idlist']

  #print(old)
  #print(new)

  #print()
  print(gard_id) 
  print(term)
  #print('IN CURRENT DB')
  #print(old_ids)
  #print('NEW QUERY')
  #print(new_ids)
  #print('----------------------')

  temp = list()
  for element in old_ids:
    if element not in new_ids:
      temp.append(element)

  #print(temp)
  #print('----------------------')
  time.sleep(1)
  return temp

db = AlertCypher('pubmedtest')
today = datetime.now()
today = today.strftime("%Y/%m/%d")
last_update = datetime.strptime(today, "%Y/%m/%d") - relativedelta(years=80)
last_update = last_update.strftime("%Y/%m/%d")

maxdate = today
mindate = last_update

#old_query = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term={term}&mindate={mindate}&maxdate={maxdate}&retmode=json&retmax=10000"
new_query = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=\"{term}\"[tiab:~0]&mindate={mindate}&maxdate={maxdate}&retmode=json&retmax=10000"


results = get_gard_list()
search_source = 'pubmed_evidence'

for idx, gard_id in enumerate(results):
  if gard_id == None:
    continue

  no = 0
  rd = results[gard_id] #'GARD:0007893'
  searchterms = list()
  searchterms.extend([rd['name']])

  for searchterm in searchterms:
    try:
      pubmedIDs = compare_queries(gard_id,searchterm)
      #print(pubmedIDs)
      for pmid in pubmedIDs:
        try:
          #pass
          query = 'MATCH (x:Article)-[r:MENTIONED_IN]-(g:GARD) WHERE x.pubmed_id = \"{pmid}\" AND g.GardId = \"{gard}\" DELETE r'.format(pmid=pmid,gard=gard_id)
          #print(query)
          db.run(query)
        except Exception:
          continue

    except Exception as e:
      print('error')
      print(e)
      continue








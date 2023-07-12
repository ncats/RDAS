import requests
import logging
from multiprocessing import Process, Lock
from threading import Thread
import os
import sys
import urllib3
import json
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
from alert.new.AlertCypher import AlertCypher


epiapi_url = "https://ncats-rdas-lnx-dev.ncats.nih.gov:443/api/"

def get_isEpi(text, lock, url=f"{epiapi_url}postEpiClassifyText/"):
  #check if the article is an Epi article using API
  try:
    #lock.acquire(timeout=60)
    response = requests.post(url, json={'text': text}, verify=False)
    #lock.release()
    response = response.json()
    return response['IsEpi']
  except Exception as e:
    pass
    #lock.acquire(timeout=60)
    #print(f'Exception during get_isEpi. text: {text}, error: {e}')
    #lock.release()

def get_epiExtract(text, lock, url=f"{epiapi_url}postEpiExtractText"):
  #Returns a dictionary of the form
  '''
  {DATE:['1989'],
  LOC:['Uruguay', 'Brazil'],
  STAT:['1 in 10000',1/83423]
  ...}
  '''
  #proxies = {'https': 'https://ncats-rdas-lnx-dev.ncats.nih.gov:443/api/'}
  try:
    request_body = {'text': text,'extract_diseases':False}
    http = urllib3.PoolManager(cert_reqs='CERT_NONE')
    encoded_data = json.dumps(request_body).encode('utf-8')

    #lock.acquire(timeout=60)
    epi_info = http.request(method='POST',url=url,body=encoded_data,headers={'Content-Type':'application/json'})
    #lock.release()
    epi_info = json.loads(epi_info.data.decode('utf-8'))

    #epi_info = requests.post(url, json={'text': text,'extract_diseases':False}, verify=False)
    return dict(epi_info)
  except Exception as e:
    pass
    #lock.acquire(timeout=60)
    #print(f'Exception during get_EpiExtract. text: {text}, error: {e}')
    #lock.release()
    #print(requests.post(url,json={'text': text,'extract_diseases':False}))

def create_epidemiology(tx, abstractDataRel, article_node, lock):
  text = abstractDataRel['title'] + ' ' + abstractDataRel['abstractText']
  lock.acquire(timeout=1000)
  isEpi = get_isEpi(text, lock)
  lock.release()
  if isEpi:
    lock.acquire(timeout=1000)
    epi_info = get_epiExtract(text, lock)
    lock.release()
    #This checks if each of the values in the epi_info dictionary is empty. If they all are empty, then the node is not added.
    if type(epi_info) == dict and  sum([1 for x in epi_info.values() if x]) > 0:
      try:
        create_epidemiology_query = '''
          MATCH (a:Article) WHERE id(a) = $article_id
          SET a.isEpi = $isEpi
          MERGE (n:EpidemiologyAnnotation {isEpi:$isEpi, epidemiology_type:$epidemiology_type, epidemiology_rate:$epidemiology_rate, date:$date, location:$location, sex:$sex, ethnicity:$ethnicity})
          MERGE (n) -[r:EPIDEMIOLOGY_ANNOTATION_FOR]-> (a)
          '''
        #lock.acquire(timeout=1000)
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
        #lock.release()
      except Exception as e:
        print(f'Exception during tx.run(create_epidemiology_query) where isEpi is True. {e}')

  create_epidemiology_query = '''
      MATCH (a:Article) WHERE id(a) = $article_id
      SET a.isEpi = $isEpi'''
  try:
    #lock.acquire(timeout=1000)
    tx.run(create_epidemiology_query, args={"article_id":article_node, 'isEpi': isEpi})
    #lock.release()
  except Exception as e:
    pass
    #lock.acquire(timeout=1000)
    #print(f'Exception during tx.run(create_epidemiology_query) where isEpi is False. {e}')
    #lock.release()

def gather_epi(db, batch, tnum, lock):
  for idx, r in enumerate(batch):
    abstract = r['abstract']
    title = r['title']
    abstractDataRel = {'abstractText':abstract,'title':title}
    ID = r['id']
    create_epidemiology(db, abstractDataRel, ID, lock)

    lock.acquire(timeout=1000)
    with open(f'nohup_{tnum}.txt', 'w') as tfile:
      tfile.write(f'{idx} Processed')
    lock.release()

db = AlertCypher('pubmed')
res = db.run('MATCH (x:Article) WHERE NOT x.abstractText = \"\" OR NOT x.title = \"\" RETURN x.abstractText AS abstract, x.title AS title, ID(x) AS id').data()

res = res[428704:len(res)-2632] #399483:len(res)-2632 #438704
print('[PM] STARTING FROM IDX 438704')

length = len(res)
batch_size = (length//16)+1 #8
threads = list()
batches = list()
lock = Lock()

batches = [res[i:i + batch_size] for i in range(0, len(res), batch_size)]
print(len(batches))
print(len(batches[0]))
print('CREATING PROCESSES')
for idx,b in enumerate(batches):
  b = b[28229:] #39221
  thr = Process(target=gather_epi, args=(db, b, idx, lock))
  threads.append(thr)

for thread in threads:
  thread.start()
  print('thread started')

for thread in threads:
  thread.join()

print('FINISHED')

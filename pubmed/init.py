import sys
import os
import sysvars
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
from pubmed import methods as rdas
from AlertCypher import AlertCypher
import pandas as pd
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
import ast

def main():
    db = AlertCypher('pubmed')

    maxdate = datetime.now()
    maxdate = maxdate.strftime("%Y/%m/%d")
    mindate = datetime.strptime(maxdate, "%Y/%m/%d") - relativedelta(years=50)
    mindate = mindate.strftime("%Y/%m/%d")
    search_source = 'pubmed_evidence'
    all_pmids = dict()

    results = rdas.get_gard_list()
    num_diseases = len(results)

    if not os.path.exists(f'{sysvars.pm_files_path}all_article_data.json'):
          print('[PM] GATHERING ALL ARTICLE PMIDS')

          for idx, gard_id in enumerate(results):
            print('\n' + str(idx) + '/' + str(num_diseases) + ' Completed')

            if gard_id == None:
              continue
        
            if os.path.exists(f'{sysvars.pm_files_path}all_article_data.json'):
              with open(f'{sysvars.pm_files_path}all_article_data.json', 'r') as f:
                all_pmids = json.load(f)
                all_pmids[gard_id] = list()
                rd = results[gard_id]
                searchterms = rdas.filter_synonyms(rd['synonyms'])
                searchterms.extend([rd['name']])

                for searchterm in searchterms:
                  try:
                    pubmedIDs = rdas.find_articles(searchterm,mindate,maxdate)
                    pubmedIDs = pubmedIDs['esearchresult']['idlist']            
                    all_pmids[gard_id].extend(pubmedIDs)

                  except Exception as e:
                    print(f'Exception when finding articles: {pubmedIDs}')
                    continue

                all_pmids[gard_id] = list(set(all_pmids[gard_id]))
                print('Articles found for: ' + rd['name'] + '[' + gard_id + ']; ' + str(len(all_pmids[gard_id])))

                with open(f'{sysvars.pm_files_path}all_article_data.json', 'w') as outfile:
                  json.dump(all_pmids, outfile)

            else:
              all_pmids[gard_id] = list()
              rd = results[gard_id]
              searchterms = rdas.filter_synonyms(rd['synonyms'])
              searchterms.extend([rd['name']])

              for searchterm in searchterms:
                try:
                  pubmedIDs = rdas.find_articles(searchterm,mindate,maxdate)
                  pubmedIDs = pubmedIDs['esearchresult']['idlist']
                  all_pmids[gard_id].extend(pubmedIDs)

                except Exception as e:
                  print(f'Exception when finding articles: {pubmedIDs}')
                  continue

              all_pmids[gard_id] = list(set(all_pmids[gard_id]))
              disease_node = rdas.create_disease(db, gard_id, rd)
              print('Articles found for: ' + rd['name'] + '[' + gard_id + ']; ' + str(len(all_pmids[gard_id])))

              with open(f'{sysvars.pm_files_path}all_article_data.json', 'w') as outfile:
                json.dump(all_pmids, outfile)



    if not os.path.exists(f'{sysvars.pm_files_path}abstracts') or not db.getConf('DATABASE','pm_abstract_progress') == '':
      if not os.path.exists(f'{sysvars.pm_files_path}abstracts'):
        os.makedirs(f'{sysvars.pm_files_path}abstracts')

        progress = int(db.getConf('DATABASE','pm_abstract_progress'))
        if progress == '':
            progress = 0

        print('[PM] GATHERING ALL ARTICLE ABSTRACTS')
        with open(f'{sysvars.pm_files_path}all_article_data.json', 'r') as f:
          pmids = json.load(f)
          num_diseases = len(pmids)

        for idx, (k,v) in enumerate(pmids.items()):
          if idx < progress:
            continue

          print(str(idx) + '/' + str(num_diseases) + ' Completed [{gard}]'.format(gard=k))
          abstracts = rdas.fetch_abstracts(v)
              
          with open(f'{sysvars.pm_files_path}abstracts/{k}.json', 'w') as abs_write:
            json.dump(abstracts, abs_write)

          db.setConf('DATABASE','pm_abstract_progress',str(idx))
    


    for idx,filename in enumerate(os.listdir(f'{sysvars.pm_files_path}abstracts/')):
      with open(f'{sysvars.pm_files_path}abstracts/{filename}', 'r') as json_file:
        abstracts = json.load(json_file)

        # abstracts var is a list of batches of 1000 article abstract data
        for batch in abstracts:
          # process files
          if ('resultList' in batch and 'result' in batch['resultList'] and len(batch['resultList']['result']) > 0):
            for result in batch['resultList']['result']:
              pubmedID = result['id'] if 'id' in result else None

              if pubmedID is None:
                continue

              res = db.run("match(a:Article{pubmed_id:$pmid}) set a.pubmed_evidence=TRUE return id(a)", args={"pmid":pubmedID})
              reslist = list(res)
              matching_articles = len(reslist)

              if (matching_articles > 0):
                pass
              else:
                save_all(result, disease_node, pubmedID, search_source, db)
      
    





    # Run EPI API after entire pubmed database is created
    pass

if __name__ == '__main__':
    main()

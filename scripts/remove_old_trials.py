from AlertCypher import AlertCypher
from datetime import datetime
import gard.methods as gmethods

def keywithmaxval(d):
     """ a) create a list of the dict's keys and values; 
         b) return the key with the max value"""  
     v = list(d.values())
     k = list(d.keys())
     return k[v.index(max(v))]

db = AlertCypher('clinicaltest')
response = db.run('MATCH (c:ClinicalTrial) with c.NCTId as p, count(c.NCTId) as cpr where cpr>1 return p as NCTID').data()
for res in response:
    nctid = res['NCTID']
    res = db.run('MATCH (x:ClinicalTrial) WHERE x.NCTId = \"{nctid}\" RETURN ID(x) as ct_id, x.LastUpdatePostDate as date'.format(nctid=nctid)).data()
    print(nctid)

    date_dict = dict()
    for r in res:
        ctid = r['ct_id']
        print(ctid)
        date = r['date']
        print(date)
        date = datetime.strptime(date, "%B %d, %Y") #June 15, 2023
        print(date)
        date_dict[ctid] = date

    
    maxkey = keywithmaxval(date_dict)
    print(maxkey)
    print('----')
    
    db.run('MATCH (x:ClinicalTrial) WHERE ID(x) = {maxkey} DETACH DELETE x'.format(maxkey=maxkey))    

gmethods.get_node_counts()
        

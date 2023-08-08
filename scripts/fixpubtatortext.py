from AlertCypher import AlertCypher

db = AlertCypher("pubmed")
result = db.run("MATCH (x:PubtatorAnnotation) RETURN x.text as text,ID(x) as pubid").data()

for idx,res in enumerate(result):
    print(idx)
    txt = res['text']
    pubid = res['pubid']

    if type(txt) == str:
        lsttxt = [txt]

    else:
        continue

    db.run("MATCH (x:PubtatorAnnotation) WHERE ID(x) = {pubid} SET x.text = {lsttxt}".format(pubid=pubid,lsttxt=lsttxt))
    

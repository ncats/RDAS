from neo4j_backend import Neo4jPubmed as np

def test():
    uri = "bolt+s://rdip2.ncats.io"
    p = np(uri)
    dlist = p.get_diseases()
    print(dlist)
    p.close()
    
if __name__ == "__main__":
    test()

from neo4j import GraphDatabase
from multiprocessing import Pool



uri= "uri"
username="username"
password="password"

# Initialize the Neo4j driver
driver = GraphDatabase.driver(uri, auth=(username, password))

'''
EpidemiologyAnnotation                 33047999
FullTextUrl                     3982440
CONTENT_FOR                    7951120
CONTENT_OF                     1051928
HAS_OMIM_REF                   50938
MESH_QUALIFIER_FOR             995881
MESH_TERM_FOR                  40161141
SUBSTANCE_ANNOTATED_BY_PUBMED  5961279 
'''

# Define a function to run the Cypher query
def delete_and_detach_nodes1():
   n=0
   while n<= 2737737:
    driver = GraphDatabase.driver(uri, auth=(username, password))
    cypher_query = """
    MATCH (r:PubtatorAnnotation) with r limit 50000 delete r 
    """
    n+=50000
    with driver.session(database="socialnetwork1") as session:
        session.run(cypher_query)
    driver.close()
   print("relationships deleted successfully.")
        
def delete_and_detach_nodes2():
   n=0
   while n<= 7946848:
    driver = GraphDatabase.driver(uri, auth=(username, password))   
    cypher_query = """
    MATCH (r:FullTextUrl) with r limit 50000 delete r 
    """
    n+=50000
    with driver.session(database="socialnetwork1") as session:
        session.run(cypher_query)
    driver.close()
   print("relationships deleted successfully.")
   
def delete_and_detach_nodes3():
   n=0
   while n<= 16349:
    driver = GraphDatabase.driver(uri, auth=(username, password))   
    cypher_query = """
    MATCH (r:Journal) with r limit 50000 delete r 
    """
    n+=50000
    with driver.session(database="socialnetwork1") as session:
        session.run(cypher_query)
    driver.close()
   print("relationships deleted successfully.")
   
def delete_and_detach_nodes4():
   n=0
   while n<= 1045783:
    driver = GraphDatabase.driver(uri, auth=(username, password))   
    cypher_query = """
    MATCH (r:JournalVolume) with r limit 50000 delete r 
    """
    n+=50000
    with driver.session(database="socialnetwork1") as session:
        session.run(cypher_query)
    driver.close()
   print("relationships deleted successfully.")

def delete_and_detach_nodes5():
   n=0
   while n<= 329:
    driver = GraphDatabase.driver(uri, auth=(username, password))   
    cypher_query = """
    MATCH (r:MeshQualifier) with r limit 50000 delete r 
    """
    n+=50000
    with driver.session(database="socialnetwork1") as session:
        session.run(cypher_query)
    driver.close()
   print("relationships deleted successfully.")
        
def delete_and_detach_nodes6():
   n=0
   while n<= 93759:
    driver = GraphDatabase.driver(uri, auth=(username, password))   
    cypher_query = """
    MATCH (r:MeshTerm) with r limit 50000 delete r 
    """
    n+=50000
    with driver.session(database="socialnetwork1") as session:
        session.run(cypher_query)
    driver.close()
   print("relationships deleted successfully.")

def delete_and_detach_nodes7():
   n=0
   while n<= 74488:
    driver = GraphDatabase.driver(uri, auth=(username, password))   
    cypher_query = """
    MATCH (r:Substance) with r limit 50000 delete r 
    """
    n+=50000
    with driver.session(database="socialnetwork1") as session:
        session.run(cypher_query)
    driver.close()
   print("relationships deleted successfully.")

   
def delete_and_detach_nodes8():
   n=0
   while n<= 26235:
    driver = GraphDatabase.driver(uri, auth=(username, password))   
    cypher_query = """
    MATCH (r:OMIMRef) with r limit 50000 delete r 
    """
    n+=50000
    with driver.session(database="socialnetwork1") as session:
        session.run(cypher_query)
    driver.close()
   print("relationships deleted successfully.")        





def run_task(function):
    return function()

# Execute the function
def run_parallel_queries(num_processes=8):
    tasks = [
        (delete_and_detach_nodes1, ),
        (delete_and_detach_nodes2, ),
        (delete_and_detach_nodes3, ),
        (delete_and_detach_nodes4,),
        (delete_and_detach_nodes5,),
        (delete_and_detach_nodes6, ),
        (delete_and_detach_nodes7, ),
        (delete_and_detach_nodes8, )
    ]
    # Use multiprocessing Pool to run queries in parallel
    with Pool(num_processes) as pool:
        pool.starmap(run_task, tasks)


if __name__ == "__main__":

    run_parallel_queries( num_processes=8)



# Close the driver connection






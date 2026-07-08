from neo4j import GraphDatabase
from multiprocessing import Pool

# Create a connection to the Neo4j database
#uri = "bolt://localhost:7687"  # Change this to your Neo4j URI
#username = "neo4j"  # Change to your username
#password = "password"  # Change to your password

uri= "uri"
username="username"
password="password"

# Initialize the Neo4j driver
driver = GraphDatabase.driver(uri, auth=(username, password))

'''
annotation_for                 33047999
APPEARS_IN                     3982440
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
   while n<= 33047999:
    driver = GraphDatabase.driver(uri, auth=(username, password))
    cypher_query = """
    MATCH ()-[r:ANNOTATION_FOR]->() with r limit 25000 delete r 
    """
    n+=25000
    with driver.session(database="socialnetwork1") as session:
        session.run(cypher_query)
    driver.close()
   print("relationships deleted successfully.")
        
def delete_and_detach_nodes2():
   n=0
   while n<= 3982440:
    driver = GraphDatabase.driver(uri, auth=(username, password))   
    cypher_query = """
    MATCH ()-[r:APPEARS_IN]->() with r limit 25000 delete r 
    """
    n+=25000
    with driver.session(database="socialnetwork1") as session:
        session.run(cypher_query)
    driver.close()
   print("relationships deleted successfully.")
   
def delete_and_detach_nodes3():
   n=0
   while n<= 7951120:
    driver = GraphDatabase.driver(uri, auth=(username, password))   
    cypher_query = """
    MATCH ()-[r:CONTENT_FOR]->() with r limit 25000 delete r 
    """
    n+=25000
    with driver.session(database="socialnetwork1") as session:
        session.run(cypher_query)
    driver.close()
   print("relationships deleted successfully.")
   
def delete_and_detach_nodes4():
   n=0
   while n<= 1051928:
    driver = GraphDatabase.driver(uri, auth=(username, password))   
    cypher_query = """
    MATCH ()-[r:CONTENT_OF]->() with r limit 25000 delete r 
    """
    n+=25000
    with driver.session(database="socialnetwork1") as session:
        session.run(cypher_query)
    driver.close()
   print("relationships deleted successfully.")

def delete_and_detach_nodes5():
   n=0
   while n<= 50938:
    driver = GraphDatabase.driver(uri, auth=(username, password))   
    cypher_query = """
    MATCH ()-[r:HAS_OMIM_REF]->() with r limit 25000 delete r 
    """
    n+=25000
    with driver.session(database="socialnetwork1") as session:
        session.run(cypher_query)
    driver.close()
   print("relationships deleted successfully.")
        
def delete_and_detach_nodes6():
   n=0
   while n<= 995881:
    driver = GraphDatabase.driver(uri, auth=(username, password))   
    cypher_query = """
    MATCH ()-[r:MESH_QUALIFIER_FOR]->() with r limit 25000 delete r 
    """
    n+=25000
    with driver.session(database="socialnetwork1") as session:
        session.run(cypher_query)
    driver.close()
   print("relationships deleted successfully.")

def delete_and_detach_nodes7():
   n=0
   while n<= 40161141:
    driver = GraphDatabase.driver(uri, auth=(username, password))   
    cypher_query = """
    MATCH ()-[r:MESH_TERM_FOR]->() with r limit 25000 delete r 
    """
    n+=25000
    with driver.session(database="socialnetwork1") as session:
        session.run(cypher_query)
    driver.close()
   print("relationships deleted successfully.")

   
def delete_and_detach_nodes8():
   n=0
   while n<= 5961279:
    driver = GraphDatabase.driver(uri, auth=(username, password))   
    cypher_query = """
    MATCH ()-[r:SUBSTANCE_ANNOTATED_BY_PUBMED]->() with r limit 25000 delete r 
    """
    n+=25000
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






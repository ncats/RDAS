from multiprocessing import Pool

print('0')
#pip install locationtagger
#pip install spacy
#python -m spacy download en_core_web_sm

#pip install lxml[html_clean]

import nltk
import spacy
# essential entity models downloads
nltk.downloader.download('maxent_ne_chunker')
nltk.downloader.download('words')
nltk.downloader.download('treebank')
nltk.downloader.download('maxent_treebank_pos_tagger')
print('1')
nltk.downloader.download('punkt')
print('2')
nltk.download('averaged_perceptron_tagger')
print('3')
import locationtagger
import concurrent.futures



import pandas as pd
from neo4j import GraphDatabase
import nltk



fips_finding=pd.read_csv("/home/valinejadj2/uscities.csv")
fips_finding_dict=dict()
for i in fips_finding.index:
     fips_finding_dict[(fips_finding['city'][i].lower().strip(),fips_finding['state_name'][i].lower().strip() )]= ['{:0>5}'.format(fips_finding['county_fips'][i]),fips_finding['county_name'][i]]
     fips_finding_dict[fips_finding['city'][i].lower().strip()]= ['{:0>5}'.format(fips_finding['county_fips'][i]),fips_finding['county_name'][i]]

def find_fip(x,y):
 if y != None:
  try:
    return (fips_finding_dict[ (x.lower().strip(),y.lower().strip())][0]), fips_finding_dict[ (x.lower(),y.lower())][1]
  except:
    return  '',''
 else:
    try:
      return (fips_finding_dict[x.lower().strip()][0]),fips_finding_dict[ x.lower()][1]
    except:
      return  '',''





def location_information(j):
 if j:
    place_entity = locationtagger.find_locations(text=j)
    if place_entity.countries and place_entity.countries[0] != 'United States':
       if place_entity.countries:country = place_entity.countries[0]
       else:country = None  # or some default value
       if place_entity.regions:region = place_entity.regions[0]
       else: region = None  # or some default value
       if place_entity.cities: city = place_entity.cities[0]
       else:city = None  # or some default value
       return country, region, city
    else:
     if place_entity.countries or (place_entity.regions and place_entity.regions[0].lower() in states_lower_set):
        # If country is specified as US or region matches a state, proceed
        city_lower=None
        for i in reversed(place_entity.cities):
            city_lower = i.lower()
            if city_lower in fips_finding_dict and city_lower not in ['hospital', 'university']:
                   break
        if place_entity.countries:country = place_entity.countries[0]
        else:country = None  # or some default value
        if place_entity.regions:region = place_entity.regions[0]
        else: region = None  # or some default value
        if city_lower: city = city_lower
        else:city = None  # or some default value
        return country, region, city
    return None, None, None
 return None, None, None

nltk.download('all')

states = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "Florida",
    "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine",
    "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska",
    "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio",
    "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas",
    "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"
]
states_lower = [state.lower() for state in states]
states_lower_set = set(state.lower() for state in states)

uri= "uri"
username="username"
password="password"

#def running_neo4j_chunk(chunk_start, chunk_end, uri, username, password):
print('first step done')

def create_Pubmed_node(chunk_start, chunk_end):
    with GraphDatabase.driver(uri, auth=(username, password)) as driver:
        # Open a session to fetch the data from the source database
        with driver.session(database="socialnetwork1") as session:
          # Query to fetch required data from the source database
          offset = chunk_start    # 1637456 #107456  #1107142  # 112282     #113020   125191
          batch_size_q = 200000
          pubmed_institution={}
          n=0 
          while offset < chunk_end:   #True    
            query = (

                      "MATCH (a:Author)  "
                      f"SKIP {offset} LIMIT {batch_size_q} "
                      "RETURN a.fullName, a.affiliation "
                       )
            
            results = session.run(query)  # Stream results directly from the database
            # Open a session to the destination database (where you will insert data)

            if not results.peek():
                break
            
            iteration_=0
            with driver.session(database="socialnetwork1") as session_dest:
                batch_size = 2000  # Adjust batch size based on your environment and testing
                current_batch = []  # Collect queries for the current batch

                # Process each result and accumulate it in a batch
                for record in results:
                 # try:  
                   # print(n)#,record.data()['ar.title'])
                    # print(n,'-----------------',record)
                   # n+=1
                    
                    info = record.data() 
                    event_properties = {
                        'fullName': str(info.get('a.fullName','')),
                        'ar_affiliation': str(info.get('a.affiliation',''))
                    }
                    
                    
               
                    # Get data from 'organ_address' DataFrame
                    B=event_properties['ar_affiliation']
                    if B in pubmed_institution: place_entity=pubmed_institution[B]
                    else:
                          place_entity =location_information(B)
                          pubmed_institution[B]= place_entity
                    event_properties.update({
                                'Aff_Zip': '',
                                'Aff_country':  place_entity[0] if place_entity[0] != None else '',
                                'Aff_state': place_entity[1] if place_entity[1] != None else '',
                                'Aff_county': find_fip(place_entity[2], place_entity[1] )[1],
                                'Aff_city': place_entity[2] if place_entity[2] != None else '',
                                'Aff_FIPS': find_fip(place_entity[2], place_entity[1] )[0]})

                    
                    # Prepare Cypher query for insertion
                    
                    query = (
                        "Match (a:Author) "
                        "where a.fullName =$fullName  and  a.affiliation=$ar_affiliation  "
                        "MERGE (Loc:Location {Aff_country: $Aff_country, Aff_state: $Aff_state, Aff_county: $Aff_county}) "
                        "ON CREATE SET Loc.Aff_city = $Aff_city, Loc.Aff_FIPS = $Aff_FIPS "
             "MERGE (a)-[:Researcher_location]->(Loc)"
                    )
                    

                    
                    #dd this query to the current batch
                    session_dest.run(query, event_properties)
                    
                    
                    # If batch is full, execute the current batch and reset it
                    if len(current_batch) >= batch_size:
                        iteration_+=1
                        print('offset: ',offset,',   batch:',iteration_)
                        # Execute all queries in the current batch
                        for batch_query, properties in current_batch:
                            session_dest.run(batch_query, **properties)
                        # Clear the current batch after execution
                        current_batch = []
                
                 # except:
                 #   pass
                # If there are any remaining queries in the batch after the loop
                if current_batch:
                    print('offset: ',offset,',   batch:',iteration_) 
                    for batch_query, properties in current_batch:
                        session_dest.run(batch_query, **properties)
                    #print(pubmed_institution)
            offset += batch_size_q  # Move to next batch

def run_parallel_queries(start, total_articles, num_processes=100):
    chunk_size = (total_articles-start) // num_processes
    chunks = [(i * chunk_size+start, (i + 1) * chunk_size+start) for i in range(num_processes)]

    # Use multiprocessing Pool to run queries in parallel
    with Pool(num_processes) as pool:
        pool.starmap(create_Pubmed_node, [(chunk_start, chunk_end) for chunk_start, chunk_end in chunks])

if __name__ == "__main__":
    start=0  #8187213      #3500000  # 3000000
    total_articles=11884028   #12374426    #7687213  # 40497704  # Example total number of articles
    run_parallel_queries(start,total_articles, num_processes=100)

print('Next step')
'''
with GraphDatabase.driver(uri, auth=(username, password)) as driver:
    with driver.session(database="socialnetwork") as session_dest1:
        query = """
            MATCH (r1:Researcher)--(g:Article)--(r2:Researcher)
            WHERE r1.Name <> r2.Name  // Ensure no self-loops
            WITH r1, r2
            ORDER BY r1.Name, r2.Name  // Ensure correct order
            WITH 
                CASE WHEN r1.Name < r2.Name THEN r1 ELSE r2 END AS leftRes,
                CASE WHEN r1.Name < r2.Name THEN r2 ELSE r1 END AS rightRes
            MERGE (leftRes)-[r:Collaborated_With]->(rightRes)
            ON CREATE SET r.num_colab = 1
            ON MATCH SET r.num_colab = r.num_colab + 1
            """
        session_dest1.run(query)
'''

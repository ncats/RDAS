#!pip3 install spacy
#!python3 -m spacy download en_core_web_sm
from multiprocessing import Pool

from neo4j import GraphDatabase
import pandas as pd
import spacy
nlp = spacy.load("en_core_web_sm")


def classify_entity(text):
    if not isinstance(text, str):  # Check if the input is a string
        return False
    doc = nlp(text)
    for ent in doc.ents:
        if ent.label_ == "PERSON":
            return True
    return False


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

uri= "uri"
username="username"
password="password"

def create_CT_node(chunk_start, chunk_end):
    with GraphDatabase.driver(uri, auth=(username, password)) as driver:
        # Open a session to fetch the data from the source database
        with driver.session(database="rdas.ctkg") as session:
            # Query to fetch required data from the source database
            chunk_end=chunk_end-chunk_start
            query = (
                        "MATCH (c:ClinicalTrial)--(a:Condition)--(co:ConditionAnnotation)--(g:GARD) "
                        "WITH distinct c,g "
                        "OPTIONAL MATCH (f:Location)--(c) "
                        "WITH c,g, head(collect(f)) AS l "
                        "OPTIONAL MATCH (q: Intervention)<-[: has_intervention]-(c) "
                        "WITH c,g, l, COLLECT(DISTINCT q.InterventionName) AS Interventions "
                        "optional MATCH (i:Investigator)-[:investigates]->(c) "
                        "RETURN  i.OfficialName,i.OfficialAffiliation,  c.BriefTitle, c.OfficialTitle, c.BriefSummary, c.NCTId ,c.StartDate, l.LocationCity, l.LocationState, l.LocationCountry, l.LocationFacility, Interventions, g.GardId, g.GardName "
                        f"SKIP {chunk_start} LIMIT {chunk_end}"
                        )

            results = session.run(query)  # Stream results directly from the database
            iteration_=0
            # Open a session to the destination database (where you will insert data)
            with driver.session(database="socialnetwork1") as session_dest:
                batch_size = 600  # Adjust batch size based on your environment and testing
                current_batch = []  # Collect queries for the current batch
                
                  
                # Process each result and accumulate it in a batch
                for record in results:
                    info = record.data()
                    event_properties = {
                'OfficialName': str(info['i.OfficialName']),
                'OfficialAffiliation': str(info['i.OfficialAffiliation']),
                'BriefTitle': str(info['c.BriefTitle']),
                'OfficialTitle':str(info['c.OfficialTitle']),
                'BriefSummary': str(info['c.BriefSummary']),
                'NCTId': info['c.NCTId'],
                'LocationCity': info['l.LocationCity'],
                 'LocationState': info['l.LocationState'],
                'LocationCountry': info['l.LocationCountry'],
                'LocationFacility': info['l.LocationFacility'],
                'Interventions': str(info['Interventions']),
                'GARDname': info['g.GardName'],
                'GARDID': info['g.GardId'],
                'year': str(info['c.StartDate'])[-4:]                            
                    }


                
                    
                    if  classify_entity(event_properties['OfficialName'])==True:
                           city_  = str(event_properties['LocationCity']).lower().strip()
                           State_ = str(event_properties['LocationState']).lower().strip()
                           event_properties.update({
                                'Aff_Zip': '',
                                'Aff_country': str(event_properties['LocationCountry']),
                                'Aff_state': State_,
                                'Aff_county': find_fip(city_, State_ )[1],
                                'Aff_city': city_,
                                 'Aff_FIPS': find_fip(city_, State_ )[0]
                            })
                    else:
                            event_properties.update({
                                'Aff_Zip': '',
                                'Aff_country': '',
                                'Aff_state': '',
                                'Aff_county': '',
                                'Aff_city': '',
                                'Aff_FIPS': ''
                            })
     
                    # Prepare Cypher query for insertion
                    query = (
                        "MERGE (ga:GARD {GardId: $GARDID, GardName: $GARDname}) "
                        "MERGE (gr:ClinicalTrial {NCTId: $NCTId}) "
                        "ON CREATE SET gr.OfficialTitle = $OfficialTitle, gr.BriefTitle = $BriefTitle, gr.BriefSummary = $BriefSummary, gr.Interventions = $Interventions, gr.year = $year "
                        
                        
                        "MERGE (ga)-[:RELATED_GARD]->(gr) "
                        "MERGE (Re:Author1 {fullName: $OfficialName, affiliation: $OfficialAffiliation}) "
                        "ON CREATE SET  Re.Aff_Zip = $Aff_Zip "
                        "MERGE (gr)-[:Involves_Researcher_ct]->(Re) "
                        "MERGE (Loc:Location {Aff_country: $Aff_country, Aff_state: $Aff_state, Aff_county: $Aff_county}) "
                        "ON CREATE SET Loc.Aff_city = $Aff_city, Loc.Aff_FIPS = $Aff_FIPS "
                        "MERGE (Re)-[:Researcher_location]->(Loc)"
                    )

                    # Add this query to the current batch
                    current_batch.append((query, event_properties))

                    # If batch is full, execute the current batch and reset it
                    if len(current_batch) >= batch_size:
                        iteration_+=1
                        print('batch:',iteration_)
                        # Execute all queries in the current batch
                        for batch_query, properties in current_batch:
                            session_dest.run(batch_query, **properties)

                        # Clear the current batch after execution
                        current_batch = []

                # If there are any remaining queries in the batch after the loop
                if current_batch:
                    print('batch:',iteration_)
                    for batch_query, properties in current_batch:
                        session_dest.run(batch_query, **properties)

def run_parallel_queries(start, total_articles, num_processes=128):
    chunk_size = (total_articles-start) // num_processes
    chunks = [(i * chunk_size+start, (i + 1) * chunk_size+start) for i in range(num_processes)]

    # Use multiprocessing Pool to run queries in parallel
    with Pool(num_processes) as pool:
        pool.starmap(create_CT_node, [(chunk_start, chunk_end) for chunk_start, chunk_end in chunks])

if __name__ == "__main__":
    start=0
    total_articles=125251   
    run_parallel_queries(start,total_articles, num_processes=128)

print('Next step')

with GraphDatabase.driver(uri, auth=(username, password)) as driver:
    with driver.session(database="socialnetwork1") as session_dest1:
        query = """
            MATCH (r1:Author1)--(g:ClinicalTrial)--(r2:Author1)
            WHERE r1.fullName <> r2.fullName  // Ensure no self-loops
            WITH r1, r2
            ORDER BY r1.fullName, r2.fullName  // Ensure correct order
            WITH 
                CASE WHEN r1.fullName < r2.fullName THEN r1 ELSE r2 END AS leftRes,
                CASE WHEN r1.fullName < r2.fullName THEN r2 ELSE r1 END AS rightRes
            MERGE (leftRes)-[r:Collaborated_With]->(rightRes)
            ON CREATE SET r.num_colab = 1
            ON MATCH SET r.num_colab = r.num_colab + 1
            """
        session_dest1.run(query)

from multiprocessing import Pool

from neo4j import GraphDatabase
import pandas as pd

organ_address=pd.read_csv('/home/valinejadj2/Grant_org1.csv')
organ_address['n.org_name']=organ_address.apply(lambda x: x['n.org_name'].replace('"','').lower().strip(),axis=1)
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


def create_Grant_node(chunk_start, chunk_end):
    with GraphDatabase.driver(uri, auth=(username, password)) as driver:
        # Open a session to fetch the data from the source database
        with driver.session(database="rdas.gfkg") as session:
            # Query to fetch required data from the source database
            chunk_end=chunk_end-chunk_start
            query = (
                "MATCH (pi:PrincipalInvestigator)-[:INVESTIGATED]-(p:Project)--(g:GARD) "
                "RETURN pi.pi_name, pi.org_name, p.title, p.funding_year, p.application_id, "
                "p.abstract, p.terms, g.GardId, g.GardName "
                f"SKIP {chunk_start} LIMIT {chunk_end}"
                )
            results = session.run(query)  # Stream results directly from the database
            iteration_=0
            # Open a session to the destination database (where you will insert data)
            with driver.session(database="socialnetwork1") as session_dest:
                batch_size = 700  # Adjust batch size based on your environment and testing
                current_batch = []  # Collect queries for the current batch
                
                  
                # Process each result and accumulate it in a batch
                for record in results:
                    info = record.data()
                    event_properties = {
                        'pi_name': str(info['pi.pi_name']),
                        'org_name': str(info['pi.org_name']),
                        'title': str(info['p.title']),
                        'application_id': str(info['p.application_id']),
                        'abstract': str(info['p.abstract']),
                        'terms': str(info['p.terms']),
                        'GARDname': info['g.GardName'],
                        'GARDID': info['g.GardId'],
                        'year': info['p.funding_year']
                    }

                    # Get data from 'organ_address' DataFrame
                    data = organ_address[organ_address['n.org_name'] == event_properties['org_name'].lower().strip()]
                    if len(data) > 0:
                        zip_code, A, B, C = data.iloc[0]['zip_code'], data.iloc[0]['Country'], data.iloc[0]['state'], data.iloc[0]['County']
                        if isinstance(A, str) and isinstance(B, str) and isinstance(C, str):
                            event_properties.update({
                                'Aff_Zip': zip_code,
                                'Aff_country': A,
                                'Aff_state': B,
                                'Aff_county': C,
                                'Aff_city': data.iloc[0]['city'],
                                'Aff_FIPS': find_fip(data.iloc[0]['city'], data.iloc[0]['state'])[0]
                            })
                        else:
                            event_properties.update({
                                'Aff_Zip': '',
                                'Aff_country': 'USA',
                                'Aff_state': '',
                                'Aff_county': '',
                                'Aff_city': '',
                                'Aff_FIPS': ''
                            })

                    # Prepare Cypher query for insertion
                    query = (
                        "MERGE (ga:GARD {GardId: $GARDID, GardName: $GARDname}) "
                        "MERGE (gr:Grant {application_id: $application_id}) "
                        "ON CREATE SET gr.title = $title, gr.abstract = $abstract, gr.terms = $terms, gr.year = $year "
                        "MERGE (ga)-[:RELATED_GARD]->(gr) "
                        "MERGE (Re:Author1 {fullName: $pi_name, affiliation: $org_name}) "
                        "ON CREATE SET  Re.Aff_Zip = $Aff_Zip "
                        "MERGE (gr)-[:Involves_Researcher_g]->(Re) "
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
        pool.starmap(create_Grant_node, [(chunk_start, chunk_end) for chunk_start, chunk_end in chunks])

if __name__ == "__main__":
    start=0
    total_articles=488717   
    run_parallel_queries(start,total_articles, num_processes=128)

#create_Grant_node(0,500000 )

print('Next step')

 
with GraphDatabase.driver(uri, auth=(username, password)) as driver:
    with driver.session(database="socialnetwork1") as session_dest1:
        query = """
            MATCH (r1:Author1)--(g:Grant)--(r2:Author1)
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
print('done')


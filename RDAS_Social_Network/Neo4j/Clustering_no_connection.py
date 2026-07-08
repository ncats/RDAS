from multiprocessing import Pool
import nltk
import spacy
import locationtagger
import concurrent.futures
import pandas as pd
from neo4j import GraphDatabase
import nltk
import pandas as pd

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import MiniBatchKMeans  # Use MiniBatchKMeans for faster clustering
import numpy as np
import pandas as pd
from sklearn.decomposition import TruncatedSVD  # Dimensionality reduction to speed up vectorization
import warnings
import warnings
import warnings



uri= "uri"
username="username"
password="password"

from sklearn.cluster import DBSCAN
from sklearn.metrics.pairwise import cosine_distances

def name_clusters(df, text_column, cluster_column, eps=0.9, min_samples=5, n_components=100):
    # Extract abstracts from the specified column and handle NaN values
    abstracts = df[text_column].fillna("").astype(str).tolist()

    # TF-IDF vectorization with ngram_range (optimize based on your dataset)
    vectorizer = TfidfVectorizer(stop_words='english', ngram_range=(1, 2))  # Bi-grams may help for better context
    tfidf_matrix = vectorizer.fit_transform(abstracts)
    n_components = min(n_components, tfidf_matrix.shape[1])
    # Optionally reduce dimensionality to speed up clustering
    if n_components:
         svd = TruncatedSVD(n_components=n_components, random_state=42)
         tfidf_matrix = svd.fit_transform(tfidf_matrix)

    # DBSCAN clustering
    dbscan = DBSCAN(eps=eps, min_samples=min_samples, metric='cosine')  # Using cosine distance as the metric
    clusters = dbscan.fit_predict(tfidf_matrix)

    # Assign cluster labels to the DataFrame
    df[cluster_column] = clusters

    return df

def three_most_frequent_terms(term_string):
    # Convert the string representation of the list to an actual list
    #term_list = ast.literal_eval(term_string)
    # Count the frequency of each term in the list
    term_counts = Counter(term_string)
    # Get the three most common terms
    most_common_terms = term_counts.most_common(5)

    return ' , '.join([term  for term, count in most_common_terms])

def sum_strings(series):
    # Convert all items to string, handling NaNs, and remove duplicates by converting to a set
    return ', '.join(sorted(set(str(item) for item in series if pd.notna(item))))

# Function to clean and flatten terms
def sum_terms(series):
    result = []
    for item in series:
        if isinstance(item, list):  # Check if item is a list
            for subitem in item:
                if isinstance(subitem, str):  # Ensure it's a string
                    subitem_cleaned = subitem.strip('[]')  # Remove surrounding brackets
                    result.extend(subitem_cleaned.split(','))  # Split and add terms
        elif pd.notna(item):  # Handle non-list, non-NaN values
            item_cleaned = str(item).strip('[]')  # Convert to string and remove brackets
            result.extend(item_cleaned.split(','))  # Split and add terms
    # Remove extra whitespace from terms
    return [term.strip() for term in result if term.strip()]




def create_Pubmed_node(chunk_start, chunk_end):
    #warnings.filterwarnings("ignore")
    with GraphDatabase.driver(uri, auth=(username, password)) as driver:
        # Open a session to fetch the data from the source database
        with driver.session(database="socialnetwork1") as session:
          # Query to fetch required data from the source database
          offset = chunk_start    # 1637456 #107456  #1107142  # 112282     #113020   125191
          batch_size_q = 1
          pubmed_institution={}
          n=0 
          while offset < chunk_end:   #True    
            query = (
                     "MATCH (g:GARD) "
                     f"SKIP {offset} LIMIT {batch_size_q} "
                     "optional MATCH (g)--(i:ClinicalTrial) "
                     "RETURN i.OfficialTitle + ' ' + i.BriefSummary AS Abstract, i.NCTId AS SNid "
                     "UNION ALL "
                     "MATCH (g:GARD) "
                     f"SKIP {offset} LIMIT {batch_size_q} "
                     "optional MATCH (g)--(i:Grant) "
                     "RETURN i.title + ' ' + i.abstract AS Abstract, i.application_id AS SNid "
                     "UNION ALL "
                     "MATCH (g:GARD) "
                     f"SKIP {offset} LIMIT {batch_size_q} "
                     "optional MATCH (g)--(i:Article) "
                     "RETURN i.title + ' ' + i.abstractText AS Abstract, 'ar:' + i.pubmed_id AS SNid "   
                     )
            
            results = session.run(query)  # Stream results directly from the database
            # Open a session to the destination database (where you will insert data)
            data = [record.data() for record in results]  # Convert result to a list of dictionaries
            Data_table = pd.DataFrame(data)
            Data_table = Data_table.fillna('')
           # if (Data_table.shape[0] <1) :
           #     n+=1
           #     print(n)
           # offset+=1
           # if (offset%300)==0: print( 'fisnish batch')
           # print('offset:', offset)



            if 'Abstract' not in Data_table.columns: Data_table['Abstract'] = None 
            if 'SNid' not in Data_table.columns: Data_table['SNid'] = None 
           # print(Data_table)
            if Data_table.shape[0] == 0:
                      #  print(f"Skipping chunk {chunk_start} to {chunk_end} because Data_table is empty.")
                        continue            
            try:
             clustering_ = name_clusters(Data_table, 'Abstract', 'Community', eps=0.9, min_samples=10, n_components=100)
            # print('yes')
             result = clustering_.groupby('Community').agg(
                                        Abstract=('Abstract', sum_strings),
                                       Size=('Community', 'size')  # Count of occurrences
                                      ).reset_index()

            # print(result)
             data=result
             #data['frequent_words']  = data.apply(lambda x:  three_most_frequent_terms(x['Terms'])   , axis=1)
             iteration_=0
            
             with driver.session(database="socialnetwork1") as session_dest:
                batch_size = 100  # Adjust batch size based on your environment and testing
                current_batch = []  # Collect queries for the current batch

                for i in data.index:
                    #'Key_trems: $Key_trems'
                    Cluster_ID= data['Community'][i]
                    list_projects = list(clustering_['SNid'][clustering_['Community'] == Cluster_ID])
                    event_properties = {
                    'Cluster_ID': data['Community'][i],
                    'Cluster_Size': str(data['Size'][i]),
                    'Evidence': str(data['Abstract'][i]),
                    'Projects_id': list_projects
                    }
                    #'Key_trems': str(data['frequent_words'][i])
                    #}

                    query = ( 
                        "MATCH (g:GARD) "
                        f"SKIP {offset} LIMIT {batch_size_q} "                
                        "CREATE (Cl:Cluster1 {"
                        'Cluster_ID: $Cluster_ID,'
                        'Cluster_Size: $Cluster_Size,'
                        'Evidence: $Evidence,'
                        'Projects_id:$Projects_id '
                         "}) "
                        "CREATE (Cl)-[:Cluster_to_GARD1]->(g)" 
                        )    
                    current_batch.append((query, event_properties))



                    # If batch is full, execute the current batch and reset it
                    if len(current_batch) >= batch_size:
                        iteration_+=1
                        print('offset: ',offset,',   batch:',iteration_)
                        # Execute all queries in the current batch
                        for batch_query, properties in current_batch:
                            session_dest.run(batch_query, **properties)
                        # Clear the current batch after execution
                        current_batch = []

                if current_batch:
                    print('offset: ',offset,',   batch:',iteration_) 
                    for batch_query, properties in current_batch:
                        session_dest.run(batch_query, **properties)
                    #print(pubmed_institution)
             offset += batch_size_q  # Move to next batc
            except:
                pass
    warnings.resetwarnings()
    

#create_Pubmed_node(0, 2)

def run_parallel_queries(start, total_articles, num_processes=100):
    chunk_size = (total_articles-start) // num_processes
    chunks = [(i * chunk_size+start, (i + 1) * chunk_size+start) for i in range(num_processes)]

    # Use multiprocessing Pool to run queries in parallel
    with Pool(num_processes) as pool:
        pool.starmap(create_Pubmed_node, [(chunk_start, chunk_end) for chunk_start, chunk_end in chunks])

if __name__ == "__main__":
    start=0  #8187213     
    total_articles=12004   
    run_parallel_queries(start,total_articles, num_processes=100)



from multiprocessing import Pool
import pandas as pd
from neo4j import GraphDatabase
import os
import pickle
import time
import openai
import pandas as pd
import numpy as np
from openai import OpenAI
import concurrent.futures
from tqdm import tqdm
import random
import re
import json
import ast
from langchain.text_splitter import SpacyTextSplitter
import spacy
from transformers import AutoTokenizer


client = OpenAI(
  base_url ="https://Meta-Llama-3-70B-Instruct-vycmo-serverless.eastus2.inference.ai.azure.com",
  api_key= "api_key",
)
uri= "uri"
username="username"
password="password"


def extract_dict_from_text(text):
    #text=text.replace("\n", ',')
    # Find the start and end of the dictionary in the text
    start_index = text.find("{")
    end_index = text.rfind("}") + 1
    
    # Extract the dictionary part from the text
    dict_text = text[start_index:end_index]
    result_dict = ast.literal_eval(dict_text)
    return result_dict


# Load the language model
nlp = spacy.load("en_core_web_sm")
# Adjust the max_length parameter if necessary
nlp.max_length = 5000000  # Adjust based on your system's available memory
# Initialize SpacyTextSplitter with chunk size and overlap
splitter = SpacyTextSplitter(chunk_size=5000, chunk_overlap=200)
# Initialize the tokenizer
tokenizer1 = AutoTokenizer.from_pretrained("gpt2")  # Replace with your tokenizer if using a different model

def trim_tokens1(content,n):
    tokenized_content = tokenizer1.tokenize(content)    
    if len(tokenized_content) > n:
        tokenized_content = tokenized_content[:n]    
    truncated_content = tokenizer1.convert_tokens_to_string(tokenized_content)
    return truncated_content

import spacy
nlp = spacy.load("en_core_web_sm")
def trim_tokens(input_text, n_tokens=7000):
    input_text=input_text[:100000]
    doc = nlp(input_text)
    tokens = [token.text for token in doc]
    first_n_tokens = tokens[:n_tokens]
    return " ".join(first_n_tokens)

def create_Grant_node(chunk_start, chunk_end):
    with GraphDatabase.driver(uri, auth=(username, password)) as driver:
        # Open a session to fetch the data from the source database
        with driver.session(database="socialnetwork1") as session:
            # Query to fetch required data from the source database
            chunk_end=chunk_end-chunk_start
            query = (
                 "Match (c:Cluster1) "
                 f"SKIP {chunk_start} LIMIT {chunk_end} "
                 "Match (c)--(g:GARD)"
                "RETURN c.Evidence, c.Cluster_ID, g.GardId, g.GardName "
                )
            results = session.run(query)  # Stream results directly from the database
            iteration_=0
            # Open a session to the destination database (where you will insert data)
            with driver.session(database="socialnetwork1") as session_dest:
                batch_size = 5  # Adjust batch size based on your environment and testing
                current_batch = []  # Collect queries for the current batch
                # Process each result and accumulate it in a batch
                for record in results:
                    info = record.data()
                    event_properties = {
                        'Abstract': trim_tokens(str(info['c.Evidence']),500),
                        'Cluster_ID': info['c.Cluster_ID'],
                        'GardName': str(info['g.GardName']),
                        'GARDID': info['g.GardId']
                    }
                    prompt = event_properties['Abstract']
                    RD=event_properties['GardName']
                    Content_='Given the work of authors (or principal investigators) in the context of {RD} which is a rare disease, identify 3 key detailed research expertise areas they have contributed to. Follow these steps: 1-Research Contribution Summary: Based on their publications, clinical trials, or grant proposals, summarize the major contributions made by the researchers in the field. 2- Identify Expertise Areas: Based on their contributions, identify 3 key detailed research expertise areas that emerge from their work. Format your response as a dictionary as follwos:  {"summary of expertise 1": "explanation" , "summary of expertise 2" : "explanation" , "summary of expertise 3" : "explanation"}'
                    message_text = [{"role":"system","content":Content_},
                    {"role":"user","content":prompt}]
                   # print('yes')
                    completion = client.chat.completions.create(
                           messages=message_text,
                           model="azureai",
                           max_tokens = 7000,
                           temperature=0.4,
                           frequency_penalty=0,
                           presence_penalty=0,
                           stop=None,
                           seed=42
                              )
             
                    res =   completion.choices[0].message.content
                   # print(res)
                   # print('next')
                    dic= extract_dict_from_text(res)                
                   # print(dic)
                    if iteration_%20==0:  print(iteration_)
                    if dic:  
                      for sentence in dic:
                        # print(sentence,event_properties['GARDID'],event_properties['Cluster_ID'] )  
                        event_properties['Summarized_expertise']= sentence
                        event_properties[ 'expertise_explnation']= dic[sentence]
                                            

                        query = ("match (c:Cluster1)--(g:GARD) "
                                 "where c.Cluster_ID =$Cluster_ID  and g.GardId=$GARDID  "
                                 "with c,g "
                                 "merge (e:Expertise {Summarized_expertise: $Summarized_expertise, expertise_explnation:$expertise_explnation}) "
                                  "WITH c,e "
                                  "merge (c)-[:HAS_EXPERTISE ]->(e) "
                                  )
                        
                        
                        # print(event_properties)    
                        # print('cuurent_batch',len(current_batch))
                        session_dest.run(query, event_properties)
                      iteration_+=1
                       # current_batch.append((query, event_properties))
                       # print(current_batch)
                      '''
                   if len(current_batch) >= batch_size:
                            iteration_+=1
                            print('batch:',iteration_)
                            # Execute all queries in the current batch
                            for batch_query, properties in current_batch:
                                  # print(properties)
                                   session_dest.run(batch_query, **properties)
                           # print('yes')
                            current_batch = []

                if current_batch:
                            print('help batch:',iteration_)
                            for batch_query, properties in current_batch:
                               session_dest.run(batch_query, **properties)
                      '''

def run_parallel_queries(start, total_articles, num_processes=50):
    chunk_size = (total_articles-start) // num_processes
    chunks = [(i * chunk_size+start, (i + 1) * chunk_size+start) for i in range(num_processes)]

    # Use multiprocessing Pool to run queries in parallel
    with Pool(num_processes) as pool:
        pool.starmap(create_Grant_node, [(chunk_start, chunk_end) for chunk_start, chunk_end in chunks])

if __name__ == "__main__":
    start=0
    total_articles=1468
    run_parallel_queries(start,total_articles, num_processes=50)


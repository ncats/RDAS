# Introduction
One of the target of the alert project is to load all PubMed articles related to rare diseases to a knowledge graph database - Neo4j. Here we use `Python` and third party `API` to parse and load all the information on-the-fly to Neo4j database.


# Architecture
## Data Source
The following data sources are used to build this Neo4j database:
- GARD data lake.
- NCBI.
  - NCBI eutils
  - NCBI Pubtator
- EBI
- OMIM

Rare diseases related information such as GARD_ID and Name are returned by cypher query against GARD data lake Neo4j database. Disease name is used to query NCBI PubMed archieve to get a list of PubMed article IDs. Because of the large number of articles for same diseases, we limit the number of articles for each disease to 1000.

EBI, instead of NCBI PubMed archieve and `APIs` are used to get the article informaiton, such as title, keywords, abstract, etc. because of the following reseans:
1. NCBI has limitation on how many `API` calls can make in one second (around 3 per second).
2. EBI does not have such limit and it also provids an `API` that can query/return as much as 1000 PubMed articles.

Pubtator `API` is used to get further annotations such as MESH term and Gene for an article. There is also a limit (3 calls per second) for how often the `API` can be called. The loading program force itself to sleep for a while (0.34 second) between the calls.

OMIM `APIs` are used to get all the OMIM reference PubMed articles for rare diseases. These articles are then checked against what have loaded into the Neo4j, and the missing ones are loaded using the same procedure.

## Data Flow
The following chart gives the how and what kind of data is collected and load into Neo4j:
![Architecture](./img/pubmed-neo4j-architecture.png)

# Neo4j Data Model
The data model or schema show all the nodes and their relationships.
![Data Model](./img/pubmed-neo4j-data-model.png)

# Source Codes
## Python packages required for runnin the PubMed article loading program
The following packages and their versions from `alert-requirements.txt` file are the required python packages for running the loading program to laod PubMed articles related information to Neo4j:
```
certifi==2021.10.8
charset-normalizer==2.0.7
idna==3.3
jmespath==0.10.0
neo4j==1.7.6
neobolt==1.7.17
neotime==1.7.4
pytz==2021.3
requests==2.26.0
six==1.16.0
urllib3==1.26.7
```

# Publication
[Q. Zhu et al., "Scientific Evidence Based Knowledge Graph in Rare Diseases," 2021 IEEE International Conference on Bioinformatics and Biomedicine (BIBM), 2021, pp. 2614-2617, doi: 10.1109/BIBM52615.2021.9669645.](https://ieeexplore.ieee.org/document/9669645)

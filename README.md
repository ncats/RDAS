# Introduction
One of the targets of the alert project is to load all PubMed articles related to rare diseases to a knowledge graph database - Neo4j. Here we use `Python` and third-party `API` to parse and load all the information on-the-fly to Neo4j database.


# Architecture
## Data Source
The following data sources are used to build this Neo4j database:
- GARD data lake.
- NCBI.
  - NCBI eutils
  - NCBI Pubtator
- EBI
- OMIM

Rare diseases related information such as GARD_ID and Name are returned by cypher query against GARD data lake Neo4j database. Disease name is used to query NCBI PubMed archive to get a list of PubMed article IDs. Because of the large number of articles for same diseases, we limit the number of articles for each disease to 1000.

EBI, instead of NCBI PubMed archive and `APIs` are used to get the article information, such as title, keywords, abstract, etc. because of the following reasons:
1. NCBI has limitation on how many `API` calls can make in one second (around 3 per second).
2. EBI does not have such limit and it also provides an `API` that can query/return as much as 1000 PubMed articles.

Pubtator `API` is used to get further annotations such as MESH term and Gene for an article. There is also a limit (3 calls per second) for how often the `API` can be called. The loading program force itself to sleep for a while (0.34 second) between the calls.

OMIM `APIs` are used to get all the OMIM reference PubMed articles for rare diseases. These articles are then checked against what have loaded into the Neo4j, and the missing ones are loaded using the same procedure.

## Data Flow
The following chart gives the how and what kind of data is collected and load into Neo4j:
![Architecture](./img/pubmed-neo4j-architecture.png)

# Neo4j Data Model
The data model or schema show all the nodes and their relationships.
![Data Model](./img/pubmed-neo4j-data-model.png)

# Source Codes
## Python packages required for running the PubMed article loading program
We utilized conda package manager to install the following packages:
```
python=3.10.4
charset-normalizer
idna
jmespath
neo4j
pytz
requests
six
urllib3
```
See [here](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#creating-an-environment-from-an-environment-yml-file) for how to create an environment from the `alert-env.yml` file.

## `config.ini`
File used to store variables for use in scripts. Contains credentials for databases and API's as well as some special variables for script functionality

`neo4j_username`, `neo4j_password`, and `neo4j_uri` contains your credentials for writing to your own neo4j database

`omim_api_key` contains your OMIM API key for article retrieval

`database_last_run` contains the date (format: YYYY/MM/DD) in which the script was last completed

`from_disease` and `to_disease` control what's the range of disease to handle. For testing, these can be set to a small range. To load all rare diseases, set them to `0` and `None`

## `update_neo4j.py`
The is the main program to run for loading PubMed articles for all rare diseases. It initially loads the database for the past fifty years and can update the database with new information. The rare diseases list comes from Neo4j data lake at https://disease.ncats.io.

## `initial_loading.py`
The is the main program to run for loading PubMed and OMIM articles for all rare diseases. The rare diseases list comes from Neo4j data lake at https://disease.ncats.io

`mindate` and `maxdate` control the time period of PubMed articles. Dates are set as variables and run with a sliding 50 year window of articles, updated weekly.

## `load-substance.py`
The `initial_loading.py` code did not load "substance" from the API call. This program - `load-substance.py` loop through all the pubmedIDs in current Neo4j database and call the EBI API to get substance information from `chemicalList` of the returned JSON object and add new property to `Article` node.

## `load-pubtype.py`
Again, the `initial_loading.py` code did not load "pubtype" from the API call. This program - `load-pubtype.py` loop through all the pubmedIDs in current Neo4j database and call the EBI API to get pubType information from `pubTypeList` of the returned JSON object and add new property to `Article` node.

## `neo4j_access` folder
This is a python project structure folder that can be used to build a standard python package for other projects to import directly.

We may move some of the code in `api` folder to here to build a python package.

## `saved_model` folder
This folder holds the trained model of Neural Network for classify if an PubMed article is epidemiology related study or not. The `my_model_orphanet_final` is the actual model used in `alert_add_epi.py`

## `api` folder
This folder is for backend python code for connecting to multiple Neo4j databases and provide a general, easy, wrapped way to access rare disease related information. It will support the following two use cases, for now:
- Alert web application.
- Rare diseases public APIs for research community.

`neo4j_backend.py` is an example python class for access Neo4j with `cypher` query. `test_neo4j_backend.py` is just a test class on how to use `neo4j_backend.py`

# Historic Files
## `invoke-esearch` files
These three python files: `invoke-esearch.py`, `invoke-esearch2.py`, and `invoke-esearch3.py` are historic files, and not used in build the Neo4j database.

## `alert-requirements.txt`
The following packages and their versions from `alert-requirements.txt` file were the required python packages for running the loading program to load PubMed articles related information to Neo4j, but are now superseded but `alert-env.yaml`.

# Publication
[Q. Zhu et al., "Scientific Evidence Based Knowledge Graph in Rare Diseases," 2021 IEEE International Conference on Bioinformatics and Biomedicine (BIBM), 2021, pp. 2614-2617, doi: 10.1109/BIBM52615.2021.9669645.](https://ieeexplore.ieee.org/document/9669645)

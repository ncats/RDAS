![https://github.com/Jaber-Valinejad/RDAS/edit/master/RDAS_Social_Network/Figs/snb.png](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Social_Network/Figs/snb.png)

--------------------

# Rare Disease Research Collaborative Network

|                |                                                   |
| -------------- | ------------------------------------------------- |
| **Testing**    | ![Static Badge](https://img.shields.io/badge/Project%20Status-Passing-green) |
| **Docs**       | ![Static Badge](https://img.shields.io/badge/Docs-Passing-green) |
| **Package**    | ![Python](https://img.shields.io/badge/Python-3.8%2B-blue.svg) ![GitHub last commit](https://img.shields.io/github/last-commit/Jaber-Valinejad/RDAS) [![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/Jaber-Valinejad/RDAS/blob/master/RDAS_FAERS/Methods/Neo4j_v2.ipynb) ![Static Badge](https://img.shields.io/badge/GraphDB-Neo4j-blue) ![Static Badge](https://img.shields.io/badge/Query%20Language-Cypher-yellow) |
| **Meta**       | [![DOI](https://zenodo.org/badge/DOI/10.1109/BIBM62325.2024.10822513.svg)](https://doi.org/10.1109/BIBM62325.2024.10822513) [![Docs](https://img.shields.io/badge/Docs-ReadTheDocs-blue)](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_FAERS/Docs/BIBM24_paper.pdf) ![GitHub License](https://img.shields.io/github/license/Jaber-Valinejad/RDAS) ![GitHub Sponsors](https://img.shields.io/github/sponsors/Jaber-Valinejad) |



## What is it?

We developed a **Rare Disease Research Collaborative Network (RCN)** to connect rare disease (RD) researchers 
who co-authored on RD related publications, co-investigated on NIH funded RD projects, co-directed on RD based clinical trials. 

## Table of Contents

- [Data Collection and Sources](#data-collection-and-sources)
- [Knowledge Graph](#knowledge-graph)
- [Large Language Models (LLM)](#large-language-models-llm)
- [Implementation of Retrieval-Augmented Generation (RAG) and Agent for Expertise Identification](#implementation-of-retrieval-augmented-generation-(rag)-and-agent-for-expertise-identification)
- [Analysis](#analysis)
- [Dependencies](#dependencies)
- [Documentation](#documentation)
- [Getting Help](#getting-help)
- [Discussion and Development](#discussion-and-development)
  
## Data Collection and Sources

- **Researcher data** has been collected from various sources, including **publications**, **NIH-funded projects**, and **clinical trial** data related to Rare Diseases (RD). Data is retrieved from **[RDAS](https://rdas.ncats.nih.gov/)**.
- The 'affiliations' and 'locations' of researchers have been mapped to 'FIPS' codes using the [uscities.csv](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Social_Network/Data/uscities.csv) file.
- To facilitate the mapping of researcher affiliations in NIH-funded projects to FIPS codes, the [Grant_org1.csv](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Social_Network/Data/Grant_org1.csv) file was also utilized.

## Knowledge Graph

- For more detailed information about the data model, refer to the [Report](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Social_Network/Report.md).
- Initially, we developed nodes for [Grant](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Social_Network/Neo4j/Grant_populating_parallel.py), [PubMed](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Social_Network/Neo4j/Pubmed_populating_parallel.py), and [ClinicalTrial](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Social_Network/Neo4j/CT_populating_parallel.py) which are interconnected with GARD nodes and Researcher nodes. 
- We then developed [Cluster](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Social_Network/Neo4j/Clustering.py) nodes and their respective edges, followed by the creation of [Expertise](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Social_Network/Neo4j/Expertise.py) nodes. The knowledge graph structure was further refined through modifications to both [nodes](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Social_Network/Neo4j/Pubmed_populating_parallel_modification_node.py) and [edges](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Social_Network/Neo4j/Pubmed_populating_parallel_modification_edge.py).
- To build the knowledge graph in [Neo4j AuraDB](https://console-preview.neo4j.io/tools/query), [AuraDB.ipynb](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Social_Network/AuraDB/AuraDB.ipynb) file are used. This file includes various clustering techniques, such as **DBSCAN**, **K-means**, and **community detection**.

## Large Language Models (LLM)

- In the [LLMs&PromptEng.ipynb](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Social_Network/LLM/LLMs%26PromptEng.ipynb), we experimented with various prompting techniques, including **direct prompting, Zero-shot chain of thoughts, Self-Consistency CoT, self-feedback, and self-critique**.
- Several LLMs, such as **OpenAI GPT, Llama 2, Llama 3, XGen-7B, Microsoft Phi, Google Gemini,** and **Claude**, were tested within the [LLMs&PromptEng.ipynb](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Social_Network/LLM/LLMs%26PromptEng.ipynb) environment.
- Performance metrics, including ROUGE score, BERT score, [Mover Score](https://github.com/Jaber-Valinejad/emnlp19-moverscore), QuestEval, and BLANC, were used to evaluate the LLMs in the [Metrics.ipynb](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Social_Network/LLM/Metrics.ipynb).
- Additionally, **Deepseek** and **Llama 3** were tested on the Azure platform using the [Azure.ipynb](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Social_Network/LLM/Azure.ipynb) file.


## Implementation of Retrieval-Augmented Generation (RAG) and Agent for Expertise Identification

The implementation of Retrieval-Augmented Generation (RAG) for expertise identification is integrated within the [Agent.ipynb](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Social_Network/LLM/Agent.ipynb) notebook. In this implementation, we utilize `tavily_search` and `rag` as tools to enhance information retrieval and generation. Additionally, a user interface (UI) is provided using Gradio for seamless interaction.

## Analysis

- To visualize the social network and draw maps, the [Network_visualization.ipynb](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Social_Network/Analysis/Network_visualization.ipynb) is used.
- Various network analysis metrics, such as **Cliques, PageRank, Hubs**, and **Authorities**, are computed in [Analysis_v3.ipynb](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Social_Network/Analysis/Analysis_v3.ipynb).

## Dependencies

For a complete list of required packages, see the [full list of necessary packages](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Social_Network/requirements-dev.txt). 


## Documentation

 Documentation about the developed RCN can be found in the [Documentation](https://github.com/Jaber-Valinejad/RDAS/tree/master/RDAS_Social_Network/Docs). 


## Getting Help

If you have any questions or need assistance, please reach out through the GitHub issues page.

## Discussion and Development

For ongoing development discussions and to report issues, please use the [GitHub Issue Tracker](https://github.com/ncats/RDAS/issues). We welcome contributions and collaboration on GitHub.



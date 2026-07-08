
<p align="center">
  <img src="https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Target_Deconvolution/Figs/TD.png" width="600"/>
</p>

---

# Target Identification and Drug Discovery for Rare Diseases through High-Throughput Screening and Phenotypic Assays


|                |                                                   |
| -------------- | ------------------------------------------------- |
| **Testing**    | ![Static Badge](https://img.shields.io/badge/Project%20Status-Passing-green) |
| **Docs**       | ![Static Badge](https://img.shields.io/badge/Docs-Passing-green) |
| **Package**    | ![Python](https://img.shields.io/badge/Python-3.8%2B-blue.svg) ![GitHub last commit](https://img.shields.io/github/last-commit/Jaber-Valinejad/RDAS) [![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/Jaber-Valinejad/RDAS/blob/master/RDAS_FAERS/Methods/Neo4j_v2.ipynb) ![Static Badge](https://img.shields.io/badge/GraphDB-Neo4j-blue) ![Static Badge](https://img.shields.io/badge/Query%20Language-Cypher-yellow) |
| **Meta**       | [![DOI](https://zenodo.org/badge/DOI/10.1109/BIBM62325.2024.10822513.svg)](https://doi.org/10.1109/BIBM62325.2024.10822513) [![Docs](https://img.shields.io/badge/Docs-ReadTheDocs-blue)](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_FAERS/Docs/BIBM24_paper.pdf) ![GitHub License](https://img.shields.io/github/license/Jaber-Valinejad/RDAS) ![GitHub Sponsors](https://img.shields.io/github/sponsors/Jaber-Valinejad) |



## What is it?

This project focuses on identifying potential therapeutic targets for rare diseases by using phenotypic assays and high-throughput screening of compound libraries. The goal is to uncover small-molecule modulators that can address disease-specific cellular processes. By leveraging innovative target deconvolution methods and secondary screening, the project aims to accelerate drug discovery for rare diseases with limited treatment options, contributing to the development of effective therapies.

## Table of Contents

- [Data Collection and Sources](#data-collection-and-sources)
- [Discussion and Development](#discussion-and-development)
- [Target Deconvolution](#target-deconvolution)
  - [Main Approach 1: Target Deconvolution via Predicted Genes](#main-approach-1-target-deconvolution-via-predicted-genes)
  - [Main Approach 2: Target Deconvolution via Similar Compounds (CTD)](#main-approach-2-target-deconvolution-via-similar-compounds-ctd)
  - [Main Approach 3: Target Deconvolution via Similar Compounds (CID)](#main-approach-3-target-deconvolution-via-similar-compounds-cid)
- [Association Checking](#association-checking)
- [Fine-Tuned LLM Model for Association Discovery](#fine-tuned-LLM-model-for-association-discovery)
- [Dependencies](#dependencies)
- [Documentation](#documentation)
- [Getting Help](#getting-help)
- [Discussion and Development](#discussion_and_development)



## Data Collection and Sources

All data used for this project can be used in [Data folder](https://github.com/Jaber-Valinejad/RDAS/tree/master/RDAS_Target_Deconvolution/Data). Plus, for literature mining, we utilize the publication database in RDAS related to the target diseases. To perform this, we can run the following command in Neo4j:

```sh
# Neo4j
MATCH (p)<-[m: MENTIONED_IN]-(g:GARD)
WHERE g.GardId = "GARD:0002027"
optional MATCH (p:Article)-[r:ANNOTATION_FOR]-(t:PubtatorAnnotation)
WITH p, collect(t.text) AS texts
WITH p, reduce(all_texts = [], t IN texts | all_texts + t) AS all_texts
RETURN p.pubmed_id, p.title, p.abstractText, p.publicationYear, apoc.coll.toSet([text IN all_texts | toLower(text)]) AS unique_texts
```


## Target Deconvolution


### Main Approach 1: Target Deconvolution via Predicted Genes using [Pathway.ipynb](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Target_Deconvolution/Methods/Pathway_v3.ipynb)

In this approach, we use tools like [**SwissDrugDesign**](http://www.swisstargetprediction.ch/result.php?job=105216416&organism=Homo_sapiens) or **SuperPRED** to predict genes associated with the newly identified compounds. The following methods are used to assess the association:

- Then, we chcek association between these genes with target disease.

- **Pathway Enrichment Analysis**: After predicting the genes, we perform a pathway enrichment analysis using [ShinyGO](http://bioinformatics.sdstate.edu/go/) to identify any enriched pathways. Then, we chcek association between these enriched pathways with target disease.  
  
- **Enriched Biological Terms**: Biological terms enriched in the target genes are analyzed using ShinyGO with all available gene sets as pathway databases. Then, we chcek association between these enriched biological terms with target disease.  

#### Key Metrics:
- **Fold Enrichment**: The ratio of the percentage of genes in your list compared to the background genes. Higher values indicate stronger enrichment.
- **False Discovery Rate (FDR)**: Calculated using the Benjamini-Hochberg method to adjust for multiple comparisons.

### Main Approach 2: Target Deconvolution via Similar Compounds (CTD) using [CTD.ipynb](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Target_Deconvolution/Methods/CTD_V2.ipynb)

This method leverages the **Comparative Toxicogenomics Database (CTD)** and **ChEMBL** to identify similar compounds related to the target disease. Steps include:

- **Identifying Targets**: Using the 'chembl_webresource_client' and organism ‘Homo sapiens’ to find targets.
- **Identifying similiar compounds**: To find similar compounds we use [ChEMBL API](https://www.ebi.ac.uk/chembl/g/#search_results/all/query=CHEMBL1372162). Then, we find related genes using [uniprot](https://www.uniprot.org/uniprotkb/P00381/entry.)
- **Identifying realted disaese**: Visit [pubchem](https://pubchem.ncbi.nlm.nih.gov/compound/Dextilidine). Then  we Find the [CTD link](https://ctdbase.org/detail.go?type=chem&acc=D013993) there. Then, we use this information to find related diseases. 

- **Association Checking**: We check associations between any of genes, phenotypes, and diseases related to the identified compounds using resources like OMIM, Orphanet, and the [Human Phenotype Ontology (HPO)]([https://hpo.jax.org/data/annotations) and target gene.



### Main Approach 3: Target Deconvolution via Similar Compounds (CID) using [CID.ipynp](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Target_Deconvolution/Methods/CID.ipynb)

When using a threshold of 0.8 from CTD, fewer similar compounds may be identified. To increase the chance of finding associations, we use **Compound Identifier (CID)**, and convert CIDs to CTD codes. To link these CIDs to relevant diseases and phenotypes, we need to convert CIDs to CTD codes. This conversion can be achieved by mapping CID values to their respective CTD codes. Below are the steps to accomplish this:

1. **Using SID map**: To convert CIDs to CTD codes, we can use the [SID-Map](https://ftp.ncbi.nlm.nih.gov/pubchem/Substance/Extras/SID-Map.gz) file, which contains the mapping between substances (SID), their registry identifiers, and their standardized CID. This is a gzipped text file that lists substances with their corresponding SID, source names, registry identifiers, and the CID (if available). We can use command-line tools to filter the SID-Map file and extract relevant mappings.

```sh
(structuredev12)(structure) gzc $PUBCHEM_FTP/Substance/Extras/SID-Map.gz | grep "Comparative Toxicogenomics Database" | egrep ' 30131$'
```

```sh
134223583       Comparative Toxicogenomics Database (CTD)       D013993 30131
```
SID-Map.gz: This is a listing of all (live) SIDs with their source names and registry identifiers, and the standardized CID if present. It is  a gzipped text file where each line contains at least three columns: SID, tab, source name, tab, registry identifier; then  a fourth column of tab, CID if there is a standardized CID for the given SID. This SID-Map file helps identify the standardized CID for substances and their corresponding CTD identifier, enabling the association between compounds and diseases.
   
4. **API Integration**: Additional conversion can be done via [pubchem API](https://pubchem.ncbi.nlm.nih.gov) to map CIDs to related diseases and phenotypes. Please refer to [SID-Map](https://ftp.ncbi.nlm.nih.gov/pubchem/Substance/Extras/SID-Map.gz).




## Association Checking

The association checking process is multi-faceted and involves:

1. **Literature Search**: It includes the follwoing steps: 1) A comprehensive search through relevant scientific literature; 2) Verifying associations through known datasets; 3) Checking concurrency on sentence level. Please refer to [CTD.ipynb](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Target_Deconvolution/Methods/CTD_V2.ipynb).
2. **Semantic Similarity**: Evaluating similarities between biological terms, diseases, genes, and phenotypes. Please refer to [Pathway.ipynb](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Target_Deconvolution/Methods/Pathway_v2.ipynb)
3. **Scientific Evidence Mining using Translator**: Using tools like [Translator](https://arax.ncats.io) to mine scientific evidence for associations.


-We assess the association between genes, phenotypes, diseases, and target genes. 

-In addition to the original terms, we consider their synonyms, descriptions, and clinical features obtained from sources such as OMIM and Orphanet.

-Synonyms for diseases and biological terms can be accessed through [**OMIM**](https://api.omim.org/api/), [**Orphanet**](https://www.orpha.net/en/disease/detail/), and so on. For pathways, we refer to the [Gene Ontology database](https://amigo.geneontology.org/amigo/term/GO:0019430).

## Fine-Tuned LLM Model for Association Discovery
The [annotation datasets](https://hpo.jax.org/data/annotations) are obtained through [The Human Phenotype Ontology](https://hpo.jax.org/data/annotations). These datasets include:

- [Genes to Phenotype](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Target_Deconvolution/Data/genes_to_phenotype.txt)
- [Phenotype to Genes](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Target_Deconvolution/Data/phenotype_to_genes.zip)
- [Genes to Disease](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Target_Deconvolution/Data/genes_to_disease.txt)

In addition to these datasets, we utilized a fine-tuned dataset available in [FT_data_v2.csv](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_GRANT/Data/FT_data_v2.csv) to construct the final fine-tuning dataset. The final [dataset](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Target_Deconvolution/Data/finetuning_datasets.zip) was generated using the [finetuning_datasets.ipynb](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Target_Deconvolution/Methods/finetuning_datasets.ipynb) notebook. 

The fine-tuning process is detailed in the [Lora.ipynb](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Target_Deconvolution/Methods/Lora.ipynb) notebook.

## Dependencies

- **Python** (version 3.x)
- **RDAS** Python package
- **ShinyGO** for pathway enrichment analysis
- **SwissDrugDesign** and **SuperPRED** for gene prediction
- **ChemBL** API for compound information
- **HPO** and **OMIM** for disease and phenotype data



## Documentation

For more detailed documentation, please refer to [Docs folder](https://github.com/Jaber-Valinejad/RDAS/tree/master/RDAS_Target_Deconvolution/Docs).



## Getting Help

For any issues or questions, please open an issue in the GitHub repository or contact the project maintainers.



## Discussion and Development
We are working towards developing machine learning and deep learning models to predict genes associated with newly identified compounds. Currently, we are using tools like SwissDrugDesign and SuperPRED for gene-target predictions, which involve predicting genes based on the compounds' chemical structures. However, these tools have limitations, such as the inability to set prediction thresholds, leading to lower-confidence predictions (e.g., probabilities around 0.1). As we move forward, we plan to integrate machine learning and deep learning techniques to enhance the accuracy and reliability of these predictions. This will enable us to refine our approach, increase the confidence of gene-target associations, and accelerate the identification of promising therapeutic targets for rare diseases.
To discuss new ideas, improvements, or any questions, please join the conversation in the Discussions section of the repository.




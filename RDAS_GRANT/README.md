<p align="center">
  <img src="https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_GRANT/Figs/Gr.webp" width="600"/>
</p>

---

# Rare Disease Identification in Unstructured Data


|                |                                                   |
| -------------- | ------------------------------------------------- |
| **Testing**    | ![Static Badge](https://img.shields.io/badge/Project%20Status-Passing-green) |
| **Docs**       | ![Static Badge](https://img.shields.io/badge/Docs-Passing-green) |
| **Package**    | ![Python](https://img.shields.io/badge/Python-3.8%2B-blue.svg) ![GitHub last commit](https://img.shields.io/github/last-commit/Jaber-Valinejad/RDAS) [![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/Jaber-Valinejad/RDAS/blob/master/RDAS_FAERS/Methods/Neo4j_v2.ipynb) ![Static Badge](https://img.shields.io/badge/GraphDB-Neo4j-blue) ![Static Badge](https://img.shields.io/badge/Query%20Language-Cypher-yellow) |
| **Meta**       | [![DOI](https://zenodo.org/badge/DOI/10.1109/BIBM62325.2024.10822513.svg)](https://doi.org/10.1109/BIBM62325.2024.10822513) [![Docs](https://img.shields.io/badge/Docs-ReadTheDocs-blue)](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_FAERS/Docs/BIBM24_paper.pdf) ![GitHub License](https://img.shields.io/github/license/Jaber-Valinejad/RDAS) ![GitHub Sponsors](https://img.shields.io/github/sponsors/Jaber-Valinejad) |



## What is it?
The significant challenges associated with rare diseases in the medical and research domains include the scarcity of information, which is often confined to unstructured formats. Although existing approaches provide valuable insights, there is a need to develop effective methods to identify information pertinent to rare diseases for advancing rare disease research. We identified mentions of rare diseases in relevant texts and assessed their relevance using derived scores, the confidence score and semantic similarity from a fine-tuned BioMedBERT encoder. This encoder was fine-tuned using rare disease related text from Online Mendelian Inheritance in Man (OMIM), Orphanet, a manually validated dataset, and STS benchmark datasets. The process of identifying meaningful rare disease mentioned was presented through retrieved relevant NIH-funded projects, utilizing a generated knowledge graph in Neo4j to host data on 2,067 GARD diseases with over 320,000 NIH funded projects.

## Table of Contents


1. [Data and Resources](#data-and-resources)
   - [Data](#data)
   - [Fine-tuning](#fine-tuning)
2. [Method](#method)
3. [Tableau](#tableau)
4. [Analysis](#analysis)
5. [Dependencies](#dependencies)
6. [Documentation](#documentation)
7. [Getting Help](#getting-help)
8. [Discussion and Development](#discussion-and-development)


## Data and Resources

### Data

- Download the NIH grant funding datasets from the following API:  
  [NIH Reporter API](https://api.reporter.nih.gov/v2/projects/search)

### Fine-tuning

For fine-tuning the model, we used the following datasets: 
- [**GARD_V1**](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_GRANT/Data/Gard_V1.csv)
- [**ORPHAlinearisation_en**](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_GRANT/Data/ORPHAlinearisation_en.csv)
- **SourceSynonym** and **OmimMember** columns from [**J_GARD_master**](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_GRANT/Data/J_GARD_master.csv)
- [**Curated data**](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_GRANT/Data/final_result_.csv) 

## Method

- **Approach 1:** Fine-tuning is performed using the [Bert.ipynb](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_GRANT/Methods/Bert.ipynb) notebook. The main method for identifying rare diseases and calculating the semantic similarity score and confidence score is implemented in the [Main.ipynb](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_GRANT/Methods/Main.ipynb) notebook.
- **Approach 2:** This approach leverages a fine-tuned large language model (LLM) for rare disease identification. The fine-tuning process is detailed in [Lora.ipynb](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_GRANT/Methods/Lora.ipynb), with the corresponding fine-tuned dataset available in [FT_data_v2.csv](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_GRANT/Data/FT_data_v2.csv). The dataset is generated using the [Finetunning_data.ipynb](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_GRANT/Methods/Finetunning_data.ipynb) notebook.
  
## Tableau

For data visualization, we used **Tableau Desktop** and **Tableau Prep** for data preprocessing. The interactive version of ffures can be accessed online via [Tableau Public](https://public.tableau.com/app/profile/jab.valinejad/viz/GrantFundingDistributionforRareDiseaseResearchAcrossU_S_Institutions/GrantFundingDistributionforRareDiseaseResearchAcrossU_S_Institutions), which includes filters for:
- Institution names
- Grant funding ($)
- Research outputs



## Analysis

- Please refer to [Analysis.ipynp](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_GRANT/Analysis/Analysis_v1.ipynb) 

## Dependencies

For a complete list of required packages, see the [full list of necessary packages](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_GRANT/requirements-dev.txt). 


## Documentation

 Documentation about the developed RCN can be found in the [Documentation](https://github.com/Jaber-Valinejad/RDAS/tree/master/RDAS_GRANT/Docs). 


## Getting Help

If you have any questions or need assistance, please reach out through the GitHub issues page.

## Discussion and Development

For ongoing development discussions and to report issues, please use the [GitHub Issue Tracker](https://github.com/ncats/RDAS/issues). We welcome contributions and collaboration on GitHub.

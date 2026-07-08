<p align="center">
  <img src="https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_FAERS/Figs/AE.webp" width="600"/>
</p>

---


|                |                                                   |
| -------------- | ------------------------------------------------- |
| **Testing**    | ![Static Badge](https://img.shields.io/badge/Project%20Status-Passing-green) |
| **Docs**       | ![Static Badge](https://img.shields.io/badge/Docs-Passing-green) |
| **Package**    | ![Python](https://img.shields.io/badge/Python-3.8%2B-blue.svg) ![GitHub last commit](https://img.shields.io/github/last-commit/Jaber-Valinejad/RDAS) [![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/Jaber-Valinejad/RDAS/blob/master/RDAS_FAERS/Methods/Neo4j_v2.ipynb) ![Static Badge](https://img.shields.io/badge/GraphDB-Neo4j-blue) ![Static Badge](https://img.shields.io/badge/Query%20Language-Cypher-yellow) |
| **Meta**       | [![DOI](https://zenodo.org/badge/DOI/10.1109/BIBM62325.2024.10822513.svg)](https://doi.org/10.1109/BIBM62325.2024.10822513) [![Docs](https://img.shields.io/badge/Docs-ReadTheDocs-blue)](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_FAERS/Docs/BIBM24_paper.pdf) ![GitHub License](https://img.shields.io/github/license/Jaber-Valinejad/RDAS) ![GitHub Sponsors](https://img.shields.io/github/sponsors/Jaber-Valinejad) |


# What is it?
Rare diseases affect fewer than 200,000 individuals in the United States, with some being so rare that only a handful of people are impacted. According to the U.S. Food and Drug Administration (FDA), there are 1,268 approved orphan drugs available for treating these conditions. However, potentially beneficial drugs can also have side effects. Some adverse events, while serious, may be rare, making them difficult to identify or quantify in randomized controlled trials. Understanding these events is critical for improving patient safety and treatment outcomes. To better assess these risks, we aimed at summarizing adverse drug events for rare diseases by utilizing FDA Adverse Event Reporting System (FAERS). This study offers a foundation for future research of improving drug safety in rare diseases.

# Table of Contents

1. [What is it?](#what-is-it)
2. [Data and Resources](#data-and-resources)
3. [Methods](#methods)
   - [Mapping](#mapping)
   - [Data Model](#data-model)
   - [Cypher Query for Analyzing ADEs](#cypher-query-for-analyzing-ades)
4. [Dependencies](#dependencies)
5.  [Documentation](#documentation)
6.  [Getting Help](#getting-help)
7.  [Discussion and Development](#discussion-and-development)

## Data and Resources

- **Adverse Event Reporting System (FAERS)** information can be found on the [FDA website](https://www.fda.gov/drugs/surveillance/fdas-adverse-event-reporting-system-faers).
- **Adverse drug event datasets** are available on the [OpenFDA Human Drug Datasets](https://open.fda.gov/data/downloads/). These datasets include:
  - Drug Adverse Events [/drug/event]
  - Drug Labeling [/drug/label]
  - National Drug Code Directory [/drug/ndc]
  - Drug Recall Enforcement Reports [/drug/enforcement]
  
  Using [data.ipynb](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_FAERS/Methods/Data.ipynb), we can utilize the OpenFDA API to download datasets.
  
- A list of all **rare diseases** for mapping can be obtained from resources like [GARD](https://rarediseases.info.nih.gov/). Please also refer to the file [Gard_v1.csv](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_FAERS/Data/Gard_v1.csv).
  
- A list of **approved and not approved drugs** by the FDA can be found from the [FDA Orphan Drug Designations and Approvals](https://www.accessdata.fda.gov/scripts/opdlisting/oopd/). Please see the file [all_drugs.csv](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_FAERS/Data/all_drugs.xlsx) to find all approved drugs.

## Methods

### Mapping

To capture associations between orphan drugs/designations and rare diseases, we collected orphan drug designations and approvals from the [FDA Orphan Drug Designations and Approvals](https://fis.fda.gov/sense/app/95239e26-e0be-42d9-a960-9a5f7f1c25ee/sheet/7a47a261-d58b-4203-a8aa-6d3021737452/state/analysis). 

Subsequently, we mapped rare diseases obtained from the [Genetic and Rare Diseases (GARD) Information Center](https://ncats.nih.gov/research/research-resources/gard) to:
- "Approved Labeled Indication" for FDA-approved orphan drugs
- "Orphan Designation" for orphan designations based on string matching.

Table 1 presents examples of these mappings:

### Table 1: Examples of Mappings Between Orphan Designations and Rare Diseases

| Orphan Designation | Mapped Rare Diseases |
| ------------------ | -------------------- |
| Treatment of retinopathy of prematurity | Retinopathy of prematurity (GARD ID: 0005695) |
| Treatment of idiopathic pulmonary fibrosis | Idiopathic pulmonary fibrosis (GARD ID: 0008609) |
| Treatment of cutaneous variants of porphyria (including treatment and prevention of cutaneous manifestations of disease) | Porphyria (GARD ID: 0010353) |

The drug-to-GARD mapping can be found in the file [mapping.csv](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_FAERS/Data/mapping.csv).

### Data Model

To build the data model in **Neo4j**, you can use the [neo4j_v2.ipynb](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_FAERS/Methods/Neo4j_v2.ipynb). For further analysis, use the [analysis.ipynb](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_FAERS/Methods/Analysis.ipynb).

### Cypher Query for Analyzing ADEs

To further analyze the impact of specific orphan drugs, we composed **Cypher Query 1** to generate the top 10 orphan drugs associated with the highest number of ADEs in Neo4j:

```cypher
# Neo4j Cypher Query 1
MATCH (n:Event_Information)-[r:HAS_ADVERSE_EFFECT]- (m:Drug) 
WITH DISTINCT n, m.openfda_generic_name AS generic_name 
WITH generic_name, COUNT(DISTINCT n) AS event_count 
RETURN generic_name, event_count 
ORDER BY event_count DESC 
LIMIT 10
```


## Dependencies

For a complete list of required packages, see the [full list of necessary packages](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_FAERS/requirements-dev.txt)

## Documentation

 Documentation about the developed RCN can be found in the [Documentation](https://github.com/Jaber-Valinejad/RDAS/tree/master/RDAS_FAERS/Docs). 


## Getting Help

If you have any questions or need assistance, please reach out through the GitHub issues page.

## Discussion and Development

For ongoing development discussions and to report issues, please use the [GitHub Issue Tracker](https://github.com/ncats/RDAS/issues). We welcome contributions and collaboration on GitHub.

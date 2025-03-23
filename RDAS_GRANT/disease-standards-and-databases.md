# Disease Standards and Notations

This document lists the disease-related standards and databases, including their descriptions and links to their official websites.

## What is it?
The primary goal is to standardize disease information across different databases and sources, creating a consistent and comprehensive mapping of rare diseases to enable more accurate research, diagnosis, and treatment options. This data integration process is essential for understanding the clinical manifestations, genetic underpinnings, and phenotypic traits of rare diseases. Additionally, it lays the groundwork for enhancing disease-related data interoperability across various platforms.


## Table of Contents

-[Standards and Notations](#standards_and_notations)

-[Conceptual Relationships](#conceptual-relationships)

-[Data Integration and Mapping](#data-integration-and-mapping)


## Standards and Notations

#### 1. [UMLS (Unified Medical Language System)](https://www.nlm.nih.gov/research/umls/)
- **Description**: A comprehensive vocabulary system that includes various biomedical vocabularies and classifications. UMLS enables mapping across different systems like SNOMED CT, ICD-10, etc.

#### 2. [MONDO (The MONDO Disease Ontology)](https://mondo.monarchinitiative.org/)
- **Description**: A disease ontology that integrates multiple disease classification systems into a single, unified framework.

#### 3. [SNOMED CT (Systematized Nomenclature of Medicine - Clinical Terms)](https://www.snomed.org/)
- **Description**: A comprehensive medical terminology used to classify diseases, conditions, procedures, and other clinical information.

#### 4. [ICD-10 (International Classification of Diseases, 10th Edition)](https://www.who.int/classifications/icd/en/)
- **Description**: The ICD-10 is the international standard for coding and classifying health conditions, including diseases and disorders.

#### 5. [ICD-11 (International Classification of Diseases, 11th Edition)](https://www.who.int/classifications/icd/en/)
- **Description**: The updated edition of ICD-10, providing a more granular classification for health conditions and diseases.

#### 6. [OMIM (Online Mendelian Inheritance in Man)](https://www.omim.org/)
- **Description**: A comprehensive, authoritative resource for the genetic basis of diseases, with detailed information on inherited conditions.

#### 7. [Orphanet](https://www.orpha.net/)
- **Description**: A database of rare diseases and orphan drugs, providing information on rare disease classification, symptoms, and treatment options.

#### 8. [GARD (Genetic and Rare Diseases Information Center)](https://rarediseases.info.nih.gov/)
- **Description**: A center providing information on rare diseases, including disease descriptions, prevalence, and related research.

#### 9. [KEGG (Kyoto Encyclopedia of Genes and Genomes)](https://www.kegg.jp/)
- **Description**: A database that links biological information, including diseases and metabolic pathways, to gene functions.

#### 10. [NORD (National Organization for Rare Disorders)](https://rarediseases.org/)
- **Description**: NORD provides information and advocacy for rare diseases, including research and treatment options.

#### 11. [OMIA (Online Mendelian Inheritance in Animals)](https://omia.org/)
- **Description**: A resource dedicated to the inheritance of diseases in animals, including genetic disorders in various species.

#### 12. [NIFSTD (NeuroInformatics Framework Standardized Terminology)](https://www.nif.org/)
- **Description**: A collection of terms related to neuroinformatics and neuroscience data.

#### 13. [MPATH (Molecular Pathology)](https://www.monarchinitiative.org/)
- **Description**: A collection of terms related to molecular pathology and disease processes.

#### 14. [GTR (Genetic Testing Registry)](https://www.ncbi.nlm.nih.gov/gtr/)
- **Description**: A central resource for genetic tests and their associated clinical relevance.

#### 15. [HGNC (HUGO Gene Nomenclature Committee)](https://www.genenames.org/)
- **Description**: A database for the standard naming of genes, including their aliases and functional annotations.

#### 16. [DOID (Disease Ontology Identifier)](http://disease-ontology.org/)
- **Description**: A database of human diseases, with each disease assigned a unique identifier.

#### 17. [DECIPHER (DatabasE of Chromosomal Imbalance and Phenotype in Humans using Ensembl Resources)](https://www.deciphergenomics.org/)
- **Description**: A database of chromosomal abnormalities in humans and their related diseases.

#### 18. [CSP (Clinical Studies Platform)](https://clinicalstudies.info.nih.gov/)
- **Description**: A resource related to clinical studies, including data on patient outcomes and diseases.

---

##  Conceptual Relationships:

- **Disease Classification Systems** (e.g., **ICD-10**, **ICD-11**, **SNOMED CT**, **MONDO**, **Orphanet**) are used to standardize the categorization of diseases, which helps in organizing large disease datasets.

- **Genetic Databases** (e.g., **OMIM**, **GARD**, **HGNC**, **KEGG**) provide detailed genetic information associated with diseases, often used for identifying genetic markers or potential therapies.

- **Phenotype and Clinical Data** (e.g., **DOID**, **MPATH**, **CSP**) link diseases to clinical presentations or phenotypes, which are important for understanding the manifestation of diseases in patients.

#### Example: Mapping Cystic Fibrosis to Relevant Databases and Standards

| Database     | ID        | Description                              | Link                                          |
|--------------|-----------|------------------------------------------|-----------------------------------------------|
| **GARD**     | GARD:1234 | Rare disease information for CF         | [GARD Info](https://rarediseases.info.nih.gov/)|
| **OMIM**     | OMIM:2197 | Genetic basis of CF, CFTR mutations     | [OMIM Info](https://www.omim.org/)             |
| **MONDO**    | MONDO:0016| Classification of CF as a genetic disorder | [MONDO Info](https://mondo.monarchinitiative.org/)|
| **SNOMED CT**| SNOMED:4177| Clinical term for CF                    | [SNOMED Info](https://www.snomed.org/)         |
| **KEGG**     | KEGG:001  | Pathways related to CFTR gene            | [KEGG Info](https://www.kegg.jp/)              |



## Data Integration and Mapping:

- **MONDO/DOID Integration**: Loading and merging data from **MONDO** (Disease Ontology) and **DOID** (Disease Ontology Identifiers) to establish consistent disease classifications.
- **GARD to ORPHANET Mapping**: Mapping **GARD** (Genetic and Rare Diseases Information Center) IDs to **ORPHANET** (rare disease database) using standard codes like **ICD-10** and **SNOMED-CT**.
- **Group of Disorders Filter**: Filtering diseases based on their association with groups of disorders and ensuring that the diseases map correctly to standardized codes.



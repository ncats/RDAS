# The Social Network for Rare Disease Specialists

Rare diseases present unique challenges for both patients and researchers, stemming from limited access to researchers, fragmented diagnostic journeys, and the scarcity of collaborative research networks. In the work, we developed a Rare disease (RD) Research Collaborative Network (RCN) for supporting RD researchers or potential collaboration recommendation.
We first gathered information about RD researchers from the Rare Disease Alert System (RDAS). Co-authors on the publications, co-investigators on the NIH funded projects, and co-points-of-contact on the clinical trials were applied to create the RCN. Next, we clustered the RCN by using the clustering algorithm, density-based spatial clustering of applications with noise (DBSCAN) to analyze research backgrounds in free text. Finally, we employed a chain of thought approach, fine-tuned with Low-Rank Adaptation (LoRA) for Large Language Models (LLMs), to identify the research expertise for each cluster.

![https://github.com/Jaber-Valinejad/RDAS/edit/master/RDAS_Social_Network/Figs/sn.jpg](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Social_Network/Figs/sn.jpg)
### Figure 1. Workflow of the Rare Disease Research Collaborative Network Development 

--------------
## Data Model
To semantically represent the information about the researchers, their belonging clusters and their research expertise, we predefined a data model. we defined eight predicates (i.e., edges) to connect two classes (i.e., nodes) in the graph include COLLABORATED_WITH, RELATED_GARD, RESEARCHER_LOCATION, INVOLVES_RESEARCHER_CT, INVOLVES_RESEARCHER_G, INVOLVES_RESEARCHER_P, RESEARCH_ON, and HAS_EXPERTISE.  The number of collaborations as an edge property was attached to the predicate of COLLABORATED_WITH, to indicate the strength of collaboration. The nodes in this knowledge graph include Clinical Trials, PubMed articles, NIH-funded projects, GARD, researchers, affiliation locations, research clusters, and research expertise.

![https://github.com/Jaber-Valinejad/RDAS/edit/master/RDAS_Social_Network/Figs/dl.png](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Social_Network/Figs/dl.png)
### Figure 2. Data model 

Table below provides a comprehensive list of the primary classes and their associated data properties. 

### Table 1. Full list of primary classes and their associated data properties

| Primary Classes           | Associated Data Properties                                                                                           | 
|---------------------------|----------------------------------------------------------------------------------------------------------------------|
| Clinical Trial            | Official Name, Official Affiliation, Brief Title, Official Title, Brief Summary, NCT Id, Location City, Location State, Location Country, Interventions |                   
| PubMed articles           | Full Name, Title, Affiliation, PubMed ID, Abstract Text, Keyword, Mesh terms                                        |                   
| NIH funded projects       | PI Name, Organization Name, Title, Application ID, Abstract, Terms, Core Project Number                             |                   
| GARD                      | GARD Name, GARD ID                                                                                                  |                   
| Researchers               | Name, Affiliation Name, Contact Information, Affiliation Zip Code                                                   |                   
| Location of Affiliation   | Affiliation Country, Affiliation State, Affiliation County, Affiliation City, Affiliation FIPS                      |                   
| Research Clusters         | Cluster ID, Cluster Size, Evidence, Key Terms                                                                       |                   
| Research Expertise        | Summarized Expertise                                                                                                |                   


### Table 2. Relationships between primary classes

| Class (Subject)                               | Class (Object)       | Predicate               |
|-----------------------------------------------|-----------------------|-------------------------|
| Researchers                                   | Researchers           | COLLABORATED_WITH       |
| Clinical Trial, NIH funded projects, PubMed articles | GARD                  | RELATED_GARD            |
| Location of Affiliation                       | Researchers           | RESEARCHER_LOCATION     |
| Clinical Trial                                | Researchers           | INVOLVES_RESEARCHER_CT  |
| NIH funded projects                           | Researchers           | INVOLVES_RESEARCHER_G   |
| PubMed articles                               | Researchers           | INVOLVES_RESEARCHER_P   |
| Researchers                                   | Research Clusters     | RESEARCH_ON             |
| Research Clusters                             | Research Expertise    | HAS_EXPERTISE           |

## Analysis  
Cystic fibrosis (GARD:0006233) is a progressive genetic disorder that affects the lungs, digestive system, and other organs by causing thick, sticky mucus buildup, leading to respiratory complications and difficulty in nutrient absorption. Despite significant advancements in treatment, Cystic fibrosis remains a complex disease requiring continuous research to develop more effective therapies [19]. Addressing such challenges benefits from the integration of diverse expertise across disciplines, as breakthroughs often emerge through the combined efforts of researchers, clinicians, and organizations working together to advance scientific understanding and improve patient outcomes. 
A geographic map in the USA showing researchers working on Cystic Fibrosis was generated by executing Cypher Query 3 and the map is shown in Figure below. 

![https://github.com/Jaber-Valinejad/RDAS/edit/master/RDAS_Social_Network/Figs/Map.png](https://github.com/Jaber-Valinejad/RDAS/blob/master/RDAS_Social_Network/Figs/Map.png)
### Figure 3. geographic map of the USA showing researchers working on Cystic Fibrosis (the number labelled on the nodes denoting FIPS codes, node size denoting intensity of collaboration, self-loops  represent collaborations among researchers within the same county).

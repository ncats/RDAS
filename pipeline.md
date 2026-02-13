# 2. Clinical Trial 
-
    ### 2.1 init_1_clinical_trial_step_1.py 

        2.1.1 Get the all GARD nodes: MATCH (x:GARD) RETURN x.GardId AS GardId, x.Name AS Name, x.Synonyms AS Synonyms ORDER BY x.GardId ASC
        2.1.2  For each GARD node:
            2.1.2.1 Takes the node’s Name and Synonyms.
            2.1.2.2 Filters synonyms to exclude ones detected as English (_is_english) and ones under a character threshold (_is_under_char_threshold).
            2.1.2.3 Builds a names list: [primary Name] + filtered_non_English_long_synonyms.   
        
        2.1.3 For each name of names list:
            2.1.3.1 Builds a ClinicalTrials.gov v2 “studies” query with parameters that include: a condition/name expansion based on the disease name ,
               a last update date range, and fields=NCTId, pageSize=1000, and countTotal=true.
               Fetches JSON and handles pagination with nextPageToken until there are no more pages.
        
        2.1.4 Fetch full study detail per NCTId
            2.1.4.1 For each NCTId: https://clinicaltrials.gov/api/v2/studies/{NCTID}
            2.1.4.2 Store into table clinical_trial(gardId, disease, nctid, studies, url)

    ### 2.2 init_1dot5_clinical_trial_step_1.py 
        Create and Store UNIQUE Clinical Trial into  clinical_trial_unique table


    ### 2.3 init_5_clinical_trial_retrieve_pmids_umlti.py
        Purpose of the Script

        This script extracts PubMed IDs (PMIDs) from clinical trial records stored in a database, and inserts those PMIDs into another table for further analysis. 
        It works in batches and updates the database to mark processed records.

        2.3.1  
            SELECT nctid, studies  FROM clinical_trial_unique WHERE  nctid IS NOT NULL 
            AND  (id BETWEEN {start_id} AND {end_id})
            AND  pmid_processed IS NULL

        2.3.2 Parse studies (by nctid) and get the PMIDs
        2.3.3 Store PMIDS inot table  clinical_trial_nctid_pmids

    ### 2.4 init_6_clinical_trial_pmids_not_in_Article_umlti.py


# 3. Publication 
-
    ### 3.1 init_1_publication_gard_pubmed-id-list.py

        Purpose of the Script
        This script queries the PubMed API (NCBI Entrez esearch.fcgi) to find PubMed article IDs (PMIDs) 
        for given search terms (e.g., disease names) and stores the results in a MySQL database table publication_pubmed.

        3.1.1 Calls the PubMed API: 
            https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term={term_search_query}&mindate={mindate}&maxdate={maxdate}&retmode=json&retmax=10000&api_key={self.api_key}
        
        3.1.2 INSERT INTO publication_pubmed (gard_id,year_range,search_term,total,retmax,retstart,pubmed_ids,query_translation,phrases_ignored,quoted_phrases_not_found, ref_json)
    
    ### 3.2 init_2_2_publication-gard-searchterm-pubmed-mapping.py
        Generate gard_id - search item - pubmd_id mapping 

    ### 3.3 init_3_1_publication-article-by-pubmed-id.py
        Retrieve Articles information by the pubmed_id(from unique_pubmed_ids.txt) by API endpoint, and store into the table publication_article

    ### 3.4 init_5_publication-gard-omim-mapping.py
        Create gard_id <---> omim_id mapping
        Retrieve GARD id and OMIM id from gard table, and insert into table publication_gard_omim 

    ### 3.5 init_6_publication-retrieve-omim.py
        3.5.1 Get omim raw data: https://api.omim.org/api/entry?mimNumber={omim_id}&include=all&format=json&apiKey={api_key}
        3.5.2 Store into database: INSERT INTO publication_omim (omim_id, entry_json) VALUES (%s, %s)

    ### 3.6 Add OMIM articles into publication_article
        See 3_publication/initializer/omim_article.py


# 4. Grant 
    4.1 


        
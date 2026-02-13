This Python script is designed to establish relationships between **GARD (Genetic and Rare Diseases Information Center) diseases** and **NIH (National Institutes of Health) grant projects**. It does this by analyzing the text content of grant project titles, public health relevance statements, and abstract texts to identify mentions of GARD-related terms.

Here's a breakdown of its purpose and key functionalities:

---

## Core Purpose

The primary goal of this script is to **identify and quantify the relevance of various GARD diseases to NIH grant projects**. It does this by performing sophisticated text analysis on different parts of grant applications (project titles, public health relevance statements, and abstract texts) to find mentions of GARD disease names and their synonyms.

---

## Key Functionalities

* **Database Interaction:** It connects to a MySQL database (`rdas_db`) to fetch grant project information (`grant_project` and `grant_abstract` tables) and pre-processed GARD disease names and their synonyms (`grant_gard_processed_names` table).
* **Text Preprocessing:** It utilizes `nltk` (Natural Language Toolkit) and `spacy` to perform various text processing steps, including:
    * **Tokenization:** Breaking down text into individual words.
    * **Stop Word Removal:** Eliminating common words (like "the," "is," "and") that don't carry significant meaning.
    * **Stemming:** Reducing words to their root form (e.g., "running" to "run") to improve matching.
    * **Sentence Analysis:** Identifying sentence structure, verb tenses, and negations using `spacy` to prioritize certain parts of the text (e.g., future-oriented sentences).
* **GARD Term Matching:** It implements multiple strategies for matching GARD disease names and their synonyms within the grant text:
    * **Exact Matching:** Finding exact occurrences of GARD terms.
    * **Stemmed Matching:** Matching based on the stemmed versions of words.
    * **Bag-of-Words (BoW) Matching:** Matching based on the presence of individual words in a multi-word term.
* **Semantic Similarity (ClinicalBERT):** It uses a pre-trained ClinicalBERT model to calculate the semantic similarity between segments of the grant text and GARD disease names. This helps determine if the text is conceptually "about" a particular disease, even if the exact term isn't present.
* **Relevance Scoring and Normalization:** It assigns scores to identified GARD diseases based on where they appear in the grant text (e.g., title mentions are weighted higher) and the frequency of their occurrence. These scores are then normalized to provide a comparable measure of relevance.
* **Database Insertion:** The script inserts the identified relationships (GARD ID, application ID, GARD name, source type, scores, etc.) into the `grant_gard_project_relation` table in the database.
* **Batch Processing:** It processes grants in batches to manage memory usage and improve efficiency when dealing with large datasets.

---

## How It Works

The script systematically iterates through grant projects, extracting their titles, public health relevance statements, and abstract texts. For each grant, it attempts to find relevant GARD diseases by:

1.  **Prioritizing Project Title:** It first checks for GARD terms in the `PROJECT_TITLE`. If a match is found, it's given a higher weight.
2.  **Analyzing Public Health Relevance Statement (PHR):** If no strong matches are found in the title, it moves on to the `PHR`, applying detailed text analysis to identify key sentences and their tenses to understand the project's focus.
3.  **Examining Abstract Text:** Finally, it analyzes the `ABSTRACT_TEXT` for GARD terms, considering the entire abstract for broader relevance.
4.  **Combining and Normalizing Scores:** The scores from various matching methods and text sections are combined and normalized to provide a comprehensive relevance score for each GARD disease found in a grant.

This comprehensive approach allows the script to identify nuanced connections between research projects and rare diseases, which can be valuable for research categorization, funding analysis, and understanding research impact.
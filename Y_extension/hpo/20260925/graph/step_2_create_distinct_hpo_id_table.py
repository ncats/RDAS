
# Create and populate table extension_hpo_distinct_phenotye before running this script.
"""
CREATE TABLE extension_hpo_distinct_phenotye (
    id INT AUTO_INCREMENT PRIMARY KEY,
    hpo_id VARCHAR(50) NOT NULL,     
    hpo_name VARCHAR(255),    
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_hpo_id (hpo_id)
);

INSERT INTO extension_hpo_distinct_phenotye (hpo_id, hpo_name)
SELECT hpo_id, hpo_name
FROM rdas_db.extension_hpo_genes_to_phenotype
GROUP BY hpo_id, hpo_name;
"""
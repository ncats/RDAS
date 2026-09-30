-- Gene source file:
-- Y_extension/RARe-SOURCE/20260929/data/RARe-SOURCE-Browse-Genes-09-29-2026.csv
--
-- CSV-to-table column mapping:
--   Gene Information         -> gene_symbol
--   Description              -> description
--   Associated Rare Diseases -> associated_rare_diseases
--   Gene Annotations         -> gene_annotations
--   Gene IDs                 -> gene_id
--   Ensembl Gene ID          -> ensembl_gene_id
--   HGNC ID                  -> hgnc_id
--   Links                    -> links
--
-- associated_rare_diseases remains one raw text value because the CSV stores
-- multiple disease-name/id entries in that column, separated by semicolons.
-- Identifier columns are VARCHAR values because they are external identifiers,
-- and Ensembl Gene ID can contain more than one semicolon-separated value.

CREATE TABLE IF NOT EXISTS `rdas_db`.`extension_rare_source_genes` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `gene_symbol` VARCHAR(50) NOT NULL,
    `description` VARCHAR(255) NOT NULL,
    `associated_rare_diseases` TEXT NOT NULL,
    `gene_annotations` VARCHAR(255) NOT NULL,
    `gene_id` VARCHAR(20) DEFAULT NULL,
    `ensembl_gene_id` VARCHAR(255) DEFAULT NULL,
    `hgnc_id` VARCHAR(20) DEFAULT NULL,
    `links` VARCHAR(255) NOT NULL,
    `created` DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    KEY `ext_rsg_gene_symbol` (`gene_symbol`),
    KEY `ext_rsg_gene_id` (`gene_id`),
    KEY `ext_rsg_ensembl_gene_id` (`ensembl_gene_id`),
    KEY `ext_rsg_hgnc_id` (`hgnc_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- Rare-disease source file:
-- Y_extension/RARe-SOURCE/20260929/data/RARe-SOURCE-Browse-Rare-Diseases-09-29-2026.csv
--
-- CSV-to-table column mapping:
--   Rare Disease Name   -> rare_disease_name
--   Disease Aliases     -> disease_aliases
--   Associated Genes    -> associated_genes
--   Disease Annotations -> disease_annotations
--   OMIM                -> omim
--   Orphanet            -> orphanet
--   UMLS                -> umls
--   Mesh                -> mesh
--   ICD10CM             -> icd10cm
--   Gene Descriptions   -> gene_descriptions
--   Links               -> links
--
-- Disease aliases, associated genes, and gene descriptions remain raw text
-- values because the CSV stores multiple entries in these columns. External
-- identifier columns are nullable because the source does not provide every
-- identifier type for every rare disease.

CREATE TABLE IF NOT EXISTS `rdas_db`.`extension_rare_source_diseases` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `rare_disease_name` VARCHAR(255) NOT NULL,
    `disease_aliases` TEXT DEFAULT NULL,
    `associated_genes` TEXT NOT NULL,
    `disease_annotations` VARCHAR(255) NOT NULL,
    `omim` VARCHAR(20) DEFAULT NULL,
    `orphanet` VARCHAR(20) DEFAULT NULL,
    `umls` VARCHAR(20) DEFAULT NULL,
    `mesh` VARCHAR(255) DEFAULT NULL,
    `icd10cm` VARCHAR(50) DEFAULT NULL,
    `gene_descriptions` TEXT NOT NULL,
    `links` VARCHAR(500) NOT NULL,
    `created` DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    KEY `ext_rsd_name` (`rare_disease_name`),
    KEY `ext_rsd_annotations` (`disease_annotations`),
    KEY `ext_rsd_omim` (`omim`),
    KEY `ext_rsd_orphanet` (`orphanet`),
    KEY `ext_rsd_umls` (`umls`),
    KEY `ext_rsd_mesh` (`mesh`),
    KEY `ext_rsd_icd10cm` (`icd10cm`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

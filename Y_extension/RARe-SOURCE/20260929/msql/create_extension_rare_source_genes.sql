-- Source file:
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

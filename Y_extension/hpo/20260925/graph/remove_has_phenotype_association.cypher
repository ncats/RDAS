// Count the existing Gene-to-Phenotype relationships before removing them.
MATCH ()-[relationship:has_phenotype_association]->()
RETURN count(relationship) AS relationshipsBeforeRemoval;

// Delete only has_phenotype_association relationships. Gene, Phenotype, GARD,
// and all other nodes and relationships remain unchanged.
MATCH ()-[relationship:has_phenotype_association]->()
DELETE relationship;

// Confirm that no has_phenotype_association relationships remain.
MATCH ()-[relationship:has_phenotype_association]->()
RETURN count(relationship) AS relationshipsAfterRemoval;

# Building up the graph is done in a series of steps executed one after the
# other. Each step is a query that typically adds some kind of node and/or
# connection, although some steps do other things.

# Breaking the process up into steps is not only logical and easier to process,
# but also necessary, because LOAD CSV limits us to loading in data from only
# one CSV file at a time.

# Each step is specified by 3 (optionally 4) values:
# - The "description" is just a short comment about what the step does. This be
#   logged to the terminal along with other info as the program runs for
#   diagnostic purposes.
# - the "data_folder" is the folder in which the CSV file(s) required by that
#   particular step are contained (for example, it could be "projects", in which
#   it'll refer to the CSV files RD_PROJECTS_XXXX.csv).
# - the "constraint" is an *optional* parameter that can be used to specify that
#   certain properties must be unique or other constraints.
# - the "query" is the actual cypher query that takes data from the CSV file and
#   creates nodes and relationships. All queries implicitly start with a `LOAD
#   CSV WITH HEADERS FROM $path AS data`, where $path is the path to a
#   particular file in the specified data_folder. So queries will have access to
#   CSV data via the `data` cypher variable.


steps: list = [

	# First, we add new CoreProject nodes for all CORE_PROJECT_NUM (column in
	# CSV files found in "projects" directory) that are not already associated
	# with an existing node. Note the schema specifies that CoreProject nodes
	# have an rd_total_cost property, which is not set here. It will be updated
	# in a later step.
	{
		"description": "Adding CoreProject nodes",
		"data_folder": "projects",
		"constraint":
			"""
			CREATE CONSTRAINT unique_core_num IF NOT EXISTS ON (c:CoreProject)
			ASSERT c.core_project_num IS UNIQUE
			""",
		"query":
			"""
			FOREACH (
				_ IN
				CASE WHEN data.CORE_PROJECT_NUM IS NOT NULL
				THEN [1] ELSE [] END |
			MERGE (:CoreProject {core_project_num: data.CORE_PROJECT_NUM})
			)
			"""
	},

	# For the next few steps, order doesn't matter too much. We build out the left
	# side of the schema diagram first, starting with Agent nodes. This next step
	# adds an Agent node with a particular name (if not already exists) and links
	# it back to the CoreProject node.
	{
		"description": "Adding Agent nodes and FUNDED_BY relationships",
		"data_folder": "projects",
		"constraint": 
			"""
			CREATE CONSTRAINT unique_agent_name IF NOT EXISTS ON (a:Agent)
			ASSERT a.name IS UNIQUE
			""",
		"query":
			"""
			MERGE (a:Agent {name: data.IC_NAME})
			WITH a, data
			MATCH (c:CoreProject {core_project_num: data.CORE_PROJECT_NUM})
			MERGE (a)<-[:FUNDED_BY]-(c)
			"""
	},

	# Similar thing, but now with ClinicalStudies nodes.
	{
		"description": "Adding ClinicalStudies nodes and STUDIED relationships",
		"data_folder": "clinical_studies",
		"query":
			"""
			MERGE (n:ClinicalStudies {
				gov_id: data.`ClinicalTrials.gov ID`,
				title: data.Study,
				status: data.`Study Status`})
			WITH n, data
			MATCH (c:CoreProject {core_project_num: data.CORE_PROJECT_NUM})
			MERGE (n)<-[:STUDIED]-(c)
			"""
	},

	# Again similar, but for Patent nodes. Note data.PATENT_ID is casted to an
	# integer in compliance with the specification.
	{
		"description": "Adding Patent nodes and PATENTED relationships",
		"data_folder": "patents",
		"query":
			"""
			MERGE (p:Patent {
				id: data.PATENT_ID,
				title: data.PATENT_TITLE,
				org_name: data.PATENT_ORG_NAME})
			WITH p, data
			MATCH (c:CoreProject {core_project_num: data.CORE_PROJECT_NUM})
			MERGE (p)<-[:PATENTED]-(c)
			"""
	},

	# For the remainder of the left side of the schema, we'll add Journal nodes
	# first (since they have a single unique id that makes them easy to refer
	# back to)...
	{
		"description": "Adding Journal nodes",
		"data_folder": "publications",
		"constraint":
			"""
			CREATE CONSTRAINT unique_journal_title IF NOT EXISTS ON (j:Journal)
			ASSERT j.title IS UNIQUE
			""",
		"query":
			"""
			MERGE (:Journal {title: data.JOURNAL_TITLE})
			"""
	},

	# ...then we'll go back and add the Publication nodes and link them to the
	# Journal nodes we just created. Note after this step they still aren't
	# connected to the CoreProject nodes, so we aren't quite done with the left
	# side of the schema yet
	# WARNING: Splitting on '; ' seems kind of brittle, could be an issue later on
	{
		"description": "Adding Publication nodes",
		"data_folder": "publications",
		"constraint":
			"""
			CREATE CONSTRAINT unique_pmid_date IF NOT EXISTS ON (n:Publication)
			ASSERT (n.pmid, n.date) IS UNIQUE
			""",
		"query":
			"""
			MERGE (n:Publication {
				country: data.COUNTRY,
				language: data.LANG,
				pmid: toInteger(data.PMID),
				date: data.PUB_DATE})
				ON CREATE SET
					n.affiliation = data.AFFILIATION,
					n.pmc_id = toInteger(data.PMC_ID),
					n.authors = split(data.AUTHOR_LIST, '; '),
					n.title = data.PUB_TITLE
				ON MATCH SET
					n.affiliation = coalesce(data.AFFILIATION, n.affiliation),
					n.pmc_id = coalesce(toInteger(data.PMC_ID), n.pmc_id),
					n.authors = coalesce(split(data.AUTHOR_LIST, '; '), n.authors),
					n.title = coalesce(data.PUB_TITLE, n.title)
			"""
	},
	# Go back and link up Publication nodes to Journal nodes. Even though it uses
	# the same data_folder, doing this in a separate step from the previous
	# results in huge performance improvements.
	{
		"description": "Adding PUBLISHED_IN relationships",
		"data_folder": "publications",
		"query":
			"""
			MATCH (j:Journal {title: data.JOURNAL_TITLE})
			MATCH (n:Publication {pmid: toInteger(data.PMID), date: data.PUB_DATE})
			MERGE (n)-[p:PUBLISHED_IN]->(j)
				ON CREATE SET
					p.issue = data.JOURNAL_ISSUE,
					p.volume = data.JOURNAL_VOLUME,
					p.page = data.PAGE_NUMBER
				ON MATCH SET
					p.issue = data.JOURNAL_ISSUE,
					p.volume = data.JOURNAL_VOLUME,
					p.page = data.PAGE_NUMBER
			"""
	},	

	# We have to make a separate step for connecting Publication to CoreProject
	# because the info for that is in different CSV files, the link_tables
	# directory.
	{
		"description": "Adding PUBLISHED relationships",
		"data_folder": "link_tables",
		"constraint":
			"""
			CREATE INDEX pmid_index IF NOT EXISTS FOR (p:Publication) ON (p.pmid)
			""",
		"query":
			"""
			MATCH (p:Publication {pmid: toInteger(data.PMID)})
			MATCH (c:CoreProject {core_project_num: data.CORE_PROJECT_NUM})
			MERGE (c)-[:PUBLISHED]->(p)
			"""
	},

	# Now we begin working on the right side of the schema. First we start by
	# adding Project nodes and linking them back to CoreProject nodes. Note that
	# it is in this step where we dynamically update the CoreProject's
	# rd_total_cost.
	# We aren't done with the Project nodes after this step. We need to add their
	# abstracts, but those are in a separate file, so will be done in the next
	# step.
	{
		"description": "Adding Project nodes and UNDER_CORE relationships",
		"data_folder": "projects",
		"constraint":
			"""
			CREATE CONSTRAINT unique_application_id_fy IF NOT EXISTS ON (p:Project)
			ASSERT (p.application_id, p.funding_year) IS UNIQUE
			""",
		"query":
			"""
			WITH [x in split(data.PROJECT_TERMS, ';') WHERE x <> "" | x]
				AS terms, data
			MERGE (p:Project {
				application_id: toInteger(data.APPLICATION_ID),
				funding_year: toInteger(data.FY)
				})
				ON CREATE SET
					p.phr = data.PHR,
					p.terms = terms,
					p.total_cost = toInteger(data.TOTAL_COST),
					p.title = data.PROJECT_TITLE,
					p.application_type = toInteger(data.APPLICATION_TYPE),
					p.subproject_id = toInteger(data.SUBPROJECT_ID)
				ON MATCH SET
					p.phr = coalesce(data.PHR, p.phr),
					p.terms = coalesce(terms, p.terms),
					p.total_cost = coalesce(toInteger(data.TOTAL_COST), p.total_cost),
					p.title = coalesce(data.PROJECT_TITLE),
					p.application_type =
						coalesce(toInteger(data.APPLICATION_TYPE), p.application_type),
					p.subproject_id =
						coalesce(toInteger(data.SUBPROJECT_ID), p.subproject_id)
			WITH p, data
			MATCH (c:CoreProject {core_project_num: data.CORE_PROJECT_NUM})
			MERGE (p)-[:UNDER_CORE]->(c)
				ON CREATE SET c.rd_total_cost = coalesce(c.rd_total_cost, 0) + p.total_cost
			"""
	},

	# Now time to load from the abstracts CSV files and go back in and add the
	# `abstract` property to Project nodes.
	{
		"description": "Adding abstract property to Project nodes",
		"data_folder": "abstracts",
		"constraint":
			"""
			CREATE INDEX appid_index IF NOT EXISTS FOR (p:Project) ON (p.application_id)
			""",
		"query":
			"""
			MATCH (p:Project {application_id: toInteger(data.APPLICATION_ID)})
			SET p.abstract = data.ABSTRACT_TEXT
			"""
	},

	# Similar to how we handled Journal and Publication nodes, for DiseaseCategory
	# and Disease nodes, we'll start from the outside and work our way in, first
	# adding all the DiseaseCategory nodes. As of now the only DiseaseCategory
	# is TBD, but TODO I'm assuming by the column name "CATEGORIES" that it
	# can actually be a list of categories on each row, delimited by ; (can of
	# course be changed).
	{
		"description": "Adding DiseaseCategory nodes",
		"data_folder": "disease",
		"constraint":
			"""
			CREATE CONSTRAINT unique_disease_category IF NOT EXISTS ON (d:DiseaseCategory)
			ASSERT d.name IS UNIQUE
			""",
		"query":
			"""
			UNWIND split(data.CATEGORIES, ';') AS c
			MERGE (:DiseaseCategory {name: c})
			"""
	},

	# Add Disease nodes and connect to associated DiseaseCategory nodes as well as
	# back to the Project node.
	# See Annotation nodes comment for what the WITH statement is doing
	{
		"description": "Adding Disease nodes and IN_CLASS, RESEARCHED_BY relationships",
		"data_folder": "disease",
		"query":
			"""
			WITH [x in split(substring(data.SYNONYMS, 2, size(data.SYNONYMS) - 4), "', '")
				WHERE size(x) > 0 | x] as snns, data
			MERGE (d:Disease {
				name: data.NAME,
				gard_id: data.GARD_ID,
				is_rare: data.IS_RARE = 'TRUE'})
				ON CREATE SET
					d.synonyms = snns
				ON MATCH SET
					d.synonyms = snns
			WITH d, data
			UNWIND split(data.CATEGORIES, ';') AS x
			MATCH (c:DiseaseCategory {name: x})
			MATCH (r:Project {application_id: toInteger(data.APPLICATION_ID)})
			MERGE (r)<-[:RESEARCHED_BY]-(d)-[:IN_CLASS]->(c)
			"""
	},

	# Adding PrincipalInvestigator nodes. Code here is a little complicated;
	# PrincipalInvestigator IDs and names are stored in two separate lists. Some
	# IDs and names are followed by (contact). We need to, based on order,
	# associate IDs with names and potentially remove the (contact) at the end.
	# Removing (contact) is done with trim(split(string, '(')[0]), i.e. splitting
	# on (, getting the zeroth element (if the original string didn't have a
	# parentheses, this will just be the whole original string), and trimming it.
	# Zipping the two lists up and getting back a list of [ID, name pairs] is done
	# with a list comprehension, the result of which we immediately unwind into
	# separate nodes.
	{
		"description": "Adding PrincipalInvestigator nodes and INVESTIGATED relationships",
		"data_folder": "projects",
		"constraint":
			"""
			CREATE INDEX pi_id_index IF NOT EXISTS FOR (p:PrincipalInvestigator) ON (p.pi_id)
			""",
		"query":
			"""
			WITH split(data.PI_IDS, ';') as ids,
			     split(data.PI_NAMEs, ';') as names, data
			UNWIND [x in range(0, coalesce(size(ids) - 1, -1)) |
				[trim(split(ids[x], '(')[0]), trim(split(names[x], '(')[0])]
			] as pi_data
			MERGE (p:PrincipalInvestigator {
				pi_id: pi_data[0],
				pi_name: pi_data[1],
				org_state: coalesce(data.ORG_STATE, ""),
				org_name: coalesce(data.ORG_NAME, "")})
			WITH p, data
			MATCH (r:Project {application_id: toInteger(data.APPLICATION_ID)})
			MERGE (r)<-[:INVESTIGATED]-(p)
			"""
	},

	# Adding Annotation nodes. This below is probably the hairiest
	# cypher query of the whole process, because SEMANTIC_TYPES
	# and SEMANTIC_TYPES_NAMES are stored in a string formatted as
	# "['a', 'b', 'c']" and we need to parse that into a list of strings
	# ["a", "b", "c"]. We can't simply split on commas, since some elements have
	# commas in them
	# (e.g. "['Amino Acid, Peptide, or Protein', 'Immunologic Factor']")
	# So instead we first use substring to remove the square brackets, split on
	# single quote, and only take the elements that are non-empty and not ", "
	# (the comma delimiter). It's kind of suspicious but not sure how else to
	# do it purely with Cypher.
	# At the end, we add a *blank* :ANNOTATED conection between the project node
	# and the Annotation node. We add the source property specified by the
	# spec in the next step, since data for that is in a separate CSV file, but
	# it's much simpler to add a blank connection here to prepare for that.
	{
		"description": "Adding Annotation nodes and ANNOTATED relationships",
		"data_folder": "annotation_umls",
		"constraint":
			"""
			CREATE INDEX anno_cui_index IF NOT EXISTS FOR (a:Annotation) ON (a.umls_cui)
			""",
		"query":
			"""
			WITH [x in split(substring(data.SEMANTIC_TYPES, 1, size(data.SEMANTIC_TYPES) - 2), \"'\")
				WHERE x <> \", \" AND size(x) > 0 | x] as types, data
			WITH [x in split(substring(data.SEMANTIC_TYPES_NAMES, 1, size(data.SEMANTIC_TYPES_NAMES) - 2), \"'\")
				WHERE x <> \", \" AND size(x) > 0 | x] as names, types, data
			MERGE (a:Annotation {
				umls_cui: data.UMLS_CUI,
				umls_concept: data.UMLS_CONCEPT,
				semantic_types: types,
				semantic_types_names: names})
			WITH a, data
			MATCH (r:Project {application_id: toInteger(data.APPLICATION_ID)})
			MERGE (r)-[:ANNOTATED]->(a)
			"""
	},

	# Last step: adding the `source` property to :ANNOTATED connections.
	{
		"description": "Adding ANNOTATED relationships",
		"data_folder": "annotation_source",
		"query":
			"""
			MATCH (:Project {application_id: toInteger(data.APPLICATION_ID)})
				-[a:ANNOTATED]->(:Annotation)
			SET a.source = data.SOURCE
			"""
	}
]

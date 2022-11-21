import re
from functools import reduce
from typing import Callable, Any
from neo4j import Result, Session, Transaction, Record
from neo4j.graph import Node
from dataclasses import dataclass


@dataclass(frozen=True)
class AnnotationData:
	"""
	An ``AnnotationData`` object is a simple struct containing all the data needed
	to create a new ``PubtatorAnnotation`` node.
	"""
	infons_identifier: str
	infons_type: str
	text: list[str]
	type: str

	def __getitem__(self, item):
		"""
		Retrieve a property from AnnotationData using the square brackets operator,
		e.g. annotation["infons_type"] instead of annotation.infons_type.
		This is just for convenience purposes.
		:param item: one of "infons_type", "infons_identifier", "text", or "type"
		:return: the corresponding attribute
		"""
		return getattr(self, item)


class QueryBuilder:
	"""
		A simple fluent interface for building MATCH Cypher queries that involve
		chaining together multiple predicates following the WHERE clause. For now,
		this class is not meant for general-purpose use, but it is sufficient for
		the needs of the ``AnnotationManager``.

		See the ``AnnotationManager`` class, particularly the predicate functions,
		for examples on how to use the ``QueryBuilder`` class.

		:ivar predicates: the list of predicates that the query will have.
		:ivar args: a dictionary mapping Cypher variables ($etc) to their values, to
			be returned alongside the query string.
		:ivar closing: the closing clause to the query, typically just "RETURN n"
			or "RETURN n LIMIT x". This will be appended following the predicates.
		:ivar base: The base AnnotationData to be compared against when MATCHing for
			other nodes in the database.
	"""

	predicates: list[str]
	args: dict[str, Any]
	closing: str
	base: AnnotationData

	def __init__(self, base: AnnotationData):
		self.predicates = []
		self.args = {}
		self.base = base
		self.closing = " RETURN n"  # note the space before return - necessary

	def same_infons_type(self) -> "QueryBuilder":
		"""
		Make the query only match nodes with the same ``infons_type`` as the base
		``AnnotationData``.

		:return: this QueryBuilder, for chaining purposes
		"""

		self.predicates.append("n.infons_type = $infons_type")
		self.args["infons_type"] = self.base.infons_type
		return self

	def same_infons_identifier(self) -> "QueryBuilder":
		"""
		Make the query only match nodes with the same ``infons_identifier`` as the
		base ``AnnotationData``.

		:return: this QueryBuilder, for chaining purposes
		"""

		self.predicates.append("n.infons_identifier = $infons_identifier")
		self.args["infons_identifier"] = self.base.infons_identifier
		return self

	def empty_infons_identifier(self) -> "QueryBuilder":
		"""
		Make the query only match nodes with an empty ``infons_identifier`` - i.e.
		one of None, "", or "-". These nodes lacking infons_identifiers are
		especially prevalent with Disease and Chemical annotations.

		:return: this QueryBuilder, for chaining purposes
		"""
		self.predicates.append(
			"(n.infons_identifier IS NULL OR n.infons_identifier IN ['', '-'])")
		return self

	def text_exact_match(self) -> "QueryBuilder":
		"""
		Make the query only match nodes that have at least one element in their
		``text`` list that exactly (case-sensitive) matches one of the elements in
		the base node. e.g. used for Gene nodes where case matters.

		:return: this QueryBuilder, for chaining purposes
		"""

		self.predicates.append("any(x IN $tem WHERE x IN n.text)")
		self.args["tem"] = self.base.text
		return self

	def text_insensitive_match(self) -> "QueryBuilder":
		"""
		Similar to ``text_exact_match``, except matches case-insensitively.
		:return: this QueryBuilder, for chaining purposes
		"""

		self.predicates.append(
			"any(x IN $tim WHERE toLower(x) IN [i IN n.text | toLower(i)])")
		self.args["tim"] = self.base.text
		return self

	def text_starts_with_any(self, prefixes) -> "QueryBuilder":
		"""
		Similar to ``text_insensitive_match``, except also matches as long as
		the ``text`` in the node STARTS WITH any of the provided prefixes.
		This is only used for the Chemical case for now, where we want to match
		all potential ions of a certain atom or molecule.

		:return: this QueryBuilder, for chaining purposes
		"""

		self.predicates.append(
			"any(x IN $tswa WHERE "
			"any(y IN n.text WHERE toLower(y) STARTS WITH toLower(x)))")
		self.args["tswa"] = prefixes
		return self

	def limit(self, n: int) -> "QueryBuilder":
		"""
		Limit the number of nodes returned from the query. This is primarily useful
		for testing purposes, but in practice we would want to be sure we are
		merging *all* duplicate nodes, not just a limited number of them.

		:return: this QueryBuilder, for chaining purposes
		"""

		# Note the leading space, so closing becomes "RETURN n LIMIT $n" instead of
		# "RETURN nLIMIT $n".
		self.closing += " LIMIT $n"
		self.args[str(n)] = n
		return self

	def to_query(self) -> (str, dict[str, Any]):
		"""
		Convert this QueryBuilder into the actual query string and kwargs to be
		passed to ``tx.run`` alongside it.

		:return: A tuple in which the first element is the query string, and the
			second element is the kwargs that define the variables in the query.
		"""

		# Join all predicates together with `AND` and cap it off with the closing
		# string. (These concatenations are safe, as no user-defined variables can
		# make it into these operations, so there should be no threat of injection)
		query = "MATCH (n:PubtatorAnnotation) WHERE "
		query += " AND ".join(self.predicates)
		query += self.closing
		return query, self.args


class AnnotationManager:
	"""
	The ``AnnotationManager`` acts as a general proxy over direct Cypher queries
	when dealing with ``PubtatorAnnotation`` nodes. It handles checking for and
	removing duplicates, creating new nodes and relationships, and pruning
	existing connections.

	In general, when dealing with ``PubtatorAnnotation`` nodes, this proxy should
	be used rather than direct Cypher queries. This will ensure duplicate nodes
	do not get created, and reduce the need for whole-scale database cleaning in
	the future.

	:ivar session: The Neo4j Session connected to the database that this
		AnnotationManager is to be a proxy over.
	:ivar predicates: A map from infons_types to handler functions, as we
		determine if nodes are duplicates in different manner based on their
		infons_type.
	"""

	session: Session
	predicates: dict[str, Callable[[AnnotationData], list[Node]]]

	def __init__(self, session: Session) -> None:
		self.session = session

		# This dict can safely be modified later on, if rules need to be tweaked
		# or new infons_types are added.
		# The handler for the "default" case is run whenever the given
		# AnnotationData has an infons_type that is not recognized. In this case, we
		# should always use the strictest predicate, __same_it_ii_txt.
		self.predicates = {
			"CellLine": self.__same_it_ii,
			"Chemical": self.__chemical_handler,
			"Disease": self.__disease_handler,
			"Gene": self.__same_it_ii_txt,
			"Genus": self.__same_it_ii,
			"Mutation": self.__same_it_ii,
			"Species": self.__same_it_ii,
			"Strain": self.__same_it_ii_txt,
			"default": self.__same_it_ii_txt
		}

	def get_duplicates(self, annotation: AnnotationData) -> list[Node]:
		"""
		Get all nodes from the database that would be duplicates with the given
		AnnotationData

		:param annotation: the potential PubtatorAnnotation to compare against
		:return: all Nodes that are duplicates with the given AnnotationData
		"""

		try:
			# Call the corresponding predicate for the node's infons_type
			return self.predicates[annotation.infons_type](annotation)
		except KeyError:
			# If we are here, for some reason the annotation is not one of the 8 cases
			# we know about. We use the default predicate, which should be as strict
			# as possible (__same_it_ii_txt).
			return self.predicates["default"](annotation)

	def __same_it_ii(self, annotation: AnnotationData) -> list[Node]:
		"""
		Get all nodes that have the same ``infons_type`` and ``infons_identifier``
		as the given ``AnnotationData``.

		:param annotation: The AnnotationData for which to find all nodes in the
			database with the same infons_type and infons_identifier
		:return: A list of PubtatorAnnotation Nodes that have the same infons_type
			and infons_identifier
		"""

		# Comparing against this annotation, we want all nodes that have
		# - The same infons_type
		# - The same infons_identifier
		qb = QueryBuilder(annotation) \
			.same_infons_type() \
			.same_infons_identifier() \
			.to_query()

		# Run the query generated by the QueryBuilder and return all nodes that
		# satisfy the query
		return self.get_single_nodes(*qb)

	def __same_it_ii_txt(self, annotation: AnnotationData) -> list[Node]:
		"""
		Get all nodes that have the same ``infons_type``, ``infons_identifier``, and
		``text`` (exact, case-sensitive match). This is currently the strictest
		predicate, so it is suitable for cases we are not so sure about.

		:param annotation: The AnnotationData for which to find all nodes in the
			database with the same infons_type, infons_identifier, and text
		:return: A list of PubtatorAnnotation Nodes that have the same infons_type,
			infons_identifier, and text
		"""

		qb = QueryBuilder(annotation) \
			.same_infons_type() \
			.same_infons_identifier() \
			.text_exact_match() \
			.to_query()

		return self.get_single_nodes(*qb)

	def __disease_handler(self, annotation: AnnotationData) -> list[Node]:
		"""
		For Disease annotations, we simply require same infons_type,
		same infons_identifier, and text inexact match. Therefore, this predicate
		can be viewed as a slightly looser version of __same_it_ii_txt. We also
		handle cases where the disease node has an empty infons_identifier, in which
		case we require a strictly matching text to be safe.

		:param annotation: The Disease annotation to compare with for duplicates
		:return: A list of PubtatorAnnotation nodes that are duplicates.
		"""

		if annotation.infons_identifier in [None, "", "-"]:
			# Unknown infons_identifier, so just text_exact_match is enough
			qb = QueryBuilder(annotation) \
				.same_infons_type() \
				.text_exact_match() \
				.to_query()
			return self.get_single_nodes(*qb)
		else:
			# We have an infons_identifier to enforce. Text can now just match
			# case-insensitively.
			qb1 = QueryBuilder(annotation) \
				.same_infons_type() \
				.same_infons_identifier() \
				.text_insensitive_match() \
				.to_query()

			# We also match all the nodes with empty infons_identifiers as well.
			qb2 = QueryBuilder(annotation) \
				.same_infons_type() \
				.empty_infons_identifier() \
				.text_insensitive_match() \
				.to_query()

			return self.get_single_nodes(*qb1) + self.get_single_nodes(*qb2)

	def __chemical_handler(self, annotation: AnnotationData) -> list[Node]:
		"""
		The Chemical infons_type is a bit more complicated in that we also want to
		combine all cations and anions of a certain chemical into the same node.
		Thus, this predicate is not reusable for other cases, as it uses
		chemical-specific regexes as part of its logic.

		:param annotation: The Chemical annotation to search for duplicates for
		:return: All duplicate PubtatorAnnotation nodes.
		"""

		# We first extract the major name from the chemical using a regex.
		# For example, running this regex on "Ca2+",
		# If there is a match, the 0th capture group is the whole match, "Ca2+"
		# the 1st capture group is what we want, the major name "Ca"
		# the 2nd capture group is the ion, "2+"
		# the 3rd capture group is trash data.
		pattern = re.compile(
			"([A-Z0-9]+?) ?"  # match chemical + optional space
			"(\\(I\\)|\\(II\\)|\\(III\\)|\\(IV\\)"  # roman numeral notation
			"|\\([0-9]?[+-]\\)"  # anion/cation notation with parens
			"|[0-9]?[+-])"  # anion/cation notation without parens
			"($|\\s+$)", re.IGNORECASE)  # must occur at end of str

		def to_major_set(lst: list[str]) -> set[str]:
			# A helper function to convert a list of chemicals to their major name,
			# storing the resulting major names in a set instead of a list.
			# Set helps us avoid duplicates and also makes it easy for intersection
			# later

			result: set[str] = set()
			for item in lst:
				# We standardize everything to lowercase, as we don't care about case
				# sensitivity when dealing with chemicals.
				lower = item.lower()
				if match := pattern.match(lower.strip()):
					# It is some cation/anion, so we add only the 1st capture group
					result.add(match.group(1))
				else:
					# It's not an ion based on our regex, so we add the whole text
					result.add(lower)
			return result

		this_set = to_major_set(annotation.text)

		qb = QueryBuilder(annotation) \
			.same_infons_type() \
			.text_starts_with_any(list(this_set)) \
			.to_query()

		nodes = self.get_single_nodes(*qb)

		# If the infons_identifier is non-empty, make sure all returned nodes have
		# either an empty infons_identifier or a matching infons_identifier
		if annotation.infons_identifier not in ["", "-", None]:
			nodes = list(filter(
				lambda node: (
								node["infons_identifier"] == annotation.infons_identifier or
								node["infons_identifier"] in ["", "-", None]),
				nodes))

		# Make sure all returned nodes have some major name in common with the given
		# AnnotationData.
		return list(filter(
			lambda node: len(this_set & to_major_set(node["text"])) > 0,
			nodes))

	def get_single_nodes(self, query: str, params: dict[str, Any]) -> list[Node]:
		"""
		The AnnotationManager makes use of many queries that are simply MATCH
		statements that return a list of individual nodes. This function simplifies
		the process of running the query and unpacking the Result into a list of
		Nodes.

		Note that the query itself does not have to return a singular node, it is
		just assumed each Record in the Result is just a single node (i.e. no
		connections, extra columns, etc).

		:param query: The query to execute. The query should be read-only and end
			with something single like "RETURN n" (e.g. not "RETURN (n)-[p]->(q)")
		:param params: The parameters that will be substituted in the query.
		:return: A list of nodes that resulted from the query.
		"""

		def do_query(tx: Transaction):
			query_result: Result = tx.run(query, **params)

			# Essentially we are doing [record.values()[0] for record in query_result],
			# but it's better to throw a more descriptive error message if we get back
			# a list of length != 1.
			result_nodes: list[Node] = []
			for record in query_result:
				record_vals = record.values()
				if len(record_vals) != 1:
					raise RuntimeWarning("get_single_nodes expects records of size 1.")
				else:
					result_nodes.append(record_vals[0])
			return result_nodes

		return self.session.read_transaction(do_query)

	def __write_query(self, query: str, params: dict[str, Any]) -> list[Record]:
		"""
		A small convenience function for running write transactions against the
		database.

		:param query: The query to execute
		:param params: The parameters to substitute in the query
		:return: The returned records from the query
		"""
		return self.session.write_transaction(
			lambda tx: [record for record in tx.run(query, **params)])

	def create_or_merge(
					self,
					annotation: AnnotationData,
					duplicates: list[Node] = None) -> Node:
		"""
		If the ``duplicates`` parameter is an empty list, a new node is created in
		the database from the given ``AnnotationData``. Otherwise, we merge the
		``AnnotationData`` and all of the ``duplicates`` together, and return that
		final merged node.

		:param annotation: The AnnotationData to be added to the database
		:param duplicates: A list of duplicates, potentially None. This is useful
			if you want to separately query for duplicates and do something with those
			duplicate nodes beforehand, and avoid redundantly searching for duplicates
			again.
		:return: If duplicates is empty, return the newly created node. Else, return
			the final node that results from merging the AnnotationData and duplicates
			together.
		"""

		if duplicates is None:
			duplicates = self.get_duplicates(annotation)

		# Implementation detail: We aren't really creating _or_ merging, we are
		# always creating a new node first, _and then_ maybe merging.
		# This just simplifies our code for merging later on, as we only have to
		# deal with node-node cases (and not something like AnnotationData-node
		# cases). Also, just creating a single new node temporarily is pretty cheap.

		base_node: Node = self.__write_query(
			"CREATE (n:PubtatorAnnotation {"
			"infons_identifier: $infons_identifier, "
			"infons_type: $infons_type, "
			"text: $text, type: $type}) "
			"RETURN n", annotation.__dict__)[0].values()[0]

		if len(duplicates) > 0:
			# maybe this swap is not necessary, but this prevents us from moving
			# all existing connections to the new node.
			base_node, duplicates[0] = duplicates[0], base_node

		if annotation.infons_type in \
			["Disease", "Chemical", "CellLine", "Genus", "Mutation", "Species"]:
			# For these cases, we do not want to keep alternative `text` that only
			# differs by capitalization (i.e. we don't care about case sensitivity)
			return reduce(self.merge_nodes, duplicates, base_node)
		else:
			# For all other cases, keep all alternatives. This is the safer option.
			return reduce(self.merge_sensitive, duplicates, base_node)

	def merge_sensitive(self, base: Node, to_merge: Node) -> Node:
		"""
		Just an alias for ``merge_nodes(base, to_merge, True)``. See ``merge_nodes``
		for further documentation.
		"""
		return self.merge_nodes(base, to_merge, True)

	def merge_nodes(self, base: Node, to_merge: Node, case_sensitive=False) \
		-> Node:
		"""
		Merge two nodes together, more specifically, merge the ``to_merge`` node
		into the ``base`` node (only the ``base`` node will be maintained,
		``to_merge`` will be deleted once its data is moved to ``base``).

		:param base: The node to be updated with to_merge's data and relationships.
		:param to_merge: The node to merge into the base node. Will be deleted.
		:param case_sensitive: Whether or not to be case-sensitive when deciding
			whether or not to keep text from to_merge. Default False.
		:return: The base node, after merging of data has occurred.
		"""

		# Here we do one last sanity check that we aren't merging two conflicting
		# non-empty infons_identifiers or infons_types
		empty_infons = ["-", "", None]
		if base["infons_identifier"] != to_merge["infons_identifier"] \
			and to_merge["infons_identifier"] not in empty_infons \
			and base["infons_identifier"] not in empty_infons:
			raise RuntimeError(
				f"Cannot merge conflicting infons_identifiers "
				f"{base['infons_identifier']}, {to_merge['infons_identifier']} together")
		elif base["infons_type"] != to_merge["infons_type"]:
			raise RuntimeError("Cannot merge conflicting infons_types together")

		if base["infons_identifier"] in empty_infons:
			# If the base node's infons_identifier is empty, but to_merge has the
			# infons_identifier we need, keep it.
			# Execute an additional query to update the base node's infons_identifier.
			self.__write_query(
				"MATCH (n:PubtatorAnnotation) WHERE id(n) = $id "
				"SET n.infons_identifier = $infons_identifier",
				{"id": base.id, "infons_identifier": to_merge["infons_identifier"]})

		if case_sensitive:
			# If we are caring about case, we can simply do a set union of the two
			# nodes' `text` properties.
			text = list(set(base["text"]) | set(to_merge["text"]))
		else:
			# Similar to above, but more logic required to be case-insensitive.
			lowercased = set(x.lower() for x in base["text"])
			text = base["text"]
			for i in to_merge["text"]:
				if i.lower() in lowercased:
					continue
				else:
					lowercased.add(i.lower())
					text.append(i)

		if base["type"] != to_merge["type"]:
			# If they are different in any way (covers all cases including
			# "title" vs "abstract", "title and abstract" vs "abstract", etc.),
			# We want to set the `type` field to "title and abstract"
			title_abstract = "title and abstract"
		else:
			# If they are the same, just keep the old `type` field.
			title_abstract = base["type"]

		# Update the text and type fields of the base node.
		self.__write_query(
			"MATCH (n:PubtatorAnnotation) WHERE id(n) = $id "
			"SET n.text = $text, n.type = $type",
			{"id": base.id, "text": text, "type": title_abstract})

		# Transfer all connections from to_merge to base
		self.__write_query(
			"MATCH (a)<-[:ANNOTATION_FOR]-(o), (n) "
			"WHERE id(o) = $old_id AND id(n) = $new_id "
			"MERGE (a)<-[:ANNOTATION_FOR]-(n)",
			{"old_id": to_merge.id, "new_id": base.id})

		# Delete the old to_merge node from the database
		self.__write_query(
			"MATCH (o) WHERE id(o) = $id DETACH DELETE o",
			{"id": to_merge.id})

		return self.get_single_nodes(
			"MATCH (n) WHERE id(n) = $id RETURN n",
			{"id": base.id})[0]

	def prune_article_rels(self, article_id: int) -> None:
		"""
		Given the ID for an Article node, we delete all duplicate connections to
		Gene PubtatorAnnotation nodes based on whether or not more than one Species
		is mentioned in the Article.

		:param article_id: The ID for the Article node we want to clean connections
		"""

		# Get all the PubtatorAnnotation nodes connected to the Article we
		# wish to clean
		anno_nodes = self.get_single_nodes(
			"MATCH (a:Article)<-[:ANNOTATION_FOR]-(p:PubtatorAnnotation) "
			"WHERE id(a) = $art_id AND p.infons_type IN ['Species', 'Gene'] "
			"RETURN p",
			{"art_id": article_id})

		# If the Article has 0 or more than 1 Species PubtatorAnnotation, we don't
		# want to handle this case, so return without doing anything
		if len(list(filter(lambda a: a.infons_type == "Species", anno_nodes))) != 1:
			return

		gene_nodes = list(filter(lambda a: a.infons_type == "Gene", anno_nodes))

		# If the Article doesn't have plural number of Gene nodes, there is nothing
		# to be done here, so return
		if len(gene_nodes) <= 1:
			return

		# We initialize a dictionary of nodes we've already seen. This allows us
		# to skip over relationships we've already removed in later iterations of
		# the below for loop.
		seen = {node.id: False for node in gene_nodes}

		for n1 in gene_nodes:

			# If we've already seen (and removed this duplicate) node, skip. Otherwise
			# mark the node as seen
			if seen[n1.id]:
				continue
			seen[n1.id] = True

			for n2 in gene_nodes:
				if seen[n2.id]:
					continue

				# We convert the text property of both n1 and n2 to a lowercase set...
				n1lower = set(item.to_lower() for item in n1["text"])
				n2lower = set(item.to_lower() for item in n2["text"])

				# ...and then check if their intersection is non-empty.
				if len(n1lower & n2lower) > 0:
					# TODO IMPLEMENT
					print("DUPLICATE ARTICLE CONNECTION:")
					print(n1)
					print(n2)

	def connect_annotation(self, annotation_id: int, article_id: int) -> None:
		"""
		Connect the PubtatorAnnotation node with the given ID to the Article
		node with the given ID with an :ANNOTATION_FOR relationship. This should
		be used instead of manually running a query, as this will ensure no
		redundant connections are added.

		:param annotation_id: The ID of the AnnotationNode to connect from.
		:param article_id: The ID of the Article node to connect to.
		"""

		self.__write_query(
			"MATCH (n), (r) WHERE id(n) = $anno_id, id(r) = $arti_id "
			"MERGE (a)<-[:ANNOTATION_FOR]-(n)",
			{"anno_id": annotation_id, "arti_id": article_id})
		self.prune_article_rels(article_id)

	@staticmethod
	def node_to_annotationdata(node: Node) -> AnnotationData:
		"""
		Convert the given Neo4j Node to an AnnotationData object. This typically
		should not need to be used, unless you are trying to deduplicate existing
		nodes in the database. To turn an AnnotationData into a node, use
		create_or_merge.

		:param node: The Node to convert into an AnnotationData
		:return: The AnnotationData object containing all the info from the Node.
		"""

		return AnnotationData(
			node["infons_identifier"],
			node["infons_type"],
			node["text"],
			node["type"])

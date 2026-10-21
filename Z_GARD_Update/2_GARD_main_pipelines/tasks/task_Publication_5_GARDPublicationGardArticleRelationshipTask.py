"""Create current-GARD Article edges even for previously completed Articles.

Invocation: run_publication_graph_updates() substitutes this class for the shared
GARD/Article relationship task. It executes after source enrichment and the
Article graph tasks have made newly imported Article nodes available.

Example destination state:
    article = {"pubmed_id": 9282845, "is_new": 0}
    mapping = {"gard_id": "GARD:0000016", "search_term": "Coma", "pubmed_id": 9282845}

An Article completed for an earlier disease must still receive the current
GARD association. The instance query selects mappings for GARD:0000016 instead
of requiring article.is_new=1, while retaining the parent's validity checks and
graph MERGE behavior. The Article is not restaged merely to create another edge.

The GARD ID is validated before SQL serialization. Only this instance's SQL is
changed; the shared Alert class constant and regular Alert behavior stay intact."""

import re

from pipelines.pipeline_3_publication.task_publication_graph_8 import NewPublicationGardArticleRelationshipTask
from utils.tools import _escape_sql_literal


class GARDPublicationGardArticleRelationshipTask(NewPublicationGardArticleRelationshipTask):
    """Link current-GARD mappings even when the target Article was imported earlier."""

    __module__ = NewPublicationGardArticleRelationshipTask.__module__

    def __init__(self, gard_id):

        if not re.fullmatch(r"GARD:\d{7}", gard_id):
            raise ValueError(f"Invalid current GARD ID: {gard_id!r}")
        super().__init__()
        '''
        Existing Articles stay is_new=0 to avoid repeating enrichment, graph
        metadata writes, and author extraction for every disease. A new mapping
        still needs its edge. Scope the shared relationship query to the exact
        current GARD ID rather than requiring a newly inserted Article.
        The shared task executes a constant SQL string, so serialize this
        validated internal ID with the existing SQL-literal helper.
        '''
        self.FETCH_NEW_RELATIONS_QUERY = self.FETCH_NEW_RELATIONS_QUERY.replace("WHERE pa.is_new = 1", f"WHERE pgspm.gard_id = {_escape_sql_literal(gard_id)}")

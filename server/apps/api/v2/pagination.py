from rest_framework.pagination import LimitOffsetPagination as Pagination
from .constants import ITEMS_PER_PAGE

class LimitOffsetPagination(Pagination):
  default_limit = ITEMS_PER_PAGE
  max_limit = 1000

  def paginate_queryset(self, queryset, request, view=None):
    if 'no_page' in request.query_params:
      return None
    return super().paginate_queryset(queryset, request, view)

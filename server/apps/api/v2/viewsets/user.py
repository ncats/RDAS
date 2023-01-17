from django.db.models.expressions import Value
from rest_framework.renderers import BrowsableAPIRenderer

from server.apps.api.v2.serializers.profile import FullNameSerializer, UserSerializer, EditableUserSerializer
from server.apps.core.models import User
from rest_framework.viewsets import ModelViewSet

from django.db.models import Value
from django.db.models.functions import Concat

from server.apps.api.v2.pagination import LimitOffsetPagination
from server.apps.api.v2.permissions import BaseCUREIDAccessPermission
from server.apps.api.v2.renderers import CustomRenderer


class UserViewSet(ModelViewSet):
    pagination_class = LimitOffsetPagination
    permission_classes = [BaseCUREIDAccessPermission,]
    renderer_classes = [CustomRenderer, BrowsableAPIRenderer]

    queryset = User.objects.all()
    serializer_class = UserSerializer

    def get_serializer_class(self):
        serializer = UserSerializer
        current_user_id = str(self.request.user.id)
        pk = self.kwargs.get('pk')
        typeahead = self.request.query_params.get("typeahead", None)
        if current_user_id == pk:
            serializer = EditableUserSerializer
        if typeahead:
            serializer = FullNameSerializer
        return serializer

    def get_queryset(self):
        typeahead = self.request.query_params.get("typeahead", None)
        if typeahead:
            results = (
                self.queryset.annotate(full_name=Concat('first_name', Value(' '), 'last_name')).\
                filter(full_name__istartswith=typeahead)
            )
        else:
            results = self.queryset.order_by("first_name")

        return results

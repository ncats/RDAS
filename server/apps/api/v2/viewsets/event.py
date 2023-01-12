from django.contrib.auth.models import User
from django.db.models import Count, Q, BooleanField, Case, When, Value
from django_filters import FilterSet
from django_filters.filters import CharFilter
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.permissions import SAFE_METHODS
from rest_framework.renderers import BrowsableAPIRenderer
from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet

from server.apps.api.models import LogRequest
from server.apps.api.v2.constants import ALLOWED_SORT, LIMIT_NEWSFEEDS
from server.apps.api.v2.pagination import LimitOffsetPagination
from server.apps.api.v2.permissions import BaseCUREIDAccessPermission
from server.apps.api.v2.renderers import CustomRenderer
from server.apps.api.v2.serializers.event import EventSerializer,WritableEventSerializer
from server.apps.api.v2.views import not_allowed_to_change_status
from server.apps.core.constants import DELETED, APPROVED, FLAGGED, REJECTED
from server.apps.core.models import Event


class EventOrderFilter(DjangoFilterBackend):
    def filter_queryset(self, request, queryset, view):
        queryset = super().filter_queryset(request, queryset, view)

        if request.query_params.get("origin", "") == "home":
            return queryset

        sort = request.query_params.get("sort", None)
        filter_sort = request.query_params.get("filter_sort", "").lower()
        if filter_sort in ALLOWED_SORT and not sort:
            sort = filter_sort

        sort = sort if sort else "latest"
        sort = sort if sort in ALLOWED_SORT else "latest"

        if sort == "latest":
            queryset = queryset.order_by('-updated', '-created')

        if sort == "most-viewed":
            path = "/events/"
            length = len(path)
            lr = LogRequest.objects.filter(path__contains=path).values('path').annotate(count=Count('path')).order_by('-count')[:LIMIT_NEWSFEEDS]
            data = {}
            for item in lr:
                ndx = item["path"].index(path)
                object_id = item["path"][ ndx + length :-1]
                if object_id:
                    data[int(object_id)] = item["count"]
            queryset = sorted(queryset, key=lambda x: data[x.id] if x.id in data else 0, reverse=True)

        return queryset

class EventFilter(FilterSet):
    disease = CharFilter(method='by_disease')
    origin = CharFilter(method='by_origin')

    def by_origin(self, queryset, name, value):
        if isinstance(self.request.user, User):
            favorited_disease_ids = self.request.user.profile.favorited_diseases.all().values_list("id", flat=True)
            return queryset.annotate(
                    favorited=Case(
                        When(disease_id__in=favorited_disease_ids, then=Value(True)),
                        default=Value(False),
                        output_field=BooleanField()
                    )
                ).order_by("-favorited", "-created")
        return queryset

    class Meta:
        model = Event
        fields = '__all__'

    def by_disease(self, queryset, name, value):
        return queryset.filter(disease_id=value)


class EventViewSet(ModelViewSet):
    pagination_class = LimitOffsetPagination
    permission_classes = [BaseCUREIDAccessPermission,]
    queryset = Event.objects.filter(status=APPROVED)
    renderer_classes = [CustomRenderer, BrowsableAPIRenderer]
    filter_class = EventFilter
    filter_backends = (EventOrderFilter,)

    def get_serializer_class(self):
        if self.request.method in SAFE_METHODS:
            return EventSerializer
        return WritableEventSerializer

    def create(self,request):
        try:
            serializer = WritableEventSerializer(data=request.data, context={'request': request})
            serializer.is_valid(raise_exception=True)
            serializer.save()
            if serializer.is_valid():
                return Response({
                    "data":serializer.data,
                },status=200)
            else:
                return Response({
                    "detail":str(serializer.errors)
                },status=400)
        except Exception as e:
            return Response({
                "detail":str(e)
            },status=500)

    def partial_update(self, request, pk=None):
        return self.update(request, pk)

    def update(self, request, pk=None):
        try:
            event = self.queryset.filter(pk=pk)[0]
            if event.author != request.user and not request.user.is_superuser:
                return Response({
                    "detail": "You do not have permission to edit this event."
                }, status=200)

            if not_allowed_to_change_status(request.user, event.status, request.data.get('status', None)):
                return Response({
                    "detail": "You do not have permission to edit status."
                }, status=200)

            serializer = WritableEventSerializer(event, data=request.data, context={'request': request}, partial=True)
            serializer.is_valid(raise_exception=True)
            serializer.save()
            if serializer.is_valid():
                return Response({
                    "data": serializer.data,
                },status=200)
            else:
                return Response({
                    "detail":str(serializer.errors)
                },status=400)
        except Exception as e:
            return Response({
                "detail":str(e)
            },status=400)

    def destroy(self, request, *args, **kwargs):
        instance = self.get_object()
        if request.user.id != instance.author.id:
            return Response({
                "detail": "You do not have permission to delete this."
            }, status=200)
        instance.status = DELETED
        instance.save()
        return Response({
            "detail": "Event deleted.",
            "data": {"id": instance.id}
        }, status=200)

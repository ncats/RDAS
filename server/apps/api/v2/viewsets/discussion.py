from django.contrib.auth.models import User
from django.db.models import Count, Q, ExpressionWrapper, BooleanField
from django_filters import FilterSet
from django_filters.filters import CharFilter
from django_filters.rest_framework import DjangoFilterBackend
from django.utils.timezone import now
from rest_framework.permissions import SAFE_METHODS
from rest_framework.renderers import BrowsableAPIRenderer
from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet
from server.apps.api.v2.pagination import LimitOffsetPagination
from server.apps.api.v2.permissions import BaseCUREIDAccessPermission
from server.apps.api.v2.renderers import CustomRenderer
from server.apps.api.v2.serializers.discussion import DiscussionSerializer, WritableDiscussionSerializer
from server.apps.api.v2.views import not_allowed_to_change_status

import datetime

from server.apps.api.v2.constants import ALLOWED_SORT, LIMIT_NEWSFEEDS
from server.apps.api.models import LogRequest
from server.apps.core.constants import APPROVED
from server.apps.core.models import Discussion, Profile


class DiscussionOrderFilter(DjangoFilterBackend):
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
            path = "/discussions/"
            length = len(path)
            lr = LogRequest.objects.filter(path__contains=path).values('path').annotate(count=Count('path')).order_by('-count')[:LIMIT_NEWSFEEDS]
            data = {}
            for item in lr:
                ndx = item["path"].index(path)
                object_id = item["path"][ ndx + length :-1]
                if object_id:
                    data[int(object_id)] = item["count"]
            queryset = sorted(queryset, key=lambda x: data[x.id] if x.id in data else 0, reverse=True)

        if filter_sort == "most-comments":
            disc_ids = queryset.values('id').annotate(count=Count('id'))
            data = {}
            for item in disc_ids:
                if item["id"]:
                    data[item["id"]] = item["count"]
            queryset = sorted(queryset, key=lambda x: data[x.id] if x.id in data else 0, reverse=True)

        if filter_sort == "most-favorited":
            disc_ids = Profile.objects.values('favorited_discussions__id').annotate(count=Count('favorited_discussions__id'))
            data = {}
            for item in disc_ids:
                id_ = item["favorited_discussions__id"]
                if id_:
                    data[id_] = item["count"]
            queryset = sorted(queryset, key=lambda x: data[x.id] if x.id in data else 0, reverse=True)

        return queryset


class DiscussionFilter(FilterSet):
    disease = CharFilter(method='by_disease')
    date = CharFilter(method='by_date')
    author = CharFilter(method='by_author')
    origin = CharFilter(method='by_origin')

    class Meta:
        model = Discussion
        fields = '__all__'

    def by_origin(self, queryset, name, value):
        if isinstance(self.request.user, User):
            favorited_disease_ids = self.request.user.profile.favorited_diseases.all().values_list("id", flat=True)
            return queryset.annotate(
                    favorited=ExpressionWrapper(
                        Q(disease_id__in=favorited_disease_ids),
                        output_field=BooleanField()
                    )
                ).order_by("-favorited", "-created")
        return queryset

    def by_disease(self, queryset, name, value):
        return queryset.filter(disease_id=value)

    def by_date(self, queryset, name, value):
        value = value.lower()
        if value == "last 30 days":
            days = 30
        elif value == "last 60 days":
            days = 60
        elif value == "last 90 days":
            days = 90
        elif value == "last 6 months":
            days = 120
        else:
            return queryset

        start_date = now() - datetime.timedelta(days)
        return queryset.filter(created__gte=start_date)

    def by_author(self, queryset, name, value):
        values = value.split(',')
        return queryset.filter(author__in=values)


class DiscussionViewSet(ModelViewSet):
  pagination_class = LimitOffsetPagination
  permission_classes = [BaseCUREIDAccessPermission,]
  queryset = Discussion.objects.filter(status=APPROVED)
  renderer_classes = [CustomRenderer, BrowsableAPIRenderer]
  serializer_class = WritableDiscussionSerializer
  filter_class = DiscussionFilter
  filter_backends = (DiscussionOrderFilter,)

  def get_serializer_class(self):
    if self.request.method in SAFE_METHODS:
        return DiscussionSerializer
    return WritableDiscussionSerializer

  def get_queryset(self):
    disease = self.request.query_params.get('disease', None)
    if disease:
        self.queryset = self.queryset.filter(disease_id=int(disease))
    return self.queryset

  def retrieve(self, request, pk=None):
      try:
        discussion = self.queryset.filter(pk=pk)[0]
        if discussion:
            # TODO: what about CUREID staff or admins?
            if (discussion.status != APPROVED or discussion.deleted or discussion.flagged) and request.user != discussion.author:
                discussion = Discussion.objects.none()
            return Response(self.serializer_class(discussion, context={'request': request}).data)
        else:
            self.queryset = Discussion.objects.none()
            return super().retrieve(request, pk)
      except Exception as e:
          return Response(str(e))

  def create(self,request):
    try:
        serializer = WritableDiscussionSerializer(data=request.data, context={'request': request})
        serializer.is_valid(raise_exception=True)
        new_discussion = serializer.save()
        if serializer.is_valid():
            return Response({
                "data":serializer.data
            })
        else:
            return Response({
                "detail":str(serializer.errors)
            },status=400)
    except Exception as e:
        return Response({
            "detail":str(e),
        },status=500
        )

  def partial_update(self, request, pk=None, **kwargs):
      return self.update(request, pk)

  def update(self, request, pk=None):
    try:
        discussion = Discussion.objects.filter(pk=pk)[0]
        if request.user != discussion.author:
            return Response({
                "detail": "You do not have permission to edit this discussion."
            }, status=200)

        if not_allowed_to_change_status(request.user, discussion.status, request.data.get('status', None)):
            return Response({
                "detail": "You do not have permission to edit status."
            }, status=200)

        serializer = WritableDiscussionSerializer(discussion, data=request.data, context={'request': request},)
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
            "detail":str(e),
        },status=500)

  def destroy(self, request, *args, **kwargs):
    instance = self.get_object()
    if request.user != instance.author:
        return Response({
            "detail": "You do not have permission to delete this discussion."
        }, status=200)
    instance.deleted = True
    instance.save()
    return Response({
        "detail": f"Discussion deleted.",
        "data": {"id": instance.id}
    },status=200)

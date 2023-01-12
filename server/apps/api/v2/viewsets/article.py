from django.contrib.auth.models import User
from django.db.models import Count, Q, BooleanField, When, Case, Value
from django_filters import FilterSet
from django_filters.filters import CharFilter
from django_filters.rest_framework import DjangoFilterBackend
from django.utils.timezone import now
from rest_framework.permissions import SAFE_METHODS
from rest_framework.renderers import BrowsableAPIRenderer
from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet
from django.shortcuts import get_object_or_404

import datetime

from server.apps.api.v2.constants import ALLOWED_SORT, LIMIT_NEWSFEEDS
from server.apps.api.models import LogRequest
from server.apps.api.v2.pagination import LimitOffsetPagination
from server.apps.api.v2.permissions import BaseCUREIDAccessPermission
from server.apps.api.v2.renderers import CustomRenderer
from server.apps.api.v2.serializers.article import ArticleSerializer, WritableArticleSerializer
from server.apps.api.v2.views import not_allowed_to_change_status
from server.apps.core.constants import DELETED, APPROVED
from server.apps.core.models import Article


class ArticleOrderFilter(DjangoFilterBackend):
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
            path = "/articles/"
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
            article_ids = queryset.values('id').annotate(count=Count('id'))
            data = {}
            for item in article_ids:
                if item["id"]:
                    data[item["id"]] = item["count"]
            queryset = sorted(queryset, key=lambda x: data[x.id] if x.id in data else 0, reverse=True)

        if filter_sort == "most-favorited":
            article_ids = Profile.objects.values('favorited_articles__id').annotate(count=Count('favorited_articles__id'))
            data = {}
            for item in article_ids:
                id_ = item["favorited_articles__id"]
                if id_:
                    data[id_] = item["count"]
            queryset = sorted(queryset, key=lambda x: data[x.id] if x.id in data else 0, reverse=True)

        return queryset


class ArticleFilter(FilterSet):
    type = CharFilter(method='by_type')
    disease = CharFilter(method='by_disease')
    date = CharFilter(method='by_date')
    author = CharFilter(method='by_author')
    journal = CharFilter(method='by_journal')
    origin = CharFilter(method='by_origin')

    class Meta:
        model = Article
        fields = '__all__'

    def by_type(self, queryset, name, value):
        return queryset.filter(publication_type__iexact=value)

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
        value = value.lower()
        return queryset.filter(published_authors__icontains=value)

    def by_journal(self, queryset, name, value):
        value = value.lower()
        return queryset.filter(publication_name__icontains=value)

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


class ArticleViewSet(ModelViewSet):
    pagination_class = LimitOffsetPagination
    permission_classes = [BaseCUREIDAccessPermission,]
    queryset = Article.objects.filter(status=APPROVED)
    renderer_classes = [CustomRenderer, BrowsableAPIRenderer]
    serializer_class = WritableArticleSerializer
    filter_class = ArticleFilter
    filter_backends = (ArticleOrderFilter,)

    def get_serializer_class(self):
        if self.request.method in SAFE_METHODS:
            return ArticleSerializer
        return WritableArticleSerializer

    def get_queryset(self):
        # TODO: can cause problems in the future

        return self.queryset.exclude(Q(title="") | Q(abstract="") | Q(article_url="") | Q(abstract="Abstract could not be parsed.") | Q(abstract="No abstract available"))

    def create(self, request):
        serializer = WritableArticleSerializer(data=request.data, context={'request': request})
        serializer.is_valid(raise_exception=True)
        serializer.save()
        if serializer.is_valid():
            return Response({
                "data":serializer.data,
            },status=200)
        else:
            return Response({
                "detail": str(serializer.errors)
            },status=400)

    def retrieve(self, request, pk=None):
      queryset = self.queryset
      ct = get_object_or_404(queryset, pk=pk)
      serializer = self.serializer_class(ct)
      return Response(serializer.data)

    def partial_update(self, request, pk=None):
        return self.update(request, pk)

    def update(self, request, pk=None):
        try:
            article = Article.objects.filter(pk=pk)[0]
            if article.author != request.user:
                return Response({
                    "detail": "You do not have permission to edit this article."
                }, status=200)

            if not_allowed_to_change_status(request.user, article.status, request.data.get('status', None)):
                return Response({
                    "detail": "You do not have permission to edit status."
                }, status=200)

            serializer = WritableArticleSerializer(article, data=request.data, context={'request': request}, partial=True)
            serializer.is_valid(raise_exception=True)
            serializer.save()
            if serializer.is_valid():
                return Response({
                    "data":serializer.data,
                },status=200)
            else:
                return Response({
                    "detail": str(serializer.errors)
                },status=400)
        except Exception as e:
            return Response({
                "detail": str(e)
            },status=400)

    def destroy(self, request, *args, **kwargs):
        instance = self.get_object()
        if request.user.id != instance.author.id:
            return Response({
                "detail": "You do not have permission to delete this article."
            }, status=200)
        instance.status = DELETED
        instance.save()
        return Response({
            "detail": "Article deleted.",
            "data": {"id": instance.id}
        },status=200)

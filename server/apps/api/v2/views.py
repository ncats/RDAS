from collections import Counter
from datetime import datetime
from django.contrib.contenttypes.models import ContentType
from django.contrib.auth.models import User
from django.core.paginator import Paginator, PageNotAnInteger, EmptyPage
from django.db.models.functions import Lower
from elasticsearch_dsl import Q
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST, HTTP_500_INTERNAL_SERVER_ERROR

from django.core.exceptions import ValidationError as DjangoValidationError
from django.db.models import Value
from django.db.models.functions import Concat

import math
import metadata_parser
from rest_framework.exceptions import ValidationError

from server.apps.api.v2.serializers.drug import DrugSerializer
from server.apps.api.v2.serializers.article import ArticleSerializer
from server.apps.api.v2.serializers.clinical_trial import ClinicalTrialSerializer
from server.apps.api.v2.serializers.discussion import DiscussionSerializer
from server.apps.api.v2.serializers.event import EventSerializer
from server.apps.api.v2.serializers.report import ReportSerializer, CureReportSerializer
from server.apps.api.v2.serializers.profile import FullNameSerializer
from server.apps.core.constants import ATTACHED_IMAGE_FILLER

from server.apps.core.models import (
    Article,
    ClinicalTrial,
    Discussion,
    Disease,
    Drug,
    Event,
    Report,
    CureReport,
    Comment,
    AttachedImage,
    UserProposedArticle,
)
from server.apps.core.models import Profile as UserProfile
from server.apps.core.constants import SAVED, APPROVED, OUTCOME_CASES, FLAGGED, REJECTED, DELETED
from server.apps.core.constants_country import COUNTRIES
from server.apps.search.documents import DiseaseDocument
from .constants import ITEMS_PER_PAGE


def not_allowed_to_change_status(user, current_status, new_status):
    if not new_status:
        return False
    if current_status == new_status:
        return False
    statuses = [APPROVED, FLAGGED, REJECTED]
    if not user.is_superuser and (current_status in statuses or new_status in statuses):
        return True


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def get_unfinished_reports(request):
    user = request.user
    unfinished_reports = CureReport.objects.filter(author=user, status=SAVED).order_by("-updated")

    reports, next_, previous, count = pagination_vars(
        request,
        "/v2/unfinished-reports",
        unfinished_reports
    )

    data = CureReportSerializer(reports, many=True, context={"request":request})
    return Response(
        status=HTTP_200_OK,
        data={
            "count": count,
            "next": next_,
            "previous": previous,
            "results": data.data,
        },
    )


# TODO: why not combine get_all_reports and get_unfinished_reports
@api_view(['GET'])
@permission_classes([IsAuthenticated])
def get_all_reports(request):
    user = request.user
    all_reports = CureReport.objects.filter(author=user).exclude(status=DELETED)

    reports, next_, previous, count = pagination_vars(
        request,
        "/v2/all-reports",
        all_reports
    )

    data = CureReportSerializer(reports, many=True, context={"request": request})
    return Response(
        status=HTTP_200_OK,
        data={
            "count": count,
            "next": next_,
            "previous": previous,
            "results": data.data,
        },
    )


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def get_all_discussions(request):
    user = request.user
    all_discussions = Discussion.objects.filter(author=user).exclude(
            status=DELETED
        ).exclude(
            deleted=True
        ).order_by("-updated")

    discussions, next_, previous, count = pagination_vars(
        request,
        "/v2/all-discussions",
        all_discussions
    )

    data = DiscussionSerializer(discussions, many=True, context={"request": request})
    return Response(
        status=HTTP_200_OK,
        data={
            "count": count,
            "next": next_,
            "previous": previous,
            "results": data.data,
        },
    )


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def get_all_clinical_trials(request):
    user = request.user
    # TODO: CTs doesn't have "author" field
    all_cts = ClinicalTrial.objects.filter(author=user).exclude(
            status=DELETED
        ).exclude(
            deleted=True
        ).order_by("-updated")

    cts, next_, previous, count = pagination_vars(
        request,
        "/v2/all-clinical-trials",
        all_cts,
    )

    data = ClinicalTrialSerializer(cts, many=True, context={"request": request})
    return Response(
        status=HTTP_200_OK,
        data={
            "count": count,
            "next": next_,
            "previous": previous,
            "results": data.data,
        },
    )


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def get_all_articles(request):
    user = request.user
    # TODO: Article doesn't have "author" field
    all_articles = Article.objects.filter(author=user).exclude(status=DELETED).order_by("-updated")

    articles, next_, previous, count = pagination_vars(
        request,
        "/v2/all-articles",
        all_articles,
    )

    data = ArticleSerializer(articles, many=True, context={"request": request})
    return Response(
        status=HTTP_200_OK,
        data={
            "count": count,
            "next": next_,
            "previous": previous,
            "results": data.data,
        },
    )


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def get_all_events(request):
    user = request.user
    # TODO: Event doesn't have "author" field
    all_events = Event.objects.filter(author=user).exclude(status=DELETED).order_by("-updated")

    events, next_, previous, count = pagination_vars(
        request,
        "/v2/all-events",
        all_events
    )

    data = EventSerializer(events, many=True, context={"request": request})
    return Response(
        status=HTTP_200_OK,
        data={
            "count": count,
            "next": next_,
            "previous": previous,
            "results": data.data,
        },
    )


@api_view(["GET"])
def most_common_regimen(request):
    disease_id = request.GET.get('disease', None)
    if not disease_id:
        return Response(
            status=HTTP_400_BAD_REQUEST,
            data={"detail": "<disease> parameter is missing or is blank."}
        )

    cure_reports = CureReport.objects.filter(
        report__disease_id=disease_id,
        status=APPROVED
    ).select_related('report')
    data = []
    drugs = {}
    outcomes = {}
    for creport in cure_reports:
        r = creport.report
        regimen = r.regimens.all().select_related("drug")
        drug_names = [i.drug.name for i in regimen]
        key = sorted(drug_names)
        data.append(key)
        drugs["|".join(key)] = [{"id": i.drug.id, "name": i.drug.name} for i in regimen]

        outcomes_key = "|".join(key)
        if outcomes_key not in outcomes:
            outcomes[outcomes_key] = {"deteriorated": 0, "improved": 0, "undetermined": 0}
        outcome_computed = OUTCOME_CASES[getattr(r, "outcome", "")].lower()
        outcomes[outcomes_key][outcome_computed] += 1

    if data:
        data = map(sorted, data)
        counted_data = [
            {
                "drugs": drugs["|".join(key)],
                "total": value,
                "undetermined": outcomes["|".join(key)]["undetermined"],
                "improved": outcomes["|".join(key)]["improved"],
                "deteriorated": outcomes["|".join(key)]["deteriorated"],
            } for key, value in Counter(map(tuple, data)).items()
        ]

        return Response(
            {
                "data": counted_data,
                "disease_id": disease_id,
            },
            status=HTTP_200_OK,
        )
    else:
        return Response(
            {"detail": "No regimens available."},
            status=HTTP_200_OK,
        )

@api_view(["GET"])
def disease_drug_typeahead(request):
    # TODO: not using ES
    value = request.query_params.get("typeahead", None)
    if not value:
        return Response({
                "disease": [],
                "drug": [],
            },
            status=HTTP_200_OK,
        )

    value = str(value)
    diseases = Disease.objects.filter(name__icontains=value)
    diseases = diseases.values("id", "name").order_by(Lower("name"))
    drugs = Drug.objects.filter(name__icontains=value)
    drugs = drugs.values("id", "name").order_by(Lower("name"))

    return Response({
            "disease": diseases,
            "drug": drugs,
        },
        status=HTTP_200_OK,
    )


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def get_reports_by_interest(request):
    user = request.user
    diseases = user.profile.favorited_diseases.all()
    interesting_reports = Report.objects.filter(disease__in=diseases)

    reports, next_, previous, count = pagination_vars(
        request,
        "/v2/reports-by-interest",
        interesting_reports,
    )

    data = ReportSerializer(reports, many=True, context={"request":request})
    return Response(
        status=HTTP_200_OK,
        data={
            "count": count,
            "next": next_,
            "previous": previous,
            "results": data.data,
        },
    )


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def get_cts_by_interest(request):
    user = request.user
    diseases = user.profile.favorited_diseases.all()
    interesting_cts = ClinicalTrial.objects.filter(disease__in=diseases)

    cts, next_, previous, count = pagination_vars(
        request,
        "/v2/cts-by-interest",
        interesting_cts,
    )

    data = ClinicalTrialSerializer(cts, many=True, context={"request":request})
    return Response(
        status=HTTP_200_OK,
        data={
            "count": count,
            "next": next_,
            "previous": previous,
            "results": data.data,
        },
    )


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def get_discussions_by_interest(request):
    user = request.user
    diseases = user.profile.favorited_diseases.all()
    interesting_discussions = Discussion.objects.filter(disease__in=diseases)

    discussions, next_, previous, count = pagination_vars(
        request,
        "/v2/cts-by-interest",
        interesting_discussions,
    )

    data = DiscussionSerializer(discussions, many=True, context={"request":request})
    return Response(
        status=HTTP_200_OK,
        data={
            "count": count,
            "next": next_,
            "previous": previous,
            "results": data.data,
        },
    )

@api_view(["GET"])
@permission_classes([IsAuthenticated])
def get_articles_by_interest(request):
    user = request.user
    diseases = user.profile.favorited_diseases.all()
    interesting_articles = Article.objects.filter(disease__in=diseases)

    discussions, next_, previous, count = pagination_vars(
        request,
        "/v2/articles-by-interest",
        interesting_articles,
    )

    data = ArticleSerializer(discussions, many=True, context={"request":request})
    return Response(
        status=HTTP_200_OK,
        data={
            "count": count,
            "next": next_,
            "previous": previous,
            "results": data.data,
        },
    )

@api_view(["GET"])
@permission_classes([IsAuthenticated])
def get_events_by_interest(request):
    user = request.user
    diseases = user.profile.favorited_diseases.all()
    interesting_events = Event.objects.filter(disease__in=diseases)

    discussions, next_, previous, count = pagination_vars(
        request,
        "/v2/events-by-interest",
        interesting_events,
    )

    data = EventSerializer(discussions, many=True, context={"request":request})
    return Response(
        status=HTTP_200_OK,
        data={
            "count": count,
            "next": next_,
            "previous": previous,
            "results": data.data,
        },
    )


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def get_favorite_reports(request):
    user = request.user
    favorited_reports = user.profile.favorited_reports.all()
    curereports = CureReport.objects.filter(report__in=favorited_reports, status=APPROVED)

    reports, next_, previous, count = pagination_vars(
        request,
        "/v2/favorite-reports",
        curereports,
    )

    data = CureReportSerializer(reports, many=True, context={"request":request})
    return Response(
        status=HTTP_200_OK,
        data={
            "count": count,
            "next": next_,
            "previous": previous,
            "results": data.data,
        },
    )


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def get_favorite_discussions(request):
    user = request.user
    favorited_discussions = user.profile.favorited_discussions.filter(status=APPROVED)

    discussions, next_, previous, count = pagination_vars(
        request,
        "/v2/favorite-discussions",
        favorited_discussions,
    )

    data = DiscussionSerializer(discussions, many=True, context={"request":request})
    return Response(
        status=HTTP_200_OK,
        data={
            "count": count,
            "next": next_,
            "previous": previous,
            "results": data.data,
        },
    )


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def get_favorite_clinical_trials(request):
    user = request.user
    favorited_clinical_trials = user.profile.favorited_clinical_trials.filter(status=APPROVED)

    clinical_trials, next_, previous, count = pagination_vars(
        request,
        "/v2/favorite-clinical-trials",
        favorited_clinical_trials,
    )

    data = ClinicalTrialSerializer(clinical_trials, many=True, context={"request":request})
    return Response(
        status=HTTP_200_OK,
        data={
            "count": count,
            "next": next_,
            "previous": previous,
            "results": data.data,
        },
    )


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def get_favorite_articles(request):
    user = request.user
    favorited_articles = user.profile.favorited_articles.filter(status=APPROVED)

    articles, next_, previous, count = pagination_vars(
        request,
        "/v2/favorite-articles",
        favorited_articles,
    )

    data = ArticleSerializer(articles, many=True, context={"request":request})
    return Response(
        status=HTTP_200_OK,
        data={
            "count": count,
            "next": next_,
            "previous": previous,
            "results": data.data,
        },
    )


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def get_favorite_events(request):
    user = request.user
    favorited_events = user.profile.favorited_events.filter(status=APPROVED)

    events, next_, previous, count = pagination_vars(
        request,
        "/v2/favorite-events",
        favorited_events,
    )

    data = EventSerializer(events, many=True, context={"request":request})
    return Response(
        status=HTTP_200_OK,
        data={
            "count": count,
            "next": next_,
            "previous": previous,
            "results": data.data,
        },
    )

@api_view(['GET'])
def get_tuberculosis_resistant_drugs(request):
    tb_drugs = Drug.objects.filter(is_tuberculosis_resistant=True)

    data = DrugSerializer(tb_drugs, many=True, context={"request":request})
    return Response(
        status=HTTP_200_OK,
        data={
            "results": data.data,
        },
    )
def pagination_vars(request, url, objects):
    limit = int(request.GET.get('limit', ITEMS_PER_PAGE))
    offset = int(request.GET.get('offset', 0))
    page = math.floor(offset/limit + 1)
    count = objects.count()

    if offset + limit >= count:
        next_ = None
    else:
        new_offset = offset + limit
        next_ = request.build_absolute_uri(f"{url}?limit={limit}&offset={new_offset}")

    if page == 1:
        previous = None
    elif next_:
        previous = request.build_absolute_uri()
    else:
        new_offset = offset - limit
        previous = request.build_absolute_uri(f"{url}?limit={limit}&offset={new_offset}")

    paginator = Paginator(objects, limit)
    try:
        data = paginator.page(page)
    except PageNotAnInteger:
        data = paginator.page(1)
    except EmptyPage:
        data = paginator.page(paginator.num_pages)

    return [data, next_, previous, count]


@api_view(['GET'])
def search_diseases(request):
    keyword = request.GET.get('typeahead', '').lower()
    results = []
    if keyword:
        docs = DiseaseDocument.search()
        pattern = f".*{keyword}.*"
        q_name = Q("regexp", name=pattern)
        q_synonyms = Q("nested", path="synonyms", query=Q("regexp", synonyms__key=pattern))
        q_cldiseases = Q("nested", path="cross_linked_diseases", query=Q("regexp", cross_linked_diseases__key=pattern))
        results = docs.query(q_name | q_synonyms | q_cldiseases)

    data = []
    for hit in results:

        cldiseases = {}
        for pair in hit.cross_linked_diseases:
            reports = cldiseases.get(pair['key'], [])
            reports.append(pair['report_id'])
            cldiseases[pair['key']] = reports

        item = {
            "id": hit.meta.id,
            "name": hit.name,
            "synonyms": [{"name": i['key']} for i in hit.synonyms],
            "url": hit.url,
            "discussion_count": hit.discussion_count,
            "report_count": hit.report_count,
            "trial_count": hit.trial_count,
            "event_count": hit.event_count,
            "article_count": hit.article_count,
            "image_url": hit.image_url,
            "times_viewed": hit.times_viewed,
            "cross_linked_diseases": cldiseases,
            "_score": hit.meta.score,
        }
        data.append(item)

    return Response(
        status=HTTP_200_OK,
        data=data,
    )


@api_view(['GET'])
def get_countries(request):
    keyword = request.GET.get("typeahead", "").lower()
    if keyword:
        found = filter(lambda x: keyword in x[0].lower(), COUNTRIES)
    else:
        found = COUNTRIES

    results = [{"name": i[0]} for i in found]
    return Response(
        status=HTTP_200_OK,
        data={"results": results,}
    )


@api_view(['GET'])
def get_sponsors(request):
    sponsors = ClinicalTrial.objects.filter(status=APPROVED).values_list('sponsor', flat=True)
    results = set()
    for sponsor in sponsors:
        if not sponsor:
            continue
        sponsor = sponsor.replace(", ", "; ")
        values = sponsor.split(",")
        results.update(map(lambda x: x.replace("; ", ", "), values))

    sorted_results = sorted(results)

    keyword = request.GET.get("typeahead", "").lower()
    if keyword:
        found = filter(lambda x: keyword in x.lower(), sorted_results)
    else:
        found = sorted_results

    # Limit output to 20 first sponsors
    data = [{"name": item} for item in found][:20]

    return Response(
        status=HTTP_200_OK,
        data={"results": data,}
    )


@api_view(['GET'])
def get_authors(request):
    # makes sense only for articles
    authors = Article.objects.filter(status=APPROVED).values_list("published_authors", flat=True)
    results = set()
    for author in authors:
        if not author:
            continue
        values = author.splitlines()
        values = [item.split(",") for item in values]
        flat_values = [item for sublist in values for item in sublist]
        results.update(flat_values)

    if "" in results:
        results.remove("")
    if "et al." in results:
        results.remove("et al.")
    sorted_authors = sorted(results)

    keyword = request.GET.get("typeahead", "").lower()
    if keyword:
        found = filter(lambda x: keyword in x.lower(), sorted_authors)
    else:
        found = sorted_authors

    data = [{"name": item} for item in found]

    return Response(
        status=HTTP_200_OK,
        data={"results": data,}
    )

@api_view(['GET'])
def get_years(request):
    keyword = request.GET.get("typeahead", "").lower()
    all_years = []
    for year in _compose_years():
        if keyword and not year.startswith(keyword):
            continue
        all_years.append({"name": year})

    length = len(keyword)
    if "1979"[:length] >= keyword:
        all_years.append({"name": "Pre-1980"})

    years = {"results": all_years,}

    return Response(
        status=HTTP_200_OK,
        data=years,
    )

def _compose_years():
    current_year = datetime.now().year
    years=[]
    for year in range(current_year, 1979, -1):
        years.append(str(year))
    return years

def unfurl_article(sourceurl,source=None):
    try:
        headers = { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        page = metadata_parser.MetadataParser(url=sourceurl,
            force_doctype=True,
            search_head_only=False,
            only_parse_http_ok=False,
            support_malformed=True,
            url_headers=headers,
        )
        type = page.get_metadatas('type')
        if not type:
            type = ['article']

        #for cases where the
        if source == 'pubmed':
            if 'not available' in page.get_metadatas('title')[0]:
                    return None

        publication_name = page.get_metadatas('site_name')[0] if page.get_metadatas('site_name') else ''
        field_names = ['al:ipad:app_name', 'twitter:app:name:googleplay', 'al:iphone:app_name', 'al:android:app_name']
        while not publication_name and field_names:
            field = field_names.pop()
            publication_name = page.get_metadatas(field)[0] if page.get_metadatas(field) else ''

        published_authors = page.get_metadatas('citation_authors')[0] if page.get_metadatas('citation_authors') else''
        if not published_authors:
            published_authors = page.get_metadatas('byl')[0] if page.get_metadatas('byl') else''
            if 'By ' in published_authors:
                published_authors = published_authors.replace('By ', '')

        data = {
            "title": page.get_metadatas('title')[0] if page.get_metadatas('title') else '',
            "article_url": page.get_metadatas('url')[0] if page.get_metadatas('url') else sourceurl,
            "abstract": page.get_metadatas('description')[0] if page.get_metadatas('description') else '',
            "publication_name": publication_name,
            "publication_type": 'news' if type[0] == 'article' else 'journal',
            "published_authors": published_authors,
            "doi": page.get_metadatas('citation_doi')[0] if page.get_metadatas('citation_doi') else'',
            "pubmed_id": page.get_metadatas('citation_pmid')[0] if page.get_metadatas('citation_pmid') else'',
            "attached_images": []

        }

        disease = getDiseaseFromTitle(data['title'])
        if disease:
            data['disease'] = disease

        images = page.get_metadatas('image')
        for i in images:
            index = images.index(i)
            image = {
                "url": i,
                "caption": page.get_metadatas('image:alt')[index] if page.get_metadatas('image:alt') else ''
            }
            data['attached_images'].append(image)


        return Response({
            "data":data
        },status=HTTP_200_OK)
    except Exception as e:
        return Response({
            "data":{},
            # "message": str(e)
        },status=HTTP_200_OK
        )

def getDiseaseFromTitle(title):
    return Disease.objects.filter(name__in=title.split()).values().first()


def check_url(url: str) -> bool:
    import requests
    try:
        requests.get(url, timeout=1)
    except requests.exceptions.ReadTimeout:
        return False
    return True


@api_view(['GET'])
def get_article_by_url_pubmed_id(request):
    try:
        url = request.query_params.get('url', None)
        pubmed_id = request.query_params.get('pubmed_id', None)
        found = False
        if pubmed_id:
            article = Article.objects.filter(pubmed_id=pubmed_id).first()
        if url:
            article = Article.objects.filter(article_url=url).first()
        if article:
            found=True
            serializer = ArticleSerializer(article)
        if not found and pubmed_id:
            PUBMED_URL = f"https://pubmed.ncbi.nlm.nih.gov/{pubmed_id}/"
            data = unfurl_article(PUBMED_URL, "pubmed")
            if data is not None:
                return data
        if not found and url and check_url(url):
            return unfurl_article(url)

        if not found:
            return Response({
                "data":{},
            }   ,status=HTTP_200_OK)

        return Response({
            "data": serializer.data
        },status=200)

    except Exception as e:
        return Response({
            "detail": str(e)
        },status=200)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def public_profile(request, *args, **kwargs):
    profile = {}
    try:
        user = User.objects.get(pk=kwargs["user_id"])
        profile["first_name"] = user.first_name
        profile["last_name"] = user.last_name

        content_type = ContentType.objects.get_for_model(user.profile)
        profile_attached_image = AttachedImage.objects.filter(content_type_id=content_type, object_id=user.profile.id).first()
        profile_image = ""
        if profile_attached_image:
            if profile_attached_image.url == ATTACHED_IMAGE_FILLER:
                profile_image = f"assets/{profile_image.url}"
            else:
                profile_image = profile_attached_image.url

        profile["profile"] = {
            "qualification": user.profile.qualification,
            "profile_image": profile_image,
            "specialty": user.profile.specialty,
            "institution": user.profile.institution,
            "fav_diseases": [{"name": i.name, "id": i.id} for i in user.profile.favorited_diseases.all()],
            "fav_drugs": [{"name": i.name, "id": i.id} for i in user.profile.favorited_drugs.all()],
        }
    except User.DoesNotExist:
        return Response({
                "detail": "No data found.",
            },
            status=HTTP_400_BAD_REQUEST,
        )
    except UserProfile.DoesNotExist:
        return Response({
                "detail": "We couldn't fulfill your request.",
            },
            status=HTTP_500_INTERNAL_SERVER_ERROR,
        )

    return Response({
            "detail": profile,
        },
        status=HTTP_200_OK
    )


@api_view(['GET'])
def search_user(request):
    typeahead = request.query_params.get('typeahead', None)

    if typeahead:
        users = User.objects.annotate(full_name=Concat('first_name', Value(' '), 'last_name')).filter(full_name__istartswith=typeahead)
        serializer = FullNameSerializer(users, many=True)


        return Response(
        status=HTTP_200_OK,
        data={
            "results": serializer.data[:5],
        })

    return Response({
        "data":"No typeahead"
    },status=HTTP_200_OK)


@api_view(['PUT'])
def report_comment(request, *args, **kwargs):
    comment_id = kwargs['comment_id']
    comment = Comment.objects.filter(pk=comment_id).update(flagged=True)

    return Response({
        "detail":'Comment flagged.'
    })

@api_view(['PUT'])
def untag_comment(request, *args, **kwargs):
    import re
    from server.apps.api.v2.serializers.comment import CommentSerializer

    comment = Comment.objects.get(pk=kwargs['comment_id'])
    CLEANR = re.compile('<.*?>') 
    cleantext = re.sub(CLEANR, '', comment.body)
    Comment.objects.filter(pk=kwargs['comment_id']).update(body=cleantext)
    
    comments = Comment.objects.filter(content_type_id=comment.content_type_id, object_id=comment.object_id).order_by('created', 'parent')
    all_organized = _organize_comments(comments)

    serializer = CommentSerializer(all_organized, many=True)
    return Response({
        "data":serializer.data
    })

    
def _organize_comments(comments):
    root_comments = []
    all_comments = {}
    data = list(comments)
    for comment in data:
        comment.children = []
        all_comments[comment.id] = comment
        if not comment.parent:
            root_comments.append(comment)
        else:
            all_comments[comment.parent_id].children.append(comment)

    cleaned_comments = _remove_childless_comments(root_comments)
    return cleaned_comments

def _remove_childless_comments(comments):
    """ Removes Deleted or Flagged comments (from the arg list)
        that don't have other comments pointing to them.
    """
    for item in comments:
        if item.children:
            item.children = _remove_childless_comments(item.children)
        if item.deleted and not item.children:
            comments.remove(item)
    return comments

@api_view(['POST',])
@permission_classes([IsAuthenticated])
def add_user_proposed_article(request):
    author = request.user
    pubmed_id = request.data.get("pubmed_id", None)
    article_url = request.data.get("article_url", None)

    try:
        upa = UserProposedArticle(author=author, pubmed_id=pubmed_id, article_url=article_url)
        upa.save()
    except DjangoValidationError as e:
        return Response(
            status=HTTP_400_BAD_REQUEST,
            data={
                "detail": e.message,
            }
        )

    return Response(
        status=HTTP_200_OK,
        data={
            "detail": "Thank you for the suggestion.",
        },
    )


